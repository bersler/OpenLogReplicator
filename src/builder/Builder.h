/* Header for Builder class
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <atomic>
#include <cmath>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "../common/Ctx.h"
#include "../common/LobCtx.h"
#include "../common/LobData.h"
#include "../common/LobKey.h"
#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/types.h"
#include "../common/typeLobId.h"
#include "../common/typeRowId.h"
#include "../common/typeXid.h"
#include "../common/exception/RedoLogException.h"
#include "../locales/CharacterSet.h"
#include "../locales/Locales.h"

#ifndef BUILDER_H_
#define BUILDER_H_

namespace OpenLogReplicator {
    class Builder;
    class Ctx;
    class CharacterSet;
    class DbTable;
    class Locales;
    class Metadata;
    class SystemTransaction;
    class XmlCtx;

    struct BuilderQueue {
        uint64_t id;
        std::atomic<uint64_t> size;
        std::atomic<uint64_t> start;
        uint8_t* data;
        std::atomic<BuilderQueue*> next;
    };

    struct BuilderMsg {
        enum class OUTPUT_BUFFER : unsigned short int {
            NONE = 0, ALLOCATED = 1 << 0, CONFIRMED = 1 << 1, CHECKPOINT = 1 << 2
        };

        void* ptr;
        uint64_t id;
        uint64_t queueId;
        std::atomic<uint64_t> size;
        uint64_t tagSize;
        typeScn scn;
        typeScn lwnScn;
        typeIdx lwnIdx;
        uint8_t* data;
        typeSeq sequence;
        typeObj obj;
        OUTPUT_BUFFER flags;

        bool isFlagsSet(OUTPUT_BUFFER flag) const {
            return (static_cast<uint>(flags) & static_cast<uint>(flag)) != 0;
        };

        void setFlag(OUTPUT_BUFFER flag) {
            flags = static_cast<OUTPUT_BUFFER>(static_cast<OUTPUT_BUFFER>(static_cast<uint>(flags) | static_cast<uint>(flag)));
        };

        void unsetFlag(OUTPUT_BUFFER flag) {
            flags = static_cast<OUTPUT_BUFFER>(static_cast<OUTPUT_BUFFER>(static_cast<uint>(flags) & ~static_cast<uint>(flag)));
        };
    };

    class Builder {
    public:
        static constexpr uint64_t OUTPUT_BUFFER_DATA_SIZE = Ctx::MEMORY_CHUNK_SIZE - sizeof(struct BuilderQueue);

        enum class ATTRIBUTES_FORMAT {
            DEFAULT = 0, BEGIN = 1UL << 0, DML = 1UL << 1, COMMIT = 1UL << 2
        };

        enum class DB_FORMAT {
            DEFAULT = 0, ADD_DML = 1, ADD_DDL = 2
        };

        enum class CHAR_FORMAT {
            UTF8 = 0, NOMAPPING = 1, HEX = 2
        };

        enum class COLUMN_FORMAT {
            CHANGED = 0,        // Default, only changed columns for update, or PK
            FULL_INS_DEC = 1,   // Show full nulls from insert and delete
            FULL_UPD = 2        // Show all from redo;
        };

        enum class INTERVAL_DTS_FORMAT {
            UNIX_NANO, UNIX_MICRO, UNIX_MILLI, UNIX, UNIX_NANO_STRING, UNIX_MICRO_STRING, UNIX_MILLI_STRING, UNIX_STRING, ISO8601_SPACE, ISO8601_COMMA,
            ISO8601_DASH
        };

        enum class INTERVAL_YTM_FORMAT {
            MONTHS, MONTHS_STRING, STRING_YM_SPACE, STRING_YM_COMMA, STRING_YM_DASH
        };

        enum class MESSAGE_FORMAT {
            DEFAULT, FULL = 1 << 0, ADD_SEQUENCES = 1 << 1,
            // JSON only:
            SKIP_BEGIN = 1 << 2, SKIP_COMMIT = 1 << 3, ADD_OFFSET = 1 << 4
        };

        enum class RID_FORMAT {
            SKIP, TEXT
        };

        enum class SCN_FORMAT {
            NUMERIC, TEXT_HEX
        };

        enum class SCN_TYPE {
            NONE = 0, ALL_PAYLOADS = 1 << 0, COMMIT_VALUE = 1 << 1
        };

        enum class SCHEMA_FORMAT {
            DEFAULT = 0, FULL = 1 << 0, REPEATED = 1 << 1, OBJ = 1 << 2
        };

        enum class TIMESTAMP_ALL {
            JUST_BEGIN, ALL_PAYLOADS
        };

        enum class TIMESTAMP_FORMAT {
            UNIX_NANO, UNIX_MICRO, UNIX_MILLI, UNIX, UNIX_NANO_STRING, UNIX_MICRO_STRING, UNIX_MILLI_STRING, UNIX_STRING, ISO8601_NANO_TZ, ISO8601_MICRO_TZ,
            ISO8601_MILLI_TZ, ISO8601_TZ, ISO8601_NANO, ISO8601_MICRO, ISO8601_MILLI, ISO8601
        };

        enum class TIMESTAMP_TZ_FORMAT {
            UNIX_NANO_STRING, UNIX_MICRO_STRING, UNIX_MILLI_STRING, UNIX_STRING, ISO8601_NANO_TZ, ISO8601_MICRO_TZ, ISO8601_MILLI_TZ, ISO8601_TZ, ISO8601_NANO,
            ISO8601_MICRO, ISO8601_MILLI, ISO8601
        };

        enum class TRANSACTION_TYPE {
            T_NONE, INSERT, DELETE, UPDATE
        };

        enum class UNKNOWN_FORMAT {
            QUESTION_MARK, DUMP
        };

        enum class UNKNOWN_TYPE {
            HIDE, SHOW
        };

        enum class VALUE_TYPE {
            BEFORE, AFTER, BEFORE_SUPP, AFTER_SUPP, LENGTH
        };

        enum class XID_FORMAT {
            TEXT_HEX, TEXT_DEC, NUMERIC
        };

    protected:
        static constexpr uint64_t BUFFER_START_UNDEFINED{0xFFFFFFFFFFFFFFFF};

        static constexpr uint64_t VALUE_BUFFER_MIN{1048576};
        static constexpr uint64_t VALUE_BUFFER_MAX{4294967296};

        static constexpr uint8_t XML_HEADER_STANDALONE{0x01};
        static constexpr uint8_t XML_HEADER_XMLDECL{0x02};
        static constexpr uint8_t XML_HEADER_ENCODING{0x04};
        static constexpr uint8_t XML_HEADER_VERSION{0x08};
        static constexpr uint8_t XML_HEADER_STANDALONE_YES{0x10};
        static constexpr uint8_t XML_HEADER_VERSION_1_1{0x80};

        static constexpr uint8_t XML_PROLOG_RGUID{0x04};
        static constexpr uint8_t XML_PROLOG_DOCID{0x08};
        static constexpr uint8_t XML_PROLOG_PATHID{0x10};
        static constexpr uint8_t XML_PROLOG_BIGINT{0x40};

        Ctx* ctx;
        Locales* locales;
        Metadata* metadata;
        BuilderMsg* msg;

        DB_FORMAT dbFormat;
        ATTRIBUTES_FORMAT attributesFormat;
        INTERVAL_DTS_FORMAT intervalDtsFormat;
        INTERVAL_YTM_FORMAT intervalYtmFormat;
        MESSAGE_FORMAT messageFormat;
        RID_FORMAT ridFormat;
        XID_FORMAT xidFormat;
        TIMESTAMP_FORMAT timestampFormat;
        TIMESTAMP_TZ_FORMAT timestampTzFormat;
        TIMESTAMP_ALL timestampAll;
        CHAR_FORMAT charFormat;
        SCN_FORMAT scnFormat;
        SCN_TYPE scnType;
        UNKNOWN_FORMAT unknownFormat;
        SCHEMA_FORMAT schemaFormat;
        COLUMN_FORMAT columnFormat;
        UNKNOWN_TYPE unknownType;
        uint64_t unconfirmedSize;
        uint64_t messageSize;
        uint64_t messagePosition;
        uint64_t flushBuffer;
        char* valueBuffer;
        uint64_t valueSize;
        uint64_t valueBufferSize;
        char* valueBufferOld;
        uint64_t valueSizeOld;
        std::unordered_set<const DbTable*> tables;
        typeScn commitScn;
        typeXid lastXid;
        uint64_t valuesSet[Ctx::COLUMN_LIMIT_23_0 / sizeof(uint64_t)];
        uint64_t valuesMerge[Ctx::COLUMN_LIMIT_23_0 / sizeof(uint64_t)];
        int64_t sizes[Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(VALUE_TYPE::LENGTH)];
        const uint8_t* values[Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(VALUE_TYPE::LENGTH)];
        uint64_t sizesPart[3][Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(VALUE_TYPE::LENGTH)];
        const uint8_t* valuesPart[3][Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(VALUE_TYPE::LENGTH)];
        uint64_t valuesMax;
        uint8_t* merges[Ctx::COLUMN_LIMIT_23_0 * static_cast<uint>(VALUE_TYPE::LENGTH)];
        uint64_t mergesMax;
        uint64_t id;
        uint64_t num;
        uint64_t maxMessageMb;      // Maximum message size able to handle by writer
        bool newTran;
        bool compressedBefore;
        bool compressedAfter;
        uint8_t prevChars[CharacterSet::MAX_CHARACTER_LENGTH * 2];
        uint64_t prevCharsSize;
        const std::unordered_map<std::string, std::string>* attributes;

        std::mutex mtx;
        std::condition_variable condNoWriterWork;

        double decodeFloat(const uint8_t* data);
        long double decodeDouble(const uint8_t* data);

        bool isAttributesFormatBegin() const {
            return (static_cast<uint>(attributesFormat) & static_cast<uint>(ATTRIBUTES_FORMAT::BEGIN)) != 0;
        };

        bool isAttributesFormatDml() const {
            return (static_cast<uint>(attributesFormat) & static_cast<uint>(ATTRIBUTES_FORMAT::DML)) != 0;
        };

        bool isAttributesFormatCommit() const {
            return (static_cast<uint>(attributesFormat) & static_cast<uint>(ATTRIBUTES_FORMAT::COMMIT)) != 0;
        };

        bool isCharFormatNoMapping() const {
            return (static_cast<uint>(charFormat) & static_cast<uint>(CHAR_FORMAT::NOMAPPING)) != 0;
        };

        bool isCharFormatHex() const {
            return (static_cast<uint>(charFormat) & static_cast<uint>(CHAR_FORMAT::HEX)) != 0;
        };

        bool isScnTypeAllPayloads() const {
            return (static_cast<uint>(scnType) & static_cast<uint>(SCN_TYPE::ALL_PAYLOADS)) != 0;
        };

        bool isScnTypeCommitValue() const {
            return (static_cast<uint>(scnType) & static_cast<uint>(SCN_TYPE::COMMIT_VALUE)) != 0;
        };

        bool isSchemaFormatFull() const {
            return (static_cast<uint>(schemaFormat) & static_cast<uint>(SCHEMA_FORMAT::FULL)) != 0;
        };

        bool isSchemaFormatRepeated() const {
            return (static_cast<uint>(schemaFormat) & static_cast<uint>(SCHEMA_FORMAT::REPEATED)) != 0;
        };

        bool isSchemaFormatObj() const {
            return (static_cast<uint>(schemaFormat) & static_cast<uint>(SCHEMA_FORMAT::OBJ)) != 0;
        };

        bool isMessageFormatFull() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::FULL)) != 0;
        }

        bool isMessageFormatAddSequences() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::ADD_SEQUENCES)) != 0;
        }

        bool isMessageFormatSkipBegin() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::SKIP_BEGIN)) != 0;
        }

        bool isMessageFormatSkipCommit() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::SKIP_COMMIT)) != 0;
        }

        bool isMessageFormatAddOffset() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::ADD_OFFSET)) != 0;
        }

        bool isDbFormatAddDml() const {
            return (static_cast<uint>(dbFormat) & static_cast<uint>(DB_FORMAT::ADD_DML)) != 0;
        }

        bool isDbFormatAddDdl() const {
            return (static_cast<uint>(dbFormat) & static_cast<uint>(DB_FORMAT::ADD_DDL)) != 0;
        }

        inline void builderRotate(bool copy) {
            if (messageSize > ctx->memoryChunksWriteBufferMax * Ctx::MEMORY_CHUNK_SIZE_MB * 1024 * 1024)
                throw RedoLogException(10072, "writer buffer (parameter \"write-buffer-max-mb\" = " +
                                              std::to_string(ctx->memoryChunksWriteBufferMax * Ctx::MEMORY_CHUNK_SIZE_MB) +
                                              ") is too small to fit a message with size: " +
                                              std::to_string(messageSize));
            auto nextBuffer = reinterpret_cast<BuilderQueue*>(ctx->getMemoryChunk(ctx->parserThread, Ctx::MEMORY::BUILDER));
            ctx->parserThread->contextSet(Thread::CONTEXT::TRAN, Thread::REASON::TRAN);
            nextBuffer->next = nullptr;
            nextBuffer->id = lastBuilderQueue->id + 1;
            nextBuffer->data = reinterpret_cast<uint8_t*>(nextBuffer) + sizeof(struct BuilderQueue);
            nextBuffer->size = 0;

            // Message could potentially fit in one buffer
            if (likely(copy && msg != nullptr && messageSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE)) {
                memcpy(reinterpret_cast<void*>(nextBuffer->data), msg, messagePosition);
                msg = reinterpret_cast<BuilderMsg*>(nextBuffer->data);
                msg->data = nextBuffer->data + sizeof(struct BuilderMsg);
                nextBuffer->start = 0;
            } else {
                lastBuilderQueue->size += messagePosition;
                messageSize += messagePosition;
                messagePosition = 0;
                nextBuffer->start = BUFFER_START_UNDEFINED;
            }

            {
                ctx->parserThread->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::BUILDER_ROTATE);
                std::unique_lock<std::mutex> lck(mtx);
                lastBuilderQueue->next = nextBuffer;
                ++buffersAllocated;
                lastBuilderQueue = nextBuffer;
            }
            ctx->parserThread->contextSet(Thread::CONTEXT::TRAN, Thread::REASON::TRAN);
        }

        void processValue(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeCol col, const uint8_t* data, uint32_t size, uint64_t offset,
                          bool after, bool compressed);

        inline void valuesRelease() {
            for (uint64_t i = 0; i < mergesMax; ++i)
                delete[] merges[i];
            mergesMax = 0;

            uint64_t baseMax = valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                auto column = static_cast<typeCol>(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (valuesSet[base] < mask)
                        break;
                    if ((valuesSet[base] & mask) == 0)
                        continue;

                    valuesSet[base] &= ~mask;
                    values[column][static_cast<uint>(VALUE_TYPE::BEFORE)] = nullptr;
                    values[column][static_cast<uint>(VALUE_TYPE::BEFORE_SUPP)] = nullptr;
                    values[column][static_cast<uint>(VALUE_TYPE::AFTER)] = nullptr;
                    values[column][static_cast<uint>(VALUE_TYPE::AFTER_SUPP)] = nullptr;
                }
            }
            valuesMax = 0;
            compressedBefore = false;
            compressedAfter = false;
        };

        inline void valueSet(VALUE_TYPE type, uint16_t column, const uint8_t* data, typeSize size, uint8_t fb, bool dump) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DML) || dump)) {
                std::ostringstream ss;
                ss << "DML: value: " << std::dec << static_cast<uint>(type) << "/" << column << "/" << std::dec << size << "/" << std::setfill('0') <<
                   std::setw(2) << std::hex << static_cast<uint64_t>(fb) << " to: ";
                for (uint64_t i = 0; i < size && i < 64; ++i) {
                    ss << "0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[i]) << ", ";
                }
                ctx->info(0, ss.str());
            }

            uint64_t base = static_cast<uint64_t>(column) >> 6;
            uint64_t mask = static_cast<uint64_t>(1) << (column & 0x3F);
            // New value
            if ((valuesSet[base] & mask) == 0)
                valuesSet[base] |= mask;
            if (column >= valuesMax)
                valuesMax = column + 1;

            switch (fb & (RedoLogRecord::FB_P | RedoLogRecord::FB_N)) {
                case 0:
                    sizes[column][static_cast<uint>(type)] = size;
                    values[column][static_cast<uint>(type)] = data;
                    break;

                case RedoLogRecord::FB_N:
                    sizesPart[0][column][static_cast<uint>(type)] = size;
                    valuesPart[0][column][static_cast<uint>(type)] = data;
                    if ((valuesMerge[base] & mask) == 0)
                        valuesMerge[base] |= mask;
                    break;

                case RedoLogRecord::FB_P | RedoLogRecord::FB_N:
                    sizesPart[1][column][static_cast<uint>(type)] = size;
                    valuesPart[1][column][static_cast<uint>(type)] = data;
                    if ((valuesMerge[base] & mask) == 0)
                        valuesMerge[base] |= mask;
                    break;

                case RedoLogRecord::FB_P:
                    sizesPart[2][column][static_cast<uint>(type)] = size;
                    valuesPart[2][column][static_cast<uint>(type)] = data;
                    if ((valuesMerge[base] & mask) == 0)
                        valuesMerge[base] |= mask;
                    break;
            }
        };

        inline void builderShift(bool copy) {
            ++messagePosition;

            if (unlikely(lastBuilderQueue->size + messagePosition >= OUTPUT_BUFFER_DATA_SIZE))
                builderRotate(copy);
        };

        inline void builderShiftFast(uint64_t bytes) {
            messagePosition += bytes;
        };

        inline void builderBegin(typeScn scn, typeSeq sequence, typeObj obj, BuilderMsg::OUTPUT_BUFFER flags) {
            messageSize = 0;
            messagePosition = 0;
            if (isScnTypeCommitValue())
                scn = commitScn;

            if (unlikely(lastBuilderQueue->size + messagePosition + sizeof(struct BuilderMsg) >= OUTPUT_BUFFER_DATA_SIZE))
                builderRotate(true);

            msg = reinterpret_cast<BuilderMsg*>(lastBuilderQueue->data + lastBuilderQueue->size);
            builderShiftFast(sizeof(struct BuilderMsg));
            msg->scn = scn;
            msg->lwnScn = lwnScn;
            msg->lwnIdx = lwnIdx++;
            msg->sequence = sequence;
            msg->size = 0;
            msg->tagSize = 0;
            msg->id = id++;
            msg->obj = obj;
            msg->flags = flags;
            msg->data = lastBuilderQueue->data + lastBuilderQueue->size + sizeof(struct BuilderMsg);
        };

        inline void builderCommit() {
            messageSize += messagePosition;
            if (unlikely(messageSize == sizeof(struct BuilderMsg)))
                throw RedoLogException(50058, "output buffer - commit of empty transaction");

            msg->queueId = lastBuilderQueue->id;
            builderShiftFast((8 - (messagePosition & 7)) & 7);
            unconfirmedSize += messageSize;
            msg->size = messageSize - sizeof(struct BuilderMsg);
            msg = nullptr;
            lastBuilderQueue->size += messagePosition;
            if (lastBuilderQueue->start == BUFFER_START_UNDEFINED)
                lastBuilderQueue->start = static_cast<uint64_t>(lastBuilderQueue->size);

            if (flushBuffer == 0 || unconfirmedSize > flushBuffer)
                flush();
        };

        inline void append(char character) {
            lastBuilderQueue->data[lastBuilderQueue->size + messagePosition] = character;
            builderShift(true);
        };

        inline void append(const char* str, uint64_t size) {
            if (unlikely(lastBuilderQueue->size + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                memcpy(reinterpret_cast<void*>(lastBuilderQueue->data + lastBuilderQueue->size + messagePosition),
                       reinterpret_cast<const void*>(str), size);
                messagePosition += size;
            } else {
                for (uint64_t i = 0; i < size; ++i)
                    append(*str++);
            }
        };

        inline void append(const std::string& str) {
            uint64_t size = str.length();
            if (unlikely(lastBuilderQueue->size + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                memcpy(reinterpret_cast<void*>(lastBuilderQueue->data + lastBuilderQueue->size + messagePosition),
                       reinterpret_cast<const void*>(str.c_str()), size);
                messagePosition += size;
            } else {
                const char* charStr = str.c_str();
                for (uint64_t i = 0; i < size; ++i)
                    append(*charStr++);
            }
        };

        inline void columnUnknown(const std::string& columnName, const uint8_t* data, uint32_t size) {
            valueBuffer[0] = '?';
            valueSize = 1;
            columnString(columnName);
            if (unlikely(unknownFormat == UNKNOWN_FORMAT::DUMP)) {
                std::ostringstream ss;
                for (uint32_t j = 0; j < size; ++j)
                    ss << " " << std::hex << std::setfill('0') << std::setw(2) << (static_cast<uint64_t>(data[j]));
                ctx->warning(60002, "unknown value (column: " + columnName + "): " + std::to_string(size) + " - " + ss.str());
            }
        };

        inline void valueBufferAppend(const char* text, uint32_t size) {
            for (uint32_t i = 0; i < size; ++i)
                valueBufferAppend(*text++);
        };

        inline void valueBufferAppend(uint8_t value) {
            valueBuffer[valueSize++] = static_cast<char>(value);
        };

        inline void valueBufferAppendHex(uint8_t value, uint64_t offset) {
            valueBufferCheck(2, offset);
            valueBuffer[valueSize++] = Ctx::map16((value >> 4) & 0x0F);
            valueBuffer[valueSize++] = Ctx::map16(value & 0x0F);
        };

        inline void parseNumber(const uint8_t* data, uint64_t size, uint64_t offset) {
            valueBufferPurge();
            valueBufferCheck(size * 2 + 2, offset);

            uint8_t digits = data[0];
            // Just zero
            if (digits == 0x80) {
                valueBufferAppend('0');
            } else {
                uint64_t j = 1;
                uint64_t jMax = size - 1;

                // Positive number
                if (digits > 0x80 && jMax >= 1) {
                    uint64_t value;
                    uint64_t zeros = 0;
                    // Part of the total
                    if (digits <= 0xC0) {
                        valueBufferAppend('0');
                        zeros = 0xC0 - digits;
                    } else {
                        digits -= 0xC0;
                        // Part of the total - omitting first zero for a first digit
                        value = data[j] - 1;
                        if (value < 10)
                            valueBufferAppend(Ctx::map10(value));
                        else {
                            valueBufferAppend(Ctx::map10(value / 10));
                            valueBufferAppend(Ctx::map10(value % 10));
                        }

                        ++j;
                        --digits;

                        while (digits > 0) {
                            value = data[j] - 1;
                            if (j <= jMax) {
                                valueBufferAppend(Ctx::map10(value / 10));
                                valueBufferAppend(Ctx::map10(value % 10));
                                ++j;
                            } else {
                                valueBufferAppend('0');
                                valueBufferAppend('0');
                            }
                            --digits;
                        }
                    }

                    // Fraction part
                    if (j <= jMax) {
                        valueBufferAppend('.');

                        while (zeros > 0) {
                            valueBufferAppend('0');
                            valueBufferAppend('0');
                            --zeros;
                        }

                        while (j <= jMax - 1U) {
                            value = data[j] - 1;
                            valueBufferAppend(Ctx::map10(value / 10));
                            valueBufferAppend(Ctx::map10(value % 10));
                            ++j;
                        }

                        // Last digit - omitting 0 at the end
                        value = data[j] - 1;
                        valueBufferAppend(Ctx::map10(value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend(Ctx::map10(value % 10));
                    }
                } else if (digits < 0x80 && jMax >= 1) {
                    // Negative number
                    uint64_t value;
                    uint64_t zeros = 0;
                    valueBufferAppend('-');

                    if (data[jMax] == 0x66)
                        --jMax;

                    // Part of the total
                    if (digits >= 0x3F) {
                        valueBufferAppend('0');
                        zeros = digits - 0x3F;
                    } else {
                        digits = 0x3F - digits;

                        value = 101 - data[j];
                        if (value < 10)
                            valueBufferAppend(Ctx::map10(value));
                        else {
                            valueBufferAppend(Ctx::map10(value / 10));
                            valueBufferAppend(Ctx::map10(value % 10));
                        }
                        ++j;
                        --digits;

                        while (digits > 0) {
                            if (j <= jMax) {
                                value = 101 - data[j];
                                valueBufferAppend(Ctx::map10(value / 10));
                                valueBufferAppend(Ctx::map10(value % 10));
                                ++j;
                            } else {
                                valueBufferAppend('0');
                                valueBufferAppend('0');
                            }
                            --digits;
                        }
                    }

                    if (j <= jMax) {
                        valueBufferAppend('.');

                        while (zeros > 0) {
                            valueBufferAppend('0');
                            valueBufferAppend('0');
                            --zeros;
                        }

                        while (j <= jMax - 1U) {
                            value = 101 - data[j];
                            valueBufferAppend(Ctx::map10(value / 10));
                            valueBufferAppend(Ctx::map10(value % 10));
                            ++j;
                        }

                        value = 101 - data[j];
                        valueBufferAppend(Ctx::map10(value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend(Ctx::map10(value % 10));
                    }
                } else
                    throw RedoLogException(50009, "error parsing numeric value at offset: " + std::to_string(offset));
            }
        };

        inline std::string dumpLob(const uint8_t* data, uint64_t size) const {
            std::ostringstream ss;
            for (uint64_t j = 0; j < size; ++j) {
                ss << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]);
            }
            return ss.str();
        }

        inline void addLobToOutput(const uint8_t* data, uint64_t size, uint64_t charsetId, uint64_t offset, bool appendData, bool isClob, bool hasPrev,
                                   bool hasNext, bool isSystem) {
            if (isClob) {
                parseString(data, size, charsetId, offset, appendData, hasPrev, hasNext, isSystem);
            } else {
                memcpy(reinterpret_cast<void*>(valueBuffer + valueSize),
                       reinterpret_cast<const void*>(data), size);
                valueSize += size;
            };
        }

        inline bool parseLob(LobCtx* lobCtx, const uint8_t* data, uint64_t size, uint64_t charsetId, typeObj obj, uint64_t offset, bool isClob, bool isSystem) {
            bool appendData = false, hasPrev = false, hasNext = true;
            valueSize = 0;
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA)))
                ctx->logTrace(Ctx::TRACE::LOB_DATA, dumpLob(data, size));

            if (unlikely(size < 20)) {
                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 1");
                return false;
            }

            uint32_t flags = data[5];
            typeLobId lobId(data + 10);
            lobCtx->checkOrphanedLobs(ctx, lobId, lastXid, offset);

            // In-index
            if ((flags & 0x04) == 0) {
                auto lobsIt = lobCtx->lobs.find(lobId);
                if (unlikely(lobsIt == lobCtx->lobs.end())) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA)))
                        ctx->logTrace(Ctx::TRACE::LOB_DATA, "LOB missing LOB index xid: " + lastXid.toString() + " LOB: " + lobId.lower() +
                                                            " data: " + dumpLob(data, size));
                    return true;
                }
                LobData* lobData = lobsIt->second;
                valueBufferCheck(static_cast<uint64_t>(lobData->pageSize) * static_cast<uint64_t>(lobData->sizePages) + lobData->sizeRest, offset);

                typeDba pageNo = 0;
                for (auto indexMapIt: lobData->indexMap) {
                    typeDba pageNoLob = indexMapIt.first;
                    typeDba page = indexMapIt.second;
                    if (unlikely(pageNo != pageNoLob)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 2");
                        pageNo = pageNoLob;
                    }

                    LobDataElement element(page, 0);
                    auto dataMapIt = lobData->dataMap.find(element);
                    if (unlikely(dataMapIt == lobData->dataMap.end())) {
                        if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB (in-index) for xid: " + lastXid.toString() + " LOB: " +
                                                                lobId.lower() + " page: " + std::to_string(page) + " obj: " + std::to_string(obj));
                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                        }
                        return false;
                    }
                    uint64_t chunkSize = lobData->pageSize;

                    // Last
                    if (pageNo == lobData->sizePages) {
                        chunkSize = lobData->sizeRest;
                        hasNext = false;
                    }

                    const RedoLogRecord* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));

                    valueBufferCheck(chunkSize * 4, offset);
                    addLobToOutput(redoLogRecordLob->data() + redoLogRecordLob->lobData, chunkSize, charsetId, offset, appendData, isClob,
                                   hasPrev, hasNext, isSystem);
                    appendData = true;
                    hasPrev = true;
                    ++pageNo;
                }

                if (hasNext)
                    addLobToOutput(nullptr, 0, charsetId, offset, appendData, isClob, true, false, isSystem);
            } else {
                // In-row
                if (unlikely(size < 23)) {
                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 3");
                    return false;
                }
                uint16_t bodySize = ctx->read16Big(data + 20);
                if (unlikely(size != static_cast<uint64_t>(bodySize + 20))) {
                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 4");
                    return false;
                }
                uint16_t flg2 = ctx->read16Big(data + 22);

                uint64_t totalLobSize = 0;
                uint64_t chunkSize;
                uint64_t dataOffset;

                // In-index
                if ((flg2 & 0x0400) == 0x0400) {
                    if (unlikely(size < 36)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 5");
                        return false;
                    }
                    uint32_t pageCnt = ctx->read32Big(data + 24);
                    uint16_t sizeRest = ctx->read16Big(data + 28);
                    dataOffset = 36;

                    auto lobsIt = lobCtx->lobs.find(lobId);
                    if (lobsIt == lobCtx->lobs.end()) {
                        if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB (in-index) for xid: " + lastXid.toString() + " obj: " +
                                                                std::to_string(obj));
                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                        }
                        return false;
                    }
                    LobData* lobData = lobsIt->second;
                    totalLobSize = pageCnt * lobData->pageSize + sizeRest;
                    if (totalLobSize == 0)
                        return true;

                    uint32_t jMax = pageCnt;
                    if (sizeRest > 0)
                        ++jMax;

                    for (uint32_t j = 0; j < jMax; ++j) {
                        typeDba page = 0;
                        if (dataOffset < size) {
                            if (unlikely(size < dataOffset + 4)) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                    ", location: 6");
                                return false;
                            }
                            page = ctx->read32Big(data + dataOffset);
                        } else {
                            // Rest of data in LOB index
                            auto indexMapIt = lobData->indexMap.find(j);
                            if (unlikely(indexMapIt == lobData->indexMap.end())) {
                                ctx->warning(60004, "can't find page " + std::to_string(j) + " for xid: " + lastXid.toString() + ", LOB: " +
                                                    lobId.lower() + ", obj: " + std::to_string(obj));
                                break;
                            }
                            page = indexMapIt->second;
                        }

                        LobDataElement element(page, 0);
                        auto dataMapIt = lobData->dataMap.find(element);
                        if (dataMapIt == lobData->dataMap.end()) {
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB index (in-index) for xid: " + lastXid.toString() + " LOB: " +
                                                                    lobId.lower() + " page: " + std::to_string(page) + " obj: " + std::to_string(obj));
                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                            }
                            return false;
                        }

                        while (dataMapIt != lobData->dataMap.end() && dataMapIt->first.dba == page) {
                            const RedoLogRecord* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                            if (j < pageCnt)
                                chunkSize = redoLogRecordLob->lobDataSize;
                            else
                                chunkSize = sizeRest;
                            if (j == jMax - 1U)
                                hasNext = false;

                            valueBufferCheck(chunkSize * 4, offset);
                            addLobToOutput(redoLogRecordLob->data() + redoLogRecordLob->lobData, chunkSize, charsetId, offset, appendData, isClob,
                                           hasPrev, hasNext, isSystem);
                            appendData = true;
                            hasPrev = true;
                            totalLobSize -= chunkSize;
                            ++dataMapIt;
                        }

                        ++page;
                        dataOffset += 4;
                    }
                } else if ((flg2 & 0x0100) == 0x0100) {
                    // In-value
                    if (unlikely(bodySize < 16)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 7");
                        return false;
                    }

                    if (unlikely(size < 34)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 8");
                        return false;
                    }
                    uint32_t zero1 = ctx->read32Big(data + 24);
                    chunkSize = ctx->read16Big(data + 28);
                    uint32_t zero2 = ctx->read32Big(data + 30);

                    if (unlikely(zero1 != 0 || zero2 != 0 || chunkSize + 16 != bodySize)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 9");
                        return false;
                    }

                    if (chunkSize == 0) {
                        // Null value
                    } else {
                        if (unlikely(size < static_cast<uint64_t>(chunkSize) + 36)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 10");
                            return false;
                        }

                        valueBufferCheck(chunkSize * 4, offset);
                        addLobToOutput(data + 36, chunkSize, charsetId, offset, false, isClob, false, false, isSystem);
                    }
                } else {
                    if (unlikely(bodySize < 10)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                            ", location: 11");
                        return false;
                    }
                    uint8_t flg3 = data[26];
                    uint8_t flg4 = data[27];
                    if ((flg3 & 0x03) == 0) {
                        if (unlikely(size < 30)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 12");
                            return false;
                        }

                        totalLobSize = data[28];
                        dataOffset = 29;
                    } else if ((flg3 & 0x03) == 1) {
                        if (unlikely(size < 30)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 13");
                            return false;
                        }

                        totalLobSize = ctx->read16Big(data + 28);
                        dataOffset = 30;
                    } else if ((flg3 & 0x03) == 2) {
                        if (unlikely(size < 32)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 14");
                            return false;
                        }

                        totalLobSize = ctx->read24Big(data + 28);
                        dataOffset = 31;
                    } else if ((flg3 & 0x03) == 3) {
                        if (unlikely(size < 32)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 15");
                            return false;
                        }

                        totalLobSize = ctx->read32Big(data + 28);
                        dataOffset = 32;
                    } else {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                            ", location: 16");
                        return false;
                    }

                    if ((flg4 & 0x0F) == 0) {
                        ++dataOffset;
                    } else if ((flg4 & 0x0F) == 0x01) {
                        dataOffset += 2;
                    } else {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                            ", location: 17");
                        return false;
                    }

                    // Null value
                    if (totalLobSize == 0)
                        return true;

                    // Data
                    if ((flg2 & 0x0800) == 0x0800) {
                        chunkSize = totalLobSize;

                        if (unlikely(dataOffset + chunkSize < size)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 18");
                            return false;
                        }

                        valueBufferCheck(chunkSize * 4, offset);
                        addLobToOutput(data + dataOffset, chunkSize, charsetId, offset, false, isClob, false, false,
                                       isSystem);
                        totalLobSize -= chunkSize;

                    } else if ((flg2 & 0x4000) == 0x4000) {
                        // 12+ data
                        auto lobsIt = lobCtx->lobs.find(lobId);
                        if (lobsIt == lobCtx->lobs.end()) {
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB index (12+ in-value) for xid: " + lastXid.toString() + " LOB: " +
                                                                    lobId.lower() + " obj: " + std::to_string(obj));
                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                            }
                            return false;
                        }
                        LobData* lobData = lobsIt->second;

                        // Style 1
                        if ((flg3 & 0xF0) == 0x20) {
                            uint8_t lobPages = data[dataOffset++] + 1;

                            for (uint64_t i = 0; i < lobPages; ++i) {
                                if (unlikely(dataOffset + 1U >= size)) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                        ", location: 19");
                                    return false;
                                }
                                uint8_t flg5 = data[dataOffset++];

                                typeDba page = ctx->read32Big(data + dataOffset);
                                dataOffset += 4;
                                uint16_t pageCnt = 0;
                                if ((flg5 & 0x20) == 0) {
                                    pageCnt = data[dataOffset++];
                                } else if ((flg5 & 0x20) == 0x20) {
                                    pageCnt = ctx->read16Big(data + dataOffset);
                                    dataOffset += 2;
                                }

                                for (uint16_t j = 0; j < pageCnt; ++j) {
                                    LobDataElement element(page, 0);
                                    auto dataMapIt = lobData->dataMap.find(element);
                                    if (unlikely(dataMapIt == lobData->dataMap.end())) {
                                        if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB data (new in-value) for xid: " + lastXid.toString() +
                                                                                " LOB: " + lobId.lower() + " page: " + std::to_string(page) + " obj: " +
                                                                                std::to_string(obj));
                                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                                        }
                                        return false;
                                    }

                                    while (dataMapIt != lobData->dataMap.end() && dataMapIt->first.dba == page) {
                                        const RedoLogRecord* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                        chunkSize = redoLogRecordLob->lobDataSize;
                                        if (i == lobPages - 1U && j == pageCnt - 1U)
                                            hasNext = false;

                                        valueBufferCheck(chunkSize * 4, offset);
                                        addLobToOutput(redoLogRecordLob->data() + redoLogRecordLob->lobData, chunkSize, charsetId, offset,
                                                       appendData, isClob, hasPrev, hasNext, isSystem);
                                        appendData = true;
                                        hasPrev = true;
                                        totalLobSize -= chunkSize;
                                        ++dataMapIt;
                                    }
                                    ++page;
                                }
                            }

                        } else if ((flg3 & 0xF0) == 0x40) {
                            // Style 2
                            if (unlikely(dataOffset + 4 != size)) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                    ", location: 20");
                                return false;
                            }
                            typeDba listPage = ctx->read32Big(data + dataOffset);

                            while (listPage != 0) {
                                auto listMapIt = lobCtx->listMap.find(listPage);
                                if (unlikely(listMapIt == lobCtx->listMap.end())) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                        ", location: 21, page: " + std::to_string(listPage) + ", offset: " + std::to_string(dataOffset));
                                    return false;
                                }

                                const uint8_t* dataLob = listMapIt->second;
                                listPage = *reinterpret_cast<const typeDba*>(dataLob);
                                uint32_t aSiz = ctx->read32(dataLob + 4);

                                for (uint64_t i = 0; i < aSiz; ++i) {
                                    uint16_t pageCnt = ctx->read16(dataLob + i * 8 + 8 + 2);
                                    typeDba page = ctx->read32(dataLob + i * 8 + 8 + 4);

                                    for (uint16_t j = 0; j < pageCnt; ++j) {
                                        LobDataElement element(page, 0);
                                        auto dataMapIt = lobData->dataMap.find(element);
                                        if (unlikely(dataMapIt == lobData->dataMap.end())) {
                                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB data (new in-value 12+) for xid: " + lastXid.toString() +
                                                                                    " LOB: " + lobId.lower() + " page: " + std::to_string(page) + " obj: " +
                                                                                    std::to_string(obj));
                                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " +
                                                                                    dumpLob(dataLob, size));
                                            }
                                            return false;
                                        }

                                        const RedoLogRecord* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                        chunkSize = redoLogRecordLob->lobDataSize;
                                        if (listPage == 0 && i == aSiz - 1U && j == pageCnt - 1U)
                                            hasNext = false;

                                        valueBufferCheck(chunkSize * 4, offset);
                                        addLobToOutput(redoLogRecordLob->data() + redoLogRecordLob->lobData, chunkSize, charsetId, offset,
                                                       appendData, isClob, hasPrev, hasNext, isSystem);
                                        appendData = true;
                                        hasPrev = true;
                                        totalLobSize -= chunkSize;
                                        ++page;
                                    }
                                }
                            }
                        } else {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 22");
                            return false;
                        }

                    } else {
                        // Index
                        if (unlikely(dataOffset + 1U >= size)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                ", location: 23");
                            return false;
                        }

                        uint8_t lobPages = data[dataOffset++] + 1;

                        auto lobsIt = lobCtx->lobs.find(lobId);
                        if (unlikely(lobsIt == lobCtx->lobs.end())) {
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB index (new in-value) for xid: " + lastXid.toString() + " LOB: " +
                                                                    lobId.lower() + " obj: " + std::to_string(obj));
                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                            }
                            return false;
                        }
                        LobData* lobData = lobsIt->second;

                        for (uint64_t i = 0; i < lobPages; ++i) {
                            if (unlikely(dataOffset + 5 >= size)) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                    ", location: 24");
                                return false;
                            }

                            uint8_t flg5 = data[dataOffset++];
                            typeDba page = ctx->read32Big(data + dataOffset);
                            dataOffset += 4;

                            uint64_t pageCnt = 0;
                            if ((flg5 & 0xF0) == 0x00) {
                                pageCnt = data[dataOffset++];
                            } else if ((flg5 & 0xF0) == 0x20) {
                                if (unlikely(dataOffset + 1U >= size)) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                        ", location: 26");
                                    return false;
                                }
                                pageCnt = ctx->read16Big(data + dataOffset);
                                dataOffset += 2;
                            } else {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                    ", location: 27");
                                return false;
                            }

                            for (uint64_t j = 0; j < pageCnt; ++j) {
                                LobDataElement element(page, 0);
                                auto dataMapIt = lobData->dataMap.find(element);
                                if (unlikely(dataMapIt == lobData->dataMap.end())) {
                                    ctx->warning(60005, "missing LOB data (new in-value) for xid: " + lastXid.toString() + ", LOB: " +
                                                        lobId.lower() + ", page: " + std::to_string(page) + ", obj: " + std::to_string(obj));
                                    ctx->warning(60006, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                                    return false;
                                }

                                const RedoLogRecord* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                chunkSize = redoLogRecordLob->lobDataSize;
                                if (i == lobPages - 1U && j == pageCnt - 1U)
                                    hasNext = false;

                                valueBufferCheck(chunkSize * 4, offset);
                                addLobToOutput(redoLogRecordLob->data() + redoLogRecordLob->lobData, chunkSize, charsetId, offset, appendData,
                                               isClob, hasPrev, hasNext, isSystem);
                                appendData = true;
                                hasPrev = true;
                                ++page;
                                totalLobSize -= chunkSize;
                            }
                        }
                    }
                }

                if (unlikely(totalLobSize != 0)) {
                    ctx->warning(60007, "incorrect LOB sum xid: " + lastXid.toString() + " left: " + std::to_string(totalLobSize) +
                                        " obj: " + std::to_string(obj));
                    ctx->warning(60006, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                    return false;
                }
            }

            return true;
        }

        inline void parseRaw(const uint8_t* data, uint64_t size, uint64_t offset) {
            valueBufferPurge();
            valueBufferCheck(size * 2, offset);

            if (size == 0)
                return;

            for (uint64_t j = 0; j < size; ++j) {
                valueBufferAppend(Ctx::map16U(data[j] >> 4));
                valueBufferAppend(Ctx::map16U(data[j] & 0x0F));
            }
        };

        inline void parseString(const uint8_t* data, uint64_t size, uint64_t charsetId, uint64_t offset, bool appendData, bool hasPrev, bool hasNext,
                                bool isSystem) {
            const CharacterSet* characterSet = locales->characterMap[charsetId];
            if (unlikely(characterSet == nullptr && !isCharFormatNoMapping()))
                throw RedoLogException(50010, "can't find character set map for id = " + std::to_string(charsetId) + " at offset: " +
                                              std::to_string(offset));
            if (!appendData)
                valueBufferPurge();
            if (size == 0 && !(hasPrev && prevCharsSize > 0))
                return;

            const uint8_t* parseData = data;
            uint64_t parseSize = size;
            uint64_t overlap = 0;

            // Something left to parse from previous run
            if (hasPrev && prevCharsSize > 0) {
                overlap = 2 * CharacterSet::MAX_CHARACTER_LENGTH - prevCharsSize;
                if (overlap > size)
                    overlap = size;
                memcpy(prevChars + prevCharsSize, data, overlap);
                parseData = prevChars;
                parseSize = prevCharsSize + overlap;
            }

            while (parseSize > 0) {
                // Leave for next time
                if (hasNext && parseSize < CharacterSet::MAX_CHARACTER_LENGTH && overlap == 0) {
                    memcpy(prevChars, parseData, parseSize);
                    prevCharsSize = parseSize;
                    break;
                }

                // Switch to data buffer
                if (parseSize <= overlap && size > overlap && overlap > 0) {
                    uint64_t processed = overlap - parseSize;
                    parseData = data + processed;
                    parseSize = size - processed;
                    overlap = 0;
                }

                typeUnicode unicodeCharacter;

                if (!isCharFormatNoMapping()) {
                    unicodeCharacter = characterSet->decode(ctx, lastXid, parseData, parseSize);

                    if (!isCharFormatHex() || isSystem) {
                        if (unicodeCharacter <= 0x7F) {
                            // 0xxxxxxx
                            valueBufferAppend(unicodeCharacter);

                        } else if (unicodeCharacter <= 0x7FF) {
                            // 110xxxxx 10xxxxxx
                            valueBufferAppend(0xC0 | static_cast<uint8_t>(unicodeCharacter >> 6));
                            valueBufferAppend(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F));

                        } else if (unicodeCharacter <= 0xFFFF) {
                            // 1110xxxx 10xxxxxx 10xxxxxx
                            valueBufferAppend(0xE0 | static_cast<uint8_t>(unicodeCharacter >> 12));
                            valueBufferAppend(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F));
                            valueBufferAppend(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F));

                        } else if (unicodeCharacter <= 0x10FFFF) {
                            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                            valueBufferAppend(0xF0 | static_cast<uint8_t>(unicodeCharacter >> 18));
                            valueBufferAppend(0x80 | static_cast<uint8_t>((unicodeCharacter >> 12) & 0x3F));
                            valueBufferAppend(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F));
                            valueBufferAppend(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F));

                        } else
                            throw RedoLogException(50011, "got character code: U+" + std::to_string(unicodeCharacter) + " at offset: " +
                                                          std::to_string(offset));
                    } else {
                        if (unicodeCharacter <= 0x7F) {
                            // 0xxxxxxx
                            valueBufferAppendHex(unicodeCharacter, offset);

                        } else if (unicodeCharacter <= 0x7FF) {
                            // 110xxxxx 10xxxxxx
                            valueBufferAppendHex(0xC0 | static_cast<uint8_t>(unicodeCharacter >> 6), offset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F), offset);

                        } else if (unicodeCharacter <= 0xFFFF) {
                            // 1110xxxx 10xxxxxx 10xxxxxx
                            valueBufferAppendHex(0xE0 | static_cast<uint8_t>(unicodeCharacter >> 12), offset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F), offset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F), offset);

                        } else if (unicodeCharacter <= 0x10FFFF) {
                            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                            valueBufferAppendHex(0xF0 | static_cast<uint8_t>(unicodeCharacter >> 18), offset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>((unicodeCharacter >> 12) & 0x3F), offset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F), offset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F), offset);

                        } else
                            throw RedoLogException(50011, "got character code: U+" + std::to_string(unicodeCharacter) + " at offset: " +
                                                          std::to_string(offset));
                    }
                } else {
                    unicodeCharacter = *parseData++;
                    --parseSize;

                    if (!isCharFormatHex() || isSystem) {
                        valueBufferAppend(unicodeCharacter);
                    } else {
                        valueBufferAppendHex(unicodeCharacter, offset);
                    }
                }
            }
        };

        inline void valueBufferCheck(uint64_t size, uint64_t offset) {
            if (unlikely(valueSize + size > VALUE_BUFFER_MAX))
                throw RedoLogException(50012, "trying to allocate length for value: " + std::to_string(valueSize + size) +
                                              " exceeds maximum: " + std::to_string(VALUE_BUFFER_MAX) + " at offset: " + std::to_string(offset));

            if (valueSize + size < valueBufferSize)
                return;

            do {
                valueBufferSize <<= 1;
            } while (valueSize + size >= valueBufferSize);

            char* newValueBuffer = new char[valueBufferSize];
            memcpy(reinterpret_cast<void*>(newValueBuffer),
                   reinterpret_cast<const void*>(valueBuffer), valueSize);
            delete[] valueBuffer;
            valueBuffer = newValueBuffer;
        };

        inline void valueBufferPurge() {
            valueSize = 0;
            if (valueBufferSize == VALUE_BUFFER_MIN)
                return;

            delete[] valueBuffer;
            valueBuffer = new char[VALUE_BUFFER_MIN];
            valueBufferSize = VALUE_BUFFER_MIN;
        };

        virtual void columnFloat(const std::string& columnName, double value) = 0;
        virtual void columnDouble(const std::string& columnName, long double value) = 0;
        virtual void columnString(const std::string& columnName) = 0;
        virtual void columnNumber(const std::string& columnName, int precision, int scale) = 0;
        virtual void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t size) = 0;
        virtual void columnRowId(const std::string& columnName, typeRowId rowId) = 0;
        virtual void columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) = 0;
        virtual void columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const char* tz) = 0;
        virtual void processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) = 0;
        virtual void processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) = 0;
        virtual void processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) = 0;
        virtual void processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const DbTable* table, typeObj obj, typeDataObj dataObj, uint16_t type,
                                uint16_t seq, const char* sql, uint64_t sqlSize) = 0;
        virtual void processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) = 0;
        bool parseXml(const XmlCtx* xmlCtx, const uint8_t* data, uint64_t size, uint64_t offset);

    public:
        SystemTransaction* systemTransaction;
        uint64_t buffersAllocated;
        BuilderQueue* firstBuilderQueue;
        BuilderQueue* lastBuilderQueue;
        typeScn lwnScn;
        typeIdx lwnIdx;

        Builder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, DB_FORMAT newDbFormat, ATTRIBUTES_FORMAT newAttributesFormat,
                INTERVAL_DTS_FORMAT newIntervalDtsFormat, INTERVAL_YTM_FORMAT newIntervalYtmFormat, MESSAGE_FORMAT newMessageFormat, RID_FORMAT newRidFormat,
                XID_FORMAT newXidFormat, TIMESTAMP_FORMAT newTimestampFormat, TIMESTAMP_TZ_FORMAT newTimestampTzFormat, TIMESTAMP_ALL newTimestampAll,
                CHAR_FORMAT newCharFormat, SCN_FORMAT newScnFormat, SCN_TYPE newScnType, UNKNOWN_FORMAT newUnknownFormat, SCHEMA_FORMAT newSchemaFormat,
                COLUMN_FORMAT newColumnFormat, UNKNOWN_TYPE newUnknownType, uint64_t newFlushBuffer);
        virtual ~Builder();

        [[nodiscard]] uint64_t builderSize() const;
        [[nodiscard]] uint64_t getMaxMessageMb() const;
        void setMaxMessageMb(uint64_t maxMessageMb);
        void processBegin(typeXid xid, typeScn scn, typeScn newLwnScn, const std::unordered_map<std::string, std::string>* newAttributes);
        void processInsertMultiple(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                   const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDeleteMultiple(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                   const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDml(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const std::deque<const RedoLogRecord*>& redo1,
                        const std::deque<const RedoLogRecord*>& redo2, TRANSACTION_TYPE transactionType, bool system, bool schema, bool dump);
        void processDdlHeader(typeScn scn, typeSeq sequence, time_t timestamp, const RedoLogRecord* redoLogRecord1);
        virtual void initialize();
        virtual void processCommit(typeScn scn, typeSeq sequence, time_t timestamp) = 0;
        virtual void processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp, uint64_t offset, bool redo) = 0;
        void releaseBuffers(Thread* t, uint64_t maxId);
        void sleepForWriterWork(Thread* t, uint64_t queueSize, uint64_t nanoseconds);
        void wakeUp();

        inline void flush() {
            {
                ctx->parserThread->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::BUILDER_COMMIT);
                std::unique_lock<std::mutex> lck(mtx);
                condNoWriterWork.notify_all();
            }
            ctx->parserThread->contextSet(Thread::CONTEXT::TRAN, Thread::REASON::TRAN);
            unconfirmedSize = 0;
        };


        friend class SystemTransaction;
    };
}

#endif
