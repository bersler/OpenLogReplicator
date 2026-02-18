/* Header for Builder class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef BUILDER_H_
#define BUILDER_H_

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "../common/Attribute.h"
#include "../common/Ctx.h"
#include "../common/Format.h"
#include "../common/LobCtx.h"
#include "../common/LobData.h"
#include "../common/LobKey.h"
#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/exception/RedoLogException.h"
#include "../common/table/SysUser.h"
#include "../common/types/Data.h"
#include "../common/types/FileOffset.h"
#include "../common/types/LobId.h"
#include "../common/types/RowId.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Types.h"
#include "../common/types/Xid.h"
#include "../locales/CharacterSet.h"
#include "../locales/Locales.h"

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
        std::atomic<uint64_t> confirmedSize;
        std::atomic<uint64_t> start;
        uint8_t* data;
        std::atomic<BuilderQueue*> next;
    };

    struct BuilderMsg {
        enum class OUTPUT_BUFFER : unsigned char {
            NONE       = 0,
            ALLOCATED  = 1 << 0,
            CONFIRMED  = 1 << 1,
            CHECKPOINT = 1 << 2,
            REDO       = 1 << 3
        };

        void* ptr;
        uint64_t id;
        uint64_t queueId;
        std::atomic<uint64_t> size;
        Scn scn;
        Scn lwnScn;
        typeIdx lwnIdx;
        uint8_t* data;
        Seq sequence;
        typeObj obj;
        typeTag tagSize;
        OUTPUT_BUFFER flags;

        bool isFlagSet(OUTPUT_BUFFER flag) const {
            return (static_cast<uint>(flags) & static_cast<uint>(flag)) != 0;
        }

        void setFlag(OUTPUT_BUFFER flag) {
            flags = static_cast<OUTPUT_BUFFER>(static_cast<uint>(flags) | static_cast<uint>(flag));
        }

        void unsetFlag(OUTPUT_BUFFER flag) {
            flags = static_cast<OUTPUT_BUFFER>(static_cast<uint>(flags) & ~static_cast<uint>(flag));
        }
    };

    class Builder {
    public:
        static constexpr uint64_t OUTPUT_BUFFER_DATA_SIZE = Ctx::MEMORY_CHUNK_SIZE - sizeof(BuilderQueue);

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
        BuilderMsg* msg{nullptr};

        Format format;
        uint64_t unconfirmedSize{0};
        uint64_t messageSize{0};
        uint64_t messagePosition{0};
        uint64_t flushBuffer;
        char* valueBuffer{nullptr};
        uint64_t valueSize{0};
        uint64_t valueBufferSize{0};
        char* valueBufferOld{nullptr};
        uint64_t valueSizeOld{0};
        std::unordered_set<const DbTable*> tables;
        uint64_t lastBuilderSize{0};
        Scn beginScn{Scn::none()};
        Scn commitScn{Scn::none()};
        Time beginTimestamp{0};
        Time commitTimestamp{0};
        Seq beginSequence{Seq::zero()};
        Seq commitSequence{Seq::zero()};
        Xid lastXid;
        typeMask valuesSet[Ctx::COLUMN_LIMIT_23_0 / sizeof(uint64_t)]{};
        typeMask valuesMerge[Ctx::COLUMN_LIMIT_23_0 / sizeof(uint64_t)]{};
        int64_t sizes[Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(Format::VALUE_TYPE::LENGTH)]{};
        const uint8_t* values[Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(Format::VALUE_TYPE::LENGTH)]{};
        uint64_t sizesPart[3][Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(Format::VALUE_TYPE::LENGTH)]{};
        const uint8_t* valuesPart[3][Ctx::COLUMN_LIMIT_23_0][static_cast<uint>(Format::VALUE_TYPE::LENGTH)]{};
        typeCol valuesMax{0};
        uint8_t* merges[Ctx::COLUMN_LIMIT_23_0 * static_cast<int>(Format::VALUE_TYPE::LENGTH)]{};
        typeCol mergesMax{0};
        uint8_t* ddlFirst{nullptr};
        uint8_t* ddlLast{nullptr};
        uint64_t ddlSize{0};
        uint64_t id{0};
        uint64_t num{0};
        uint64_t maxMessageMb{0}; // Maximum message size able to handle by writer
        bool newTran{false};
        bool compressedBefore{false};
        bool compressedAfter{false};
        uint8_t prevChars[CharacterSet::MAX_CHARACTER_LENGTH * 2]{};
        uint64_t prevCharsSize{0};
        const AttributeMap* attributes{};
        uint16_t thread{0};

        std::mutex mtx;
        std::condition_variable condNoWriterWork;
        char ddlSchemaName[SysUser::NAME_LENGTH]{};
        typeSize ddlSchemaSize{0};

        static double decodeFloat(const uint8_t* data);
        static long double decodeDouble(const uint8_t* data);

        template<bool copy>
        void builderRotate() {
            if (messageSize > ctx->memoryChunksWriteBufferMax * Ctx::MEMORY_CHUNK_SIZE_MB * 1024 * 1024)
                throw RedoLogException(10072, "writer buffer (parameter \"write-buffer-max-mb\" = " +
                                       std::to_string(ctx->memoryChunksWriteBufferMax * Ctx::MEMORY_CHUNK_SIZE_MB) +
                                       ") is too small to fit a message with size: " +
                                       std::to_string(messageSize));
            auto* nextBuffer = reinterpret_cast<BuilderQueue*>(ctx->getMemoryChunk(ctx->parserThread, Ctx::MEMORY::BUILDER));
            ctx->parserThread->contextSet(Thread::CONTEXT::TRAN, Thread::REASON::TRAN);
            nextBuffer->next = nullptr;
            nextBuffer->id = lastBuilderQueue->id + 1;
            nextBuffer->data = reinterpret_cast<uint8_t*>(nextBuffer) + sizeof(BuilderQueue);
            nextBuffer->confirmedSize = 0;
            lastBuilderSize = 0;

            // Message could potentially fit in one buffer
            if (likely(copy && msg != nullptr && messageSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE)) {
                memcpy(nextBuffer->data, msg, messagePosition);
                msg = reinterpret_cast<BuilderMsg*>(nextBuffer->data);
                msg->data = nextBuffer->data + sizeof(BuilderMsg);
                nextBuffer->start = 0;
            } else {
                lastBuilderQueue->confirmedSize += messagePosition;
                messageSize += messagePosition;
                messagePosition = 0;
                nextBuffer->start = BUFFER_START_UNDEFINED;
            }

            {
                ctx->parserThread->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::BUILDER_ROTATE);
                std::unique_lock const lck(mtx);
                lastBuilderQueue->next = nextBuffer;
                ++buffersAllocated;
                lastBuilderQueue = nextBuffer;
            }
            ctx->parserThread->contextSet(Thread::CONTEXT::TRAN, Thread::REASON::TRAN);
        }

        void processValue(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeCol col, const uint8_t* data, uint32_t size, FileOffset fileOffset,
                          bool after, bool compressed);

        void releaseValues() {
            for (typeCol i = 0; i < mergesMax; ++i)
                delete[] merges[i];
            mergesMax = 0;

            const typeCol baseMax = valuesMax >> 6;
            for (typeCol base = 0; base <= baseMax; ++base) {
                const auto columnBase = static_cast<typeCol>(base << 6);
                while (valuesSet[base] != 0) {
                    const typeCol pos = ffsll(valuesSet[base]) - 1;
                    valuesSet[base] &= ~(1ULL << pos);
                    const typeCol column = columnBase + pos;

                    values[column][+Format::VALUE_TYPE::BEFORE] = nullptr;
                    values[column][+Format::VALUE_TYPE::BEFORE_SUPP] = nullptr;
                    values[column][+Format::VALUE_TYPE::AFTER] = nullptr;
                    values[column][+Format::VALUE_TYPE::AFTER_SUPP] = nullptr;
                }
            }
            valuesMax = 0;
            compressedBefore = false;
            compressedAfter = false;
        }

        void valueSet(Format::VALUE_TYPE type, uint16_t column, const uint8_t* data, typeSize size, uint8_t fb, bool dump) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DML) || dump)) {
                std::ostringstream ss;
                ss << "DML: value: " << std::dec << static_cast<uint>(type) << "/" << column << "/" << std::dec << size << "/" << std::setfill('0') <<
                        std::setw(2) << std::hex << static_cast<uint>(fb) << " to: ";
                for (typeSize i = 0; i < size && i < 64; ++i) {
                    ss << "0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(data[i]) << ", ";
                }
                ctx->info(0, ss.str());
            }

            const typeMask base = static_cast<uint64_t>(column) >> 6;
            const typeMask mask = static_cast<uint64_t>(1) << (column & 0x3F);
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
        }

        template<bool copy>
        void builderShift() {
            ++messagePosition;

            if (unlikely(lastBuilderSize + messagePosition >= OUTPUT_BUFFER_DATA_SIZE))
                builderRotate<copy>();
            ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
        }

        void builderShiftFast(uint64_t bytes) {
            messagePosition += bytes;
        }

        void builderBegin(Seq sequence, Scn scn, typeObj obj, BuilderMsg::OUTPUT_BUFFER flags) {
            messageSize = 0;
            messagePosition = 0;
            if (format.isScnTypeCommitValue())
                scn = commitScn;

            if (unlikely(lastBuilderSize + messagePosition + sizeof(BuilderMsg) >= OUTPUT_BUFFER_DATA_SIZE))
                builderRotate<true>();

            msg = reinterpret_cast<BuilderMsg*>(lastBuilderQueue->data + lastBuilderSize);
            builderShiftFast(sizeof(BuilderMsg));
            ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            msg->scn = scn;
            msg->lwnScn = lwnScn;
            msg->lwnIdx = lwnIdx++;
            msg->sequence = sequence;
            msg->size = 0;
            msg->tagSize = 0;
            msg->id = id++;
            msg->obj = obj;
            msg->flags = flags;
            msg->data = lastBuilderQueue->data + lastBuilderSize + sizeof(BuilderMsg);
        }

        void builderCommit() {
            messageSize += messagePosition;
            if (unlikely(messageSize == sizeof(BuilderMsg)))
                throw RedoLogException(50058, "output buffer - commit of empty transaction");

            msg->queueId = lastBuilderQueue->id;
            builderShiftFast((8 - (messagePosition & 7)) & 7);
            unconfirmedSize += messageSize;
            msg->size = messageSize - sizeof(BuilderMsg);
            msg = nullptr;
            lastBuilderQueue->confirmedSize += messagePosition;
            lastBuilderSize += messagePosition;
            if (unlikely(lastBuilderQueue->start == BUFFER_START_UNDEFINED))
                lastBuilderQueue->start = static_cast<uint64_t>(lastBuilderQueue->confirmedSize);

            if (flushBuffer == 0 || unconfirmedSize > flushBuffer)
                flush();
        }

        template<bool fast = false>
        void append(char character) {
            lastBuilderQueue->data[lastBuilderSize + messagePosition] = character;
            if constexpr (fast) {
                ++messagePosition;
                ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            } else {
                builderShift<true>();
            }
        }

        template<bool fast = false>
        void appendArr(const char* str, uint64_t size) {
            if (fast || likely(lastBuilderSize + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                memcpy(lastBuilderQueue->data + lastBuilderSize + messagePosition, str, size);
                messagePosition += size;
                ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            } else {
                for (uint64_t i = 0; i < size; ++i)
                    append(*str++);
            }
        }

        template<bool fast = false>
        void append(const std::string_view& str) {
            appendArr<fast>(str.data(), str.size());
        }

        template<bool fast = false>
        void append(const std::string& str) {
            const size_t size = str.length();
            if (unlikely(lastBuilderSize + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                memcpy(lastBuilderQueue->data + lastBuilderSize + messagePosition, str.c_str(), size);
                messagePosition += size;
                ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            } else {
                const char* charStr = str.c_str();
                for (size_t i = 0; i < size; ++i)
                    append<fast>(*charStr++);
            }
        }

        void columnUnknown(const std::string& columnName, const uint8_t* data, uint32_t size) {
            valueBuffer[0] = '?';
            valueSize = 1;
            columnString(columnName);
            if (unlikely(format.unknownFormat == Format::UNKNOWN_FORMAT::DUMP)) {
                std::ostringstream ss;
                for (uint32_t j = 0; j < size; ++j)
                    ss << " " << std::hex << std::setfill('0') << std::setw(2) << (static_cast<uint64_t>(data[j]));
                ctx->warning(60002, "unknown value (column: " + columnName + "): " + std::to_string(size) + " - " + ss.str());
            }
        }

        void valueBufferAppend(const char* text, uint32_t size) {
            for (uint32_t i = 0; i < size; ++i)
                valueBufferAppend(*text++);
        }

        void valueBufferAppend(uint8_t value) {
            valueBuffer[valueSize++] = static_cast<char>(value);
        }

        void valueBufferAppendHex(uint8_t value, FileOffset fileOffset) {
            valueBufferCheck(2, fileOffset);
            valueBuffer[valueSize++] = Data::map16((value >> 4) & 0x0F);
            valueBuffer[valueSize++] = Data::map16(value & 0x0F);
        }

        void parseNumber(const uint8_t* data, uint64_t size, FileOffset fileOffset) {
            valueBufferPurge();
            valueBufferCheck((size * 2) + 2, fileOffset);

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
                            valueBufferAppend(Data::map10(value));
                        else {
                            valueBufferAppend(Data::map10(value / 10));
                            valueBufferAppend(Data::map10(value % 10));
                        }

                        ++j;
                        --digits;

                        while (digits > 0) {
                            value = data[j] - 1;
                            if (j <= jMax) {
                                valueBufferAppend(Data::map10(value / 10));
                                valueBufferAppend(Data::map10(value % 10));
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
                            valueBufferAppend(Data::map10(value / 10));
                            valueBufferAppend(Data::map10(value % 10));
                            ++j;
                        }

                        // Last digit - omitting 0 at the end
                        value = data[j] - 1;
                        valueBufferAppend(Data::map10(value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend(Data::map10(value % 10));
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
                            valueBufferAppend(Data::map10(value));
                        else {
                            valueBufferAppend(Data::map10(value / 10));
                            valueBufferAppend(Data::map10(value % 10));
                        }
                        ++j;
                        --digits;

                        while (digits > 0) {
                            if (j <= jMax) {
                                value = 101 - data[j];
                                valueBufferAppend(Data::map10(value / 10));
                                valueBufferAppend(Data::map10(value % 10));
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
                            valueBufferAppend(Data::map10(value / 10));
                            valueBufferAppend(Data::map10(value % 10));
                            ++j;
                        }

                        value = 101 - data[j];
                        valueBufferAppend(Data::map10(value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend(Data::map10(value % 10));
                    }
                } else {
                    if (digits == 0) {
                        valueBufferAppend('0');
                    } else {
                        if (unlikely(format.unknownFormat == Format::UNKNOWN_FORMAT::DUMP)) {
                            std::ostringstream ss;
                            for (uint32_t k = 0; k < size; ++k)
                                ss << " " << std::hex << std::setfill('0') << std::setw(2) << (static_cast<uint64_t>(data[k]));
                            ctx->warning(60002, "unknown value: " + std::to_string(size) + " - " + ss.str());
                        }
                        throw RedoLogException(50009, "error parsing numeric value at offset: " + fileOffset.toString());
                    }
                }
            }
        }

        static std::string dumpLob(const uint8_t* data, uint64_t size) {
            std::ostringstream ss;
            for (uint64_t j = 0; j < size; ++j) {
                ss << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(data[j]);
            }
            return ss.str();
        }

        void addLobToOutput(const uint8_t* data, uint64_t size, uint64_t charsetId, FileOffset fileOffset, bool appendData, bool isClob, bool hasPrev,
                            bool hasNext, bool isSystem) {
            if (isClob) {
                parseString(data, size, charsetId, fileOffset, appendData, hasPrev, hasNext, isSystem);
            } else {
                memcpy(valueBuffer + valueSize, data, size);
                valueSize += size;
            }
        }

        bool parseLob(LobCtx* lobCtx, const uint8_t* data, uint64_t size, uint64_t charsetId, typeObj obj, FileOffset fileOffset, bool isClob, bool isSystem) {
            bool appendData = false;
            bool hasPrev = false;
            bool hasNext = true;
            valueSize = 0;
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA)))
                ctx->logTrace(Ctx::TRACE::LOB_DATA, dumpLob(data, size));

            if (unlikely(size < 20)) {
                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 1");
                return false;
            }

            const uint32_t flags = data[5];
            const LobId lobId(data + 10);
            lobCtx->checkOrphanedLobs(ctx, lobId, lastXid, fileOffset);

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
                valueBufferCheck(lobData->pageSize * static_cast<uint64_t>(lobData->sizePages) + lobData->sizeRest, fileOffset);

                typeDba pageNo = 0;
                for (const auto& [pageNoLob, page]: lobData->indexMap) {
                    if (unlikely(pageNo != pageNoLob)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 2");
                        pageNo = pageNoLob;
                    }

                    const LobDataElement element(page, 0);
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

                    const auto* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));

                    valueBufferCheck(chunkSize * 4, fileOffset);
                    addLobToOutput(redoLogRecordLob->data(redoLogRecordLob->lobData), chunkSize, charsetId, fileOffset, appendData, isClob,
                                   hasPrev, hasNext, isSystem);
                    appendData = true;
                    hasPrev = true;
                    ++pageNo;
                }

                if (hasNext)
                    addLobToOutput(nullptr, 0, charsetId, fileOffset, appendData, isClob, true, false, isSystem);
            } else {
                // In-row
                if (unlikely(size < 23)) {
                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 3");
                    return false;
                }
                const uint16_t bodySize = Ctx::read16Big(data + 20);
                if (unlikely(size != static_cast<uint64_t>(bodySize + 20))) {
                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 4");
                    return false;
                }
                const uint16_t flg2 = Ctx::read16Big(data + 22);

                uint64_t totalLobSize = 0;
                uint64_t chunkSize;
                uint64_t dataOffset;

                // In-index
                if ((flg2 & 0x0400) == 0x0400) {
                    if (unlikely(size < 36)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 5");
                        return false;
                    }
                    const uint32_t pageCnt = Ctx::read32Big(data + 24);
                    const uint16_t sizeRest = Ctx::read16Big(data + 28);
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
                            page = Ctx::read32Big(data + dataOffset);
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

                        const LobDataElement element(page, 0);
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
                            const auto* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                            if (j < pageCnt)
                                chunkSize = redoLogRecordLob->lobDataSize;
                            else
                                chunkSize = sizeRest;
                            if (j == jMax - 1U)
                                hasNext = false;

                            valueBufferCheck(chunkSize * 4, fileOffset);
                            addLobToOutput(redoLogRecordLob->data(redoLogRecordLob->lobData), chunkSize, charsetId, fileOffset, appendData, isClob,
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
                    const uint32_t zero1 = Ctx::read32Big(data + 24);
                    chunkSize = Ctx::read16Big(data + 28);
                    const uint32_t zero2 = Ctx::read32Big(data + 30);

                    if (unlikely(zero1 != 0 || zero2 != 0 || chunkSize + 16 != bodySize)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) + ", location: 9");
                        return false;
                    }

                    if (chunkSize == 0) {
                        // Null value
                    } else {
                        if (unlikely(size < chunkSize + 36)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                         ", location: 10");
                            return false;
                        }

                        valueBufferCheck(chunkSize * 4, fileOffset);
                        addLobToOutput(data + 36, chunkSize, charsetId, fileOffset, false, isClob, false, false, isSystem);
                    }
                } else {
                    if (unlikely(bodySize < 10)) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                     ", location: 11");
                        return false;
                    }
                    const uint8_t flg3 = data[26];
                    const uint8_t flg4 = data[27];
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

                        totalLobSize = Ctx::read16Big(data + 28);
                        dataOffset = 30;
                    } else if ((flg3 & 0x03) == 2) {
                        if (unlikely(size < 32)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                         ", location: 14");
                            return false;
                        }

                        totalLobSize = Ctx::read24Big(data + 28);
                        dataOffset = 31;
                    } else if ((flg3 & 0x03) == 3) {
                        if (unlikely(size < 32)) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                         ", location: 15");
                            return false;
                        }

                        totalLobSize = Ctx::read32Big(data + 28);
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

                        valueBufferCheck(chunkSize * 4, fileOffset);
                        addLobToOutput(data + dataOffset, chunkSize, charsetId, fileOffset, false, isClob, false, false,
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
                            const uint lobPages = static_cast<uint>(data[dataOffset++]) + 1;

                            for (uint i = 0; i < lobPages; ++i) {
                                if (unlikely(dataOffset + 1U >= size)) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                 ", location: 19");
                                    return false;
                                }
                                const uint8_t flg5 = data[dataOffset++];

                                typeDba page = Ctx::read32Big(data + dataOffset);
                                dataOffset += 4;
                                uint16_t pageCnt = 0;
                                if ((flg5 & 0x20) == 0) {
                                    pageCnt = data[dataOffset++];
                                } else if ((flg5 & 0x20) == 0x20) {
                                    pageCnt = Ctx::read16Big(data + dataOffset);
                                    dataOffset += 2;
                                }

                                for (uint16_t j = 0; j < pageCnt; ++j) {
                                    const LobDataElement element(page, 0);
                                    auto dataMapIt = lobData->dataMap.find(element);
                                    if (unlikely(dataMapIt == lobData->dataMap.end())) {
                                        if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB data (new in-value) for xid: " + lastXid.toString() +
                                                          " LOB: " + lobId.lower() + " page: " + std::to_string(page) + " obj: " + std::to_string(obj));
                                            ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                                        }
                                        return false;
                                    }

                                    while (dataMapIt != lobData->dataMap.end() && dataMapIt->first.dba == page) {
                                        const auto* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                        chunkSize = redoLogRecordLob->lobDataSize;
                                        if (i == lobPages - 1U && j == pageCnt - 1U)
                                            hasNext = false;

                                        valueBufferCheck(chunkSize * 4, fileOffset);
                                        addLobToOutput(redoLogRecordLob->data(redoLogRecordLob->lobData), chunkSize, charsetId, fileOffset, appendData, isClob,
                                                       hasPrev, hasNext, isSystem);
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
                            typeDba listPage = Ctx::read32Big(data + dataOffset);

                            while (listPage != 0) {
                                auto listMapIt = lobCtx->listMap.find(listPage);
                                if (unlikely(listMapIt == lobCtx->listMap.end())) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                                 ", location: 21, page: " + std::to_string(listPage) + ", offset: " + std::to_string(dataOffset));
                                    return false;
                                }

                                const uint8_t* dataLob = listMapIt->second;
                                listPage = *reinterpret_cast<const typeDba*>(dataLob);
                                const uint32_t aSiz = ctx->read32(dataLob + 4);

                                for (uint32_t i = 0; i < aSiz; ++i) {
                                    const uint16_t pageCnt = ctx->read16(dataLob + (i * 8) + 8 + 2);
                                    typeDba page = ctx->read32(dataLob + (i * 8) + 8 + 4);

                                    for (uint16_t j = 0; j < pageCnt; ++j) {
                                        const LobDataElement element(page, 0);
                                        auto dataMapIt = lobData->dataMap.find(element);
                                        if (unlikely(dataMapIt == lobData->dataMap.end())) {
                                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB_DATA))) {
                                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "missing LOB data (new in-value 12+) for xid: " + lastXid.toString() +
                                                              " LOB: " + lobId.lower() + " page: " + std::to_string(page) + " obj: " + std::to_string(obj));
                                                ctx->logTrace(Ctx::TRACE::LOB_DATA, "dump LOB: " + lobId.lower() + " data: " +
                                                              dumpLob(dataLob, size));
                                            }
                                            return false;
                                        }

                                        const auto* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                        chunkSize = redoLogRecordLob->lobDataSize;
                                        if (listPage == 0 && i == aSiz - 1U && j == pageCnt - 1U)
                                            hasNext = false;

                                        valueBufferCheck(chunkSize * 4, fileOffset);
                                        addLobToOutput(redoLogRecordLob->data(redoLogRecordLob->lobData), chunkSize, charsetId, fileOffset,
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

                        const uint lobPages = static_cast<uint>(data[dataOffset++]) + 1;

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

                        for (uint i = 0; i < lobPages; ++i) {
                            if (unlikely(dataOffset + 5 >= size)) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                             ", location: 24");
                                return false;
                            }

                            const uint8_t flg5 = data[dataOffset++];
                            typeDba page = Ctx::read32Big(data + dataOffset);
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
                                pageCnt = Ctx::read16Big(data + dataOffset);
                                dataOffset += 2;
                            } else {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, size) +
                                             ", location: 27");
                                return false;
                            }

                            for (uint64_t j = 0; j < pageCnt; ++j) {
                                const LobDataElement element(page, 0);
                                auto dataMapIt = lobData->dataMap.find(element);
                                if (unlikely(dataMapIt == lobData->dataMap.end())) {
                                    ctx->warning(60005, "missing LOB data (new in-value) for xid: " + lastXid.toString() + ", LOB: " +
                                                 lobId.lower() + ", page: " + std::to_string(page) + ", obj: " + std::to_string(obj));
                                    ctx->warning(60006, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, size));
                                    return false;
                                }

                                const auto* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                chunkSize = redoLogRecordLob->lobDataSize;
                                if (i == lobPages - 1U && j == pageCnt - 1U)
                                    hasNext = false;

                                valueBufferCheck(chunkSize * 4, fileOffset);
                                addLobToOutput(redoLogRecordLob->data(redoLogRecordLob->lobData), chunkSize, charsetId, fileOffset, appendData,
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

        void parseRaw(const uint8_t* data, uint64_t size, FileOffset fileOffset) {
            valueBufferPurge();
            valueBufferCheck(size * 2, fileOffset);

            if (size == 0)
                return;

            for (uint64_t j = 0; j < size; ++j) {
                valueBufferAppend(Data::map16U(data[j] >> 4));
                valueBufferAppend(Data::map16U(data[j] & 0x0F));
            }
        }

        void parseString(const uint8_t* data, uint64_t size, uint64_t charsetId, FileOffset fileOffset, bool appendData, bool hasPrev, bool hasNext,
                         bool isSystem) {
            const CharacterSet* characterSet = locales->characterMap[charsetId];
            if (unlikely(characterSet == nullptr && !format.isCharFormatNoMapping()))
                throw RedoLogException(50010, "can't find character set map for id = " + std::to_string(charsetId) + " at offset: " + fileOffset.toString());
            if (!appendData)
                valueBufferPurge();
            if (size == 0 && (!hasPrev || prevCharsSize <= 0))
                return;

            const uint8_t* parseData = data;
            uint64_t parseSize = size;
            uint64_t overlap = 0;

            // Something left to parse from previous run
            if (hasPrev && prevCharsSize > 0) {
                overlap = std::min((2 * CharacterSet::MAX_CHARACTER_LENGTH) - prevCharsSize, size);
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
                    const uint64_t processed = overlap - parseSize;
                    parseData = data + processed;
                    parseSize = size - processed;
                    overlap = 0;
                }

                typeUnicode unicodeCharacter;

                if (!format.isCharFormatNoMapping()) {
                    unicodeCharacter = characterSet->decode(ctx, lastXid, parseData, parseSize);

                    if (likely(!format.isCharFormatHex() || isSystem)) {
                        if (likely(unicodeCharacter <= 0x7F)) {
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
                            throw RedoLogException(50011, "got character code: U+" + std::to_string(unicodeCharacter) + " at offset: " + fileOffset.toString());
                    } else {
                        if (unicodeCharacter <= 0x7F) {
                            // 0xxxxxxx
                            valueBufferAppendHex(unicodeCharacter, fileOffset);

                        } else if (unicodeCharacter <= 0x7FF) {
                            // 110xxxxx 10xxxxxx
                            valueBufferAppendHex(0xC0 | static_cast<uint8_t>(unicodeCharacter >> 6), fileOffset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F), fileOffset);

                        } else if (unicodeCharacter <= 0xFFFF) {
                            // 1110xxxx 10xxxxxx 10xxxxxx
                            valueBufferAppendHex(0xE0 | static_cast<uint8_t>(unicodeCharacter >> 12), fileOffset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F), fileOffset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F), fileOffset);

                        } else if (unicodeCharacter <= 0x10FFFF) {
                            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                            valueBufferAppendHex(0xF0 | static_cast<uint8_t>(unicodeCharacter >> 18), fileOffset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>((unicodeCharacter >> 12) & 0x3F), fileOffset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F), fileOffset);
                            valueBufferAppendHex(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F), fileOffset);

                        } else
                            throw RedoLogException(50011, "got character code: U+" + std::to_string(unicodeCharacter) + " at offset: " + fileOffset.toString());
                    }
                } else {
                    unicodeCharacter = *parseData++;
                    --parseSize;

                    if (!format.isCharFormatHex() || isSystem) {
                        valueBufferAppend(unicodeCharacter);
                    } else {
                        valueBufferAppendHex(unicodeCharacter, fileOffset);
                    }
                }
            }
        }

        void valueBufferCheck(uint64_t size, FileOffset fileOffset) {
            if (unlikely(valueSize + size > VALUE_BUFFER_MAX))
                throw RedoLogException(50012, "trying to allocate length for value: " + std::to_string(valueSize + size) +
                                       " exceeds maximum: " + std::to_string(VALUE_BUFFER_MAX) + " at offset: " + fileOffset.toString());

            if (valueSize + size < valueBufferSize)
                return;

            do {
                valueBufferSize <<= 1;
            } while (valueSize + size >= valueBufferSize);

            const auto newValueBuffer = new char[valueBufferSize];
            memcpy(newValueBuffer, valueBuffer, valueSize);
            delete[] valueBuffer;
            valueBuffer = newValueBuffer;
        }

        void valueBufferPurge() {
            valueSize = 0;
            if (valueBufferSize == VALUE_BUFFER_MIN)
                return;

            delete[] valueBuffer;
            valueBuffer = new char[VALUE_BUFFER_MIN];
            valueBufferSize = VALUE_BUFFER_MIN;
        }

        virtual void columnFloat(const std::string& columnName, double value) = 0;
        virtual void columnDouble(const std::string& columnName, long double value) = 0;
        virtual void columnString(const std::string& columnName) = 0;
        virtual void columnNumber(const std::string& columnName, int precision, int scale) = 0;
        virtual void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t size) = 0;
        virtual void columnRowId(const std::string& columnName, RowId rowId) = 0;
        virtual void columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) = 0;
        virtual void columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const std::string_view& tz) = 0;
        virtual void processInsert(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) = 0;
        virtual void processUpdate(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) = 0;
        virtual void processDelete(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) = 0;
        virtual void processDdl(Seq sequence, Scn scn, Time timestamp, const DbTable* table, typeObj obj) = 0;
        virtual void processBeginMessage(Seq sequence, Time timestamp) = 0;
        bool parseXml(const XmlCtx* xmlCtx, const uint8_t* data, uint64_t size, FileOffset fileOffset);

    public:
        SystemTransaction* systemTransaction{nullptr};
        uint64_t buffersAllocated{0};
        BuilderQueue* firstBuilderQueue{nullptr};
        BuilderQueue* lastBuilderQueue{nullptr};
        Scn lwnScn{Scn::none()};
        typeIdx lwnIdx{0};

        Builder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, const Format& newFormat, uint64_t newFlushBuffer);
        virtual ~Builder();

        [[nodiscard]] uint64_t builderSize() const;
        [[nodiscard]] uint64_t getMaxMessageMb() const;
        void setMaxMessageMb(uint64_t maxMessageMb);
        void processBegin(Xid xid, uint16_t newThread, Seq newBeginSequence, Scn newBeginScn, Time newBeginTimestamp, Seq newCommitSequence, Scn newCommitScn,
                          Time newCommitTimestamp, const AttributeMap* newAttributes);
        void processInsertMultiple(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                   const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDeleteMultiple(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                   const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDml(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const std::deque<const RedoLogRecord*>& redo1,
                        const std::deque<const RedoLogRecord*>& redo2, Format::TRANSACTION_TYPE transactionType, bool system, bool schema, bool dump);
        void processDdl(Seq sequence, Scn scn, Time timestamp, const RedoLogRecord* redoLogRecord1);
        virtual void initialize();
        virtual void processCommit() = 0;
        virtual void processCheckpoint(Seq sequence, Scn scn, Time timestamp, FileOffset fileOffset, bool redo) = 0;
        void releaseBuffers(Thread* t, uint64_t maxId);
        void releaseDdl();
        void appendDdlChunk(const uint8_t* data, typeTransactionSize size);
        void sleepForWriterWork(Thread* t, uint64_t queueSize, uint64_t nanoseconds);
        void wakeUp();

        void flush() {
            {
                ctx->parserThread->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::BUILDER_COMMIT);
                std::unique_lock const lck(mtx);
                condNoWriterWork.notify_all();
            }
            ctx->parserThread->contextSet(Thread::CONTEXT::TRAN, Thread::REASON::TRAN);
            unconfirmedSize = 0;
        }


        friend class SystemTransaction;
    };
}

#endif
