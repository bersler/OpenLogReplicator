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
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "../common/Ctx.h"
#include "../common/LobCtx.h"
#include "../common/LobData.h"
#include "../common/LobKey.h"
#include "../common/RedoLogRecord.h"
#include "../common/types.h"
#include "../common/typeLobId.h"
#include "../common/typeRowId.h"
#include "../common/typeXid.h"
#include "../common/exception/RedoLogException.h"
#include "../locales/CharacterSet.h"
#include "../locales/Locales.h"

#ifndef BUILDER_H_
#define BUILDER_H_

#define OUTPUT_BUFFER_DATA_SIZE                 (MEMORY_CHUNK_SIZE - sizeof(struct BuilderQueue))
#define OUTPUT_BUFFER_MESSAGE_ALLOCATED         0x0001
#define OUTPUT_BUFFER_MESSAGE_CONFIRMED         0x0002
#define OUTPUT_BUFFER_MESSAGE_CHECKPOINT        0x0004
#define VALUE_BUFFER_MIN                        1048576
#define VALUE_BUFFER_MAX                        4294967296
#define BUFFER_START_UNDEFINED                  0xFFFFFFFFFFFFFFFF

#define XML_PROLOG_RGUID                        0x04
#define XML_PROLOG_DOCID                        0x08
#define XML_PROLOG_PATHID                       0x10
#define XML_PROLOG_BIGINT                       0x40

#define XML_HEADER_STANDALONE                   0x01
#define XML_HEADER_XMLDECL                      0x02
#define XML_HEADER_ENCODING                     0x04
#define XML_HEADER_VERSION                      0x08
#define XML_HEADER_STANDALONE_YES               0x10
#define XML_HEADER_VERSION_1_1                  0x80

namespace OpenLogReplicator {
    class Ctx;
    class CharacterSet;
    class Locales;
    class OracleTable;
    class Builder;
    class Metadata;
    class SystemTransaction;
    class XmlCtx;

    struct BuilderQueue {
        uint64_t id;
        std::atomic<uint64_t> length;
        std::atomic<uint64_t> start;
        uint8_t* data;
        std::atomic<BuilderQueue*> next;
    };

    struct BuilderMsg {
        void* ptr;
        uint64_t id;
        uint64_t queueId;
        std::atomic<uint64_t> length;
        typeScn scn;
        typeScn lwnScn;
        typeIdx lwnIdx;
        uint8_t* data;
        typeSeq sequence;
        typeObj obj;
        uint16_t pos;
        uint16_t flags;
    };

    class Builder {
    protected:
        Ctx* ctx;
        Locales* locales;
        Metadata* metadata;
        BuilderMsg* msg;

        uint64_t dbFormat;
        uint64_t attributesFormat;
        uint64_t intervalDtsFormat;
        uint64_t intervalYtmFormat;
        uint64_t messageFormat;
        uint64_t ridFormat;
        uint64_t xidFormat;
        uint64_t timestampFormat;
        uint64_t timestampTzFormat;
        uint64_t timestampAll;
        uint64_t charFormat;
        uint64_t scnFormat;
        uint64_t scnAll;
        uint64_t unknownFormat;
        uint64_t schemaFormat;
        uint64_t columnFormat;
        uint64_t unknownType;
        uint64_t unconfirmedLength;
        uint64_t messageLength;
        uint64_t messagePosition;
        uint64_t flushBuffer;
        char* valueBuffer;
        uint64_t valueLength;
        uint64_t valueBufferLength;
        char* valueBufferOld;
        uint64_t valueLengthOld;
        std::unordered_set<const OracleTable*> tables;
        typeScn commitScn;
        typeXid lastXid;
        uint64_t valuesSet[COLUMN_LIMIT_23_0 / sizeof(uint64_t)];
        uint64_t valuesMerge[COLUMN_LIMIT_23_0 / sizeof(uint64_t)];
        int64_t lengths[COLUMN_LIMIT_23_0][4];
        uint8_t* values[COLUMN_LIMIT_23_0][4];
        uint64_t lengthsPart[3][COLUMN_LIMIT_23_0][4];
        uint8_t* valuesPart[3][COLUMN_LIMIT_23_0][4];
        uint64_t valuesMax;
        uint8_t* merges[COLUMN_LIMIT_23_0 * 4];
        uint64_t mergesMax;
        uint64_t id;
        uint64_t num;
        uint64_t maxMessageMb;      // Maximum message size able to handle by writer
        bool newTran;
        bool compressedBefore;
        bool compressedAfter;
        uint8_t prevChars[MAX_CHARACTER_LENGTH * 2];
        uint64_t prevCharsSize;
        const std::unordered_map<std::string, std::string>* attributes;

        std::mutex mtx;
        std::condition_variable condNoWriterWork;

        double decodeFloat(const uint8_t* data);
        long double decodeDouble(const uint8_t* data);

        inline void builderRotate(bool copy) {
            auto nextBuffer = reinterpret_cast<BuilderQueue*>(ctx->getMemoryChunk(MEMORY_MODULE_BUILDER, true));
            nextBuffer->next = nullptr;
            nextBuffer->id = lastBuilderQueue->id + 1;
            nextBuffer->data = reinterpret_cast<uint8_t*>(nextBuffer) + sizeof(struct BuilderQueue);

            // Message could potentially fit in one buffer
            if (copy && msg != nullptr && messageLength + messagePosition < OUTPUT_BUFFER_DATA_SIZE) {
                memcpy(reinterpret_cast<void*>(nextBuffer->data), msg, messagePosition);
                msg = reinterpret_cast<BuilderMsg*>(nextBuffer->data);
                msg->data = nextBuffer->data + sizeof(struct BuilderMsg);
                nextBuffer->start = 0;
            } else {
                lastBuilderQueue->length += messagePosition;
                messageLength += messagePosition;
                messagePosition = 0;
                nextBuffer->start = BUFFER_START_UNDEFINED;
            }
            nextBuffer->length = 0;

            {
                std::unique_lock<std::mutex> lck(mtx);
                lastBuilderQueue->next = nextBuffer;
                ++buffersAllocated;
                lastBuilderQueue = nextBuffer;
            }
        }

        void processValue(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeCol col, const uint8_t* data, uint64_t length, uint64_t offset,
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
                    values[column][VALUE_BEFORE] = nullptr;
                    values[column][VALUE_BEFORE_SUPP] = nullptr;
                    values[column][VALUE_AFTER] = nullptr;
                    values[column][VALUE_AFTER_SUPP] = nullptr;
                }
            }
            valuesMax = 0;
            compressedBefore = false;
            compressedAfter = false;
        };

        inline void valueSet(uint64_t type, uint16_t column, uint8_t* data, uint16_t length, uint8_t fb, bool dump) {
            if ((ctx->trace & TRACE_DML) != 0 || dump) {
                std::ostringstream ss;
                ss << "DML: value: " << std::dec << type << "/" << column << "/" << std::dec << length << "/" << std::setfill('0') <<
                   std::setw(2) << std::hex << static_cast<uint64_t>(fb) << " to: ";
                for (uint64_t i = 0; i < length && i < 64; ++i) {
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

            switch (fb & (FB_P | FB_N)) {
                case 0:
                    lengths[column][type] = length;
                    values[column][type] = data;
                    break;

                case FB_N:
                    lengthsPart[0][column][type] = length;
                    valuesPart[0][column][type] = data;
                    if ((valuesMerge[base] & mask) == 0)
                        valuesMerge[base] |= mask;
                    break;

                case FB_P | FB_N:
                    lengthsPart[1][column][type] = length;
                    valuesPart[1][column][type] = data;
                    if ((valuesMerge[base] & mask) == 0)
                        valuesMerge[base] |= mask;
                    break;

                case FB_P:
                    lengthsPart[2][column][type] = length;
                    valuesPart[2][column][type] = data;
                    if ((valuesMerge[base] & mask) == 0)
                        valuesMerge[base] |= mask;
                    break;
            }
        };

        inline void builderShift(bool copy) {
            ++messagePosition;

            if (lastBuilderQueue->length + messagePosition >= OUTPUT_BUFFER_DATA_SIZE)
                builderRotate(copy);
        };

        inline void builderShiftFast(uint64_t bytes) {
            messagePosition += bytes;
        };

        inline void builderBegin(typeScn scn, typeSeq sequence, typeObj obj, uint16_t flags) {
            messageLength = 0;
            messagePosition = 0;
            if ((scnFormat & SCN_ALL_COMMIT_VALUE) != 0)
                scn = commitScn;

            if (lastBuilderQueue->length + messagePosition + sizeof(struct BuilderMsg) >= OUTPUT_BUFFER_DATA_SIZE)
                builderRotate(true);

            msg = reinterpret_cast<BuilderMsg*>(lastBuilderQueue->data + lastBuilderQueue->length);
            builderShiftFast(sizeof(struct BuilderMsg));
            msg->scn = scn;
            msg->lwnScn = lwnScn;
            msg->lwnIdx = lwnIdx++;
            msg->sequence = sequence;
            msg->length = 0;
            msg->id = id++;
            msg->obj = obj;
            msg->pos = 0;
            msg->flags = flags;
            msg->data = lastBuilderQueue->data + lastBuilderQueue->length + sizeof(struct BuilderMsg);
        };

        inline void builderCommit(bool force) {
            messageLength += messagePosition;
            if (messageLength == sizeof(struct BuilderMsg))
                throw RedoLogException(50058, "output buffer - commit of empty transaction");

            msg->queueId = lastBuilderQueue->id;
            builderShiftFast((8 - (messagePosition & 7)) & 7);
            unconfirmedLength += messageLength;
            msg->length = messageLength - sizeof(struct BuilderMsg);
            lastBuilderQueue->length += messagePosition;
            if (lastBuilderQueue->start == BUFFER_START_UNDEFINED)
                lastBuilderQueue->start = static_cast<uint64_t>(lastBuilderQueue->length);

            if (force || flushBuffer == 0 || unconfirmedLength > flushBuffer) {
                {
                    std::unique_lock<std::mutex> lck(mtx);
                    condNoWriterWork.notify_all();
                }
                unconfirmedLength = 0;
            }
            msg = nullptr;
        };

        void append(char character) {
            lastBuilderQueue->data[lastBuilderQueue->length + messagePosition] = character;
            builderShift(true);
        };

        void append(const char* str, uint64_t length) {
            if (lastBuilderQueue->length + messagePosition + length < OUTPUT_BUFFER_DATA_SIZE) {
                memcpy(reinterpret_cast<void*>(lastBuilderQueue->data + lastBuilderQueue->length + messagePosition),
                       reinterpret_cast<const void*>(str), length);
                messagePosition += length;
            } else {
                for (uint64_t i = 0; i < length; ++i)
                    append(*str++);
            }
        };

        inline void append(const std::string& str) {
            uint64_t length = str.length();
            if (lastBuilderQueue->length + messagePosition + length < OUTPUT_BUFFER_DATA_SIZE) {
                memcpy(reinterpret_cast<void*>(lastBuilderQueue->data + lastBuilderQueue->length + messagePosition),
                       reinterpret_cast<const void*>(str.c_str()), length);
                messagePosition += length;
            } else {
                const char* charStr = str.c_str();
                for (uint64_t i = 0; i < length; ++i)
                    append(*charStr++);
            }
        };

        inline void columnUnknown(const std::string& columnName, const uint8_t* data, uint64_t length) {
            valueBuffer[0] = '?';
            valueLength = 1;
            columnString(columnName);
            if (unknownFormat == UNKNOWN_FORMAT_DUMP) {
                std::ostringstream ss;
                for (uint64_t j = 0; j < length; ++j)
                    ss << " " << std::hex << std::setfill('0') << std::setw(2) << (static_cast<uint64_t>(data[j]));
                ctx->warning(60002, "unknown value (column: " + columnName + "): " + std::to_string(length) + " - " + ss.str());
            }
        };

        inline void valueBufferAppend(const char* text, uint64_t length) {
            for (uint64_t i = 0; i < length; ++i)
                valueBufferAppend(*text++);
        };

        inline void valueBufferAppend(uint8_t value) {
            valueBuffer[valueLength++] = static_cast<char>(value);
        };

        inline void valueBufferAppendHex(uint8_t value, uint64_t offset) {
            valueBufferCheck(2, offset);
            valueBuffer[valueLength++] = Ctx::map16((value >> 4) & 0x0F);
            valueBuffer[valueLength++] = Ctx::map16(value & 0x0F);
        };

        inline void parseNumber(const uint8_t* data, uint64_t length, uint64_t offset) {
            valueBufferPurge();
            valueBufferCheck(length * 2 + 2, offset);

            uint8_t digits = data[0];
            // Just zero
            if (digits == 0x80) {
                valueBufferAppend('0');
            } else {
                uint64_t j = 1;
                uint64_t jMax = length - 1;

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

                        while (j <= jMax - 1) {
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

                        while (j <= jMax - 1) {
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

        inline std::string dumpLob(const uint8_t* data, uint64_t length) const {
            std::ostringstream ss;
            for (uint64_t j = 0; j < length; ++j) {
                ss << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]);
            }
            return ss.str();
        }

        inline void addLobToOutput(const uint8_t* data, uint64_t length, uint64_t charsetId, uint64_t offset, bool appendData, bool isClob, bool hasPrev,
                            bool hasNext, bool isSystem) {
            if (isClob) {
                parseString(data, length, charsetId, offset, appendData, hasPrev, hasNext, isSystem);
            } else {
                memcpy(reinterpret_cast<void*>(valueBuffer + valueLength),
                       reinterpret_cast<const void*>(data), length);
                valueLength += length;
            };
        }

        inline bool parseLob(LobCtx* lobCtx, const uint8_t* data, uint64_t length, uint64_t charsetId, typeObj obj, uint64_t offset, bool isClob, bool isSystem) {
            bool appendData = false, hasPrev = false, hasNext = true;
            valueLength = 0;
            if (ctx->trace & TRACE_LOB_DATA)
                ctx->logTrace(TRACE_LOB_DATA, dumpLob(data, length));

            if (length < 20) {
                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 1");
                return false;
            }

            uint32_t flags = data[5];
            typeLobId lobId(data + 10);
            lobCtx->checkOrphanedLobs(ctx, lobId, lastXid, offset);

            // In-index
            if ((flags & 0x04) == 0) {
                auto lobsIt = lobCtx->lobs.find(lobId);
                if (lobsIt == lobCtx->lobs.end()) {
                    if (ctx->trace & TRACE_LOB_DATA)
                        ctx->logTrace(TRACE_LOB_DATA, "LOB missing LOB index xid: " + lastXid.toString() + " LOB: " + lobId.lower() +
                                                      " data: " + dumpLob(data, length));
                    return true;
                }
                LobData* lobData = lobsIt->second;
                valueBufferCheck(static_cast<uint64_t>(lobData->pageSize) * static_cast<uint64_t>(lobData->sizePages) + lobData->sizeRest, offset);

                uint32_t pageNo = 0;
                for (auto indexMapIt: lobData->indexMap) {
                    uint32_t pageNoLob = indexMapIt.first;
                    typeDba page = indexMapIt.second;
                    if (pageNo != pageNoLob) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 2");
                        pageNo = pageNoLob;
                    }

                    LobDataElement element(page, 0);
                    auto dataMapIt = lobData->dataMap.find(element);
                    if (dataMapIt == lobData->dataMap.end()) {
                        if (ctx->trace & TRACE_LOB_DATA)
                            ctx->logTrace(TRACE_LOB_DATA, "missing LOB (in-index) for xid: " + lastXid.toString() + " LOB: " +
                                                          lobId.lower() + " page: " + std::to_string(page) + " obj: " + std::to_string(obj));
                        if (ctx->trace & TRACE_LOB_DATA)
                            ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                        return false;
                    }
                    uint64_t chunkLength = lobData->pageSize;

                    // Last
                    if (pageNo == lobData->sizePages) {
                        chunkLength = lobData->sizeRest;
                        hasNext = false;
                    }

                    RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                    redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));

                    valueBufferCheck(chunkLength * 4, offset);
                    addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, offset, appendData, isClob,
                                   hasPrev, hasNext, isSystem);
                    appendData = true;
                    hasPrev = true;
                    ++pageNo;
                }

                if (hasNext)
                    addLobToOutput(nullptr, 0, charsetId, offset, appendData, isClob, true, false, isSystem);
            } else {
                // In-row
                if (length < 23) {
                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 3");
                    return false;
                }
                uint16_t bodyLength = ctx->read16Big(data + 20);
                if (length != static_cast<uint64_t>(bodyLength + 20)) {
                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 4");
                    return false;
                }
                uint16_t flg2 = ctx->read16Big(data + 22);

                uint64_t totalLobLength = 0;
                uint64_t chunkLength;
                uint64_t dataOffset;

                // In-index
                if ((flg2 & 0x0400) == 0x0400) {
                    if (length < 36) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 5");
                        return false;
                    }
                    uint32_t pageCnt = ctx->read32Big(data + 24);
                    uint16_t sizeRest = ctx->read16Big(data + 28);
                    dataOffset = 36;

                    auto lobsIt = lobCtx->lobs.find(lobId);
                    if (lobsIt == lobCtx->lobs.end()) {
                        if (ctx->trace & TRACE_LOB_DATA) {
                            ctx->logTrace(TRACE_LOB_DATA, "missing LOB (in-index) for xid: " + lastXid.toString() + " obj: " +
                                                          std::to_string(obj));
                            ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                        }
                        return false;
                    }
                    LobData* lobData = lobsIt->second;
                    totalLobLength = pageCnt * lobData->pageSize + sizeRest;
                    if (totalLobLength == 0)
                        return true;

                    uint64_t jMax = pageCnt;
                    if (sizeRest > 0)
                        ++jMax;

                    for (uint64_t j = 0; j < jMax; ++j) {
                        typeDba page = 0;
                        if (dataOffset < length) {
                            if (length < dataOffset + 4) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                    ", location: 6");
                                return false;
                            }
                            page = ctx->read32Big(data + dataOffset);
                        } else {
                            // Rest of data in LOB index
                            auto indexMapIt = lobData->indexMap.find(j);
                            if (indexMapIt == lobData->indexMap.end()) {
                                ctx->warning(60004, "can't find page " + std::to_string(j) + " for xid: " + lastXid.toString() + ", LOB: " +
                                                    lobId.lower() + ", obj: " + std::to_string(obj));
                                break;
                            }
                            page = indexMapIt->second;
                        }

                        LobDataElement element(page, 0);
                        auto dataMapIt = lobData->dataMap.find(element);
                        if (dataMapIt == lobData->dataMap.end()) {
                            if (ctx->trace & TRACE_LOB_DATA) {
                                ctx->logTrace(TRACE_LOB_DATA, "missing LOB index (in-index) for xid: " + lastXid.toString() + " LOB: " +
                                                              lobId.lower() + " page: " + std::to_string(page) + " obj: " + std::to_string(obj));
                                ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                            }
                            return false;
                        }

                        while (dataMapIt != lobData->dataMap.end() && dataMapIt->first.dba == page) {
                            RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                            redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));
                            if (j < pageCnt)
                                chunkLength = redoLogRecordLob->lobDataLength;
                            else
                                chunkLength = sizeRest;
                            if (j == jMax - 1)
                                hasNext = false;

                            valueBufferCheck(chunkLength * 4, offset);
                            addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, offset, appendData, isClob,
                                           hasPrev, hasNext, isSystem);
                            appendData = true;
                            hasPrev = true;
                            totalLobLength -= chunkLength;
                            ++dataMapIt;
                        }

                        ++page;
                        dataOffset += 4;
                    }
                } else if ((flg2 & 0x0100) == 0x0100) {
                    // In-value
                    if (bodyLength < 16) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 7");
                        return false;
                    }

                    if (length < 34) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 8");
                        return false;
                    }
                    uint32_t zero1 = ctx->read32Big(data + 24);
                    chunkLength = ctx->read16Big(data + 28);
                    uint32_t zero2 = ctx->read32Big(data + 30);

                    if (zero1 != 0 || zero2 != 0 || chunkLength + 16 != bodyLength) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) + ", location: 9");
                        return false;
                    }

                    if (chunkLength == 0) {
                        // Null value
                    } else {
                        if (length < static_cast<uint64_t>(chunkLength) + 36) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 10");
                            return false;
                        }

                        valueBufferCheck(chunkLength * 4, offset);
                        addLobToOutput(data + 36, chunkLength, charsetId, offset, false, isClob, false, false, isSystem);
                    }
                } else {
                    if (bodyLength < 10) {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                            ", location: 11");
                        return false;
                    }
                    uint8_t flg3 = data[26];
                    uint8_t flg4 = data[27];
                    if ((flg3 & 0x03) == 0) {
                        if (length < 30) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 12");
                            return false;
                        }

                        totalLobLength = data[28];
                        dataOffset = 29;
                    } else if ((flg3 & 0x03) == 1) {
                        if (length < 30) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 13");
                            return false;
                        }

                        totalLobLength = ctx->read16Big(data + 28);
                        dataOffset = 30;
                    } else if ((flg3 & 0x03) == 2) {
                        if (length < 32) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 14");
                            return false;
                        }

                        totalLobLength = ctx->read24Big(data + 28);
                        dataOffset = 31;
                    } else if ((flg3 & 0x03) == 3) {
                        if (length < 32) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 15");
                            return false;
                        }

                        totalLobLength = ctx->read32Big(data + 28);
                        dataOffset = 32;
                    } else {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                            ", location: 16");
                        return false;
                    }

                    if ((flg4 & 0x0F) == 0) {
                        ++dataOffset;
                    } else if ((flg4 & 0x0F) == 0x01) {
                        dataOffset += 2;
                    } else {
                        ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                            ", location: 17");
                        return false;
                    }

                    // Null value
                    if (totalLobLength == 0)
                        return true;

                    // Data
                    if ((flg2 & 0x0800) == 0x0800) {
                        chunkLength = totalLobLength;

                        if (dataOffset + chunkLength < length) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 18");
                            return false;
                        }

                        valueBufferCheck(chunkLength * 4, offset);
                        addLobToOutput(data + dataOffset, chunkLength, charsetId, offset, false, isClob, false, false,
                                       isSystem);
                        totalLobLength -= chunkLength;

                    } else if ((flg2 & 0x4000) == 0x4000) {
                        // 12+ data
                        auto lobsIt = lobCtx->lobs.find(lobId);
                        if (lobsIt == lobCtx->lobs.end()) {
                            if (ctx->trace & TRACE_LOB_DATA) {
                                ctx->logTrace(TRACE_LOB_DATA, "missing LOB index (12+ in-value) for xid: " + lastXid.toString() + " LOB: " +
                                                              lobId.lower() + " obj: " + std::to_string(obj));
                                ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                            }
                            return false;
                        }
                        LobData* lobData = lobsIt->second;

                        // Style 1
                        if ((flg3 & 0xF0) == 0x20) {
                            uint8_t lobPages = data[dataOffset++] + 1;

                            for (uint64_t i = 0; i < lobPages; ++i) {
                                if (dataOffset + 1 >= length) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
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

                                for (uint64_t j = 0; j < pageCnt; ++j) {
                                    LobDataElement element(page, 0);
                                    auto dataMapIt = lobData->dataMap.find(element);
                                    if (dataMapIt == lobData->dataMap.end()) {
                                        if (ctx->trace & TRACE_LOB_DATA) {
                                            ctx->logTrace(TRACE_LOB_DATA, "missing LOB data (new in-value) for xid: " + lastXid.toString() +
                                                                          " LOB: " + lobId.lower() + " page: " + std::to_string(page) + " obj: " +
                                                                          std::to_string(obj));
                                            ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                                        }
                                        return false;
                                    }

                                    while (dataMapIt != lobData->dataMap.end() && dataMapIt->first.dba == page) {
                                        RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                        redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));
                                        chunkLength = redoLogRecordLob->lobDataLength;
                                        if (i == static_cast<uint64_t>(lobPages - 1) && j == static_cast<uint64_t>(pageCnt - 1))
                                            hasNext = false;

                                        valueBufferCheck(chunkLength * 4, offset);
                                        addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, offset,
                                                       appendData, isClob, hasPrev, hasNext, isSystem);
                                        appendData = true;
                                        hasPrev = true;
                                        totalLobLength -= chunkLength;
                                        ++dataMapIt;
                                    }
                                    ++page;
                                }
                            }

                        } else if ((flg3 & 0xF0) == 0x40) {
                            // Style 2
                            if (dataOffset + 4 != length) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                    ", location: 20");
                                return false;
                            }
                            typeDba listPage = ctx->read32Big(data + dataOffset);

                            while (listPage != 0) {
                                auto listMapIt = lobCtx->listMap.find(listPage);
                                if (listMapIt == lobCtx->listMap.end()) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                        ", location: 21, page: " + std::to_string(listPage) + ", offset: " + std::to_string(dataOffset));
                                    return false;
                                }

                                uint8_t* dataLob = listMapIt->second;
                                listPage = *(reinterpret_cast<typeDba*>(dataLob));
                                uint32_t aSiz = ctx->read32(dataLob + 4);

                                for (uint64_t i = 0; i < aSiz; ++i) {
                                    uint16_t pageCnt = ctx->read16(dataLob + i * 8 + 8 + 2);
                                    typeDba page = ctx->read32(dataLob + i * 8 + 8 + 4);

                                    for (uint64_t j = 0; j < pageCnt; ++j) {
                                        LobDataElement element(page, 0);
                                        auto dataMapIt = lobData->dataMap.find(element);
                                        if (dataMapIt == lobData->dataMap.end()) {
                                            if (ctx->trace & TRACE_LOB_DATA) {
                                                ctx->logTrace(TRACE_LOB_DATA, "missing LOB data (new in-value 12+) for xid: " +
                                                                              lastXid.toString() + " LOB: " + lobId.lower() + " page: " + std::to_string(page) +
                                                                              " obj: " + std::to_string(obj));
                                                ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " +
                                                                              dumpLob(dataLob, length));
                                            }
                                            return false;
                                        }

                                        RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                        redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));
                                        chunkLength = redoLogRecordLob->lobDataLength;
                                        if (listPage == 0 && i == static_cast<uint64_t>(aSiz - 1) && j == static_cast<uint64_t>(pageCnt - 1))
                                            hasNext = false;

                                        valueBufferCheck(chunkLength * 4, offset);
                                        addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, offset,
                                                       appendData, isClob, hasPrev, hasNext, isSystem);
                                        appendData = true;
                                        hasPrev = true;
                                        totalLobLength -= chunkLength;
                                        ++page;
                                    }
                                }
                            }
                        } else {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 22");
                            return false;
                        }

                    } else {
                        // Index
                        if (dataOffset + 1 >= length) {
                            ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                ", location: 23");
                            return false;
                        }

                        uint8_t lobPages = data[dataOffset++] + 1;

                        auto lobsIt = lobCtx->lobs.find(lobId);
                        if (lobsIt == lobCtx->lobs.end()) {
                            if (ctx->trace & TRACE_LOB_DATA)
                                ctx->logTrace(TRACE_LOB_DATA, "missing LOB index (new in-value) for xid: " + lastXid.toString() + " LOB: " +
                                                              lobId.lower() + " obj: " + std::to_string(obj));
                            if (ctx->trace & TRACE_LOB_DATA)
                                ctx->logTrace(TRACE_LOB_DATA, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                            return false;
                        }
                        LobData* lobData = lobsIt->second;

                        for (uint64_t i = 0; i < lobPages; ++i) {
                            if (dataOffset + 5 >= length) {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
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
                                if (dataOffset + 1 >= length) {
                                    ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                        ", location: 26");
                                    return false;
                                }
                                pageCnt = ctx->read16Big(data + dataOffset);
                                dataOffset += 2;
                            } else {
                                ctx->warning(60003, "incorrect LOB for xid: " + lastXid.toString() + ", data:" + dumpLob(data, length) +
                                                    ", location: 27");
                                return false;
                            }

                            for (uint64_t j = 0; j < pageCnt; ++j) {
                                LobDataElement element(page, 0);
                                auto dataMapIt = lobData->dataMap.find(element);
                                if (dataMapIt == lobData->dataMap.end()) {
                                    ctx->warning(60005, "missing LOB data (new in-value) for xid: " + lastXid.toString() + ", LOB: " +
                                                        lobId.lower() + ", page: " + std::to_string(page) + ", obj: " + std::to_string(obj));
                                    ctx->warning(60006, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                                    return false;
                                }

                                RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));
                                chunkLength = redoLogRecordLob->lobDataLength;
                                if (i == static_cast<uint64_t>(lobPages - 1) && j == static_cast<uint64_t>(pageCnt - 1))
                                    hasNext = false;

                                valueBufferCheck(chunkLength * 4, offset);
                                addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, offset, appendData,
                                               isClob, hasPrev, hasNext, isSystem);
                                appendData = true;
                                hasPrev = true;
                                ++page;
                                totalLobLength -= chunkLength;
                            }
                        }
                    }
                }

                if (totalLobLength != 0) {
                    ctx->warning(60007, "incorrect LOB sum xid: " + lastXid.toString() + " left: " + std::to_string(totalLobLength) +
                                        " obj: " + std::to_string(obj));
                    ctx->warning(60006, "dump LOB: " + lobId.lower() + " data: " + dumpLob(data, length));
                    return false;
                }
            }

            return true;
        }

        inline void parseRaw(const uint8_t* data, uint64_t length, uint64_t offset) {
            valueBufferPurge();
            valueBufferCheck(length * 2, offset);

            if (length == 0)
                return;

            for (uint64_t j = 0; j < length; ++j) {
                valueBufferAppend(Ctx::map16U(data[j] >> 4));
                valueBufferAppend(Ctx::map16U(data[j] & 0x0F));
            }
        };

        inline void parseString(const uint8_t* data, uint64_t length, uint64_t charsetId, uint64_t offset, bool appendData, bool hasPrev, bool hasNext,
                         bool isSystem) {
            CharacterSet* characterSet = locales->characterMap[charsetId];
            if (characterSet == nullptr && (charFormat & CHAR_FORMAT_NOMAPPING) == 0)
                throw RedoLogException(50010, "can't find character set map for id = " + std::to_string(charsetId) + " at offset: " +
                                              std::to_string(offset));
            if (!appendData)
                valueBufferPurge();
            if (length == 0 && !(hasPrev && prevCharsSize > 0))
                return;

            const uint8_t* parseData = data;
            uint64_t parseLength = length;
            uint64_t overlap = 0;

            // Something left to parse from previous run
            if (hasPrev && prevCharsSize > 0) {
                overlap = 2 * MAX_CHARACTER_LENGTH - prevCharsSize;
                if (overlap > length)
                    overlap = length;
                memcpy(prevChars + prevCharsSize, data, overlap);
                parseData = prevChars;
                parseLength = prevCharsSize + overlap;
            }

            while (parseLength > 0) {
                // Leave for next time
                if (hasNext && parseLength < MAX_CHARACTER_LENGTH && overlap == 0) {
                    memcpy(prevChars, parseData, parseLength);
                    prevCharsSize = parseLength;
                    break;
                }

                // Switch to data buffer
                if (parseLength <= overlap && length > overlap && overlap > 0) {
                    uint64_t processed = overlap - parseLength;
                    parseData = data + processed;
                    parseLength = length - processed;
                    overlap = 0;
                }

                typeUnicode unicodeCharacter;

                if ((charFormat & CHAR_FORMAT_NOMAPPING) == 0) {
                    unicodeCharacter = characterSet->decode(ctx, lastXid, parseData, parseLength);

                    if ((charFormat & CHAR_FORMAT_HEX) == 0 || isSystem) {
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
                    --parseLength;

                    if ((charFormat & CHAR_FORMAT_HEX) == 0 || isSystem) {
                        valueBufferAppend(unicodeCharacter);
                    } else {
                        valueBufferAppendHex(unicodeCharacter, offset);
                    }
                }
            }
        };

        inline void valueBufferCheck(uint64_t length, uint64_t offset) {
            if (valueLength + length > VALUE_BUFFER_MAX)
                throw RedoLogException(50012, "trying to allocate length for value: " + std::to_string(valueLength + length) +
                                              " exceeds maximum: " + std::to_string(VALUE_BUFFER_MAX) + " at offset: " + std::to_string(offset));

            if (valueLength + length < valueBufferLength)
                return;

            do {
                valueBufferLength <<= 1;
            } while (valueLength + length >= valueBufferLength);

            char* newValueBuffer = new char[valueBufferLength];
            memcpy(reinterpret_cast<void*>(newValueBuffer),
                   reinterpret_cast<const void*>(valueBuffer), valueLength);
            delete[] valueBuffer;
            valueBuffer = newValueBuffer;
        };

        inline void valueBufferPurge() {
            valueLength = 0;
            if (valueBufferLength == VALUE_BUFFER_MIN)
                return;

            delete[] valueBuffer;
            valueBuffer = new char[VALUE_BUFFER_MIN];
            valueBufferLength = VALUE_BUFFER_MIN;
        };

        virtual void columnFloat(const std::string& columnName, double value) = 0;
        virtual void columnDouble(const std::string& columnName, long double value) = 0;
        virtual void columnString(const std::string& columnName) = 0;
        virtual void columnNumber(const std::string& columnName, uint64_t precision, uint64_t scale) = 0;
        virtual void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t length) = 0;
        virtual void columnRowId(const std::string& columnName, typeRowId rowId) = 0;
        virtual void columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) = 0;
        virtual void columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const char* tz) = 0;
        virtual void processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) = 0;
        virtual void processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) = 0;
        virtual void processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) = 0;
        virtual void processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const OracleTable* table, typeObj obj, typeDataObj dataObj, uint16_t type,
                                uint16_t seq, const char* sql, uint64_t sqlLength) = 0;
        virtual void processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) = 0;
        bool parseXml(const XmlCtx* xmlCtx, const uint8_t* data, uint64_t length, uint64_t offset);

    public:
        SystemTransaction* systemTransaction;
        uint64_t buffersAllocated;
        BuilderQueue* firstBuilderQueue;
        BuilderQueue* lastBuilderQueue;
        typeScn lwnScn;
        typeIdx lwnIdx;

        Builder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat, uint64_t newAttributesFormat, uint64_t newIntervalDtsFormat,
                uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat, uint64_t newTimestampFormat,
                uint64_t newTimestampTzFormat, uint64_t newTimestampAll, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newScnAll,
                uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer);
        virtual ~Builder();

        [[nodiscard]] uint64_t builderSize() const;
        [[nodiscard]] uint64_t getMaxMessageMb() const;
        void setMaxMessageMb(uint64_t maxMessageMb);
        void processBegin(typeXid xid, typeScn scn, typeScn newLwnScn, const std::unordered_map<std::string, std::string>* newAttributes);
        void processInsertMultiple(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                   const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDeleteMultiple(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                   const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDml(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                        const RedoLogRecord* redoLogRecord2, uint64_t type, bool system, bool schema, bool dump);
        void processDdlHeader(typeScn scn, typeSeq sequence, time_t timestamp, const RedoLogRecord* redoLogRecord1);
        virtual void initialize();
        virtual void processCommit(typeScn scn, typeSeq sequence, time_t timestamp) = 0;
        virtual void processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp, uint64_t offset, bool redo) = 0;
        void releaseBuffers(uint64_t maxId);
        void sleepForWriterWork(uint64_t queueSize, uint64_t nanoseconds);
        void wakeUp();

        friend class SystemTransaction;
    };
}

#endif
