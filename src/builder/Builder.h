/* Header for Builder class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/RuntimeException.h"
#include "../common/types.h"
#include "../common/typeLobId.h"
#include "../common/typeTime.h"
#include "../common/typeXid.h"
#include "../locales/CharacterSet.h"
#include "../locales/Locales.h"

#ifndef BUILDER_H_
#define BUILDER_H_

#define OUTPUT_BUFFER_DATA_SIZE                 (MEMORY_CHUNK_SIZE - sizeof(struct BuilderQueue))
#define OUTPUT_BUFFER_ALLOCATED                 0x0001
#define OUTPUT_BUFFER_CONFIRMED                 0x0002
#define VALUE_BUFFER_MIN                        1048576
#define VALUE_BUFFER_MAX                        4294967296

namespace OpenLogReplicator {
    class Ctx;
    class CharacterSet;
    class Locales;
    class OracleTable;
    class Builder;
    class Metadata;
    class SystemTransaction;

    struct BuilderQueue {
        uint64_t id;
        std::atomic<uint64_t> length;
        uint8_t* data;
        std::atomic<BuilderQueue*> next;
    };

    struct BuilderMsg {
        void* ptr;
        uint64_t id;
        uint64_t queueId;
        uint64_t length;
        typeScn scn;
        typeSeq sequence;
        uint8_t* data;
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

        uint64_t messageFormat;
        uint64_t ridFormat;
        uint64_t xidFormat;
        uint64_t timestampFormat;
        uint64_t charFormat;
        uint64_t scnFormat;
        uint64_t unknownFormat;
        uint64_t schemaFormat;
        uint64_t columnFormat;
        uint64_t unknownType;
        uint64_t unconfirmedLength;
        uint64_t messageLength;
        uint64_t flushBuffer;
        char* valueBuffer;
        uint64_t valueBufferLength;
        uint64_t valueLength;
        std::unordered_set<OracleTable*> tables;
        typeTime lastTime;
        typeScn lastScn;
        typeSeq lastSequence;
        typeXid lastXid;
        uint64_t valuesSet[MAX_NO_COLUMNS / sizeof(uint64_t)];
        uint64_t valuesMerge[MAX_NO_COLUMNS / sizeof(uint64_t)];
        int64_t lengths[MAX_NO_COLUMNS][4];
        uint8_t* values[MAX_NO_COLUMNS][4];
        uint64_t lengthsPart[3][MAX_NO_COLUMNS][4];
        uint8_t* valuesPart[3][MAX_NO_COLUMNS][4];
        uint64_t valuesMax;
        uint8_t* merges[MAX_NO_COLUMNS*4];
        uint64_t mergesMax;
        uint64_t id;
        uint64_t num;
        uint64_t maxMessageMb;      // Maximum message size able to handle by writer
        bool newTran;
        bool compressedBefore;
        bool compressedAfter;

        std::mutex mtx;
        std::condition_variable condNoWriterWork;

        double decodeFloat(const uint8_t* data);
        long double decodeDouble(const uint8_t* data);
        void builderRotate(bool copy);
        void processValue(LobCtx* lobCtx, OracleTable* table, typeCol col, const uint8_t* data, uint64_t length, bool after, bool compressed);

        void valuesRelease() {
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

        void valueSet(uint64_t type, uint16_t column, uint8_t* data, uint16_t length, uint8_t fb, bool dump) {
            if ((ctx->trace2 & TRACE2_DML) != 0 || dump) {
                std::ostringstream ss;
                ss << "DML: value: " << std::dec << type << "/" << column << "/" << std::dec << length << "/" << std::setfill('0') <<
                        std::setw(2) << std::hex << static_cast<uint64_t>(fb) << " to: ";
                for (uint64_t i = 0; i < length && i < 64; ++i) {
                    ss << "0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[i]) << ", ";
                }
                INFO(ss.str())
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

        void builderShift(uint64_t bytes, bool copy) {
            lastBuilderQueue->length += bytes;

            if (lastBuilderQueue->length >= OUTPUT_BUFFER_DATA_SIZE)
                builderRotate(copy);
        };

        void builderShiftFast(uint64_t bytes) const {
            lastBuilderQueue->length += bytes;
        };

        void builderBegin(typeObj obj) {
            messageLength = 0;

            if (lastBuilderQueue->length + sizeof(struct BuilderMsg) >= OUTPUT_BUFFER_DATA_SIZE)
                builderRotate(true);

            msg = reinterpret_cast<BuilderMsg*>(lastBuilderQueue->data + lastBuilderQueue->length);
            builderShift(sizeof(struct BuilderMsg), true);
            msg->scn = lastScn;
            msg->sequence = lastSequence;
            msg->length = 0;
            msg->id = id++;
            msg->obj = obj;
            msg->pos = 0;
            msg->flags = 0;
            msg->data = lastBuilderQueue->data + lastBuilderQueue->length;
        };

        void builderCommit(bool force) {
            if (messageLength == 0) {
                WARNING("output buffer - commit of empty transaction")
            }

            msg->queueId = lastBuilderQueue->id;
            builderShift((8 - (messageLength & 7)) & 7, false);
            unconfirmedLength += messageLength;
            msg->length = messageLength;

            if (force || flushBuffer == 0 || unconfirmedLength > flushBuffer) {
                {
                    std::unique_lock<std::mutex> lck(mtx);
                    condNoWriterWork.notify_all();
                }
                unconfirmedLength = 0;
            }
            msg = nullptr;
        };

        void builderAppend(char character) {
            lastBuilderQueue->data[lastBuilderQueue->length] = character;
            ++messageLength;
            builderShift(1, true);
        };

        void builderAppend(const char* str, uint64_t length) {
            if (lastBuilderQueue->length + length < OUTPUT_BUFFER_DATA_SIZE) {
                memcpy(reinterpret_cast<void*>(lastBuilderQueue->data + lastBuilderQueue->length),
                       reinterpret_cast<const void*>(str), length);
                lastBuilderQueue->length += length;
                messageLength += length;
            } else {
                for (uint64_t i = 0; i < length; ++i)
                    builderAppend(*str++);
            }
        };

        void builderAppend(const char* str) {
            char character = *str++;
            while (character != 0) {
                builderAppend(character);
                character = *str++;
            }
        };

        void builderAppend(const std::string& str) {
            uint64_t length = str.length();
            if (lastBuilderQueue->length + length < OUTPUT_BUFFER_DATA_SIZE) {
                memcpy(lastBuilderQueue->data + lastBuilderQueue->length,
                       reinterpret_cast<const void*>(str.c_str()), length);
                lastBuilderQueue->length += length;
                messageLength += length;
            } else {
                const char* charstr = str.c_str();
                for (uint64_t i = 0; i < length; ++i)
                    builderAppend(*charstr++);
            }
        };

        void columnUnknown(const std::string& columnName, const uint8_t* data, uint64_t length) {
            valueBuffer[0] = '?';
            valueLength = 1;
            columnString(columnName);
            if (unknownFormat == UNKNOWN_FORMAT_DUMP) {
                std::ostringstream ss;
                for (uint64_t j = 0; j < length; ++j)
                    ss << " " << std::hex << std::setfill('0') << std::setw(2) << (static_cast<uint64_t>(data[j]));
                WARNING("unknown value (column: " << columnName << "): " << std::dec << length << " - " << ss.str())
            }
        };

        void valueBufferAppend(uint8_t value) {
            valueBuffer[valueLength++] = (char)value;
        };

        void valueBufferAppendHex(typeUnicode value, uint64_t length) {
            uint64_t j = (length - 1) * 4;
            valueBufferCheck(length);
            for (uint64_t i = 0; i < length; ++i) {
                valueBuffer[valueLength++] = Ctx::map16[(value >> j) & 0xF];
                j -= 4;
            }
        };

        void parseNumber(const uint8_t* data, uint64_t length) {
            valueBufferPurge();
            valueBufferCheck(length * 2 + 2);

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
                        // Part of the total - omitting first zero for first digit
                        value = data[j] - 1;
                        if (value < 10)
                            valueBufferAppend('0' + value);
                        else {
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                        }

                        ++j;
                        --digits;

                        while (digits > 0) {
                            value = data[j] - 1;
                            if (j <= jMax) {
                                valueBufferAppend('0' + (value / 10));
                                valueBufferAppend('0' + (value % 10));
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
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                            ++j;
                        }

                        // Last digit - omitting 0 at the end
                        value = data[j] - 1;
                        valueBufferAppend('0' + (value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend('0' + (value % 10));
                    }
                // Negative number
                } else if (digits < 0x80 && jMax >= 1) {
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
                            valueBufferAppend('0' + value);
                        else {
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                        }
                        ++j;
                        --digits;

                        while (digits > 0) {
                            if (j <= jMax) {
                                value = 101 - data[j];
                                valueBufferAppend('0' + (value / 10));
                                valueBufferAppend('0' + (value % 10));
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
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                            ++j;
                        }

                        value = 101 - data[j];
                        valueBufferAppend('0' + (value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend('0' + (value % 10));
                    }
                } else
                    throw RuntimeException("got unknown numeric value");
            }
        };

        std::string dumpLob(const uint8_t* data, uint64_t length) {
            std::ostringstream ss;
            for (uint64_t j = 0; j < length; ++j) {
                ss << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]);
            }
            return ss.str();
        }

        void addLobToOutput(const uint8_t* data, uint32_t length, uint64_t charsetId, bool append, bool isClob) {
            if (isClob) {
                parseString(data, length, charsetId, append);
            } else {
                memcpy(reinterpret_cast<void*>(valueBuffer + valueLength),
                       reinterpret_cast<const void*>(data), length);
                valueLength += length;
            };
        }

        void parseLob(LobCtx* lobCtx, const uint8_t* data, uint64_t length, uint64_t charsetId, typeDataObj dataObj, bool isClob) {
            bool append = false;
            valueLength = 0;

            if (length < 20) {
                WARNING("incorrect LOB data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " << std::dec << dataObj <<
                        " - too short")
                WARNING("dump LOB data: " << dumpLob(data, length))
                return;
            }

            uint32_t flags = data[5];
            typeLobId lobId(data + 10);
            lobCtx->checkOrphanedLobs(ctx, lobId, lastXid);

            // in-index
            if ((flags & 0x04) == 0) {
                auto lobsIt = lobCtx->lobs.find(lobId);
                if (lobsIt == lobCtx->lobs.end()) {
                    DEBUG("missing index1 xid: " << lastXid << " LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                    return;
                }
                LobData* lobData = lobsIt->second;
                valueBufferCheck(static_cast<uint64_t>(lobData->pageSize) * static_cast<uint64_t>(lobData->sizePages) + lobData->sizeRest);

                uint32_t pageNo = 0;
                for (auto indexMapIt: lobData->indexMap) {
                    uint32_t pageNoLob = indexMapIt.first;
                    typeDba page = indexMapIt.second;
                    if (pageNo != pageNoLob) {
                        WARNING("xid: " << lastXid << " LOB: " << lobId.upper() << " incorrect page: " << std::dec << pageNoLob << " while expected: " <<
                                pageNo << " data: " << dumpLob(data, length) << " dataobj: " << std::dec << dataObj)
                        pageNo = pageNoLob;
                    }

                    auto dataMapIt = lobData->dataMap.find(page);
                    if (dataMapIt == lobData->dataMap.end()) {
                        WARNING("missing LOB (in-index) for xid: " << lastXid << " LOB: " << lobId.upper() + " page: " + std::to_string(page) <<
                                " dataobj: " << std::dec << dataObj)
                        WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                        return;
                    }
                    uint32_t chunkLength = lobData->pageSize;
                    if (pageNo == lobData->sizePages)
                        chunkLength = lobData->sizeRest;

                    valueBufferCheck(chunkLength * 4);
                    RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                    redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));

                    addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, append, isClob);
                    append = true;
                    ++pageNo;
                }
            // in-row
            } else {
                if (length < 23) {
                    WARNING("incorrect LOB (in-value) data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " << std::dec <<
                            dataObj << " - too short")
                    WARNING("dump LOB data: " << dumpLob(data, length))
                    return;
                }
                uint16_t bodyLength = ctx->read16Big(data + 20);
                if (length != static_cast<uint64_t>(bodyLength + 20)) {
                    WARNING("incorrect LOB (in-value) xid: " << lastXid << " dataobj: " << std::dec << dataObj)
                    WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                    return;
                }
                uint16_t flg2 = ctx->read16Big(data + 22);

                uint32_t totalLobLength = 0;
                uint16_t chunkLength;
                uint64_t dataOffset;

                // in-index
                if ((flg2 & 0x0400) == 0x0400) {
                    if (length < 36) {
                        WARNING("incorrect LOB (in-index1) data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " << std::dec <<
                                dataObj << " - too short")
                        WARNING("dump LOB data: " << dumpLob(data, length))
                        return;
                    }
                    uint32_t pageCnt = ctx->read32Big(data + 24);
                    uint16_t sizeRest = ctx->read16Big(data + 28);
                    dataOffset = 36;

                    auto lobsIt = lobCtx->lobs.find(lobId);
                    if (lobsIt == lobCtx->lobs.end()) {
                        WARNING("missing LOB (in-index) for xid: " << lastXid << " dataobj: " << std::dec << dataObj)
                        WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                        return;
                    }
                    LobData *lobData = lobsIt->second;
                    totalLobLength = pageCnt * lobData->pageSize + sizeRest;
                    uint64_t jMax = pageCnt;
                    if (sizeRest > 0)
                        ++jMax;

                    for (uint64_t j = 0; j < jMax; ++j) {
                        typeDba page = 0;
                        if (dataOffset < length) {
                            if (length < dataOffset + 4) {
                                WARNING("incorrect LOB (in-index2) data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " <<
                                                                               std::dec << dataObj << " - too short")
                                WARNING("dump LOB data: " << dumpLob(data, length))
                                return;
                            }
                            page = ctx->read32Big(data + dataOffset);
                        } else {
                            // rest of data in LOB index
                            auto indexMapIt = lobData->indexMap.find(j);
                            if (indexMapIt == lobData->indexMap.end()) {
                                WARNING("can't find page " << std::dec << j << " for xid: " << lastXid << " LOB: " << lobId.upper() << " dataobj: " <<
                                        std::dec << dataObj)
                                break;
                            }
                            page = indexMapIt->second;
                        }

                        auto dataMapIt = lobData->dataMap.find(page);
                        if (dataMapIt == lobData->dataMap.end()) {
                            WARNING("missing LOB index (in-index) for xid: " << lastXid << " LOB: " << lobId.upper() << " page: " <<
                                    std::to_string(page) << " dataobj: " << std::dec << dataObj)
                            WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                            return;
                        }

                        valueBufferCheck(lobData->pageSize * 4);
                        //uint64_t redoLogRecordLength = *(reinterpret_cast<uint64_t*>(dataMapIt->second));
                        RedoLogRecord *redoLogRecordLob = reinterpret_cast<RedoLogRecord *>(dataMapIt->second + sizeof(uint64_t));
                        redoLogRecordLob->data = reinterpret_cast<uint8_t *>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));
                        if (j < pageCnt)
                            chunkLength = redoLogRecordLob->lobDataLength;
                        else
                            chunkLength = sizeRest;

                        addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, append, isClob);
                        append = true;
                        ++page;
                        totalLobLength -= chunkLength;
                        dataOffset += 4;
                    }
                // in-value
                } else if ((flg2 & 0x0100) == 0x0100) {
                    if (bodyLength < 16) {
                        WARNING("incorrect LOB (old in-value) xid: " << lastXid << " bodyLength: " << std::dec << bodyLength << " dataobj: " << std::dec <<
                                dataObj)
                        WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                        return;
                    }

                    if (length < 34) {
                        WARNING("incorrect LOB (in-value) data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " << std::dec <<
                                dataObj << " - too short")
                        WARNING("dump LOB data: " << dumpLob(data, length))
                        return;
                    }
                    uint32_t zero1 = ctx->read32Big(data + 24);
                    chunkLength = ctx->read16Big(data + 28);
                    uint32_t zero2 = ctx->read32Big(data + 30);

                    if (zero1 != 0 || zero2 != 0 || chunkLength + 16  != bodyLength) {
                        WARNING("incorrect LOB (old in-value) xid: " << lastXid << " length: " << std::dec << chunkLength << " " << bodyLength <<
                                " data: " << zero1 << " " << zero2 << " dataobj: " << std::dec << dataObj)
                        WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                        return;
                    }

                    if (chunkLength == 0) {
                        //null value
                    } else {
                        if (length < static_cast<uint64_t>(chunkLength) + 36) {
                            WARNING("incorrect LOB (read value) data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " <<
                                    std::dec << dataObj << " - too short")
                            WARNING("dump LOB data: " << dumpLob(data, length))
                            return;
                        }

                        addLobToOutput(data + 36, chunkLength, charsetId, false, isClob);
                    }
                } else {
                    if (bodyLength < 10) {
                        WARNING("incorrect LOB (new in-value) xid: " << lastXid << " bodyLength: " << std::dec << bodyLength << " dataobj: " <<
                                std::dec << dataObj)
                        WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                        return;
                    }

                    if (length < 32) {
                        WARNING("incorrect LOB (read value) data xid: " << lastXid << " length: " << std::to_string(length) << " dataobj: " <<
                                std::dec << dataObj << " - too short")
                        WARNING("dump LOB data: " << dumpLob(data, length))
                        return;
                    }

                    uint8_t flg3 = data[26];
                    if ((flg3 & 0x03) == 0) {
                        totalLobLength = data[28];
                        dataOffset = 30;
                    } else if ((flg3 & 0x03) == 1) {
                        totalLobLength = ctx->read16Big(data + 28);
                        dataOffset = 31;
                    } else if ((flg3 & 0x03) == 2) {
                        totalLobLength = ctx->read24Big(data + 28);
                        dataOffset = 32;
                    } else if ((flg3 & 0x03) == 3) {
                        totalLobLength = ctx->read32Big(data + 28);
                        dataOffset = 33;
                    } else {
                        WARNING("incorrect LOB (new in-value) xid: " << lastXid << " flag: " << std::dec << static_cast<uint64_t>(flg3) << " dataobj: " <<
                                std::dec << dataObj)
                        WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                        return;
                    }

                    //uint16_t sizeRest = ctx->read16Big(data + 24);

                    // data
                    if ((flg2 & 0x0800) == 0x0800) {
                        chunkLength = totalLobLength;
                        if (chunkLength == 0) {
                            //null value
                        } else {
                            if (dataOffset + chunkLength < length) {
                                WARNING("incorrect LOB (read value data) data xid: " << lastXid << " length: " << std::to_string(length) <<
                                        " dataobj: " << std::dec << dataObj << " - too short")
                                WARNING("dump LOB data: " << dumpLob(data, length))
                                return;
                            }

                            addLobToOutput(data + dataOffset, chunkLength, charsetId, false, isClob);
                            totalLobLength -= chunkLength;
                        }
                    // index
                    } else {
                        if (dataOffset + 1 < length) {
                            WARNING("incorrect LOB (read value index1) data xid: " << lastXid << " length: " << std::to_string(length) <<
                                                                                 " dataobj: " << std::dec << dataObj << " - too short")
                            WARNING("dump LOB data: " << dumpLob(data, length))
                            return;
                        }

                        uint8_t lobPages = data[dataOffset++] + 1;

                        auto lobsIt = lobCtx->lobs.find(lobId);
                        if (lobsIt == lobCtx->lobs.end()) {
                            WARNING("missing LOB index (new in-value) for xid: " << lastXid << " LOB: " << lobId.upper() << " dataobj: " << std::dec <<
                                    dataObj)
                            WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                            return;
                        }
                        LobData* lobData = lobsIt->second;

                        for (uint64_t i = 0; i < lobPages; ++i) {
                            if (dataOffset + 5 < length) {
                                WARNING("incorrect LOB (read value index2) data xid: " << lastXid << " length: " << std::to_string(length) <<
                                        " dataobj: " << std::dec << dataObj << " - too short")
                                WARNING("dump LOB data: " << dumpLob(data, length))
                                return;
                            }

                            uint8_t flg4 = data[dataOffset++];
                            typeDba page = ctx->read32Big(data + dataOffset);
                            dataOffset += 4;

                            uint64_t pageCnt = 0;
                            if ((flg4 & 0xF0) == 0x00) {
                                if (dataOffset + 1 < length) {
                                    WARNING("incorrect LOB (read value index3) data xid: " << lastXid << " length: " << std::to_string(length) <<
                                            " dataobj: " << std::dec << dataObj << " - too short")
                                    WARNING("dump LOB data: " << dumpLob(data, length))
                                    return;
                                }
                                pageCnt = data[dataOffset++];
                            } else if ((flg4 & 0xF0) == 0x20) {
                                if (dataOffset + 2 < length) {
                                    WARNING("incorrect LOB (read value index4) data xid: " << lastXid << " length: " << std::to_string(length) <<
                                            " dataobj: " << std::dec << dataObj << " - too short")
                                    WARNING("dump LOB data: " << dumpLob(data, length))
                                    return;
                                }
                                pageCnt = ctx->read16Big(data + dataOffset);
                                dataOffset += 2;
                            } else {
                                WARNING("incorrect LOB (new in-value) xid: " << lastXid << " dataobj: " << std::dec << dataObj << " page: 0x" <<
                                        std::hex << page << " offset: " << std::dec << dataOffset)
                                WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                                return;
                            }

                            for (uint64_t j = 0; j < pageCnt; ++j) {
                                auto dataMapIt = lobData->dataMap.find(page);
                                if (dataMapIt == lobData->dataMap.end()) {
                                    WARNING("missing LOB data (new in-value) for xid: " << lastXid << " LOB: " << lobId.upper() << " page: " <<
                                            std::to_string(page) << " dataobj: " << std::dec << dataObj)
                                    WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                                    return;
                                }

                                valueBufferCheck(lobData->pageSize * 4);
                                RedoLogRecord *redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(dataMapIt->second + sizeof(uint64_t));
                                redoLogRecordLob->data = reinterpret_cast<uint8_t*>(dataMapIt->second + sizeof(uint64_t) + sizeof(RedoLogRecord));
                                chunkLength = redoLogRecordLob->lobDataLength;

                                addLobToOutput(redoLogRecordLob->data + redoLogRecordLob->lobData, chunkLength, charsetId, append, isClob);
                                append = true;
                                ++page;
                                totalLobLength -= chunkLength;
                            }
                        }
                    }
                }

                if (totalLobLength != 0) {
                    WARNING("incorrect LOB sum xid: " << lastXid << " left: " << std::dec << totalLobLength << " dataobj: " << std::dec << dataObj)
                    WARNING("dump LOB: " << lobId.upper() << " data: " << dumpLob(data, length))
                }
            }
        }

        void parseString(const uint8_t* data, uint64_t length, uint64_t charsetId, bool append) {
            CharacterSet* characterSet = locales->characterMap[charsetId];
            if (characterSet == nullptr && (charFormat & CHAR_FORMAT_NOMAPPING) == 0)
                throw RuntimeException("can't find character set map for id = " + std::to_string(charsetId));
            if (!append)
                valueBufferPurge();

            while (length > 0) {
                typeUnicode unicodeCharacter;
                uint64_t unicodeCharacterLength;

                if ((charFormat & CHAR_FORMAT_NOMAPPING) == 0) {
                    unicodeCharacter = characterSet->decode(lastXid, data, length);
                    unicodeCharacterLength = 8;
                } else {
                    unicodeCharacter = *data++;
                    --length;
                    unicodeCharacterLength = 2;
                }

                if ((charFormat & CHAR_FORMAT_HEX) != 0) {
                    valueBufferAppendHex(unicodeCharacter, unicodeCharacterLength);
                } else {
                    // 0xxxxxxx
                    if (unicodeCharacter <= 0x7F) {
                        valueBufferAppend(unicodeCharacter);

                    // 110xxxxx 10xxxxxx
                    } else if (unicodeCharacter <= 0x7FF) {
                        valueBufferAppend(0xC0 | static_cast<uint8_t>(unicodeCharacter >> 6));
                        valueBufferAppend(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F));

                    // 1110xxxx 10xxxxxx 10xxxxxx
                    } else if (unicodeCharacter <= 0xFFFF) {
                        valueBufferAppend(0xE0 | static_cast<uint8_t>(unicodeCharacter >> 12));
                        valueBufferAppend(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F));
                        valueBufferAppend(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F));

                    // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                    } else if (unicodeCharacter <= 0x10FFFF) {
                        valueBufferAppend(0xF0 | static_cast<uint8_t>(unicodeCharacter >> 18));
                        valueBufferAppend(0x80 | static_cast<uint8_t>((unicodeCharacter >> 12) & 0x3F));
                        valueBufferAppend(0x80 | static_cast<uint8_t>((unicodeCharacter >> 6) & 0x3F));
                        valueBufferAppend(0x80 | static_cast<uint8_t>(unicodeCharacter & 0x3F));

                    } else
                        throw RuntimeException("got character code: U+" + std::to_string(unicodeCharacter));
                }
            }
        };

        void valueBufferCheck(uint64_t length) {
            if (valueLength + length > VALUE_BUFFER_MAX)
                throw RuntimeException("trying to allocate length for value: " + std::to_string(valueLength + length));

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

        void valueBufferPurge() {
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
        virtual void columnTimestamp(const std::string& columnName, struct tm &time_, uint64_t fraction, const char* tz) = 0;
        virtual void processInsert(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid) = 0;
        virtual void processUpdate(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid) = 0;
        virtual void processDelete(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid) = 0;
        virtual void processDdl(OracleTable* table, typeDataObj dataObj, uint16_t type, uint16_t seq, const char* operation, const char* sql,
                                uint64_t sqlLength) = 0;
        virtual void processBeginMessage() = 0;

    public:
        SystemTransaction* systemTransaction;
        uint64_t buffersAllocated;
        BuilderQueue* firstBuilderQueue;
        BuilderQueue* lastBuilderQueue;

        Builder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat,
                uint64_t newTimestampFormat, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newUnknownFormat, uint64_t newSchemaFormat,
                uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer);
        virtual ~Builder();

        [[nodiscard]] uint64_t builderSize() const;
        [[nodiscard]] uint64_t getMaxMessageMb() const;
        void setMaxMessageMb(uint64_t maxMessageMb);
        void processBegin(typeScn scn, typeTime time_, typeSeq sequence, typeXid xid);
        void processInsertMultiple(LobCtx* lobCtx, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDeleteMultiple(LobCtx* lobCtx, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump);
        void processDml(LobCtx* lobCtx, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2, uint64_t type, bool system, bool schema, bool dump);
        void processDdlHeader(RedoLogRecord* redoLogRecord1);
        virtual void initialize();
        virtual void processCommit() = 0;
        virtual void processCheckpoint(typeScn scn, typeTime time_, typeSeq sequence, uint64_t offset, bool redo) = 0;
        void releaseBuffers(uint64_t maxId);
        void sleepForWriterWork(uint64_t queueSize, uint64_t nanoseconds);
        void wakeUp();

        friend class SystemTransaction;
    };
}

#endif
