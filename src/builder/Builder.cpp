/* Memory buffer for handling output data
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

#include "../common/OracleColumn.h"
#include "../common/OracleTable.h"
#include "../common/RedoLogRecord.h"
#include "../common/SysCol.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "Builder.h"
#include "SystemTransaction.h"

namespace OpenLogReplicator {

    Builder::Builder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat,  uint64_t newIntervalDtsFormat,
                     uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat, uint64_t newTimestampFormat,
                     uint64_t newTimestampTzFormat, uint64_t newTimestampAll, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newScnAll,
                     uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer) :
            ctx(newCtx),
            locales(newLocales),
            metadata(newMetadata),
            msg(nullptr),
            dbFormat(newDbFormat),
            intervalDtsFormat(newIntervalDtsFormat),
            intervalYtmFormat(newIntervalYtmFormat),
            messageFormat(newMessageFormat),
            ridFormat(newRidFormat),
            xidFormat(newXidFormat),
            timestampFormat(newTimestampFormat),
            timestampTzFormat(newTimestampTzFormat),
            timestampAll(newTimestampAll),
            charFormat(newCharFormat),
            scnFormat(newScnFormat),
            scnAll(newScnAll),
            unknownFormat(newUnknownFormat),
            schemaFormat(newSchemaFormat),
            columnFormat(newColumnFormat),
            unknownType(newUnknownType),
            unconfirmedLength(0),
            messageLength(0),
            flushBuffer(newFlushBuffer),
            valueBuffer(nullptr),
            valueBufferLength(0),
            valueLength(0),
            commitScn(ZERO_SCN),
            lastXid(typeXid()),
            valuesMax(0),
            mergesMax(0),
            id(0),
            num(0),
            maxMessageMb(0),
            newTran(false),
            compressedBefore(false),
            compressedAfter(false),
            prevCharsSize(0),
            systemTransaction(nullptr),
            buffersAllocated(0),
            firstBuilderQueue(nullptr),
            lastBuilderQueue(nullptr) {
        memset(reinterpret_cast<void*>(valuesSet), 0, sizeof(valuesSet));
        memset(reinterpret_cast<void*>(valuesMerge), 0, sizeof(valuesMerge));
        memset(reinterpret_cast<void*>(values), 0, sizeof(values));
        memset(reinterpret_cast<void*>(valuesPart), 0, sizeof(valuesPart));
    }

    Builder::~Builder() {
        valuesRelease();
        tables.clear();

        while (firstBuilderQueue != nullptr) {
            BuilderQueue* nextBuffer = firstBuilderQueue->next;
            ctx->freeMemoryChunk("builder", reinterpret_cast<uint8_t*>(firstBuilderQueue), true);
            firstBuilderQueue = nextBuffer;
            --buffersAllocated;
        }

        if (systemTransaction != nullptr) {
            delete systemTransaction;
            systemTransaction = nullptr;
        }

        if (valueBuffer != nullptr) {
            delete[] valueBuffer;
            valueBuffer = nullptr;
        }
    }

    void Builder::initialize() {
        buffersAllocated = 1;
        firstBuilderQueue = reinterpret_cast<BuilderQueue*>(ctx->getMemoryChunk("builder", true));
        firstBuilderQueue->id = 0;
        firstBuilderQueue->next = nullptr;
        firstBuilderQueue->data = reinterpret_cast<uint8_t*>(firstBuilderQueue) + sizeof(struct BuilderQueue);
        firstBuilderQueue->length = 0;
        lastBuilderQueue = firstBuilderQueue;

        valueBuffer = new char[VALUE_BUFFER_MIN];
        valueBufferLength = VALUE_BUFFER_MIN;
    }

    void Builder::processValue(LobCtx* lobCtx, OracleTable* table, typeCol col, const uint8_t* data, uint64_t length, uint64_t offset, bool after,
                               bool compressed) {
        if (compressed) {
            std::string columnName("COMPRESSED");
            columnRaw(columnName, data, length);
            return;
        }
        if (table == nullptr) {
            std::string columnName("COL_" + std::to_string(col));
            columnRaw(columnName, data, length);
            return;
        }
        OracleColumn* column = table->columns[col];
        if (FLAG(REDO_FLAGS_RAW_COLUMN_DATA)) {
            columnRaw(column->name, data, length);
            return;
        }
        if (column->guard && !FLAG(REDO_FLAGS_SHOW_GUARD_COLUMNS))
            return;
        if (column->nested && !FLAG(REDO_FLAGS_SHOW_NESTED_COLUMNS))
            return;
        if (column->hidden && !FLAG(REDO_FLAGS_SHOW_HIDDEN_COLUMNS))
            return;
        if (column->unused && !FLAG(REDO_FLAGS_SHOW_UNUSED_COLUMNS))
            return;

        if (length == 0)
            throw RedoLogException(50013, "trying to output null data for column: " + column->name + ", offset: " +
                                   std::to_string(offset));

        if (column->storedAsLob) {
            // VARCHAR2 stored as CLOB
            if (column->type == SYS_COL_TYPE_VARCHAR)
                column->type = SYS_COL_TYPE_CLOB;
            // RAW stored as BLOB
            else if (column->type == SYS_COL_TYPE_RAW)
                column->type = SYS_COL_TYPE_BLOB;
        }

        switch (column->type) {
        case SYS_COL_TYPE_VARCHAR:
        case SYS_COL_TYPE_CHAR:
            parseString(data, length, column->charsetId, offset, false, false, false, table->systemTable > 0);
            columnString(column->name);
            break;

        case SYS_COL_TYPE_NUMBER:
            parseNumber(data, length, offset);
            columnNumber(column->name, column->precision, column->scale);
            break;

        case SYS_COL_TYPE_BLOB:
            if (after && table != nullptr) {
                if (parseLob(lobCtx, data, length, 0, table->obj, offset, false, table->sys))
                    columnRaw(column->name, reinterpret_cast<uint8_t*>(valueBuffer), valueLength);
            }
            break;

        case SYS_COL_TYPE_CLOB:
            if (after && table != nullptr) {
                if (parseLob(lobCtx, data, length, column->charsetId, table->obj, offset, true, table->systemTable > 0))
                    columnString(column->name);
            }
            break;

        case SYS_COL_TYPE_DATE:
        case SYS_COL_TYPE_TIMESTAMP:
        case SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ:
            if (length != 7 && length != 11)
                columnUnknown(column->name, data, length);
            else {
                struct tm epochTime = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        nullptr};
                epochTime.tm_sec = data[6] - 1;  // 0..59
                epochTime.tm_min = data[5] - 1;  // 0..59
                epochTime.tm_hour = data[4] - 1; // 0..23
                epochTime.tm_mday = data[3];     // 1..31
                epochTime.tm_mon = data[2];      // 1..12

                int val1 = data[0];
                int val2 = data[1];
                // AD
                if (val1 >= 100 && val2 >= 100) {
                    val1 -= 100;
                    val2 -= 100;
                    epochTime.tm_year = val1 * 100 + val2;

                } else {
                    val1 = 100 - val1;
                    val2 = 100 - val2;
                    epochTime.tm_year = - (val1 * 100 + val2);
                }

                uint64_t fraction = 0;
                if (length == 11)
                    fraction = Ctx::read32Big(data + 7);

                if (epochTime.tm_sec < 0 || epochTime.tm_sec > 59 ||
                    epochTime.tm_min < 0 || epochTime.tm_min > 59 ||
                    epochTime.tm_hour < 0 || epochTime.tm_hour > 23 ||
                    epochTime.tm_mday < 1 || epochTime.tm_mday > 31 ||
                    epochTime.tm_mon < 1 || epochTime.tm_mon > 12) {
                    columnUnknown(column->name, data, length);
                } else {
                    columnTimestamp(column->name, epochTime, fraction);
                }
            }
            break;

        case SYS_COL_TYPE_RAW:
            columnRaw(column->name, data, length);
            break;

        case SYS_COL_TYPE_FLOAT:
            if (length == 4)
                columnFloat(column->name, decodeFloat(data));
            else
                columnUnknown(column->name, data, length);
            break;

        case SYS_COL_TYPE_DOUBLE:
            if (length == 8)
                columnDouble(column->name, decodeDouble(data));
            else
                columnUnknown(column->name, data, length);
            break;

        case SYS_COL_TYPE_TIMESTAMP_WITH_TZ:
            if (length != 9 && length != 13) {
                columnUnknown(column->name, data, length);
            } else {
                struct tm epochTime = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        nullptr};
                epochTime.tm_sec = data[6] - 1;  // 0..59
                epochTime.tm_min = data[5] - 1;  // 0..59
                epochTime.tm_hour = data[4] - 1; // 0..23
                epochTime.tm_mday = data[3];     // 1..31
                epochTime.tm_mon = data[2];      // 1..12

                int val1 = data[0];
                int val2 = data[1];
                // AD
                if (val1 >= 100 && val2 >= 100) {
                    val1 -= 100;
                    val2 -= 100;
                    epochTime.tm_year = val1 * 100 + val2;

                } else {
                    val1 = 100 - val1;
                    val2 = 100 - val2;
                    epochTime.tm_year = - (val1 * 100 + val2);
                }

                uint64_t fraction = 0;
                if (length == 13)
                    fraction = Ctx::read32Big(data + 7);

                const char* tz = nullptr;
                char tz2[7];

                if (data[11] >= 5 && data[11] <= 36) {
                    if (data[11] < 20 ||
                            (data[11] == 20 && data[12] < 60))
                        tz2[0] = '-';
                    else
                        tz2[0] = '+';

                    if (data[11] < 20) {
                        uint64_t val = 20 - data[11];
                        tz2[1] = ctx->map10[val / 10];
                        tz2[2] = ctx->map10[val % 10];
                    } else {
                        uint64_t val = data[11] - 20;
                        tz2[1] = ctx->map10[val / 10];
                        tz2[2] = ctx->map10[val % 10];
                    }

                    tz2[3] = ':';

                    if (data[12] < 60) {
                        uint64_t val = 60 - data[12];
                        tz2[4] = ctx->map10[val / 10];
                        tz2[5] = ctx->map10[val % 10];
                    } else {
                        uint64_t val = data[12] - 60;
                        tz2[4] = ctx->map10[val / 10];
                        tz2[5] = ctx->map10[val % 10];
                    }
                    tz2[6] = 0;
                    tz = tz2;
                } else {
                    uint16_t tzkey = (data[11] << 8) | data[12];
                    auto timeZoneMapIt = locales->timeZoneMap.find(tzkey);
                    if (timeZoneMapIt != locales->timeZoneMap.end())
                        tz = timeZoneMapIt->second;
                    else
                        tz = "TZ?";
                }

                if (epochTime.tm_sec < 0 || epochTime.tm_sec > 59 ||
                    epochTime.tm_min < 0 || epochTime.tm_min > 59 ||
                    epochTime.tm_hour < 0 || epochTime.tm_hour > 23 ||
                    epochTime.tm_mday < 1 || epochTime.tm_mday > 31 ||
                    epochTime.tm_mon < 1 || epochTime.tm_mon > 12) {
                    columnUnknown(column->name, data, length);
                } else {
                    columnTimestampTz(column->name, epochTime, fraction, tz);
                }
            }
            break;

        case SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH:
            if (length != 5 || data[4] < 49 || data[4] > 71)
                columnUnknown(column->name, data, length);
            else {
                bool minus = false;
                uint64_t year = 0;
                if ((data[0] & 0x80) != 0)
                    year = Ctx::read32Big(data) - 0x80000000;
                else {
                    year = 0x80000000 - Ctx::read32Big(data);
                    minus = true;
                }

                if (year > 999999999)
                    columnUnknown(column->name, data, length);
                else {
                    uint64_t month = 0;
                    if (data[4] >= 60)
                        month = data[4] - 60;
                    else {
                        month = 60 - data[4];
                        minus = true;
                    }

                    char buffer[12];
                    uint64_t val = 0;
                    uint64_t len = 0;
                    valueLength = 0;

                    if (minus)
                        valueBuffer[valueLength++] = '-';

                    if (intervalYtmFormat == INTERVAL_YTM_FORMAT_MONTHS || intervalYtmFormat == INTERVAL_YTM_FORMAT_MONTHS_STRING) {
                        val = year * 12 + month;
                        if (val == 0) {
                            valueBuffer[valueLength++] = '0';
                        } else {
                            while (val) {
                                buffer[len++] = ctx->map10[val % 10];
                                val /= 10;
                            }
                            while (len > 0)
                                valueBuffer[valueLength++] = buffer[--len];
                        }

                        if (intervalYtmFormat == INTERVAL_YTM_FORMAT_MONTHS)
                            columnNumber(column->name, 17, 0);
                        else
                            columnString(column->name);
                    } else {
                        val = year;
                        if (val == 0) {
                            valueBuffer[valueLength++] = '0';
                        } else {
                            while (val) {
                                buffer[len++] = ctx->map10[val % 10];
                                val /= 10;
                            }
                            while (len > 0)
                                valueBuffer[valueLength++] = buffer[--len];
                        }

                        if (intervalYtmFormat == INTERVAL_YTM_FORMAT_STRING_YM_SPACE)
                            valueBuffer[valueLength++] = ' ';
                        else if (intervalYtmFormat == INTERVAL_YTM_FORMAT_STRING_YM_COMMA)
                            valueBuffer[valueLength++] = ',';
                        else if (intervalYtmFormat == INTERVAL_YTM_FORMAT_STRING_YM_DASH)
                            valueBuffer[valueLength++] = '-';

                        if (month >= 10) {
                            valueBuffer[valueLength++] = '1';
                            valueBuffer[valueLength++] = ctx->map10[month - 10];
                        } else
                            valueBuffer[valueLength++] = ctx->map10[month];

                        columnString(column->name);
                    }
                }
            }
            break;

        case SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND:
            if (length != 11 || data[4] < 37 || data[4] > 83 || data[5] < 1 || data[5] > 119 || data[6] < 1 || data[6] > 119)
                columnUnknown(column->name, data, length);
            else {
                bool minus = false;
                uint64_t day = 0;
                if ((data[0] & 0x80) != 0)
                    day = Ctx::read32Big(data) - 0x80000000;
                else {
                    day = 0x80000000 - Ctx::read32Big(data);
                    minus = true;
                }

                int32_t us = 0;
                if ((data[7] & 0x80) != 0)
                    us = Ctx::read32Big(data + 7) - 0x80000000;
                else {
                    us = 0x80000000 - Ctx::read32Big(data + 7);
                    minus = true;
                }

                if (day > 999999999 || us > 999999999)
                    columnUnknown(column->name, data, length);
                else {
                    uint64_t hour = 0;
                    if (data[4] >= 60)
                        hour = data[4] - 60;
                    else {
                        hour = 60 - data[4];
                        minus = true;
                    }

                    uint64_t minute = 0;
                    if (data[5] >= 60)
                        minute = data[5] - 60;
                    else {
                        minute = 60 - data[5];
                        minus = true;
                    }

                    uint64_t second = 0;
                    if (data[6] >= 60)
                        second = data[6] - 60;
                    else {
                        second = 60 - data[6];
                        minus = true;
                    }

                    char buffer[30];
                    valueLength = 0;
                    uint64_t val = 0;
                    uint64_t len = 0;

                    if (minus)
                        valueBuffer[valueLength++] = '-';

                    if (intervalDtsFormat == INTERVAL_DTS_FORMAT_ISO8601_SPACE || intervalDtsFormat == INTERVAL_DTS_FORMAT_ISO8601_COMMA ||
                            intervalDtsFormat == INTERVAL_DTS_FORMAT_ISO8601_DASH) {
                        val = day;
                        if (day == 0) {
                            valueBuffer[valueLength++] = '0';
                        } else {
                            while (val) {
                                buffer[len++] = ctx->map10[val % 10];
                                val /= 10;
                            }
                            while (len > 0)
                                valueBuffer[valueLength++] = buffer[--len];
                        }

                        if (intervalDtsFormat == INTERVAL_DTS_FORMAT_ISO8601_SPACE)
                            valueBuffer[valueLength++] = ' ';
                        else if (intervalDtsFormat == INTERVAL_DTS_FORMAT_ISO8601_COMMA)
                            valueBuffer[valueLength++] = ',';
                        else if (intervalDtsFormat == INTERVAL_DTS_FORMAT_ISO8601_DASH)
                            valueBuffer[valueLength++] = '-';

                        valueBuffer[valueLength++] = ctx->map10[hour / 10];
                        valueBuffer[valueLength++] = ctx->map10[hour % 10];
                        valueBuffer[valueLength++] = ':';
                        valueBuffer[valueLength++] = ctx->map10[minute / 10];
                        valueBuffer[valueLength++] = ctx->map10[minute % 10];
                        valueBuffer[valueLength++] = ':';
                        valueBuffer[valueLength++] = ctx->map10[second / 10];
                        valueBuffer[valueLength++] = ctx->map10[second % 10];
                        valueBuffer[valueLength++] = '.';

                        for (uint64_t j = 0; j < 9; ++j) {
                            valueBuffer[valueLength + 8 - j] = ctx->map10[us % 10];
                            us /= 10;
                        }
                        valueLength += 9;

                        columnString(column->name);
                    } else {
                        switch (intervalDtsFormat) {
                            case INTERVAL_DTS_FORMAT_UNIX_NANO:
                            case INTERVAL_DTS_FORMAT_UNIX_NANO_STRING:
                                val = (((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us;
                                break;

                            case INTERVAL_DTS_FORMAT_UNIX_MICRO:
                            case INTERVAL_DTS_FORMAT_UNIX_MICRO_STRING:
                                val = ((((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us + 500) / 1000;
                                break;

                            case INTERVAL_DTS_FORMAT_UNIX_MILLI:
                            case INTERVAL_DTS_FORMAT_UNIX_MILLI_STRING:
                                val = ((((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us + 500000) / 1000000;
                                break;

                            case INTERVAL_DTS_FORMAT_UNIX:
                            case INTERVAL_DTS_FORMAT_UNIX_STRING:
                                val = ((((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us + 500000000) / 1000000000;
                        }

                        if (val == 0) {
                            valueBuffer[valueLength++] = '0';
                        } else {
                            while (val) {
                                buffer[len++] = ctx->map10[val % 10];
                                val /= 10;
                            }
                            while (len > 0)
                                valueBuffer[valueLength++] = buffer[--len];
                        }

                        switch (intervalDtsFormat) {
                            case INTERVAL_DTS_FORMAT_UNIX_NANO:
                            case INTERVAL_DTS_FORMAT_UNIX_MICRO:
                            case INTERVAL_DTS_FORMAT_UNIX_MILLI:
                            case INTERVAL_DTS_FORMAT_UNIX:
                                columnNumber(column->name, 17, 0);
                                break;

                            case INTERVAL_DTS_FORMAT_UNIX_NANO_STRING:
                            case INTERVAL_DTS_FORMAT_UNIX_MICRO_STRING:
                            case INTERVAL_DTS_FORMAT_UNIX_MILLI_STRING:
                            case INTERVAL_DTS_FORMAT_UNIX_STRING:
                                columnString(column->name);
                        }
                    }
                }
            }
            break;

        case SYS_COL_TYPE_UROWID:
            if (length == 13 && data[0] == 0x01) {
                typeRowId rowId;
                rowId.decodeFromHex(data + 1);
                columnRowId(column->name, rowId);
            } else {
                columnUnknown(column->name, data, length);
            }
            break;

        default:
            if (unknownType == UNKNOWN_TYPE_SHOW)
                columnUnknown(column->name, data, length);
        }
    }

    double Builder::decodeFloat(const uint8_t* data) {
        uint8_t sign = data[0] & 0x80;
        int64_t exponent = (static_cast<uint64_t>(data[0] & 0x7F) << 1) | (static_cast<uint64_t>(data[1]) >> 7);
        uint64_t significand = (static_cast<uint64_t>(data[1] & 0x7F) << 16) | (static_cast<uint64_t>(data[2]) << 8) | static_cast<uint64_t>(data[3]);

        if (sign) {
            if (significand == 0) {
                if (exponent == 0)
                    return 0.0;
                if (exponent == 0xFF)
                    return INFINITY;
            } else if (significand == 0x400000 && exponent == 0xFF)
                return NAN;

            if (exponent > 0)
                significand += 0x800000;
            exponent -= 0x7F;
            return ldexp(((double) significand) / ((double) 0x800000), exponent);
        } else {
            if (exponent == 0 && significand == 0x7FFFFF)
                return -INFINITY;

            significand = 0x7FFFFF - significand;
            if (exponent < 0xFF)
                significand += 0x800000;
            exponent = 0x80 - exponent;
            return -ldexp((((double) significand / (double) 0x800000)), exponent);
        }
    }

    long double Builder::decodeDouble(const uint8_t* data) {
        uint8_t sign = data[0] & 0x80;
        int64_t exponent = (static_cast<uint64_t>(data[0] & 0x7F) << 4) | (static_cast<uint64_t>(data[1]) >> 4);
        uint64_t significand = (static_cast<uint64_t>(data[1] & 0x0F) << 48) | (static_cast<uint64_t>(data[2]) << 40) |
                (static_cast<uint64_t>(data[3]) << 32) | (static_cast<uint64_t>(data[4]) << 24) | (static_cast<uint64_t>(data[5]) << 16) |
                (static_cast<uint64_t>(data[6]) << 8) | static_cast<uint64_t>(data[7]);

        if (sign) {
            if (significand == 0) {
                if (exponent == 0)
                    return 0.0;
                if (exponent == 0x7FF)
                    return INFINITY;
            } else if (significand == 0x8000000000000 && exponent == 0x7FF)
                return NAN;

            if (exponent > 0)
                significand += 0x10000000000000;
            exponent -= 0x3FF;
            return ldexpl(((long double) significand) / ((long double) 0x10000000000000), exponent);
        } else {
            if (exponent == 0 && significand == 0xFFFFFFFFFFFFF)
                return -INFINITY;

            significand = 0xFFFFFFFFFFFFF - significand;
            if (exponent < 0x7FF)
                significand += 0x10000000000000;
            exponent = 0x400 - exponent;
            return -ldexpl((((long double) significand / (long double) 0x10000000000000)), exponent);
        }
    }

    void Builder::builderRotate(bool copy) {
        auto nextBuffer = reinterpret_cast<BuilderQueue*>(ctx->getMemoryChunk("builder", true));
        nextBuffer->next = nullptr;
        nextBuffer->id = lastBuilderQueue->id + 1;
        nextBuffer->data = reinterpret_cast<uint8_t*>(nextBuffer) + sizeof(struct BuilderQueue);

        // Message could potentially fit in one buffer
        if (copy && msg != nullptr && sizeof(struct BuilderMsg) + messageLength < OUTPUT_BUFFER_DATA_SIZE) {
            memcpy(reinterpret_cast<void*>(nextBuffer->data), msg, sizeof(struct BuilderMsg) + messageLength);
            msg = reinterpret_cast<BuilderMsg*>(nextBuffer->data);
            msg->data = nextBuffer->data + sizeof(struct BuilderMsg);
            nextBuffer->length = sizeof(struct BuilderMsg) + messageLength;
            lastBuilderQueue->length -= sizeof(struct BuilderMsg) + messageLength;
        } else
            nextBuffer->length = 0;

        {
            std::unique_lock<std::mutex> lck(mtx);
            lastBuilderQueue->next = nextBuffer;
            ++buffersAllocated;
            lastBuilderQueue = nextBuffer;
        }
    }

    uint64_t Builder::builderSize() const {
        return ((messageLength + 7) & 0xFFFFFFFFFFFFFFF8) + sizeof(struct BuilderMsg);
    }

    uint64_t Builder::getMaxMessageMb() const {
        return maxMessageMb;
    }

    void Builder::setMaxMessageMb(uint64_t maxMessageMb_) {
        maxMessageMb = maxMessageMb_;
    }

    void Builder::processBegin(typeXid xid, typeScn scn) {
        lastXid = xid;
        commitScn = scn;
        newTran = true;
    }

    // 0x05010B0B
    void Builder::processInsertMultiple(typeScn scn, typeSeq sequence, typeTime time_, LobCtx* lobCtx, RedoLogRecord* redoLogRecord1,
                                        RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump) {
        uint64_t pos = 0;
        uint64_t fieldPos = 0;
        uint64_t fieldPosStart;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;
        uint16_t colLength = 0;
        OracleTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if ((scnFormat && SCN_ALL_COMMIT_VALUE) != 0)
            scn = commitScn;

        while (fieldNum < redoLogRecord2->rowData)
            RedoLogRecord::nextField(ctx, redoLogRecord2, fieldNum, fieldPos, fieldLength, 0x000001);

        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord2->nrow; ++r) {
            pos = 0;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord2->data[fieldPos + pos + 2];
            pos = 3;

            if ((redoLogRecord2->op & OP_ROWDEPENDENCIES) != 0) {
                if (ctx->version < REDO_VERSION_12_2)
                    pos += 6;
                else
                    pos += 8;
            }

            typeCol maxI;
            if (table != nullptr)
                maxI = table->maxSegCol;
            else
                maxI = jcc;

            for (typeCol i = 0; i < maxI; ++i) {
                if (i >= jcc) {
                    colLength = 0;
                } else {
                    colLength = redoLogRecord2->data[fieldPos + pos];
                    ++pos;
                    if (colLength == 0xFF) {
                        colLength = 0;
                    } else if (colLength == 0xFE) {
                        colLength = ctx->read16(redoLogRecord2->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (colLength > 0 || columnFormat >= COLUMN_FORMAT_FULL_INS_DEC || table == nullptr || table->columns[i]->numPk > 0)
                    valueSet(VALUE_AFTER, i, redoLogRecord2->data + fieldPos + pos, colLength, 0, dump);
                pos += colLength;
            }

            if (system && table != nullptr && (table->options & OPTIONS_SYSTEM_TABLE) != 0)
                systemTransaction->processInsert(table, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                                                 ctx->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2),
                                                 redoLogRecord1->dataOffset);
            if ((!schema && table != nullptr && (table->options & (OPTIONS_SYSTEM_TABLE | OPTIONS_DEBUG_TABLE)) == 0) ||
                    FLAG(REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) || FLAG(REDO_FLAGS_SCHEMALESS))
                processInsert(scn, sequence, time_, lobCtx, table, redoLogRecord2->obj, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                              ctx->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2), redoLogRecord1->xid,
                              redoLogRecord1->dataOffset);

            valuesRelease();

            fieldPosStart += ctx->read16(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta + r * 2);
        }
    }

    // 0x05010B0C
    void Builder::processDeleteMultiple(typeScn scn, typeSeq sequence, typeTime time_, LobCtx* lobCtx, RedoLogRecord* redoLogRecord1,
                                        RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump) {
        uint64_t pos = 0;
        uint64_t fieldPos = 0;
        uint64_t fieldPosStart;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;
        uint16_t colLength = 0;
        OracleTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if ((scnFormat && SCN_ALL_COMMIT_VALUE) != 0)
            scn = commitScn;

        while (fieldNum < redoLogRecord1->rowData)
            RedoLogRecord::nextField(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x000002);

        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord1->nrow; ++r) {
            pos = 0;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord1->data[fieldPos + pos + 2];
            pos = 3;

            if ((redoLogRecord1->op & OP_ROWDEPENDENCIES) != 0) {
                if (ctx->version < REDO_VERSION_12_2)
                    pos += 6;
                else
                    pos += 8;
            }

            typeCol maxI;
            if (table != nullptr)
                maxI = table->maxSegCol;
            else
                maxI = jcc;

            for (typeCol i = 0; i < maxI; ++i) {
                if (i >= jcc) {
                    colLength = 0;
                } else {
                    colLength = redoLogRecord1->data[fieldPos + pos];
                    ++pos;
                    if (colLength == 0xFF) {
                        colLength = 0;
                    } else if (colLength == 0xFE) {
                        colLength = ctx->read16(redoLogRecord1->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (colLength > 0 || columnFormat >= COLUMN_FORMAT_FULL_INS_DEC || table == nullptr || table->columns[i]->numPk > 0)
                    valueSet(VALUE_BEFORE, i, redoLogRecord1->data + fieldPos + pos, colLength, 0, dump);
                pos += colLength;
            }

            if (system && table != nullptr && (table->options & OPTIONS_SYSTEM_TABLE) != 0)
                systemTransaction->processDelete(table, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                                                 ctx->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2),
                                                 redoLogRecord1->dataOffset);

            if ((!schema && table != nullptr && (table->options & (OPTIONS_SYSTEM_TABLE | OPTIONS_DEBUG_TABLE)) == 0) ||
                    FLAG(REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) || FLAG(REDO_FLAGS_SCHEMALESS))
                processDelete(scn, sequence, time_, lobCtx, table, redoLogRecord2->obj, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                              ctx->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2), redoLogRecord1->xid,
                              redoLogRecord1->dataOffset);

            valuesRelease();

            fieldPosStart += ctx->read16(redoLogRecord1->data + redoLogRecord1->rowLenghsDelta + r * 2);
        }
    }

    void Builder::processDml(typeScn scn, typeSeq sequence, typeTime time_, LobCtx* lobCtx, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2,
                             uint64_t type, bool system, bool schema, bool dump) {
        uint8_t fb;
        typeObj obj;
        typeDataObj dataObj;
        typeDba bdba;
        typeSlot slot;
        RedoLogRecord* redoLogRecord1p;
        RedoLogRecord* redoLogRecord2p = nullptr;
        OracleTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if ((scnFormat && SCN_ALL_COMMIT_VALUE) != 0)
            scn = commitScn;

        if (type == TRANSACTION_INSERT) {
            redoLogRecord2p = redoLogRecord2;
            while (redoLogRecord2p != nullptr) {
                if ((redoLogRecord2p->fb & FB_F) != 0)
                    break;
                redoLogRecord2p = redoLogRecord2p->next;
            }

            if (redoLogRecord2p == nullptr) {
                ctx->warning(60001, "incomplete row for table (obj: " + std::to_string(redoLogRecord1->obj) + "), probably IOT, offset: " +
                             std::to_string(redoLogRecord1->dataOffset) + ", xid: " + lastXid.toString());
                obj = 0;
                dataObj = 0;
                bdba = 0;
                slot = 0;
            } else {
                obj = redoLogRecord2p->obj;
                dataObj = redoLogRecord2p->dataObj;
                bdba = redoLogRecord2p->bdba;
                slot = redoLogRecord2p->slot;
            }
        } else {
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                obj = redoLogRecord1->obj;
                dataObj = redoLogRecord1->dataObj;
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                obj = redoLogRecord2->obj;
                dataObj = redoLogRecord2->dataObj;
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        }

        uint64_t fieldPos;
        typeField fieldNum;
        uint16_t fieldLength;
        uint16_t colLength;
        uint16_t colNum = 0;
        uint16_t colShift;
        uint8_t* nulls;
        uint8_t bits;
        uint8_t* colNums;
        bool suppPrev = false;

        // Data in UNDO
        redoLogRecord1p = redoLogRecord1;
        redoLogRecord2p = redoLogRecord2;
        colNums = nullptr;

        while (redoLogRecord1p != nullptr) {
            fieldPos = 0;
            fieldNum = 0;
            fieldLength = 0;

            // UNDO
            if (redoLogRecord1p->rowData > 0) {
                if ((ctx->trace & TRACE_DML) != 0 || dump) {
                    ctx->logTrace(TRACE_DML, "UNDO");
                }

                nulls = redoLogRecord1p->data + redoLogRecord1p->nullsDelta;
                bits = 1;

                if (redoLogRecord1p->suppLogBefore > 0)
                    colShift = redoLogRecord1p->suppLogBefore - 1;
                else
                    colShift = 0;

                if (redoLogRecord1p->colNumsDelta > 0 && !redoLogRecord1p->compressed) {
                    colNums = redoLogRecord1p->data + redoLogRecord1p->colNumsDelta;
                    colShift -= ctx->read16(colNums);
                } else {
                    colNums = nullptr;
                }
                if (colShift >= MAX_NO_COLUMNS)
                    throw RedoLogException(50059, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                           std::to_string(redoLogRecord1p->dataObj) + "): invalid column shift: " + std::to_string(colShift) +
                                           ", before: " + std::to_string(redoLogRecord1p->suppLogBefore) + ", xid: " + lastXid.toString() + ", offset: " +
                                           std::to_string(redoLogRecord1p->dataOffset));

                while (fieldNum < redoLogRecord1p->rowData - 1)
                    RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldLength, 0x000003);

                uint64_t cc = redoLogRecord1p->cc;
                if (redoLogRecord1p->compressed) {
                    if (redoLogRecord1p->sizeDelt > 0)
                        cc = 1;
                    else
                        cc = 0;
                    compressedBefore = true;
                }

                for (uint64_t i = 0; i < cc; ++i) {
                    if (colNums != nullptr) {
                        colNum = ctx->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (fieldNum + 1 > redoLogRecord1p->fieldCnt) {
                        if (table != nullptr)
                            throw RedoLogException(50014, "table: " + table->owner + "." + table->name + ": out of columns (Undo): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint64_t>(redoLogRecord1p->cc)) + ", " +
                                                   std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                                   std::to_string(redoLogRecord1p->rowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                                   ", xid: " + lastXid.toString() + ", offset: " + std::to_string(redoLogRecord1p->dataOffset));
                        else
                            throw RedoLogException(50014, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord1p->dataObj) + "): out of columns (Undo): " + std::to_string(colNum) +
                                                   "/" + std::to_string(static_cast<uint64_t>(redoLogRecord1p->cc)) + ", " +
                                                   std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                                   std::to_string(redoLogRecord1p->rowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                                   ", xid: " + lastXid.toString() + ", offset: " + std::to_string(redoLogRecord1p->dataOffset));
                    }

                    fb = 0;
                    if (i == 0 && (redoLogRecord1p->fb & FB_P) != 0)
                        fb |= FB_P;
                    if (i == static_cast<uint64_t>(redoLogRecord1p->cc - 1) && (redoLogRecord1p->fb & FB_N) != 0)
                        fb |= FB_N;

                    if (table != nullptr) {
                        if (colNum >= table->maxSegCol) {
                            if (!schema)
                                throw RedoLogException(50060, "table: " + table->owner + "." + table->name +
                                                       ": referring to invalid column id(" + std::to_string(colNum) +  ", xid: " + lastXid.toString() +
                                                       "), offset: " + std::to_string(redoLogRecord1p->dataOffset));
                            break;
                        }
                    } else {
                        if (colNum >= MAX_NO_COLUMNS)
                            throw RedoLogException(50060, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord1p->dataObj) + "): referring to invalid column id(" +
                                                   std::to_string(colNum) + "), xid: " + lastXid.toString() + ", offset: " +
                                                   std::to_string(redoLogRecord1p->dataOffset));
                    }

                    if ((*nulls & bits) != 0)
                        colLength = 0;
                    else {
                        RedoLogRecord::skipEmptyFields(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                        RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldLength, 0x000004);
                        colLength = fieldLength;
                    }

                    valueSet(VALUE_BEFORE, colNum, redoLogRecord1p->data + fieldPos, colLength, fb, dump);

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            // Supplemental columns
            if (redoLogRecord1p->suppLogRowData > 0) {
                if ((ctx->trace & TRACE_DML) != 0 || dump) {
                    ctx->logTrace(TRACE_DML, "UNDO SUP");
                }

                while (fieldNum < redoLogRecord1p->suppLogRowData - 1)
                    RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldLength, 0x000005);

                colNums = redoLogRecord1p->data + redoLogRecord1p->suppLogNumsDelta;
                uint8_t* colSizes = redoLogRecord1p->data + redoLogRecord1p->suppLogLenDelta;

                for (uint64_t i = 0; i < static_cast<uint64_t>(redoLogRecord1p->suppLogCC); ++i) {
                    colNum = ctx->read16(colNums) - 1;
                    if (fieldNum + 1 > redoLogRecord1p->fieldCnt) {
                        if (table != nullptr)
                            throw RedoLogException(50014, "table: " + table->owner + "." + table->name + ": out of columns (supp): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint64_t>(redoLogRecord1p->cc)) + ", " +
                                                   std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                                   std::to_string(redoLogRecord1p->suppLogRowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                                   ", xid: " + lastXid.toString() + ", offset: " + std::to_string(redoLogRecord1p->dataOffset));
                        else
                            throw RedoLogException(50014, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord1p->dataObj) + "): out of columns (Supp): " + std::to_string(colNum) +
                                                   "/" + std::to_string(static_cast<uint64_t>(redoLogRecord1p->cc)) + ", " +
                                                   std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                                   std::to_string(redoLogRecord1p->suppLogRowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                                   ", xid: " + lastXid.toString() + ", offset: " + std::to_string(redoLogRecord1p->dataOffset));
                    }

                    RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldLength, 0x000006);

                    if (table != nullptr) {
                        if (colNum >= table->maxSegCol) {
                            if (!schema)
                                throw RedoLogException(50060, "table: " + table->owner + "." + table->name +
                                                       ": referring to invalid column id(" + std::to_string(colNum) + "), xid: " + lastXid.toString() +
                                                       ", offset: " + std::to_string(redoLogRecord1p->dataOffset));
                            break;
                        }
                    } else {
                        if (colNum >= MAX_NO_COLUMNS)
                            throw RedoLogException(50060, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord1p->dataObj) + "): referring to invalid column id(" +
                                                   std::to_string(colNum) + "), xid: " + lastXid.toString() + ", offset: " +
                                                   std::to_string(redoLogRecord1p->dataOffset));
                    }

                    colNums += 2;
                    colLength = ctx->read16(colSizes);

                    if (colLength == 0xFFFF)
                        colLength = 0;

                    fb = 0;
                    if (i == 0 && (redoLogRecord1p->suppLogFb & FB_P) != 0 && suppPrev) {
                        fb |= FB_P;
                        suppPrev = false;
                    }
                    if (i == static_cast<uint64_t>(redoLogRecord1p->suppLogCC - 1) && (redoLogRecord1p->suppLogFb & FB_N) != 0) {
                        fb |= FB_N;
                        suppPrev = true;
                    }

                    // Insert, lock, update, supplemental log data
                    if (redoLogRecord2p->opCode == 0x0B02 || redoLogRecord2p->opCode == 0x0B04 || redoLogRecord2p->opCode == 0x0B05 ||
                            redoLogRecord2p->opCode == 0x0B10)
                        valueSet(VALUE_AFTER_SUPP, colNum, redoLogRecord1p->data + fieldPos, colLength, fb, dump);

                    // Delete, update, overwrite, supplemental log data
                    if (redoLogRecord2p->opCode == 0x0B03 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B06 ||
                            redoLogRecord2p->opCode == 0x0B10)
                        valueSet(VALUE_BEFORE_SUPP, colNum, redoLogRecord1p->data + fieldPos, colLength, fb, dump);

                    colSizes += 2;
                }
            }

            // REDO
            if (redoLogRecord2p->rowData > 0) {
                if ((ctx->trace & TRACE_DML) != 0 || dump) {
                    ctx->logTrace(TRACE_DML, "REDO");
                }

                fieldPos = 0;
                fieldNum = 0;
                fieldLength = 0;
                nulls = redoLogRecord2p->data + redoLogRecord2p->nullsDelta;
                bits = 1;

                if (redoLogRecord2p->suppLogAfter > 0)
                    colShift = redoLogRecord2p->suppLogAfter - 1;
                else
                    colShift = 0;

                if (redoLogRecord2p->colNumsDelta > 0 && !redoLogRecord2p->compressed) {
                    colNums = redoLogRecord2p->data + redoLogRecord2p->colNumsDelta;
                    colShift -= ctx->read16(colNums);
                } else {
                    colNums = nullptr;
                }
                if (colShift >= MAX_NO_COLUMNS) {
                    throw RedoLogException(50059, "table: (obj: " + std::to_string(redoLogRecord2p->obj) + ", dataobj: " +
                                           std::to_string(redoLogRecord2p->dataObj) + "): invalid column shift: " + std::to_string(colShift) +
                                           ", before: " + std::to_string(redoLogRecord2p->suppLogBefore) + ", xid: " + lastXid.toString() + ", offset: " +
                                           std::to_string(redoLogRecord2p->dataOffset));
                }

                while (fieldNum < redoLogRecord2p->rowData - 1)
                    RedoLogRecord::nextField(ctx, redoLogRecord2p, fieldNum, fieldPos, fieldLength, 0x000007);

                uint64_t cc = redoLogRecord2p->cc;
                if (redoLogRecord2p->compressed) {
                    if (redoLogRecord2p->sizeDelt > 0)
                        cc = 1;
                    else
                        cc = 0;
                    compressedAfter = true;
                } else
                    compressedAfter = false;

                for (uint64_t i = 0; i < cc; ++i) {
                    if (fieldNum + 1 > redoLogRecord2p->fieldCnt) {
                        if (table != nullptr)
                            throw RedoLogException(50014, "table: " + table->owner + "." + table->name + ": out of columns (Redo): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint64_t>(redoLogRecord2p->cc)) + ", " +
                                                   std::to_string(redoLogRecord2p->sizeDelt) + ", " + std::to_string(fieldNum) + ", " +
                                                   std::to_string(fieldNum) + "-" + std::to_string(redoLogRecord2p->rowData) + "-" +
                                                   std::to_string(redoLogRecord2p->fieldCnt) + ", xid: " + lastXid.toString() + ", offset: " +
                                                   std::to_string(redoLogRecord2p->dataOffset));
                        else
                            throw RedoLogException(50014, "table: (obj: " + std::to_string(redoLogRecord2p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord2p->dataObj) + "): out of columns (Redo): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint64_t>(redoLogRecord2p->cc)) + ", " +
                                                   std::to_string(redoLogRecord2p->sizeDelt) + ", " +  std::to_string(fieldNum) + ", " +
                                                   std::to_string(fieldNum) + "-" + std::to_string(redoLogRecord2p->rowData) + "-" +
                                                   std::to_string(redoLogRecord2p->fieldCnt) + ", xid: " + lastXid.toString() + ", offset: " +
                                                   std::to_string(redoLogRecord2p->dataOffset));
                        break;
                    }

                    fb = 0;
                    if (i == 0 && (redoLogRecord2p->fb & FB_P) != 0)
                        fb |= FB_P;
                    if (i == static_cast<uint64_t>(redoLogRecord2p->cc - 1) && (redoLogRecord2p->fb & FB_N) != 0)
                        fb |= FB_N;

                    RedoLogRecord::nextField(ctx, redoLogRecord2p, fieldNum, fieldPos, fieldLength, 0x000008);

                    if (colNums != nullptr) {
                        colNum = ctx->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (table != nullptr) {
                        if (colNum >= table->maxSegCol) {
                            if (!schema)
                                throw RedoLogException(50060, "table: " + table->owner + "." + table->name +
                                                       ": referring to invalid column id(" + std::to_string(colNum) +
                                                       "), xid: " + lastXid.toString() + ", offset: " + std::to_string(redoLogRecord2p->dataOffset));
                            break;
                        }
                    } else {
                        if (colNum >= MAX_NO_COLUMNS)
                            throw RedoLogException(50060, "table: (obj: " + std::to_string(redoLogRecord2p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord2p->dataObj) + "): referring to invalid column id(" +
                                                   std::to_string(colNum) + "), xid: " + lastXid.toString() + "), offset: " +
                                                   std::to_string(redoLogRecord2p->dataOffset));
                    }

                    if ((*nulls & bits) != 0)
                        colLength = 0;
                    else
                        colLength = fieldLength;

                    valueSet(VALUE_AFTER, colNum, redoLogRecord2p->data + fieldPos, colLength, fb, dump);

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            redoLogRecord1p = redoLogRecord1p->next;
            redoLogRecord2p = redoLogRecord2p->next;
        }

        typeCol guardPos = -1;
        if (table != nullptr && table->guardSegNo != -1)
            guardPos = table->guardSegNo;

        uint64_t baseMax = valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (valuesSet[base] < mask)
                    break;
                if ((valuesSet[base] & mask) == 0)
                    continue;

                // Merge column values
                if ((valuesMerge[base] & mask) != 0) {

                    for (uint64_t j = 0; j < 4; ++j) {
                        uint64_t length = 0;

                        if (valuesPart[0][column][j] != nullptr)
                            length += lengthsPart[0][column][j];
                        if (valuesPart[1][column][j] != nullptr)
                            length += lengthsPart[1][column][j];
                        if (valuesPart[2][column][j] != nullptr)
                            length += lengthsPart[2][column][j];

                        if (length == 0)
                            continue;

                        if (values[column][j] != nullptr)
                            throw RedoLogException(50015, "value for " + std::to_string(column) + "/" + std::to_string(j) +
                                                   " is already set when merging, xid: " + lastXid.toString() + ", offset: " +
                                                   std::to_string(redoLogRecord1->dataOffset));

                        auto buffer = new uint8_t[length];
                        merges[mergesMax++] = buffer;

                        values[column][j] = buffer;
                        lengths[column][j] = length;

                        if (valuesPart[0][column][j] != nullptr) {
                            memcpy(reinterpret_cast<void*>(buffer), valuesPart[0][column][j], lengthsPart[0][column][j]);
                            buffer += lengthsPart[0][column][j];
                            valuesPart[0][column][j] = nullptr;
                        }
                        if (valuesPart[1][column][j] != nullptr) {
                            memcpy(reinterpret_cast<void*>(buffer), valuesPart[1][column][j], lengthsPart[1][column][j]);
                            buffer += lengthsPart[1][column][j];
                            valuesPart[1][column][j] = nullptr;
                        }
                        if (valuesPart[2][column][j] != nullptr) {
                            memcpy(reinterpret_cast<void*>(buffer), valuesPart[2][column][j], lengthsPart[2][column][j]);
                            buffer += lengthsPart[2][column][j];
                            valuesPart[2][column][j] = nullptr;
                        }
                    }
                    valuesMerge[base] &= ~mask;
                }

                if (values[column][VALUE_BEFORE] == nullptr) {
                    bool guardPresent = false;
                    if (guardPos != -1 && table->columns[column]->guardSeg != -1 && values[guardPos][VALUE_BEFORE] != nullptr) {
                        typeCol column2 = table->columns[column]->guardSeg;
                        uint8_t* guardData = values[guardPos][VALUE_BEFORE];
                        if (guardData != nullptr && static_cast<int64_t>(column2 / static_cast<typeCol>(8)) < lengths[guardPos][VALUE_BEFORE]) {
                            guardPresent = true;
                            if ((values[guardPos][VALUE_BEFORE][column2 / 8] & (1 << (column2 & 7))) != 0) {
                                values[column][VALUE_BEFORE] = reinterpret_cast<uint8_t*>(1);
                                lengths[column][VALUE_BEFORE] = 0;
                            }
                        }
                    }

                    if (!guardPresent && values[column][VALUE_BEFORE_SUPP] != nullptr) {
                        values[column][VALUE_BEFORE] = values[column][VALUE_BEFORE_SUPP];
                        lengths[column][VALUE_BEFORE] = lengths[column][VALUE_BEFORE_SUPP];
                    }
                }

                if (values[column][VALUE_AFTER] == nullptr) {
                    bool guardPresent = false;
                    if (guardPos != -1 && table->columns[column]->guardSeg != -1 && values[guardPos][VALUE_AFTER] != nullptr) {
                        typeCol column2 = table->columns[column]->guardSeg;
                        uint8_t* guardData = values[guardPos][VALUE_AFTER];
                        if (guardData != nullptr && static_cast<int64_t>(column2 / static_cast<typeCol>(8)) < lengths[guardPos][VALUE_AFTER]) {
                            guardPresent = true;
                            if ((values[guardPos][VALUE_AFTER][column2 / 8] & (1 << (column2 & 7))) != 0) {
                                values[column][VALUE_AFTER] = reinterpret_cast<uint8_t*>(1);
                                lengths[column][VALUE_AFTER] = 0;
                            }
                        }
                    }

                    if (!guardPresent && values[column][VALUE_AFTER_SUPP] != nullptr) {
                        values[column][VALUE_AFTER] = values[column][VALUE_AFTER_SUPP];
                        lengths[column][VALUE_AFTER] = lengths[column][VALUE_AFTER_SUPP];
                    }
                }
            }
        }

        if ((ctx->trace & TRACE_DML) != 0 || dump) {
            if (table != nullptr) {
                ctx->logTrace(TRACE_DML, "tab: " + table->owner + "." + table->name + " type: " + std::to_string(type) + " columns: " +
                              std::to_string(valuesMax));

                baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        ctx->logTrace(TRACE_DML, "DML: " + std::to_string(column + 1) + ":  B(" +
                                      std::to_string(values[column][VALUE_BEFORE] != nullptr ? lengths[column][VALUE_BEFORE] : -1) + ") A(" +
                                      std::to_string(values[column][VALUE_AFTER] != nullptr ? lengths[column][VALUE_AFTER] : -1) + ") BS(" +
                                      std::to_string(values[column][VALUE_BEFORE_SUPP] != nullptr ? lengths[column][VALUE_BEFORE_SUPP] : -1) + ")" +
                                      " AS(" + std::to_string(values[column][VALUE_AFTER_SUPP] != nullptr ? lengths[column][VALUE_AFTER_SUPP] : -1) +
                                      ") pk: " + std::to_string(table->columns[column]->numPk));
                    }
                }
            } else {
                ctx->logTrace(TRACE_DML, "tab: (obj: " + std::to_string(redoLogRecord1->obj) + ", dataobj: " +
                        std::to_string(redoLogRecord1->dataObj) + ") type: " + std::to_string(type) + " columns: " + std::to_string(valuesMax));

                baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        ctx->logTrace(TRACE_DML, "DML: " + std::to_string(column + 1) + ":  B(" +
                                      std::to_string(lengths[column][VALUE_BEFORE]) + ") A(" + std::to_string(lengths[column][VALUE_AFTER]) +
                                      ") BS(" + std::to_string(lengths[column][VALUE_BEFORE_SUPP]) + ") AS(" +
                                      std::to_string(lengths[column][VALUE_AFTER_SUPP]) + ")");
                    }
                }
            }
        }

        if (type == TRANSACTION_UPDATE) {
            if (!compressedBefore && !compressedAfter) {
                baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (table != nullptr && columnFormat < COLUMN_FORMAT_FULL_UPD) {
                            if (table->columns[column]->numPk == 0) {
                                // Remove unchanged column values - only for tables with a defined primary key
                                if (values[column][VALUE_BEFORE] != nullptr && lengths[column][VALUE_BEFORE] == lengths[column][VALUE_AFTER] &&
                                        values[column][VALUE_AFTER] != nullptr) {
                                    if (lengths[column][VALUE_BEFORE] == 0 ||
                                            memcmp(values[column][VALUE_BEFORE], values[column][VALUE_AFTER], lengths[column][VALUE_BEFORE]) == 0) {
                                        valuesSet[base] &= ~mask;
                                        values[column][VALUE_BEFORE] = nullptr;
                                        values[column][VALUE_BEFORE_SUPP] = nullptr;
                                        values[column][VALUE_AFTER] = nullptr;
                                        values[column][VALUE_AFTER_SUPP] = nullptr;
                                        continue;
                                    }
                                }

                                // Remove columns additionally present, but null
                                if (values[column][VALUE_BEFORE] != nullptr && lengths[column][VALUE_BEFORE] == 0 && values[column][VALUE_AFTER] == nullptr) {
                                    valuesSet[base] &= ~mask;
                                    values[column][VALUE_BEFORE] = nullptr;
                                    values[column][VALUE_BEFORE_SUPP] = nullptr;
                                    values[column][VALUE_AFTER_SUPP] = nullptr;
                                    continue;
                                }

                                if (values[column][VALUE_AFTER] != nullptr && lengths[column][VALUE_AFTER] == 0 && values[column][VALUE_BEFORE] == nullptr) {
                                    valuesSet[base] &= ~mask;
                                    values[column][VALUE_AFTER] = nullptr;
                                    values[column][VALUE_BEFORE_SUPP] = nullptr;
                                    values[column][VALUE_AFTER_SUPP] = nullptr;
                                    continue;
                                }

                            } else {
                                // Leave null value & propagate
                                if (values[column][VALUE_BEFORE] != nullptr && lengths[column][VALUE_BEFORE] == 0 && values[column][VALUE_AFTER] == nullptr) {
                                    values[column][VALUE_AFTER] = values[column][VALUE_BEFORE];
                                    lengths[column][VALUE_AFTER] = lengths[column][VALUE_BEFORE];
                                }

                                if (values[column][VALUE_AFTER] != nullptr && lengths[column][VALUE_AFTER] == 0 && values[column][VALUE_BEFORE] == nullptr) {
                                    values[column][VALUE_BEFORE] = values[column][VALUE_AFTER];
                                    lengths[column][VALUE_BEFORE] = lengths[column][VALUE_AFTER];
                                }
                            }
                        }

                        // For update assume null for missing columns
                        if (values[column][VALUE_BEFORE] != nullptr && values[column][VALUE_AFTER] == nullptr) {
                            values[column][VALUE_AFTER] = reinterpret_cast<uint8_t*>(1);
                            lengths[column][VALUE_AFTER] = 0;
                        }

                        if (values[column][VALUE_AFTER] != nullptr && values[column][VALUE_BEFORE] == nullptr) {
                            values[column][VALUE_BEFORE] = reinterpret_cast<uint8_t*>(1);
                            lengths[column][VALUE_BEFORE] = 0;
                        }
                    }
                }
            }

            if (system && table != nullptr && (table->options & OPTIONS_SYSTEM_TABLE) != 0)
                systemTransaction->processUpdate(table, dataObj, bdba, slot, redoLogRecord1->dataOffset);

            if ((!schema && table != nullptr && (table->options & (OPTIONS_SYSTEM_TABLE | OPTIONS_DEBUG_TABLE)) == 0) ||
                    FLAG(REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) || FLAG(REDO_FLAGS_SCHEMALESS))
                processUpdate(scn, sequence, time_, lobCtx, table, obj, dataObj, bdba, slot, redoLogRecord1->xid, redoLogRecord1->dataOffset);

        } else if (type == TRANSACTION_INSERT) {
            if (table != nullptr && !compressedAfter) {
                // Assume null values for all missing columns
                if (columnFormat >= COLUMN_FORMAT_FULL_INS_DEC) {
                    auto maxCol = static_cast<typeCol>(table->columns.size());
                    for (typeCol column = 0; column < maxCol; ++column) {
                        uint64_t base = column >> 6;
                        uint64_t mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) == 0) {
                            valuesSet[base] |= mask;
                            values[column][VALUE_AFTER] = reinterpret_cast<uint8_t*>(1);
                            lengths[column][VALUE_AFTER] = 0;
                        }
                    }
                } else {
                    // Remove null values from insert if not PK
                    baseMax = valuesMax >> 6;
                    for (uint64_t base = 0; base <= baseMax; ++base) {
                        auto column = static_cast<typeCol>(base << 6);
                        for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                            if (valuesSet[base] < mask)
                                break;
                            if ((valuesSet[base] & mask) == 0)
                                continue;
                            if (table->columns[column]->numPk > 0)
                                continue;

                            if (values[column][VALUE_AFTER] == nullptr || lengths[column][VALUE_AFTER] == 0) {
                                valuesSet[base] &= ~mask;
                                values[column][VALUE_AFTER] = nullptr;
                                values[column][VALUE_AFTER_SUPP] = nullptr;
                            }
                        }
                    }

                    // Assume null values for pk missing columns
                    for (typeCol column: table->pk) {
                        uint64_t base = column >> 6;
                        uint64_t mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) == 0) {
                            valuesSet[base] |= mask;
                            values[column][VALUE_AFTER] = reinterpret_cast<uint8_t*>(1);
                            lengths[column][VALUE_AFTER] = 0;
                        }
                    }
                }
            }

            if (system && table != nullptr && (table->options & OPTIONS_SYSTEM_TABLE) != 0)
                systemTransaction->processInsert(table, dataObj, bdba, slot, redoLogRecord1->dataOffset);

            if ((!schema && table != nullptr && (table->options & (OPTIONS_SYSTEM_TABLE | OPTIONS_DEBUG_TABLE)) == 0) ||
                    FLAG(REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) || FLAG(REDO_FLAGS_SCHEMALESS))
                processInsert(scn, sequence, time_, lobCtx, table, obj, dataObj, bdba, slot, redoLogRecord1->xid, redoLogRecord1->dataOffset);

        } else if (type == TRANSACTION_DELETE) {
            if (table != nullptr && !compressedBefore) {
                // Assume null values for all missing columns
                if (columnFormat >= COLUMN_FORMAT_FULL_INS_DEC) {
                    auto maxCol = static_cast<typeCol>(table->columns.size());
                    for (typeCol column = 0; column < maxCol; ++column) {
                        uint64_t base = column >> 6;
                        uint64_t mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) == 0) {
                            valuesSet[base] |= mask;
                            values[column][VALUE_BEFORE] = reinterpret_cast<uint8_t*>(1);
                            lengths[column][VALUE_BEFORE] = 0;
                        }
                    }
                } else {
                    // Remove null values from delete if not PK
                    baseMax = valuesMax >> 6;
                    for (uint64_t base = 0; base <= baseMax; ++base) {
                        auto column = static_cast<typeCol>(base << 6);
                        for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                            if (valuesSet[base] < mask)
                                break;
                            if ((valuesSet[base] & mask) == 0)
                                continue;
                            if (table->columns[column]->numPk > 0)
                                continue;

                            if (values[column][VALUE_BEFORE] == nullptr || lengths[column][VALUE_BEFORE] == 0) {
                                valuesSet[base] &= ~mask;
                                values[column][VALUE_BEFORE] = nullptr;
                                values[column][VALUE_BEFORE_SUPP] = nullptr;
                            }
                        }
                    }

                    // Assume null values for pk missing columns
                    for (typeCol column: table->pk) {
                        uint64_t base = column >> 6;
                        uint64_t mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) == 0) {
                            valuesSet[base] |= mask;
                            values[column][VALUE_BEFORE] = reinterpret_cast<uint8_t*>(1);
                            lengths[column][VALUE_BEFORE] = 0;
                        }
                    }
                }
            }

            if (system && table != nullptr && (table->options & OPTIONS_SYSTEM_TABLE) != 0)
                systemTransaction->processDelete(table, dataObj, bdba, slot, redoLogRecord1->dataOffset);

            if ((!schema && table != nullptr && (table->options & (OPTIONS_SYSTEM_TABLE | OPTIONS_DEBUG_TABLE)) == 0) ||
                    FLAG(REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) || FLAG(REDO_FLAGS_SCHEMALESS))
                processDelete(scn, sequence, time_, lobCtx, table, obj, dataObj, bdba, slot, redoLogRecord1->xid, redoLogRecord1->dataOffset);
        }

        valuesRelease();
    }

    // 0x18010000
    void Builder::processDdlHeader(typeScn scn, typeSeq sequence, typeTime time_, RedoLogRecord* redoLogRecord1) {
        uint64_t fieldPos = 0;
        uint64_t sqlLength;
        typeField fieldNum = 0;
        uint16_t seq = 0;
        // uint16_t cnt = 0;
        uint16_t type = 0;
        uint16_t fieldLength = 0;
        char* sqlText = nullptr;
        OracleTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if ((scnFormat && SCN_ALL_COMMIT_VALUE) != 0)
            scn = commitScn;

        RedoLogRecord::nextField(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x000009);
        // Field: 1
        type = ctx->read16(redoLogRecord1->data + fieldPos + 12);
        seq = ctx->read16(redoLogRecord1->data + fieldPos + 18);
        // cnt = ctx->read16(redoLogRecord1->data + fieldPos + 20);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x00000A))
            return;
        // Field: 2

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x00000B))
            return;
        // Field: 3

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x00000C))
            return;
        // Field: 4

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x00000D))
            return;
        // Field: 5

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x00000E))
            return;
        // Field: 6

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x00000F))
            return;
        // Field: 7

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldLength, 0x000011))
            return;
        // Field: 8
        sqlLength = fieldLength;
        sqlText = reinterpret_cast<char*>(redoLogRecord1->data) + fieldPos;

        if (type == 85)
            processDdl(scn, sequence, time_, table, redoLogRecord1->obj, redoLogRecord1->dataObj, type, seq, "truncate", sqlText,
                       sqlLength - 1);
        else if (type == 12)
            processDdl(scn, sequence, time_, table, redoLogRecord1->obj, redoLogRecord1->dataObj, type, seq, "drop", sqlText, sqlLength - 1);
        else if (type == 15)
            processDdl(scn, sequence, time_, table, redoLogRecord1->obj, redoLogRecord1->dataObj, type, seq, "alter", sqlText, sqlLength - 1);
        else
            processDdl(scn, sequence, time_, table, redoLogRecord1->obj, redoLogRecord1->dataObj, type, seq, "?", sqlText, sqlLength - 1);
    }

    void Builder::releaseBuffers(uint64_t maxId) {
        BuilderQueue* builderQueue = nullptr;
        {
            std::unique_lock<std::mutex> lck(mtx);
            builderQueue = firstBuilderQueue;
            while (firstBuilderQueue->id < maxId) {
                firstBuilderQueue = firstBuilderQueue->next;
                --buffersAllocated;
            }
        }

        if (builderQueue != nullptr) {
            while (builderQueue->id < maxId) {
                BuilderQueue* nextBuffer = builderQueue->next;
                ctx->freeMemoryChunk("builder", reinterpret_cast<uint8_t*>(builderQueue), true);
                builderQueue = nextBuffer;
            }
        }
    }

    void Builder::sleepForWriterWork(uint64_t queueSize, uint64_t nanoseconds) {
        if (ctx->trace & TRACE_SLEEP)
            ctx->logTrace(TRACE_SLEEP, "Builder:sleepForWriterWork");

        std::unique_lock<std::mutex> lck(mtx);
        if (queueSize > 0)
            condNoWriterWork.wait_for(lck, std::chrono::nanoseconds(nanoseconds));
        else
            condNoWriterWork.wait_for(lck, std::chrono::seconds(5));
    }

    void Builder::wakeUp() {
        std::unique_lock<std::mutex> lck(mtx);
        condNoWriterWork.notify_all();
    }
}
