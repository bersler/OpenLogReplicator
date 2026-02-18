/* Memory buffer for handling output data
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

#include <cmath>
#include <vector>

#include "../common/DbColumn.h"
#include "../common/DbTable.h"
#include "../common/RedoLogRecord.h"
#include "../common/XmlCtx.h"
#include "../common/metrics/Metrics.h"
#include "../common/table/SysCol.h"
#include "../common/Thread.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "Builder.h"
#include "SystemTransaction.h"

namespace OpenLogReplicator {
    Builder::Builder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, const Format& newFormat, uint64_t newFlushBuffer):
            ctx(newCtx),
            locales(newLocales),
            metadata(newMetadata),
            format(newFormat),
            flushBuffer(newFlushBuffer) {
        memset(valuesSet, 0, sizeof(valuesSet));
        memset(valuesMerge, 0, sizeof(valuesMerge));
        memset(values, 0, sizeof(values));
        memset(valuesPart, 0, sizeof(valuesPart));
    }

    Builder::~Builder() {
        releaseValues();
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_DDL))
            releaseDdl();
        tables.clear();

        while (firstBuilderQueue != nullptr) {
            BuilderQueue* nextBuffer = firstBuilderQueue->next;
            ctx->freeMemoryChunk(ctx->parserThread, Ctx::MEMORY::BUILDER, reinterpret_cast<uint8_t*>(firstBuilderQueue));
            firstBuilderQueue = nextBuffer;
            --buffersAllocated;
        }

        if (systemTransaction != nullptr) {
            delete systemTransaction;
            systemTransaction = nullptr;
        }

        delete[] valueBuffer;
        valueBuffer = nullptr;

        delete[] valueBufferOld;
        valueBufferOld = nullptr;
    }

    void Builder::initialize() {
        buffersAllocated = 1;
        firstBuilderQueue = reinterpret_cast<BuilderQueue*>(ctx->getMemoryChunk(ctx->parserThread, Ctx::MEMORY::BUILDER));
        ctx->parserThread->contextSet(Thread::CONTEXT::CPU);
        firstBuilderQueue->id = 0;
        firstBuilderQueue->next = nullptr;
        firstBuilderQueue->data = reinterpret_cast<uint8_t*>(firstBuilderQueue) + sizeof(BuilderQueue);
        firstBuilderQueue->confirmedSize = 0;
        firstBuilderQueue->start = 0;
        lastBuilderQueue = firstBuilderQueue;
        lastBuilderSize = 0;

        valueBuffer = new char[VALUE_BUFFER_MIN];
        valueBufferSize = VALUE_BUFFER_MIN;
    }

    void Builder::processValue(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeCol col, const uint8_t* data, uint32_t size,
                               FileOffset fileOffset, bool after, bool compressed) {
        if (compressed) {
            const std::string columnName("COMPRESSED");
            columnRaw(columnName, data, size);
            return;
        }
        if (unlikely(table == nullptr)) {
            const std::string columnName("COL_" + std::to_string(col));
            columnRaw(columnName, data, size);
            return;
        }
        DbColumn* column = table->columns[col];
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::RAW_COLUMN_DATA)) {
            columnRaw(column->name, data, size);
            return;
        }
        if (column->guard && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_GUARD_COLUMNS))
            return;
        if (column->nested && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_NESTED_COLUMNS))
            return;
        if (column->hidden && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_HIDDEN_COLUMNS))
            return;
        if (column->unused && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_UNUSED_COLUMNS))
            return;

        if (unlikely(size == 0))
            throw RedoLogException(50013, "trying to output null data for column: " + column->name + ", offset: " + fileOffset.toString());

        if (column->storedAsLob) {
            if (column->type == SysCol::COLTYPE::VARCHAR) {
                // VARCHAR2 stored as CLOB
                column->type = SysCol::COLTYPE::CLOB;
            } else if (column->type == SysCol::COLTYPE::RAW) {
                // RAW stored as BLOB
                column->type = SysCol::COLTYPE::BLOB;
            }
        }

        switch (column->type) {
            case SysCol::COLTYPE::VARCHAR:
            case SysCol::COLTYPE::CHAR:
                parseString(data, size, column->charsetId, fileOffset, false, false, false, table->systemTable > DbTable::TABLE::NONE);
                columnString(column->name);
                break;

            case SysCol::COLTYPE::NUMBER:
                parseNumber(data, size, fileOffset);
                columnNumber(column->name, column->precision, column->scale);
                break;

            case SysCol::COLTYPE::BLOB:
                if (after) {
                    if (parseLob(lobCtx, data, size, 0, table->obj, fileOffset, false, table->sys)) {
                        if (column->xmlType && ctx->isFlagSet(Ctx::REDO_FLAGS::EXPERIMENTAL_XMLTYPE)) {
                            if (parseXml(xmlCtx, reinterpret_cast<const uint8_t*>(valueBuffer), valueSize, fileOffset))
                                columnString(column->name);
                            else
                                columnRaw(column->name, reinterpret_cast<const uint8_t*>(valueBufferOld), valueSizeOld);
                        } else
                            columnRaw(column->name, reinterpret_cast<const uint8_t*>(valueBuffer), valueSize);
                    }
                }
                break;

            case SysCol::COLTYPE::JSON:
                if (ctx->isFlagSet(Ctx::REDO_FLAGS::EXPERIMENTAL_JSON))
                    if (parseLob(lobCtx, data, size, 0, table->obj, fileOffset, false, table->sys))
                        columnRaw(column->name, reinterpret_cast<const uint8_t*>(valueBuffer), valueSize);
                break;

            case SysCol::COLTYPE::CLOB:
                if (after) {
                    if (parseLob(lobCtx, data, size, column->charsetId, table->obj, fileOffset, true, table->systemTable > DbTable::TABLE::NONE))
                        columnString(column->name);
                }
                break;

            case SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ:
                if (size != 7 && size != 11)
                    columnUnknown(column->name, data, size);
                else {
                    int year;
                    const int month = data[2] - 1;  // 0..11
                    const int day = data[3] - 1;    // 0..30
                    const int hour = data[4] - 1;   // 0..23
                    const int minute = data[5] - 1; // 0..59
                    const int second = data[6] - 1; // 0..59

                    int val1 = data[0];
                    int val2 = data[1];
                    // AD
                    if (val1 >= 100 && val2 >= 100) {
                        val1 -= 100;
                        val2 -= 100;
                        year = (val1 * 100) + val2;

                    } else {
                        val1 = 100 - val1;
                        val2 = 100 - val2;
                        year = -((val1 * 100) + val2);
                    }

                    uint64_t fraction = 0;
                    if (size == 11)
                        fraction = Ctx::read32Big(data + 7);

                    if (second < 0 || second > 59 || minute < 0 || minute > 59 || hour < 0 || hour > 23 || day < 0 || day > 30 || month < 0 || month > 11 ||
                            fraction > 999999999) {
                        columnUnknown(column->name, data, size);
                    } else {
                        time_t timestamp = Data::valuesToEpoch(year, month, day, hour, minute, second, metadata->dbTimezone);
                        if (year < 0 && fraction > 0) {
                            fraction = 1000000000 - fraction;
                            --timestamp;
                        }
                        columnTimestamp(column->name, timestamp, fraction);
                    }
                }
                break;

            case SysCol::COLTYPE::DATE:
            case SysCol::COLTYPE::TIMESTAMP:
                if (size != 7 && size != 11)
                    columnUnknown(column->name, data, size);
                else {
                    int year;
                    const int month = data[2] - 1;  // 0..11
                    const int day = data[3] - 1;    // 0..30
                    const int hour = data[4] - 1;   // 0..23
                    const int minute = data[5] - 1; // 0..59
                    const int second = data[6] - 1; // 0..59

                    int val1 = data[0];
                    int val2 = data[1];
                    // AD
                    if (val1 >= 100 && val2 >= 100) {
                        val1 -= 100;
                        val2 -= 100;
                        year = (val1 * 100) + val2;

                    } else {
                        val1 = 100 - val1;
                        val2 = 100 - val2;
                        year = -((val1 * 100) + val2);
                    }

                    uint64_t fraction = 0;
                    if (size == 11)
                        fraction = Ctx::read32Big(data + 7);

                    if (second < 0 || second > 59 || minute < 0 || minute > 59 || hour < 0 || hour > 23 || day < 0 || day > 30 || month < 0 || month > 11 ||
                            fraction > 999999999) {
                        columnUnknown(column->name, data, size);
                    } else {
                        time_t timestamp = Data::valuesToEpoch(year, month, day, hour, minute, second, 0);
                        if (year < 0 && fraction > 0) {
                            fraction = 1000000000 - fraction;
                            --timestamp;
                        }
                        columnTimestamp(column->name, timestamp, fraction);
                    }
                }
                break;

            case SysCol::COLTYPE::RAW:
                columnRaw(column->name, data, size);
                break;

            case SysCol::COLTYPE::FLOAT:
                if (size == 4)
                    columnFloat(column->name, decodeFloat(data));
                else
                    columnUnknown(column->name, data, size);
                break;

            case SysCol::COLTYPE::DOUBLE:
                if (size == 8)
                    columnDouble(column->name, decodeDouble(data));
                else
                    columnUnknown(column->name, data, size);
                break;

            case SysCol::COLTYPE::TIMESTAMP_WITH_TZ:
                if (size != 9 && size != 13) {
                    columnUnknown(column->name, data, size);
                } else {
                    int year;
                    const int month = data[2] - 1;  // 0..11
                    const int day = data[3] - 1;    // 0..30
                    const int hour = data[4] - 1;   // 0..23
                    const int minute = data[5] - 1; // 0..59
                    const int second = data[6] - 1; // 0..59

                    int val1 = data[0];
                    int val2 = data[1];
                    // AD
                    if (val1 >= 100 && val2 >= 100) {
                        val1 -= 100;
                        val2 -= 100;
                        year = (val1 * 100) + val2;

                    } else {
                        val1 = 100 - val1;
                        val2 = 100 - val2;
                        year = -((val1 * 100) + val2);
                    }

                    uint64_t fraction = 0;
                    if (size == 13)
                        fraction = Ctx::read32Big(data + 7);

                    char tz2[6] {'+', '0', '0', ':', '0', '0'};
                    std::string_view tz(tz2, 6);

                    if (data[11] >= 5 && data[11] <= 36) {
                        if (data[11] < 20 || (data[11] == 20 && data[12] < 60))
                            tz2[0] = '-';
                        else
                            tz2[0] = '+';

                        if (data[11] < 20) {
                            const uint val = 20 - data[11];
                            tz2[1] = Data::map10(val / 10);
                            tz2[2] = Data::map10(val % 10);
                        } else {
                            const uint val = data[11] - 20;
                            tz2[1] = Data::map10(val / 10);
                            tz2[2] = Data::map10(val % 10);
                        }

                        tz2[3] = ':';

                        if (data[12] < 60) {
                            const uint val = 60 - data[12];
                            tz2[4] = Data::map10(val / 10);
                            tz2[5] = Data::map10(val % 10);
                        } else {
                            const uint val = data[12] - 60;
                            tz2[4] = Data::map10(val / 10);
                            tz2[5] = Data::map10(val % 10);
                        }
                        tz = std::string_view(tz2, 6);
                    } else {
                        const uint16_t tzKey = (data[11] << 8) | data[12];
                        auto timeZoneMapIt = locales->timeZoneMap.find(tzKey);
                        if (timeZoneMapIt != locales->timeZoneMap.end())
                            tz = timeZoneMapIt->second;
                        else
                            tz = "TZ?";
                    }

                    if (second < 0 || second > 59 || minute < 0 || minute > 59 || hour < 0 || hour > 23 || day < 0 || day > 30 || month < 0 || month > 11) {
                        columnUnknown(column->name, data, size);
                    } else {
                        time_t timestamp = Data::valuesToEpoch(year, month, day, hour, minute, second, 0);
                        if (year < 0 && fraction > 0) {
                            fraction = 1000000000 - fraction;
                            --timestamp;
                        }
                        columnTimestampTz(column->name, timestamp, fraction, tz);
                    }
                }
                break;

            case SysCol::COLTYPE::INTERVAL_YEAR_TO_MONTH:
                if (size != 5 || data[4] < 49 || data[4] > 71)
                    columnUnknown(column->name, data, size);
                else {
                    bool minus = false;
                    uint64_t year;
                    if ((data[0] & 0x80) != 0)
                        year = Ctx::read32Big(data) - 0x80000000;
                    else {
                        year = 0x80000000 - Ctx::read32Big(data);
                        minus = true;
                    }

                    if (year > 999999999)
                        columnUnknown(column->name, data, size);
                    else {
                        uint64_t month;
                        if (data[4] >= 60)
                            month = data[4] - 60;
                        else {
                            month = 60 - data[4];
                            minus = true;
                        }

                        char buffer[12];
                        uint64_t len = 0;
                        valueSize = 0;

                        if (minus)
                            valueBuffer[valueSize++] = '-';

                        if (format.intervalYtmFormat == Format::INTERVAL_YTM_FORMAT::MONTHS ||
                                format.intervalYtmFormat == Format::INTERVAL_YTM_FORMAT::MONTHS_STRING) {
                            uint64_t val = (year * 12) + month;
                            if (val == 0) {
                                valueBuffer[valueSize++] = '0';
                            } else {
                                while (val != 0) {
                                    buffer[len++] = Data::map10(val % 10);
                                    val /= 10;
                                }
                                while (len > 0)
                                    valueBuffer[valueSize++] = buffer[--len];
                            }

                            if (format.intervalYtmFormat == Format::INTERVAL_YTM_FORMAT::MONTHS)
                                columnNumber(column->name, 17, 0);
                            else
                                columnString(column->name);
                        } else {
                            uint64_t val = year;
                            if (val == 0) {
                                valueBuffer[valueSize++] = '0';
                            } else {
                                while (val != 0) {
                                    buffer[len++] = Data::map10(val % 10);
                                    val /= 10;
                                }
                                while (len > 0)
                                    valueBuffer[valueSize++] = buffer[--len];
                            }

                            if (format.intervalYtmFormat == Format::INTERVAL_YTM_FORMAT::STRING_YM_SPACE)
                                valueBuffer[valueSize++] = ' ';
                            else if (format.intervalYtmFormat == Format::INTERVAL_YTM_FORMAT::STRING_YM_COMMA)
                                valueBuffer[valueSize++] = ',';
                            else if (format.intervalYtmFormat == Format::INTERVAL_YTM_FORMAT::STRING_YM_DASH)
                                valueBuffer[valueSize++] = '-';

                            if (month >= 10) {
                                valueBuffer[valueSize++] = '1';
                                valueBuffer[valueSize++] = Data::map10(month - 10);
                            } else
                                valueBuffer[valueSize++] = Data::map10(month);

                            columnString(column->name);
                        }
                    }
                }
                break;

            case SysCol::COLTYPE::INTERVAL_DAY_TO_SECOND:
                if (size != 11 || data[4] < 37 || data[4] > 83 || data[5] < 1 || data[5] > 119 || data[6] < 1 || data[6] > 119)
                    columnUnknown(column->name, data, size);
                else {
                    bool minus = false;
                    uint64_t day;
                    if ((data[0] & 0x80) != 0)
                        day = Ctx::read32Big(data) - 0x80000000;
                    else {
                        day = 0x80000000 - Ctx::read32Big(data);
                        minus = true;
                    }

                    int32_t us;
                    if ((data[7] & 0x80) != 0)
                        us = Ctx::read32Big(data + 7) - 0x80000000;
                    else {
                        us = 0x80000000 - Ctx::read32Big(data + 7);
                        minus = true;
                    }

                    if (day > 999999999 || us > 999999999)
                        columnUnknown(column->name, data, size);
                    else {
                        int64_t hour;
                        if (data[4] >= 60)
                            hour = data[4] - 60;
                        else {
                            hour = 60 - data[4];
                            minus = true;
                        }

                        int64_t minute;
                        if (data[5] >= 60)
                            minute = data[5] - 60;
                        else {
                            minute = 60 - data[5];
                            minus = true;
                        }

                        int64_t second;
                        if (data[6] >= 60)
                            second = data[6] - 60;
                        else {
                            second = 60 - data[6];
                            minus = true;
                        }

                        char buffer[30];
                        valueSize = 0;
                        uint64_t val = 0;
                        uint64_t len = 0;

                        if (minus)
                            valueBuffer[valueSize++] = '-';

                        if (format.intervalDtsFormat == Format::INTERVAL_DTS_FORMAT::ISO8601_SPACE ||
                                format.intervalDtsFormat == Format::INTERVAL_DTS_FORMAT::ISO8601_COMMA ||
                                format.intervalDtsFormat == Format::INTERVAL_DTS_FORMAT::ISO8601_DASH) {

                            val = day;
                            if (day == 0) {
                                valueBuffer[valueSize++] = '0';
                            } else {
                                while (val != 0) {
                                    buffer[len++] = Data::map10(val % 10);
                                    val /= 10;
                                }
                                while (len > 0)
                                    valueBuffer[valueSize++] = buffer[--len];
                            }

                            if (format.intervalDtsFormat == Format::INTERVAL_DTS_FORMAT::ISO8601_SPACE)
                                valueBuffer[valueSize++] = ' ';
                            else if (format.intervalDtsFormat == Format::INTERVAL_DTS_FORMAT::ISO8601_COMMA)
                                valueBuffer[valueSize++] = ',';
                            else if (format.intervalDtsFormat == Format::INTERVAL_DTS_FORMAT::ISO8601_DASH)
                                valueBuffer[valueSize++] = '-';

                            valueBuffer[valueSize++] = Data::map10(hour / 10);
                            valueBuffer[valueSize++] = Data::map10(hour % 10);
                            valueBuffer[valueSize++] = ':';
                            valueBuffer[valueSize++] = Data::map10(minute / 10);
                            valueBuffer[valueSize++] = Data::map10(minute % 10);
                            valueBuffer[valueSize++] = ':';
                            valueBuffer[valueSize++] = Data::map10(second / 10);
                            valueBuffer[valueSize++] = Data::map10(second % 10);
                            valueBuffer[valueSize++] = '.';

                            for (uint j = 0; j < 9; ++j) {
                                valueBuffer[valueSize + 8 - j] = Data::map10(us % 10);
                                us /= 10;
                            }
                            valueSize += 9;

                            columnString(column->name);
                        } else {
                            switch (format.intervalDtsFormat) {
                                case Format::INTERVAL_DTS_FORMAT::UNIX_NANO:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_NANO_STRING:
                                    val = (((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us;
                                    break;

                                case Format::INTERVAL_DTS_FORMAT::UNIX_MICRO:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_MICRO_STRING:
                                    val = ((((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us + 500) / 1000;
                                    break;

                                case Format::INTERVAL_DTS_FORMAT::UNIX_MILLI:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_MILLI_STRING:
                                    val = ((((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us + 500000) / 1000000;
                                    break;

                                case Format::INTERVAL_DTS_FORMAT::UNIX:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_STRING:
                                    val = ((((day * 24 + hour) * 60 + minute) * 60 + second) * 1000000000 + us + 500000000) / 1000000000;
                                    break;

                                default:
                                    break;
                            }

                            if (val == 0) {
                                valueBuffer[valueSize++] = '0';
                            } else {
                                while (val != 0) {
                                    buffer[len++] = Data::map10(val % 10);
                                    val /= 10;
                                }
                                while (len > 0)
                                    valueBuffer[valueSize++] = buffer[--len];
                            }

                            switch (format.intervalDtsFormat) {
                                case Format::INTERVAL_DTS_FORMAT::UNIX_NANO:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_MICRO:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_MILLI:
                                case Format::INTERVAL_DTS_FORMAT::UNIX:
                                    columnNumber(column->name, 17, 0);
                                    break;

                                case Format::INTERVAL_DTS_FORMAT::UNIX_NANO_STRING:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_MICRO_STRING:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_MILLI_STRING:
                                case Format::INTERVAL_DTS_FORMAT::UNIX_STRING:
                                    columnString(column->name);
                                    break;

                                default:
                                    break;
                            }
                        }
                    }
                }
                break;

            case SysCol::COLTYPE::BOOLEAN:
                if (size == 1 && data[0] <= 1) {
                    valueSize = 0;
                    valueBuffer[valueSize++] = Data::map10(data[0]);
                    columnNumber(column->name, column->precision, column->scale);
                } else {
                    columnUnknown(column->name, data, size);
                }
                break;

            case SysCol::COLTYPE::UROWID:
                if (size == 13 && data[0] == 0x01) {
                    RowId rowId;
                    rowId.decodeFromHex(data + 1);
                    columnRowId(column->name, rowId);
                } else {
                    columnUnknown(column->name, data, size);
                }
                break;

            default:
                if (format.unknownType == Format::UNKNOWN_TYPE::SHOW)
                    columnUnknown(column->name, data, size);
        }
    }

    double Builder::decodeFloat(const uint8_t* data) {
        const uint8_t sign = data[0] & 0x80;
        int64_t exponent = (static_cast<uint64_t>(data[0] & 0x7F) << 1) | (static_cast<uint64_t>(data[1]) >> 7);
        uint64_t significand = (static_cast<uint64_t>(data[1] & 0x7F) << 16) | (static_cast<uint64_t>(data[2]) << 8) | static_cast<uint64_t>(data[3]);

        if (sign != 0) {
            if (significand == 0) {
                if (exponent == 0)
                    return 0.0;
                if (exponent == 0xFF)
                    return std::numeric_limits<double>::infinity();
            } else if (significand == 0x400000 && exponent == 0xFF)
                return std::numeric_limits<double>::quiet_NaN();

            if (exponent > 0)
                significand += 0x800000;
            exponent -= 0x7F;
            return ldexp((static_cast<double>(significand)) / (static_cast<double>(0x800000)), exponent);
        }

        if (exponent == 0 && significand == 0x7FFFFF)
            return -std::numeric_limits<double>::infinity();

        significand = 0x7FFFFF - significand;
        if (exponent < 0xFF)
            significand += 0x800000;
        exponent = 0x80 - exponent;
        return -ldexp(((static_cast<double>(significand) / static_cast<double>(0x800000))), exponent);
    }

    long double Builder::decodeDouble(const uint8_t* data) {
        const uint8_t sign = data[0] & 0x80;
        int64_t exponent = (static_cast<uint64_t>(data[0] & 0x7F) << 4) | (static_cast<uint64_t>(data[1]) >> 4);
        uint64_t significand = (static_cast<uint64_t>(data[1] & 0x0F) << 48) | (static_cast<uint64_t>(data[2]) << 40) |
                               (static_cast<uint64_t>(data[3]) << 32) | (static_cast<uint64_t>(data[4]) << 24) | (static_cast<uint64_t>(data[5]) << 16) |
                               (static_cast<uint64_t>(data[6]) << 8) | static_cast<uint64_t>(data[7]);

        if (sign != 0) {
            if (significand == 0) {
                if (exponent == 0)
                    return 0.0L;
                if (exponent == 0x7FF)
                    return std::numeric_limits<long double>::infinity();
            } else if (significand == 0x8000000000000 && exponent == 0x7FF)
                return std::numeric_limits<long double>::quiet_NaN();

            if (exponent > 0)
                significand += 0x10000000000000;
            exponent -= 0x3FF;
            return ldexpl(static_cast<long double>(significand) / static_cast<long double>(0x10000000000000), exponent);
        }

        if (exponent == 0 && significand == 0xFFFFFFFFFFFFF)
            return -std::numeric_limits<long double>::infinity();

        significand = 0xFFFFFFFFFFFFF - significand;
        if (exponent < 0x7FF)
            significand += 0x10000000000000;
        exponent = 0x400 - exponent;
        return -ldexpl(static_cast<long double>(significand) / static_cast<long double>(0x10000000000000), exponent);
    }

    uint64_t Builder::builderSize() const {
        return ((messageSize + messagePosition + 7) & 0xFFFFFFFFFFFFFFF8);
    }

    uint64_t Builder::getMaxMessageMb() const {
        return maxMessageMb;
    }

    void Builder::setMaxMessageMb(uint64_t maxMessageMb_) {
        maxMessageMb = maxMessageMb_;
    }

    void Builder::processBegin(Xid xid, uint16_t newThread, Seq newBeginSequence, Scn newBeginScn, Time newBeginTimestamp, Seq newCommitSequence,
                               Scn newCommitScn, Time newCommitTimestamp, const AttributeMap* newAttributes) {
        lastXid = xid;
        thread = newThread;
        beginSequence = newBeginSequence;
        beginScn = newBeginScn;
        beginTimestamp = newBeginTimestamp;
        commitSequence = newCommitSequence;
        commitScn = newCommitScn;
        commitTimestamp = newCommitTimestamp;
        if (lwnScn != beginScn) {
            lwnScn = beginScn;
            lwnIdx = 0;
        }
        newTran = true;
        attributes = newAttributes;

        if (attributes->empty()) {
            metadata->ctx->warning(50065, "empty attributes for XID: " + lastXid.toString());
        }
    }

    // 0x05010B0B
    void Builder::processInsertMultiple(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                        const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump) {
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;
        typeSize colSize;
        const DbTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if (format.isScnTypeCommitValue())
            scn = commitScn;

        while (fieldNum < redoLogRecord2->rowData)
            RedoLogRecord::nextField(ctx, redoLogRecord2, fieldNum, fieldPos, fieldSize, 0x000001);

        typePos fieldPosStart = fieldPos;

        for (typeCC r = 0; r < redoLogRecord2->nRow; ++r) {
            typePos pos = 0;
            fieldPos = fieldPosStart;
            const typeCC jcc = *redoLogRecord2->data(fieldPos + pos + 2);
            pos = 3;

            if ((redoLogRecord2->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0) {
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    pos += 6;
                else
                    pos += 8;
            }

            typeCol maxI;
            if (likely(table != nullptr))
                maxI = table->maxSegCol;
            else
                maxI = jcc;

            for (typeCol i = 0; i < maxI; ++i) {
                if (i >= jcc) {
                    colSize = 0;
                } else {
                    colSize = *redoLogRecord2->data(fieldPos + pos);
                    ++pos;
                    if (colSize == 0xFF) {
                        colSize = 0;
                    } else if (colSize == 0xFE) {
                        colSize = ctx->read16(redoLogRecord2->data(fieldPos + pos));
                        pos += 2;
                    }
                }

                if (colSize > 0 || format.columnFormat >= Format::COLUMN_FORMAT::FULL_INS_DEC || table == nullptr || table->columns[i]->numPk > 0)
                    valueSet(Format::VALUE_TYPE::AFTER, i, redoLogRecord2->data(fieldPos + pos), colSize, 0, dump);
                pos += colSize;
            }

            if (system && table != nullptr && DbTable::isSystemTable(table->options))
                systemTransaction->processInsert(table, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                                                 ctx->read16(redoLogRecord2->data(redoLogRecord2->slotsDelta + (r * 2))),
                                                 redoLogRecord1->fileOffset);

            if ((!schema && table != nullptr && !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options) &&
                    table->matchesCondition(ctx, 'i', attributes)) || ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_SYSTEM_TRANSACTIONS) ||
                    ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                processInsert(sequence, scn, timestamp, lobCtx, xmlCtx, table, redoLogRecord2->obj, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                              ctx->read16(redoLogRecord2->data(redoLogRecord2->slotsDelta + (r * 2))), redoLogRecord1->fileOffset);
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsInsertOut(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsInsertOut(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsInsertOut(1);
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsInsertSkip(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsInsertSkip(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsInsertSkip(1);
                }
            }

            releaseValues();

            fieldPosStart += ctx->read16(redoLogRecord2->data(redoLogRecord2->rowSizesDelta + (r * 2)));
        }
    }

    // 0x05010B0C
    void Builder::processDeleteMultiple(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const RedoLogRecord* redoLogRecord1,
                                        const RedoLogRecord* redoLogRecord2, bool system, bool schema, bool dump) {
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;
        typeSize colSize;
        const DbTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if (format.isScnTypeCommitValue())
            scn = commitScn;

        while (fieldNum < redoLogRecord1->rowData)
            RedoLogRecord::nextField(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x000002);

        typePos fieldPosStart = fieldPos;

        for (typeCC r = 0; r < redoLogRecord1->nRow; ++r) {
            typePos pos = 0;
            fieldPos = fieldPosStart;
            const typeCC jcc = *redoLogRecord1->data(fieldPos + pos + 2);
            pos = 3;

            if ((redoLogRecord1->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0) {
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    pos += 6;
                else
                    pos += 8;
            }

            typeCol maxI;
            if (likely(table != nullptr))
                maxI = table->maxSegCol;
            else
                maxI = jcc;

            for (typeCol i = 0; i < maxI; ++i) {
                if (i >= jcc) {
                    colSize = 0;
                } else {
                    colSize = *redoLogRecord1->data(fieldPos + pos);
                    ++pos;
                    if (colSize == 0xFF) {
                        colSize = 0;
                    } else if (colSize == 0xFE) {
                        colSize = ctx->read16(redoLogRecord1->data(fieldPos + pos));
                        pos += 2;
                    }
                }

                if (colSize > 0 || format.columnFormat >= Format::COLUMN_FORMAT::FULL_INS_DEC || table == nullptr || table->columns[i]->numPk > 0)
                    valueSet(Format::VALUE_TYPE::BEFORE, i, redoLogRecord1->data(fieldPos + pos), colSize, 0, dump);
                pos += colSize;
            }

            if (system && table != nullptr && DbTable::isSystemTable(table->options))
                systemTransaction->processDelete(table, redoLogRecord2->dataObj, redoLogRecord2->bdba,
                                                 ctx->read16(redoLogRecord1->data(redoLogRecord1->slotsDelta + (r * 2))),
                                                 redoLogRecord1->fileOffset);

            if ((!schema && table != nullptr && !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options) &&
                    table->matchesCondition(ctx, 'd', attributes)) || ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_SYSTEM_TRANSACTIONS) ||
                    ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                processDelete(sequence, scn, timestamp, lobCtx, xmlCtx, table, redoLogRecord2->obj, redoLogRecord2->dataObj,
                              redoLogRecord2->bdba, ctx->read16(redoLogRecord1->data(redoLogRecord1->slotsDelta + (r * 2))),
                              redoLogRecord1->fileOffset);
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteOut(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteOut(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsDeleteOut(1);
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteSkip(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteSkip(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsDeleteSkip(1);
                }
            }

            releaseValues();

            fieldPosStart += ctx->read16(redoLogRecord1->data(redoLogRecord1->rowSizesDelta + (r * 2)));
        }
    }

    void Builder::processDml(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const std::deque<const RedoLogRecord*>& redo1,
                             const std::deque<const RedoLogRecord*>& redo2, Format::TRANSACTION_TYPE transactionType, bool system, bool schema, bool dump) {
        uint8_t fb;
        typeObj obj;
        typeDataObj dataObj;
        typeDba bdba;
        typeSlot slot;
        const RedoLogRecord* redoLogRecord1p;
        const RedoLogRecord* redoLogRecord2p = nullptr;
        auto it1 = redo1.cbegin();
        auto it2 = redo2.cbegin();
        const RedoLogRecord* redoLogRecord1 = *it1;
        const RedoLogRecord* redoLogRecord2 = *it2;

        DbTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if (format.isScnTypeCommitValue())
            scn = commitScn;

        if (transactionType == Format::TRANSACTION_TYPE::INSERT) {
            for (const auto* it3: redo2) {
                if ((it3->fb & RedoLogRecord::FB_F) != 0) {
                    redoLogRecord2p = it3;
                    break;
                }
            }

            if (redoLogRecord2p == nullptr) {
                ctx->warning(60001, "incomplete row for table (obj: " + std::to_string(redoLogRecord1->obj) + "), probably IOT, offset: " +
                             redoLogRecord1->fileOffset.toString() + ", xid: " + lastXid.toString());
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

        typeSize colSize;
        uint16_t colNum = 0;
        typeSize colShift;
        const uint8_t* nulls;
        uint8_t bits;
        const uint8_t* colNums;
        bool suppPrev = false;

        // Data in UNDO
        redoLogRecord1p = redoLogRecord1;
        redoLogRecord2p = redoLogRecord2;
        colNums = nullptr;

        while (redoLogRecord1p != nullptr) {
            typePos fieldPos = 0;
            typeField fieldNum = 0;
            typeSize fieldSize = 0;

            // UNDO
            if (redoLogRecord1p->rowData > 0) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DML) || dump)) {
                    ctx->logTrace(Ctx::TRACE::DML, "UNDO");
                }

                nulls = redoLogRecord1p->data(redoLogRecord1p->nullsDelta);
                bits = 1;

                if (redoLogRecord1p->suppLogBefore > 0)
                    colShift = redoLogRecord1p->suppLogBefore - 1;
                else
                    colShift = 0;

                if (redoLogRecord1p->colNumsDelta > 0 && !redoLogRecord1p->compressed) {
                    colNums = redoLogRecord1p->data(redoLogRecord1p->colNumsDelta);
                    colShift -= ctx->read16(colNums);
                } else {
                    colNums = nullptr;
                }
                if (unlikely(colShift >= ctx->columnLimit))
                    throw RedoLogException(50059, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                           std::to_string(redoLogRecord1p->dataObj) + "): invalid column shift: " + std::to_string(colShift) +
                                           ", before: " + std::to_string(redoLogRecord1p->suppLogBefore) + ", xid: " + lastXid.toString() +
                                           ", offset: " + redoLogRecord1p->fileOffset.toString());

                while (fieldNum < redoLogRecord1p->rowData - 1U)
                    RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldSize, 0x000003);

                typeCC cc = redoLogRecord1p->cc;
                if (redoLogRecord1p->compressed) {
                    if (redoLogRecord1p->sizeDelt > 0)
                        cc = 1;
                    else
                        cc = 0;
                    compressedBefore = true;
                }

                for (typeCC i = 0; i < cc; ++i) {
                    if (colNums != nullptr) {
                        colNum = ctx->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (unlikely(fieldNum + 1U > redoLogRecord1p->fieldCnt)) {
                        if (likely(table != nullptr))
                            throw RedoLogException(50014, "table: " + table->owner + "." + table->name + ": out of columns (Undo): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint>(redoLogRecord1p->cc)) + ", " +
                                                   std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                                   std::to_string(redoLogRecord1p->rowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                                   ", xid: " + lastXid.toString() + ", offset: " + redoLogRecord1p->fileOffset.toString());

                        throw RedoLogException(50014, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                               std::to_string(redoLogRecord1p->dataObj) + "): out of columns (Undo): " + std::to_string(colNum) +
                                               "/" + std::to_string(static_cast<uint>(redoLogRecord1p->cc)) + ", " +
                                               std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                               std::to_string(redoLogRecord1p->rowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                               ", xid: " + lastXid.toString() + ", offset: " + redoLogRecord1p->fileOffset.toString());
                    }

                    fb = 0;
                    if (i == 0 && (redoLogRecord1p->fb & RedoLogRecord::FB_P) != 0)
                        fb |= RedoLogRecord::FB_P;
                    if (i == redoLogRecord1p->cc - 1U && (redoLogRecord1p->fb & RedoLogRecord::FB_N) != 0)
                        fb |= RedoLogRecord::FB_N;

                    if (likely(table != nullptr)) {
                        if (colNum >= table->maxSegCol) {
                            if (unlikely(!schema))
                                throw RedoLogException(50060, "table: " + table->owner + "." + table->name +
                                                       ": referring to invalid column id(" + std::to_string(colNum) + ", xid: " + lastXid.toString() +
                                                       "), offset: " + redoLogRecord1p->fileOffset.toString());
                            break;
                        }
                    } else {
                        if (unlikely(colNum >= ctx->columnLimit))
                            throw RedoLogException(50060, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord1p->dataObj) + "): referring to invalid column id(" +
                                                   std::to_string(colNum) + "), xid: " + lastXid.toString() + ", offset: " +
                                                   redoLogRecord1p->fileOffset.toString());
                    }

                    if ((*nulls & bits) != 0)
                        colSize = 0;
                    else {
                        RedoLogRecord::skipEmptyFields(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldSize);
                        RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldSize, 0x000004);
                        colSize = fieldSize;
                    }

                    valueSet(Format::VALUE_TYPE::BEFORE, colNum, redoLogRecord1p->data(fieldPos), colSize, fb, dump);

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            // Supplemental columns
            if (redoLogRecord1p->suppLogRowData > 0) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DML) || dump)) {
                    ctx->logTrace(Ctx::TRACE::DML, "UNDO SUP");
                }

                while (fieldNum < redoLogRecord1p->suppLogRowData - 1U)
                    RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldSize, 0x000005);

                colNums = redoLogRecord1p->data(redoLogRecord1p->suppLogNumsDelta);
                const uint8_t* colSizes = redoLogRecord1p->data(redoLogRecord1p->suppLogLenDelta);

                for (uint16_t i = 0; i < redoLogRecord1p->suppLogCC; ++i) {
                    colNum = ctx->read16(colNums) - 1;
                    if (unlikely(fieldNum + 1U > redoLogRecord1p->fieldCnt)) {
                        if (likely(table != nullptr))
                            throw RedoLogException(50014, "table: " + table->owner + "." + table->name + ": out of columns (supp): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint>(redoLogRecord1p->cc)) + ", " +
                                                   std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                                   std::to_string(redoLogRecord1p->suppLogRowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                                   ", xid: " + lastXid.toString() + ", offset: " + redoLogRecord1p->fileOffset.toString());

                        throw RedoLogException(50014, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                               std::to_string(redoLogRecord1p->dataObj) + "): out of columns (Supp): " + std::to_string(colNum) +
                                               "/" + std::to_string(static_cast<uint>(redoLogRecord1p->cc)) + ", " +
                                               std::to_string(redoLogRecord1p->sizeDelt) + ", " + std::to_string(fieldNum) + "-" +
                                               std::to_string(redoLogRecord1p->suppLogRowData) + "-" + std::to_string(redoLogRecord1p->fieldCnt) +
                                               ", xid: " + lastXid.toString() + ", offset: " + redoLogRecord1p->fileOffset.toString());
                    }

                    RedoLogRecord::nextField(ctx, redoLogRecord1p, fieldNum, fieldPos, fieldSize, 0x000006);

                    if (likely(table != nullptr)) {
                        if (colNum >= table->maxSegCol) {
                            if (unlikely(!schema))
                                throw RedoLogException(50060, "table: " + table->owner + "." + table->name +
                                                       ": referring to invalid column id(" + std::to_string(colNum) + "), xid: " + lastXid.toString() +
                                                       ", offset: " + redoLogRecord1p->fileOffset.toString());
                            break;
                        }
                    } else {
                        if (unlikely(colNum >= ctx->columnLimit))
                            throw RedoLogException(50060, "table: (obj: " + std::to_string(redoLogRecord1p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord1p->dataObj) + "): referring to invalid column id(" +
                                                   std::to_string(colNum) + "), xid: " + lastXid.toString() + ", offset: " +
                                                   redoLogRecord1p->fileOffset.toString());
                    }

                    colNums += 2;
                    colSize = ctx->read16(colSizes);

                    if (colSize == 0xFFFF)
                        colSize = 0;

                    fb = 0;
                    if (i == 0 && (redoLogRecord1p->suppLogFb & RedoLogRecord::FB_P) != 0 && suppPrev) {
                        fb |= RedoLogRecord::FB_P;
                        suppPrev = false;
                    }
                    if (i == redoLogRecord1p->suppLogCC - 1U && (redoLogRecord1p->suppLogFb & RedoLogRecord::FB_N) != 0) {
                        fb |= RedoLogRecord::FB_N;
                        suppPrev = true;
                    }

                    // Insert, lock, update, supplemental log data
                    if (redoLogRecord2p->opCode == 0x0B02 || redoLogRecord2p->opCode == 0x0B04 || redoLogRecord2p->opCode == 0x0B05 ||
                        redoLogRecord2p->opCode == 0x0B10)
                        valueSet(Format::VALUE_TYPE::AFTER_SUPP, colNum, redoLogRecord1p->data(fieldPos), colSize, fb, dump);

                    // Delete, update, overwrite, supplemental log data
                    if (redoLogRecord2p->opCode == 0x0B03 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B06 ||
                        redoLogRecord2p->opCode == 0x0B10)
                        valueSet(Format::VALUE_TYPE::BEFORE_SUPP, colNum, redoLogRecord1p->data(fieldPos), colSize, fb, dump);

                    colSizes += 2;
                }
            }

            // REDO
            if (redoLogRecord2p->rowData > 0) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DML) || dump)) {
                    ctx->logTrace(Ctx::TRACE::DML, "REDO");
                }

                fieldPos = 0;
                fieldNum = 0;
                fieldSize = 0;
                nulls = redoLogRecord2p->data(redoLogRecord2p->nullsDelta);
                bits = 1;

                if (redoLogRecord2p->suppLogAfter > 0)
                    colShift = redoLogRecord2p->suppLogAfter - 1;
                else
                    colShift = 0;

                if (redoLogRecord2p->colNumsDelta > 0 && !redoLogRecord2p->compressed) {
                    colNums = redoLogRecord2p->data(redoLogRecord2p->colNumsDelta);
                    colShift -= ctx->read16(colNums);
                } else {
                    colNums = nullptr;
                }
                if (unlikely(colShift >= ctx->columnLimit)) {
                    throw RedoLogException(50059, "table: (obj: " + std::to_string(redoLogRecord2p->obj) + ", dataobj: " +
                                           std::to_string(redoLogRecord2p->dataObj) + "): invalid column shift: " + std::to_string(colShift) +
                                           ", before: " + std::to_string(redoLogRecord2p->suppLogBefore) + ", xid: " + lastXid.toString() +
                                           ", offset: " + redoLogRecord2p->fileOffset.toString());
                }

                while (fieldNum < redoLogRecord2p->rowData - 1U)
                    RedoLogRecord::nextField(ctx, redoLogRecord2p, fieldNum, fieldPos, fieldSize, 0x000007);

                typeCC cc = redoLogRecord2p->cc;
                if (redoLogRecord2p->compressed) {
                    if (redoLogRecord2p->sizeDelt > 0)
                        cc = 1;
                    else
                        cc = 0;
                    compressedAfter = true;
                } else
                    compressedAfter = false;

                for (typeCC i = 0; i < cc; ++i) {
                    if (unlikely(fieldNum + 1U > redoLogRecord2p->fieldCnt)) {
                        if (likely(table != nullptr))
                            throw RedoLogException(50014, "table: " + table->owner + "." + table->name + ": out of columns (Redo): " +
                                                   std::to_string(colNum) + "/" + std::to_string(static_cast<uint>(redoLogRecord2p->cc)) + ", " +
                                                   std::to_string(redoLogRecord2p->sizeDelt) + ", " + std::to_string(fieldNum) + ", " +
                                                   std::to_string(fieldNum) + "-" + std::to_string(redoLogRecord2p->rowData) + "-" +
                                                   std::to_string(redoLogRecord2p->fieldCnt) + ", xid: " + lastXid.toString() + ", offset: " +
                                                   redoLogRecord2p->fileOffset.toString());

                        throw RedoLogException(50014, "table: (obj: " + std::to_string(redoLogRecord2p->obj) + ", dataobj: " +
                                               std::to_string(redoLogRecord2p->dataObj) + "): out of columns (Redo): " +
                                               std::to_string(colNum) + "/" + std::to_string(static_cast<uint>(redoLogRecord2p->cc)) + ", " +
                                               std::to_string(redoLogRecord2p->sizeDelt) + ", " + std::to_string(fieldNum) + ", " +
                                               std::to_string(fieldNum) + "-" + std::to_string(redoLogRecord2p->rowData) + "-" +
                                               std::to_string(redoLogRecord2p->fieldCnt) + ", xid: " + lastXid.toString() + ", offset: " +
                                               redoLogRecord2p->fileOffset.toString());
                    }

                    fb = 0;
                    if (i == 0 && (redoLogRecord2p->fb & RedoLogRecord::FB_P) != 0)
                        fb |= RedoLogRecord::FB_P;
                    if (i == static_cast<uint64_t>(redoLogRecord2p->cc - 1U) && (redoLogRecord2p->fb & RedoLogRecord::FB_N) != 0)
                        fb |= RedoLogRecord::FB_N;

                    RedoLogRecord::nextField(ctx, redoLogRecord2p, fieldNum, fieldPos, fieldSize, 0x000008);

                    if (colNums != nullptr) {
                        colNum = ctx->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (likely(table != nullptr)) {
                        if (unlikely(colNum >= table->maxSegCol)) {
                            if (!schema)
                                throw RedoLogException(50060, "table: " + table->owner + "." + table->name +
                                                       ": referring to invalid column id(" + std::to_string(colNum) +
                                                       "), xid: " + lastXid.toString() + ", offset: " + redoLogRecord2p->fileOffset.toString());
                            break;
                        }
                    } else {
                        if (unlikely(colNum >= ctx->columnLimit))
                            throw RedoLogException(50060, "table: (obj: " + std::to_string(redoLogRecord2p->obj) + ", dataobj: " +
                                                   std::to_string(redoLogRecord2p->dataObj) + "): referring to invalid column id(" +
                                                   std::to_string(colNum) + "), xid: " + lastXid.toString() + "), offset: " +
                                                   redoLogRecord2p->fileOffset.toString());
                    }

                    if ((*nulls & bits) != 0)
                        colSize = 0;
                    else
                        colSize = fieldSize;

                    valueSet(Format::VALUE_TYPE::AFTER, colNum, redoLogRecord2p->data(fieldPos), colSize, fb, dump);

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            ++it1;
            ++it2;
            if (it1 == redo1.cend() || it2 == redo2.cend())
                break;

            redoLogRecord1p = *it1;
            redoLogRecord2p = *it2;
        }

        typeCol guardPos = -1;
        if (likely(table != nullptr)) {
            if (table->guardSegNo != -1)
                guardPos = table->guardSegNo;
        }

        typeCol baseMax = valuesMax >> 6;
        for (typeCol base = 0; base <= baseMax; ++base) {
            const typeCol columnBase = base << 6;
            typeMask set = valuesSet[base];
            while (set != 0) {
                const typeCol pos = ffsll(set) - 1;
                const typeMask mask = 1ULL << pos;
                set &= ~mask;
                const typeCol column = columnBase + pos;

                // Merge column values
                if ((valuesMerge[base] & mask) != 0) {
                    for (int j = 0; j < 4; ++j) {
                        uint64_t mergeSize = 0;

                        if (valuesPart[0][column][j] != nullptr)
                            mergeSize += sizesPart[0][column][j];
                        if (valuesPart[1][column][j] != nullptr)
                            mergeSize += sizesPart[1][column][j];
                        if (valuesPart[2][column][j] != nullptr)
                            mergeSize += sizesPart[2][column][j];

                        if (mergeSize == 0)
                            continue;

                        if (unlikely(values[column][j] != nullptr))
                            throw RedoLogException(50015, "value for " + std::to_string(column) + "/" + std::to_string(j) +
                                                   " is already set when merging, xid: " + lastXid.toString() + ", offset: " +
                                                   redoLogRecord1->fileOffset.toString());

                        auto* buffer = new uint8_t[mergeSize];
                        merges[mergesMax++] = buffer;

                        values[column][j] = buffer;
                        sizes[column][j] = mergeSize;

                        if (valuesPart[0][column][j] != nullptr) {
                            memcpy(buffer, valuesPart[0][column][j], sizesPart[0][column][j]);
                            buffer += sizesPart[0][column][j];
                            valuesPart[0][column][j] = nullptr;
                        }
                        if (valuesPart[1][column][j] != nullptr) {
                            memcpy(buffer, valuesPart[1][column][j], sizesPart[1][column][j]);
                            buffer += sizesPart[1][column][j];
                            valuesPart[1][column][j] = nullptr;
                        }
                        if (valuesPart[2][column][j] != nullptr) {
                            memcpy(buffer, valuesPart[2][column][j], sizesPart[2][column][j]);
                            buffer += sizesPart[2][column][j];
                            valuesPart[2][column][j] = nullptr;
                        }
                    }
                    valuesMerge[base] &= ~mask;
                }

                if (values[column][+Format::VALUE_TYPE::BEFORE] == nullptr) {
                    bool guardPresent = false;
                    if (guardPos != -1 && table->columns[column]->guardSeg != -1 &&
                            values[guardPos][+Format::VALUE_TYPE::BEFORE] != nullptr) {
                        const typeCol column2 = table->columns[column]->guardSeg;
                        const uint8_t* guardData = values[guardPos][+Format::VALUE_TYPE::BEFORE];
                        if (guardData != nullptr && static_cast<int64_t>(column2 / static_cast<typeCol>(8)) <
                                sizes[guardPos][+Format::VALUE_TYPE::BEFORE]) {
                            guardPresent = true;
                            if ((values[guardPos][+Format::VALUE_TYPE::BEFORE][column2 / 8] & (1 << (column2 & 7))) != 0) {
                                values[column][+Format::VALUE_TYPE::BEFORE] = reinterpret_cast<const uint8_t*>(1);
                                sizes[column][+Format::VALUE_TYPE::BEFORE] = 0;
                            }
                        }
                    }

                    if (!guardPresent && values[column][+Format::VALUE_TYPE::BEFORE_SUPP] != nullptr) {
                        values[column][+Format::VALUE_TYPE::BEFORE] = values[column][+Format::VALUE_TYPE::BEFORE_SUPP];
                        sizes[column][+Format::VALUE_TYPE::BEFORE] = sizes[column][+Format::VALUE_TYPE::BEFORE_SUPP];
                    }
                } else if (sizes[column][+Format::VALUE_TYPE::BEFORE] == 0 &&
                        values[column][+Format::VALUE_TYPE::BEFORE_SUPP] != nullptr) {
                    values[column][+Format::VALUE_TYPE::BEFORE] = values[column][+Format::VALUE_TYPE::BEFORE_SUPP];
                    sizes[column][+Format::VALUE_TYPE::BEFORE] = sizes[column][+Format::VALUE_TYPE::BEFORE_SUPP];
                }

                if (values[column][+Format::VALUE_TYPE::AFTER] == nullptr) {
                    bool guardPresent = false;
                    if (guardPos != -1 && table->columns[column]->guardSeg != -1 && values[guardPos][+Format::VALUE_TYPE::AFTER] != nullptr) {
                        const typeCol column2 = table->columns[column]->guardSeg;
                        const uint8_t* guardData = values[guardPos][+Format::VALUE_TYPE::AFTER];
                        if (guardData != nullptr && static_cast<int64_t>(column2 / static_cast<typeCol>(8)) <
                                sizes[guardPos][+Format::VALUE_TYPE::AFTER]) {
                            guardPresent = true;
                            if ((values[guardPos][+Format::VALUE_TYPE::AFTER][column2 / 8] & (1 << (column2 & 7))) != 0) {
                                values[column][+Format::VALUE_TYPE::AFTER] = reinterpret_cast<const uint8_t*>(1);
                                sizes[column][+Format::VALUE_TYPE::AFTER] = 0;
                            }
                        }
                    }

                    if (!guardPresent && values[column][+Format::VALUE_TYPE::AFTER_SUPP] != nullptr) {
                        values[column][+Format::VALUE_TYPE::AFTER] = values[column][+Format::VALUE_TYPE::AFTER_SUPP];
                        sizes[column][+Format::VALUE_TYPE::AFTER] = sizes[column][+Format::VALUE_TYPE::AFTER_SUPP];
                    }
                } else if (sizes[column][+Format::VALUE_TYPE::AFTER] == 0 &&
                        values[column][+Format::VALUE_TYPE::AFTER_SUPP] != nullptr) {
                    values[column][+Format::VALUE_TYPE::AFTER] = values[column][+Format::VALUE_TYPE::AFTER_SUPP];
                    sizes[column][+Format::VALUE_TYPE::AFTER] = sizes[column][+Format::VALUE_TYPE::AFTER_SUPP];
                }
            }
        }

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DML) || dump)) {
            if (likely(table != nullptr)) {
                ctx->logTrace(Ctx::TRACE::DML, "tab: " + table->owner + "." + table->name + " transactionType: " +
                              std::to_string(static_cast<uint>(transactionType)) + " columns: " +
                              std::to_string(valuesMax));

                baseMax = valuesMax >> 6;
                for (typeCol base = 0; base <= baseMax; ++base) {
                    const typeCol columnBase = base << 6;
                    typeMask set = valuesSet[base];
                    while (set != 0) {
                        const typeCol pos = ffsll(set) - 1;
                        set &= ~(1ULL << pos);
                        const typeCol column = columnBase + pos;

                        ctx->logTrace(Ctx::TRACE::DML, "DML: " + std::to_string(column + 1) + ":  B(" +
                                      std::to_string(values[column][+Format::VALUE_TYPE::BEFORE] != nullptr ?
                                      sizes[column][+Format::VALUE_TYPE::BEFORE] : -1) +
                                      ") A(" + std::to_string(values[column][+Format::VALUE_TYPE::AFTER] != nullptr ?
                                      sizes[column][+Format::VALUE_TYPE::AFTER] :
                                      -1) + ") BS(" + std::to_string(values[column][+Format::VALUE_TYPE::BEFORE_SUPP] !=
                                      nullptr ? sizes[column][+Format::VALUE_TYPE::BEFORE_SUPP] : -1) + ") AS(" +
                                      std::to_string(values[column][+Format::VALUE_TYPE::AFTER_SUPP] != nullptr ?
                                      sizes[column][+Format::VALUE_TYPE::AFTER_SUPP] : -1) + ") pk: " +
                                      std::to_string(table->columns[column]->numPk));
                    }
                }
            } else {
                ctx->logTrace(Ctx::TRACE::DML, "tab: (obj: " + std::to_string(redoLogRecord1->obj) + ", dataobj: " +
                              std::to_string(redoLogRecord1->dataObj) + ") transactionType: " +
                              std::to_string(static_cast<uint>(transactionType)) +
                              " columns: " + std::to_string(valuesMax));

                baseMax = valuesMax >> 6;
                for (typeCol base = 0; base <= baseMax; ++base) {
                    const typeCol columnBase = base << 6;
                    typeMask set = valuesSet[base];
                    while (set != 0) {
                        const typeCol pos = ffsll(set) - 1;
                        set &= ~(1ULL << pos);
                        const typeCol column = columnBase + pos;

                        ctx->logTrace(Ctx::TRACE::DML, "DML: " + std::to_string(column + 1) + ":  B(" +
                                      std::to_string(sizes[column][+Format::VALUE_TYPE::BEFORE]) + ") A(" +
                                      std::to_string(sizes[column][+Format::VALUE_TYPE::AFTER]) + ") BS(" +
                                      std::to_string(sizes[column][+Format::VALUE_TYPE::BEFORE_SUPP]) + ") AS(" +
                                      std::to_string(sizes[column][+Format::VALUE_TYPE::AFTER_SUPP]) + ")");
                    }
                }
            }
        }

        if (transactionType == Format::TRANSACTION_TYPE::UPDATE) {
            if (!compressedBefore && !compressedAfter) {
                baseMax = valuesMax >> 6;
                for (typeCol base = 0; base <= baseMax; ++base) {
                    const typeCol columnBase = base << 6;
                    typeMask set = valuesSet[base];
                    while (set != 0) {
                        const typeCol pos = ffsll(set) - 1;
                        const typeMask mask = 1ULL << pos;
                        set &= ~mask;
                        const typeCol column = columnBase + pos;

                        if (likely(table != nullptr)) {
                            if (unlikely(static_cast<size_t>(column) >= table->columns.size()))
                                throw RedoLogException(50073, "table: " + table->owner + "." + table->name + ": missmatch in column details: " +
                                                       std::to_string(table->columns.size()) + " < " + std::to_string(column) + ", xid: " +
                                                       lastXid.toString() + ", offset: " + redoLogRecord2p->fileOffset.toString());

                            if (!table->columns[column]->nullable &&
                                values[column][+Format::VALUE_TYPE::BEFORE] != nullptr &&
                                values[column][+Format::VALUE_TYPE::AFTER] != nullptr &&
                                sizes[column][+Format::VALUE_TYPE::BEFORE] == 0 &&
                                sizes[column][+Format::VALUE_TYPE::AFTER] > 0) {
                                if (!table->columns[column]->nullWarning) {
                                    table->columns[column]->nullWarning = true;
                                    ctx->warning(60034, "observed UPDATE operation for NOT NULL column with NULL value for table " +
                                                 table->owner + "." + table->name + " column " + table->columns[column]->name);
                                }
                                if (ctx->isFlagSet(Ctx::REDO_FLAGS::EXPERIMENTAL_NOT_NULL_MISSING)) {
                                    values[column][+Format::VALUE_TYPE::BEFORE] =
                                            values[column][+Format::VALUE_TYPE::AFTER];
                                    sizes[column][+Format::VALUE_TYPE::BEFORE] = sizes[column][+Format::VALUE_TYPE::AFTER];
                                    values[column][+Format::VALUE_TYPE::BEFORE_SUPP] =
                                            values[column][+Format::VALUE_TYPE::AFTER_SUPP];
                                    sizes[column][+Format::VALUE_TYPE::BEFORE_SUPP] =
                                            sizes[column][+Format::VALUE_TYPE::AFTER_SUPP];
                                }
                            }

                            if (format.columnFormat < Format::COLUMN_FORMAT::FULL_UPD) {
                                if (table->columns[column]->numPk == 0) {
                                    // Remove unchanged column values - only for tables with a defined primary key
                                    if (values[column][+Format::VALUE_TYPE::BEFORE] != nullptr &&
                                            sizes[column][+Format::VALUE_TYPE::BEFORE] ==
                                            sizes[column][+Format::VALUE_TYPE::AFTER] &&
                                            values[column][+Format::VALUE_TYPE::AFTER] != nullptr) {
                                        if (sizes[column][+Format::VALUE_TYPE::BEFORE] == 0 ||
                                                memcmp(values[column][+Format::VALUE_TYPE::BEFORE],
                                                       values[column][+Format::VALUE_TYPE::AFTER],
                                                       sizes[column][+Format::VALUE_TYPE::BEFORE]) == 0) {
                                            valuesSet[base] &= ~mask;
                                            values[column][+Format::VALUE_TYPE::BEFORE] = nullptr;
                                            values[column][+Format::VALUE_TYPE::BEFORE_SUPP] = nullptr;
                                            values[column][+Format::VALUE_TYPE::AFTER] = nullptr;
                                            values[column][+Format::VALUE_TYPE::AFTER_SUPP] = nullptr;
                                            continue;
                                        }
                                    }

                                    // Remove columns additionally present, but null
                                    if (values[column][+Format::VALUE_TYPE::BEFORE] != nullptr &&
                                            sizes[column][+Format::VALUE_TYPE::BEFORE] == 0 &&
                                            values[column][+Format::VALUE_TYPE::AFTER] == nullptr) {
                                        valuesSet[base] &= ~mask;
                                        values[column][+Format::VALUE_TYPE::BEFORE] = nullptr;
                                        values[column][+Format::VALUE_TYPE::BEFORE_SUPP] = nullptr;
                                        values[column][+Format::VALUE_TYPE::AFTER_SUPP] = nullptr;
                                        continue;
                                    }

                                    if (values[column][+Format::VALUE_TYPE::AFTER] != nullptr &&
                                            sizes[column][+Format::VALUE_TYPE::AFTER] == 0 &&
                                            values[column][+Format::VALUE_TYPE::BEFORE] == nullptr) {
                                        valuesSet[base] &= ~mask;
                                        values[column][+Format::VALUE_TYPE::AFTER] = nullptr;
                                        values[column][+Format::VALUE_TYPE::BEFORE_SUPP] = nullptr;
                                        values[column][+Format::VALUE_TYPE::AFTER_SUPP] = nullptr;
                                        continue;
                                    }

                                } else {
                                    // Leave NULL value & propagate
                                    if (values[column][+Format::VALUE_TYPE::BEFORE] != nullptr &&
                                            sizes[column][+Format::VALUE_TYPE::BEFORE] == 0 &&
                                            values[column][+Format::VALUE_TYPE::AFTER] == nullptr) {
                                        values[column][+Format::VALUE_TYPE::AFTER] =
                                                values[column][+Format::VALUE_TYPE::BEFORE];
                                        sizes[column][+Format::VALUE_TYPE::AFTER] =
                                                sizes[column][+Format::VALUE_TYPE::BEFORE];
                                    }

                                    if (values[column][+Format::VALUE_TYPE::AFTER] != nullptr &&
                                            sizes[column][+Format::VALUE_TYPE::AFTER] == 0 &&
                                            values[column][+Format::VALUE_TYPE::BEFORE] == nullptr) {
                                        values[column][+Format::VALUE_TYPE::BEFORE] =
                                                values[column][+Format::VALUE_TYPE::AFTER];
                                        sizes[column][+Format::VALUE_TYPE::BEFORE] =
                                                sizes[column][+Format::VALUE_TYPE::AFTER];
                                    }
                                }
                            }
                        }

                        // For update assume null for missing columns
                        if (values[column][+Format::VALUE_TYPE::BEFORE] != nullptr &&
                                values[column][+Format::VALUE_TYPE::AFTER] == nullptr) {
                            values[column][+Format::VALUE_TYPE::AFTER] = reinterpret_cast<const uint8_t*>(1);
                            sizes[column][+Format::VALUE_TYPE::AFTER] = 0;
                        }

                        if (values[column][+Format::VALUE_TYPE::AFTER] != nullptr &&
                                values[column][+Format::VALUE_TYPE::BEFORE] == nullptr) {
                            values[column][+Format::VALUE_TYPE::BEFORE] = reinterpret_cast<const uint8_t*>(1);
                            sizes[column][+Format::VALUE_TYPE::BEFORE] = 0;
                        }
                    }
                }
            }

            if (system && table != nullptr && DbTable::isSystemTable(table->options))
                systemTransaction->processUpdate(table, dataObj, bdba, slot, redoLogRecord1->fileOffset);

            if ((!schema && table != nullptr && !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options) &&
                    table->matchesCondition(ctx, 'u', attributes)) || ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_SYSTEM_TRANSACTIONS) ||
                    ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                processUpdate(sequence, scn, timestamp, lobCtx, xmlCtx, table, obj, dataObj, bdba, slot, redoLogRecord1->fileOffset);
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsUpdateOut(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsUpdateOut(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsUpdateOut(1);
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsUpdateSkip(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsUpdateSkip(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsUpdateSkip(1);
                }
            }

        } else if (transactionType == Format::TRANSACTION_TYPE::INSERT) {
            if (table != nullptr && !compressedAfter) {
                // Assume NULL values for all missing columns
                if (format.columnFormat >= Format::COLUMN_FORMAT::FULL_INS_DEC) {
                    auto maxCol = static_cast<typeCol>(table->columns.size());
                    for (typeCol column = 0; column < maxCol; ++column) {
                        const typeCol base = column >> 6;
                        const typeMask mask = 1ULL << (column & 0x3F);
                        if ((valuesSet[base] & mask) != 0)
                            continue;
                        valuesSet[base] |= mask;
                        values[column][+Format::VALUE_TYPE::AFTER] = reinterpret_cast<const uint8_t*>(1);
                        sizes[column][+Format::VALUE_TYPE::AFTER] = 0;
                        if (column >= valuesMax)
                            valuesMax = column + 1;
                    }
                } else {
                    // Remove NULL values from insert if not PK
                    baseMax = valuesMax >> 6;
                    for (typeCol base = 0; base <= baseMax; ++base) {
                        const typeCol columnBase = base << 6;
                        typeMask set = valuesSet[base];
                        while (set != 0) {
                            const typeCol pos = ffsll(set) - 1;
                            const typeMask mask = 1ULL << pos;
                            set &= ~mask;
                            const typeCol column = columnBase + pos;

                            if (table->columns[column]->numPk == 0 &&
                                    (values[column][+Format::VALUE_TYPE::AFTER] == nullptr ||
                                    sizes[column][+Format::VALUE_TYPE::AFTER] == 0)) {
                                valuesSet[base] &= ~mask;
                                values[column][+Format::VALUE_TYPE::AFTER] = nullptr;
                                values[column][+Format::VALUE_TYPE::AFTER_SUPP] = nullptr;
                            }
                        }
                    }

                    // Assume NULL values for PK missing columns
                    for (const typeCol column: table->pk) {
                        const typeCol base = column >> 6;
                        const typeMask mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) != 0)
                            continue;
                        valuesSet[base] |= mask;
                        values[column][+Format::VALUE_TYPE::AFTER] = reinterpret_cast<const uint8_t*>(1);
                        sizes[column][+Format::VALUE_TYPE::AFTER] = 0;
                        if (column >= valuesMax)
                            valuesMax = column + 1;
                    }
                }
            }

            if (system && table != nullptr && DbTable::isSystemTable(table->options))
                systemTransaction->processInsert(table, dataObj, bdba, slot, redoLogRecord1->fileOffset);

            if ((!schema && table != nullptr && !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options) &&
                    table->matchesCondition(ctx, 'i', attributes)) || ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_SYSTEM_TRANSACTIONS) ||
                    ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                processInsert(sequence, scn, timestamp, lobCtx, xmlCtx, table, obj, dataObj, bdba, slot, redoLogRecord1->fileOffset);
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsInsertOut(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsInsertOut(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsInsertOut(1);
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsInsertSkip(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsInsertSkip(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsInsertSkip(1);
                }
            }

        } else if (transactionType == Format::TRANSACTION_TYPE::DELETE) {
            if (table != nullptr && !compressedBefore) {
                // Assume NULL values for all missing columns
                if (format.columnFormat >= Format::COLUMN_FORMAT::FULL_INS_DEC) {
                    auto maxCol = static_cast<typeCol>(table->columns.size());
                    for (typeCol column = 0; column < maxCol; ++column) {
                        const typeCol base = column >> 6;
                        const typeMask mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) != 0)
                            continue;
                        valuesSet[base] |= mask;
                        values[column][+Format::VALUE_TYPE::BEFORE] = reinterpret_cast<const uint8_t*>(1);
                        sizes[column][+Format::VALUE_TYPE::BEFORE] = 0;
                    }
                } else {
                    // Remove NULL values from delete if not PK
                    baseMax = valuesMax >> 6;
                    for (typeCol base = 0; base <= baseMax; ++base) {
                        const typeCol columnBase = base << 6;
                        typeMask set = valuesSet[base];
                        while (set != 0) {
                            const typeCol pos = ffsll(set) - 1;
                            const typeMask mask = 1ULL << pos;
                            set &= ~mask;
                            const typeCol column = columnBase + pos;

                            if (table->columns[column]->numPk == 0 &&
                                    (values[column][+Format::VALUE_TYPE::BEFORE] == nullptr ||
                                    sizes[column][+Format::VALUE_TYPE::BEFORE] == 0)) {
                                valuesSet[base] &= ~mask;
                                values[column][+Format::VALUE_TYPE::BEFORE] = nullptr;
                                values[column][+Format::VALUE_TYPE::BEFORE_SUPP] = nullptr;
                            }
                        }
                    }

                    // Assume NULL values for PK missing columns
                    for (const typeCol column: table->pk) {
                        const typeCol base = column >> 6;
                        const typeMask mask = static_cast<uint64_t>(1) << (column & 0x3F);
                        if ((valuesSet[base] & mask) != 0)
                            continue;
                        valuesSet[base] |= mask;
                        values[column][+Format::VALUE_TYPE::BEFORE] = reinterpret_cast<const uint8_t*>(1);
                        sizes[column][+Format::VALUE_TYPE::BEFORE] = 0;
                    }
                }
            }

            if (system && table != nullptr && DbTable::isSystemTable(table->options))
                systemTransaction->processDelete(table, dataObj, bdba, slot, redoLogRecord1->fileOffset);

            if ((!schema && table != nullptr && !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options) &&
                    table->matchesCondition(ctx, 'd', attributes)) || ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_SYSTEM_TRANSACTIONS) ||
                    ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                processDelete(sequence, scn, timestamp, lobCtx, xmlCtx, table, obj, dataObj, bdba, slot, redoLogRecord1->fileOffset);
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteOut(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteOut(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsDeleteOut(1);
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (ctx->metrics->isTagNamesFilter() && table != nullptr &&
                            !DbTable::isSystemTable(table->options) && !DbTable::isDebugTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteSkip(1, table->owner, table->name);
                    else if (ctx->metrics->isTagNamesSys() && table != nullptr && DbTable::isSystemTable(table->options))
                        ctx->metrics->emitDmlOpsDeleteSkip(1, table->owner, table->name);
                    else
                        ctx->metrics->emitDmlOpsDeleteSkip(1);
                }
            }
        }

        releaseValues();
    }

    // 0x18010000
    void Builder::processDdl(Seq sequence, Scn scn, Time timestamp, const RedoLogRecord* redoLogRecord1) {
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;
        const DbTable* table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        if (format.isScnTypeCommitValue())
            scn = commitScn;

        RedoLogRecord::nextField(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x000009);
        // Field: 1
        const uint16_t ddlType = ctx->read16(redoLogRecord1->data(fieldPos + 12));
        const uint16_t seq = ctx->read16(redoLogRecord1->data(fieldPos + 18));
        const uint16_t cnt = ctx->read16(redoLogRecord1->data(fieldPos + 20));

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x00000A))
            return;

        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_DDL)) {
            if (seq == 1) {
                if (ddlFirst != nullptr) {
                    ctx->warning(60037, "incorrect DDL data, offset: " + redoLogRecord1->fileOffset.toString());
                    releaseDdl();
                }

                memcpy(ddlSchemaName, redoLogRecord1->data(fieldPos), fieldSize);
                ddlSchemaSize = fieldSize;
            } else {
                appendDdlChunk(redoLogRecord1->data(fieldPos), fieldSize);

                if (seq == cnt) {
                    processDdl(sequence, scn, timestamp, table, redoLogRecord1->obj);
                    releaseDdl();
                }
            }
        }
        // Field: 2

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x00000B))
            return;
        // Field: 3

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x00000C))
            return;
        // Field: 4

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x00000D))
            return;
        // Field: 5

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x00000E))
            return;
        // Field: 6

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x00000F))
            return;
        // Field: 7

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord1, fieldNum, fieldPos, fieldSize, 0x000011))
            return;

        // Field: 8
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_DDL)) {
            appendDdlChunk(redoLogRecord1->data(fieldPos), fieldSize - 1U);
            if (seq == cnt) {
                processDdl(sequence, scn, timestamp, table, redoLogRecord1->obj);
                releaseDdl();
            }
        }

        switch (ddlType) {
            case 1: // create table
            case 4: // create cluster
            case 9: // create index
                if (ctx->metrics != nullptr)
                    ctx->metrics->emitDdlOpsCreate(1);
                break;

            case 85: // truncate
                if (ctx->metrics != nullptr)
                    ctx->metrics->emitDdlOpsTruncate(1);
                break;

            case 8:  // drop cluster
            case 12: // drop table
                if (ctx->metrics != nullptr)
                    ctx->metrics->emitDdlOpsDrop(1);
                break;

            case 15: // alter table
            case 11: // alter index
                if (ctx->metrics != nullptr)
                    ctx->metrics->emitDdlOpsAlter(1);
                break;

            case 198: // purge
                if (ctx->metrics != nullptr)
                    ctx->metrics->emitDdlOpsPurge(1);
                break;

            default:
                if (ctx->metrics != nullptr)
                    ctx->metrics->emitDdlOpsOther(1);
        }
    }

    // Parse binary XML format
    bool Builder::parseXml(const XmlCtx* xmlCtx, const uint8_t* data, uint64_t size, FileOffset fileOffset) {
        if (valueBufferOld != nullptr) {
            delete[] valueBufferOld;
            valueBufferOld = nullptr;
        }

        valueBufferOld = valueBuffer;
        valueSizeOld = valueSize;
        valueBuffer = new char[VALUE_BUFFER_MIN];
        valueBufferSize = VALUE_BUFFER_MIN;
        valueSize = 0;

        // bool bigint = false;
        std::string out;
        uint pos = 0;
        std::vector<std::string> tags;
        std::map<std::string, std::string> dictNmSpcMap;
        std::map<std::string, std::string> nmSpcPrefixMap;
        bool tagOpen = false;
        bool attributeOpen = false;
        std::string lastTag;

        while (pos < size) {
            // Header
            if (data[pos] == 0x9E) {
                bool xmlDecl = false;
                const char* standalone = "";
                const char* version = nullptr;
                const char* encoding = "";

                ++pos;
                if (pos + 2U >= size) {
                    ctx->warning(60036, "incorrect XML data: header too short, can't read flags");
                    return false;
                }
                ++pos; //uint8_t flags0 = data[pos++];
                ++pos; //uint8_t flags1 = data[pos++];
                const uint8_t flags2 = data[pos++];

                if ((flags2 & XML_HEADER_XMLDECL) != 0)
                    xmlDecl = true;

                if ((flags2 & XML_HEADER_STANDALONE) != 0) {
                    if ((flags2 & XML_HEADER_STANDALONE_YES) != 0)
                        standalone = " standalone=\"yes\"";
                    else
                        standalone = " standalone=\"no\"";
                }

                if ((flags2 & XML_HEADER_ENCODING) != 0)
                    encoding = " encoding=\"UTF=8\"";

                if ((flags2 & XML_HEADER_VERSION) != 0) {
                    if ((flags2 & XML_HEADER_VERSION_1_1) != 0)
                        version = "\"1.1\"";
                    else
                        version = "\"1.0\"";
                }

                if (xmlDecl) {
                    valueBufferCheck(100, fileOffset);

                    valueBufferAppend("<?xml", 5);
                    if (version != nullptr) {
                        valueBufferAppend(" version=", 9);
                        valueBufferAppend(version, strlen(version));
                    }
                    valueBufferAppend(standalone, strlen(standalone));
                    valueBufferAppend(encoding, strlen(encoding));
                    valueBufferAppend("?>", 2);
                }

                continue;
            }

            // Prolog
            if (data[pos] == 0x9F) {
                ++pos;
                if (pos + 1U >= size) {
                    ctx->warning(60036, "incorrect XML data: prolog too short, can't read version and flags");
                    return false;
                }
                const uint8_t binaryXmlVersion = data[pos++];
                if (binaryXmlVersion != 1) {
                    ctx->warning(60036, "incorrect XML data: prolog contains incorrect version, expected: 1, found: " +
                                 std::to_string(data[pos + 1]));
                    return false;
                }
                const uint8_t flags0 = data[pos++];

                if ((flags0 & XML_PROLOG_DOCID) != 0) {
                    if (pos >= size) {
                        ctx->warning(60036, "incorrect XML data: prolog too short, can't read docid length");
                        return false;
                    }
                    const uint8_t docidSize = data[pos++];

                    if (pos + docidSize - 1U >= size) {
                        ctx->warning(60036, "incorrect XML data: prolog too short, can't read docid data");
                        return false;
                    }

                    pos += docidSize;
                }

                if ((flags0 & XML_PROLOG_PATHID) != 0) {
                    if (pos >= size) {
                        ctx->warning(60036, "incorrect XML data: prolog too short, can't read path length (1)");
                        return false;
                    }
                    uint8_t pathidSize = data[pos++];

                    if (pos + pathidSize - 1U >= size) {
                        ctx->warning(60036, "incorrect XML data: prolog too short, can't read pathid data (1)");
                        return false;
                    }

                    pos += pathidSize;
                    if (pos >= size) {
                        ctx->warning(60036, "incorrect XML data: prolog too short, can't read path length (2)");
                        return false;
                    }
                    pathidSize = data[pos++];

                    if (pos + pathidSize - 1U >= size) {
                        ctx->warning(60036, "incorrect XML data: prolog too short, can't read pathid data (2)");
                        return false;
                    }

                    pos += pathidSize;
                }

                //if ((flags0 & XML_PROLOG_BIGINT) != 0)
                //    bigint = true;
                continue;
            }

            // tag/parameter
            if (data[pos] == 0xC8 || data[pos] == 0xC9 || (data[pos] >= 0xC0 && data[pos] <= 0xC3)) {
                uint64_t tagSize = 0;
                uint64_t code = 0;
                bool isSingle = false;

                if (data[pos] == 0xC8) {
                    ++pos;
                    if (pos + 1U >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC8 data");
                        return false;
                    }
                    tagSize = 0;
                    code = Ctx::read16Big(data + pos);
                    pos += 2;
                } else if (data[pos] == 0xC9) {
                    ++pos;
                    if (pos + 3U >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC9 data");
                        return false;
                    }
                    tagSize = 0;
                    code = Ctx::read32Big(data + pos);
                    pos += 4;
                } else if (data[pos] == 0xC0) {
                    ++pos;
                    if (pos + 2U >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC0xx data");
                        return false;
                    }
                    tagSize = data[pos];
                    if (tagSize == 0x8F)
                        tagSize = 0;
                    else
                        ++tagSize;
                    ++pos;
                    code = Ctx::read16Big(data + pos);
                    pos += 2;
                    isSingle = true;
                } else if (data[pos] == 0xC1) {
                    ++pos;
                    if (pos + 3U >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC1xxxx data");
                        return false;
                    }
                    tagSize = Ctx::read16Big(data + pos);
                    pos += 2;
                    code = Ctx::read16Big(data + pos);
                    pos += 2;
                    isSingle = true;
                } else if (data[pos] == 0xC2) {
                    ++pos;
                    if (pos + 4 >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC2xxxxxxxx data");
                        return false;
                    }
                    tagSize = data[pos];
                    if (tagSize == 0x8F)
                        tagSize = 0;
                    else
                        ++tagSize;
                    ++pos;
                    code = Ctx::read32Big(data + pos);
                    pos += 4;
                    isSingle = true;
                } else if (data[pos] == 0xC3) {
                    ++pos;
                    if (pos + 5U >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC3xxxxxxxx data");
                        return false;
                    }
                    tagSize = Ctx::read16Big(data + pos);
                    pos += 2;
                    code = Ctx::read32Big(data + pos);
                    pos += 4;
                    isSingle = true;
                }

                std::string codeStr;
                if (code < 0x100)
                    codeStr = {
                        Data::map16U((code >> 4) & 0x0F),
                        Data::map16U(code & 0x0F)
                    };
                else if (code < 0x10000)
                    codeStr = {
                        Data::map16U((code >> 12) & 0x0F),
                        Data::map16U((code >> 8) & 0x0F),
                        Data::map16U((code >> 4) & 0x0F),
                        Data::map16U(code & 0x0F)
                    };
                else if (code < 0x1000000)
                    codeStr = {
                        Data::map16U((code >> 20) & 0x0F),
                        Data::map16U((code >> 16) & 0x0F),
                        Data::map16U((code >> 12) & 0x0F),
                        Data::map16U((code >> 8) & 0x0F),
                        Data::map16U((code >> 4) & 0x0F),
                        Data::map16U(code & 0x0F)
                    };
                else
                    codeStr = {
                        Data::map16U((code >> 28) & 0x0F),
                        Data::map16U((code >> 24) & 0x0F),
                        Data::map16U((code >> 20) & 0x0F),
                        Data::map16U((code >> 16) & 0x0F),
                        Data::map16U((code >> 12) & 0x0F),
                        Data::map16U((code >> 8) & 0x0F),
                        Data::map16U((code >> 4) & 0x0F),
                        Data::map16U(code & 0x0F)
                    };

                auto xdbXQnMapIdIt = xmlCtx->xdbXQnPack.unorderedMapKey.find(XdbXQnKey(codeStr));
                if (xdbXQnMapIdIt == xmlCtx->xdbXQnPack.unorderedMapKey.end()) {
                    ctx->warning(60036, "incorrect XML data: string too short, can't decode qn   " + codeStr);
                    return false;
                }

                std::string tag = xdbXQnMapIdIt->second->localName;
                // not very efficient, but it's not a problem
                const uint64_t flagsSize = xdbXQnMapIdIt->second->flags.length();
                const bool isAttribute = (((xdbXQnMapIdIt->second->flags.at(flagsSize - 1) - '0') & XdbXQn::FLAG_ISATTRIBUTE) != 0);

                if (isAttribute) {
                    out = " " + tag + "=\"";
                    valueBufferCheck(out.length(), fileOffset);
                    valueBufferAppend(out.c_str(), out.length());
                } else {
                    if (attributeOpen) {
                        valueBufferCheck(2, fileOffset);
                        valueBufferAppend("\">", 2);
                        attributeOpen = false;
                    } else if (tagOpen) {
                        valueBufferCheck(1, fileOffset);
                        valueBufferAppend('>');
                        tagOpen = false;
                    }

                    // append namespace to tag name
                    const std::string nmSpcId = xdbXQnMapIdIt->second->nmSpcId;
                    auto nmSpcPrefixMapIt = nmSpcPrefixMap.find(nmSpcId);
                    if (nmSpcPrefixMapIt != nmSpcPrefixMap.end())
                        tag = nmSpcPrefixMapIt->second + ":" + tag;

                    if (tagSize == 0 && !isSingle) {
                        out = "<" + tag;
                        tagOpen = true;
                    } else
                        out = "<" + tag + ">";
                    valueBufferCheck(out.length(), fileOffset);
                    valueBufferAppend(out.c_str(), out.length());
                }

                if (tagSize > 0) {
                    if (pos + tagSize >= size) {
                        ctx->warning(60036, "incorrect XML data: string too short, can't read 0xC1xxxx data (2)");
                        return false;
                    }
                    valueBufferCheck(tagSize, fileOffset);
                    valueBufferAppend(reinterpret_cast<const char*>(data + pos), tagSize);
                    pos += tagSize;
                }

                if (isAttribute) {
                    if (isSingle) {
                        valueBufferCheck(1, fileOffset);
                        valueBufferAppend('"');
                    } else
                        attributeOpen = true;
                } else {
                    if (isSingle) {
                        out = "</" + tag + ">";
                        valueBufferCheck(out.length(), fileOffset);
                        valueBufferAppend(out.c_str(), out.length());
                    } else
                        tags.push_back(tag);
                }

                continue;
            }

            // namespace set
            if (data[pos] == 0xB2) {
                ++pos;
                if (pos + 7 >= size) {
                    ctx->warning(60036, "incorrect XML data: string too short, can't read B2");
                    return false;
                }

                const uint8_t tagSize = data[pos];
                ++pos;
                //uint16_t tmp = Ctx::read16Big(data + pos);
                pos += 2;
                const uint16_t nmSpc = Ctx::read16Big(data + pos);
                pos += 2;
                const uint16_t dict = Ctx::read16Big(data + pos);
                pos += 2;

                std::string nmSpcId;
                if (nmSpc < 256)
                    nmSpcId = {
                        Data::map16U((nmSpc >> 4) & 0x0F),
                        Data::map16U(nmSpc & 0x0F)
                    };
                else
                    nmSpcId = {
                        Data::map16U((nmSpc >> 12) & 0x0F),
                        Data::map16U((nmSpc >> 8) & 0x0F),
                        Data::map16U((nmSpc >> 4) & 0x0F),
                        Data::map16U(nmSpc & 0x0F)
                    };

                std::string dictId;
                if (dict < 256)
                    dictId = {
                        Data::map16U((dict >> 4) & 0x0F),
                        Data::map16U(dict & 0x0F)
                    };
                else
                    dictId = {
                        Data::map16U((dict >> 12) & 0x0F),
                        Data::map16U((dict >> 8) & 0x0F),
                        Data::map16U((dict >> 4) & 0x0F),
                        Data::map16U(dict & 0x0F)
                    };

                auto dictNmSpcMapIt = dictNmSpcMap.find(dictId);
                if (dictNmSpcMapIt != dictNmSpcMap.end()) {
                    ctx->warning(60036, "incorrect XML data: namespace " + dictId + " duplicated dict");
                    return false;
                }
                dictNmSpcMap.insert_or_assign(dictId, nmSpcId);

                if (tagSize > 0) {
                    const std::string prefix(reinterpret_cast<const char*>(data + pos), tagSize);
                    pos += tagSize;

                    auto nmSpcPrefixMapIt = nmSpcPrefixMap.find(nmSpcId);
                    if (nmSpcPrefixMapIt != nmSpcPrefixMap.end()) {
                        ctx->warning(60036, "incorrect XML data: namespace " + nmSpcId + " duplicated prefix");
                        return false;
                    }
                    nmSpcPrefixMap.insert_or_assign(nmSpcId, prefix);
                }

                continue;
            }

            // namespace adds to header
            if (data[pos] == 0xDD) {
                ++pos;

                if (pos + 2U >= size) {
                    ctx->warning(60036, "incorrect XML data: string too short, can't read DD");
                    return false;
                }
                const uint16_t dict = Ctx::read16Big(data + pos);
                pos += 2;

                std::string dictId;
                if (dict < 256)
                    dictId = {
                        Data::map16U((dict >> 4) & 0x0F),
                        Data::map16U(dict & 0x0F)
                    };
                else
                    dictId = {
                        Data::map16U((dict >> 12) & 0x0F),
                        Data::map16U((dict >> 8) & 0x0F),
                        Data::map16U((dict >> 4) & 0x0F),
                        Data::map16U(dict & 0x0F)
                    };

                auto dictNmSpcMapIt = dictNmSpcMap.find(dictId);
                if (dictNmSpcMapIt == dictNmSpcMap.end()) {
                    ctx->warning(60036, "incorrect XML data: namespace " + dictId + " not found for namespace");
                    return false;
                }
                const std::string nmSpcId = dictNmSpcMapIt->second;

                // search url
                auto xdbXNmMapIdIt = xmlCtx->xdbXNmPack.unorderedMapKey.find(XdbXNmKey(nmSpcId));
                if (xdbXNmMapIdIt == xmlCtx->xdbXNmPack.unorderedMapKey.end()) {
                    ctx->warning(60036, "incorrect XML data: namespace " + nmSpcId + " not found");
                    return false;
                }

                valueBufferCheck(7, fileOffset);
                valueBufferAppend(" xmlns", 6);

                auto nmSpcPrefixMapIt = nmSpcPrefixMap.find(nmSpcId);
                if (nmSpcPrefixMapIt != nmSpcPrefixMap.end()) {
                    valueBufferCheck(1, fileOffset);
                    valueBufferAppend(':');

                    valueBufferCheck(nmSpcPrefixMapIt->second.length(), fileOffset);
                    valueBufferAppend(nmSpcPrefixMapIt->second.c_str(), nmSpcPrefixMapIt->second.length());
                }

                valueBufferCheck(2, fileOffset);
                valueBufferAppend("=\"", 2);

                valueBufferCheck(xdbXNmMapIdIt->second->nmSpcUri.length(), fileOffset);
                valueBufferAppend(xdbXNmMapIdIt->second->nmSpcUri.c_str(), xdbXNmMapIdIt->second->nmSpcUri.length());

                valueBufferCheck(1, fileOffset);
                valueBufferAppend('"');

                continue;
            }

            // chunk of data 64bit
            if (data[pos] == 0x8B) {
                if (tagOpen && !attributeOpen) {
                    valueBufferCheck(1, fileOffset);
                    valueBufferAppend('>');
                    tagOpen = false;
                }
                ++pos;

                if (pos + 8U >= size) {
                    ctx->warning(60036, "incorrect XML data: string too short, can't read 8B");
                    return false;
                }

                const uint64_t tagSize = Ctx::read64Big(data + pos);
                pos += 8;

                if (pos + tagSize >= size) {
                    ctx->warning(60036, "incorrect XML data: string too short, can't read 8B data");
                    return false;
                }

                valueBufferCheck(tagSize, fileOffset);
                valueBufferAppend(reinterpret_cast<const char*>(data + pos), tagSize);
                pos += tagSize;
                continue;
            }

            if (data[pos] < 128) {
                if (tagOpen && !attributeOpen) {
                    valueBufferCheck(1, fileOffset);
                    valueBufferAppend('>');
                    tagOpen = false;
                }

                const uint64_t tagSize = data[pos] + 1;
                ++pos;

                if (pos + tagSize >= size) {
                    ctx->warning(60036, "incorrect XML data: string too short, can't read value data");
                    return false;
                }

                valueBufferCheck(tagSize, fileOffset);
                valueBufferAppend(reinterpret_cast<const char*>(data + pos), tagSize);
                pos += tagSize;
                continue;
            }

            // end tag
            if (data[pos] == 0xD9) {
                if (attributeOpen) {
                    out = "\"";
                    attributeOpen = false;
                    tagOpen = true;
                } else {
                    if (tags.empty()) {
                        ctx->warning(60036, "incorrect XML data: end tag found, but no tags open");
                        return false;
                    }
                    lastTag = tags.back();
                    tags.pop_back();
                    out = "</" + lastTag + ">";
                }

                valueBufferCheck(out.length(), fileOffset);
                valueBufferAppend(out.c_str(), out.length());
                ++pos;
                continue;
            }

            // ignore
            if (data[pos] >= 0xD6 && data[pos] <= 0xD8) {
                ++pos;
                continue;
            }

            // repeat last tag
            if (data[pos] >= 0xD4 && data[pos] <= 0xD5) {
                tags.push_back(lastTag);
                out = "<" + lastTag;
                tagOpen = true;
                valueBufferCheck(out.length(), fileOffset);
                valueBufferAppend(out.c_str(), out.length());
                ++pos;
                continue;
            }

            // EOF
            if (data[pos] == 0xA0)
                break;

            ctx->warning(60036, "incorrect XML data: string too short, can't decode code: " + std::to_string(data[pos]) + " at pos: " +
                         std::to_string(pos));
            return false;
        }

        return true;
    }

    void Builder::releaseBuffers(Thread* t, uint64_t maxId) {
        BuilderQueue* builderQueue;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::BUILDER_RELEASE);
            std::unique_lock const lck(mtx);
            builderQueue = firstBuilderQueue;
            while (firstBuilderQueue->id < maxId) {
                firstBuilderQueue = firstBuilderQueue->next;
                --buffersAllocated;
            }
        }
        t->contextSet(Thread::CONTEXT::CPU);

        if (builderQueue == nullptr)
            return;

        while (builderQueue->id < maxId) {
            BuilderQueue* nextBuffer = builderQueue->next;
            ctx->freeMemoryChunk(ctx->parserThread, Ctx::MEMORY::BUILDER, reinterpret_cast<uint8_t*>(builderQueue));
            builderQueue = nextBuffer;
        }
    }

    void Builder::releaseDdl() {
        while (ddlFirst != nullptr) {
            uint8_t* next = *reinterpret_cast<uint8_t**>(ddlFirst);
            ctx->freeMemoryChunk(ctx->parserThread, Ctx::MEMORY::MISC, ddlFirst);
            ddlFirst = next;
        }
        ddlLast = nullptr;
        ddlSize = 0;
        ddlSchemaSize = 0;
    }

    void Builder::appendDdlChunk(const uint8_t* data, typeTransactionSize size) {
        while (size > 0) {
            typeTransactionSize* chunkSize;
            typeTransactionSize left = 0;

            if (ddlLast != nullptr) {
                chunkSize = reinterpret_cast<typeTransactionSize*>(ddlLast + sizeof(uint8_t*));
                left = Ctx::MEMORY_CHUNK_SIZE - sizeof(uint8_t*) - sizeof(uint64_t) - *chunkSize;
            }

            if (left == 0) {
                uint8_t* ddlNew = ctx->getMemoryChunk(ctx->parserThread, Ctx::MEMORY::MISC, false);

                if (ddlLast != nullptr) {
                    auto** ddlNext = reinterpret_cast<uint8_t**>(ddlLast);
                    *ddlNext = ddlNew;
                } else
                    ddlFirst = ddlNew;

                ddlLast = ddlNew;
                chunkSize = reinterpret_cast<typeTransactionSize*>(ddlLast + sizeof(uint8_t*));
                *chunkSize = 0;
                auto** ddlNext = reinterpret_cast<uint8_t**>(ddlLast);
                *ddlNext = nullptr;
                left = Ctx::MEMORY_CHUNK_SIZE - sizeof(uint8_t*) - sizeof(uint64_t);
            }

            const typeTransactionSize move = std::min(size, left);
            uint8_t* chunkData = ddlLast + sizeof(uint8_t*) + sizeof(uint64_t) + *chunkSize;
            memcpy(chunkData, data, move);
            *chunkSize += move;
            ddlSize += move;
            size -= move;
            data += move;
        }
    }

    void Builder::sleepForWriterWork(Thread* t, uint64_t queueSize, uint64_t nanoseconds) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
            ctx->logTrace(Ctx::TRACE::SLEEP, "Builder:sleepForWriterWork");

        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::WRITER_DONE);
            std::unique_lock lck(mtx);
            t->contextSet(Thread::CONTEXT::WAIT, Thread::REASON::WRITER_NO_WORK);
            if (queueSize > 0)
                condNoWriterWork.wait_for(lck, std::chrono::nanoseconds(nanoseconds));
            else
                condNoWriterWork.wait_for(lck, std::chrono::seconds(5));
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Builder::wakeUp() {
        std::unique_lock const lck(mtx);
        condNoWriterWork.notify_all();
    }
}
