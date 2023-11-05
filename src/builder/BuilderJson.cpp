/* Memory buffer for handling output buffer in JSON format
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
#include "../common/SysCol.h"
#include "../common/typeRowId.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "BuilderJson.h"

namespace OpenLogReplicator {
    BuilderJson::BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat, uint64_t newIntervalDtsFormat,
                             uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat,
                             uint64_t newTimestampFormat, uint64_t newTimestampTzFormat, uint64_t newTimestampAll, uint64_t newCharFormat,
                             uint64_t newScnFormat, uint64_t newScnAll, uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat,
                             uint64_t newUnknownType, uint64_t newFlushBuffer) :
        Builder(newCtx, newLocales, newMetadata, newDbFormat, newIntervalDtsFormat, newIntervalYtmFormat, newMessageFormat, newRidFormat, newXidFormat,
                newTimestampFormat, newTimestampTzFormat, newTimestampAll, newCharFormat, newScnFormat, newScnAll, newUnknownFormat, newSchemaFormat,
                newColumnFormat, newUnknownType, newFlushBuffer),
                hasPreviousValue(false),
                hasPreviousRedo(false),
                hasPreviousColumn(false) {
    }

    void BuilderJson::columnNull(OracleTable* table, typeCol col, bool after) {
        if (table != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
            OracleColumn* column = table->columns[col];
            if (column->guard && !FLAG(REDO_FLAGS_SHOW_GUARD_COLUMNS))
                return;
            if (column->nested && !FLAG(REDO_FLAGS_SHOW_NESTED_COLUMNS))
                return;
            if (column->hidden && !FLAG(REDO_FLAGS_SHOW_HIDDEN_COLUMNS))
                return;
            if (column->unused && !FLAG(REDO_FLAGS_SHOW_UNUSED_COLUMNS))
                return;

            uint64_t typeNo = table->columns[col]->type;
            if (typeNo != SYS_COL_TYPE_VARCHAR
                    && typeNo != SYS_COL_TYPE_NUMBER
                    && typeNo != SYS_COL_TYPE_DATE
                    && typeNo != SYS_COL_TYPE_RAW
                    && typeNo != SYS_COL_TYPE_CHAR
                    && (typeNo != SYS_COL_TYPE_XMLTYPE || !after)
                    && typeNo != SYS_COL_TYPE_FLOAT
                    && typeNo != SYS_COL_TYPE_DOUBLE
                    && (typeNo != SYS_COL_TYPE_CLOB || !after)
                    && (typeNo != SYS_COL_TYPE_BLOB || !after)
                    && typeNo != SYS_COL_TYPE_TIMESTAMP
                    && typeNo != SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH
                    && typeNo != SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND
                    && typeNo != SYS_COL_TYPE_UROWID
                    && typeNo != SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ)
                return;
        }

        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        if (table != nullptr)
            appendEscape(table->columns[col]->name);
        else {
            std::string columnName("COL_" + std::to_string(col));
            append(columnName);
        }
        append(R"(":null)", sizeof(R"(":null)") - 1);
    }

    void BuilderJson::columnFloat(const std::string& columnName, double value) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);

        std::ostringstream ss;
        ss << value;
        append(ss.str());
    }

    void BuilderJson::columnDouble(const std::string& columnName, long double value) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);

        std::ostringstream ss;
        ss << value;
        append(ss.str());
    }

    void BuilderJson::columnString(const std::string& columnName) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":")", sizeof(R"(":")") - 1);
        appendEscape(valueBuffer, valueLength);
        append('"');
    }

    void BuilderJson::columnNumber(const std::string& columnName, uint64_t precision __attribute__((unused)), uint64_t scale __attribute__((unused))) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);
        append(valueBuffer, valueLength);
    }

    void BuilderJson::columnRowId(const std::string& columnName, typeRowId rowId) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":")", sizeof(R"(":")") - 1);
        char str[19];
        rowId.toHex(str);
        append(str, 18);
        append('"');
    }

    void BuilderJson::columnRaw(const std::string& columnName, const uint8_t* data, uint64_t length) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":")", sizeof(R"(":")") - 1);
        for (uint64_t j = 0; j < length; ++j)
            appendHex(*(data + j), 2);
        append('"');
    }

    void BuilderJson::columnTimestamp(const std::string& columnName, struct tm &epochTime, uint64_t fraction) {
        int64_t val;
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);

        switch (timestampFormat) {
            case TIMESTAMP_FORMAT_UNIX_NANO:
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                val = tmToEpoch(&epochTime);
                if (val == -1) {
                    appendSDec(val * 1000000000L + fraction);
                } else {
                    if (val < 0 && fraction > 0) {
                        ++val;
                        fraction = 1000000000 - fraction;
                    }
                    if (val != 0) {
                        appendSDec(val);
                        appendDec(fraction, 9);
                    } else {
                        appendDec(fraction);
                    }
                }
                break;
            case TIMESTAMP_FORMAT_UNIX_MICRO:
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000000L + ((fraction + 500) / 1000));
                break;
            case TIMESTAMP_FORMAT_UNIX_MILLI:
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000L + ((fraction + 500000) / 1000000));
                break;
            case TIMESTAMP_FORMAT_UNIX:
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) + ((fraction + 500000000) / 1000000000));
                break;
            case TIMESTAMP_FORMAT_UNIX_NANO_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                val = tmToEpoch(&epochTime);
                if (val == -1) {
                    appendSDec(val * 1000000000L + fraction);
                } else {
                    if (val < 0 && fraction > 0) {
                        ++val;
                        fraction = 1000000000 - fraction;
                    }
                    if (val != 0) {
                        appendSDec(val);
                        appendDec(fraction, 9);
                    } else {
                        appendDec(fraction);
                    }
                }
                append('"');
                break;
            case TIMESTAMP_FORMAT_UNIX_MICRO_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000000L + ((fraction + 500) / 1000));
                append('"');
                break;
            case TIMESTAMP_FORMAT_UNIX_MILLI_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000L + ((fraction + 500000) / 1000000));
                append('"');
                break;
            case TIMESTAMP_FORMAT_UNIX_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) + ((fraction + 500000000) / 1000000000));
                append('"');
                break;
            case TIMESTAMP_FORMAT_ISO8601:
                // 2012-04-23T18:25:43.511Z - ISO 8601 format
                append('"');
                if (epochTime.tm_year > 0) {
                    appendDec(static_cast<uint64_t>(epochTime.tm_year));
                } else {
                    appendDec(static_cast<uint64_t>(-epochTime.tm_year));
                    append("BC", sizeof("BC") - 1);
                }
                append('-');
                appendDec(epochTime.tm_mon, 2);
                append('-');
                appendDec(epochTime.tm_mday, 2);
                append('T');
                appendDec(epochTime.tm_hour, 2);
                append(':');
                appendDec(epochTime.tm_min, 2);
                append(':');
                appendDec(epochTime.tm_sec, 2);

                if (fraction > 0) {
                    append('.');
                    appendDec(fraction, 9);
                }

                append('"');
                break;
        }
    }

    void BuilderJson::columnTimestampTz(const std::string& columnName, struct tm &epochTime, uint64_t fraction, const char* tz) {
        int64_t val;
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);

        switch (timestampTzFormat) {
            case TIMESTAMP_TZ_FORMAT_UNIX_NANO_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                val = tmToEpoch(&epochTime);
                if (val == -1) {
                    appendSDec(val * 1000000000L + fraction);
                } else {
                    if (val < 0 && fraction > 0) {
                        ++val;
                        fraction = 1000000000 - fraction;
                    }
                    if (val != 0) {
                        appendSDec(val);
                        appendDec(fraction, 9);
                    } else
                        appendDec(fraction);
                }
                append(',');
                append(tz);
                append('"');
                break;
            case TIMESTAMP_TZ_FORMAT_UNIX_MICRO_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000000L + ((fraction + 500) / 1000));
                append(',');
                append(tz);
                append('"');
                break;
            case TIMESTAMP_TZ_FORMAT_UNIX_MILLI_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000L + ((fraction + 500000) / 1000000));
                append(',');
                append(tz);
                append('"');
                break;
            case TIMESTAMP_TZ_FORMAT_UNIX_STRING:
                append('"');
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) + ((fraction + 500000000) / 1000000000));
                append('"');
                break;
            case TIMESTAMP_TZ_FORMAT_ISO8601:
                // 2012-04-23T18:25:43.511Z - ISO 8601 format
                append('"');
                if (epochTime.tm_year > 0) {
                    appendDec(static_cast<uint64_t>(epochTime.tm_year));
                } else {
                    appendDec(static_cast<uint64_t>(-epochTime.tm_year));
                    append("BC", sizeof("BC") - 1);
                }
                append('-');
                appendDec(epochTime.tm_mon, 2);
                append('-');
                appendDec(epochTime.tm_mday, 2);
                append('T');
                appendDec(epochTime.tm_hour, 2);
                append(':');
                appendDec(epochTime.tm_min, 2);
                append(':');
                appendDec(epochTime.tm_sec, 2);

                if (fraction > 0) {
                    append('.');
                    appendDec(fraction, 9);
                }

                append(' ');
                append(tz);

                append('"');
                break;
        }
    }

    void BuilderJson::appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
        if ((messageFormat & MESSAGE_FORMAT_ADD_SEQUENCES) != 0) {
            append(R"(,"num":)", sizeof(R"(,"num":)") - 1);
            appendDec(num);
        }

        if (ridFormat == RID_FORMAT_SKIP)
            return;
        else if (ridFormat == RID_FORMAT_TEXT) {
            typeRowId rowId(dataObj, bdba, slot);
            char str[19];
            rowId.toString(str);
            append(R"(,"rid":")", sizeof(R"(,"rid":")") - 1);
            append(str, 18);
            append('"');
        }
    }

    void BuilderJson::appendHeader(typeScn scn, typeTime time_, bool first, bool showDb, bool showXid) {
        if (first || (scnAll & SCN_ALL_PAYLOADS) != 0) {
            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if ((scnFormat & SCN_FORMAT_TEXT_HEX) != 0) {
                append(R"("scns":"0x)", sizeof(R"("scns":"0x)") - 1);
                appendHex(scn, 16);
                append('"');
            } else {
                append(R"("scn":)", sizeof(R"("scn":)") - 1);
                appendDec(scn);
            }
        }

        if (first || (timestampAll & TIMESTAMP_ALL_PAYLOADS) != 0) {
            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            time_t val;
            switch (timestampFormat) {
                case TIMESTAMP_FORMAT_UNIX_NANO:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    val = time_.toTime();
                    appendDec(val);
                    if (val != 0)
                        append("000000000", 9);
                    break;
                case TIMESTAMP_FORMAT_UNIX_MICRO:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    val = time_.toTime();
                    appendDec(val);
                    if (val != 0)
                        append("000000", 6);
                    break;
                case TIMESTAMP_FORMAT_UNIX_MILLI:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    appendDec(time_.toTime());
                    append("000", 3);
                    break;
                case TIMESTAMP_FORMAT_UNIX:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    appendDec(time_.toTime());
                    break;
                case TIMESTAMP_FORMAT_UNIX_NANO_STRING:
                    append(R"("tms":")", sizeof(R"("tm":)") - 1);
                    val = time_.toTime();
                    appendDec(val);
                    if (val != 0)
                        append("000000000", 9);
                    append('"');
                    break;
                case TIMESTAMP_FORMAT_UNIX_MICRO_STRING:
                    append(R"("tms":")", sizeof(R"("tm":)") - 1);
                    val = time_.toTime();
                    appendDec(val);
                    if (val != 0)
                        append("000000", 6);
                    append('"');
                    break;
                case TIMESTAMP_FORMAT_UNIX_MILLI_STRING:
                    append(R"("tms":")", sizeof(R"("tm":)") - 1);
                    val = time_.toTime();
                    appendDec(val);
                    if (val != 0)
                        append("000", 3);
                    append('"');
                    break;
                case TIMESTAMP_FORMAT_UNIX_STRING:
                    append(R"("tms":")", sizeof(R"("tm":)") - 1);
                    appendDec(time_.toTime());
                    append('"');
                    break;
                case TIMESTAMP_FORMAT_ISO8601:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    char iso[21];
                    time_.toIso8601(iso);
                    append(iso, 20);
                    append('"');
                    break;
            }
        }

        if (hasPreviousValue)
            append(',');
        else
            hasPreviousValue = true;
        append(R"("c_scn":)", sizeof(R"("c_scn":)") - 1);
        appendDec(lwnScn);
        append(R"(,"c_idx":)", sizeof(R"(,"c_idx":)") - 1);
        appendDec(lwnIdx);

        if (showXid) {
            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if (xidFormat == XID_FORMAT_TEXT_HEX) {
                append(R"("xid":"0x)", sizeof(R"("xid":"0x)") - 1);
                appendHex(lastXid.usn(), 4);
                append('.');
                appendHex(lastXid.slt(), 3);
                append('.');
                appendHex(lastXid.sqn(), 8);
                append('"');
            } else if (xidFormat == XID_FORMAT_TEXT_DEC) {
                append(R"("xid":")", sizeof(R"("xid":")") - 1);
                appendDec(lastXid.usn());
                append('.');
                appendDec(lastXid.slt());
                append('.');
                appendDec(lastXid.sqn());
                append('"');
            } else if (xidFormat == XID_FORMAT_NUMERIC) {
                append(R"("xidn":)", sizeof(R"("xidn":)") - 1);
                appendDec(lastXid.getData());
            }
        }

        if (showDb) {
            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            append(R"("db":")", sizeof(R"("db":")") - 1);
            append(metadata->conName);
            append('"');
        }
    }

    void BuilderJson::appendSchema(OracleTable* table, typeObj obj) {
        if (table == nullptr) {
            std::string ownerName;
            std::string tableName;
            // try to read object name from ongoing uncommitted transaction data
            if (metadata->schema->checkTableDictUncommitted(obj, ownerName, tableName)) {
                append(R"("schema":{"owner":")", sizeof(R"("schema":{"owner":")") - 1);
                appendEscape(ownerName);
                append(R"(","table":")", sizeof(R"(","table":")") - 1);
                appendEscape(tableName);
                append('"');
            } else {
                append(R"("schema":{"table":")", sizeof(R"("schema":{"table":")") - 1);
                tableName = "OBJ_" + std::to_string(obj);
                append(tableName);
                append('"');
            }

            if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0) {
                append(R"(,"obj":)", sizeof(R"(,"obj":)") - 1);
                appendDec(obj);
            }
            append('}');
            return;
        }

        append(R"("schema":{"owner":")", sizeof(R"("schema":{"owner":")") - 1);
        appendEscape(table->owner);
        append(R"(","table":")", sizeof(R"(","table":")") - 1);
        appendEscape(table->name);
        append('"');

        if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0) {
            append(R"(,"obj":)", sizeof(R"(,"obj":)") - 1);
            appendDec(obj);
        }

        if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
            if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                if (tables.count(table) > 0)
                    return;
                else
                    tables.insert(table);
            }

            append(R"(,"columns":[)", sizeof(R"(,"columns":[)") - 1);

            bool hasPrev = false;
            for (typeCol column = 0; column < static_cast<typeCol>(table->columns.size()); ++column) {
                if (table->columns[column] == nullptr)
                    continue;

                if (hasPrev)
                    append(',');
                else
                    hasPrev = true;

                append(R"({"name":")", sizeof(R"({"name":")") - 1);
                appendEscape(table->columns[column]->name);

                append(R"(","type":)", sizeof(R"(","type":)") - 1);
                switch (table->columns[column]->type) {
                case SYS_COL_TYPE_VARCHAR:
                    append(R"("varchar2","length":)", sizeof(R"("varchar2","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_NUMBER:
                    append(R"("number","precision":)", sizeof(R"("number","precision":)") - 1);
                    appendSDec(table->columns[column]->precision);
                    append(R"(,"scale":)", sizeof(R"(,"scale":)") - 1);
                    appendSDec(table->columns[column]->scale);
                    break;

                // Long, not supported
                case SYS_COL_TYPE_LONG:
                    append(R"("long")", sizeof(R"("long")") - 1);
                    break;

                case SYS_COL_TYPE_DATE:
                    append(R"("date")", sizeof(R"("date")") - 1);
                    break;

                case SYS_COL_TYPE_RAW:
                    append(R"("raw","length":)", sizeof(R"("raw","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_LONG_RAW: // Not supported
                    append(R"("long raw")", sizeof(R"("long raw")") - 1);
                    break;

                case SYS_COL_TYPE_CHAR:
                    append(R"("char","length":)", sizeof(R"("char","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_FLOAT:
                    append(R"("binary_float")", sizeof(R"("binary_float")") - 1);
                    break;

                case SYS_COL_TYPE_DOUBLE:
                    append(R"("binary_double")", sizeof(R"("binary_double")") - 1);
                    break;

                case SYS_COL_TYPE_CLOB:
                    append(R"("clob")", sizeof(R"("clob")") - 1);
                    break;

                case SYS_COL_TYPE_BLOB:
                    append(R"("blob")", sizeof(R"("blob")") - 1);
                    break;

                case SYS_COL_TYPE_TIMESTAMP:
                    append(R"("timestamp","length":)", sizeof(R"("timestamp","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_TIMESTAMP_WITH_TZ:
                    append(R"("timestamp with time zone","length":)", sizeof(R"("timestamp with time zone","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH:
                    append(R"("interval year to month","length":)", sizeof(R"("interval year to month","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND:
                    append(R"("interval day to second","length":)", sizeof(R"("interval day to second","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_UROWID:
                    append(R"("urowid","length":)", sizeof(R"("urowid","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ:
                    append(R"("timestamp with local time zone","length":)", sizeof(R"("timestamp with local time zone","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                default:
                    append(R"("unknown")", sizeof(R"("unknown")") - 1);
                    break;
                }

                append(R"(,"nullable":)", sizeof(R"(,"nullable":)") - 1);
                if (table->columns[column]->nullable)
                    append("true");
                else
                    append("false");

                append('}');
            }
            append(']');
        }

        append('}');
    }

    time_t BuilderJson::tmToEpoch(struct tm* epoch) {
        static const int cumDays[12] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
        long year;
        time_t result;

        year = 1900 + epoch->tm_year;
        if (year > 0) {
            result = year * 365 + cumDays[epoch->tm_mon % 12];
            result += year / 4;
            result -= year / 100;
            result += year / 400;
            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && (epoch->tm_mon % 12) < 2)
                result--;
            result += epoch->tm_mday - 1;
            result *= 24;
            result += epoch->tm_hour;
            result *= 60;
            result += epoch->tm_min;
            result *= 60;
            result += epoch->tm_sec;
            return result - 62167132800L; // adjust to 1970 epoch, 719527 days
        } else {
            // treat dates BC with the exact rules as AD for leap years
            year = -year;
            result = year * 365 - cumDays[epoch->tm_mon % 12];
            result += year / 4;
            result -= year / 100;
            result += year / 400;
            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && (epoch->tm_mon % 12) < 2)
                result--;
            result -= epoch->tm_mday - 1;
            result *= 24;
            result -= epoch->tm_hour;
            result *= 60;
            result -= epoch->tm_min;
            result *= 60;
            result -= epoch->tm_sec;
            result = -result;
            return result - 62104147200L; // adjust to 1970 epoch, 718798 days (year 0 does not exist)
            return 0;
        }
    }

    void BuilderJson::processBeginMessage(typeScn scn, typeSeq sequence, typeTime time_) {
        newTran = false;
        hasPreviousRedo = false;

        if ((messageFormat & MESSAGE_FORMAT_SKIP_BEGIN) != 0)
            return;

        builderBegin(scn, sequence, 0, 0);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, time_, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

        if (hasPreviousValue)
            append(',');
        else
            hasPreviousValue = true;

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        } else {
            append(R"("payload":[{"op":"begin"}]})", sizeof(R"("payload":[{"op":"begin"}]})") - 1);
            builderCommit(false);
        }
    }

    void BuilderJson::processCommit(typeScn scn, typeSeq sequence, typeTime time_) {
        // Skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(true);
        } else if ((messageFormat & MESSAGE_FORMAT_SKIP_COMMIT) == 0) {
            builderBegin(scn, sequence, 0, 0);
            append('{');

            hasPreviousValue = false;
            appendHeader(scn, time_, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            append(R"("payload":[{"op":"commit"}]})", sizeof(R"("payload":[{"op":"commit"}]})") - 1);
            builderCommit(true);
        }
        num = 0;
    }

    void BuilderJson::processInsert(typeScn scn, typeSeq sequence, typeTime time_, LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj,
                                    typeDba bdba, typeSlot slot, typeXid xid  __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, time_);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, time_, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"c",)", sizeof(R"({"op":"c",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, table, offset);
        append('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processUpdate(typeScn scn, typeSeq sequence, typeTime time_, LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj,
                                    typeDba bdba, typeSlot slot, typeXid xid  __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, time_);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, time_, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"u",)", sizeof(R"({"op":"u",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, table, offset);
        appendAfter(lobCtx, table, offset);
        append('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processDelete(typeScn scn, typeSeq sequence, typeTime time_, LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj,
                                    typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, time_);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, time_, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"d",)", sizeof(R"({"op":"d",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, table, offset);
        append('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processDdl(typeScn scn, typeSeq sequence, typeTime time_, OracleTable* table, typeObj obj, typeDataObj dataObj __attribute__((unused)),
                                 uint16_t type __attribute__((unused)), uint16_t seq __attribute__((unused)), const char* operation __attribute__((unused)),
                                 const char* sql, uint64_t sqlLength) {
        if (newTran)
            processBeginMessage(scn, sequence, time_);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, time_, false, (dbFormat & DB_FORMAT_ADD_DDL) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"ddl",)", sizeof(R"({"op":"ddl",)") - 1);
        appendSchema(table, obj);
        append(R"(,"sql":")", sizeof(R"(,"sql":")") - 1);
        appendEscape(sql, sqlLength);
        append(R"("})", sizeof(R"("})") - 1);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(true);
        }
        ++num;
    }

    void BuilderJson::processCheckpoint(typeScn scn, typeSeq sequence, typeTime time_, uint64_t offset, bool redo) {
        if (lwnScn != scn) {
            lwnScn = scn;
            lwnIdx = 0;
        }

        builderBegin(scn, sequence, 0, OUTPUT_BUFFER_MESSAGE_CHECKPOINT);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, time_, true, false, false);

        if (hasPreviousValue)
            append(',');
        else
            hasPreviousValue = true;

        append(R"("payload":[{"op":"chkpt","seq":)", sizeof(R"("payload":[{"op":"chkpt","seq":)") - 1);
        appendDec(sequence);
        append(R"(,"offset":)", sizeof(R"(,"offset":)") - 1);
        appendDec(offset);
        if (redo)
            append(R"(,"redo":true)", sizeof(R"(,"redo":true)") - 1);
        append("}]}", sizeof("}]}") - 1);
        builderCommit(true);
    }
}
