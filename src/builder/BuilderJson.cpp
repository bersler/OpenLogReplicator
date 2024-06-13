/* Memory buffer for handling output buffer in JSON format
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

#include "../common/OracleColumn.h"
#include "../common/OracleTable.h"
#include "../common/typeRowId.h"
#include "../common/table/SysCol.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "BuilderJson.h"

namespace OpenLogReplicator {
    BuilderJson::BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat, uint64_t newAttributesFormat,
                             uint64_t newIntervalDtsFormat, uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat,
                             uint64_t newXidFormat, uint64_t newTimestampFormat, uint64_t newTimestampTzFormat, uint64_t newTimestampAll,
                             uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newScnAll, uint64_t newUnknownFormat, uint64_t newSchemaFormat,
                             uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer) :
            Builder(newCtx, newLocales, newMetadata, newDbFormat, newAttributesFormat, newIntervalDtsFormat, newIntervalYtmFormat, newMessageFormat,
                    newRidFormat, newXidFormat, newTimestampFormat, newTimestampTzFormat, newTimestampAll, newCharFormat, newScnFormat, newScnAll,
                    newUnknownFormat, newSchemaFormat, newColumnFormat, newUnknownType, newFlushBuffer),
            hasPreviousValue(false),
            hasPreviousRedo(false),
            hasPreviousColumn(false) {
    }

    void BuilderJson::columnNull(const OracleTable* table, typeCol col, bool after) {
        if (table != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
            const OracleColumn* column = table->columns[col];
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
                && typeNo != SYS_COL_TYPE_FLOAT
                && typeNo != SYS_COL_TYPE_DOUBLE
                && (typeNo != SYS_COL_TYPE_XMLTYPE || !after)
                && (typeNo != SYS_COL_TYPE_JSON || !after)
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

    void BuilderJson::columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);
        char buffer[22];

        switch (timestampFormat) {
            case TIMESTAMP_FORMAT_UNIX_NANO:
                // 1712345678123456789
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec(timestamp * 1000000000L + fraction);
                else {
                    // Big number
                    int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDec(timestamp * 1000000000L + fraction, 18);
                }
                break;

            case TIMESTAMP_FORMAT_UNIX_MICRO:
                // 1712345678123457
                appendSDec(timestamp * 1000000L + ((fraction + 500) / 1000));
                break;

            case TIMESTAMP_FORMAT_UNIX_MILLI:
                // 1712345678123
                appendSDec(timestamp * 1000L + ((fraction + 500000) / 1000000));
                break;

            case TIMESTAMP_FORMAT_UNIX:
                // 1712345678
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                break;

            case TIMESTAMP_FORMAT_UNIX_NANO_STRING:
                // "1712345678123456789"
                append('"');
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec(timestamp * 1000000000L + fraction);
                else {
                    // Big number
                    int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDec(timestamp * 1000000000L + fraction, 18);
                }
                append('"');
                break;

            case TIMESTAMP_FORMAT_UNIX_MICRO_STRING:
                // "1712345678123457"
                append('"');
                appendSDec(timestamp * 1000000L + ((fraction + 500) / 1000));
                append('"');
                break;

            case TIMESTAMP_FORMAT_UNIX_MILLI_STRING:
                // "1712345678123"
                append('"');
                appendSDec(timestamp * 1000L + ((fraction + 500000) / 1000000));
                append('"');
                break;

            case TIMESTAMP_FORMAT_UNIX_STRING:
                // "1712345678"
                append('"');
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                append('"');
                break;

            case TIMESTAMP_FORMAT_ISO8601_NANO_TZ:
                // "2024-04-05T19:34:38.123456789Z"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDec(fraction, 9);
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;

            case TIMESTAMP_FORMAT_ISO8601_MICRO_TZ:
                // "2024-04-05T19:34:38.123456Z"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDec(fraction, 6);
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;

            case TIMESTAMP_FORMAT_ISO8601_MILLI_TZ:
                // "2024-04-05T19:34:38.123Z"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDec(fraction, 3);
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;

            case TIMESTAMP_FORMAT_ISO8601_TZ:
                // "2024-04-05T19:34:38Z"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;
            case TIMESTAMP_FORMAT_ISO8601_NANO:
                // "2024-04-05 19:34:38.123456789"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDec(fraction, 9);
                append('"');
                break;

            case TIMESTAMP_FORMAT_ISO8601_MICRO:
                // "2024-04-05 19:34:38.123456"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDec(fraction, 6);
                append('"');
                break;

            case TIMESTAMP_FORMAT_ISO8601_MILLI:
                // "2024-04-05 19:34:38.123"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDec(fraction, 3);
                append('"');
                break;

            case TIMESTAMP_FORMAT_ISO8601:
                // "2024-04-05 19:34:38"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('"');
                break;
        }
    }

    void BuilderJson::columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const char* tz) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);
        char buffer[22];

        switch (timestampTzFormat) {
            case TIMESTAMP_TZ_FORMAT_UNIX_NANO_STRING:
                // "1700000000.123456789,Europe/Warsaw"
                append('"');
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec(timestamp * 1000000000L + fraction);
                else {
                    // Big number
                    int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDec(timestamp * 1000000000L + fraction, 18);
                }
                append(',');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_UNIX_MICRO_STRING:
                // "1700000000.123456,Europe/Warsaw"
                append('"');
                appendSDec(timestamp * 1000000L + ((fraction + 500) / 1000));
                append(',');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_UNIX_MILLI_STRING:
                // "1700000000.123,Europe/Warsaw"
                append('"');
                appendSDec(timestamp * 1000L + ((fraction + 500000) / 1000000));
                append(',');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_UNIX_STRING:
                // "1700000000,Europe/Warsaw"
                append('"');
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                append(',');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_NANO_TZ:
                // "2024-04-05T19:34:38.123456789Z Europe/Warsaw"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDec(fraction, 9);
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_MICRO_TZ:
                // "2024-04-05T19:34:38.123456Z Europe/Warsaw"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDec(fraction, 6);
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_MILLI_TZ:
                // "2024-04-05T19:34:38.123Z Europe/Warsaw"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDec(fraction, 3);
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_TZ:
                // "2024-04-05T19:34:38Z Europe/Warsaw"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_NANO:
                // "2024-04-05 19:34:38.123456789,Europe/Warsaw"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDec(fraction, 9);
                append(' ');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_MICRO:
                // "2024-04-05 19:34:38.123456,Europe/Warsaw"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDec(fraction, 6);
                append(' ');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601_MILLI:
                // "2024-04-05 19:34:38.123 Europe/Warsaw"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDec(fraction, 3);
                append(' ');
                append(tz);
                append('"');
                break;

            case TIMESTAMP_TZ_FORMAT_ISO8601:
                // "2024-04-05 19:34:38 Europe/Warsaw"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
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

    void BuilderJson::appendHeader(typeScn scn, time_t timestamp, bool first, bool showDb, bool showXid) {
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

            char buffer[22];
            switch (timestampFormat) {
                case TIMESTAMP_FORMAT_UNIX_NANO:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    appendDec(timestamp);
                    if (timestamp != 0)
                        append("000000000", 9);
                    break;

                case TIMESTAMP_FORMAT_UNIX_MICRO:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    appendDec(timestamp);
                    if (timestamp != 0)
                        append("000000", 6);
                    break;

                case TIMESTAMP_FORMAT_UNIX_MILLI:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    appendDec(timestamp);
                    if (timestamp != 0)
                        append("000", 3);
                    break;

                case TIMESTAMP_FORMAT_UNIX:
                    append(R"("tm":)", sizeof(R"("tm":)") - 1);
                    appendDec(timestamp);
                    break;

                case TIMESTAMP_FORMAT_UNIX_NANO_STRING:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    appendDec(timestamp);
                    if (timestamp != 0)
                        append("000000000", 9);
                    append('"');
                    break;

                case TIMESTAMP_FORMAT_UNIX_MICRO_STRING:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    appendDec(timestamp);
                    if (timestamp != 0)
                        append("000000", 6);
                    append('"');
                    break;

                case TIMESTAMP_FORMAT_UNIX_MILLI_STRING:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    appendDec(timestamp);
                    if (timestamp != 0)
                        append("000", 3);
                    append('"');
                    break;

                case TIMESTAMP_FORMAT_UNIX_STRING:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    appendDec(timestamp);
                    append('"');
                    break;

                case TIMESTAMP_FORMAT_ISO8601_NANO_TZ:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                    append(R"(.000000000Z")", sizeof(R"(.000000000Z")") - 1);
                    break;

                case TIMESTAMP_FORMAT_ISO8601_MICRO_TZ:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, true, true));
                    append(R"(.000000Z")", sizeof(R"(.000000Z")") - 1);
                    break;

                case TIMESTAMP_FORMAT_ISO8601_MILLI_TZ:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                    append(R"(.000Z")", sizeof(R"(.000Z")") - 1);
                    break;

                case TIMESTAMP_FORMAT_ISO8601_TZ:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, true, true));
                    append('"');
                    break;

                case TIMESTAMP_FORMAT_ISO8601_NANO:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                    append(R"(.000000000")", sizeof(R"(.000000000")") - 1);
                    break;

                case TIMESTAMP_FORMAT_ISO8601_MICRO:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                    append(R"(.000000")", sizeof(R"(.000000")") - 1);
                    break;

                case TIMESTAMP_FORMAT_ISO8601_MILLI:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                    append(R"(.000")", sizeof(R"(.000")") - 1);
                    break;

                case TIMESTAMP_FORMAT_ISO8601:
                    append(R"("tms":")", sizeof(R"("tms":")") - 1);
                    append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
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
            append(metadata->conName.length() ? metadata->conName : metadata->context);
            append('"');
        }
    }

    void BuilderJson::appendAttributes() {
        append(R"("attributes":{)", sizeof(R"("attributes":[)") - 1);
        bool hasPreviousAttribute = false;
        for (const auto& attributeIt: *attributes) {
            if (hasPreviousAttribute)
                append(',');
            else
                hasPreviousAttribute = true;

            append('"');
            appendEscape(attributeIt.first);
            append(R"(":")", sizeof(R"(":")") - 1);
            appendEscape(attributeIt.second);
            append('"');
        }
        append("},", 2);
    }

    void BuilderJson::appendSchema(const OracleTable* table, typeObj obj) {
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

                    case SYS_COL_TYPE_LONG:
                        // Long, not supported
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

    void BuilderJson::processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) {
        newTran = false;
        hasPreviousRedo = false;

        if ((messageFormat & MESSAGE_FORMAT_SKIP_BEGIN) != 0)
            return;

        builderBegin(scn, sequence, 0, 0);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

        if (hasPreviousValue)
            append(',');
        else
            hasPreviousValue = true;

        if ((attributesFormat & ATTRIBUTES_FORMAT_BEGIN) != 0)
            appendAttributes();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        } else {
            append(R"("payload":[{"op":"begin"}]})", sizeof(R"("payload":[{"op":"begin"}]})") - 1);
            builderCommit(false);
        }
    }

    void BuilderJson::processCommit(typeScn scn, typeSeq sequence, time_t timestamp) {
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
            appendHeader(scn, timestamp, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if ((attributesFormat & ATTRIBUTES_FORMAT_COMMIT) != 0)
                appendAttributes();

            append(R"("payload":[{"op":"commit"}]})", sizeof(R"("payload":[{"op":"commit"}]})") - 1);
            builderCommit(true);
        }
        num = 0;
    }

    void BuilderJson::processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                    typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid  __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if ((attributesFormat & ATTRIBUTES_FORMAT_DML) != 0)
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"c",)", sizeof(R"({"op":"c",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, xmlCtx, table, offset);
        append('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                    typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid  __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if ((attributesFormat & ATTRIBUTES_FORMAT_DML) != 0)
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"u",)", sizeof(R"({"op":"u",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, offset);
        appendAfter(lobCtx, xmlCtx, table, offset);
        append('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                    typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if ((attributesFormat & ATTRIBUTES_FORMAT_DML) != 0)
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"d",)", sizeof(R"({"op":"d",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, offset);
        append('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const OracleTable* table, typeObj obj, typeDataObj dataObj __attribute__((unused)),
                                 uint16_t type __attribute__((unused)), uint16_t seq __attribute__((unused)), const char* sql, uint64_t sqlLength,
                                 const char* owner, uint64_t ownerLength, const char* name, uint64_t nameLength) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, 0);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, (dbFormat & DB_FORMAT_ADD_DDL) != 0, true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if ((attributesFormat & ATTRIBUTES_FORMAT_DML) != 0)
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"ddl",)", sizeof(R"({"op":"ddl",)") - 1);
        appendSchema(table, obj);

        append(R"(,"owner":")", sizeof(R"(,"owner":")")-1);
        appendEscape(owner, ownerLength);
        append(R"(")", sizeof(R"(")")-1);

        append(R"(,"table":)",  sizeof(R"(,"table":)")-1);
        appendEscape(name, nameLength);
        append(R"(")", sizeof(R"(")")-1);

        append(R"(,"sql":")", sizeof(R"(,"sql":")") - 1);
        appendEscape(sql, sqlLength);
        append(R"("})", sizeof(R"("})") - 1);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            append("]}", sizeof("]}") - 1);
            builderCommit(true);
        }
        ++num;
    }

    void BuilderJson::processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp, uint64_t offset, bool redo) {
        if (lwnScn != scn) {
            lwnScn = scn;
            lwnIdx = 0;
        }

        builderBegin(scn, sequence, 0, OUTPUT_BUFFER_MESSAGE_CHECKPOINT);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, timestamp, true, false, false);

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
