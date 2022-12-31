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
#include "BuilderJson.h"

namespace OpenLogReplicator {
    BuilderJson::BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat,
                             uint64_t newTimestampFormat, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newUnknownFormat, uint64_t newSchemaFormat,
                             uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer) :
        Builder(newCtx, newLocales, newMetadata, newMessageFormat, newRidFormat, newXidFormat, newTimestampFormat, newCharFormat, newScnFormat,
                newUnknownFormat, newSchemaFormat, newColumnFormat, newUnknownType, newFlushBuffer),
                hasPreviousValue(false),
                hasPreviousRedo(false),
                hasPreviousColumn(false) {
    }

    void BuilderJson::columnNull(OracleTable* table, typeCol col, bool after) {
        if (table != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
            OracleColumn* column = table->columns[col];
            if (column->constraint && !FLAG(REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS))
                return;
            if (column->nested && !FLAG(REDO_FLAGS_SHOW_NESTED_COLUMNS))
                return;
            if (column->invisible && !FLAG(REDO_FLAGS_SHOW_INVISIBLE_COLUMNS))
                return;
            if (column->unused && !FLAG(REDO_FLAGS_SHOW_UNUSED_COLUMNS))
                return;

            uint64_t typeNo = table->columns[col]->type;
            if (typeNo != SYS_COL_TYPE_VARCHAR
                    && typeNo != SYS_COL_TYPE_CHAR
                    && typeNo != SYS_COL_TYPE_NUMBER
                    && typeNo != SYS_COL_TYPE_DATE
                    && typeNo != SYS_COL_TYPE_TIMESTAMP
                    && typeNo != SYS_COL_TYPE_RAW
                    && typeNo != SYS_COL_TYPE_FLOAT
                    && typeNo != SYS_COL_TYPE_DOUBLE
                    && (typeNo != SYS_COL_TYPE_BLOB || !after)
                    && (typeNo != SYS_COL_TYPE_CLOB || !after)
                    && typeNo != SYS_COL_TYPE_TIMESTAMP_WITH_TZ)
                return;
        }

        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        if (table != nullptr)
            builderAppend(table->columns[col]->name);
        else {
            std::string columnName("COL_" + std::to_string(col));
            builderAppend(columnName);
        }
        builderAppend(R"(":null)", sizeof(R"(":null)") - 1);
    }

    void BuilderJson::columnFloat(const std::string& columnName, double value) {
        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        builderAppend(columnName);
        builderAppend(R"(":)", sizeof(R"(":)") - 1);

        std::ostringstream ss;
        ss << value;
        builderAppend(ss.str());
    }

    void BuilderJson::columnDouble(const std::string& columnName, long double value) {
        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        builderAppend(columnName);
        builderAppend(R"(":)", sizeof(R"(":)") - 1);

        std::ostringstream ss;
        ss << value;
        builderAppend(ss.str());
    }

    void BuilderJson::columnString(const std::string& columnName) {
        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        builderAppend(columnName);
        builderAppend(R"(":")", sizeof(R"(":")") - 1);
        appendEscape(valueBuffer, valueLength);
        builderAppend('"');
    }

    void BuilderJson::columnNumber(const std::string& columnName, uint64_t precision __attribute__((unused)), uint64_t scale __attribute__((unused))) {
        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        builderAppend(columnName);
        builderAppend(R"(":)", sizeof(R"(":)") - 1);
        builderAppend(valueBuffer, valueLength);
    }

    void BuilderJson::columnRaw(const std::string& columnName, const uint8_t* data, uint64_t length) {
        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        builderAppend(columnName);
        builderAppend(R"(":")", sizeof(R"(":")") - 1);
        for (uint64_t j = 0; j < length; ++j)
            appendHex(*(data + j), 2);
        builderAppend('"');
    }

    void BuilderJson::columnTimestamp(const std::string& columnName, struct tm &epochTime, uint64_t fraction, const char* tz) {
        if (hasPreviousColumn)
            builderAppend(',');
        else
            hasPreviousColumn = true;

        builderAppend('"');
        builderAppend(columnName);
        builderAppend(R"(":)", sizeof(R"(":)") - 1);

        if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
            // 2012-04-23T18:25:43.511Z - ISO 8601 format
            builderAppend('"');
            if (epochTime.tm_year > 0) {
                appendDec(static_cast<uint64_t>(epochTime.tm_year));
            } else {
                appendDec(static_cast<uint64_t>(-epochTime.tm_year));
                builderAppend("BC", sizeof("BC") - 1);
            }
            builderAppend('-');
            appendDec(epochTime.tm_mon, 2);
            builderAppend('-');
            appendDec(epochTime.tm_mday, 2);
            builderAppend('T');
            appendDec(epochTime.tm_hour, 2);
            builderAppend(':');
            appendDec(epochTime.tm_min, 2);
            builderAppend(':');
            appendDec(epochTime.tm_sec, 2);

            if (fraction > 0) {
                builderAppend('.');
                appendDec(fraction, 9);
            }

            if (tz != nullptr) {
                builderAppend(' ');
                builderAppend(tz);
            }
            builderAppend('"');
        } else {
            // Unix epoch format
            if (epochTime.tm_year >= 1900) {
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendSDec(tmToEpoch(&epochTime) * 1000 + ((fraction + 500000) / 1000000));
            } else
                appendDec(0);
        }
    }

    void BuilderJson::appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
        if ((messageFormat & MESSAGE_FORMAT_ADD_SEQUENCES) != 0) {
            builderAppend(R"(,"num":)", sizeof(R"(,"num":)") - 1);
            appendDec(num);
        }

        if (ridFormat == RID_FORMAT_SKIP)
            return;

        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        builderAppend(R"(,"rid":")", sizeof(R"(,"rid":")") - 1);
        builderAppend(str, 18);
        builderAppend('"');
    }

    void BuilderJson::appendHeader(bool first, bool showXid) {
        if (first || (scnFormat & SCN_FORMAT_ALL_PAYLOADS) != 0) {
            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            if ((scnFormat & SCN_FORMAT_HEX) != 0) {
                builderAppend(R"("scns":"0x)", sizeof(R"("scns":"0x)") - 1);
                appendHex(lastScn, 16);
                builderAppend('"');
            } else {
                builderAppend(R"("scn":)", sizeof(R"("scn":)") - 1);
                appendDec(lastScn);
            }
        }

        if (first || (timestampFormat & TIMESTAMP_FORMAT_ALL_PAYLOADS) != 0) {
            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
                builderAppend(R"("tms":")", sizeof(R"("tms":")") - 1);
                char iso[21];
                lastTime.toIso8601(iso);
                builderAppend(iso, 20);
                builderAppend('"');
            } else {
                builderAppend(R"("tm":)", sizeof(R"("tm":)") - 1);
                appendDec(lastTime.toTime() * 1000);
            }
        }

        if (showXid) {
            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            if (xidFormat == XID_FORMAT_TEXT_HEX) {
                builderAppend(R"("xid":"0x)", sizeof(R"("xid":"0x)") - 1);
                appendHex(lastXid.usn(), 4);
                builderAppend('.');
                appendHex(lastXid.slt(), 3);
                builderAppend('.');
                appendHex(lastXid.sqn(), 8);
                builderAppend('"');
            } else if (xidFormat == XID_FORMAT_TEXT_DEC) {
                builderAppend(R"("xid":")", sizeof(R"("xid":")") - 1);
                appendDec(lastXid.usn());
                builderAppend('.');
                appendDec(lastXid.slt());
                builderAppend('.');
                appendDec(lastXid.sqn());
                builderAppend('"');
            } else {
                builderAppend(R"("xidn":)", sizeof(R"("xidn":)") - 1);
                appendDec(lastXid.getData());
            }
        }
    }

    void BuilderJson::appendSchema(OracleTable* table, typeObj obj) {
        if (table == nullptr) {
            builderAppend(R"("schema":{"table":")", sizeof(R"("schema":{"table":")") - 1);
            std::string tableName("OBJ_" + std::to_string(obj));
            builderAppend(tableName);
            builderAppend(R"("})");
            return;
        }

        builderAppend(R"("schema":{"owner":")", sizeof(R"("schema":{"owner":")") - 1);
        builderAppend(table->owner);
        builderAppend(R"(","table":")", sizeof(R"(","table":")") - 1);
        builderAppend(table->name);
        builderAppend('"');

        if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0) {
            builderAppend(R"(,"obj":)", sizeof(R"(,"obj":)") - 1);
            appendDec(table->obj);
        }

        if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
            if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                if (tables.count(table) > 0)
                    return;
                else
                    tables.insert(table);
            }

            builderAppend(R"(,"columns":[)", sizeof(R"(,"columns":[)") - 1);

            bool hasPrev = false;
            for (typeCol column = 0; column < static_cast<typeCol>(table->columns.size()); ++column) {
                if (table->columns[column] == nullptr)
                    continue;

                if (hasPrev)
                    builderAppend(',');
                else
                    hasPrev = true;

                builderAppend(R"({"name":")", sizeof(R"({"name":")") - 1);
                builderAppend(table->columns[column]->name);

                builderAppend(R"(","type":)", sizeof(R"(","type":)") - 1);
                switch(table->columns[column]->type) {
                case SYS_COL_TYPE_VARCHAR:
                    builderAppend(R"("varchar2","length":)", sizeof(R"("varchar2","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_NUMBER:
                    builderAppend(R"("number","precision":)", sizeof(R"("number","precision":)") - 1);
                    appendSDec(table->columns[column]->precision);
                    builderAppend(R"(,"scale":)", sizeof(R"(,"scale":)") - 1);
                    appendSDec(table->columns[column]->scale);
                    break;

                case SYS_COL_TYPE_LONG: // long, not supported
                    builderAppend(R"("long")", sizeof(R"("long")") - 1);
                    break;

                case SYS_COL_TYPE_DATE:
                    builderAppend(R"("date")", sizeof(R"("date")") - 1);
                    break;

                case SYS_COL_TYPE_RAW:
                    builderAppend(R"("raw","length":)", sizeof(R"("raw","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_LONG_RAW: // Not supported
                    builderAppend(R"("long raw")", sizeof(R"("long raw")") - 1);
                    break;

                case SYS_COL_TYPE_ROWID: // Not supported
                    builderAppend(R"("rowid")", sizeof(R"("rowid")") - 1);
                    break;

                case SYS_COL_TYPE_CHAR:
                    builderAppend(R"("char","length":)", sizeof(R"("char","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_FLOAT:
                    builderAppend(R"("binary_float")", sizeof(R"("binary_float")") - 1);
                    break;

                case SYS_COL_TYPE_DOUBLE:
                    builderAppend(R"("binary_double")", sizeof(R"("binary_double")") - 1);
                    break;

                case SYS_COL_TYPE_CLOB:
                    builderAppend(R"("clob")", sizeof(R"("clob")") - 1);
                    break;

                case SYS_COL_TYPE_BLOB:
                    builderAppend(R"("blob")", sizeof(R"("blob")") - 1);
                    break;

                case SYS_COL_TYPE_TIMESTAMP:
                    builderAppend(R"("timestamp","length":)", sizeof(R"("timestamp","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_TIMESTAMP_WITH_TZ:
                    builderAppend(R"("timestamp with time zone","length":)", sizeof(R"("timestamp with time zone","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH:
                    builderAppend(R"("interval year to month","length":)", sizeof(R"("interval year to month","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND:
                    builderAppend(R"("interval day to second","length":)", sizeof(R"("interval day to second","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_URAWID:
                    builderAppend(R"("urawid","length":)", sizeof(R"("urawid","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                case SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ: // Not supported
                    builderAppend(R"("timestamp with local time zone","length":)", sizeof(R"("timestamp with local time zone","length":)") - 1);
                    appendDec(table->columns[column]->length);
                    break;

                default:
                    builderAppend(R"("unknown")", sizeof(R"("unknown")") - 1);
                    break;
                }

                builderAppend(R"(,"nullable":)", sizeof(R"(,"nullable":)") - 1);
                if (table->columns[column]->nullable)
                    builderAppend('1');
                else
                    builderAppend('0');

                builderAppend('}');
            }
            builderAppend(']');
        }

        builderAppend('}');
    }

    time_t BuilderJson::tmToEpoch(struct tm* epoch) {
        static const int cumdays[12] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
        long year;
        time_t result;

        year = 1900 + epoch->tm_year + epoch->tm_mon / 12;
        result = (year - 1970) * 365 + cumdays[epoch->tm_mon % 12];
        result += (year - 1968) / 4;
        result -= (year - 1900) / 100;
        result += (year - 1600) / 400;
        if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) &&
            (epoch->tm_mon % 12) < 2)
            result--;
        result += epoch->tm_mday - 1;
        result *= 24;
        result += epoch->tm_hour;
        result *= 60;
        result += epoch->tm_min;
        result *= 60;
        result += epoch->tm_sec;
        return result;
    }

    void BuilderJson::processBeginMessage() {
        newTran = false;
        hasPreviousRedo = false;

        if ((messageFormat & MESSAGE_FORMAT_SKIP_BEGIN) != 0)
            return;

        builderBegin(0);
        builderAppend('{');
        hasPreviousValue = false;
        appendHeader(true, true);

        if (hasPreviousValue)
            builderAppend(',');
        else
            hasPreviousValue = true;

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            builderAppend(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        } else {
            builderAppend(R"("payload":[{"op":"begin"}]})", sizeof(R"("payload":[{"op":"begin"}]})") - 1);
            builderCommit(false);
        }
    }

    void BuilderJson::processCommit() {
        // Skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            builderAppend("]}", sizeof("]}") - 1);
            builderCommit(true);
        } else if ((messageFormat & MESSAGE_FORMAT_SKIP_COMMIT) == 0) {
            builderBegin(0);
            builderAppend('{');

            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            builderAppend(R"("payload":[{"op":"commit"}]})", sizeof(R"("payload":[{"op":"commit"}]})") - 1);
            builderCommit(true);
        }
        num = 0;
    }

    void BuilderJson::processInsert(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot,
                                    typeXid xid  __attribute__((unused))) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                builderAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            builderAppend('{');
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            builderAppend(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        builderAppend(R"({"op":"c",)", sizeof(R"({"op":"c",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, table);
        builderAppend('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            builderAppend("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processUpdate(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot,
                                    typeXid xid  __attribute__((unused))) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                builderAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            builderAppend('{');
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            builderAppend(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        builderAppend(R"({"op":"u",)", sizeof(R"({"op":"u",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, table);
        appendAfter(lobCtx, table);
        builderAppend('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            builderAppend("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processDelete(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot,
                                    typeXid xid __attribute__((unused))) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                builderAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            builderAppend('{');
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            builderAppend(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        builderAppend(R"({"op":"d",)", sizeof(R"({"op":"d",)") - 1);
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, table);
        builderAppend('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            builderAppend("]}", sizeof("]}") - 1);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderJson::processDdl(OracleTable* table, typeDataObj dataObj __attribute__((unused)), uint16_t type __attribute__((unused)),
                                 uint16_t seq __attribute__((unused)), const char* operation __attribute__((unused)), const char* sql, uint64_t sqlLength) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (hasPreviousRedo)
                builderAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            builderAppend('{');
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                builderAppend(',');
            else
                hasPreviousValue = true;

            builderAppend(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        builderAppend(R"({"op":"ddl","sql":")", sizeof(R"({"op":"ddl","sql":")") - 1);
        appendEscape(sql, sqlLength);
        builderAppend(R"("})", sizeof(R"("})") - 1);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            builderAppend("]}", sizeof("]}") - 1);
            builderCommit(true);
        }
        ++num;
    }

    void BuilderJson::processCheckpoint(typeScn scn, typeTime time_, typeSeq sequence, uint64_t offset, bool redo) {
        if (FLAG(REDO_FLAGS_HIDE_CHECKPOINT))
            return;

        lastTime = time_;
        lastScn = scn;
        lastSequence = sequence;
        builderBegin(0);
        builderAppend('{');
        hasPreviousValue = false;
        appendHeader(true, false);

        if (hasPreviousValue)
            builderAppend(',');
        else
            hasPreviousValue = true;

        builderAppend(R"("payload":[{"op":"chkpt","seq":)", sizeof(R"("payload":[{"op":"chkpt","seq":)") - 1);
        appendDec(sequence);
        builderAppend(R"(,"offset":)", sizeof(R"(,"offset":)") - 1);
        appendDec(offset);
        if (redo)
            builderAppend(R"(,"redo":true)", sizeof(R"(,"redo":true)") - 1);
        builderAppend("}]}", sizeof("}]}") - 1);
        builderCommit(true);
    }
}
