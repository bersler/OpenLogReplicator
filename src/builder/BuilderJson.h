/* Header for BuilderJson class
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

#include "Builder.h"
#include "../common/OracleColumn.h"
#include "../common/OracleTable.h"
#include "../common/table/SysCol.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"

#ifndef BUILDER_JSON_H_
#define BUILDER_JSON_H_

namespace OpenLogReplicator {
    class BuilderJson final : public Builder {
    protected:
        bool hasPreviousValue;
        bool hasPreviousRedo;
        bool hasPreviousColumn;

        inline void columnNull(const OracleTable* table, typeCol col, bool after) {
            if (table != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
                const OracleColumn* column = table->columns[col];
                if (column->guard && !ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_GUARD_COLUMNS))
                    return;
                if (column->nested && !ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_NESTED_COLUMNS))
                    return;
                if (column->hidden && !ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_HIDDEN_COLUMNS))
                    return;
                if (column->unused && !ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_UNUSED_COLUMNS))
                    return;

                uint64_t typeNo = table->columns[col]->type;
                if (typeNo != SysCol::TYPE_VARCHAR
                    && typeNo != SysCol::TYPE_NUMBER
                    && typeNo != SysCol::TYPE_DATE
                    && typeNo != SysCol::TYPE_RAW
                    && typeNo != SysCol::TYPE_CHAR
                    && typeNo != SysCol::TYPE_FLOAT
                    && typeNo != SysCol::TYPE_DOUBLE
                    && (typeNo != SysCol::TYPE_XMLTYPE || !after)
                    && (typeNo != SysCol::TYPE_JSON || !after)
                    && (typeNo != SysCol::TYPE_CLOB || !after)
                    && (typeNo != SysCol::TYPE_BLOB || !after)
                    && typeNo != SysCol::TYPE_TIMESTAMP
                    && typeNo != SysCol::TYPE_INTERVAL_YEAR_TO_MONTH
                    && typeNo != SysCol::TYPE_INTERVAL_DAY_TO_SECOND
                    && typeNo != SysCol::TYPE_UROWID
                    && typeNo != SysCol::TYPE_TIMESTAMP_WITH_LOCAL_TZ)
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

        inline void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
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

        inline void appendHeader(typeScn scn, time_t timestamp, bool first, bool showDb, bool showXid) {
            if (first || (scnAll & SCN_ALL_PAYLOADS) != 0) {
                if (hasPreviousValue)
                    append(',');
                else
                    hasPreviousValue = true;

                if ((scnFormat & SCN_FORMAT_TEXT_HEX) != 0) {
                    append(R"("scns":"0x)", sizeof(R"("scns":"0x)") - 1);
                    appendHex16(scn);
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
                    appendHex4(lastXid.usn());
                    append('.');
                    appendHex3(lastXid.slt());
                    append('.');
                    appendHex8(lastXid.sqn());
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

        inline void appendAttributes() {
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

        inline void appendSchema(const OracleTable* table, typeObj obj) {
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
                        case SysCol::TYPE_VARCHAR:
                            append(R"("varchar2","length":)", sizeof(R"("varchar2","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_NUMBER:
                            append(R"("number","precision":)", sizeof(R"("number","precision":)") - 1);
                            appendSDec(table->columns[column]->precision);
                            append(R"(,"scale":)", sizeof(R"(,"scale":)") - 1);
                            appendSDec(table->columns[column]->scale);
                            break;

                        case SysCol::TYPE_LONG:
                            // Long, not supported
                            append(R"("long")", sizeof(R"("long")") - 1);
                            break;

                        case SysCol::TYPE_DATE:
                            append(R"("date")", sizeof(R"("date")") - 1);
                            break;

                        case SysCol::TYPE_RAW:
                            append(R"("raw","length":)", sizeof(R"("raw","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_LONG_RAW: // Not supported
                            append(R"("long raw")", sizeof(R"("long raw")") - 1);
                            break;

                        case SysCol::TYPE_CHAR:
                            append(R"("char","length":)", sizeof(R"("char","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_FLOAT:
                            append(R"("binary_float")", sizeof(R"("binary_float")") - 1);
                            break;

                        case SysCol::TYPE_DOUBLE:
                            append(R"("binary_double")", sizeof(R"("binary_double")") - 1);
                            break;

                        case SysCol::TYPE_CLOB:
                            append(R"("clob")", sizeof(R"("clob")") - 1);
                            break;

                        case SysCol::TYPE_BLOB:
                            append(R"("blob")", sizeof(R"("blob")") - 1);
                            break;

                        case SysCol::TYPE_TIMESTAMP:
                            append(R"("timestamp","length":)", sizeof(R"("timestamp","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_TIMESTAMP_WITH_TZ:
                            append(R"("timestamp with time zone","length":)", sizeof(R"("timestamp with time zone","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_INTERVAL_YEAR_TO_MONTH:
                            append(R"("interval year to month","length":)", sizeof(R"("interval year to month","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_INTERVAL_DAY_TO_SECOND:
                            append(R"("interval day to second","length":)", sizeof(R"("interval day to second","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_UROWID:
                            append(R"("urowid","length":)", sizeof(R"("urowid","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::TYPE_TIMESTAMP_WITH_LOCAL_TZ:
                            append(R"("timestamp with local time zone","length":)", sizeof(R"("timestamp with local time zone","length":)") - 1);
                            appendDec(table->columns[column]->length);
                            break;

                        default:
                            append(R"("unknown")", sizeof(R"("unknown")") - 1);
                            break;
                    }

                    append(R"(,"nullable":)", sizeof(R"(,"nullable":)") - 1);
                    if (table->columns[column]->nullable)
                        append("true", sizeof("true") - 1);
                    else
                        append("false", sizeof("false") - 1);

                    append('}');
                }
                append(']');
            }

            append('}');
        }

        inline void appendHex2(uint8_t value) {
            append(Ctx::map16((value >> 4) & 0xF));
            append(Ctx::map16(value & 0xF));
        }

        inline void appendHex3(uint16_t value) {
            append(Ctx::map16((value >> 8) & 0xF));
            append(Ctx::map16((value >> 4) & 0xF));
            append(Ctx::map16(value & 0xF));
        }

        inline void appendHex4(uint16_t value) {
            append(Ctx::map16((value >> 12) & 0xF));
            append(Ctx::map16((value >> 8) & 0xF));
            append(Ctx::map16((value >> 4) & 0xF));
            append(Ctx::map16(value & 0xF));
        }

        inline void appendHex8(uint32_t value) {
            append(Ctx::map16((value >> 28) & 0xF));
            append(Ctx::map16((value >> 24) & 0xF));
            append(Ctx::map16((value >> 20) & 0xF));
            append(Ctx::map16((value >> 16) & 0xF));
            append(Ctx::map16((value >> 12) & 0xF));
            append(Ctx::map16((value >> 8) & 0xF));
            append(Ctx::map16((value >> 4) & 0xF));
            append(Ctx::map16(value & 0xF));
        }

        inline void appendHex16(uint64_t value) {
            append(Ctx::map16((value >> 60) & 0xF));
            append(Ctx::map16((value >> 56) & 0xF));
            append(Ctx::map16((value >> 52) & 0xF));
            append(Ctx::map16((value >> 48) & 0xF));
            append(Ctx::map16((value >> 44) & 0xF));
            append(Ctx::map16((value >> 40) & 0xF));
            append(Ctx::map16((value >> 36) & 0xF));
            append(Ctx::map16((value >> 32) & 0xF));
            append(Ctx::map16((value >> 28) & 0xF));
            append(Ctx::map16((value >> 24) & 0xF));
            append(Ctx::map16((value >> 20) & 0xF));
            append(Ctx::map16((value >> 16) & 0xF));
            append(Ctx::map16((value >> 12) & 0xF));
            append(Ctx::map16((value >> 8) & 0xF));
            append(Ctx::map16((value >> 4) & 0xF));
            append(Ctx::map16(value & 0xF));
        }

        inline void appendDec(uint64_t value, uint64_t size) {
            char buffer[21];

            for (uint64_t i = 0; i < size; ++i) {
                buffer[i] = Ctx::map10(value % 10);
                value /= 10;
            }

            if (likely(lastBuilderQueue->size + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                uint8_t *ptr = lastBuilderQueue->data + lastBuilderQueue->size + messagePosition;
                for (uint64_t i = 0; i < size; ++i)
                    *ptr++ = buffer[size - i - 1];
                messagePosition += size;
            } else {
                for (uint64_t i = 0; i < size; ++i)
                    append(buffer[size - i - 1]);
            }
        }

        inline void appendDec(uint64_t value) {
            char buffer[21];
            uint64_t size = 0;

            if (value == 0) {
                buffer[0] = '0';
                size = 1;
            } else {
                while (value > 0) {
                    buffer[size++] = Ctx::map10(value % 10);
                    value /= 10;
                }
            }

            if (likely(lastBuilderQueue->size + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                uint8_t* ptr = lastBuilderQueue->data + lastBuilderQueue->size + messagePosition;
                for (uint64_t i = 0; i < size; ++i)
                    *ptr++ = buffer[size - i - 1];
                messagePosition += size;
            } else {
                for (uint64_t i = 0; i < size; ++i)
                    append(buffer[size - i - 1]);
            }
        }

        inline void appendSDec(int64_t value) {
            char buffer[22];
            uint64_t size = 0;

            if (value == 0) {
                buffer[0] = '0';
                size = 1;
            } else {
                if (value < 0) {
                    value = -value;
                    while (value > 0) {
                        buffer[size++] = Ctx::map10(value % 10);
                        value /= 10;
                    }
                    buffer[size++] = '-';
                } else {
                    while (value > 0) {
                        buffer[size++] = Ctx::map10(value % 10);
                        value /= 10;
                    }
                }
            }

            for (uint64_t i = 0; i < size; ++i)
                append(buffer[size - i - 1]);
        }

        inline void appendEscape(const std::string& str) {
            appendEscape(str.c_str(), str.length());
        }

        inline void appendEscape(const char* str, uint64_t size) {
            while (size > 0) {
                if (*str == '\t') {
                    append("\\t", sizeof("\\t") - 1);
                } else if (*str == '\r') {
                    append("\\r", sizeof("\\r") - 1);
                } else if (*str == '\n') {
                    append("\\n", sizeof("\\n") - 1);
                } else if (*str == '\f') {
                    append("\\f", sizeof("\\f") - 1);
                } else if (*str == '\b') {
                    append("\\b", sizeof("\\b") - 1);
                } else if (static_cast<unsigned char>(*str) < 32) {
                    append("\\u00", sizeof("\\u00") - 1);
                    appendDec(*str, 2);
                } else {
                    if (*str == '"' || *str == '\\' || *str == '/')
                        append('\\');
                    append(*str);
                }
                ++str;
                --size;
            }
        }

        inline void appendAfter(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, uint64_t offset) {
            append(R"(,"after":{)", sizeof(R"(,"after":{)") - 1);

            hasPreviousColumn = false;
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (sizes[column][VALUE_AFTER] > 0)
                            processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_AFTER], sizes[column][VALUE_AFTER], offset, true,
                                         compressedAfter);
                        else
                            columnNull(table, column, true);
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_AFTER] != nullptr) {
                            if (sizes[column][VALUE_AFTER] > 0)
                                processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_AFTER], sizes[column][VALUE_AFTER], offset,
                                             true, compressedAfter);
                            else
                                columnNull(table, column, true);
                        }
                    }
                }
            }
            append('}');
        }

        inline void appendBefore(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, uint64_t offset) {
            append(R"(,"before":{)", sizeof(R"(,"before":{)") - 1);

            hasPreviousColumn = false;
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (sizes[column][VALUE_BEFORE] > 0)
                            processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_BEFORE], sizes[column][VALUE_BEFORE], offset,
                                         false, compressedBefore);
                        else
                            columnNull(table, column, false);
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_BEFORE] != nullptr) {
                            if (sizes[column][VALUE_BEFORE] > 0)
                                processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_BEFORE], sizes[column][VALUE_BEFORE], offset,
                                             false, compressedBefore);
                            else
                                columnNull(table, column, false);
                        }
                    }
                }
            }
            append('}');
        }


        virtual void columnFloat(const std::string& columnName, double value) override;
        virtual void columnDouble(const std::string& columnName, long double value) override;
        virtual void columnString(const std::string& columnName) override;
        virtual void columnNumber(const std::string& columnName, uint64_t precision, uint64_t scale) override;
        virtual void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t size) override;
        virtual void columnRowId(const std::string& columnName, typeRowId rowId) override;
        virtual void columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) override;
        virtual void columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const char* tz) override;
        virtual void processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) override;
        virtual void processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) override;
        virtual void processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) override;
        virtual void processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const OracleTable* table, typeObj obj, typeDataObj dataObj, uint16_t type,
                                uint16_t seq, const char* sql, uint64_t sqlSize) override;
        virtual void processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) override;

    public:
        BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat, uint64_t newAttributesFormat, uint64_t newIntervalDtsFormat,
                    uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat, uint64_t newTimestampFormat,
                    uint64_t newTimestampTzFormat, uint64_t newTimestampAll, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newScnAll,
                    uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer);

        virtual void processCommit(typeScn scn, typeSeq sequence, time_t timestamp) override;
        virtual void processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp, uint64_t offset, bool redo) override;
    };
}

#endif
