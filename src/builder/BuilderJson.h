/* Header for BuilderJson class
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

#ifndef BUILDER_JSON_H_
#define BUILDER_JSON_H_

#include "Builder.h"
#include "../common/DbColumn.h"
#include "../common/DbTable.h"
#include "../common/table/SysCol.h"
#include "../common/types/Data.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"

namespace OpenLogReplicator {
    class BuilderJson final : public Builder {
    protected:
        bool hasPreviousValue{false};
        bool hasPreviousRedo{false};
        bool hasPreviousColumn{false};

        inline void comma(bool &prev) {
            if (prev)
                append(',');
            else
                prev = true;
        }

        void columnNull(const DbTable* table, typeCol col, bool after) {
            if (unlikely(table != nullptr && format.unknownType == Format::UNKNOWN_TYPE::HIDE)) {
                const DbColumn* column = table->columns[col];
                if (column->guard && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_GUARD_COLUMNS))
                    return;
                if (column->nested && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_NESTED_COLUMNS))
                    return;
                if (column->hidden && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_HIDDEN_COLUMNS))
                    return;
                if (column->unused && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_UNUSED_COLUMNS))
                    return;

                const SysCol::COLTYPE typeNo = table->columns[col]->type;
                if (typeNo != SysCol::COLTYPE::VARCHAR
                    && typeNo != SysCol::COLTYPE::NUMBER
                    && typeNo != SysCol::COLTYPE::DATE
                    && typeNo != SysCol::COLTYPE::RAW
                    && typeNo != SysCol::COLTYPE::CHAR
                    && typeNo != SysCol::COLTYPE::FLOAT
                    && typeNo != SysCol::COLTYPE::DOUBLE
                    && (typeNo != SysCol::COLTYPE::XMLTYPE || !after)
                    && (typeNo != SysCol::COLTYPE::JSON || !after)
                    && (typeNo != SysCol::COLTYPE::CLOB || !after)
                    && (typeNo != SysCol::COLTYPE::BLOB || !after)
                    && typeNo != SysCol::COLTYPE::TIMESTAMP
                    && typeNo != SysCol::COLTYPE::INTERVAL_YEAR_TO_MONTH
                    && typeNo != SysCol::COLTYPE::INTERVAL_DAY_TO_SECOND
                    && typeNo != SysCol::COLTYPE::UROWID
                    && typeNo != SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ)
                    return;
            }

            comma(hasPreviousColumn);
            append('"');
            if (likely(table != nullptr))
                appendEscape(table->columns[col]->name);
            else {
                const std::string columnName("COL_" + std::to_string(col));
                append(columnName);
            }
            append(std::string_view(R"(":null)"));
        }

        void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
            if (format.isMessageFormatAddSequences()) {
                append(std::string_view(R"(,"num":)"));
                appendDec(num);
            }

            if (format.ridFormat == Format::RID_FORMAT::TEXT) {
                const RowId rowId(dataObj, bdba, slot);
                char str[RowId::SIZE + 1];
                rowId.toString(str);
                append(std::string_view(R"(,"rid":")"));
                appendArr(str, 18);
                append('"');
            }
        }

        void appendTimestamp(const std::string_view fieldn, const std::string_view fields, Time timestamp) {
            time_t tm = timestamp.toEpoch(metadata->ctx->hostTimezone);
            comma(hasPreviousValue);
            char buffer[22];
            append('"');
            switch (format.timestampMetadataFormat) {
                case Format::TIMESTAMP_FORMAT::UNIX_NANO:
                    append(fieldn);
                    append(std::string_view(R"(":)"));
                    appendDec(tm);
                    if (tm != 0)
                        append(std::string_view("000000000"));
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX_MICRO:
                    append(fieldn);
                    append(std::string_view(R"(":)"));
                    appendDec(tm);
                    if (tm != 0)
                        append(std::string_view("000000"));
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX_MILLI:
                    append(fieldn);
                    append(std::string_view(R"(":)"));
                    appendDec(tm);
                    if (tm != 0)
                        append(std::string_view("000"));
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX:
                    append(fieldn);
                    append(std::string_view(R"(":)"));
                    appendDec(tm);
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX_NANO_STRING:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendDec(tm);
                    if (tm != 0)
                        append(std::string_view("000000000"));
                    append('"');
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX_MICRO_STRING:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendDec(tm);
                    if (tm != 0)
                        append(std::string_view("000000"));
                    append('"');
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX_MILLI_STRING:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendDec(tm);
                    if (tm != 0)
                        append(std::string_view("000"));
                    append('"');
                    break;

                case Format::TIMESTAMP_FORMAT::UNIX_STRING:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendDec(tm);
                    append('"');
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_NANO_TZ:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, true, false));
                    append(std::string_view(R"(.000000000Z")"));
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_MICRO_TZ:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, true, true));
                    append(std::string_view(R"(.000000Z")"));
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_MILLI_TZ:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, true, false));
                    append(std::string_view(R"(.000Z")"));
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_TZ:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, true, true));
                    append('"');
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_NANO:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, false, false));
                    append(std::string_view(R"(.000000000")"));
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_MICRO:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, false, false));
                    append(std::string_view(R"(.000000")"));
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601_MILLI:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, false, false));
                    append(std::string_view(R"(.000")"));
                    break;

                case Format::TIMESTAMP_FORMAT::ISO8601:
                    append(fields);
                    append(std::string_view(R"(":")"));
                    appendArr(buffer, Data::epochToIso8601(tm, buffer, false, false));
                    append('"');
                    break;
            }
        }

        void appendHeader(Scn scn, Time timestamp, bool first, bool showDb, bool showXid, bool showUser) {
            __builtin_prefetch(&lastBuilderQueue->data[lastBuilderSize + messagePosition], 1, 0);
            __builtin_prefetch(&lastBuilderQueue->data[lastBuilderSize + messagePosition] + 64, 1, 0);
            __builtin_prefetch(&lastBuilderQueue->data[lastBuilderSize + messagePosition] + 128, 1, 0);
            __builtin_prefetch(&lastBuilderQueue->data[lastBuilderSize + messagePosition] + 192, 1, 0);
            if (first || format.isScnTypeDml()) {
                comma(hasPreviousValue);
                if (format.scnFormat == Format::SCN_FORMAT::TEXT_HEX) {
                    append(std::string_view(R"("scns":"0x)"));
                    if (format.isScnTypeCommitValue())
                        appendHex16(commitScn.getData());
                    else
                        appendHex16(scn.getData());
                    append('"');
                } else {
                    append(std::string_view(R"("scn":)"));
                    if (format.isScnTypeCommitValue())
                        appendDec(commitScn.getData());
                    else
                        appendDec(scn.getData());
                }
            }

            if (format.isScnTypeBegin()) {
                comma(hasPreviousValue);
                if (format.scnFormat == Format::SCN_FORMAT::TEXT_HEX) {
                    append(std::string_view(R"("b_scns":"0x)"));
                    appendHex16(beginScn.getData());
                    append('"');
                } else {
                    append(std::string_view(R"("b_scn":)"));
                    appendDec(beginScn.getData());
                }
            }

            if (format.isScnTypeCommit()) {
                comma(hasPreviousValue);
                if (format.scnFormat == Format::SCN_FORMAT::TEXT_HEX) {
                    append(std::string_view(R"("e_scns":"0x)"));
                    appendHex16(commitScn.getData());
                    append('"');
                } else {
                    append(std::string_view(R"("e_scn":)"));
                    appendDec(commitScn.getData());
                }
            }

            if (first || format.isTimestampTypeDml()) {
                if (format.isTimestampTypeCommitValue())
                    appendTimestamp("tm", "tms", commitTimestamp);
                else
                    appendTimestamp("tm", "tms", timestamp);
            }
            if (format.isTimestampTypeBegin())
                appendTimestamp("b_tm", "b_tms", beginTimestamp);
            if (format.isTimestampTypeCommit())
                appendTimestamp("e_tm", "e_tms", commitTimestamp);

            comma(hasPreviousValue);
            append(std::string_view(R"("c_scn":)"));
            appendDec(lwnScn.getData());
            append(std::string_view(R"(,"c_idx":)"));
            appendDec(lwnIdx);

            if (showXid) {
                comma(hasPreviousValue);
                switch (format.xidFormat) {
                    case Format::XID_FORMAT::TEXT_HEX:
                        append(std::string_view(R"("xid":"0x)"));
                        appendHex4(lastXid.usn());
                        append('.');
                        appendHex3(lastXid.slt());
                        append('.');
                        appendHex8(lastXid.sqn());
                        append('"');
                        break;
                    case Format::XID_FORMAT::TEXT_DEC:
                        append(std::string_view(R"("xid":")"));
                        appendDec(lastXid.usn());
                        append('.');
                        appendDec(lastXid.slt());
                        append('.');
                        appendDec(lastXid.sqn());
                        append('"');
                        break;
                    case Format::XID_FORMAT::NUMERIC:
                        append(std::string_view(R"("xidn":)"));
                        appendDec(lastXid.getData());
                        break;
                    case Format::XID_FORMAT::TEXT_REVERSED:
                        append(std::string_view(R"("xid":")"));
                        appendHex16Reversed(lastXid.getData());
                        append('"');
                        break;
                }
            }

            if (showDb) {
                comma(hasPreviousValue);
                append(std::string_view(R"("db":")"));
                append(metadata->conName);
                append('"');
            }

            if (showUser) {
                const auto itUserName = attributes->find(Attribute::KEY::LOGIN_USER_NAME);
                if (itUserName != attributes->end()) {
                    comma(hasPreviousValue);
                    append(std::string_view(R"("usr":")"));
                    append(itUserName->second);
                    append('"');
                }
            }

            if (format.redoThreadFormat == Format::REDO_THREAD_FORMAT::TEXT) {
                comma(hasPreviousValue);
                append(std::string_view(R"("rth":)"));
                appendDec(thread);
            }
        }

        void appendAttributes() {
            bool hasPreviousAttribute = false;
            append(std::string_view(R"("attrs":{)"));
            for (const auto& [key, value]: *attributes) {
                comma(hasPreviousAttribute);
                append('"');
                appendEscape(Attribute::toString(key));
                append(std::string_view(R"(":")"));
                appendEscape(value);
                append('"');
            }
            append(std::string_view("},"));
        }

        void appendSchema(const DbTable* table, typeObj obj) {
            if (unlikely(table == nullptr)) {
                std::string ownerName;
                std::string tableName;
                // try to read object name from ongoing uncommitted transaction data
                if (metadata->schema->checkTableDictUncommitted(obj, ownerName, tableName)) {
                    append(std::string_view(R"("schema":{"owner":")"));
                    appendEscape(ownerName);
                    append(std::string_view(R"(","table":")"));
                    appendEscape(tableName);
                    append('"');
                } else if (ddlSchemaSize > 0) {
                    append(std::string_view(R"("schema":{"owner":")"));
                    appendEscape(ddlSchemaName, ddlSchemaSize);
                    append(std::string_view(R"(","table":")"));
                    tableName = "OBJ_" + std::to_string(obj);
                    append(tableName);
                    append('"');
                } else {
                    append(std::string_view(R"("schema":{"table":")"));
                    tableName = "OBJ_" + std::to_string(obj);
                    append(tableName);
                    append('"');
                }

                if (format.isSchemaFormatObj()) {
                    append(std::string_view(R"(,"obj":)"));
                    appendDec(obj);
                }
                append('}');
                return;
            }

            append(std::string_view(R"("schema":{"owner":")"));
            appendEscape(table->owner);
            append(std::string_view(R"(","table":")"));
            appendEscape(table->name);
            append('"');

            if (format.isSchemaFormatObj()) {
                append(std::string_view(R"(,"obj":)"));
                appendDec(obj);
            }

            if (format.isSchemaFormatFull()) {
                if (!format.isSchemaFormatRepeated()) {
                    if (tables.count(table) > 0) {
                        append('}');
                        return;
                    }
                    tables.insert(table);
                }

                append(std::string_view(R"(,"columns":[)"));

                bool hasPrev = false;
                for (typeCol column = 0; column < static_cast<typeCol>(table->columns.size()); ++column) {
                    if (table->columns[column] == nullptr)
                        continue;

                    comma(hasPrev);
                    append(std::string_view(R"({"name":")"));
                    appendEscape(table->columns[column]->name);

                    append(std::string_view(R"(","type":)"));
                    switch (table->columns[column]->type) {
                        case SysCol::COLTYPE::VARCHAR:
                            append(std::string_view(R"("varchar2","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::NUMBER:
                            append(std::string_view(R"("number","precision":)"));
                            appendSDec(table->columns[column]->precision);
                            append(std::string_view(R"(,"scale":)"));
                            appendSDec(table->columns[column]->scale);
                            break;

                        case SysCol::COLTYPE::LONG:
                            // Long, not supported
                            append(std::string_view(R"("long")"));
                            break;

                        case SysCol::COLTYPE::DATE:
                            append(std::string_view(R"("date")"));
                            break;

                        case SysCol::COLTYPE::RAW:
                            append(std::string_view(R"("raw","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::LONG_RAW: // Not supported
                            append(std::string_view(R"("long raw")"));
                            break;

                        case SysCol::COLTYPE::CHAR:
                            append(std::string_view(R"("char","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::FLOAT:
                            append(std::string_view(R"("binary_float")"));
                            break;

                        case SysCol::COLTYPE::DOUBLE:
                            append(std::string_view(R"("binary_double")"));
                            break;

                        case SysCol::COLTYPE::CLOB:
                            append(std::string_view(R"("clob")"));
                            break;

                        case SysCol::COLTYPE::BLOB:
                            append(std::string_view(R"("blob")"));
                            break;

                        case SysCol::COLTYPE::TIMESTAMP:
                            append(std::string_view(R"("timestamp","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::TIMESTAMP_WITH_TZ:
                            append(std::string_view(R"("timestamp with time zone","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::INTERVAL_YEAR_TO_MONTH:
                            append(std::string_view(R"("interval year to month","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::INTERVAL_DAY_TO_SECOND:
                            append(std::string_view(R"("interval day to second","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::UROWID:
                            append(std::string_view(R"("urowid","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        case SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ:
                            append(std::string_view(R"("timestamp with local time zone","length":)"));
                            appendDec(table->columns[column]->length);
                            break;

                        default:
                            append(std::string_view(R"("unknown")"));
                            break;
                    }

                    append(std::string_view(R"(,"nullable":)"));
                    if (table->columns[column]->nullable)
                        append(std::string_view("true"));
                    else
                        append(std::string_view("false"));

                    append('}');
                }
                append(']');
            }

            append('}');
        }

        template<bool fast = false>
        void appendHex2(uint8_t value) {
            if (likely(fast || lastBuilderSize + messagePosition + 2 < OUTPUT_BUFFER_DATA_SIZE)) {
                append<true>(Data::map16((value >> 4) & 0xF));
                append<true>(Data::map16(value & 0xF));
            } else {
                append(Data::map16((value >> 4) & 0xF));
                append(Data::map16(value & 0xF));
            }
        }

        void appendHex3(uint16_t value) {
            if (likely(lastBuilderSize + messagePosition + 3 < OUTPUT_BUFFER_DATA_SIZE)) {
                append<true>(Data::map16((value >> 8) & 0xF));
                append<true>(Data::map16((value >> 4) & 0xF));
                append<true>(Data::map16(value & 0xF));
            } else {
                append(Data::map16((value >> 8) & 0xF));
                append(Data::map16((value >> 4) & 0xF));
                append(Data::map16(value & 0xF));
            }
        }

        void appendHex4(uint16_t value) {
            if (likely(lastBuilderSize + messagePosition + 4 < OUTPUT_BUFFER_DATA_SIZE)) {
                append<true>(Data::map16((value >> 12) & 0xF));
                append<true>(Data::map16((value >> 8) & 0xF));
                append<true>(Data::map16((value >> 4) & 0xF));
                append<true>(Data::map16(value & 0xF));
            } else {
                append(Data::map16((value >> 12) & 0xF));
                append(Data::map16((value >> 8) & 0xF));
                append(Data::map16((value >> 4) & 0xF));
                append(Data::map16(value & 0xF));
            }
        }

        void appendHex8(uint32_t value) {
            if (likely(lastBuilderSize + messagePosition + 8 < OUTPUT_BUFFER_DATA_SIZE)) {
                append<true>(Data::map16((value >> 28) & 0xF));
                append<true>(Data::map16((value >> 24) & 0xF));
                append<true>(Data::map16((value >> 20) & 0xF));
                append<true>(Data::map16((value >> 16) & 0xF));
                append<true>(Data::map16((value >> 12) & 0xF));
                append<true>(Data::map16((value >> 8) & 0xF));
                append<true>(Data::map16((value >> 4) & 0xF));
                append<true>(Data::map16(value & 0xF));
            } else {
                append(Data::map16((value >> 28) & 0xF));
                append(Data::map16((value >> 24) & 0xF));
                append(Data::map16((value >> 20) & 0xF));
                append(Data::map16((value >> 16) & 0xF));
                append(Data::map16((value >> 12) & 0xF));
                append(Data::map16((value >> 8) & 0xF));
                append(Data::map16((value >> 4) & 0xF));
                append(Data::map16(value & 0xF));
            }
        }

        void appendHex16(uint64_t value) {
            if (likely(lastBuilderSize + messagePosition + 16 < OUTPUT_BUFFER_DATA_SIZE)) {
                append<true>(Data::map16((value >> 60) & 0xF));
                append<true>(Data::map16((value >> 56) & 0xF));
                append<true>(Data::map16((value >> 52) & 0xF));
                append<true>(Data::map16((value >> 48) & 0xF));
                append<true>(Data::map16((value >> 44) & 0xF));
                append<true>(Data::map16((value >> 40) & 0xF));
                append<true>(Data::map16((value >> 36) & 0xF));
                append<true>(Data::map16((value >> 32) & 0xF));
                append<true>(Data::map16((value >> 28) & 0xF));
                append<true>(Data::map16((value >> 24) & 0xF));
                append<true>(Data::map16((value >> 20) & 0xF));
                append<true>(Data::map16((value >> 16) & 0xF));
                append<true>(Data::map16((value >> 12) & 0xF));
                append<true>(Data::map16((value >> 8) & 0xF));
                append<true>(Data::map16((value >> 4) & 0xF));
                append<true>(Data::map16(value & 0xF));
            } else {
                append(Data::map16((value >> 60) & 0xF));
                append(Data::map16((value >> 56) & 0xF));
                append(Data::map16((value >> 52) & 0xF));
                append(Data::map16((value >> 48) & 0xF));
                append(Data::map16((value >> 44) & 0xF));
                append(Data::map16((value >> 40) & 0xF));
                append(Data::map16((value >> 36) & 0xF));
                append(Data::map16((value >> 32) & 0xF));
                append(Data::map16((value >> 28) & 0xF));
                append(Data::map16((value >> 24) & 0xF));
                append(Data::map16((value >> 20) & 0xF));
                append(Data::map16((value >> 16) & 0xF));
                append(Data::map16((value >> 12) & 0xF));
                append(Data::map16((value >> 8) & 0xF));
                append(Data::map16((value >> 4) & 0xF));
                append(Data::map16(value & 0xF));
            }
        }

        void appendHex16Reversed(uint64_t value) {
            if (likely(lastBuilderSize + messagePosition + 16 < OUTPUT_BUFFER_DATA_SIZE)) {
                append<true>(Data::map16((value >> 52) & 0xF));
                append<true>(Data::map16((value >> 48) & 0xF));
                append<true>(Data::map16((value >> 60) & 0xF));
                append<true>(Data::map16((value >> 56) & 0xF));
                append<true>(Data::map16((value >> 36) & 0xF));
                append<true>(Data::map16((value >> 32) & 0xF));
                append<true>(Data::map16((value >> 44) & 0xF));
                append<true>(Data::map16((value >> 40) & 0xF));
                append<true>(Data::map16((value >> 4) & 0xF));
                append<true>(Data::map16(value & 0xF));
                append<true>(Data::map16((value >> 12) & 0xF));
                append<true>(Data::map16((value >> 8) & 0xF));
                append<true>(Data::map16((value >> 20) & 0xF));
                append<true>(Data::map16((value >> 16) & 0xF));
                append<true>(Data::map16((value >> 28) & 0xF));
                append<true>(Data::map16((value >> 24) & 0xF));
            } else {
                append(Data::map16((value >> 52) & 0xF));
                append(Data::map16((value >> 48) & 0xF));
                append(Data::map16((value >> 60) & 0xF));
                append(Data::map16((value >> 56) & 0xF));
                append(Data::map16((value >> 36) & 0xF));
                append(Data::map16((value >> 32) & 0xF));
                append(Data::map16((value >> 44) & 0xF));
                append(Data::map16((value >> 40) & 0xF));
                append(Data::map16((value >> 4) & 0xF));
                append(Data::map16(value & 0xF));
                append(Data::map16((value >> 12) & 0xF));
                append(Data::map16((value >> 8) & 0xF));
                append(Data::map16((value >> 20) & 0xF));
                append(Data::map16((value >> 16) & 0xF));
                append(Data::map16((value >> 28) & 0xF));
                append(Data::map16((value >> 24) & 0xF));
            }
        }

        template<uint size, bool fast = false>
        void appendDecN(uint64_t value) {
            char buffer[21];

            for (uint i = 0; i < size; ++i) {
                buffer[i] = Data::map10(value % 10);
                value /= 10;
            }

            if (likely(fast || lastBuilderSize + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                uint8_t* ptr = lastBuilderQueue->data + lastBuilderSize + messagePosition;
                for (uint i = 0; i < size; ++i)
                    *ptr++ = buffer[size - i - 1];
                messagePosition += size;
                ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            } else {
                for (uint i = 0; i < size; ++i)
                    append(buffer[size - i - 1]);
            }
        }

        template<bool fast = false>
        void appendDec(uint64_t value) {
            char buffer[21];
            uint size = 0;

            if (value == 0) {
                buffer[0] = '0';
                size = 1;
            } else {
                while (value > 0) {
                    buffer[size++] = Data::map10(value % 10);
                    value /= 10;
                }
            }

            if (likely(fast || lastBuilderSize + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                uint8_t* ptr = lastBuilderQueue->data + lastBuilderSize + messagePosition;
                for (uint i = 0; i < size; ++i)
                    *ptr++ = buffer[size - i - 1];
                messagePosition += size;
                ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            } else {
                for (uint i = 0; i < size; ++i)
                    append(buffer[size - i - 1]);
            }
        }

        void appendSDec(int64_t value) {
            char buffer[22];
            uint size = 0;

            if (value == 0) {
                buffer[0] = '0';
                size = 1;
            } else {
                if (value < 0) {
                    value = -value;
                    while (value > 0) {
                        buffer[size++] = Data::map10(value % 10);
                        value /= 10;
                    }
                    buffer[size++] = '-';
                } else {
                    while (value > 0) {
                        buffer[size++] = Data::map10(value % 10);
                        value /= 10;
                    }
                }
            }

            if (likely(lastBuilderSize + messagePosition + size < OUTPUT_BUFFER_DATA_SIZE)) {
                uint8_t* ptr = lastBuilderQueue->data + lastBuilderSize + messagePosition;
                for (uint i = 0; i < size; ++i)
                    *ptr++ = buffer[size - i - 1];
                messagePosition += size;
                ctx->assertDebug(lastBuilderSize + messagePosition < OUTPUT_BUFFER_DATA_SIZE);
            } else {
                for (uint i = 0; i < size; ++i)
                    append(buffer[size - i - 1]);
            }
        }

        template<bool fast = false>
        void appendEscape(const std::string& str) {
            appendEscape<fast>(str.c_str(), str.length());
        }

        template<bool fast = false>
        void appendEscape(const std::string_view& str) {
            appendEscape<fast>(str.data(), str.length());
        }

        template<bool fast = false>
        void appendEscape(const char* str, uint64_t size) {
            if (fast || likely(lastBuilderSize + messagePosition + size * 5 < OUTPUT_BUFFER_DATA_SIZE)) {
                appendEscapeInternal<true>(str, size);
            } else {
                appendEscapeInternal<false>(str, size);
            }
        }

        template<bool fast = false>
        void appendEscapeInternal(const char* str, uint64_t size) {
            while (size > 0) {
                switch (*str) {
                    case '\t':
                        append<fast>(std::string_view("\\t"));
                        break;
                    case '\r':
                        append<fast>(std::string_view("\\r"));
                        break;
                    case '\n':
                        append<fast>(std::string_view("\\n"));
                        break;
                    case '\f':
                        append<fast>(std::string_view("\\f"));
                        break;
                    case '\b':
                        append<fast>(std::string_view("\\b"));
                        break;
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                    //case 8:  // \b
                    //case 9:  // \t
                    //case 10: // \n
                    case 11:
                    //case 12: // \f
                    //case 13: // \r
                    case 14:
                    case 15:
                    case 16:
                    case 17:
                    case 18:
                    case 19:
                    case 20:
                    case 21:
                    case 22:
                    case 23:
                    case 24:
                    case 25:
                    case 26:
                    case 27:
                    case 28:
                    case 29:
                    case 30:
                        append<fast>(std::string_view("\\u00"));
                        appendDecN<2, fast>(*str);
                        break;
                    case '"':
                    case '\\':
                    case '/':
                        append<fast>('\\');
                        append<fast>(*str);
                        break;
                    default:
                        append<fast>(*str);
                }
                ++str;
                --size;
            }
        }

        void appendAfter(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, FileOffset fileOffset) {
            append(std::string_view(R"(,"after":{)"));

            hasPreviousColumn = false;
            if (format.columnFormat > Format::COLUMN_FORMAT::CHANGED && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][+Format::VALUE_TYPE::AFTER] == nullptr)
                        continue;

                    if (sizes[column][+Format::VALUE_TYPE::AFTER] > 0)
                        processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::AFTER],
                                     sizes[column][+Format::VALUE_TYPE::AFTER], fileOffset, true, compressedAfter);
                    else
                        columnNull(table, column, true);
                }
            } else {
                const typeCol baseMax = valuesMax >> 6;
                for (typeCol base = 0; base <= baseMax; ++base) {
                    const auto columnBase = static_cast<typeCol>(base << 6);
                    typeMask set = valuesSet[base];
                    while (set != 0) {
                        const typeCol pos = ffsll(set) - 1;
                        set &= ~(1ULL << pos);
                        const typeCol column = columnBase + pos;

                        if (values[column][+Format::VALUE_TYPE::AFTER] != nullptr) {
                            if (sizes[column][+Format::VALUE_TYPE::AFTER] > 0)
                                processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::AFTER],
                                             sizes[column][+Format::VALUE_TYPE::AFTER], fileOffset, true, compressedAfter);
                            else
                                columnNull(table, column, true);
                        }
                    }
                }
            }
            append('}');
        }

        void appendBefore(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, FileOffset fileOffset) {
            append(std::string_view(R"(,"before":{)"));

            hasPreviousColumn = false;
            if (format.columnFormat > Format::COLUMN_FORMAT::CHANGED && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][+Format::VALUE_TYPE::BEFORE] == nullptr)
                        continue;

                    if (sizes[column][+Format::VALUE_TYPE::BEFORE] > 0)
                        processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::BEFORE],
                                     sizes[column][+Format::VALUE_TYPE::BEFORE], fileOffset, false, compressedBefore);
                    else
                        columnNull(table, column, false);
                }
            } else {
                const typeCol baseMax = valuesMax >> 6;
                for (typeCol base = 0; base <= baseMax; ++base) {
                    const auto columnBase = static_cast<typeCol>(base << 6);
                    typeMask set = valuesSet[base];
                    while (set != 0) {
                        const typeCol pos = ffsll(set) - 1;
                        set &= ~(1ULL << pos);
                        const typeCol column = columnBase + pos;

                        if (values[column][+Format::VALUE_TYPE::BEFORE] != nullptr) {
                            if (sizes[column][+Format::VALUE_TYPE::BEFORE] > 0)
                                processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::BEFORE],
                                             sizes[column][+Format::VALUE_TYPE::BEFORE], fileOffset, false, compressedBefore);
                            else
                                columnNull(table, column, false);
                        }
                    }
                }
            }
            append('}');
        }


        void columnFloat(const std::string& columnName, double value) override;
        void columnDouble(const std::string& columnName, long double value) override;
        void columnString(const std::string& columnName) override;
        void columnNumber(const std::string& columnName, int precision, int scale) override;
        void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t size) override;
        void columnRowId(const std::string& columnName, RowId rowId) override;
        void columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) override;
        void columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const std::string_view& tz) override;
        void processInsert(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                           typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) override;
        void processUpdate(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                           typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) override;
        void processDelete(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, typeObj obj,
                           typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) override;
        void processDdl(Seq sequence, Scn scn, Time timestamp, const DbTable* table, typeObj obj) override;
        void processBeginMessage(Seq sequence, Time timestamp) override;
        void addTagData(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, Format::VALUE_TYPE valueType, FileOffset fileOffset);

    public:
        BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, Format& newFormat, uint64_t newFlushBuffer);

        void processCommit() override;
        void processCheckpoint(Seq sequence, Scn scn, Time timestamp, FileOffset fileOffset, bool redo) override;
    };
}

#endif
