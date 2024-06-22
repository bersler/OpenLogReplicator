/* Header for BuilderProtobuf class
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

#include "../common/OracleTable.h"
#include "../common/OraProtoBuf.pb.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "Builder.h"

#ifndef BUILDER_PROTOBUF_H_
#define BUILDER_PROTOBUF_H_

namespace OpenLogReplicator {
    class BuilderProtobuf final : public Builder {
    protected:
        pb::RedoResponse* redoResponsePB;
        pb::Value* valuePB;
        pb::Payload* payloadPB;
        pb::Schema* schemaPB;

        inline void columnNull(const OracleTable* table, typeCol col, bool after) {
            if (table != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
                OracleColumn* column = table->columns[col];
                if (column->storedAsLob)
                    return;
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

            if (table == nullptr || ctx->flagsSet(Ctx::REDO_FLAGS_RAW_COLUMN_DATA)) {
                std::string columnName("COL_" + std::to_string(col));
                valuePB->set_name(columnName);
                return;
            }

            valuePB->set_name(table->columns[col]->name);
        }

        inline void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
            if ((messageFormat & MESSAGE_FORMAT_ADD_SEQUENCES) != 0)
                payloadPB->set_num(num);

            if (ridFormat == RID_FORMAT_SKIP)
                return;
            else if (ridFormat == RID_FORMAT_TEXT) {
                typeRowId rowId(dataObj, bdba, slot);
                char str[19];
                rowId.toString(str);
                payloadPB->set_rid(str, 18);
            }
        }

        inline void appendHeader(typeScn scn, time_t timestamp, bool first, bool showDb, bool showXid) {
            redoResponsePB->set_code(pb::ResponseCode::PAYLOAD);
            if (first || (scnAll & SCN_ALL_PAYLOADS) != 0) {
                if ((scnFormat & SCN_FORMAT_TEXT_HEX) != 0) {
                    char buf[17];
                    numToString(scn, buf, 16);
                    redoResponsePB->set_scns(buf);
                } else {
                    redoResponsePB->set_scn(scn);
                }
            }

            if (first || (timestampAll & TIMESTAMP_ALL_PAYLOADS) != 0) {
                std::string str;
                switch (timestampFormat) {
                    case TIMESTAMP_FORMAT_UNIX_NANO:
                        redoResponsePB->set_tm(timestamp * 1000000000L);
                        break;

                    case TIMESTAMP_FORMAT_UNIX_MICRO:
                        redoResponsePB->set_tm(timestamp * 1000000L);
                        break;

                    case TIMESTAMP_FORMAT_UNIX_MILLI:
                        redoResponsePB->set_tm(timestamp * 1000L);
                        break;

                    case TIMESTAMP_FORMAT_UNIX:
                        redoResponsePB->set_tm(timestamp);
                        break;

                    case TIMESTAMP_FORMAT_UNIX_NANO_STRING:
                        str = std::to_string(timestamp * 1000000000L);
                        redoResponsePB->set_tms(str);
                        break;

                    case TIMESTAMP_FORMAT_UNIX_MICRO_STRING:
                        str = std::to_string(timestamp * 1000000L);
                        redoResponsePB->set_tms(str);
                        break;

                    case TIMESTAMP_FORMAT_UNIX_MILLI_STRING:
                        str = std::to_string(timestamp * 1000L);
                        redoResponsePB->set_tms(str);
                        break;

                    case TIMESTAMP_FORMAT_UNIX_STRING:
                        str = std::to_string(timestamp);
                        redoResponsePB->set_tms(str);
                        break;

                    case TIMESTAMP_FORMAT_ISO8601:
                        char buffer[22];
                        str.assign(buffer, ctx->epochToIso8601(timestamp, buffer, true, true));
                        redoResponsePB->set_tms(str);
                        break;
                }
            }

            redoResponsePB->set_c_scn(lwnScn);
            redoResponsePB->set_c_idx(lwnIdx);

            if (showXid) {
                if (xidFormat == XID_FORMAT_TEXT_HEX) {
                    std::ostringstream ss;
                    ss << "0x";
                    ss << std::setfill('0') << std::setw(4) << std::hex << static_cast<uint64_t>(lastXid.usn());
                    ss << '.';
                    ss << std::setfill('0') << std::setw(3) << std::hex << static_cast<uint64_t>(lastXid.slt());
                    ss << '.';
                    ss << std::setfill('0') << std::setw(8) << std::hex << static_cast<uint64_t>(lastXid.sqn());
                    redoResponsePB->set_xid(ss.str());
                } else if (xidFormat == XID_FORMAT_TEXT_DEC) {
                    std::ostringstream ss;
                    ss << static_cast<uint64_t>(lastXid.usn());
                    ss << '.';
                    ss << static_cast<uint64_t>(lastXid.slt());
                    ss << '.';
                    ss << static_cast<uint64_t>(lastXid.sqn());
                    redoResponsePB->set_xid(ss.str());
                } else if (xidFormat == XID_FORMAT_NUMERIC) {
                    redoResponsePB->set_xidn(lastXid.getData());
                }
            }

            if (showDb)
                redoResponsePB->set_db(metadata->conName);
        }

        inline void appendSchema(const OracleTable* table, typeObj obj) {
            if (table == nullptr) {
                std::string ownerName;
                std::string tableName;
                // try to read object name from ongoing uncommitted transaction data
                if (metadata->schema->checkTableDictUncommitted(obj, ownerName, tableName)) {
                    schemaPB->set_owner(ownerName);
                    schemaPB->set_name(tableName);
                } else {
                    tableName = "OBJ_" + std::to_string(obj);
                    schemaPB->set_name(tableName);
                }

                if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0)
                    schemaPB->set_obj(obj);

                return;
            }

            schemaPB->set_owner(table->owner);
            schemaPB->set_name(table->name);

            if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0)
                schemaPB->set_obj(obj);

            if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
                if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                    if (tables.count(table) > 0)
                        return;
                    else
                        tables.insert(table);
                }

                schemaPB->add_column();
                pb::Column* columnPB = schemaPB->mutable_column(schemaPB->column_size() - 1);

                for (typeCol column = 0; column < static_cast<typeCol>(table->columns.size()); ++column) {
                    if (table->columns[column] == nullptr)
                        continue;

                    columnPB->set_name(table->columns[column]->name);

                    switch (table->columns[column]->type) {
                        case SysCol::TYPE_VARCHAR:
                            columnPB->set_type(pb::VARCHAR2);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_NUMBER:
                            columnPB->set_type(pb::NUMBER);
                            columnPB->set_precision(static_cast<int32_t>(table->columns[column]->precision));
                            columnPB->set_scale(static_cast<int32_t>(table->columns[column]->scale));
                            break;

                        case SysCol::TYPE_LONG:
                            // Long, not supported
                            columnPB->set_type(pb::LONG);
                            break;

                        case SysCol::TYPE_DATE:
                            columnPB->set_type(pb::DATE);
                            break;

                        case SysCol::TYPE_RAW:
                            columnPB->set_type(pb::RAW);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_LONG_RAW: // Not supported
                            columnPB->set_type(pb::LONG_RAW);
                            break;

                        case SysCol::TYPE_CHAR:
                            columnPB->set_type(pb::CHAR);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_FLOAT:
                            columnPB->set_type(pb::BINARY_FLOAT);
                            break;

                        case SysCol::TYPE_DOUBLE:
                            columnPB->set_type(pb::BINARY_DOUBLE);
                            break;

                        case SysCol::TYPE_CLOB:
                            columnPB->set_type(pb::CLOB);
                            break;

                        case SysCol::TYPE_BLOB:
                            columnPB->set_type(pb::BLOB);
                            break;

                        case SysCol::TYPE_TIMESTAMP:
                            columnPB->set_type(pb::TIMESTAMP);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_TIMESTAMP_WITH_TZ:
                            columnPB->set_type(pb::TIMESTAMP_WITH_TZ);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_INTERVAL_YEAR_TO_MONTH:
                            columnPB->set_type(pb::INTERVAL_YEAR_TO_MONTH);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_INTERVAL_DAY_TO_SECOND:
                            columnPB->set_type(pb::INTERVAL_DAY_TO_SECOND);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_UROWID:
                            columnPB->set_type(pb::UROWID);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::TYPE_TIMESTAMP_WITH_LOCAL_TZ:
                            columnPB->set_type(pb::TIMESTAMP_WITH_LOCAL_TZ);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        default:
                            columnPB->set_type(pb::UNKNOWN);
                            break;
                    }

                    columnPB->set_nullable(table->columns[column]->nullable);
                }
            }
        }

        inline void appendAfter(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, uint64_t offset) {
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (sizes[column][VALUE_AFTER] > 0) {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_AFTER], sizes[column][VALUE_AFTER], offset,
                                         true, compressedAfter);
                        } else {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            columnNull(table, column, true);
                        }
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
                            if (sizes[column][VALUE_AFTER] > 0) {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_AFTER], sizes[column][VALUE_AFTER], offset,
                                             true, compressedAfter);
                            } else {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                columnNull(table, column, true);
                            }
                        }
                    }
                }
            }
        }

        inline void appendBefore(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, uint64_t offset) {
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (sizes[column][VALUE_BEFORE] > 0) {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_BEFORE], sizes[column][VALUE_BEFORE], offset,
                                         false, compressedBefore);
                        } else {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            columnNull(table, column, false);
                        }
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
                            if (sizes[column][VALUE_BEFORE] > 0) {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_BEFORE], sizes[column][VALUE_BEFORE], offset,
                                             false, compressedBefore);
                            } else {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                columnNull(table, column, false);
                            }
                        }
                    }
                }
            }
        }

        inline void createResponse() {
            if (unlikely(redoResponsePB != nullptr))
                throw RuntimeException(50016, "PB commit processing failed, message already exists");
            redoResponsePB = new pb::RedoResponse;
        }

        void numToString(uint64_t value, char* buf, uint64_t size) {
            uint64_t j = (size - 1) * 4;
            for (uint64_t i = 0; i < size; ++i) {
                buf[i] = Ctx::map16((value >> j) & 0xF);
                j -= 4;
            }
            buf[size] = 0;
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
        void processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) override;

    public:
        BuilderProtobuf(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat, uint64_t newAttributesFormat,
                        uint64_t newIntervalDtsFormat, uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat,
                        uint64_t newTimestampFormat, uint64_t newTimestampTzFormat, uint64_t newTimestampAll, uint64_t newCharFormat, uint64_t newScnFormat,
                        uint64_t newScnAll, uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat, uint64_t newUnknownType,
                        uint64_t newFlushBuffer);
        virtual ~BuilderProtobuf() override;

        virtual void initialize() override;
        virtual void processCommit(typeScn scn, typeSeq sequence, time_t timestamp) override;
        virtual void processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp, uint64_t offset, bool redo) override;
    };
}

#endif
