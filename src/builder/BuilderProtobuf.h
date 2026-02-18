/* Header for BuilderProtobuf class
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

#ifndef BUILDER_PROTOBUF_H_
#define BUILDER_PROTOBUF_H_

#include "../common/DbTable.h"
#include "../common/OraProtoBuf.pb.h"
#include "../common/table/SysCol.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "Builder.h"

namespace OpenLogReplicator {
    class BuilderProtobuf final : public Builder {
    protected:
        pb::RedoResponse* redoResponsePB{nullptr};
        pb::Value* valuePB{nullptr};
        pb::Payload* payloadPB{nullptr};
        pb::Schema* schemaPB{nullptr};

        void columnNull(const DbTable* table, typeCol col, bool after) {
            if (table != nullptr && format.unknownType == Format::UNKNOWN_TYPE::HIDE) {
                const DbColumn* column = table->columns[col];
                if (column->storedAsLob)
                    return;
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

            if (table == nullptr || ctx->isFlagSet(Ctx::REDO_FLAGS::RAW_COLUMN_DATA)) {
                std::string columnName("COL_" + std::to_string(col));
                valuePB->set_name(columnName);
                return;
            }

            valuePB->set_name(table->columns[col]->name);
        }

        void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
            if (format.isMessageFormatAddSequences())
                payloadPB->set_num(num);

            if (format.ridFormat == Format::RID_FORMAT::SKIP)
                return;
            if (format.ridFormat == Format::RID_FORMAT::TEXT) {
                const RowId rowId(dataObj, bdba, slot);
                char str[RowId::SIZE + 1];
                rowId.toString(str);
                payloadPB->set_rid(str, 18);
            }
        }

        void appendHeader(Scn scn, Time timestamp, bool first, bool showDb, bool showXid) {
            time_t tm = timestamp.toEpoch(metadata->ctx->hostTimezone);

            redoResponsePB->set_code(pb::ResponseCode::PAYLOAD);
            if (first || format.isScnTypeDml()) {
                if (format.scnFormat == Format::SCN_FORMAT::TEXT_HEX) {
                    char buf[17];
                    numToString(scn.getData(), buf, 16);
                    redoResponsePB->set_scns(buf);
                } else {
                    redoResponsePB->set_scn(scn.getData());
                }
            }

            if (first || format.isTimestampTypeDml()) {
                std::string str;
                switch (format.timestampFormat) {
                    case Format::TIMESTAMP_FORMAT::UNIX_NANO:
                        redoResponsePB->set_tm(tm * 1000000000L);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX_MICRO:
                        redoResponsePB->set_tm(tm * 1000000L);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX_MILLI:
                        redoResponsePB->set_tm(tm * 1000L);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX:
                        redoResponsePB->set_tm(tm);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX_NANO_STRING:
                        str = std::to_string(tm * 1000000000L);
                        redoResponsePB->set_tms(str);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX_MICRO_STRING:
                        str = std::to_string(tm * 1000000L);
                        redoResponsePB->set_tms(str);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX_MILLI_STRING:
                        str = std::to_string(tm * 1000L);
                        redoResponsePB->set_tms(str);
                        break;

                    case Format::TIMESTAMP_FORMAT::UNIX_STRING:
                        str = std::to_string(tm);
                        redoResponsePB->set_tms(str);
                        break;

                    case Format::TIMESTAMP_FORMAT::ISO8601:
                        char buffer[22];
                        str.assign(buffer, Data::epochToIso8601(tm, buffer, true, true));
                        redoResponsePB->set_tms(str);
                        break;

                    default:
                        break;
                }
            }

            redoResponsePB->set_c_scn(lwnScn.getData());
            redoResponsePB->set_c_idx(lwnIdx);

            if (showXid) {
                if (format.xidFormat == Format::XID_FORMAT::TEXT_HEX) {
                    std::ostringstream ss;
                    ss << "0x";
                    ss << std::setfill('0') << std::setw(4) << std::hex << static_cast<uint>(lastXid.usn());
                    ss << '.';
                    ss << std::setfill('0') << std::setw(3) << std::hex << static_cast<uint>(lastXid.slt());
                    ss << '.';
                    ss << std::setfill('0') << std::setw(8) << std::hex << static_cast<uint>(lastXid.sqn());
                    redoResponsePB->set_xid(ss.str());
                } else if (format.xidFormat == Format::XID_FORMAT::TEXT_DEC) {
                    std::ostringstream ss;
                    ss << static_cast<uint>(lastXid.usn());
                    ss << '.';
                    ss << static_cast<uint>(lastXid.slt());
                    ss << '.';
                    ss << static_cast<uint>(lastXid.sqn());
                    redoResponsePB->set_xid(ss.str());
                } else if (format.xidFormat == Format::XID_FORMAT::NUMERIC) {
                    redoResponsePB->set_xidn(lastXid.getData());
                } else if (format.xidFormat == Format:: XID_FORMAT::TEXT_REVERSED) {
                    std::ostringstream ss;
                    ss << std::setfill('0') << std::setw(16) << std::hex << lastXid.getData();
                    redoResponsePB->set_xid(ss.str());
                }
            }

            if (showDb)
                redoResponsePB->set_db(metadata->conName);
        }

        void appendSchema(const DbTable* table, typeObj obj) {
            if (unlikely(table == nullptr)) {
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

                if (format.isSchemaFormatObj())
                    schemaPB->set_obj(obj);

                return;
            }

            schemaPB->set_owner(table->owner);
            schemaPB->set_name(table->name);

            if (format.isSchemaFormatObj())
                schemaPB->set_obj(obj);

            if (format.isSchemaFormatFull()) {
                if (!format.isSchemaFormatRepeated()) {
                    if (tables.count(table) > 0)
                        return;
                    tables.insert(table);
                }

                schemaPB->add_column();
                pb::Column* columnPB = schemaPB->mutable_column(schemaPB->column_size() - 1);

                for (typeCol column = 0; column < static_cast<typeCol>(table->columns.size()); ++column) {
                    if (table->columns[column] == nullptr)
                        continue;

                    columnPB->set_name(table->columns[column]->name);

                    switch (table->columns[column]->type) {
                        case SysCol::COLTYPE::VARCHAR:
                            columnPB->set_type(pb::VARCHAR2);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::NUMBER:
                            columnPB->set_type(pb::NUMBER);
                            columnPB->set_precision(static_cast<int32_t>(table->columns[column]->precision));
                            columnPB->set_scale(static_cast<int32_t>(table->columns[column]->scale));
                            break;

                        case SysCol::COLTYPE::LONG:
                            // Long, not supported
                            columnPB->set_type(pb::LONG);
                            break;

                        case SysCol::COLTYPE::DATE:
                            columnPB->set_type(pb::DATE);
                            break;

                        case SysCol::COLTYPE::RAW:
                            columnPB->set_type(pb::RAW);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::LONG_RAW: // Not supported
                            columnPB->set_type(pb::LONG_RAW);
                            break;

                        case SysCol::COLTYPE::CHAR:
                            columnPB->set_type(pb::CHAR);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::FLOAT:
                            columnPB->set_type(pb::BINARY_FLOAT);
                            break;

                        case SysCol::COLTYPE::DOUBLE:
                            columnPB->set_type(pb::BINARY_DOUBLE);
                            break;

                        case SysCol::COLTYPE::CLOB:
                            columnPB->set_type(pb::CLOB);
                            break;

                        case SysCol::COLTYPE::BLOB:
                            columnPB->set_type(pb::BLOB);
                            break;

                        case SysCol::COLTYPE::TIMESTAMP:
                            columnPB->set_type(pb::TIMESTAMP);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::TIMESTAMP_WITH_TZ:
                            columnPB->set_type(pb::TIMESTAMP_WITH_TZ);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::INTERVAL_YEAR_TO_MONTH:
                            columnPB->set_type(pb::INTERVAL_YEAR_TO_MONTH);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::INTERVAL_DAY_TO_SECOND:
                            columnPB->set_type(pb::INTERVAL_DAY_TO_SECOND);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::UROWID:
                            columnPB->set_type(pb::UROWID);
                            columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                            break;

                        case SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ:
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

        void appendAfter(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, FileOffset fileOffset) {
            if (format.columnFormat > Format::COLUMN_FORMAT::CHANGED && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][+Format::VALUE_TYPE::AFTER] != nullptr) {
                        if (sizes[column][+Format::VALUE_TYPE::AFTER] > 0) {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::AFTER],
                                         sizes[column][+Format::VALUE_TYPE::AFTER], fileOffset, true, compressedAfter);
                        } else {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            columnNull(table, column, true);
                        }
                    }
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
                            if (sizes[column][+Format::VALUE_TYPE::AFTER] > 0) {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::AFTER],
                                             sizes[column][+Format::VALUE_TYPE::AFTER], fileOffset, true, compressedAfter);
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

        void appendBefore(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, FileOffset fileOffset) {
            if (format.columnFormat > Format::COLUMN_FORMAT::CHANGED && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][+Format::VALUE_TYPE::BEFORE] != nullptr) {
                        if (sizes[column][+Format::VALUE_TYPE::BEFORE] > 0) {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::BEFORE],
                                         sizes[column][+Format::VALUE_TYPE::BEFORE], fileOffset, false, compressedBefore);
                        } else {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            columnNull(table, column, false);
                        }
                    }
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
                            if (sizes[column][+Format::VALUE_TYPE::BEFORE] > 0) {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                processValue(lobCtx, xmlCtx, table, column, values[column][+Format::VALUE_TYPE::BEFORE],
                                             sizes[column][+Format::VALUE_TYPE::BEFORE], fileOffset, false, compressedBefore);
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

        void createResponse() {
            if (unlikely(redoResponsePB != nullptr))
                throw RuntimeException(50016, "PB commit processing failed, message already exists");
            redoResponsePB = new pb::RedoResponse;
        }

        static void numToString(uint64_t value, char* buf, uint64_t size) {
            uint64_t j = (size - 1) * 4;
            for (uint64_t i = 0; i < size; ++i) {
                buf[i] = Data::map16((value >> j) & 0xF);
                j -= 4;
            }
            buf[size] = 0;
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

    public:
        BuilderProtobuf(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, Format& newFormat, uint64_t newFlushBuffer);
        ~BuilderProtobuf() override;

        void initialize() override;
        void processCommit() override;
        void processCheckpoint(Seq sequence, Scn scn, Time timestamp, FileOffset fileOffset, bool redo) override;
    };
}

#endif
