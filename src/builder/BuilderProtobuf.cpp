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
#include "../common/RuntimeException.h"
#include "../common/SysCol.h"
#include "../common/typeRowId.h"
#include "BuilderProtobuf.h"

namespace OpenLogReplicator {
    BuilderProtobuf::BuilderProtobuf(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newMessageFormat, uint64_t newRidFormat,
                                     uint64_t newXidFormat, uint64_t newTimestampFormat, uint64_t newCharFormat, uint64_t newScnFormat,
                                     uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat, uint64_t newUnknownType,
                                     uint64_t newFlushBuffer) :
            Builder(newCtx, newLocales, newMetadata, newMessageFormat, newRidFormat, newXidFormat, newTimestampFormat, newCharFormat, newScnFormat,
                    newUnknownFormat, newSchemaFormat, newColumnFormat, newUnknownType, newFlushBuffer),
            redoResponsePB(nullptr),
            valuePB(nullptr),
            payloadPB(nullptr),
            schemaPB(nullptr) {
    }

    BuilderProtobuf::~BuilderProtobuf() {
        if (redoResponsePB != nullptr) {
            delete redoResponsePB;
            redoResponsePB = nullptr;
        }
        google::protobuf::ShutdownProtobufLibrary();
    }

    void BuilderProtobuf::columnNull(OracleTable* table, typeCol col, bool after) {
        if (table != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
            OracleColumn* column = table->columns[col];
            if (column->storedAsLob)
                return;
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

        if (table == nullptr || FLAG(REDO_FLAGS_RAW_COLUMN_DATA)) {
            std::string columnName("COL_" + std::to_string(col));
            valuePB->set_name(columnName);
            return;
        }

        valuePB->set_name(table->columns[col]->name);
    }

    void BuilderProtobuf::columnFloat(const std::string& columnName, double value) {
        valuePB->set_name(columnName);
        valuePB->set_value_double(value);
    }

    // TODO: possible precession loss
    void BuilderProtobuf::columnDouble(const std::string& columnName, long double value) {
        valuePB->set_name(columnName);
        valuePB->set_value_double(value);
    }

    void BuilderProtobuf::columnString(const std::string& columnName) {
        valuePB->set_name(columnName);
        valuePB->set_value_string(valueBuffer, valueLength);
    }

    void BuilderProtobuf::columnNumber(const std::string& columnName, uint64_t precision, uint64_t scale) {
        valuePB->set_name(columnName);
        valueBuffer[valueLength] = 0;
        char* retPtr;

        if (scale == 0 && precision <= 17) {
            int64_t value = strtol(valueBuffer, &retPtr, 10);
            valuePB->set_value_int(value);
        } else if (precision <= 6 && scale < 38) {
            float value = strtof(valueBuffer, &retPtr);
            valuePB->set_value_float(value);
        } else if (precision <= 15 && scale <= 307) {
            double value = strtod(valueBuffer, &retPtr);
            valuePB->set_value_double(value);
        } else {
            valuePB->set_value_string(valueBuffer, valueLength);
        }
    }

    void BuilderProtobuf::columnRaw(const std::string& columnName, const uint8_t* data __attribute__((unused)), uint64_t length __attribute__((unused))) {
        valuePB->set_name(columnName);
    }

    void BuilderProtobuf::columnTimestamp(const std::string& columnName, struct tm& time_ __attribute__((unused)), uint64_t fraction __attribute__((unused)),
            const char* tz __attribute__((unused))) {
        valuePB->set_name(columnName);
    }

    void BuilderProtobuf::appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot) {
        if ((messageFormat & MESSAGE_FORMAT_ADD_SEQUENCES) != 0)
            payloadPB->set_num(num);

        if (ridFormat == RID_FORMAT_SKIP)
            return;

        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        payloadPB->set_rid(str, 18);
    }

    void BuilderProtobuf::appendHeader(bool first, bool showXid) {
        redoResponsePB->set_code(pb::ResponseCode::PAYLOAD);
        if (first || (scnFormat & SCN_FORMAT_ALL_PAYLOADS) != 0) {
            if ((scnFormat & SCN_FORMAT_HEX) != 0) {
                char buf[17];
                numToString(lastScn, buf, 16);
                redoResponsePB->set_scns(buf);
            } else {
                redoResponsePB->set_scn(lastScn);
            }
        }

        if (first || (timestampFormat & TIMESTAMP_FORMAT_ALL_PAYLOADS) != 0) {
            if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
                char iso[21];
                lastTime.toIso8601(iso);
                redoResponsePB->set_tms(iso);
            } else {
                redoResponsePB->set_tm(lastTime.toTime() * 1000);
            }
        }

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
            } else {
                redoResponsePB->set_xidn(lastXid.getData());
            }
        }
    }

    void BuilderProtobuf::appendSchema(OracleTable* table, typeObj obj) {
        if (table == nullptr) {
            std::string tableName("OBJ_" + std::to_string(obj));
            schemaPB->set_name(tableName);
            return;
        }

        schemaPB->set_owner(table->owner);
        schemaPB->set_name(table->name);

        if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0)
            schemaPB->set_obj(table->obj);

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

                switch(table->columns[column]->type) {
                case SYS_COL_TYPE_VARCHAR:
                    columnPB->set_type(pb::VARCHAR2);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_NUMBER:
                    columnPB->set_type(pb::NUMBER);
                    columnPB->set_precision(static_cast<int32_t>(table->columns[column]->precision));
                    columnPB->set_scale(static_cast<int32_t>(table->columns[column]->scale));
                    break;

                case SYS_COL_TYPE_LONG: // long, not supported
                    columnPB->set_type(pb::LONG);
                    break;

                case SYS_COL_TYPE_DATE:
                    columnPB->set_type(pb::DATE);
                    break;

                case SYS_COL_TYPE_RAW:
                    columnPB->set_type(pb::RAW);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_LONG_RAW: // Not supported
                    columnPB->set_type(pb::LONG_RAW);
                    break;

                case SYS_COL_TYPE_ROWID: // Not supported
                    columnPB->set_type(pb::ROWID);
                    break;

                case SYS_COL_TYPE_CHAR:
                    columnPB->set_type(pb::CHAR);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_FLOAT:
                    columnPB->set_type(pb::BINARY_FLOAT);
                    break;

                case SYS_COL_TYPE_DOUBLE:
                    columnPB->set_type(pb::BINARY_DOUBLE);
                    break;

                case SYS_COL_TYPE_CLOB:
                    columnPB->set_type(pb::CLOB);
                    break;

                case SYS_COL_TYPE_BLOB:
                    columnPB->set_type(pb::BLOB);
                    break;

                case SYS_COL_TYPE_TIMESTAMP:
                    columnPB->set_type(pb::TIMESTAMP);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_TIMESTAMP_WITH_TZ:
                    columnPB->set_type(pb::TIMESTAMP_WITH_TZ);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH:
                    columnPB->set_type(pb::INTERVAL_YEAR_TO_MONTH);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND:
                    columnPB->set_type(pb::INTERVAL_DAY_TO_SECOND);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_URAWID:
                    columnPB->set_type(pb::UROWID);
                    columnPB->set_length(static_cast<int32_t>(table->columns[column]->length));
                    break;

                case SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ: // Not supported
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

    void BuilderProtobuf::numToString(uint64_t value, char* buf, uint64_t length) {
        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            buf[i] = ctx->map16[(value >> j) & 0xF];
            j -= 4;
        }
        buf[length] = 0;
    }

    void BuilderProtobuf::processBeginMessage() {
        newTran = false;
        builderBegin(0);

        createResponse();
        appendHeader(true, true);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::BEGIN);

            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret)
                throw RuntimeException("PB begin processing failed, error serializing to string");
            builderAppend(output);
            builderCommit(false);
        }
    }

    void BuilderProtobuf::processInsert(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot,
                                        typeXid xid __attribute__((unused))) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr)
                throw RuntimeException("PB insert processing failed, message missing, internal error");
        } else {
            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            createResponse();
            appendHeader(true, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::INSERT);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, table);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret)
                throw RuntimeException("PB insert processing failed, error serializing to string");
            builderAppend(output);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderProtobuf::processUpdate(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot,
                                        typeXid xid __attribute__((unused))) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr)
                throw RuntimeException("PB update processing failed, message missing, internal error");
        } else {
            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            createResponse();
            appendHeader(true, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::UPDATE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, table);
        appendAfter(lobCtx, table);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret)
                throw RuntimeException("PB update processing failed, error serializing to string");
            builderAppend(output);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderProtobuf::processDelete(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot,
                                        typeXid xid __attribute__((unused))) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr)
                throw RuntimeException("PB delete processing failed, message missing, internal error");
        } else {

            if (table != nullptr)
                builderBegin(table->obj);
            else
                builderBegin(0);

            createResponse();
            appendHeader(true, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::DELETE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, table);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret)
                throw RuntimeException("PB delete processing failed, error serializing to string");
            builderAppend(output);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderProtobuf::processDdl(OracleTable* table __attribute__((unused)), typeDataObj dataObj __attribute__((unused)),
                                     uint16_t type __attribute__((unused)), uint16_t seq __attribute__((unused)),
                                     const char* operation __attribute__((unused)), const char* sql, uint64_t sqlLength) {
        if (newTran)
            processBeginMessage();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr)
                throw RuntimeException("PB commit processing failed, message missing, internal error");
        } else {
            createResponse();
            appendHeader(true, true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::DDL);
            payloadPB->set_ddl(sql, sqlLength);
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret)
                throw RuntimeException("PB commit processing failed, error serializing to string");
            builderAppend(output);
            builderCommit(true);
        }
        ++num;
    }

    void BuilderProtobuf::initialize() {
        Builder::initialize();

        GOOGLE_PROTOBUF_VERIFY_VERSION;
    }

    void BuilderProtobuf::processCommit() {
        // Skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr)
                throw RuntimeException("PB commit processing failed, message missing, internal error");
        } else {
            builderBegin(0);

            createResponse();
            appendHeader(true, true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::COMMIT);
        }

        std::string output;
        bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (!ret)
            throw RuntimeException("PB commit processing failed, error serializing to string");
        builderAppend(output);
        builderCommit(true);

        num = 0;
    }

    void BuilderProtobuf::processCheckpoint(typeScn scn __attribute__((unused)), typeTime time_ __attribute__((unused)), typeSeq sequence, uint64_t offset,
                                            bool redo) {
        if (FLAG(REDO_FLAGS_HIDE_CHECKPOINT))
            return;

        createResponse();
        builderBegin(0);
        appendHeader(true, true);

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::CHKPT);
        payloadPB->set_seq(sequence);
        payloadPB->set_offset(offset);
        payloadPB->set_redo(redo);

        std::string output;
        bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (!ret)
            throw RuntimeException("PB commit processing failed, error serializing to string");
        builderAppend(output);
        builderCommit(true);
    }
}
