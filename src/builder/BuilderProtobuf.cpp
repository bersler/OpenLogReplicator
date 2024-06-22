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
#include "../common/exception/RuntimeException.h"
#include "../common/table/SysCol.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "BuilderProtobuf.h"

namespace OpenLogReplicator {
    BuilderProtobuf::BuilderProtobuf(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newDbFormat, uint64_t newAttributesFormat,
                                     uint64_t newIntervalDtsFormat, uint64_t newIntervalYtmFormat, uint64_t newMessageFormat, uint64_t newRidFormat,
                                     uint64_t newXidFormat, uint64_t newTimestampFormat, uint64_t newTimestampTzFormat, uint64_t newTimestampAll,
                                     uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newScnAll, uint64_t newUnknownFormat, uint64_t newSchemaFormat,
                                     uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer) :
            Builder(newCtx, newLocales, newMetadata, newDbFormat, newAttributesFormat, newIntervalDtsFormat, newIntervalYtmFormat, newMessageFormat,
                    newRidFormat, newXidFormat, newTimestampFormat, newTimestampTzFormat, newTimestampAll, newCharFormat, newScnFormat, newScnAll,
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

    void BuilderProtobuf::columnFloat(const std::string& columnName, double value) {
        valuePB->set_name(columnName);
        valuePB->set_value_double(value);
    }

    // TODO: possible precision loss
    void BuilderProtobuf::columnDouble(const std::string& columnName, long double value) {
        valuePB->set_name(columnName);
        valuePB->set_value_double(value);
    }

    void BuilderProtobuf::columnString(const std::string& columnName) {
        valuePB->set_name(columnName);
        valuePB->set_value_string(valueBuffer, valueSize);
    }

    void BuilderProtobuf::columnNumber(const std::string& columnName, uint64_t precision, uint64_t scale) {
        valuePB->set_name(columnName);
        valueBuffer[valueSize] = 0;
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
            valuePB->set_value_string(valueBuffer, valueSize);
        }
    }

    void BuilderProtobuf::columnRowId(const std::string& columnName, typeRowId rowId) {
        char str[19];
        rowId.toHex(str);
        valuePB->set_name(columnName);
        valuePB->set_value_string(str, 18);
    }

    void BuilderProtobuf::columnRaw(const std::string& columnName, const uint8_t* data __attribute__((unused)), uint64_t size __attribute__((unused))) {
        valuePB->set_name(columnName);
        // TODO: implement
    }

    void BuilderProtobuf::columnTimestamp(const std::string& columnName, time_t tmstp __attribute__((unused)),
                                          uint64_t fraction __attribute__((unused))) {
        valuePB->set_name(columnName);
        // TODO: implement
    }

    void BuilderProtobuf::columnTimestampTz(const std::string& columnName, time_t tmstp __attribute__((unused)),
                                            uint64_t fraction __attribute__((unused)), const char* tz __attribute__((unused))) {
        valuePB->set_name(columnName);
        // TODO: implement
    }

    void BuilderProtobuf::processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) {
        newTran = false;
        builderBegin(scn, sequence, 0, 0);
        createResponse();
        appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::BEGIN);

            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB begin processing failed, error serializing to string");
            append(output);
            builderCommit(false);
        }
    }

    void BuilderProtobuf::processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table,
                                        typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB insert processing failed, a message is missing");
        } else {
            builderBegin(scn, sequence, obj, 0);
            createResponse();
            appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::INSERT);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, xmlCtx, table, offset);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB insert processing failed, error serializing to string");
            append(output);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderProtobuf::processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table,
                                        typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB update processing failed, a message is missing");
        } else {
            builderBegin(scn, sequence, obj, 0);
            createResponse();
            appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::UPDATE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, offset);
        appendAfter(lobCtx, xmlCtx, table, offset);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB update processing failed, error serializing to string");
            append(output);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderProtobuf::processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                        typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused)), uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB delete processing failed, a message is missing");
        } else {
            builderBegin(scn, sequence, obj, 0);
            createResponse();
            appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::DELETE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, offset);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB delete processing failed, error serializing to string");
            append(output);
            builderCommit(false);
        }
        ++num;
    }

    void BuilderProtobuf::processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const OracleTable* table __attribute__((unused)), typeObj obj,
                                     typeDataObj dataObj __attribute__((unused)), uint16_t type __attribute__((unused)), uint16_t seq __attribute__((unused)),
                                     const char* sql, uint64_t sqlSize) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB commit processing failed, a message is missing");
        } else {
            builderBegin(scn, sequence, obj, 0);
            createResponse();
            appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DDL) != 0, true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::DDL);
            appendSchema(table, obj);
            payloadPB->set_ddl(sql, sqlSize);
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            std::string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB commit processing failed, error serializing to string");
            append(output);
            builderCommit(true);
        }
        ++num;
    }

    void BuilderProtobuf::initialize() {
        Builder::initialize();

        GOOGLE_PROTOBUF_VERIFY_VERSION;
    }

    void BuilderProtobuf::processCommit(typeScn scn, typeSeq sequence, time_t timestamp) {
        // Skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB commit processing failed, a message is missing");
        } else {
            builderBegin(scn, sequence, 0, 0);
            createResponse();
            appendHeader(scn, timestamp, true, (dbFormat & DB_FORMAT_ADD_DML) != 0, true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::COMMIT);
        }

        std::string output;
        bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (unlikely(!ret))
            throw RuntimeException(50017, "PB commit processing failed, error serializing to string");
        append(output);
        builderCommit(true);

        num = 0;
    }

    void BuilderProtobuf::processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp __attribute__((unused)), uint64_t offset, bool redo) {
        if (lwnScn != scn) {
            lwnScn = scn;
            lwnIdx = 0;
        }

        builderBegin(scn, sequence, 0, OUTPUT_BUFFER_MESSAGE_CHECKPOINT);
        createResponse();
        appendHeader(scn, timestamp, true, false, false);

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

        if (unlikely(!ret))
            throw RuntimeException(50017, "PB commit processing failed, error serializing to string");
        append(output);
        builderCommit(true);
    }
}
