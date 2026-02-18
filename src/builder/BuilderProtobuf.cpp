/* Memory buffer for handling output buffer in JSON format
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

#include "../common/DbColumn.h"
#include "../common/DbTable.h"
#include "../common/exception/RuntimeException.h"
#include "../common/types/RowId.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "BuilderProtobuf.h"

namespace OpenLogReplicator {
    BuilderProtobuf::BuilderProtobuf(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, Format& newFormat, uint64_t newFlushBuffer):
            Builder(newCtx, newLocales, newMetadata, newFormat, newFlushBuffer) {}

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

    void BuilderProtobuf::columnNumber(const std::string& columnName, int precision, int scale) {
        valuePB->set_name(columnName);
        valueBuffer[valueSize] = 0;
        char* retPtr;

        if (scale == 0 && precision <= 17) {
            const int64_t value = strtol(valueBuffer, &retPtr, 10);
            valuePB->set_value_int(value);
        } else if (precision <= 6 && scale < 38) {
            const float value = strtof(valueBuffer, &retPtr);
            valuePB->set_value_float(value);
        } else if (precision <= 15 && scale <= 307) {
            const double value = strtod(valueBuffer, &retPtr);
            valuePB->set_value_double(value);
        } else {
            valuePB->set_value_string(valueBuffer, valueSize);
        }
    }

    void BuilderProtobuf::columnRowId(const std::string& columnName, RowId rowId) {
        char str[RowId::SIZE + 1];
        rowId.toHex(str);
        valuePB->set_name(columnName);
        valuePB->set_value_string(str, 18);
    }

    void BuilderProtobuf::columnRaw(const std::string& columnName, const uint8_t* data __attribute__((unused)), uint64_t size __attribute__((unused))) {
        valuePB->set_name(columnName);
        // TODO: implement
    }

    void BuilderProtobuf::columnTimestamp(const std::string& columnName, time_t timestamp __attribute__((unused)),
                                          uint64_t fraction __attribute__((unused))) {
        valuePB->set_name(columnName);
        // TODO: implement
    }

    void BuilderProtobuf::columnTimestampTz(const std::string& columnName, time_t timestamp __attribute__((unused)),
                                            uint64_t fraction __attribute__((unused)),
                                            const std::string_view& tz __attribute__((unused))) {
        valuePB->set_name(columnName);
        // TODO: implement
    }

    void BuilderProtobuf::processBeginMessage(Seq sequence, Time timestamp) {
        newTran = false;
        builderBegin(sequence, beginScn, 0, BuilderMsg::OUTPUT_BUFFER::NONE);
        createResponse();
        appendHeader(beginScn, timestamp, true, format.isDbFormatAddDml(), true);

        if (!format.isMessageFormatFull()) {
            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::BEGIN);

            std::string output;
            const bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB begin processing failed, error serializing to string");
            append(output);
            builderCommit();
        }
    }

    void BuilderProtobuf::processInsert(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                        typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB insert processing failed, a message is missing");
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            createResponse();
            appendHeader(scn, timestamp, true, format.isDbFormatAddDml(), true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::INSERT);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, xmlCtx, table, fileOffset);

        if (!format.isMessageFormatFull()) {
            std::string output;
            const bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB insert processing failed, error serializing to string");
            append(output);
            builderCommit();
        }
        ++num;
    }

    void BuilderProtobuf::processUpdate(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                        typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB update processing failed, a message is missing");
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            createResponse();
            appendHeader(scn, timestamp, true, format.isDbFormatAddDml(), true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::UPDATE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, fileOffset);
        appendAfter(lobCtx, xmlCtx, table, fileOffset);

        if (!format.isMessageFormatFull()) {
            std::string output;
            const bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB update processing failed, error serializing to string");
            append(output);
            builderCommit();
        }
        ++num;
    }

    void BuilderProtobuf::processDelete(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                        typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB delete processing failed, a message is missing");
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            createResponse();
            appendHeader(scn, timestamp, true, format.isDbFormatAddDml(), true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::DELETE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, fileOffset);

        if (!format.isMessageFormatFull()) {
            std::string output;
            const bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB delete processing failed, error serializing to string");
            append(output);
            builderCommit();
        }
        ++num;
    }

    void BuilderProtobuf::processDdl(Seq sequence, Scn scn, Time timestamp, const DbTable* table __attribute__((unused)), typeObj obj) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB commit processing failed, a message is missing");
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            createResponse();
            appendHeader(scn, timestamp, true, format.isDbFormatAddDdl(), true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::DDL);
            appendSchema(table, obj);
            // truncated to 1M
            if (ddlFirst != nullptr) {
                const typeTransactionSize* chunkSize = reinterpret_cast<uint64_t*>(ddlFirst) + sizeof(uint8_t*);
                const char* chunkData = reinterpret_cast<const char*>(ddlFirst) + sizeof(uint8_t*) + sizeof(uint64_t);
                payloadPB->set_ddl(chunkData, *chunkSize);
            }
        }

        if (!format.isMessageFormatFull()) {
            std::string output;
            const bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (unlikely(!ret))
                throw RuntimeException(50017, "PB commit processing failed, error serializing to string");
            append(output);
            builderCommit();
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

        if (format.isMessageFormatFull()) {
            if (unlikely(redoResponsePB == nullptr))
                throw RuntimeException(50018, "PB commit processing failed, a message is missing");
        } else {
            builderBegin(commitSequence, commitScn, 0, BuilderMsg::OUTPUT_BUFFER::NONE);
            createResponse();
            appendHeader(commitScn, commitTimestamp, true, format.isDbFormatAddDml(), true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::COMMIT);
        }

        std::string output;
        const bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (unlikely(!ret))
            throw RuntimeException(50017, "PB commit processing failed, error serializing to string");
        append(output);
        builderCommit();

        num = 0;
    }

    void BuilderProtobuf::processCheckpoint(Seq sequence, Scn scn, Time timestamp __attribute__((unused)), FileOffset fileOffset,
            bool redo) {
        if (lwnScn != scn) {
            lwnScn = scn;
            lwnIdx = 0;
        }

        auto flags = BuilderMsg::OUTPUT_BUFFER::CHECKPOINT;
        if (redo)
            flags = static_cast<BuilderMsg::OUTPUT_BUFFER>(static_cast<uint>(flags) | static_cast<uint>(BuilderMsg::OUTPUT_BUFFER::REDO));
        builderBegin(sequence, scn, 0, flags);
        createResponse();
        appendHeader(scn, timestamp, true, false, false);

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::CHKPT);
        payloadPB->set_seq(sequence.getData());
        payloadPB->set_offset(fileOffset.getData());
        payloadPB->set_redo(redo);

        std::string output;
        const bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (unlikely(!ret))
            throw RuntimeException(50017, "PB commit processing failed, error serializing to string");
        append(output);
        builderCommit();
    }
}
