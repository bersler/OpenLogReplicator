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

        void columnNull(const OracleTable* table, typeCol col, bool after);
        virtual void columnFloat(const std::string& columnName, double value) override;
        virtual void columnDouble(const std::string& columnName, long double value) override;
        virtual void columnString(const std::string& columnName) override;
        virtual void columnNumber(const std::string& columnName, uint64_t precision, uint64_t scale) override;
        virtual void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t length) override;
        virtual void columnRowId(const std::string& columnName, typeRowId rowId) override;
        virtual void columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) override;
        virtual void columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const char* tz) override;
        void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot);
        void appendHeader(typeScn scn, time_t timestamp, bool first, bool showDb, bool showXid);
        void appendSchema(const OracleTable* table, typeObj obj);

        void appendAfter(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, uint64_t offset) {
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (lengths[column][VALUE_AFTER] > 0) {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], offset,
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
                            if (lengths[column][VALUE_AFTER] > 0) {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], offset,
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

        void appendBefore(LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, uint64_t offset) {
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (lengths[column][VALUE_BEFORE] > 0) {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], offset,
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
                            if (lengths[column][VALUE_BEFORE] > 0) {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                processValue(lobCtx, xmlCtx, table, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], offset,
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

        void createResponse() {
            if (redoResponsePB != nullptr)
                throw RuntimeException(50016, "PB commit processing failed, message already exists");
            redoResponsePB = new pb::RedoResponse;
        }

        void numToString(uint64_t value, char* buf, uint64_t length);
        virtual void processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) override;
        virtual void processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) override;
        virtual void processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const OracleTable* table, typeObj obj,
                                   typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid, uint64_t offset) override;
        virtual void processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const OracleTable* table, typeObj obj, typeDataObj dataObj, uint16_t type,
                                uint16_t seq, const char* sql, uint64_t sqlLength, const char* owner, uint64_t ownerLength, const char* name, uint64_t nameLength) override;
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
