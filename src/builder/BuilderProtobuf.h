/* Header for BuilderProtobuf class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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
    class BuilderProtobuf : public Builder {
    protected:
        pb::RedoResponse* redoResponsePB;
        pb::Value* valuePB;
        pb::Payload* payloadPB;
        pb::Schema* schemaPB;

        void columnNull(OracleTable* table, typeCol col, bool after);
        void columnFloat(std::string& columnName, float value) override;
        void columnDouble(std::string& columnName, double value) override;
        void columnString(std::string& columnName) override;
        void columnNumber(std::string& columnName, uint64_t precision, uint64_t scale) override;
        void columnRaw(std::string& columnName, const uint8_t* data, uint64_t length) override;
        void columnTimestamp(std::string& columnName, struct tm& time_, uint64_t fraction, const char* tz) override;
        void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot);
        void appendHeader(bool first, bool showXid);
        void appendSchema(OracleTable* table, typeObj obj);

        void appendAfter(LobCtx* lobCtx, OracleTable* table) {
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (lengths[column][VALUE_AFTER] > 0) {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            processValue(lobCtx, table, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], true,
                                         compressedAfter);
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
                    auto column = (typeCol)(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_AFTER] != nullptr) {
                            if (lengths[column][VALUE_AFTER] > 0) {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                processValue(lobCtx, table, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], true,
                                             compressedAfter);
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

        void appendBefore(LobCtx* lobCtx, OracleTable* table) {
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (lengths[column][VALUE_BEFORE] > 0) {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            processValue(lobCtx, table, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], false,
                                         compressedBefore);
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
                    auto column = (typeCol)(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_BEFORE] != nullptr) {
                            if (lengths[column][VALUE_BEFORE] > 0) {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                processValue(lobCtx, table, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], false,
                                             compressedBefore);
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
                throw RuntimeException("PB commit processing failed, message already exists, internal error");
            redoResponsePB = new pb::RedoResponse;
        }

        static void numToString(uint64_t value, char* buf, uint64_t length);
        void processInsert(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid) override;
        void processUpdate(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid) override;
        void processDelete(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid) override;
        void processDdl(OracleTable* table, typeDataObj dataObj, uint16_t type, uint16_t seq, const char* operation, const char* sql, uint64_t sqlLength)
                override;
        void processBeginMessage() override;

    public:
        BuilderProtobuf(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat,
                        uint64_t newTimestampFormat, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newUnknownFormat, uint64_t newSchemaFormat,
                        uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer);
        ~BuilderProtobuf() override;

        void initialize() override;
        void processCommit(bool system) override;
        void processCheckpoint(typeScn scn, typeTime time_, typeSeq sequence, uint64_t offset, bool redo) override;
    };
}

#endif
