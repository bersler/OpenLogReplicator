/* Header for OutputBufferProtobuf class
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

#include "OracleObject.h"
#include "OraProtoBuf.pb.h"
#include "OutputBuffer.h"

#ifndef OUTPUTBUFFERPROTOBUF_H_
#define OUTPUTBUFFERPROTOBUF_H_

namespace OpenLogReplicator {
    class OutputBufferProtobuf : public OutputBuffer {
    protected:
        pb::RedoResponse* redoResponsePB;
        pb::Value* valuePB;
        pb::Payload* payloadPB;
        pb::Schema* schemaPB;

        void columnNull(OracleObject* object, typeCOL col);
        virtual void columnFloat(std::string& columnName, float value);
        virtual void columnDouble(std::string& columnName, double value);
        virtual void columnString(std::string& columnName);
        virtual void columnNumber(std::string& columnName, uint64_t precision, uint64_t scale);
        virtual void columnRaw(std::string& columnName, const uint8_t* data, uint64_t length);
        virtual void columnTimestamp(std::string& columnName, struct tm& time_, uint64_t fraction, const char* tz);
        void appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot);
        void appendHeader(bool first, bool showXid);
        void appendSchema(OracleObject* object, typeDATAOBJ dataObj);

        void appendAfter(OracleObject* object) {
            if (columnFormat > 0 && object != nullptr) {
                for (typeCOL column = 0; column < object->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (lengths[column][VALUE_AFTER] > 0) {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            processValue(object, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], compressedAfter);
                        } else {
                            payloadPB->add_after();
                            valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                            columnNull(object, column);
                        }
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    typeCOL column = base << 6;
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_AFTER] != nullptr) {
                            if (lengths[column][VALUE_AFTER] > 0) {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                processValue(object, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], compressedAfter);
                            } else {
                                payloadPB->add_after();
                                valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                                columnNull(object, column);
                            }
                        }
                    }
                }
            }
        }

        void appendBefore(OracleObject* object) {
            if (columnFormat > 0 && object != nullptr) {
                for (typeCOL column = 0; column < object->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (lengths[column][VALUE_BEFORE] > 0) {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            processValue(object, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], compressedBefore);
                        } else {
                            payloadPB->add_before();
                            valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                            columnNull(object, column);
                        }
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    typeCOL column = base << 6;
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_BEFORE] != nullptr) {
                            if (lengths[column][VALUE_BEFORE] > 0) {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                processValue(object, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], compressedBefore);
                            } else {
                                payloadPB->add_before();
                                valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                                columnNull(object, column);
                            }
                        }
                    }
                }
            }
        }

        void createResponse(void) {
            if (redoResponsePB != nullptr) {
                RUNTIME_FAIL("PB commit processing failed, message already exists, internal error");
            }
            redoResponsePB = new pb::RedoResponse;
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class pb::RedoResponse) << " bytes memory (for: PB response7)");
            }
        }

        void numToString(uint64_t value, char* buf, uint64_t length);
        virtual void processInsert(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid);
        virtual void processUpdate(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid);
        virtual void processDelete(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid);
        virtual void processDDL(OracleObject* object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char* operation,
                const char* sql, uint64_t sqlLength);
        virtual void processBegin(void);
    public:
        OutputBufferProtobuf(uint64_t messageFormat, uint64_t ridFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat,
                uint64_t scnFormat, uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat, uint64_t unknownType, uint64_t flushBuffer);
        virtual ~OutputBufferProtobuf();

        virtual void initialize(OracleAnalyzer* oracleAnalyzer);
        virtual void processCommit(void);
        virtual void processCheckpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool redo);
    };
}

#endif
