/* Memory buffer for handling output data in JSON format
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBufferProtobuf.h"
#include "RowId.h"
#include "RuntimeException.h"

namespace OpenLogReplicator {
    OutputBufferProtobuf::OutputBufferProtobuf(uint64_t messageFormat, uint64_t ridFormat, uint64_t xidFormat, uint64_t timestampFormat,
            uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat, uint64_t unknownType,
            uint64_t flushBuffer) :
        OutputBuffer(messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat, schemaFormat, columnFormat,
                unknownType, flushBuffer),
        redoResponsePB(nullptr),
        valuePB(nullptr),
        payloadPB(nullptr),
        schemaPB(nullptr) {

        GOOGLE_PROTOBUF_VERIFY_VERSION;
    }

    OutputBufferProtobuf::~OutputBufferProtobuf() {
        if (redoResponsePB != nullptr) {
            delete redoResponsePB;
            redoResponsePB = nullptr;
        }
        google::protobuf::ShutdownProtobufLibrary();
    }

    void OutputBufferProtobuf::columnNull(OracleObject* object, typeCOL col) {
        if (object != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
            OracleColumn* column = object->columns[col];
            if (column->storedAsLob)
                return;
            if (column->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                return;
            if (column->nested && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_NESTED_COLUMNS) == 0)
                return;
            if (column->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                return;
            if (column->unused && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_UNUSED_COLUMNS) == 0)
                return;

            uint64_t typeNo = object->columns[col]->typeNo;
            if (typeNo != 1 //varchar2/nvarchar2
                    && typeNo != 96 //char/nchar
                    && typeNo != 2 //number/float
                    && typeNo != 12 //date
                    && typeNo != 180 //timestamp
                    && typeNo != 23 //raw
                    && typeNo != 100 //binary_float
                    && typeNo != 101 //binary_double
                    && typeNo != 181) //timestamp with time zone
                return;
        }

        if (object != nullptr)
            valuePB->set_name(object->columns[col]->name);
        else {
            string columnName("COL_" + to_string(col));
            valuePB->set_name(columnName);
        }
    }

    void OutputBufferProtobuf::columnFloat(string& columnName, float value) {
        valuePB->set_name(columnName);
        valuePB->set_value_float(value);
    }

    void OutputBufferProtobuf::columnDouble(string& columnName, double value) {
        valuePB->set_name(columnName);
        valuePB->set_value_double(value);
    }

    void OutputBufferProtobuf::columnString(string& columnName) {
        valuePB->set_name(columnName);
        valuePB->set_value_string(valueBuffer, valueLength);
    }

    void OutputBufferProtobuf::columnNumber(string& columnName, uint64_t precision, uint64_t scale) {
        valuePB->set_name(columnName);
        valueBuffer[valueLength] = 0;
        char* retPtr;

        if (scale == 0 && precision <= 17) {
            int64_t value = strtol(valueBuffer, &retPtr, 10);
            valuePB->set_value_int(value);
        } else
        if (precision <= 6 && scale < 38)
        {
            float value = strtol(valueBuffer, &retPtr, 10);
            valuePB->set_value_float(value);
        } else
        if (precision <= 15 && scale <= 307)
        {
            double value = strtol(valueBuffer, &retPtr, 10);
            valuePB->set_value_double(value);
        } else {
            valuePB->set_value_string(valueBuffer, valueLength);
        }
    }

    void OutputBufferProtobuf::columnRaw(string& columnName, const uint8_t* data, uint64_t length) {
        valuePB->set_name(columnName);
    }

    void OutputBufferProtobuf::columnTimestamp(string& columnName, struct tm& time_, uint64_t fraction, const char* tz) {
        valuePB->set_name(columnName);
    }

    void OutputBufferProtobuf::appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot) {
        if ((messageFormat & MESSAGE_FORMAT_ADD_SEQUENCES) != 0)
            payloadPB->set_num(num);

        if (ridFormat == RID_FORMAT_SKIP)
            return;

        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        payloadPB->set_rid(str, 18);
    }

    void OutputBufferProtobuf::appendHeader(bool first, bool showXid) {
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
                lastTime.toISO8601(iso);
                redoResponsePB->set_tms(iso);
            } else {
                redoResponsePB->set_tm(lastTime.toTime() * 1000);
            }
        }

        if (showXid) {
            if (xidFormat == XID_FORMAT_TEXT) {
                stringstream sb;
                sb << (uint64_t)USN(lastXid);
                sb << '.';
                sb << (uint64_t)SLT(lastXid);
                sb << '.';
                sb << (uint64_t)SQN(lastXid);
                redoResponsePB->set_xid(sb.str());
            } else {
                redoResponsePB->set_xidn(lastXid);
            }
        }
    }

    void OutputBufferProtobuf::appendSchema(OracleObject* object, typeDATAOBJ dataObj) {
        if (object == nullptr) {
            string objectName("OBJ_" + to_string(dataObj));
            schemaPB->set_name(objectName);
            return;
        }

        schemaPB->set_owner(object->owner);
        schemaPB->set_name(object->name);

        if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0)
            schemaPB->set_obj(object->obj);

        if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
            if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                if (objects.count(object) > 0)
                    return;
                else
                    objects.insert(object);
            }

            schemaPB->add_column();
            pb::Column* columnPB = schemaPB->mutable_column(schemaPB->column_size() - 1);

            for (typeCOL column = 0; column < object->columns.size(); ++column) {
                if (object->columns[column] == nullptr)
                    continue;

                columnPB->set_name(object->columns[column]->name);

                switch(object->columns[column]->typeNo) {
                case 1: //varchar2(n), nvarchar2(n)
                    columnPB->set_type(pb::VARCHAR2);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 2: //number(p, s), float(p)
                    columnPB->set_type(pb::NUMBER);
                    columnPB->set_precision(object->columns[column]->precision);
                    columnPB->set_scale(object->columns[column]->scale);
                    break;

                case 8: //long, not supported
                    columnPB->set_type(pb::LONG);
                    break;

                case 12: //date
                    columnPB->set_type(pb::DATE);
                    break;

                case 23: //raw(n)
                    columnPB->set_type(pb::RAW);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 24: //long raw, not supported
                    columnPB->set_type(pb::LONG_RAW);
                    break;

                case 69: //rowid, not supported
                    columnPB->set_type(pb::ROWID);
                    break;

                case 96: //char(n), nchar(n)
                    columnPB->set_type(pb::CHAR);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 100: //binary float
                    columnPB->set_type(pb::BINARY_FLOAT);
                    break;

                case 101: //binary double
                    columnPB->set_type(pb::BINARY_DOUBLE);
                    break;

                case 112: //clob, nclob, not supported
                    columnPB->set_type(pb::CLOB);
                    break;

                case 113: //blob, not supported
                    columnPB->set_type(pb::BLOB);
                    break;

                case 180: //timestamp(n)
                    columnPB->set_type(pb::TIMESTAMP);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 181: //timestamp with time zone(n)
                    columnPB->set_type(pb::TIMESTAMP_WITH_TZ);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 182: //interval year to month(n)
                    columnPB->set_type(pb::INTERVAL_YEAR_TO_MONTH);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 183: //interval day to second(n)
                    columnPB->set_type(pb::INTERVAL_DAY_TO_SECOND);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 208: //urowid(n)
                    columnPB->set_type(pb::UROWID);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                case 231: //timestamp with local time zone(n), not supported
                    columnPB->set_type(pb::TIMESTAMP_WITH_LOCAL_TZ);
                    columnPB->set_length(object->columns[column]->length);
                    break;

                default:
                    columnPB->set_type(pb::UNKNOWN);
                    break;
                }

                columnPB->set_nullable(object->columns[column]->nullable);
            }
        }
    }

    void OutputBufferProtobuf::numToString(uint64_t value, char* buf, uint64_t length) {
        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            buf[i] = map16[(value >> j) & 0xF];
            j -= 4;
        };
        buf[length] = 0;
    }

    void OutputBufferProtobuf::processBegin(void) {
        newTran = false;
        outputBufferBegin(0);

        createResponse();
        appendHeader(true, true);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::BEGIN);

            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("PB begin processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit(false);
        }
    }

    void OutputBufferProtobuf::processCommit(void) {
        //skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("PB commit processing failed, message missing, internal error");
            }
        } else {
            outputBufferBegin(0);

            createResponse();
            appendHeader(true, true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::COMMIT);
        }

        string output;
        bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (!ret) {
            RUNTIME_FAIL("PB commit processing failed, error serializing to string");
        }
        outputBufferAppend(output);
        outputBufferCommit(true);

        num = 0;
    }

    void OutputBufferProtobuf::processInsert(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("PB insert processing failed, message missing, internal error");
            }
        } else {
            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);

            createResponse();
            appendHeader(true, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::INSERT);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(object, dataObj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(object);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("PB insert processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit(false);
        }
        ++num;
    }

    void OutputBufferProtobuf::processUpdate(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("PB update processing failed, message missing, internal error");
            }
        } else {
            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);

            createResponse();
            appendHeader(true, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::UPDATE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(object, dataObj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(object);
        appendAfter(object);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("PB update processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit(false);
        }
        ++num;
    }

    void OutputBufferProtobuf::processDelete(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("PB delete processing failed, message missing, internal error");
            }
        } else {

            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);

            createResponse();
            appendHeader(true, true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::DELETE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(object, dataObj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(object);

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("PB delete processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit(false);
        }
        ++num;
    }

    void OutputBufferProtobuf::processDDL(OracleObject* object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char* operation, const char* sql, uint64_t sqlLength) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("PB commit processing failed, message missing, internal error");
            }
        } else {
            createResponse();
            appendHeader(true, true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::DDL);
            payloadPB->set_ddl(sql, sqlLength);
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("PB commit processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit(true);
        }
        ++num;
    }

    void OutputBufferProtobuf::processCheckpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool redo) {
        createResponse();
        outputBufferBegin(0);
        appendHeader(true, true);

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::CHKPT);
        payloadPB->set_seq(sequence);
        payloadPB->set_offset(offset);
        payloadPB->set_redo(redo);

        string output;
        bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (!ret) {
            RUNTIME_FAIL("PB commit processing failed, error serializing to string");
        }
        outputBufferAppend(output);
        outputBufferCommit(true);
    }
}
