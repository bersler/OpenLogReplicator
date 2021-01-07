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

    OutputBufferProtobuf::OutputBufferProtobuf(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
            uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat) :
            OutputBuffer(messageFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat, schemaFormat, columnFormat),
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

    void OutputBufferProtobuf::columnNull(OracleObject *object, typeCOL col) {
        if (object != nullptr)
            valuePB->set_name(object->columns[col]->name);
        else {
            string columnName = "COL_" + to_string(col);
            valuePB->set_name(columnName);
        }
    }

    void OutputBufferProtobuf::columnFloat(string &columnName, float value) {
        valuePB->set_name(columnName);
        valuePB->set_value_float(value);
    }

    void OutputBufferProtobuf::columnDouble(string &columnName, double value) {
        valuePB->set_name(columnName);
        valuePB->set_value_double(value);
    }

    void OutputBufferProtobuf::columnString(string &columnName) {
        valuePB->set_name(columnName);
        valuePB->set_value_string(valueBuffer, valueLength);
    }

    void OutputBufferProtobuf::columnNumber(string &columnName, uint64_t precision, uint64_t scale) {
        valuePB->set_name(columnName);
        valueBuffer[valueLength] = 0;
        char *retPtr;

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

    void OutputBufferProtobuf::columnRaw(string &columnName, const uint8_t *data, uint64_t length) {
        valuePB->set_name(columnName);
    }

    void OutputBufferProtobuf::columnTimestamp(string &columnName, struct tm &time, uint64_t fraction, const char *tz) {
        valuePB->set_name(columnName);
    }

    void OutputBufferProtobuf::appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        payloadPB->set_rid(str, 18);
    }

    void OutputBufferProtobuf::appendHeader(bool first) {
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
                string isoStr(iso);
                redoResponsePB->set_tms(isoStr);
            } else {
                redoResponsePB->set_tm(lastTime.toTime() * 1000);
            }
        }

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

    void OutputBufferProtobuf::appendSchema(OracleObject *object, typeDATAOBJ dataObj) {
        if (object == nullptr) {
            string objectName = "OBJ_" + to_string(dataObj);
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
            pb::Column *column = schemaPB->mutable_column(schemaPB->column_size() - 1);

            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                column->set_name(object->columns[i]->name);

                switch(object->columns[i]->typeNo) {
                case 1: //varchar2(n), nvarchar2(n)
                    column->set_type(pb::VARCHAR2);
                    column->set_length(object->columns[i]->length);
                    break;

                case 2: //number(p, s), float(p)
                    column->set_type(pb::NUMBER);
                    column->set_precision(object->columns[i]->precision);
                    column->set_scale(object->columns[i]->scale);
                    break;

                case 8: //long, not supported
                    column->set_type(pb::LONG);
                    break;

                case 12: //date
                    column->set_type(pb::DATE);
                    break;

                case 23: //raw(n)
                    column->set_type(pb::RAW);
                    column->set_length(object->columns[i]->length);
                    break;

                case 24: //long raw, not supported
                    column->set_type(pb::LONG_RAW);
                    break;

                case 69: //rowid, not supported
                    column->set_type(pb::ROWID);
                    break;

                case 96: //char(n), nchar(n)
                    column->set_type(pb::CHAR);
                    column->set_length(object->columns[i]->length);
                    break;

                case 100: //binary float
                    column->set_type(pb::BINARY_FLOAT);
                    break;

                case 101: //binary double
                    column->set_type(pb::BINARY_DOUBLE);
                    break;

                case 112: //clob, nclob, not supported
                    column->set_type(pb::CLOB);
                    break;

                case 113: //blob, not supported
                    column->set_type(pb::BLOB);
                    break;

                case 180: //timestamp(n)
                    column->set_type(pb::TIMESTAMP);
                    column->set_length(object->columns[i]->length);
                    break;

                case 181: //timestamp with time zone(n)
                    column->set_type(pb::TIMESTAMP_WITH_TZ);
                    column->set_length(object->columns[i]->length);
                    break;

                case 182: //interval year to month(n)
                    column->set_type(pb::INTERVAL_YEAR_TO_MONTH);
                    column->set_length(object->columns[i]->length);
                    break;

                case 183: //interval day to second(n)
                    column->set_type(pb::INTERVAL_DAY_TO_SECOND);
                    column->set_length(object->columns[i]->length);
                    break;

                case 208: //urowid(n)
                    column->set_type(pb::UROWID);
                    column->set_length(object->columns[i]->length);
                    break;

                case 231: //timestamp with local time zone(n), not supported
                    column->set_type(pb::TIMESTAMP_WITH_LOCAL_TZ);
                    column->set_length(object->columns[i]->length);
                    break;

                default:
                    column->set_type(pb::UNKNOWN);
                    break;
                }

                column->set_nullable(object->columns[i]->nullable);
            }
        }
    }

    void OutputBufferProtobuf::numToString(uint64_t value, char *buf, uint64_t length) {
        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            buf[i] = map16[(value >> j) & 0xF];
            j -= 4;
        };
        buf[length] = 0;
    }

    void OutputBufferProtobuf::processBegin(typeSCN scn, typetime time, typeXID xid) {
        lastTime = time;
        lastScn = scn;
        lastXid = xid;
        outputBufferBegin(0);

        if (redoResponsePB != nullptr) {
            RUNTIME_FAIL("ERROR, PB begin processing failed, message already exists, internal error");
        }
        redoResponsePB = new pb::RedoResponse;
        appendHeader(true);

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::BEGIN);

            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB begin processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit();
        }
    }

    void OutputBufferProtobuf::processCommit(void) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message missing, internal error");
            }
        } else {
            if (redoResponsePB != nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message already exists, internal error");
            }
            outputBufferBegin(0);
            redoResponsePB = new pb::RedoResponse;
            appendHeader(true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::COMMIT);
        }

        string output;
        bool ret = redoResponsePB->SerializeToString(&output);
        delete redoResponsePB;
        redoResponsePB = nullptr;

        if (!ret) {
            RUNTIME_FAIL("ERROR, PB commit processing failed, error serializing to string");
        }
        outputBufferAppend(output);
        outputBufferCommit();
    }

    void OutputBufferProtobuf::processInsert(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("ERROR, PB insert processing failed, message missing, internal error");
            }
        } else {
            if (redoResponsePB != nullptr) {
                RUNTIME_FAIL("ERROR, PB insert processing failed, message already exists, internal error");
            }

            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);

            redoResponsePB = new pb::RedoResponse;
            appendHeader(true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::INSERT);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(object,dataObj);
        appendRowid(dataObj, bdba, slot);

        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    processValue(object, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                } else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    columnNull(object, i);
                }
            } else {
                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    processValue(nullptr, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], 0, 0);
                } else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    columnNull(nullptr, i);
                }
            }
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB insert processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit();
        }
    }

    void OutputBufferProtobuf::processUpdate(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("ERROR, PB update processing failed, message missing, internal error");
            }
        } else {
            if (redoResponsePB != nullptr) {
                RUNTIME_FAIL("ERROR, PB update processing failed, message already exists, internal error");
            }

            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);

            redoResponsePB = new pb::RedoResponse;
            appendHeader(true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::UPDATE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(object, dataObj);

        appendRowid(dataObj, bdba, slot);

        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    processValue(object, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                } else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    columnNull(object, i);
                }
            } else {
                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    processValue(nullptr, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], 0, 0);
                } else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    columnNull(nullptr, i);
                }
            }
        }

        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    processValue(object, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                } else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] != nullptr) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    columnNull(object, i);
                }
            } else {
                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    processValue(nullptr, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], 0, 0);
                } else
                if (values[pos][VALUE_AFTER].data[0] != nullptr) {
                    payloadPB->add_after();
                    valuePB = payloadPB->mutable_after(payloadPB->after_size() - 1);
                    columnNull(nullptr, i);
                }
            }
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB update processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit();
        }
    }

    void OutputBufferProtobuf::processDelete(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("ERROR, PB delete processing failed, message missing, internal error");
            }
        } else {
            if (redoResponsePB != nullptr) {
                RUNTIME_FAIL("ERROR, PB delete processing failed, message already exists, internal error");
            }

            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);
            redoResponsePB = new pb::RedoResponse;
            appendHeader(true);
        }

        redoResponsePB->add_payload();
        payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
        payloadPB->set_op(pb::DELETE);

        schemaPB = payloadPB->mutable_schema();
        appendSchema(object, dataObj);

        appendRowid(dataObj, bdba, slot);

        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    processValue(object, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                } else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    columnNull(object, i);
                }
            } else {
                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    processValue(nullptr, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], 0, 0);
                } else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC) {
                    payloadPB->add_before();
                    valuePB = payloadPB->mutable_before(payloadPB->before_size() - 1);
                    columnNull(nullptr, i);
                }
            }
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB delete processing failed, error serializing to string");
            }
            outputBufferAppend(output);
            outputBufferCommit();
        }
    }

    void OutputBufferProtobuf::processDDL(OracleObject *object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redoResponsePB == nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message missing, internal error");
            }
        } else {
            if (redoResponsePB != nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message already exists, internal error");
            }
            redoResponsePB = new pb::RedoResponse;
            appendHeader(true);

            redoResponsePB->add_payload();
            payloadPB = redoResponsePB->mutable_payload(redoResponsePB->payload_size() - 1);
            payloadPB->set_op(pb::DDL);
            payloadPB->set_ddl(sql, sqlLength);
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redoResponsePB->SerializeToString(&output);
            delete redoResponsePB;
            redoResponsePB = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, error serializing to string");
            }
            outputBufferAppend(output);
        }
        outputBufferCommit();
    }
}
