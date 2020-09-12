/* Memory buffer for handling output data in JSON format
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "CharacterSet.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBufferProtobuf.h"
#include "RuntimeException.h"
#include "Writer.h"

namespace OpenLogReplicator {

    OutputBufferProtobuf::OutputBufferProtobuf(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
            uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat) :
            OutputBuffer(messageFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat, schemaFormat, columnFormat)
#ifdef LINK_LIBRARY_PROTOBUF
            ,redo(nullptr),
            value(nullptr),
            payload(nullptr),
            schema(nullptr)

#endif /* LINK_LIBRARY_PROTOBUF */
    {
#ifdef LINK_LIBRARY_PROTOBUF
        GOOGLE_PROTOBUF_VERIFY_VERSION;
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    OutputBufferProtobuf::~OutputBufferProtobuf() {
#ifdef LINK_LIBRARY_PROTOBUF
        if (redo != nullptr)
            delete redo;
            redo = nullptr;
        google::protobuf::ShutdownProtobufLibrary();
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::numToString(uint64_t val, char *buf, uint64_t length) {
        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            buf[i] = map16[(val >> j) & 0xF];
            j -= 4;
        };
        buf[length] = 0;
    }

    void OutputBufferProtobuf::appendNull(string &columnName) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendUnknown(string &columnName, const uint8_t *data, uint64_t length) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
        //append("\"?\"");
        stringstream ss;
        for (uint64_t j = 0; j < length; ++j)
            ss << " " << hex << setfill('0') << setw(2) << (uint64_t) data[j];
        WARNING("unknown value (column: " << columnName << "): " << dec << length << " - " << ss.str());
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendNumber(string &columnName, const uint8_t *data, uint64_t length) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendFloat(string &columnName, float val) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendDouble(string &columnName, double val) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendString(string &columnName, const uint8_t *data, uint64_t length, uint64_t charsetId) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendTimestamp(string &columnName, struct tm &time, uint64_t fraction, const char *tz) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendRaw(string &columnName, const uint8_t *data, uint64_t length) {
#ifdef LINK_LIBRARY_PROTOBUF
        value->set_name(columnName);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
#ifdef LINK_LIBRARY_PROTOBUF
        uint32_t afn = bdba >> 22;
        bdba &= 0x003FFFFF;

        char rid[18];
        rid[0] = map64[(objd >> 30) & 0x3F];
        rid[1] = map64[(objd >> 24) & 0x3F];
        rid[2] = map64[(objd >> 18) & 0x3F];
        rid[3] = map64[(objd >> 12) & 0x3F];
        rid[4] = map64[(objd >> 6) & 0x3F];
        rid[5] = map64[objd & 0x3F];
        rid[6] = map64[(afn >> 12) & 0x3F];
        rid[7] = map64[(afn >> 6) & 0x3F];
        rid[8] = map64[afn & 0x3F];
        rid[9] = map64[(bdba >> 30) & 0x3F];
        rid[10] = map64[(bdba >> 24) & 0x3F];
        rid[11] = map64[(bdba >> 18) & 0x3F];
        rid[12] = map64[(bdba >> 12) & 0x3F];
        rid[13] = map64[(bdba >> 6) & 0x3F];
        rid[14] = map64[bdba & 0x3F];
        rid[15] = map64[(slot >> 12) & 0x3F];
        rid[16] = map64[(slot >> 6) & 0x3F];
        rid[17] = map64[slot & 0x3F];
        payload->set_rid(rid, 18);
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendHeader(bool first) {
#ifdef LINK_LIBRARY_PROTOBUF
        if (first || (scnFormat & SCN_FORMAT_ALL_PAYLOADS) != 0) {
            if ((scnFormat & SCN_FORMAT_HEX) != 0) {
                char buf[17];
                numToString(lastScn, buf, 16);
                redo->set_scns(buf);
            } else {
                redo->set_scn(lastScn);
            }
        }

        if (first || (timestampFormat & TIMESTAMP_FORMAT_ALL_PAYLOADS) != 0) {
            if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
                char iso[21];
                lastTime.toISO8601(iso);
                string isoStr(iso);
                redo->set_tms(isoStr);
            } else {
                redo->set_tm(lastTime.toTime() * 1000);
            }
        }

        if (xidFormat == XID_FORMAT_TEXT) {
            stringstream sb;
            sb << (uint64_t)USN(lastXid);
            sb << '.';
            sb << (uint64_t)SLT(lastXid);
            sb << '.';
            sb << (uint64_t)SQN(lastXid);
            redo->set_xid(sb.str());
        } else {
            redo->set_xidn(lastXid);
        }
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::appendSchema(OracleObject *object) {
#ifdef LINK_LIBRARY_PROTOBUF
        schema->set_owner(object->owner);
        schema->set_name(object->name);

        if ((schemaFormat & SCHEMA_FORMAT_OBJN) != 0)
            schema->set_objn(object->objn);

        if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
            if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                if (objects.count(object) > 0)
                    return;
                else
                    objects.insert(object);
            }

            schema->add_column();
            pb::Column *column = schema->mutable_column(schema->column_size() - 1);

            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                column->set_name(object->columns[i]->columnName);

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
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::processBegin(typescn scn, typetime time, typexid xid) {
#ifdef LINK_LIBRARY_PROTOBUF
        lastTime = time;
        lastScn = scn;
        lastXid = xid;
        beginMessage();

        if (redo != nullptr) {
            RUNTIME_FAIL("ERROR, PB begin processing failed, message already exists, internal error");
        }
        redo = new pb::Redo;
        appendHeader(true);

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            redo->add_payload();
            payload = redo->mutable_payload(redo->payload_size() - 1);
            payload->set_op(pb::BEGIN);

            string output;
            bool ret = redo->SerializeToString(&output);
            delete redo;
            redo = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB begin processing failed, error serializing to string");
            }
            append(output);
            commitMessage();
        }
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::processCommit(void) {
#ifdef LINK_LIBRARY_PROTOBUF
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redo == nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message missing, internal error");
            }
        } else {
            if (redo != nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message already exists, internal error");
            }
            redo = new pb::Redo;
            appendHeader(true);

            redo->add_payload();
            payload = redo->mutable_payload(redo->payload_size() - 1);
            payload->set_op(pb::COMMIT);
        }

        string output;
        bool ret = redo->SerializeToString(&output);
        delete redo;
        redo = nullptr;

        if (!ret) {
            RUNTIME_FAIL("ERROR, PB commit processing failed, error serializing to string");
        }
        append(output);
        commitMessage();
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::processInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
#ifdef LINK_LIBRARY_PROTOBUF
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redo == nullptr) {
                RUNTIME_FAIL("ERROR, PB insert processing failed, message missing, internal error");
            }
        } else {
            if (redo != nullptr) {
                RUNTIME_FAIL("ERROR, PB insert processing failed, message already exists, internal error");
            }
            beginMessage();
            redo = new pb::Redo;
            appendHeader(true);
        }

        redo->add_payload();
        payload = redo->mutable_payload(redo->payload_size() - 1);
        payload->set_op(pb::INSERT);

        schema = payload->mutable_schema();
        appendSchema(object);

        appendRowid(object->objn, object->objd, bdba, slot);

        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] != nullptr && afterLen[i] > 0) {
                payload->add_after();
                value = payload->mutable_after(payload->after_size() - 1);
                processValue(object->columns[i]->columnName, afterPos[i], afterLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            } else
            if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0) {
                payload->add_after();
                value = payload->mutable_after(payload->after_size() - 1);
                appendNull(object->columns[i]->columnName);
            }
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redo->SerializeToString(&output);
            delete redo;
            redo = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB insert processing failed, error serializing to string");
            }
            append(output);
            commitMessage();
        }
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::processUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
#ifdef LINK_LIBRARY_PROTOBUF
        checkUpdate(object, bdba, slot, xid);

        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redo == nullptr) {
                RUNTIME_FAIL("ERROR, PB update processing failed, message missing, internal error");
            }
        } else {
            if (redo != nullptr) {
                RUNTIME_FAIL("ERROR, PB update processing failed, message already exists, internal error");
            }
            beginMessage();
            redo = new pb::Redo;
            appendHeader(true);
        }

        redo->add_payload();
        payload = redo->mutable_payload(redo->payload_size() - 1);
        payload->set_op(pb::UPDATE);

        schema = payload->mutable_schema();
        appendSchema(object);

        appendRowid(object->objn, object->objd, bdba, slot);

        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (beforePos[i] != nullptr && beforeLen[i] > 0) {
                payload->add_before();
                value = payload->mutable_after(payload->before_size() - 1);
                processValue(object->columns[i]->columnName, beforePos[i], beforeLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            } else
            if (afterPos[i] != nullptr || beforePos[i] > 0) {
                payload->add_before();
                value = payload->mutable_after(payload->before_size() - 1);
                appendNull(object->columns[i]->columnName);
            }
        }

        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] != nullptr && afterLen[i] > 0) {
                payload->add_after();
                value = payload->mutable_after(payload->after_size() - 1);
                processValue(object->columns[i]->columnName, afterPos[i], afterLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            } else
            if (afterPos[i] > 0 || beforePos[i] > 0) {
                payload->add_after();
                value = payload->mutable_after(payload->after_size() - 1);
                appendNull(object->columns[i]->columnName);
            }
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redo->SerializeToString(&output);
            delete redo;
            redo = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB update processing failed, error serializing to string");
            }
            append(output);
            commitMessage();
        }
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
#ifdef LINK_LIBRARY_PROTOBUF
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redo == nullptr) {
                RUNTIME_FAIL("ERROR, PB delete processing failed, message missing, internal error");
            }
        } else {
            if (redo != nullptr) {
                RUNTIME_FAIL("ERROR, PB delete processing failed, message already exists, internal error");
            }
            beginMessage();
            redo = new pb::Redo;
            appendHeader(true);
        }

        redo->add_payload();
        payload = redo->mutable_payload(redo->payload_size() - 1);
        payload->set_op(pb::DELETE);

        schema = payload->mutable_schema();
        appendSchema(object);

        appendRowid(object->objn, object->objd, bdba, slot);

        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] != nullptr && beforeLen[i] > 0) {
                payload->add_before();
                value = payload->mutable_after(payload->before_size() - 1);
                processValue(object->columns[i]->columnName, beforePos[i], beforeLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            } else
            if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0) {
                payload->add_before();
                value = payload->mutable_after(payload->before_size() - 1);
                appendNull(object->columns[i]->columnName);
            }
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redo->SerializeToString(&output);
            delete redo;
            redo = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB delete processing failed, error serializing to string");
            }
            append(output);
            commitMessage();
        }
#endif /* LINK_LIBRARY_PROTOBUF */
    }

    void OutputBufferProtobuf::processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength) {
#ifdef LINK_LIBRARY_PROTOBUF
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (redo == nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message missing, internal error");
            }
        } else {
            if (redo != nullptr) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, message already exists, internal error");
            }
            redo = new pb::Redo;
            appendHeader(true);

            redo->add_payload();
            payload = redo->mutable_payload(redo->payload_size() - 1);
            payload->set_op(pb::DDL);
            payload->set_ddl(sql, sqlLength);
        }

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            string output;
            bool ret = redo->SerializeToString(&output);
            delete redo;
            redo = nullptr;

            if (!ret) {
                RUNTIME_FAIL("ERROR, PB commit processing failed, error serializing to string");
            }
            append(output);
        }
        commitMessage();
#endif /* LINK_LIBRARY_PROTOBUF */
    }
}
