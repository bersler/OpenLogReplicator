/* Memory buffer for handling output data in JSON-DBZ format
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
#include "OutputBufferJsonDbz.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Writer.h"

namespace OpenLogReplicator {

    OutputBufferJsonDbz::OutputBufferJsonDbz(uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat,
            uint64_t showColumns) :
                OutputBufferJson(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns) {
    }

    OutputBufferJsonDbz::~OutputBufferJsonDbz() {
    }

    void OutputBufferJsonDbz::appendDbzCols(OracleObject *object) {
        for (uint64_t i = 0; i < object->columns.size(); ++i) {
            bool microTimestamp = false;

            if (object->columns[i] == nullptr)
                continue;

            if (i > 0)
                append(',');

            appendChr("{\"type\":\"");
            switch(object->columns[i]->typeNo) {
            case 1: //varchar(2)
            case 96: //char
                appendChr("string");
                break;

            case 2: //numeric
                if (object->columns[i]->scale > 0)
                    appendChr("Decimal");
                else {
                    uint64_t digits = object->columns[i]->precision - object->columns[i]->scale;
                    if (digits < 3)
                        appendChr("int8");
                    else if (digits < 5)
                        appendChr("int16");
                    else if (digits < 10)
                        appendChr("int32");
                    else if (digits < 19)
                        appendChr("int64");
                    else
                        appendChr("Decimal");
                }
                break;

            case 12:
            case 180:
                if (timestampFormat == 0 || timestampFormat == 1)
                    appendChr("datetime");
                else if (timestampFormat == 2) {
                    appendChr("int64");
                    microTimestamp = true;
                }
                break;
            }
            appendChr("\",\"optional\":");
            if (object->columns[i]->nullable)
                appendChr("true");
            else
                appendChr("false");

            if (microTimestamp)
                appendChr(",\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1");
            appendChr(",\"field\":\"");
            appendStr(object->columns[i]->columnName);
            appendChr("\"}");
        }
    }

    void OutputBufferJsonDbz::appendDbzHead(OracleObject *object) {
        appendChr("{\"schema\":{\"type\":\"struct\",\"fields\":[");
        appendChr("{\"type\":\"struct\",\"fields\":[");
        appendDbzCols(object);
        appendChr("],\"optional\":true,\"name\":\"");
        appendStr(oracleAnalyser->alias);
        append('.');
        appendStr(object->owner);
        append('.');
        appendStr(object->objectName);
        appendChr(".Value\",\"field\":\"before\"},");
        appendChr("{\"type\":\"struct\",\"fields\":[");
        appendDbzCols(object);
        appendChr("],\"optional\":true,\"name\":\"");
        appendStr(oracleAnalyser->alias);
        append('.');
        appendStr(object->owner);
        append('.');
        appendStr(object->objectName);
        appendChr(".Value\",\"field\":\"after\"},"
                "{\"type\":\"struct\",\"fields\":["
                "{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},"
                "{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},"
                "{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"txId\"},"
                "{\"type\":\"int64\",\"optional\":true,\"field\":\"scn\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"lcr_position\"}],"
                "\"optional\":false,\"name\":\"io.debezium.connector.oracle.Source\",\"field\":\"source\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},"
                "{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},"
                "{\"type\":\"struct\",\"fields\":["
                "{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},"
                "{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},"
                "{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"messagetopic\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"messagesource\"}],\"optional\":false,\"name\":\"asgard.DEBEZIUM.CUSTOMERS.Envelope\"},\"payload\":{");
    }

    void OutputBufferJsonDbz::appendDbzTail(OracleObject *object, uint64_t time, typescn scn, char op, typexid xid) {
        appendChr(",\"source\":{\"version\":\"" PROGRAM_VERSION "\",\"connector\":\"oracle\",\"name\":\"");
        appendStr(oracleAnalyser->alias);
        appendChr("\",");
        appendMs("ts_ms", time);
        appendChr(",\"snapshot\":\"false\",\"db\":\"");
        appendStr(oracleAnalyser->databaseContext);
        appendChr("\",\"schema\":\"");
        appendStr(object->owner);
        appendChr("\",\"table\":\"");
        appendStr(object->objectName);
        appendChr("\",\"txId\":\"");
        appendDec(USN(xid));
        append('.');
        appendDec(SLT(xid));
        append('.');
        appendDec(SQN(xid));
        appendChr("\",");
        appendScn(scn);
        appendChr(",\"lcr_position\":null},\"op\":\"");
        append(op);
        appendChr("\",");
        appendMs("ts_ms", time);
        appendChr(",\"transaction\":null,\"messagetopic\":\"");
        appendStr(oracleAnalyser->alias);
        append('.');
        appendStr(object->owner);
        append('.');
        appendStr(object->objectName);
        appendChr("\",\"messagesource\":\"OpenLogReplicator from Oracle on ");
        appendStr(oracleAnalyser->alias);
        appendChr("\"}}");
    }

    void OutputBufferJsonDbz::appendInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        beginMessage();
        appendDbzHead(object);
        appendChr("\"before\":{},\"after\":{");

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] > 0 && afterLen[i] > 0)
                appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, afterPos[i], afterLen[i], prevValue);
            else
            if (showColumns >= 1 || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        append('}');
        appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'c', xid);
        commitMessage();
    }

    void OutputBufferJsonDbz::appendUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        OutputBuffer::appendUpdate(object, bdba, slot, xid);

        beginMessage();
        appendDbzHead(object);
        appendChr("\"before\":{");

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] > 0 && beforeLen[i] > 0)
                appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, beforePos[i], beforeLen[i], prevValue);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        appendChr("},\"after\":{");

        prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] > 0 && afterLen[i] > 0)
                appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, afterPos[i], afterLen[i], prevValue);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        append('}');
        appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'u', xid);
        commitMessage();
    }

    void OutputBufferJsonDbz::appendDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        beginMessage();
        appendDbzHead(object);
        appendChr("\"before\":{");

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] > 0 && beforeLen[i] > 0)
                appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, beforePos[i], beforeLen[i], prevValue);
            else
            if (showColumns >= 1 || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        appendChr("},\"after\":{}");
        appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'd', xid);
        commitMessage();
    }

    void OutputBufferJsonDbz::appendDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) {
    }
}
