/* Memory buffer for handling output data in JSON format for internal testing purposes
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
#include "OutputBufferJsonTest.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Writer.h"

namespace OpenLogReplicator {

    OutputBufferJsonTest::OutputBufferJsonTest(uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat,
            uint64_t showColumns) :
            OutputBufferJson(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns) {
    }

    OutputBufferJsonTest::~OutputBufferJsonTest() {
    }

    void OutputBufferJsonTest::appendInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        append('\n');
        append('{');
        appendScn(lastScn);
        append(',');
        appendOperation("insert");
        append(',');
        appendTable(object->owner, object->objectName);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        appendChr(",\"after\":{");

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

        appendChr("}}");
    }

    void OutputBufferJsonTest::appendUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        OutputBuffer::appendUpdate(object, bdba, slot, xid);

        append('\n');
        append('{');
        appendScn(lastScn);
        append(',');
        appendOperation("update");
        append(',');
        appendTable(object->owner, object->objectName);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        appendChr(",\"before\":{");

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

        appendChr("}}");
    }

    void OutputBufferJsonTest::appendDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        append('\n');
        append('{');
        appendScn(lastScn);
        append(',');
        appendOperation("delete");
        append(',');
        appendTable(object->owner, object->objectName);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        appendChr(",\"before\":{");

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

        appendChr("}}");
    }

    void OutputBufferJsonTest::appendDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) {
        append('\n');
        append('{');
        appendScn(lastScn);
        append(',');
        appendTable(object->owner, object->objectName);
        appendChr(",\"type\":");
        appendDec(type);
        appendChr(",\"seq\":");
        appendDec(seq);
        append(',');
        appendOperation(operation);
        appendChr(",\"sql\":\"");
        appendEscape(sql, sqlLength);
        appendChr("\"}");
    }

    void OutputBufferJsonTest::next(void) {
    }

    void OutputBufferJsonTest::commitTran(void) {
        commitMessage();
    }
}
