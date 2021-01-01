/* Oracle Redo OpCode: 18.1
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

#include "OpCode1801.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode1801::OpCode1801(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyzer, redoLogRecord),
            validDDL(false),
            type(0) {
    }

    OpCode1801::~OpCode1801() {
    }

    void OpCode1801::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        redoLogRecord->xid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 4),
                oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 6),
                oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 8));
        type = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 12);
        uint16_t tmp = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);
        //uint16_t seq = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 18);
        //uint16_t cnt = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 20);
        if (type == 85 // truncate table
                //|| type == 1 //create table
                || type == 12 // drop table
                || type == 15 // alter table
                || type == 86 // truncate partition
        )
            validDDL = true;

        //temporary object
        if (tmp == 4 || tmp == 5 || tmp == 6 || tmp == 8 || tmp == 9 || tmp == 10) {
            validDDL = false;
        }

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 3

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 4

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 5

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 6

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 7

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 8

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 9

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 10

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 11

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 12

        if (validDDL && redoLogRecord->scn > oracleAnalyzer->scn)
            redoLogRecord->obj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
    }
}
