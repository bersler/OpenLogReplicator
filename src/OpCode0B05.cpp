/* Oracle Redo OpCode: 11.5
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <iomanip>
#include <iostream>

#include "OpCode0501.h"
#include "OpCode0B05.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B05::OpCode0B05(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord) {
    }

    OpCode0B05::~OpCode0B05() {
    }

    void OpCode0B05::process() {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktbRedo(fieldPos, fieldLength);

        if (!oracleAnalyser->hasNextField(redoLogRecord, fieldNum))
            return;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 2
        kdoOpCode(fieldPos, fieldLength);
        redoLogRecord->nullsDelta = fieldPos + 26;
        uint8_t *nulls = redoLogRecord->data + redoLogRecord->nullsDelta;

        if (!oracleAnalyser->hasNextField(redoLogRecord, fieldNum))
            return;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 3
        redoLogRecord->colNumsDelta = fieldPos;
        uint8_t *colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;

        if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
            oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            //field: 4
            redoLogRecord->rowData = fieldNum;
            if (oracleAnalyser->dumpRedoLog >= 1)
                dumpColsVector(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength);
        } else {
            redoLogRecord->rowData = fieldNum + 1;
            uint8_t bits = 1;

            //fields: 4 + cc .. 4 + cc - 1
            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {
                oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);

                if (oracleAnalyser->dumpRedoLog >= 1)
                    dumpCols(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength, *nulls & bits);
                bits <<= 1;
                colNums += 2;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
        }
    }
}
