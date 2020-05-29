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
        uint8_t *colNums, *nulls, bits = 1;
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktbRedo(fieldPos, fieldLength);
            } else if (i == 2) {
                kdoOpCode(fieldPos, fieldLength);
                redoLogRecord->nullsDelta = fieldPos + 26;
                nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            } else if (i == 3) {
                redoLogRecord->colNumsDelta = fieldPos;
                colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
            } else if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
                if (i == 4)
                    if (oracleAnalyser->dumpRedoLog >= 1)
                        dumpColsVector(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength);
            } else {
                if (i > 3 && i <= 3 + (uint64_t)redoLogRecord->cc) {
                    if (oracleAnalyser->dumpRedoLog >= 1) {
                        dumpCols(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength, *nulls & bits);
                    }
                    bits <<= 1;
                    colNums += 2;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }
}
