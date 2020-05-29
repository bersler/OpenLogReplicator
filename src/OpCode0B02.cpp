/* Oracle Redo OpCode: 11.2
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

#include "CommandBuffer.h"
#include "OpCode0B02.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B02::OpCode0B02(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord) {
    }

    OpCode0B02::~OpCode0B02() {
    }

    void OpCode0B02::process() {
        OpCode::process();
        uint8_t *nulls, bits = 1;
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktbRedo(fieldPos, fieldLength);
            } else if (i == 2) {
                kdoOpCode(fieldPos, fieldLength);
                redoLogRecord->nullsDelta = fieldPos + 45;
                nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            } else if (i > 2 && i <= 2 + (uint64_t)redoLogRecord->cc) {
                if (oracleAnalyser->dumpRedoLog >= 1) {
                    dumpCols(redoLogRecord->data + fieldPos, i - 3, fieldLength, *nulls & bits);
                }
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }
}
