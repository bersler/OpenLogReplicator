/* Oracle Redo OpCode: 11.2
   Copyright (C) 2018-2019 Adam Leszczynski.

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

#include <iostream>
#include <iomanip>
#include "OpCode0B02.h"
#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B02::OpCode0B02(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
            OpCode(oracleEnvironment, redoLogRecord) {
    }

    OpCode0B02::~OpCode0B02() {
    }

    uint16_t OpCode0B02::getOpCode(void) {
        return 0x0B02;
    }

    void OpCode0B02::process() {
        uint8_t *nulls, bits = 1;
        uint32_t fieldPos = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktbRedo(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            } else if (i == 2) {
                kdoOpCode(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                redoLogRecord->nullsDelta = fieldPos + 45;
                nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            } else if (i > 2 && i <= 2 + (uint32_t)redoLogRecord->cc) {
                if (oracleEnvironment->dumpLogFile) {
                    dumpCols(redoLogRecord->data + fieldPos, i - 3, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i], *nulls & bits);
                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            fieldPos += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }
}
