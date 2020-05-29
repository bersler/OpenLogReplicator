/* Oracle Redo OpCode: 11.12
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

#include "OpCode0B0C.h"
#include "OracleAnalyser.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B0C::OpCode0B0C(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord) {
    }

    OpCode0B0C::~OpCode0B0C() {
    }

    void OpCode0B0C::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktbRedo(fieldPos, oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2));
            } else if (i == 2) {
                kdoOpCode(fieldPos, oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2));

                if (oracleAnalyser->dumpRedoLog >= 1) {
                    if ((redoLogRecord->op & 0x1F) == OP_QMD) {
                        for (uint64_t i = 0; i < redoLogRecord->nrow; ++i)
                            oracleAnalyser->dumpStream << "slot[" << i << "]: " << dec << oracleAnalyser->read16(redoLogRecord->data+redoLogRecord->slotsDelta + i * 2) << endl;
                    }
                }
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }
}
