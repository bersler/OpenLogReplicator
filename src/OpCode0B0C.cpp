/* Oracle Redo OpCode: 11.12
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
along with Open Log Replicator; see the file LICENSE;  If not see
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

    void OpCode0B0C::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktbRedo(fieldPos, fieldLength);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        kdoOpCode(fieldPos, fieldLength);

        if (oracleAnalyser->dumpRedoLog >= 1) {
            if ((redoLogRecord->op & 0x1F) == OP_QMD) {
                for (uint64_t i = 0; i < redoLogRecord->nrow; ++i)
                    oracleAnalyser->dumpStream << "slot[" << i << "]: " << dec << oracleAnalyser->read16(redoLogRecord->data+redoLogRecord->slotsDelta + i * 2) << endl;
            }
        }
    }
}
