/* Oracle Redo OpCode: 5.6
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
#include "types.h"
#include "OpCode0506.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

    OpCode0506::OpCode0506(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
        OpCode(oracleEnvironment, redoLogRecord) {
    }

    OpCode0506::~OpCode0506() {
    }

    uint16_t OpCode0506::getOpCode(void) {
        return 0x0506;
    }

    void OpCode0506::process() {
        uint32_t fieldPosTmp = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktub(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);

                if (redoLogRecord->opc == 0x0B01)
                    ktubu(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            } else if (i == 2) {
                ktuxvoff(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            }
            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }

    const char* OpCode0506::getUndoType() {
        return "User undo done   ";
    }

    void OpCode0506::ktuxvoff(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 8) {
            oracleEnvironment->dumpStream << "too short field ktuxvoff: " << dec << fieldLength << endl;
            return;
        }

        if (oracleEnvironment->dumpLogFile) {
            uint32_t off = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 0);
            uint32_t flg = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 4);

            oracleEnvironment->dumpStream << "ktuxvoff: 0x" << setfill('0') << setw(4) << hex << off << " " <<
                    " ktuxvflg: 0x" << setfill('0') << setw(4) << hex << flg << endl;
        }
    }
}
