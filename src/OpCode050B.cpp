/* Oracle Redo OpCode: 5.11
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

#include <iostream>
#include <iomanip>
#include "types.h"
#include "OpCode050B.h"

#include "OracleAnalyser.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode050B::OpCode050B(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord) {

        if (redoLogRecord->fieldCnt >= 1) {
            uint64_t fieldPos = redoLogRecord->fieldPos;
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + 1 * 2);
            if (fieldLength < 8) {
                oracleAnalyser->dumpStream << "ERROR: too short field ktub: " << dec << fieldLength << endl;
                return;
            }

            redoLogRecord->objn = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
            redoLogRecord->objd = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 4);
        }
    }

    OpCode050B::~OpCode050B() {
    }

    void OpCode050B::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktub(fieldPos, fieldLength);
                redoLogRecord->opFlags |= OPFLAG_BEGIN_TRANS;
            }
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }

    const char* OpCode050B::getUndoType() {
        return "User undo done    Begin trans    ";
    }
}
