/* Oracle Redo OpCode: 5.11
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

#include "OpCode050B.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {
    OpCode050B::OpCode050B(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord) :
        OpCode(oracleAnalyzer, redoLogRecord) {

        if (redoLogRecord->fieldCnt >= 1) {
            uint64_t fieldPos = redoLogRecord->fieldPos;
            uint16_t fieldLength = oracleAnalyzer->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + 1 * 2);
            if (fieldLength < 8) {
                oracleAnalyzer->dumpStream << "ERROR: too short field ktub: " << dec << fieldLength << endl;
                return;
            }

            redoLogRecord->obj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
            redoLogRecord->dataObj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4);
        }
    }

    OpCode050B::~OpCode050B() {
    }

    void OpCode050B::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktub(fieldPos, fieldLength);
        redoLogRecord->opFlags |= OPFLAG_BEGIN_TRANS;
    }

    const char* OpCode050B::getUndoType(void) const {
        return "User undo done    Begin trans    ";
    }
}
