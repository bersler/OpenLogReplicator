/* Oracle Redo OpCode: 11.11
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

#include "OpCode0B0B.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B0B::OpCode0B0B(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyzer, redoLogRecord) {
    }

    OpCode0B0B::~OpCode0B0B() {
    }

    void OpCode0B0B::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktbRedo(fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        kdoOpCode(fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 3
        redoLogRecord->rowLenghsDelta = fieldPos;
        if (fieldLength < redoLogRecord->nrow * 2) {
            oracleAnalyzer->dumpStream << "field length list length too short: " << dec << fieldLength << endl;
            return;
        }

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 4
        redoLogRecord->rowData = fieldNum;
        dumpRows(redoLogRecord->data + fieldPos);
    }
}
