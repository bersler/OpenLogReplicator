/* Oracle Redo OpCode: 5.14
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
#include "OpCode0513.h"
#include "OpCode0514.h"

#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0514::OpCode0514(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode0513(oracleAnalyser, redoLogRecord) {
    }

    OpCode0514::~OpCode0514() {
    }

    void OpCode0514::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;

        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);

            if (i == 1) dumpMsgSessionSerial(fieldPos, fieldLength);
            else
            if (i == 2) dumpVal(fieldPos, fieldLength, "transaction name = ");
            else
            if (i == 3) dumpMsgFlags(fieldPos, fieldLength);
            else
            if (i == 4) dumpMsgVersion(fieldPos, fieldLength);
            else
            if (i == 5) dumpMsgAuditSessionid(fieldPos, fieldLength);
            else
            if (i == 7) dumpVal(fieldPos, fieldLength, "Client Id = ");
            else
            if (i == 8) dumpVal(fieldPos, fieldLength, "login   username = ");

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }
}
