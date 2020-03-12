/* Oracle Redo OpCode: 11.3
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
#include "OpCode0B03.h"
#include "OracleReader.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B03::OpCode0B03(OracleReader *oracleReader, RedoLogRecord *redoLogRecord) :
            OpCode(oracleReader, redoLogRecord) {
    }

    OpCode0B03::~OpCode0B03() {
    }

    void OpCode0B03::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktbRedo(fieldPos, fieldLength);
            } else if (i == 2) {
                kdoOpCode(fieldPos, fieldLength);
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }
}
