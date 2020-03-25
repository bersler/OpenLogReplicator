/* Oracle Redo OpCode: 5.6
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
#include "OpCode0506.h"
#include "OracleReader.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0506::OpCode0506(OracleReader *oracleReader, RedoLogRecord *redoLogRecord) :
            OpCode(oracleReader, redoLogRecord) {

        uint64_t fieldPos = redoLogRecord->fieldPos;
        uint16_t fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + 1 * 2);
        if (fieldLength < 8) {
            oracleReader->dumpStream << "ERROR: too short field ktub: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->objn = oracleReader->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->objd = oracleReader->read32(redoLogRecord->data + fieldPos + 4);
    }

    OpCode0506::~OpCode0506() {
    }

    void OpCode0506::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktub(fieldPos, fieldLength);
            } else if (i == 2) {
                ktuxvoff(fieldPos, fieldLength);
            }
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }

    const char* OpCode0506::getUndoType() {
        return "User undo done   ";
    }

    void OpCode0506::ktuxvoff(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 8) {
            oracleReader->dumpStream << "too short field ktuxvoff: " << dec << fieldLength << endl;
            return;
        }

        if (oracleReader->dumpRedoLog >= 1) {
            uint16_t off = oracleReader->read16(redoLogRecord->data + fieldPos + 0);
            uint16_t flg = oracleReader->read16(redoLogRecord->data + fieldPos + 4);

            oracleReader->dumpStream << "ktuxvoff: 0x" << setfill('0') << setw(4) << hex << off << " " <<
                    " ktuxvflg: 0x" << setfill('0') << setw(4) << hex << flg << endl;
        }
    }
}
