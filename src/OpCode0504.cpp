/* Oracle Redo OpCode: 5.4
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
#include "OpCode0504.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0504::OpCode0504(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
        OpCode(oracleEnvironment, redoLogRecord) {
    }

    OpCode0504::~OpCode0504() {
    }

    void OpCode0504::process() {
        OpCode::process();
        uint32_t fieldPos = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktucm(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            } else if (i == 2) {
                if ((redoLogRecord->flg & 0x02) == 0x02)
                    ktucf(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            }
            fieldPos += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }

        if (oracleEnvironment->dumpLogFile) {
            oracleEnvironment->dumpStream << endl;
            if ((redoLogRecord->flg & 0x04) == 0x04)
                oracleEnvironment->dumpStream << "rolled back transaction" << endl;
        }
    }

    void OpCode0504::ktucm(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 20) {
            oracleEnvironment->dumpStream << "too short field ktucm: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(redoLogRecord->usn,
                oracleEnvironment->read16(redoLogRecord->data + fieldPos + 0),
                oracleEnvironment->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->flg = redoLogRecord->data[fieldPos + 16];

        if (oracleEnvironment->dumpLogFile) {
            uint16_t srt = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 6);
            uint32_t sta = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12);

            oracleEnvironment->dumpStream << "ktucm redo: slt: 0x" << setfill('0') << setw(4) << hex << SLT(redoLogRecord->xid) <<
                    " sqn: 0x" << setfill('0') << setw(8) << hex << SQN(redoLogRecord->xid) <<
                    " srt: " << dec << srt <<
                    " sta: " << dec << sta <<
                    " flg: 0x" << hex << redoLogRecord->flg << " ";
        }
    }

    void OpCode0504::ktucf(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 16) {
            oracleEnvironment->dumpStream << "too short field ktucf: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->uba = oracleEnvironment->read56(redoLogRecord->data + fieldPos + 0);

        if (oracleEnvironment->dumpLogFile) {
            uint16_t ext = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 8);
            uint16_t spc = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 10);
            uint8_t fbi = redoLogRecord->data[fieldPos + 12];

            oracleEnvironment->dumpStream << "ktucf redo:" <<
                    " uba: " << PRINTUBA(redoLogRecord->uba) <<
                    " ext: " << dec << ext <<
                    " spc: " << dec << spc <<
                    " fbi: " << dec << (uint32_t)fbi <<
                    " ";
        }
    }
}
