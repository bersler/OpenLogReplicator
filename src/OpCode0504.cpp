/* Oracle Redo OpCode: 5.4
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

#include "OpCode0504.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0504::OpCode0504(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord) :
        OpCode(oracleAnalyzer, redoLogRecord) {
    }

    OpCode0504::~OpCode0504() {
    }

    void OpCode0504::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktucm(fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        if ((redoLogRecord->flg & FLG_KTUCF_OP0504) != 0)
            ktucf(fieldPos, fieldLength);

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << endl;
            if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
                oracleAnalyzer->dumpStream << "rolled back transaction" << endl;
        }
    }

    void OpCode0504::ktucm(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            oracleAnalyzer->dumpStream << "too short field ktucm: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(redoLogRecord->usn,
                oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 0),
                oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->flg = redoLogRecord->data[fieldPos + 16];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint16_t srt = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8);  //to check
            uint32_t sta = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12);

            oracleAnalyzer->dumpStream << "ktucm redo: slt: 0x" << setfill('0') << setw(4) << hex << SLT(redoLogRecord->xid) <<
                    " sqn: 0x" << setfill('0') << setw(8) << hex << SQN(redoLogRecord->xid) <<
                    " srt: " << dec << srt <<
                    " sta: " << dec << sta <<
                    " flg: 0x" << hex << redoLogRecord->flg << " ";
        }
    }

    void OpCode0504::ktucf(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 16) {
            oracleAnalyzer->dumpStream << "too short field ktucf: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 0);

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint16_t ext = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8);
            uint16_t spc = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 10);
            uint8_t fbi = redoLogRecord->data[fieldPos + 12];

            oracleAnalyzer->dumpStream << "ktucf redo:" <<
                    " uba: " << PRINTUBA(redoLogRecord->uba) <<
                    " ext: " << dec << ext <<
                    " spc: " << dec << spc <<
                    " fbi: " << dec << (uint64_t)fbi <<
                    " ";
        }
    }
}
