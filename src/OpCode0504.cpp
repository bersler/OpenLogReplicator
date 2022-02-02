/* Oracle Redo OpCode: 5.4
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

namespace OpenLogReplicator {
    OpCode0504::OpCode0504(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord) :
        OpCode(oracleAnalyzer, redoLogRecord) {
    }

    OpCode0504::~OpCode0504() {
    }

    void OpCode0504::process(void) {
        OpCode::process();
        uint64_t fieldPos = 0;
        typeFIELD fieldNum = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050401);
        //field: 1
        ktucm(fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050402))
            return;
        //field: 2
        if ((redoLogRecord->flg & FLG_KTUCF_OP0504) != 0)
            ktucf(fieldPos, fieldLength);

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << std::endl;
            if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
                oracleAnalyzer->dumpStream << "rolled back transaction" << std::endl;
        }
    }

    void OpCode0504::ktucm(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field ktucm: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->xid = XID(redoLogRecord->usn,
                oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 0),
                oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->flg = redoLogRecord->data[fieldPos + 16];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint16_t srt = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8);  //to check
            uint32_t sta = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12);

            oracleAnalyzer->dumpStream << "ktucm redo: slt: 0x" << std::setfill('0') << std::setw(4) << std::hex << (uint64_t)SLT(redoLogRecord->xid) <<
                    " sqn: 0x" << std::setfill('0') << std::setw(8) << std::hex << SQN(redoLogRecord->xid) <<
                    " srt: " << std::dec << srt <<
                    " sta: " << std::dec << sta <<
                    " flg: 0x" << std::hex << redoLogRecord->flg << " ";
        }
    }

    void OpCode0504::ktucf(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 16) {
            WARNING("too short field ktucf: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 0);

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint16_t ext = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8);
            uint16_t spc = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 10);
            uint8_t fbi = redoLogRecord->data[fieldPos + 12];

            oracleAnalyzer->dumpStream << "ktucf redo:" <<
                    " uba: " << PRINTUBA(redoLogRecord->uba) <<
                    " ext: " << std::dec << ext <<
                    " spc: " << std::dec << spc <<
                    " fbi: " << std::dec << (uint64_t)fbi <<
                    " ";
        }
    }
}
