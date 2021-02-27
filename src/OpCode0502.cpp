/* Oracle Redo OpCode: 5.2
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

#include "OpCode0502.h"
#include "OracleAnalyzer.h"
#include "Reader.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {
    OpCode0502::OpCode0502(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord) :
        OpCode(oracleAnalyzer, redoLogRecord) {
    }

    OpCode0502::~OpCode0502() {
    }

    void OpCode0502::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktudh(fieldPos, fieldLength);

        if (redoLogRecord->flg == 0x0080) {
            if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
                return;
            //field: 2
            kteop(fieldPos, fieldLength);
        }

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength)) {
            oracleAnalyzer->dumpStream << endl;
            return;
        }
        //field: 2/3
        pdb(fieldPos, fieldLength);
    }

    void OpCode0502::kteop(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 36) {
            oracleAnalyzer->dumpStream << "too short field kteop: " << dec << fieldLength << endl;
            return;
        }

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint32_t highwater = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 16);
            uint16_t ext = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 4);
            typeBLK blk = 0; //FIXME
            uint32_t extSize = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12);
            uint32_t blocksFreelist = 0; //FIXME
            uint32_t blocksBelow = 0; //FIXME
            typeBLK mapblk = 0; //FIXME
            uint16_t offset = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);

            oracleAnalyzer->dumpStream << "kteop redo - redo operation on extent map" << endl;
            oracleAnalyzer->dumpStream << "   SETHWM:      " <<
                    " Highwater::  0x" << setfill('0') << setw(8) << hex << highwater << " " <<
                    " ext#: " << setfill(' ') << setw(6) << left << dec << ext <<
                    " blk#: " << setfill(' ') << setw(6) << left << dec << blk <<
                    " ext size: " << setfill(' ') << setw(6) << left << dec << extSize << endl;
            oracleAnalyzer->dumpStream << "  #blocks in seg. hdr's freelists: " << dec << blocksFreelist << "     " << endl;
            oracleAnalyzer->dumpStream << "  #blocks below: " << setfill(' ') << setw(6) << left << dec << blocksBelow << endl;
            oracleAnalyzer->dumpStream << "  mapblk  0x" << setfill('0') << setw(8) << hex << mapblk << " " <<
                    " offset: " << setfill(' ') << setw(6) << left << dec << offset << endl;
            oracleAnalyzer->dumpStream << right;
        }
    }

    void OpCode0502::ktudh(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 32) {
            oracleAnalyzer->dumpStream << "too short field ktudh: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(redoLogRecord->usn,
                oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 0),
                oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->flg = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t fbi = redoLogRecord->data[fieldPos + 20];
            uint16_t siz = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 18);

            uint16_t pxid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24),
                    oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 26),
                    oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 28));

            oracleAnalyzer->dumpStream << "ktudh redo:" <<
                    " slt: 0x" << setfill('0') << setw(4) << hex << SLT(redoLogRecord->xid) <<
                    " sqn: 0x" << setfill('0') << setw(8) << hex << SQN(redoLogRecord->xid) <<
                    " flg: 0x" << setfill('0') << setw(4) << redoLogRecord->flg <<
                    " siz: " << dec << siz <<
                    " fbi: " << dec << (uint64_t)fbi << endl;
            if (oracleAnalyzer->version < REDO_VERSION_12_1 || redoLogRecord->conId == 0)
                oracleAnalyzer->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << PRINTXID(pxid);
            else
                oracleAnalyzer->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << PRINTXID(pxid);
            if (oracleAnalyzer->version < REDO_VERSION_12_1 || redoLogRecord->conId == 0)
                oracleAnalyzer->dumpStream << endl;
        }
    }

    void OpCode0502::pdb(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 4) {
            oracleAnalyzer->dumpStream << "too short field pdb: " << dec << fieldLength << endl;
            return;
        }
        redoLogRecord->pdbId = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 0);

        oracleAnalyzer->dumpStream << "       " <<
            " pdbid:" << dec << redoLogRecord->pdbId << endl;
    }
}
