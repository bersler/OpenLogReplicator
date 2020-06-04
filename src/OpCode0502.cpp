/* Oracle Redo OpCode: 5.2
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

#include <iomanip>
#include <iostream>

#include "OpCode0502.h"
#include "OracleAnalyser.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0502::OpCode0502(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
        OpCode(oracleAnalyser, redoLogRecord) {
    }

    OpCode0502::~OpCode0502() {
    }

    void OpCode0502::process() {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktudh(fieldPos, fieldLength);

        if (!oracleAnalyser->hasNextField(redoLogRecord, fieldNum))
            return;

        if (redoLogRecord->flg == 0x0080) {
            oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            //field: 2
            kteop(fieldPos, fieldLength);
        }

        if (!oracleAnalyser->hasNextField(redoLogRecord, fieldNum))
            return;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 2/3
        pdb(fieldPos, fieldLength);
    }

    void OpCode0502::kteop(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 36) {
            oracleAnalyser->dumpStream << "too short field kteop: " << dec << fieldLength << endl;
            return;
        }

        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint32_t highwater = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 16);
            uint16_t ext = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 4);
            typeblk blk = 0; //FIXME
            uint32_t extSize = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 12);
            uint32_t blocksFreelist = 0; //FIXME
            uint32_t blocksBelow = 0; //FIXME
            typeblk mapblk = 0; //FIXME
            uint16_t offset = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 24);

            oracleAnalyser->dumpStream << "kteop redo - redo operation on extent map" << endl;
            oracleAnalyser->dumpStream << "   SETHWM:      " <<
                    " Highwater::  0x" << setfill('0') << setw(8) << hex << highwater << " " <<
                    " ext#: " << setfill(' ') << setw(6) << left << dec << ext <<
                    " blk#: " << setfill(' ') << setw(6) << left << dec << blk <<
                    " ext size: " << setfill(' ') << setw(6) << left << dec << extSize << endl;
            oracleAnalyser->dumpStream << "  #blocks in seg. hdr's freelists: " << dec << blocksFreelist << "     " << endl;
            oracleAnalyser->dumpStream << "  #blocks below: " << setfill(' ') << setw(6) << left << dec << blocksBelow << endl;
            oracleAnalyser->dumpStream << "  mapblk  0x" << setfill('0') << setw(8) << hex << mapblk << " " <<
                    " offset: " << setfill(' ') << setw(6) << left << dec << offset << endl;
            oracleAnalyser->dumpStream << right;
        }
    }

    void OpCode0502::ktudh(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 32) {
            oracleAnalyser->dumpStream << "too short field ktudh: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(redoLogRecord->usn,
                oracleAnalyser->read16(redoLogRecord->data + fieldPos + 0),
                oracleAnalyser->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->uba = oracleAnalyser->read56(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->flg = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 16);

        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint8_t fbi = redoLogRecord->data[fieldPos + 20];
            uint16_t siz = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 18);

            uint16_t pxid = XID(oracleAnalyser->read16(redoLogRecord->data + fieldPos + 24),
                    oracleAnalyser->read16(redoLogRecord->data + fieldPos + 26),
                    oracleAnalyser->read32(redoLogRecord->data + fieldPos + 28));

            oracleAnalyser->dumpStream << "ktudh redo:" <<
                    " slt: 0x" << setfill('0') << setw(4) << hex << SLT(redoLogRecord->xid) <<
                    " sqn: 0x" << setfill('0') << setw(8) << hex << SQN(redoLogRecord->xid) <<
                    " flg: 0x" << setfill('0') << setw(4) << redoLogRecord->flg <<
                    " siz: " << dec << siz <<
                    " fbi: " << dec << (uint64_t)fbi << endl;
            if (oracleAnalyser->version < 0x12100 || redoLogRecord->conId == 0)
                oracleAnalyser->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << PRINTXID(pxid);
            else
                oracleAnalyser->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << PRINTXID(pxid);
            if (oracleAnalyser->version < 0x12100 || redoLogRecord->conId == 0)
                oracleAnalyser->dumpStream << endl;
        }
    }

    void OpCode0502::pdb(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 4) {
            oracleAnalyser->dumpStream << "too short field pdb: " << dec << fieldLength << endl;
            return;
        }
        redoLogRecord->pdbId = oracleAnalyser->read56(redoLogRecord->data + fieldPos + 0);

        oracleAnalyser->dumpStream << "       " <<
            " pdbid:" << dec << redoLogRecord->pdbId << endl;
    }
}
