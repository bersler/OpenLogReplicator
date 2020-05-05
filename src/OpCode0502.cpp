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

#include <iostream>
#include <iomanip>
#include "OpCode0502.h"
#include "OracleReader.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0502::OpCode0502(OracleReader *oracleReader, RedoLogRecord *redoLogRecord) :
        OpCode(oracleReader, redoLogRecord) {
    }

    OpCode0502::~OpCode0502() {
    }

    void OpCode0502::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktudh(fieldPos, fieldLength);
            } else if (i == 2) {
                if (redoLogRecord->flg == 0x0080)
                    kteop(fieldPos, fieldLength);
                else
                    pdb(fieldPos, fieldLength);
            } else if (i == 3) {
                if (redoLogRecord->flg != 0x0080)
                    pdb(fieldPos, fieldLength);
            }
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }

    void OpCode0502::kteop(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 36) {
            oracleReader->dumpStream << "too short field kteop: " << dec << fieldLength << endl;
            return;
        }

        if (oracleReader->dumpRedoLog >= 1) {
            uint32_t highwater = oracleReader->read32(redoLogRecord->data + fieldPos + 16);
            uint16_t ext = oracleReader->read16(redoLogRecord->data + fieldPos + 4);
            typeblk blk = 0; //FIXME
            uint32_t extSize = oracleReader->read32(redoLogRecord->data + fieldPos + 12);
            uint32_t blocksFreelist = 0; //FIXME
            uint32_t blocksBelow = 0; //FIXME
            typeblk mapblk = 0; //FIXME
            uint16_t offset = oracleReader->read16(redoLogRecord->data + fieldPos + 24);

            oracleReader->dumpStream << "kteop redo - redo operation on extent map" << endl;
            oracleReader->dumpStream << "   SETHWM:      " <<
                    " Highwater::  0x" << setfill('0') << setw(8) << hex << highwater << " " <<
                    " ext#: " << setfill(' ') << setw(6) << left << dec << ext <<
                    " blk#: " << setfill(' ') << setw(6) << left << dec << blk <<
                    " ext size: " << setfill(' ') << setw(6) << left << dec << extSize << endl;
            oracleReader->dumpStream << "  #blocks in seg. hdr's freelists: " << dec << blocksFreelist << "     " << endl;
            oracleReader->dumpStream << "  #blocks below: " << setfill(' ') << setw(6) << left << dec << blocksBelow << endl;
            oracleReader->dumpStream << "  mapblk  0x" << setfill('0') << setw(8) << hex << mapblk << " " <<
                    " offset: " << setfill(' ') << setw(6) << left << dec << offset << endl;
            oracleReader->dumpStream << right;
        }
    }

    void OpCode0502::ktudh(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 32) {
            oracleReader->dumpStream << "too short field ktudh: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(redoLogRecord->usn,
                oracleReader->read16(redoLogRecord->data + fieldPos + 0),
                oracleReader->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->uba = oracleReader->read56(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->flg = oracleReader->read16(redoLogRecord->data + fieldPos + 16);

        if (oracleReader->dumpRedoLog >= 1) {
            uint8_t fbi = redoLogRecord->data[fieldPos + 20];
            uint16_t siz = oracleReader->read16(redoLogRecord->data + fieldPos + 18);

            uint16_t pxid = XID(oracleReader->read16(redoLogRecord->data + fieldPos + 24),
                    oracleReader->read16(redoLogRecord->data + fieldPos + 26),
                    oracleReader->read32(redoLogRecord->data + fieldPos + 28));

            oracleReader->dumpStream << "ktudh redo:" <<
                    " slt: 0x" << setfill('0') << setw(4) << hex << SLT(redoLogRecord->xid) <<
                    " sqn: 0x" << setfill('0') << setw(8) << hex << SQN(redoLogRecord->xid) <<
                    " flg: 0x" << setfill('0') << setw(4) << redoLogRecord->flg <<
                    " siz: " << dec << siz <<
                    " fbi: " << dec << (uint64_t)fbi << endl;
            if (oracleReader->version < 0x12100 || redoLogRecord->conId == 0)
                oracleReader->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << PRINTXID(pxid);
            else
                oracleReader->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << PRINTXID(pxid);
            if (oracleReader->version < 0x12100 || redoLogRecord->conId == 0)
                oracleReader->dumpStream << endl;
        }
    }

    void OpCode0502::pdb(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 4) {
            oracleReader->dumpStream << "too short field pdb: " << dec << fieldLength << endl;
            return;
        }
        redoLogRecord->pdbId = oracleReader->read56(redoLogRecord->data + fieldPos + 0);

        oracleReader->dumpStream << "       " <<
            " pdbid:" << dec << redoLogRecord->pdbId << endl;
    }
}
