/* Oracle Redo OpCode: 5.2
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/RedoLogRecord.h"
#include "OpCode0502.h"

namespace OpenLogReplicator {
    void OpCode0502::process0502(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050201);
        // Field: 1
        ktudh(ctx, redoLogRecord, fieldPos, fieldLength);

        if (ctx->version >= REDO_VERSION_12_1) {
            // Field: 2
            if (RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050202)) {
                if (fieldLength == 4) {
                    // Field: 2
                    pdb(ctx, redoLogRecord, fieldPos, fieldLength);
                } else {
                    kteop(ctx, redoLogRecord, fieldPos, fieldLength);

                    // Field: 3
                    if (RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050203)) {
                        pdb(ctx, redoLogRecord, fieldPos, fieldLength);
                    }
                }
            }

            ctx->dumpStream << '\n';
        }
    }

    void OpCode0502::kteop(Ctx* ctx, const RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength) {
        if (fieldLength < 36)
            throw RedoLogException(50061, "too short field kteop: " + std::to_string(fieldLength) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        if (ctx->dumpRedoLog >= 1) {
            uint32_t highwater = ctx->read32(redoLogRecord->data + fieldPos + 16);
            uint32_t ext = ctx->read32(redoLogRecord->data + fieldPos + 4);
            typeBlk blk = 0; // TODO: find field position/size
            uint32_t extSize = ctx->read32(redoLogRecord->data + fieldPos + 12);
            uint32_t blocksFreelist = 0; // TODO: find field position/size
            uint32_t blocksBelow = 0; // TODO: find field position/size
            typeBlk mapblk = 0; // TODO: find field position/size
            uint32_t offset = ctx->read32(redoLogRecord->data + fieldPos + 24);

            ctx->dumpStream << "kteop redo - redo operation on extent map\n";
            ctx->dumpStream << "   SETHWM:      " <<
                            " Highwater::  0x" << std::setfill('0') << std::setw(8) << std::hex << highwater << " " <<
                            " ext#: " << std::setfill(' ') << std::setw(6) << std::left << std::dec << ext <<
                            " blk#: " << std::setfill(' ') << std::setw(6) << std::left << std::dec << blk <<
                            " ext size: " << std::setfill(' ') << std::setw(6) << std::left << std::dec << extSize << '\n';
            ctx->dumpStream << "  #blocks in seg. hdr's freelists: " << std::dec << blocksFreelist << "     \n";
            ctx->dumpStream << "  #blocks below: " << std::setfill(' ') << std::setw(6) << std::left << std::dec << blocksBelow << '\n';
            ctx->dumpStream << "  mapblk  0x" << std::setfill('0') << std::setw(8) << std::hex << mapblk << " " <<
                            " offset: " << std::setfill(' ') << std::setw(6) << std::left << std::dec << offset << '\n';
            ctx->dumpStream << std::right;
        }
    }

    void OpCode0502::ktudh(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength) {
        if (fieldLength < 32)
            throw RedoLogException(50061, "too short field ktudh: " + std::to_string(fieldLength) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->xid = typeXid(redoLogRecord->usn,
                                     ctx->read16(redoLogRecord->data + fieldPos + 0),
                                     ctx->read32(redoLogRecord->data + fieldPos + 4));
        redoLogRecord->uba = ctx->read56(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->flg = ctx->read16(redoLogRecord->data + fieldPos + 16);

        if (ctx->dumpRedoLog >= 1) {
            uint8_t fbi = redoLogRecord->data[fieldPos + 20];
            uint16_t siz = ctx->read16(redoLogRecord->data + fieldPos + 18);

            typeXid pXid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 24)),
                                   ctx->read16(redoLogRecord->data + fieldPos + 26),
                                   ctx->read32(redoLogRecord->data + fieldPos + 28));

            ctx->dumpStream << "ktudh redo:" <<
                            " slt: 0x" << std::setfill('0') << std::setw(4) << std::hex << static_cast<uint64_t>(redoLogRecord->xid.slt()) <<
                            " sqn: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->xid.sqn() <<
                            " flg: 0x" << std::setfill('0') << std::setw(4) << redoLogRecord->flg <<
                            " siz: " << std::dec << siz <<
                            " fbi: " << std::dec << static_cast<uint64_t>(fbi) << '\n';
            /*if (ctx->version < REDO_VERSION_12_1 || redoLogRecord->conId == 0)
                ctx->dumpStream << "           " <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                        " pxid:  " << pXid;
            else*/
            ctx->dumpStream << "           " <<
                            " uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
                            " pxid:  " << pXid.toString();
            if (ctx->version < REDO_VERSION_12_1) // || redoLogRecord->conId == 0)
                ctx->dumpStream << '\n';
        }
    }

    void OpCode0502::pdb(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength) {
        if (fieldLength < 4)
            throw RedoLogException(50061, "too short field pdb: " + std::to_string(fieldLength) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->pdbId = ctx->read56(redoLogRecord->data + fieldPos + 0);

        ctx->dumpStream << "       " <<
                        " pdbid:" << std::dec << redoLogRecord->pdbId;
    }
}
