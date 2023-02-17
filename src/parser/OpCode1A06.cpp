/* Oracle Redo OpCode: 26.6
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "OpCode1A06.h"

namespace OpenLogReplicator {
    void OpCode1A06::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0601);
        // Field: 1
        if (fieldLength < 12) {
            WARNING("too short field for 26.6: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0602);
        // Field: 2
        if (fieldLength < 32) {
            WARNING("too short field for 26.6: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }
        redoLogRecord->recordDataObj = ctx->read32(redoLogRecord->data + fieldPos + 24);

        OpCode::process(ctx, redoLogRecord);
        fieldPos = 0;
        fieldNum = 0;
        fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0603);
        // Field: 1
        uint8_t op = redoLogRecord->data[fieldPos + 0];

        redoLogRecord->dba = ctx->read32(redoLogRecord->data + fieldPos + 8);

        if (ctx->dumpRedoLog >= 1) {
            const char* opCode = "????";
            switch (op) {
                case OP266_OP_REDO:
                    opCode = "REDO";
                    break;
                case OP266_OP_UNDO:
                    opCode = "UNDO";
                    break;
                case OP266_OP_CR:
                    opCode = "CR";
                    break;
                case OP266_OP_FRMT:
                    opCode = "FRMT";
                    break;
                case OP266_OP_INVL:
                    opCode = "INVL";
                    break;
                case OP266_OP_LOAD:
                    opCode = "LOAD";
                    break;
                case OP266_OP_BIMG:
                    opCode = "BIMG";
                    break;
                case OP266_OP_SINV:
                    opCode = "SINV";
                    break;

            }
            uint8_t type = redoLogRecord->data[fieldPos + 1];
            const char* typeCode = "???";
            switch (type & OP266_TYPE_MASK) {
                case OP266_TYPE_NEW:
                    typeCode = "new";
                    break;
                case OP266_TYPE_LHB:
                    typeCode = "lhb";
                    break;
                case OP266_TYPE_DATA:
                    typeCode = "data";
                    break;
                case OP266_TYPE_BTREE:
                    typeCode = "btree";
                    break;
                case OP266_TYPE_ITREE:
                    typeCode = "itree";
                    break;
                case OP266_TYPE_AUX:
                    typeCode = "aux";
                    break;
            }

            uint8_t flg0 = redoLogRecord->data[fieldPos + 2];
            uint8_t flg1 = redoLogRecord->data[fieldPos + 3];
            uint16_t psiz = ctx->read32(redoLogRecord->data + fieldPos + 4);
            uint16_t poff = ctx->read32(redoLogRecord->data + fieldPos + 6);

            ctx->dumpStream << "KDLI common [" << std::dec << fieldLength << "]" << std::endl;
            ctx->dumpStream << "  op    0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(op) <<
                    " [" << opCode << "]" << std::endl;
            ctx->dumpStream << "  type  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(type) <<
                    " [" << typeCode << "]" << std::endl;
            ctx->dumpStream << "  flg0  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flg0) << std::endl;
            ctx->dumpStream << "  flg1  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flg1) << std::endl;
            ctx->dumpStream << "  psiz  " << std::dec << psiz << std::endl;
            ctx->dumpStream << "  poff  " << std::dec << poff << std::endl;
            ctx->dumpStream << "  dba   0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba << std::endl;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0604);
        // Field: 2

        redoLogRecord->xid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 16)),
                                     ctx->read16(redoLogRecord->data + fieldPos + 18),
                                     ctx->read32(redoLogRecord->data + fieldPos + 20));
        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data + fieldPos + 24);

        if (ctx->dumpRedoLog >= 1) {
            uint8_t code = redoLogRecord->data[fieldPos + 0];
            uint16_t bsz = ctx->read16(redoLogRecord->data + fieldPos + 4);
            typeScn scn = ctx->readScn(redoLogRecord->data + fieldPos + 8);

            ctx->dumpStream << "KDLI fpload [" << std::dec << static_cast<uint64_t>(code) << "." << fieldLength << "]" << std::endl;
            ctx->dumpStream << "  bsz   " << std::dec << bsz << std::endl;
            if (ctx->version < REDO_VERSION_12_2)
                ctx->dumpStream << "  scn   " << PRINTSCN48(scn) << std::endl;
            else
                ctx->dumpStream << "  scn   " << PRINTSCN64(scn) << std::endl;
            ctx->dumpStream << "  xid   " << redoLogRecord->xid << std::endl;
            ctx->dumpStream << "  objd  " << std::dec << redoLogRecord->dataObj << std::endl;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0605);
        // Field: 3
        if (fieldLength < 56) {
            WARNING("too short field for 26.6: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->lobId.set(redoLogRecord->data + fieldPos + 12);
        redoLogRecord->lobPageNo = INVALID_LOB_PAGE_NO;
        if (ctx->dumpRedoLog >= 1) {
            uint8_t code = redoLogRecord->data[fieldPos + 0];
            typeScn scn = ctx->readScnR(redoLogRecord->data + fieldPos + 2);
            uint8_t flg0 = redoLogRecord->data[fieldPos + 10];
            const char* flg0typ = "";
            switch (flg0 & OP266_TYPE_MASK) {
                case OP266_TYPE_NEW:
                    flg0typ = "new";
                    break;
                case OP266_TYPE_LHB:
                    flg0typ = "lhb";
                    break;
                case OP266_TYPE_DATA:
                    flg0typ = "data";
                    break;
                case OP266_TYPE_BTREE:
                    flg0typ = "btree";
                    break;
                case OP266_TYPE_ITREE:
                    flg0typ = "itree";
                    break;
                case OP266_TYPE_AUX:
                    flg0typ = "aux";
                    break;
            }
            const char* flg0lock = "n";
            if (flg0 & OP266_TYPE_LOCK)
                flg0lock = "y";
            const char* flg0var = "0";
            if (flg0 & OP266_TYPE_VER1)
                flg0var = "1";
            uint8_t flg1 = redoLogRecord->data[fieldPos + 11];
            uint16_t rid1 = ctx->read16(redoLogRecord->data + fieldPos + 22);
            uint32_t rid2 = ctx->read32(redoLogRecord->data + fieldPos + 24);
            uint8_t flg2 = redoLogRecord->data[fieldPos + 28];
            const char* flg2pfill = "n";
            if (flg2 & OP266_FLG2_PFILL)
                flg2pfill = "y";
            const char* flg2cmap = "n";
            if (flg2 & OP266_FLG2_CMAP)
                flg2cmap = "y";
            const char* flg2hash = "n";
            if (flg2 & OP266_FLG2_HASH)
                flg2hash = "y";
            const char* flg2lid = "short-rowid";
            if (flg2 & OP266_FLG2_LHB)
                flg2lid = "lhb-dba";
            const char* flg2ver1 = "0";
            if (flg2 & OP266_FLG2_VER1)
                flg2ver1 = "1";
            uint8_t flg3 = redoLogRecord->data[fieldPos + 29];
            uint8_t pskip = redoLogRecord->data[fieldPos + 30];
            uint8_t sskip = redoLogRecord->data[fieldPos + 31];
            uint8_t hash[20];
            memcpy(reinterpret_cast<void*>(hash),
                   reinterpret_cast<const void*>(redoLogRecord->data + fieldPos + 32), 20);
            uint16_t hwm = ctx->read16(redoLogRecord->data + fieldPos + 52);
            uint16_t spr = ctx->read16(redoLogRecord->data + fieldPos + 54);

            ctx->dumpStream << "KDLI load data [" << std::dec << static_cast<uint64_t>(code) << "." << fieldLength << "]" << std::endl;
            ctx->dumpStream << "bdba    [0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba << "]" << std::endl;
            ctx->dumpStream << "kdlich  [0xXXXXXXXXXXXX 0]" << std::endl;
            ctx->dumpStream << "  flg0  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flg0) <<
                    " [ver=" << flg0var << " typ=" << flg0typ << " lock=" << flg0lock << "]" << std::endl;
            ctx->dumpStream << "  flg1  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flg1) << std::endl;
            if (ctx->version < REDO_VERSION_12_2)
                ctx->dumpStream << "  scn   " << PRINTSCN48(scn) << std::endl;
            else
                ctx->dumpStream << "  scn   " << PRINTSCN64(scn) << std::endl;
            ctx->dumpStream << "  lid   " << redoLogRecord->lobId.lower() << std::endl;
            ctx->dumpStream << "  rid   0x" << std::setfill('0') << std::setw(8) << std::hex << rid2 << "." << std::setfill('0') <<
                    std::setw(4) << std::hex << rid1 << std::endl;
            ctx->dumpStream << "kdlidh  [0xXXXXXXXXXXXX 24]" << std::endl;
            ctx->dumpStream << "  flg2  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flg2) <<
                    " [ver=" << flg2ver1 << " lid=" << flg2lid << " hash=" << flg2hash << " cmap=" << flg2cmap << " pfill=" << flg2pfill << "]" << std::endl;
            ctx->dumpStream << "  flg3  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flg3) << std::endl;
            ctx->dumpStream << "  pskip " << std::dec << static_cast<uint64_t>(pskip) << std::endl;
            ctx->dumpStream << "  sskip " << std::dec << static_cast<uint64_t>(sskip) << std::endl;
            ctx->dumpStream << "  hash  ";
            for (uint64_t j = 0; j < 20; ++j)
                ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(hash[j]);
            ctx->dumpStream << std::endl;
            ctx->dumpStream << "  hwm   " << std::dec << hwm << std::endl;
            ctx->dumpStream << "  spr   " << std::dec << spr << std::endl;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0606);
        // Field: 4

        redoLogRecord->lobData = fieldPos;
        redoLogRecord->lobDataLength = fieldLength;

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "KDLI data load [0xXXXXXXXXXXXX." << std::dec << fieldLength << "]" << std::endl;

            for (uint64_t j = 0; j < fieldLength; ++j) {
                ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data[fieldPos + j]);
                if ((j % 26) < 25)
                    ctx->dumpStream << " ";
                if ((j % 26) == 25 || j == static_cast<uint64_t>(fieldLength) - 1)
                    ctx->dumpStream << std::endl;
            }
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0607))
            return;
        // Field: 5
        if (fieldLength < 24) {
            WARNING("too short field for 26.6: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        typeObj objn = ctx->read32(redoLogRecord->data + fieldPos + 12);
        redoLogRecord->col = ctx->read16(redoLogRecord->data + fieldPos + 18);

        if (ctx->dumpRedoLog >= 1) {
            if (op == OP266_OP_BIMG) {
                uint8_t code = redoLogRecord->data[fieldPos + 0];
                typeXid xid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 4)),
                                      ctx->read16(redoLogRecord->data + fieldPos + 6),
                                      ctx->read32(redoLogRecord->data + fieldPos + 8));
                uint16_t objv = ctx->read16(redoLogRecord->data + fieldPos + 16);
                uint32_t flag = ctx->read32(redoLogRecord->data + fieldPos + 20);

                ctx->dumpStream << "KDLI suplog [" << std::dec << static_cast<uint64_t>(code) << "." << std::dec << fieldLength
                                << "]" << std::endl;
                ctx->dumpStream << "  xid   " << xid << std::endl;
                ctx->dumpStream << "  objn  " << std::dec << objn << std::endl;
                ctx->dumpStream << "  objv# " << std::dec << objv << std::endl;
                ctx->dumpStream << "  col#  " << std::dec << redoLogRecord->col << std::endl;
                ctx->dumpStream << "  flag  0x" << std::setfill('0') << std::setw(8) << std::hex << flag << std::endl;
            } else {
                ctx->dumpStream << "KDLI data load [0xXXXXXXXXXXXX." << std::dec << fieldLength << "]" << std::endl;

                for (uint64_t j = 0; j < fieldLength; ++j) {
                    ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data[fieldPos + j]);
                    if ((j % 26) < 25)
                        ctx->dumpStream << " ";
                    if ((j % 26) == 25 || j == static_cast<uint64_t>(fieldLength) - 1)
                        ctx->dumpStream << std::endl;
                }
            }
        }
    }
}
