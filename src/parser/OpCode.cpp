/* Oracle Redo Generic OpCode
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

#include <cstring>

#include "../common/RedoLogRecord.h"
#include "OpCode.h"

namespace OpenLogReplicator {
    void OpCode::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        if (ctx->dumpRedoLog >= 1) {
            bool encrypted = false;
            if ((redoLogRecord->typ & 0x80) != 0)
                encrypted = true;

            if (ctx->version < REDO_VERSION_12_1) {
                if (redoLogRecord->typ == 6)
                    ctx->dumpStream << "CHANGE #" << std::dec << static_cast<uint64_t>(redoLogRecord->vectorNo) <<
                        " MEDIA RECOVERY MARKER" <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << static_cast<uint64_t>(redoLogRecord->seq) <<
                        " OP:" << static_cast<uint64_t>(redoLogRecord->opCode >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << static_cast<uint64_t>(encrypted) << std::endl;
                else
                    ctx->dumpStream << "CHANGE #" << std::dec << static_cast<uint64_t>(redoLogRecord->vectorNo) <<
                        " TYP:" << static_cast<uint64_t>(redoLogRecord->typ) <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                        " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << static_cast<uint64_t>(redoLogRecord->seq) <<
                        " OP:" << static_cast<uint64_t>(redoLogRecord->opCode >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << static_cast<uint64_t>(encrypted) <<
                        " RBL:" << std::dec << redoLogRecord->rbl << std::endl;
            } else if (ctx->version < REDO_VERSION_12_2) {
                if (redoLogRecord->typ == 6)
                    ctx->dumpStream << "CHANGE #" << std::dec << static_cast<uint64_t>(redoLogRecord->vectorNo) <<
                        " MEDIA RECOVERY MARKER" <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << static_cast<uint64_t>(redoLogRecord->seq) <<
                        " OP:" << static_cast<uint64_t>(redoLogRecord->opCode >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << static_cast<uint64_t>(encrypted) <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
                else
                    ctx->dumpStream << "CHANGE #" << std::dec << static_cast<uint64_t>(redoLogRecord->vectorNo) <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " TYP:" << static_cast<uint64_t>(redoLogRecord->typ) <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                        " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << static_cast<uint64_t>(redoLogRecord->seq) <<
                        " OP:" << static_cast<uint64_t>(redoLogRecord->opCode >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << static_cast<uint64_t>(encrypted) <<
                        " RBL:" << std::dec << redoLogRecord->rbl <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
            } else {
                if (redoLogRecord->typ == 6)
                    ctx->dumpStream << "CHANGE #" << std::dec << static_cast<uint64_t>(redoLogRecord->vectorNo) <<
                        " MEDIA RECOVERY MARKER" <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " SCN:" << PRINTSCN64(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << static_cast<uint64_t>(redoLogRecord->seq) <<
                        " OP:" << static_cast<uint64_t>(redoLogRecord->opCode >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << static_cast<uint64_t>(encrypted) <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
                else
                    ctx->dumpStream << "CHANGE #" << std::dec << static_cast<uint64_t>(redoLogRecord->vectorNo) <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " TYP:" << static_cast<uint64_t>(redoLogRecord->typ) <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                        " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                        " SCN:" << PRINTSCN64(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << static_cast<uint64_t>(redoLogRecord->seq) <<
                        " OP:" << static_cast<uint64_t>(redoLogRecord->opCode >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << static_cast<uint64_t>(encrypted) <<
                        " RBL:" << std::dec << redoLogRecord->rbl <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
            }
        }

        if (ctx->dumpRawData)
            dumpHex(ctx, redoLogRecord);
    }

    void OpCode::ktbRedo(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 8)
            return;

        if (redoLogRecord->opc == 0x0A16)
            ctx->dumpStream << "index undo for leaf key operations" << std::endl;
        else if (redoLogRecord->opc == 0x0B01)
            ctx->dumpStream << "KDO undo record:" << std::endl;

        auto ktbOp = (int8_t)redoLogRecord->data[fieldPos + 0];
        uint8_t flg = redoLogRecord->data[fieldPos + 1];
        uint8_t ver = flg & 0x03;
        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "KTB Redo " << std::endl;
            ctx->dumpStream << "op: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<int32_t>(ktbOp) << " " <<
                    " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(ver) << "  " << std::endl;
            ctx->dumpStream << "compat bit: " << std::dec << static_cast<uint64_t>(flg & 0x04) << " ";
            if ((flg & 0x04) != 0)
                ctx->dumpStream << "(post-11)";
            else
                ctx->dumpStream << "(pre-11)";

            uint64_t padding = ((flg & 0x10) != 0) ? 0 : 1;
            ctx->dumpStream << " padding: " << padding << std::endl;
        }
        char opCode = '?';

        if ((ktbOp & 0x0F) == KTBOP_C) {
            if (fieldLength < 16) {
                WARNING("too short field KTB Redo C: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                return;
            }

            opCode = 'C';
            if ((flg & 0x04) != 0)
                redoLogRecord->uba = ctx->read56(redoLogRecord->data + fieldPos + 8);
            else
                redoLogRecord->uba = ctx->read56(redoLogRecord->data + fieldPos + 4);
            if (ctx->dumpRedoLog >= 1) {
                ctx->dumpStream << "op: " << opCode << " " << " uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;
            }
        } else if ((ktbOp & 0x0F) == KTBOP_Z) {
            opCode = 'Z';
            if (ctx->dumpRedoLog >= 1) {
                ctx->dumpStream << "op: " << opCode << std::endl;
            }
        } else if ((ktbOp & 0x0F) == KTBOP_L) {
            opCode = 'L';
            if ((flg & 0x08) == 0) {
                if (fieldLength < 28) {
                    WARNING("too short field KTB Redo L: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                    return;
                }
                redoLogRecord->uba = ctx->read56(redoLogRecord->data + fieldPos + 12);

                if (ctx->dumpRedoLog >= 1) {
                    typeXid itlXid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 4)),
                                             ctx->read16(redoLogRecord->data + fieldPos + 6),
                                             ctx->read32(redoLogRecord->data + fieldPos + 8));

                    ctx->dumpStream << "op: " << opCode << " " <<
                                    " itl:" <<
                                    " xid:  " << itlXid <<
                                    " uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;

                    uint8_t lkc = redoLogRecord->data[fieldPos + 20];
                    uint8_t flag = redoLogRecord->data[fieldPos + 19];
                    char flagStr[5] = "----";
                    if ((flag & 0x80) != 0) flagStr[0] = 'C';
                    if ((flag & 0x40) != 0) flagStr[1] = 'B';
                    if ((flag & 0x20) != 0) flagStr[2] = 'U';
                    if ((flag & 0x10) != 0) flagStr[3] = 'T';
                    typeScn scnx = ctx->readScnR(redoLogRecord->data + fieldPos + 26);

                    if (ctx->version < REDO_VERSION_12_2)
                        ctx->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << static_cast<uint64_t>(lkc) << "    " <<
                                " fac: " << PRINTSCN48(scnx) << std::endl;
                    else
                        ctx->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << static_cast<uint64_t>(lkc) << "    " <<
                                " fac:  " << PRINTSCN64(scnx) << std::endl;
                }

            } else {
                if (fieldLength < 32) {
                    WARNING("too short field KTB Redo L2: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                    return;
                }
                redoLogRecord->uba = ctx->read56(redoLogRecord->data + fieldPos + 16);

                if (ctx->dumpRedoLog >= 1) {
                    typeXid itlXid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 8)),
                                             ctx->read16(redoLogRecord->data + fieldPos + 10),
                                             ctx->read32(redoLogRecord->data + fieldPos + 12));

                    ctx->dumpStream << "op: " << opCode << " " <<
                                    " itl:" <<
                                    " xid:  " << itlXid <<
                                    " uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;

                    uint8_t lkc;
                    uint8_t flag;
                    if (ctx->isBigEndian()) {
                        lkc = redoLogRecord->data[fieldPos + 25];
                        flag = redoLogRecord->data[fieldPos + 24];
                    } else {
                        lkc = redoLogRecord->data[fieldPos + 24];
                        flag = redoLogRecord->data[fieldPos + 25];
                    }
                    char flagStr[5] = "----";
                    if ((flag & 0x10) != 0) flagStr[3] = 'T';
                    if ((flag & 0x20) != 0) flagStr[2] = 'U';
                    if ((flag & 0x40) != 0) flagStr[1] = 'B';
                    if ((flag & 0x80) != 0) flagStr[0] = 'C';
                    typeScn scnx = ctx->readScnR(redoLogRecord->data + fieldPos + 26);

                    if (ctx->version < REDO_VERSION_12_2)
                        ctx->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << static_cast<uint64_t>(lkc) << "    " <<
                                " scn: " << PRINTSCN48(scnx) << std::endl;
                    else
                        ctx->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << static_cast<uint64_t>(lkc) << "    " <<
                                " scn:  " << PRINTSCN64(scnx) << std::endl;
                }
            }

        } else if ((ktbOp & 0x0F) == KTBOP_R) {
            opCode = 'R';

            if (ctx->dumpRedoLog >= 1) {
                int16_t itc = ctx->read16(redoLogRecord->data + fieldPos + 10);
                ctx->dumpStream << "op: " << opCode << "  itc: " << std::dec << itc << std::endl;
                if (itc < 0)
                    itc = 0;

                if (fieldLength < 20 + itc * 24) {
                    WARNING("too short field KTB Redo R: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                    return;
                }

                ctx->dumpStream << " Itl           Xid                  Uba         Flag  Lck        Scn/Fsc" << std::endl;
                for (uint64_t i = 0; i < static_cast<uint64_t>(itc); ++i) {
                    typeXid itcXid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 20 + i * 24)),
                                             ctx->read16(redoLogRecord->data + fieldPos + 20 + 2 + i * 24),
                                             ctx->read32(redoLogRecord->data + fieldPos + 20 + 4 + i * 24));

                    typeUba itcUba = ctx->read56(redoLogRecord->data + fieldPos + 20 + 8 + i * 24);
                    char flagsStr[5] = "----";
                    typeScn scnfsc;
                    const char* scnfscStr = "fsc";
                    uint16_t lck = ctx->read16(redoLogRecord->data + fieldPos + 20 + 16 + i * 24);
                    if ((lck & 0x1000) != 0) flagsStr[3] = 'T';
                    if ((lck & 0x2000) != 0) flagsStr[2] = 'U';
                    if ((lck & 0x4000) != 0) flagsStr[1] = 'B';
                    if ((lck & 0x8000) != 0) {
                        flagsStr[0] = 'C';
                        scnfscStr = "scn";
                        lck = 0;
                        scnfsc = ctx->readScn(redoLogRecord->data + fieldPos + 20 + 18 + i * 24);
                    } else
                        scnfsc = (static_cast<uint64_t>(ctx->read16(redoLogRecord->data + fieldPos + 20 + 18 + i * 24)) << 32) |
                                static_cast<uint64_t>(ctx->read32(redoLogRecord->data + fieldPos + 20 + 20 + i * 24));
                    lck &= 0x0FFF;


                    ctx->dumpStream << "0x" << std::setfill('0') << std::setw(2) << std::hex << (i + 1) << "   " <<
                           itcXid.toString() << "  " <<
                           PRINTUBA(itcUba) << "  " << flagsStr << "  " <<
                           std::setfill(' ') << std::setw(3) << std::dec << static_cast<uint64_t>(lck) << "  " << scnfscStr << " " <<
                           PRINTSCN48(scnfsc) << std::endl;
                }
            }

        } else if ((ktbOp & 0x0F) == KTBOP_N) {
            opCode = 'N';
            if (ctx->dumpRedoLog >= 1) {
                ctx->dumpStream << "op: " << opCode << std::endl;
            }

        } else if ((ktbOp & 0x0F) == KTBOP_F) {
            if (fieldLength < 24) {
                WARNING("too short field KTB Redo F: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                return;
            }

            opCode = 'F';
            redoLogRecord->xid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data + fieldPos + 8)),
                                         ctx->read16(redoLogRecord->data + fieldPos + 10),
                                         ctx->read32(redoLogRecord->data + fieldPos + 12));
            redoLogRecord->uba = ctx->read56(redoLogRecord->data + fieldPos + 16);

            if (ctx->dumpRedoLog >= 1) {

                ctx->dumpStream << "op: " << opCode << " " <<
                        " xid:  " << redoLogRecord->xid <<
                        "    uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;
            }
        }

        // Block cleanout record
        if ((ktbOp & KTBOP_BLOCKCLEANOUT) != 0) {
            if (ctx->dumpRedoLog >= 1) {
                typeScn scn = ctx->readScn(redoLogRecord->data + fieldPos + 48);
                uint8_t opt = redoLogRecord->data[fieldPos + 44];
                uint8_t ver2 = redoLogRecord->data[fieldPos + 46];
                uint8_t entries = redoLogRecord->data[fieldPos + 45];

                if (ctx->version < REDO_VERSION_12_2)
                    ctx->dumpStream << "Block cleanout record, scn: " <<
                            " " << PRINTSCN48(scn) <<
                            " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(ver2) <<
                            " opt: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(opt) <<
                            ", entries follow..." << std::endl;
                else {
                    char bigscn = 'N';
                    char compat = 'N';
                    if ((ver2 & 0x08) != 0)
                        bigscn = 'Y';
                    if ((ver2 & 0x04) != 0)
                        compat = 'Y';
                    uint32_t spare = 0; // TODO: find field position/size
                    ver2 &= 0x03;
                    ctx->dumpStream << "Block cleanout record, scn: " <<
                            " " << PRINTSCN64(scn) <<
                            " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(ver2) <<
                            " opt: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(opt) <<
                            " bigscn: " << bigscn <<
                            " compact: " << compat <<
                            " spare: " << std::setfill('0') << std::setw(8) << std::hex << spare <<
                            ", entries follow..." << std::endl;
                }

                if (fieldLength < 56 + entries * static_cast<uint64_t>(8)) {
                    WARNING("too short field KTB Redo F2: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                    return;
                }

                for (uint64_t j = 0; j < entries; ++j) {
                    uint8_t itli = redoLogRecord->data[fieldPos + 56 + j * 8];
                    uint8_t flg2 = redoLogRecord->data[fieldPos + 57 + j * 8];
                    typeScn scnx = ctx->readScnR(redoLogRecord->data + fieldPos + 58 + j * 8);
                    if (ctx->version < REDO_VERSION_12_1)
                        ctx->dumpStream << "  itli: " << std::dec << static_cast<uint64_t>(itli) << " " <<
                                " flg: " << static_cast<uint64_t>(flg2) << " " <<
                                " scn: " << PRINTSCN48(scnx) << std::endl;
                    else if (ctx->version < REDO_VERSION_12_2)
                        ctx->dumpStream << "  itli: " << std::dec << static_cast<uint64_t>(itli) << " " <<
                                " flg: (opt=" << static_cast<uint64_t>(flg2 & 0x03) << " whr=" << static_cast<uint64_t>(flg2 >> 2) << ") " <<
                                " scn: " << PRINTSCN48(scnx) << std::endl;
                    else {
                        uint8_t opt2 = flg2 & 0x03;
                        uint8_t whr = flg2 >> 2;
                        ctx->dumpStream << "  itli: " << std::dec << static_cast<uint64_t>(itli) << " " <<
                                " flg: (opt=" << static_cast<uint64_t>(opt2) << " whr=" << static_cast<uint64_t>(whr) << ") " <<
                                " scn:  " << PRINTSCN64(scnx) << std::endl;
                    }
                }
            }
        }
    }

    void OpCode::kdoOpCodeIRP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 48) {
            WARNING("too short field KDO OpCode IRP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->fb = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->sizeDelt = ctx->read16(redoLogRecord->data + fieldPos + 40);
        redoLogRecord->slot = ctx->read16(redoLogRecord->data + fieldPos + 42);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 44];

        if ((redoLogRecord->fb & FB_L) == 0) {
            redoLogRecord->nridBdba = ctx->read32(redoLogRecord->data + fieldPos + 28);
            redoLogRecord->nridSlot = ctx->read16(redoLogRecord->data + fieldPos + 32);
        }

        if (fieldLength < 45 + (static_cast<uint64_t>(redoLogRecord->cc) + 7) / 8) {
            WARNING("too short field KDO OpCode IRP for nulls: " << std::dec << fieldLength <<
                    " (cc: " << std::dec << static_cast<uint64_t>(redoLogRecord->cc) << ") offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->nullsDelta = fieldPos + 45;
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;
        for (uint64_t i = 0; i < static_cast<uint64_t>(redoLogRecord->cc); ++i) {
            if ((*nulls & bits) == 0)
                redoLogRecord->ccData = i + 1;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
        }

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "tabn: " << static_cast<uint64_t>(redoLogRecord->tabn) <<
                    " slot: " << std::dec << static_cast<uint64_t>(redoLogRecord->slot) << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                    " size/delt: " << std::dec << redoLogRecord->sizeDelt << std::endl;

            char fbStr[9] = "--------";
            processFbFlags(redoLogRecord->fb, fbStr);
            uint8_t lb = redoLogRecord->data[fieldPos + 17];

            ctx->dumpStream << "fb: " << fbStr <<
                    " lb: 0x" << std::hex << static_cast<uint64_t>(lb) << " " <<
                    " cc: " << std::dec << static_cast<uint64_t>(redoLogRecord->cc);
            if (fbStr[1] == 'C') {
                uint8_t cki = redoLogRecord->data[fieldPos + 19];
                ctx->dumpStream << " cki: " << std::dec << static_cast<uint64_t>(cki) << std::endl;
            } else
                ctx->dumpStream << std::endl;

            if ((redoLogRecord->fb & FB_F) != 0  && (redoLogRecord->fb & FB_H) == 0) {
                typeDba hrid1 = ctx->read32(redoLogRecord->data + fieldPos + 20);
                typeSlot hrid2 = ctx->read16(redoLogRecord->data + fieldPos + 24);
                ctx->dumpStream << "hrid: 0x" << std::setfill('0') << std::setw(8) << std::hex << hrid1 << "." << std::hex << hrid2 << std::endl;
            }

            // Next DBA/SLT
            if ((redoLogRecord->fb & FB_L) == 0) {
                ctx->dumpStream << "nrid:  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->nridBdba << "." << std::hex <<
                        redoLogRecord->nridSlot << std::endl;
            }

            if ((redoLogRecord->fb & FB_K) != 0) {
                uint8_t curc = 0; // TODO: find field position/size
                uint8_t comc = 0; // TODO: find field position/size
                uint32_t pk = ctx->read32(redoLogRecord->data + fieldPos + 20);
                uint16_t pk1 = ctx->read16(redoLogRecord->data + fieldPos + 24);
                uint32_t nk = ctx->read32(redoLogRecord->data + fieldPos + 28);
                uint16_t nk1 = ctx->read16(redoLogRecord->data + fieldPos + 32);

                ctx->dumpStream << "curc: " << std::dec << static_cast<uint64_t>(curc) <<
                        " comc: " << std::dec << static_cast<uint64_t>(comc) <<
                        " pk: 0x" << std::setfill('0') << std::setw(8) << std::hex << pk << "." << std::hex << pk1 <<
                        " nk: 0x" << std::setfill('0') << std::setw(8) << std::hex << nk << "." << std::hex << nk1 << std::endl;
            }

            ctx->dumpStream << "null:";
            if (redoLogRecord->cc >= 11)
                ctx->dumpStream << std::endl << "01234567890123456789012345678901234567890123456789012345678901234567890123456789" << std::endl;
            else
                ctx->dumpStream << " ";

            nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            bits = 1;
            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {

                if ((*nulls & bits) != 0)
                    ctx->dumpStream << "N";
                else
                    ctx->dumpStream << "-";
                if ((i % 80) == 79 && i < redoLogRecord->cc)
                    ctx->dumpStream << std::endl;

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
            ctx->dumpStream << std::endl;
        }
    }

    void OpCode::kdoOpCodeDRP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field KDO OpCode DRP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->slot = ctx->read16(redoLogRecord->data + fieldPos + 16);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 18];

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "tabn: " << static_cast<uint64_t>(redoLogRecord->tabn) <<
                    " slot: " << std::dec << static_cast<uint64_t>(redoLogRecord->slot) << "(0x" << std::hex << redoLogRecord->slot << ")" << std::endl;
        }
    }

    void OpCode::kdoOpCodeLKR(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field KDO OpCode LKR: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->slot = ctx->read16(redoLogRecord->data + fieldPos + 16);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 18];

        if (ctx->dumpRedoLog >= 1) {
            uint8_t to = redoLogRecord->data[fieldPos + 19];
            ctx->dumpStream << "tabn: " << static_cast<uint64_t>(redoLogRecord->tabn) <<
                " slot: " << std::dec << redoLogRecord->slot <<
                " to: " << std::dec << static_cast<uint64_t>(to) << std::endl;
        }
    }

    void OpCode::kdoOpCodeURP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 28) {
            WARNING("too short field KDO OpCode URP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->fb = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 19];
        redoLogRecord->slot = ctx->read16(redoLogRecord->data + fieldPos + 20);
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 23];

        if (fieldLength < 26 + (static_cast<uint64_t>(redoLogRecord->cc) + 7) / 8) {
            WARNING("too short field KDO OpCode URP for nulls: " << std::dec << fieldLength <<
                    " (cc: " << std::dec << static_cast<uint64_t>(redoLogRecord->cc) << ") offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->nullsDelta = fieldPos + 26;
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;
        for (uint64_t i = 0; i < static_cast<uint64_t>(redoLogRecord->cc); ++i) {
            if ((*nulls & bits) == 0)
                redoLogRecord->ccData = i + 1;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
        }

        if (ctx->dumpRedoLog >= 1) {
            uint8_t lock = redoLogRecord->data[fieldPos + 17];
            uint8_t ckix = redoLogRecord->data[fieldPos + 18];
            uint8_t ncol = redoLogRecord->data[fieldPos + 22];
            auto size = (int16_t)ctx->read16(redoLogRecord->data + fieldPos + 24); // Signed

            ctx->dumpStream << "tabn: " << static_cast<uint64_t>(redoLogRecord->tabn) <<
                    " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                    " flag: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->fb) <<
                    " lock: " << std::dec << static_cast<uint64_t>(lock) <<
                    " ckix: " << std::dec << static_cast<uint64_t>(ckix) << std::endl;
            ctx->dumpStream << "ncol: " << std::dec << static_cast<uint64_t>(ncol) <<
                    " nnew: " << std::dec << static_cast<uint64_t>(redoLogRecord->cc) <<
                    " size: " << std::dec << size << std::endl;
        }
    }

    void OpCode::kdoOpCodeCFA(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 32) {
            WARNING("too short field KDO OpCode ORP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->nridBdba = ctx->read32(redoLogRecord->data + fieldPos + 16);
        redoLogRecord->nridSlot = ctx->read16(redoLogRecord->data + fieldPos + 20);
        redoLogRecord->slot = ctx->read16(redoLogRecord->data + fieldPos + 24);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 27];

        if (ctx->dumpRedoLog >= 1) {
            uint8_t flag = redoLogRecord->data[fieldPos + 26];
            uint8_t lock = redoLogRecord->data[fieldPos + 28];
            ctx->dumpStream <<
                    "tabn: " << std::dec << static_cast<uint64_t>(redoLogRecord->tabn) <<
                    " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                    " flag: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(flag) << std::endl <<
                    "lock: " << std::dec << static_cast<uint64_t>(lock) <<
                    " nrid: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->nridBdba << "." << std::hex <<
                    redoLogRecord->nridSlot << std::endl;
        }
    }

    void OpCode::kdoOpCodeSKL(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field KDO OpCode SKL: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->slot = redoLogRecord->data[fieldPos + 27];

        if (ctx->dumpRedoLog >= 1) {
            char flagStr[3] = "--";
            uint8_t lock = redoLogRecord->data[fieldPos + 29];
            uint8_t flag = redoLogRecord->data[fieldPos + 28];
            if ((flag & 0x01) != 0) flagStr[0] = 'F';
            if ((flag & 0x02) != 0) flagStr[1] = 'B';

            ctx->dumpStream << "flag: " << flagStr <<
                    " lock: " << std::dec << static_cast<uint64_t>(lock) <<
                    " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" << std::endl;

            if ((flag & 0x01) != 0) {
                uint8_t fwd[4];
                uint16_t fwd2 = ctx->read16(redoLogRecord->data + fieldPos + 20);
                memcpy(reinterpret_cast<void*>(fwd),
                       reinterpret_cast<const void*>(redoLogRecord->data + fieldPos + 16), 4);
                ctx->dumpStream << "fwd: 0x" <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(fwd[0]) <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(fwd[1]) <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(fwd[2]) <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(fwd[3]) << "." <<
                        std::dec << fwd2 << " " << std::endl;
            }

            if ((flag & 0x02) != 0) {
                uint8_t bkw[4];
                uint16_t bkw2 = ctx->read16(redoLogRecord->data + fieldPos + 26);
                memcpy(reinterpret_cast<void*>(bkw),
                       reinterpret_cast<const void*>(redoLogRecord->data + fieldPos + 22), 4);
                ctx->dumpStream << "bkw: 0x" <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(bkw[0]) <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(bkw[1]) <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(bkw[2]) <<
                        std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(bkw[3]) << "." <<
                        std::dec << bkw2 << std::endl;
            }
        }
    }

    void OpCode::kdoOpCodeORP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 48) {
            WARNING("too short field KDO OpCode ORP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->fb = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->slot = ctx->read16(redoLogRecord->data + fieldPos + 42);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 44];

        if (fieldLength < 45 + (static_cast<uint64_t>(redoLogRecord->cc) + 7) / 8) {
            WARNING("too short field KDO OpCode ORP for nulls: " << std::dec << fieldLength <<
                    " (cc: " << std::dec << static_cast<uint64_t>(redoLogRecord->cc) << ") offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->nullsDelta = fieldPos + 45;
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;
        for (uint64_t i = 0; i < static_cast<uint64_t>(redoLogRecord->cc); ++i) {
            if ((*nulls & bits) == 0)
                redoLogRecord->ccData = i + 1;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
        }

        if ((redoLogRecord->fb & FB_L) == 0) {
            redoLogRecord->nridBdba = ctx->read32(redoLogRecord->data + fieldPos + 28);
            redoLogRecord->nridSlot = ctx->read16(redoLogRecord->data + fieldPos + 32);
        }
        redoLogRecord->sizeDelt = ctx->read16(redoLogRecord->data + fieldPos + 40);

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "tabn: " << static_cast<uint64_t>(redoLogRecord->tabn) <<
                " slot: " << std::dec << static_cast<uint64_t>(redoLogRecord->slot) <<
                "(0x" << std::hex << static_cast<uint64_t>(redoLogRecord->slot) << ")" <<
                " size/delt: " << std::dec << redoLogRecord->sizeDelt << std::endl;

            char fbStr[9] = "--------";
            processFbFlags(redoLogRecord->fb, fbStr);
            uint8_t lb = redoLogRecord->data[fieldPos + 17];

            ctx->dumpStream << "fb: " << fbStr <<
                    " lb: 0x" << std::hex << static_cast<uint64_t>(lb) << " " <<
                    " cc: " << std::dec << static_cast<uint64_t>(redoLogRecord->cc);
            if (fbStr[1] == 'C') {
                uint8_t cki = redoLogRecord->data[fieldPos + 19];
                ctx->dumpStream << " cki: " << std::dec << static_cast<uint64_t>(cki) << std::endl;
            } else
                ctx->dumpStream << std::endl;

            if ((redoLogRecord->fb & FB_L) == 0) {
                ctx->dumpStream << "nrid:  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->nridBdba << "." << std::hex <<
                        redoLogRecord->nridSlot << std::endl;
            }

            ctx->dumpStream << "null:";
            if (redoLogRecord->cc >= 11)
                ctx->dumpStream << std::endl << "01234567890123456789012345678901234567890123456789012345678901234567890123456789" << std::endl;
            else
                ctx->dumpStream << " ";

            nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            bits = 1;
            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {

                if ((*nulls & bits) != 0)
                    ctx->dumpStream << "N";
                else
                    ctx->dumpStream << "-";
                if ((i % 80) == 79 && i < redoLogRecord->cc)
                    ctx->dumpStream << std::endl;

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
            ctx->dumpStream << std::endl;
        }
    }

    void OpCode::kdoOpCodeQM(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 24) {
            WARNING("too short field KDO OpCode QMI (1): " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->nrow = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->slotsDelta = fieldPos + 20;

        if (ctx->dumpRedoLog >= 1) {
            uint8_t lock = redoLogRecord->data[fieldPos + 17];

            ctx->dumpStream << "tabn: " << static_cast<uint64_t>(redoLogRecord->tabn) <<
                " lock: " << std::dec << static_cast<uint64_t>(lock) <<
                " nrow: " << std::dec << static_cast<uint64_t>(redoLogRecord->nrow) << std::endl;

            if (fieldLength < 22 + static_cast<uint64_t>(redoLogRecord->nrow) * 2) {
                WARNING("too short field KDO OpCode QMI (2): " << std::dec << fieldLength << ", " <<
                        static_cast<uint64_t>(redoLogRecord->nrow) << " offset: " << redoLogRecord->dataOffset)
                return;
            }
        }
    }

    void OpCode::kdoOpCode(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 16) {
            WARNING("too short field KDO OpCode: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->bdba = ctx->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->op = redoLogRecord->data[fieldPos + 10];
        redoLogRecord->flags = redoLogRecord->data[fieldPos + 11];
        redoLogRecord->itli = redoLogRecord->data[fieldPos + 12];

        if (ctx->dumpRedoLog >= 1) {
            typeDba hdba = ctx->read32(redoLogRecord->data + fieldPos + 4);
            uint16_t maxFr = ctx->read16(redoLogRecord->data + fieldPos + 8);
            uint8_t ispac = redoLogRecord->data[fieldPos + 13];

            const char* opCode = "???";
            switch (redoLogRecord->op & 0x1F) {
                case OP_IUR: opCode = "IUR"; break; // Interpret Undo Redo
                case OP_IRP: opCode = "IRP"; break; // Insert Row Piece
                case OP_DRP: opCode = "DRP"; break; // Delete Row Piece
                case OP_LKR: opCode = "LKR"; break; // LocK Row
                case OP_URP: opCode = "URP"; break; // Update Row Piece
                case OP_ORP: opCode = "ORP"; break; // Overwrite Row Piece
                case OP_MFC: opCode = "MFC"; break; // Manipulate First Column
                case OP_CFA: opCode = "CFA"; break; // Change Forwarding Address
                case OP_CKI: opCode = "CKI"; break; // Change Cluster key Index
                case OP_SKL: opCode = "SKL"; break; // Set Key Links
                case OP_QMI: opCode = "QMI"; break; // Quick Multi-row Insert
                case OP_QMD: opCode = "QMD"; break; // Quick Multi-row Delete
                case OP_DSC: opCode = "DSC"; break;
                case OP_LMN: opCode = "LMN"; break;
                case OP_LLB: opCode = "LLB"; break;
                case OP_SHK: opCode = "SHK"; break;
                case OP_CMP: opCode = "CMP"; break;
                case OP_DCU: opCode = "DCU"; break;
                case OP_MRK: opCode = "MRK"; break;
                case OP_021: opCode = " 21"; break;
                default:
                    opCode = "XXX";
                    if (ctx->dumpRedoLog >= 1)
                        ctx->dumpStream << "DEBUG op: " << std::dec << static_cast<uint64_t>(redoLogRecord->op & 0x1F) << std::endl;
            }

            const char* xtype("0");
            const char* rtype("");
            switch (redoLogRecord->flags & 0x03) {
            case FLAGS_XA:
                xtype = "XA"; // Redo
                break;
            case FLAGS_XR:
                xtype = "XR"; // Rollback
                break;
            case FLAGS_CR:
                xtype = "CR"; // Unknown
                break;
            }
            redoLogRecord->flags &= 0xFC;

            if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0)
                rtype = "xtype KDO_KDOM2";

            const char* rowDependencies("Disabled");
            if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0)
                rowDependencies = "Enabled";

            ctx->dumpStream << "KDO Op code: " << opCode << " row dependencies " << rowDependencies << std::endl;
            ctx->dumpStream << "  xtype: " << xtype << rtype <<
                    " flags: 0x" << std::setfill('0') << std::setw(8) << std::hex << static_cast<uint64_t>(redoLogRecord->flags) << " " <<
                    " bdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->bdba << " " <<
                    " hdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << hdba << std::endl;
            ctx->dumpStream << "itli: " << std::dec << static_cast<uint64_t>(redoLogRecord->itli) << " " <<
                    " ispac: " << std::dec << static_cast<uint64_t>(ispac) << " " <<
                    " maxfr: " << std::dec << static_cast<uint64_t>(maxFr) << std::endl;

            switch ((redoLogRecord->op & 0x1F)) {
            case OP_SKL:
                if (fieldLength >= 32) {
                    char fwdFl = '-';
                    uint32_t fwd = (static_cast<uint32_t>(redoLogRecord->data[fieldPos + 16]) << 24) |
                            (static_cast<uint32_t>(redoLogRecord->data[fieldPos + 17]) << 16) |
                            (static_cast<uint32_t>(redoLogRecord->data[fieldPos + 18]) << 8) |
                            static_cast<uint32_t>(redoLogRecord->data[fieldPos + 19]);
                    uint16_t fwdPos = (static_cast<uint16_t>(redoLogRecord->data[fieldPos + 20]) << 8) |
                            static_cast<uint16_t>(redoLogRecord->data[fieldPos + 21]);
                    char bkwFl = '-';
                    uint32_t bkw = (static_cast<uint32_t>(redoLogRecord->data[fieldPos + 22]) << 24) |
                            (static_cast<uint32_t>(redoLogRecord->data[fieldPos + 23]) << 16) |
                            (static_cast<uint32_t>(redoLogRecord->data[fieldPos + 24]) << 8) |
                            static_cast<uint32_t>(redoLogRecord->data[fieldPos + 25]);
                    uint16_t bkwPos = (static_cast<uint16_t>(redoLogRecord->data[fieldPos + 26]) << 8) |
                            static_cast<uint16_t>(redoLogRecord->data[fieldPos + 27]);
                    uint8_t fl = redoLogRecord->data[fieldPos + 28];
                    uint8_t lock = redoLogRecord->data[fieldPos + 29];
                    uint8_t slot = redoLogRecord->data[fieldPos + 30];

                    if (fl & 0x01) fwdFl = 'F';
                    if (fl & 0x02) bkwFl = 'B';

                    ctx->dumpStream << "flag: " << fwdFl << bkwFl <<
                            " lock: " << std::dec << static_cast<uint64_t>(lock) <<
                            " slot: " << std::dec << static_cast<uint64_t>(slot) <<
                            "(0x" << std::hex << static_cast<uint64_t>(slot) << ")" << std::endl;

                    if (fwdFl == 'F')
                        ctx->dumpStream << "fwd: 0x" << std::setfill('0') << std::setw(8) << std::hex << fwd << "." << fwdPos << " " << std::endl;
                    if (bkwFl == 'B')
                        ctx->dumpStream << "bkw: 0x" << std::setfill('0') << std::setw(8) << std::hex << bkw << "." << bkwPos << std::endl;
                }
                break;

            case OP_DSC:
                if (fieldLength >= 24) {
                    uint16_t slot = ctx->read16(redoLogRecord->data + fieldPos + 16);
                    uint8_t tabn = redoLogRecord->data[fieldPos + 18];
                    uint8_t rel = redoLogRecord->data[fieldPos + 19];

                    ctx->dumpStream << "tabn: " << std::dec << static_cast<uint64_t>(tabn) << " slot: " << slot <<
                            "(0x" << std::hex << slot << ")" << std::endl;
                    ctx->dumpStream << "piece relative column number: " << std::dec << static_cast<uint64_t>(rel) << std::endl;
                }
                break;
            }
        }

        switch (redoLogRecord->op & 0x1F) {
        case OP_IRP: kdoOpCodeIRP(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_DRP: kdoOpCodeDRP(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_LKR: kdoOpCodeLKR(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_URP: kdoOpCodeURP(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_ORP: kdoOpCodeORP(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_CKI: kdoOpCodeSKL(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_CFA: kdoOpCodeCFA(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_QMI:
        case OP_QMD: kdoOpCodeQM(ctx, redoLogRecord, fieldPos, fieldLength);
                     break;
        }
    }

    void OpCode::ktub(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, bool isKtubl) {
        if (fieldLength < 24) {
            WARNING("too short field ktub: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->obj = ctx->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data + fieldPos + 4);
        redoLogRecord->tsn = ctx->read32(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->undo = ctx->read32(redoLogRecord->data + fieldPos + 12);
        redoLogRecord->opc = (static_cast<typeOp1>(redoLogRecord->data[fieldPos + 16]) << 8) | redoLogRecord->data[fieldPos + 17];
        redoLogRecord->slt = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->rci = redoLogRecord->data[fieldPos + 19];
        redoLogRecord->flg = ctx->read16(redoLogRecord->data + fieldPos + 20);

        const char* ktuType("ktubu");
        const char* prevObj("");
        const char* postObj("");
        bool ktubl = false;

        if ((redoLogRecord->flg & FLG_BEGIN_TRANS) != 0 && isKtubl) {
            ktubl = true;
            ktuType = "ktubl";
            if (ctx->version < REDO_VERSION_19_0) {
                prevObj = "[";
                postObj = "]";
            }
        }

        if (ctx->version < REDO_VERSION_19_0) {
            ctx->dumpStream <<
                    ktuType << " redo:" <<
                    " slt: " << std::dec << static_cast<uint64_t>(redoLogRecord->slt) <<
                    " rci: " << std::dec << static_cast<uint64_t>(redoLogRecord->rci) <<
                    " opc: " << std::dec << static_cast<uint64_t>(redoLogRecord->opc >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opc & 0xFF) <<
                    " " << prevObj << "objn: " << std::dec << redoLogRecord->obj <<
                    " objd: " << std::dec << redoLogRecord->dataObj <<
                    " tsn: " << std::dec << redoLogRecord->tsn << postObj << std::endl;
        } else {
            typeDba prevDba = ctx->read32(redoLogRecord->data + fieldPos + 12);
            uint16_t wrp = ctx->read16(redoLogRecord->data + fieldPos + 22);

            ctx->dumpStream <<
                    ktuType << " redo:" <<
                    " slt: "  << std::dec << static_cast<uint64_t>(redoLogRecord->slt) <<
                    " wrp: " << std::dec << wrp <<
                    " flg: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord->flg <<
                    " prev dba:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevDba <<
                    " rci: " << std::dec << static_cast<uint64_t>(redoLogRecord->rci) <<
                    " opc: " << std::dec << static_cast<uint64_t>(redoLogRecord->opc >> 8) << "." << static_cast<uint64_t>(redoLogRecord->opc & 0xFF) <<
                    " [objn: " << std::dec << redoLogRecord->obj <<
                    " objd: " << std::dec << redoLogRecord->dataObj <<
                    " tsn: " << std::dec << redoLogRecord->tsn << "]" << std::endl;
        }

        const char* lastBufferSplit;
        if ((redoLogRecord->flg & FLG_LASTBUFFERSPLIT) != 0)
            lastBufferSplit = "Yes";
        else {
            if (ctx->version < REDO_VERSION_19_0)
                lastBufferSplit = "No";
            else
                lastBufferSplit = " No";
        }

        const char* userUndoDone;
        if ((redoLogRecord->flg & FLG_USERUNDODDONE) != 0)
            userUndoDone = "Yes";
        else {
            if (ctx->version < REDO_VERSION_19_0)
                userUndoDone = "No";
            else
                userUndoDone = " No";
        }

        const char* undoType;
        if (ctx->version < REDO_VERSION_12_2) {
            if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                undoType = "Multi-block undo - HEAD";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                undoType = "Multi-Block undo - TAIL";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                undoType = "Multi-block undo - MID";
            else
                undoType = "Regular undo      ";
        } else if (ctx->version < REDO_VERSION_19_0) {
            if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                undoType = "Multi-block undo - HEAD";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                undoType = "Multi-Block undo - TAIL";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                undoType = "Multi-Block undo - MID";
            else
                undoType = "Regular undo      ";
        } else {
            if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                undoType = "MBU - HEAD  ";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                undoType = "MBU - TAIL  ";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                undoType = "MBU - MID   ";
            else
                undoType = "Regular undo";
        }

        const char* tempObject;
        if ((redoLogRecord->flg & FLG_ISTEMPOBJECT) != 0)
            tempObject = "Yes";
        else {
            if (ctx->version < REDO_VERSION_19_0)
                tempObject = "No";
            else
                tempObject = " No";
        }

        const char* tablespaceUndo;
        if ((redoLogRecord->flg & FLG_TABLESPACEUNDO) != 0)
            tablespaceUndo = "Yes";
        else {
            if (ctx->version < REDO_VERSION_19_0)
                tablespaceUndo = "No";
            else
                tablespaceUndo = " No";
        }

        const char* userOnly(" No");
        if ((redoLogRecord->flg & FLG_USERONLY) != 0)
            userOnly = "Yes";
        else {
            if (ctx->version < REDO_VERSION_19_0)
                userOnly = "No";
            else
                userOnly = " No";
        }

        if (ctx->dumpRedoLog < 1)
            return;

        if (ktubl) {
            // KTUBL
            if (fieldLength < 28) {
                WARNING("too short field ktubl: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
                return;
            }

            if (fieldLength == 28) {
                uint16_t flg2 = ctx->read16(redoLogRecord->data + fieldPos + 24);
                auto buExtIdx = (int16_t)ctx->read16(redoLogRecord->data + fieldPos + 26);

                if (ctx->version < REDO_VERSION_19_0) {
                    ctx->dumpStream <<
                            "Undo type:  " << undoType << "  " <<
                            "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                            "Temp Object:  " << tempObject << " " << std::endl <<
                            "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                            "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " << std::endl;

                    ctx->dumpStream <<
                            " BuExt idx: " << std::dec << buExtIdx <<
                            " flg2: " << std::hex << flg2 << std::endl;
                } else {
                    ctx->dumpStream <<
                            "[Undo type  ] " << undoType << " " <<
                            " [User undo done   ] " << userUndoDone << " " <<
                            " [Last buffer split] " << lastBufferSplit << " " << std::endl <<
                            "[Temp object]          " << tempObject << " " <<
                            " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                            " [User only        ] " << userOnly << " " << std::endl <<
                            "Begin trans    " << std::endl;

                    ctx->dumpStream <<
                            "BuExt idx: " << std::dec << buExtIdx <<
                            " flg2: " << std::hex << flg2 << std::endl;
                }
            } else if (fieldLength >= 76) {
                uint16_t flg2 = ctx->read16(redoLogRecord->data + fieldPos + 24);
                auto buExtIdx = (int16_t)ctx->read16(redoLogRecord->data + fieldPos + 26);
                typeUba prevCtlUba = ctx->read56(redoLogRecord->data + fieldPos + 28);
                typeScn prevCtlMaxCmtScn = ctx->readScn(redoLogRecord->data + fieldPos + 36);
                typeScn prevTxCmtScn = ctx->readScn(redoLogRecord->data + fieldPos + 44);
                typeScn txStartScn = ctx->readScn(redoLogRecord->data + fieldPos + 56);
                uint32_t prevBrb = ctx->read32(redoLogRecord->data + fieldPos + 64);
                uint32_t prevBcl = ctx->read32(redoLogRecord->data + fieldPos + 68);
                uint32_t logonUser = ctx->read32(redoLogRecord->data + fieldPos + 72);

                if (ctx->version < REDO_VERSION_12_2) {
                    ctx->dumpStream <<
                            "Undo type:  " << undoType << "  " <<
                            "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                            "Temp Object:  " << tempObject << " " << std::endl <<
                            "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                            "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " <<
                            " prev ctl uba: " << PRINTUBA(prevCtlUba) << " " << std::endl <<
                            "prev ctl max cmt scn:  " << PRINTSCN48(prevCtlMaxCmtScn) << " " <<
                            " prev tx cmt scn:  " << PRINTSCN48(prevTxCmtScn) << " " << std::endl;

                    ctx->dumpStream <<
                            "txn start scn:  " << PRINTSCN48(txStartScn) << " " <<
                            " logon user: " << std::dec << logonUser << " " <<
                            " prev brb: " << prevBrb << " " <<
                            " prev bcl: " << std::dec << prevBcl;

                    ctx->dumpStream <<
                            " BuExt idx: " << std::dec << buExtIdx <<
                            " flg2: " << std::hex << flg2 << std::endl;
                } else if (ctx->version < REDO_VERSION_19_0) {
                    ctx->dumpStream <<
                            "Undo type:  " << undoType << "  " <<
                            "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                            "Temp Object:  " << tempObject << " " << std::endl <<
                            "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                            "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " <<
                            " prev ctl uba: " << PRINTUBA(prevCtlUba) << " " << std::endl <<
                            "prev ctl max cmt scn:  " << PRINTSCN64(prevCtlMaxCmtScn) << " " <<
                            " prev tx cmt scn:  " << PRINTSCN64(prevTxCmtScn) << " " << std::endl;

                    ctx->dumpStream <<
                            "txn start scn:  " << PRINTSCN64(txStartScn) << " " <<
                            " logon user: " << std::dec << logonUser << " " <<
                            " prev brb: " << prevBrb << " " <<
                            " prev bcl: " << std::dec << prevBcl;

                    ctx->dumpStream <<
                            " BuExt idx: " << std::dec << buExtIdx <<
                            " flg2: " << std::hex << flg2 << std::endl;
                } else {
                    ctx->dumpStream <<
                            "[Undo type  ] " << undoType << " " <<
                            " [User undo done   ] " << userUndoDone << " " <<
                            " [Last buffer split] " << lastBufferSplit << " " << std::endl <<
                            "[Temp object]          " << tempObject << " " <<
                            " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                            " [User only        ] " << userOnly << " " << std::endl <<
                            "Begin trans    " << std::endl <<
                            " prev ctl uba: " << PRINTUBA(prevCtlUba) <<
                            " prev ctl max cmt scn:  " << PRINTSCN64(prevCtlMaxCmtScn) << " " << std::endl <<
                            " prev tx cmt scn:  " << PRINTSCN64(prevTxCmtScn) << " " << std::endl;

                    ctx->dumpStream <<
                            " txn start scn:  " << PRINTSCN64(txStartScn) <<
                            "  logon user: " << std::dec << logonUser << std::endl <<
                            " prev brb:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevBrb <<
                            "  prev bcl:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevBcl << std::endl;

                    ctx->dumpStream <<
                            "BuExt idx: " << std::dec << buExtIdx <<
                            " flg2: " << std::hex << flg2 << std::endl;
                }
            }
        } else {
            // KTUBU
            if (ctx->version < REDO_VERSION_19_0) {
                ctx->dumpStream <<
                        "Undo type:  " << undoType << " " <<
                        "Undo type:  ";
                if ((redoLogRecord->flg & FLG_USERUNDODDONE) != 0)
                    ctx->dumpStream << "User undo done   ";
                if ((redoLogRecord->flg & FLG_BEGIN_TRANS) != 0)
                    ctx->dumpStream << " Begin trans    ";
                ctx->dumpStream <<
                        "Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                        "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                        "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << std::endl;

                if ((redoLogRecord->flg & FLG_BUEXT) != 0) {
                    uint16_t flg2 = ctx->read16(redoLogRecord->data + fieldPos + 24);
                    auto buExtIdx = (int16_t)ctx->read16(redoLogRecord->data + fieldPos + 26);

                    ctx->dumpStream <<
                            "BuExt idx: " << std::dec << buExtIdx <<
                            " flg2: " << std::hex << flg2 << std::endl;
                }

            } else {
                ctx->dumpStream <<
                        "[Undo type  ] " << undoType << " " <<
                        " [User undo done   ] " << userUndoDone << " " <<
                        " [Last buffer split] " << lastBufferSplit << " " << std::endl <<
                        "[Temp object]          " << tempObject << " " <<
                        " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                        " [User only        ] " << userOnly << " " << std::endl;
            }
        }
    }

    void OpCode::dumpMemory(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "Dump of memory from 0xXXXXXXXXXXXXXXXX to 0xXXXXXXXXXXXXXXXX" << std::endl;

            uint64_t start = fieldPos & 0xFFFFFFFFFFFFFFF0;
            uint64_t end = (fieldPos + fieldLength + 15) & 0xFFFFFFFFFFFFFFF0;
            for (uint64_t i = start; i < end; i += 16) {
                ctx->dumpStream << "XXXXXXXXXXXX";

                int64_t first = -1, last = -1;
                for (uint64_t j = 0; j < 4; ++j) {
                    if (i + j * 4 >= fieldPos && i + j * 4 < fieldPos + fieldLength) {
                        if (first == -1)
                            first = j;
                        last = j;
                        uint32_t val = ctx->read32(redoLogRecord->data + i + j * 4);
                        ctx->dumpStream << " " << std::setfill('0') << std::setw(8) << std::hex << std::uppercase << val;
                    } else {
                        ctx->dumpStream << "         ";
                    }
                }
                ctx->dumpStream << "  ";

                for (int64_t j = 0; j < first; ++j)
                    ctx->dumpStream << "    ";
                ctx->dumpStream << "[";

                for (int64_t j = first; j <= last; ++j)
                    ctx->dumpStream << "....";
                ctx->dumpStream << "]" << std::endl;
            }
            ctx->dumpStream << std::nouppercase;
        }
    }

    void OpCode::dumpColsVector(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum) {
        uint64_t pos = 0;

        ctx->dumpStream << "Vector content: " << std::endl;

        for (uint64_t k = 0; k < redoLogRecord->cc; ++k) {
            uint16_t fieldLength = data[pos];
            ++pos;
            uint8_t isNull = (fieldLength == 0xFF);

            if (fieldLength == 0xFE) {
                fieldLength = ctx->read16(data + pos);
                pos += 2;
            }

            dumpCols(ctx, redoLogRecord, data + pos, colnum + k, fieldLength, isNull);

            if (!isNull)
                pos += fieldLength;
        }
    }

    void OpCode::dumpCompressed(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint16_t fieldLength) {
        std::ostringstream ss;
        ss << "kdrhccnt=" << std::dec << static_cast<uint64_t>(redoLogRecord->cc) << ",full row:";
        ss << std::uppercase;

        for (uint64_t j = 0; j < fieldLength; ++j) {
            ss << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]);
            if (ss.str().length() > 256) {
                ctx->dumpStream << ss.str() << std::endl;
                ss.str(std::string());
            }
        }

        if (ss.str().length()  > 0) {
            ctx->dumpStream << ss.str() << std::endl;
        }
    }

    void OpCode::dumpCols(Ctx* ctx, RedoLogRecord* redoLogRecord __attribute__((unused)), uint8_t* data, uint64_t colnum, uint16_t fieldLength,
                          uint8_t isNull) {
        if (isNull) {
            ctx->dumpStream << "col " << std::setfill(' ') << std::setw(2) << std::dec << colnum << ": *NULL*" << std::endl;
        } else {
            ctx->dumpStream << "col " << std::setfill(' ') << std::setw(2) << std::dec << colnum << ": " <<
                    "[" << std::setfill(' ') << std::setw(2) << std::dec << fieldLength << "]";

            if (fieldLength <= 20)
                ctx->dumpStream << " ";
            else
                ctx->dumpStream << std::endl;

            for (uint64_t j = 0; j < fieldLength; ++j) {
                ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]);
                if ((j % 25) == 24 && j != static_cast<uint64_t>(fieldLength) - 1)
                    ctx->dumpStream << std::endl;
            }

            ctx->dumpStream << std::endl;
        }
    }

    void OpCode::dumpRows(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data) {
        if (ctx->dumpRedoLog >= 1) {
            uint64_t pos = 0;
            char fbStr[9] = "--------";

            for (uint64_t r = 0; r < redoLogRecord->nrow; ++r) {
                ctx->dumpStream << "slot[" << std::dec << r << "]: " << std::dec << ctx->read16(redoLogRecord->data + redoLogRecord->slotsDelta + r * 2) <<
                        std::endl;
                processFbFlags(data[pos + 0], fbStr);
                uint8_t lb = data[pos + 1];
                uint8_t jcc = data[pos + 2];
                uint16_t tl = ctx->read16(redoLogRecord->data + redoLogRecord->rowLenghsDelta + r * 2);

                ctx->dumpStream << "tl: " << std::dec << tl <<
                        " fb: " << fbStr <<
                        " lb: 0x" << std::hex << static_cast<uint64_t>(lb) << " " <<
                        " cc: " << std::dec << static_cast<uint64_t>(jcc) << std::endl;
                pos += 3;

                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    if (ctx->version < REDO_VERSION_12_2)
                        pos += 6;
                    else
                        pos += 8;
                }

                for (uint64_t k = 0; k < jcc; ++k) {
                    uint16_t fieldLength = data[pos];
                    ++pos;
                    uint8_t isNull = (fieldLength == 0xFF);

                    if (fieldLength == 0xFE) {
                        fieldLength = ctx->read16(data + pos);
                        pos += 2;
                    }

                    dumpCols(ctx, redoLogRecord, data + pos, k, fieldLength, isNull);

                    if (!isNull)
                        pos += fieldLength;
                }
            }
        }
    }

    void OpCode::dumpVal(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, const char* msg) {
        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << msg;
            for (uint64_t i = 0; i < fieldLength; ++i)
                ctx->dumpStream << redoLogRecord->data[fieldPos + i];
            ctx->dumpStream << std::endl;
        }
    }

    void OpCode::dumpHex(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        std::string header = "## 0: [" + std::to_string(redoLogRecord->dataOffset) + "] " + std::to_string(redoLogRecord->fieldLengthsDelta);
        ctx->dumpStream << header;
        if (header.length() < 36)
            ctx->dumpStream << std::string(36 - header.length(), ' ');

        for (uint64_t j = 0; j < redoLogRecord->fieldLengthsDelta; ++j)
            ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data[j]) << " ";
        ctx->dumpStream << std::endl;

        uint64_t fieldPosLocal = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = ctx->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            header = "## " + std::to_string(i) + ": [" + std::to_string(redoLogRecord->dataOffset + fieldPosLocal) + "] " + std::to_string(fieldLength) +  "   ";
            ctx->dumpStream << header;
            if (header.length() < 36)
                ctx->dumpStream << std::string(36 - header.length(), ' ');

            for (uint64_t j = 0; j < fieldLength; ++j)
                ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data[fieldPosLocal + j]) << " ";
            ctx->dumpStream << std::endl;

            fieldPosLocal += (fieldLength + 3) & 0xFFFC;
        }
    }

    void OpCode::processFbFlags(uint8_t fb, char* fbStr) {
        if ((fb & FB_N) != 0) fbStr[7] = 'N'; else fbStr[7] = '-'; // Last column continues in Next piece
        if ((fb & FB_P) != 0) fbStr[6] = 'P'; else fbStr[6] = '-'; // First column continues from Previous piece
        if ((fb & FB_L) != 0) fbStr[5] = 'L'; else fbStr[5] = '-'; // Last ctx piece
        if ((fb & FB_F) != 0) fbStr[4] = 'F'; else fbStr[4] = '-'; // First ctx piece
        if ((fb & FB_D) != 0) fbStr[3] = 'D'; else fbStr[3] = '-'; // Deleted row
        if ((fb & FB_H) != 0) fbStr[2] = 'H'; else fbStr[2] = '-'; // Head piece of row
        if ((fb & FB_C) != 0) fbStr[1] = 'C'; else fbStr[1] = '-'; // Clustered table member
        if ((fb & FB_K) != 0) fbStr[0] = 'K'; else fbStr[0] = '-'; // Cluster Key
        fbStr[8] = 0;
    }
}
