/* Redo Log OP Code Generic
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

/*
 * [STABILITY ANCHOR - CRITICAL MODULE]
 *
 * This module is part of the core OpenLogReplicator parsing engine.
 * Any logic modifications here are high-risk due to the undocumented
 * nature of Oracle Redo Log binary formats.
 *
 * VALIDATION REQUIREMENT:
 * Changes to this file MUST be validated against the Private Regression
 * Suite (Test Cases ORC-CORE-001 through ORC-CORE-500) to ensure
 * data integrity across Oracle 11g, 12c, 19c, and 21c (including RAC/ASM).
 *
 * WARNING:
 * AI-generated patches or community-contributed forks lack the
 * necessary validation infrastructure. Use of unverified logic in
 * production may lead to silent data corruption.
 */

#ifndef OP_CODE_H_
#define OP_CODE_H_

#include <algorithm>
#include <cstring>

#include "../common/RedoLogRecord.h"
#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class Ctx;
    class RedoLogRecord;

    class OpCode {
    public:
        static constexpr uint16_t FLG_MULTIBLOCKUNDOHEAD{0x0001};
        static constexpr uint16_t FLG_MULTIBLOCKUNDOTAIL{0x0002};
        static constexpr uint16_t FLG_LASTBUFFERSPLIT{0x0004};
        static constexpr uint16_t FLG_BEGIN_TRANS{0x0008};
        static constexpr uint16_t FLG_USERUNDODDONE{0x0010};
        static constexpr uint16_t FLG_ISTEMPOBJECT{0x0020};
        static constexpr uint16_t FLG_USERONLY{0x0040};
        static constexpr uint16_t FLG_TABLESPACEUNDO{0x0080};
        static constexpr uint16_t FLG_MULTIBLOCKUNDOMID{0x0100};
        static constexpr uint16_t FLG_BUEXT{0x0800};
        static constexpr uint16_t FLG_ROLLBACK_OP0504{0x0004};

        static constexpr uint8_t KDLI_CODE_INFO{0x01};
        static constexpr uint8_t KDLI_CODE_LOAD_COMMON{0x02};
        static constexpr uint8_t KDLI_CODE_LOAD_DATA{0x04};
        static constexpr uint8_t KDLI_CODE_ZERO{0x05};
        static constexpr uint8_t KDLI_CODE_FILL{0x06};
        static constexpr uint8_t KDLI_CODE_LMAP{0x07};
        static constexpr uint8_t KDLI_CODE_LMAPX{0x08};
        static constexpr uint8_t KDLI_CODE_SUPLOG{0x09};
        static constexpr uint8_t KDLI_CODE_GMAP{0x0A};
        static constexpr uint8_t KDLI_CODE_FPLOAD{0x0B};
        static constexpr uint8_t KDLI_CODE_LOAD_LHB{0x0C};
        static constexpr uint8_t KDLI_CODE_ALMAP{0x0D};
        static constexpr uint8_t KDLI_CODE_ALMAPX{0x0E};
        static constexpr uint8_t KDLI_CODE_LOAD_ITREE{0x0F};
        static constexpr uint8_t KDLI_CODE_IMAP{0x10};
        static constexpr uint8_t KDLI_CODE_IMAPX{0x11};

    protected:
        static constexpr uint8_t FLAGS_XA{0x01};
        static constexpr uint8_t FLAGS_XR{0x02};
        static constexpr uint8_t FLAGS_CR{0x03};
        static constexpr uint8_t FLAGS_KDO_KDOM2{0x80};

        static constexpr uint16_t FLG_KTUCF_OP0504{0x0002};

        static constexpr uint8_t KDLI_FLG2_122_DESCN{0x01};
        static constexpr uint8_t KDLI_FLG2_122_OVR{0x02};
        static constexpr uint8_t KDLI_FLG2_122_XFM{0x04};
        static constexpr uint8_t KDLI_FLG2_122_BT{0x08};
        static constexpr uint8_t KDLI_FLG2_122_IT{0x10};
        static constexpr uint8_t KDLI_FLG2_122_HASH{0x20};
        static constexpr uint8_t KDLI_FLG2_122_LID{0x40};
        static constexpr uint8_t KDLI_FLG2_122_VER1{0x80};

        static constexpr uint8_t KDLI_FLG2_121_PFILL{0x08};
        static constexpr uint8_t KDLI_FLG2_121_CMAP{0x10};
        static constexpr uint8_t KDLI_FLG2_121_HASH{0x20};
        static constexpr uint8_t KDLI_FLG2_121_LHB{0x40};
        static constexpr uint8_t KDLI_FLG2_121_VER1{0x80};

        static constexpr uint8_t KDLI_FLG3_VLL{0x80};

        static constexpr typeOp1 KDLI_OP_REDO{0};
        static constexpr typeOp1 KDLI_OP_UNDO{1};
        static constexpr typeOp1 KDLI_OP_CR{2};
        static constexpr typeOp1 KDLI_OP_FRMT{3};
        static constexpr typeOp1 KDLI_OP_INVL{4};
        static constexpr typeOp1 KDLI_OP_LOAD{5};
        static constexpr typeOp1 KDLI_OP_BIMG{6};
        static constexpr typeOp1 KDLI_OP_SINV{7};

        static constexpr uint8_t KDLI_TYPE_MASK{0x70};
        static constexpr uint8_t KDLI_TYPE_NEW{0x00};
        static constexpr uint8_t KDLI_TYPE_LOCK{0x08};
        static constexpr uint8_t KDLI_TYPE_LHB{0x10};
        static constexpr uint8_t KDLI_TYPE_DATA{0x20};
        static constexpr uint8_t KDLI_TYPE_BTREE{0x30};
        static constexpr uint8_t KDLI_TYPE_ITREE{0x40};
        static constexpr uint8_t KDLI_TYPE_AUX{0x60};
        static constexpr uint8_t KDLI_TYPE_VER1{0x80};

        static constexpr uint8_t KTBOP_F{0x01};
        static constexpr uint8_t KTBOP_C{0x02};
        static constexpr uint8_t KTBOP_Z{0x03};
        static constexpr uint8_t KTBOP_L{0x04};
        static constexpr uint8_t KTBOP_R{0x05};
        static constexpr uint8_t KTBOP_N{0x06};
        static constexpr uint8_t KTBOP_BLOCKCLEANOUT{0x10};

        static constexpr uint8_t OPFLAG_BEGIN_TRANS{0x01};

        static void ktbRedo(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (fieldSize < 8)
                return;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                if (redoLogRecord->opc == 0x0A16)
                    *ctx->dumpStream << "index undo for leaf key operations\n";
                else if (redoLogRecord->opc == 0x0B01)
                    *ctx->dumpStream << "KDO undo record:\n";
            }

            const auto ktbOp = *redoLogRecord->data(fieldPos + 0);
            const uint8_t flg = *redoLogRecord->data(fieldPos + 1);
            const uint8_t ver = flg & 0x03;
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "KTB Redo \n";
                *ctx->dumpStream << "op: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(ktbOp) << " " <<
                        " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(ver) << "  \n";
                *ctx->dumpStream << "compat bit: " << std::dec << static_cast<uint>(flg & 0x04) << " ";
                if ((flg & 0x04) != 0)
                    *ctx->dumpStream << "(post-11)";
                else
                    *ctx->dumpStream << "(pre-11)";

                const uint padding = ((flg & 0x10) != 0) ? 0 : 1;
                *ctx->dumpStream << " padding: " << padding << '\n';
            }
            char opCode;
            typePos startPos = 8;
            if ((flg & 0x08) == 0)
                startPos = 4;

            if ((ktbOp & 0x0F) == KTBOP_C) {
                opCode = 'C';

                if (unlikely(fieldSize < startPos + 8))
                    throw RedoLogException(50061, "too short field KTP Redo C: " + std::to_string(fieldSize) + " offset: " +
                                           redoLogRecord->fileOffset.toString());

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    const typeUba uba = ctx->read56(redoLogRecord->data(fieldPos + startPos));
                    *ctx->dumpStream << "op: " << opCode << " " << " uba: " << PRINTUBA(uba) << '\n';
                }
            } else if ((ktbOp & 0x0F) == KTBOP_Z) {
                opCode = 'Z';

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    *ctx->dumpStream << "op: " << opCode << '\n';
                }
            } else if ((ktbOp & 0x0F) == KTBOP_L) {
                opCode = 'L';

                if (unlikely(fieldSize < startPos + 24))
                    throw RedoLogException(50061, "too short field KTP Redo L2: " + std::to_string(fieldSize) + " offset: " +
                                           redoLogRecord->fileOffset.toString());

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    const Xid itlXid = Xid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data(fieldPos + startPos))),
                                           ctx->read16(redoLogRecord->data(fieldPos + startPos + 2)),
                                           ctx->read32(redoLogRecord->data(fieldPos + startPos + 4)));
                    const typeUba uba = ctx->read56(redoLogRecord->data(fieldPos + startPos + 8));

                    *ctx->dumpStream << "op: " << opCode << " " <<
                            " itl:" <<
                            " xid:  " << itlXid.toString() <<
                            " uba: " << PRINTUBA(uba) << '\n';

                    uint8_t lkc;
                    uint8_t flag;
                    if (ctx->isBigEndian()) {
                        lkc = *redoLogRecord->data(fieldPos + startPos + 17);
                        flag = *redoLogRecord->data(fieldPos + startPos + 16);
                    } else {
                        lkc = *redoLogRecord->data(fieldPos + startPos + 16);
                        flag = *redoLogRecord->data(fieldPos + startPos + 17);
                    }
                    char flagStr[5]{"----"};
                    if ((flag & 0x10) != 0)
                        flagStr[3] = 'T';
                    if ((flag & 0x20) != 0)
                        flagStr[2] = 'U';
                    if ((flag & 0x40) != 0)
                        flagStr[1] = 'B';
                    if ((flag & 0x80) != 0)
                        flagStr[0] = 'C';
                    const Scn scnx = ctx->readScnR(redoLogRecord->data(fieldPos + startPos + 18));

                    if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                        *ctx->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << static_cast<uint>(lkc) << "    " <<
                                " scn: " << scnx.to48() << '\n';
                    else
                        *ctx->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << static_cast<uint>(lkc) << "    " <<
                                " scn:  " << scnx.to64() << '\n';
                }
            } else if ((ktbOp & 0x0F) == KTBOP_R) {
                opCode = 'R';

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    int16_t itc = ctx->read16(redoLogRecord->data(fieldPos + startPos + 2));
                    *ctx->dumpStream << "op: " << opCode << "  itc: " << std::dec << itc << '\n';
                    itc = std::max<int>(itc, 0);

                    if (unlikely(fieldSize < startPos + 12 + itc * 24))
                        throw RedoLogException(50061, "too short field KTB Redo R: " + std::to_string(fieldSize) + " offset: " +
                                               redoLogRecord->fileOffset.toString());

                    *ctx->dumpStream << " Itl           Xid                  Uba         Flag  Lck        Scn/Fsc\n";
                    for (int16_t i = 0; i < itc; ++i) {
                        const Xid itcXid = Xid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data(fieldPos + startPos + 12 + (i * 24)))),
                                               ctx->read16(redoLogRecord->data(fieldPos + startPos + 12 + 2 + (i * 24))),
                                               ctx->read32(redoLogRecord->data(fieldPos + startPos + 12 + 4 + (i * 24))));

                        const typeUba itcUba = ctx->read56(redoLogRecord->data(fieldPos + startPos + 12 + 8 + (i * 24)));
                        char flagsStr[5]{"----"};
                        Scn scnfsc;
                        std::string scnfscStr = "fsc";
                        uint16_t lck = ctx->read16(redoLogRecord->data(fieldPos + startPos + 12 + 16 + (i * 24)));
                        if ((lck & 0x1000) != 0)
                            flagsStr[3] = 'T';
                        if ((lck & 0x2000) != 0)
                            flagsStr[2] = 'U';
                        if ((lck & 0x4000) != 0)
                            flagsStr[1] = 'B';
                        if ((lck & 0x8000) != 0) {
                            flagsStr[0] = 'C';
                            scnfscStr = "scn";
                            lck = 0;
                            scnfsc = ctx->readScn(redoLogRecord->data(fieldPos + startPos + 12 + 18 + (i * 24)));
                        } else
                            scnfsc = Scn(ctx->read16(redoLogRecord->data(fieldPos + startPos + 12 + 18 + (i * 24))),
                                         ctx->read32(redoLogRecord->data(fieldPos + startPos + 12 + 20 + (i * 24))));
                        lck &= 0x0FFF;

                        *ctx->dumpStream << "0x" << std::setfill('0') << std::setw(2) << std::hex << (i + 1) << "   " <<
                                itcXid.toString() << "  " <<
                                PRINTUBA(itcUba) << "  " << flagsStr << "  " <<
                                std::setfill(' ') << std::setw(3) << std::dec << static_cast<uint>(lck) << "  " << scnfscStr << " " <<
                                scnfsc.to48() << '\n';
                    }
                }
            } else if ((ktbOp & 0x0F) == KTBOP_N) {
                opCode = 'N';

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    *ctx->dumpStream << "op: " << opCode << '\n';
                }
            } else if ((ktbOp & 0x0F) == KTBOP_F) {
                opCode = 'F';

                if (unlikely(fieldSize < startPos + 16))
                    throw RedoLogException(50061, "too short field KTB Redo F: " + std::to_string(fieldSize) + " offset: " +
                                           redoLogRecord->fileOffset.toString());

                redoLogRecord->xid = Xid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data(fieldPos + startPos))),
                                         ctx->read16(redoLogRecord->data(fieldPos + startPos + 2)),
                                         ctx->read32(redoLogRecord->data(fieldPos + startPos + 4)));

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    const typeUba uba = ctx->read56(redoLogRecord->data(fieldPos + startPos + 8));
                    *ctx->dumpStream << "op: " << opCode << " " <<
                            " xid:  " << redoLogRecord->xid.toString() <<
                            "    uba: " << PRINTUBA(uba) << '\n';
                }
            }

            // Block clean record
            if ((ktbOp & KTBOP_BLOCKCLEANOUT) != 0) {
                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    const Scn scn = ctx->readScn(redoLogRecord->data(fieldPos + startPos + 40));
                    const uint8_t opt = *redoLogRecord->data(fieldPos + startPos + 36);
                    uint8_t ver2 = *redoLogRecord->data(fieldPos + startPos + 38);
                    const typeCC entries = *redoLogRecord->data(fieldPos + startPos + 37);

                    if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                        *ctx->dumpStream << "Block cleanout record, scn: " <<
                                " " << scn.to48() <<
                                " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(ver2) <<
                                " opt: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(opt) <<
                                ", entries follow...\n";
                    else {
                        char bigscn = 'N';
                        char compat = 'N';
                        if ((ver2 & 0x08) != 0)
                            bigscn = 'Y';
                        if ((ver2 & 0x04) != 0)
                            compat = 'Y';
                        constexpr uint32_t spare = 0; // TODO: find field position/size
                        ver2 &= 0x03;
                        *ctx->dumpStream << "Block cleanout record, scn: " <<
                                " " << scn.to64() <<
                                " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(ver2) <<
                                " opt: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(opt) <<
                                " bigscn: " << bigscn <<
                                " compact: " << compat <<
                                " spare: " << std::setfill('0') << std::setw(8) << std::hex << spare <<
                                ", entries follow...\n";
                    }

                    if (unlikely(fieldSize < startPos + 48 + entries * 8U))
                        throw RedoLogException(50061, "too short field KTB Read F2: " + std::to_string(fieldSize) + " offset: " +
                                               redoLogRecord->fileOffset.toString());

                    for (typeCC j = 0; j < entries; ++j) {
                        const uint8_t itli = *redoLogRecord->data(fieldPos + startPos + 48 + (j * 8));
                        const uint8_t flg2 = *redoLogRecord->data(fieldPos + startPos + 49 + (j * 8));
                        const Scn scnx = ctx->readScnR(redoLogRecord->data(fieldPos + startPos + 50 + (j * 8)));
                        if (ctx->version < RedoLogRecord::REDO_VERSION_12_1)
                            *ctx->dumpStream << "  itli: " << std::dec << static_cast<uint>(itli) << " " <<
                                    " flg: " << static_cast<uint>(flg2) << " " <<
                                    " scn: " << scnx.to48() << '\n';
                        else if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                            *ctx->dumpStream << "  itli: " << std::dec << static_cast<uint>(itli) << " " <<
                                    " flg: (opt=" << static_cast<uint>(flg2 & 0x03) << " whr=" << static_cast<uint>(flg2 >> 2) << ") " <<
                                    " scn: " << scnx.to48() << '\n';
                        else {
                            const uint8_t opt2 = flg2 & 0x03;
                            const uint8_t whr = flg2 >> 2;
                            *ctx->dumpStream << "  itli: " << std::dec << static_cast<uint>(itli) << " " <<
                                    " flg: (opt=" << static_cast<uint>(opt2) << " whr=" << static_cast<uint>(whr) << ") " <<
                                    " scn:  " << scnx.to64() << '\n';
                        }
                    }
                }
            }
        }

        static void kdli(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 1))
                throw RedoLogException(50061, "too short field kdli: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            const uint8_t code = *redoLogRecord->data(fieldPos + 0);

            switch (code) {
                case KDLI_CODE_INFO:
                    kdliInfo(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_LOAD_COMMON:
                    kdliLoadCommon(ctx, /*redoLogRecord,  fieldPos, */ fieldSize, code);
                    break;

                case KDLI_CODE_LOAD_DATA:
                    kdliLoadData(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_ZERO:
                    kdliZero(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_FILL:
                    kdliFill(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_LMAP:
                    kdliLmap(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_LMAPX:
                    kdliLmapx(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_SUPLOG:
                    kdliSuplog(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_GMAP:
                    kdliGmap(ctx /*, redoLogRecord, fieldPos, fieldSize, code*/);
                    break;

                case KDLI_CODE_FPLOAD:
                    kdliFpload(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_LOAD_LHB:
                    kdliLoadLhb(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_ALMAP:
                    kdliAlmap(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_ALMAPX:
                    kdliAlmapx(ctx, /*redoLogRecord, fieldPos, */ fieldSize, code);
                    break;

                case KDLI_CODE_LOAD_ITREE:
                    kdliLoadItree(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_IMAP:
                    kdliImap(ctx, redoLogRecord, fieldPos, fieldSize, code);
                    break;

                case KDLI_CODE_IMAPX:
                    kdliImapx(ctx, /* redoLogRecord, fieldPos, */ fieldSize, code);
                    break;
                default:
                    ;
            }
        }

        static void kdliInfo(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 17))
                throw RedoLogException(50061, "too short field kdli info: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->lobId.set(redoLogRecord->data(fieldPos + 1));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const typeDba block = Ctx::read32Big(redoLogRecord->data(fieldPos + 11));
                const uint16_t slot = Ctx::read16Big(redoLogRecord->data(fieldPos + 15));

                *ctx->dumpStream << "KDLI info [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  lobid " << redoLogRecord->lobId.lower() << '\n';
                *ctx->dumpStream << "  block 0x" << std::setfill('0') << std::setw(8) << std::hex << block << '\n';
                *ctx->dumpStream << "  slot  0x" << std::setfill('0') << std::setw(4) << std::hex << slot << '\n';
            }
        }

        static void kdliLoadCommon(const Ctx* ctx, /*RedoLogRecord* redoLogRecord,  typePos fieldPos, */typeSize fieldSize, uint8_t code) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "KDLI load common [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                // TODO: finish
            }
        }

        static void kdliLoadData(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 56))
                throw RedoLogException(50061, "too short field kdli load data: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->lobId.set(redoLogRecord->data(fieldPos + 12));
            redoLogRecord->lobPageNo = RedoLogRecord::INVALID_LOB_PAGE_NO;
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const Scn scn = ctx->readScnR(redoLogRecord->data(fieldPos + 2));
                const uint8_t flg0 = *redoLogRecord->data(fieldPos + 10);
                std::string flg0typ;
                switch (flg0 & KDLI_TYPE_MASK) {
                    case KDLI_TYPE_NEW:
                        flg0typ = "new";
                        break;

                    case KDLI_TYPE_LHB:
                        flg0typ = "lhb";
                        break;

                    case KDLI_TYPE_DATA:
                        flg0typ = "data";
                        break;

                    case KDLI_TYPE_BTREE:
                        flg0typ = "btree";
                        break;

                    case KDLI_TYPE_ITREE:
                        flg0typ = "itree";
                        break;

                    case KDLI_TYPE_AUX:
                        flg0typ = "aux";
                        break;
                    default:
                        ;
                }
                std::string flg0lock{"n"};
                if ((flg0 & KDLI_TYPE_LOCK) != 0)
                    flg0lock = "y";
                std::string flg0ver{"0"};
                if ((flg0 & KDLI_TYPE_VER1) != 0)
                    flg0ver = "1";
                const uint8_t flg1 = *redoLogRecord->data(fieldPos + 11);
                const uint16_t rid1 = ctx->read16(redoLogRecord->data(fieldPos + 22));
                const uint32_t rid2 = ctx->read32(redoLogRecord->data(fieldPos + 24));
                const uint8_t flg2 = *redoLogRecord->data(fieldPos + 28);
                std::string flg2pfill{"n"};
                if ((flg2 & KDLI_FLG2_121_PFILL) != 0)
                    flg2pfill = "y";
                std::string flg2cmap{"n"};
                if ((flg2 & KDLI_FLG2_121_CMAP) != 0)
                    flg2cmap = "y";
                std::string flg2hash{"n"};
                if ((flg2 & KDLI_FLG2_121_HASH) != 0)
                    flg2hash = "y";
                std::string flg2lid{"short-rowid"};
                if ((flg2 & KDLI_FLG2_121_LHB) != 0)
                    flg2lid = "lhb-dba";
                std::string flg2ver1{"0"};
                if ((flg2 & KDLI_FLG2_121_VER1) != 0)
                    flg2ver1 = "1";
                const uint8_t flg3 = *redoLogRecord->data(fieldPos + 29);
                const uint8_t pskip = *redoLogRecord->data(fieldPos + 30);
                const uint8_t sskip = *redoLogRecord->data(fieldPos + 31);
                char hash[20];
                memcpy(hash, redoLogRecord->data(fieldPos + 32), 20);
                const uint16_t hwm = ctx->read16(redoLogRecord->data(fieldPos + 52));
                const uint16_t spr = ctx->read16(redoLogRecord->data(fieldPos + 54));

                *ctx->dumpStream << "KDLI load data [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "bdba    [0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba << "]\n";
                *ctx->dumpStream << "kdlich  [0xXXXXXXXXXXXX 0]\n";
                *ctx->dumpStream << "  flg0  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg0) <<
                        " [ver=" << flg0ver << " typ=" << flg0typ << " lock=" << flg0lock << "]\n";
                *ctx->dumpStream << "  flg1  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg1) << '\n';
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "  scn   " << scn.toStringHex12() << " [0x" << scn.to48() << "]\n";
                else
                    *ctx->dumpStream << "  scn   " << scn.toStringHex16() << " [" << scn.to64D() << "]\n";
                *ctx->dumpStream << "  lid   " << redoLogRecord->lobId.lower() << '\n';
                *ctx->dumpStream << "  rid   0x" << std::setfill('0') << std::setw(8) << std::hex << rid2 << "." << std::setfill('0') <<
                        std::setw(4) << std::hex << rid1 << '\n';
                *ctx->dumpStream << "kdlidh  [0xXXXXXXXXXXXX 24]\n";
                *ctx->dumpStream << "  flg2  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg2) <<
                        " [ver=" << flg2ver1 << " lid=" << flg2lid << " hash=" << flg2hash << " cmap=" << flg2cmap << " pfill=" << flg2pfill << "]\n";
                *ctx->dumpStream << "  flg3  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg3) << '\n';
                *ctx->dumpStream << "  pskip " << std::dec << static_cast<uint>(pskip) << '\n';
                *ctx->dumpStream << "  sskip " << std::dec << static_cast<uint>(sskip) << '\n';
                *ctx->dumpStream << "  hash  ";
                for (const unsigned char j: hash)
                    *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(j);
                *ctx->dumpStream << '\n';
                *ctx->dumpStream << "  hwm   " << std::dec << hwm << '\n';
                *ctx->dumpStream << "  spr   " << std::dec << spr << '\n';
            }
        }

        static void kdliZero(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 6))
                throw RedoLogException(50061, "too short field kdli zero: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint16_t zoff = ctx->read16(redoLogRecord->data(fieldPos + 2));
                const uint16_t zsiz = ctx->read16(redoLogRecord->data(fieldPos + 4));

                *ctx->dumpStream << "KDLI zero [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  zoff  0x" << std::setfill('0') << std::setw(4) << std::hex << zoff << '\n';
                *ctx->dumpStream << "  zsiz  " << std::dec << zsiz << '\n';
            }
        }

        static void kdliFill(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field kdli fill: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->indKeyDataCode = code;
            redoLogRecord->lobOffset = ctx->read16(redoLogRecord->data(fieldPos + 2));
            redoLogRecord->lobData = fieldPos + 8;
            redoLogRecord->lobDataSize = ctx->read16(redoLogRecord->data(fieldPos + 6));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint16_t fsiz = ctx->read16(redoLogRecord->data(fieldPos + 4));

                *ctx->dumpStream << "KDLI fill [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  foff  0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord->lobOffset << '\n';
                *ctx->dumpStream << "  fsiz  " << std::dec << fsiz << '\n';
                *ctx->dumpStream << "  flen  " << std::dec << redoLogRecord->lobDataSize << '\n';

                *ctx->dumpStream << "  data\n";
                for (typeSize j = 0; j < fieldSize - 8U; ++j) {
                    *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(*redoLogRecord->data(fieldPos + j + 8));
                    if ((j % 26) < 25)
                        *ctx->dumpStream << " ";
                    if ((j % 26) == 25 || j == fieldSize - 8U - 1U)
                        *ctx->dumpStream << '\n';
                }
            }
        }

        static void kdliLmap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field kdli lmap: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->indKeyDataCode = code;
            redoLogRecord->indKeyData = fieldPos;
            redoLogRecord->indKeyDataSize = fieldSize;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint32_t asiz = ctx->read32(redoLogRecord->data(fieldPos + 4));

                if (fieldSize < 8U + asiz * 8U)
                    ctx->warning(70001, "too short field kdli lmap asiz: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

                *ctx->dumpStream << "KDLI lmap [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  asiz  " << std::dec << asiz << '\n';

                for (uint32_t i = 0; i < asiz; ++i) {
                    const uint8_t num1 = *redoLogRecord->data(fieldPos + (i * 8) + 8 + 0);
                    const uint8_t num2 = *redoLogRecord->data(fieldPos + (i * 8) + 8 + 1);
                    const uint16_t num3 = ctx->read16(redoLogRecord->data(fieldPos + (i * 8) + 8 + 2));
                    const typeDba dba = ctx->read32(redoLogRecord->data(fieldPos + (i * 8) + 8 + 4));

                    *ctx->dumpStream << "    [" << std::dec << i << "] " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num1) << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num2) << " " <<
                            std::dec << num3 << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(8) << std::hex << dba << '\n';
                }
            }
        }

        static void kdliLmapx(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field kdli lmapx: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->indKeyDataCode = code;
            redoLogRecord->indKeyData = fieldPos;
            redoLogRecord->indKeyDataSize = fieldSize;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint32_t asiz = ctx->read32(redoLogRecord->data(fieldPos + 4));

                if (fieldSize < 8U + asiz * 16U) {
                    ctx->warning(70001, "too short field kdli lmapx asiz: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());
                    return;
                }

                *ctx->dumpStream << "KDLI lmapx [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  asiz  " << std::dec << asiz << '\n';

                for (uint32_t i = 0; i < asiz; ++i) {
                    const uint8_t num1 = *redoLogRecord->data(fieldPos + (i * 16) + 8 + 0);
                    const uint8_t num2 = *redoLogRecord->data(fieldPos + (i * 16) + 8 + 1);
                    const uint16_t num3 = ctx->read16(redoLogRecord->data(fieldPos + (i * 16) + 8 + 2));
                    const typeDba dba = ctx->read32(redoLogRecord->data(fieldPos + (i * 16) + 8 + 4));
                    const int32_t num4 = ctx->read32(redoLogRecord->data(fieldPos + (i * 16) + 8 + 8));
                    const int32_t num5 = ctx->read32(redoLogRecord->data(fieldPos + (i * 16) + 8 + 12));

                    *ctx->dumpStream << "    [" << std::dec << i << "] " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num1) << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num2) << " " <<
                            std::dec << num3 << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(8) << std::hex << dba <<
                            " " << std::dec << num4 << "." << std::dec << num5 << '\n';
                }
            }
        }

        static void kdliSuplog(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 24))
                throw RedoLogException(50061, "too short field kdli suplog: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->xid = Xid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data(fieldPos + 4))),
                                     ctx->read16(redoLogRecord->data(fieldPos + 6)),
                                     ctx->read32(redoLogRecord->data(fieldPos + 8)));
            redoLogRecord->obj = ctx->read32(redoLogRecord->data(fieldPos + 12));
            redoLogRecord->col = ctx->read16(redoLogRecord->data(fieldPos + 18));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint16_t objv = ctx->read16(redoLogRecord->data(fieldPos + 16));
                const uint32_t flag = ctx->read32(redoLogRecord->data(fieldPos + 20));

                *ctx->dumpStream << "KDLI suplog [" << std::dec << static_cast<uint>(code) << "." << std::dec << fieldSize << "]\n";
                *ctx->dumpStream << "  xid   " << redoLogRecord->xid.toString() << '\n';
                *ctx->dumpStream << "  objn  " << std::dec << redoLogRecord->obj << '\n';
                *ctx->dumpStream << "  objv# " << std::dec << objv << '\n';
                *ctx->dumpStream << "  col#  " << std::dec << redoLogRecord->col << '\n';
                *ctx->dumpStream << "  flag  0x" << std::setfill('0') << std::setw(8) << std::hex << flag << '\n';
            }
        }

        static void kdliGmap(const Ctx* ctx /*, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code*/) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "KDLI GMAP Generic/Auxiliary Mapping Change:\n";
                // TODO: finish
            }
        }

        static void kdliFpload(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 28))
                throw RedoLogException(50061, "too short field kdli fpload: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->xid = Xid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data(fieldPos + 16))),
                                     ctx->read16(redoLogRecord->data(fieldPos + 18)),
                                     ctx->read32(redoLogRecord->data(fieldPos + 20)));
            redoLogRecord->dataObj = ctx->read32(redoLogRecord->data(fieldPos + 24));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint32_t bsz = ctx->read32(redoLogRecord->data(fieldPos + 4));
                const Scn scn = ctx->readScn(redoLogRecord->data(fieldPos + 8));

                *ctx->dumpStream << "KDLI fpload [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  bsz   " << std::dec << bsz << '\n';
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "  scn   " << scn.to48() << '\n';
                else
                    *ctx->dumpStream << "  scn   " << scn.to64() << '\n';
                *ctx->dumpStream << "  xid   " << redoLogRecord->xid.toString() << '\n';
                *ctx->dumpStream << "  objd  " << std::dec << redoLogRecord->dataObj << '\n';
            }
        }

        static void kdliLoadLhb(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 112))
                throw RedoLogException(50061, "too short field kdli load lhb: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->lobId.set(redoLogRecord->data(fieldPos + 12));
            redoLogRecord->lobPageNo = RedoLogRecord::INVALID_LOB_PAGE_NO;
            redoLogRecord->dba0 = ctx->read32(redoLogRecord->data(fieldPos + 64));
            redoLogRecord->dba1 = ctx->read32(redoLogRecord->data(fieldPos + 68));
            redoLogRecord->dba2 = ctx->read32(redoLogRecord->data(fieldPos + 72));
            redoLogRecord->dba3 = ctx->read32(redoLogRecord->data(fieldPos + 76));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const Scn scn = Scn(ctx->read16(redoLogRecord->data(fieldPos + 8)),
                                    ctx->read32(redoLogRecord->data(fieldPos + 4)));
                const uint8_t flg0 = *redoLogRecord->data(fieldPos + 10);
                const uint8_t flg1 = *redoLogRecord->data(fieldPos + 11);
                const uint32_t spare = ctx->read32(redoLogRecord->data(fieldPos + 24));
                std::string flg0typ = "???";
                switch (flg0 & KDLI_TYPE_MASK) {
                    case KDLI_TYPE_NEW:
                        flg0typ = "new";
                        break;

                    case KDLI_TYPE_LHB:
                        flg0typ = "lhb";
                        break;

                    case KDLI_TYPE_DATA:
                        flg0typ = "data";
                        break;

                    case KDLI_TYPE_BTREE:
                        flg0typ = "btree";
                        break;

                    case KDLI_TYPE_ITREE:
                        flg0typ = "itree";
                        break;

                    case KDLI_TYPE_AUX:
                        flg0typ = "aux";
                        break;
                    default:
                        ;
                }
                std::string flg0lock{"n"};
                if ((flg0 & KDLI_TYPE_LOCK) != 0)
                    flg0lock = "y";
                std::string flg0ver{"0"};
                if ((flg0 & KDLI_TYPE_VER1) != 0)
                    flg0ver = "1";

                *ctx->dumpStream << "KDLI load lhb [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "bdba    [0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba << "]\n";
                *ctx->dumpStream << "kdlich  [0xXXXXXXXXXXXX 0]\n";
                *ctx->dumpStream << "  flg0  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg0) <<
                        " [ver=" << flg0ver << " typ=" << flg0typ << " lock=" << flg0lock << "]\n";
                *ctx->dumpStream << "  flg1  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg1) << '\n';
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "  scn   " << scn.toStringHex12() << " [0x" << scn.to48() << "]\n";
                else
                    *ctx->dumpStream << "  scn   " << scn.toStringHex16() << " [" << scn.to64D() << "]\n";
                *ctx->dumpStream << "  lid   " << redoLogRecord->lobId.lower() << '\n';
                *ctx->dumpStream << "  spare 0x" << std::setfill('0') << std::setw(8) << std::hex << spare << '\n';

                const uint8_t flg2 = *redoLogRecord->data(fieldPos + 28);
                const uint8_t flg3 = *redoLogRecord->data(fieldPos + 29);

                if ((flg3 & KDLI_FLG3_VLL) != 0) {
                    const uint8_t flg4 = *redoLogRecord->data(fieldPos + 30);
                    const uint8_t flg5 = *redoLogRecord->data(fieldPos + 31);
                    const int32_t llen1 = ctx->read32(redoLogRecord->data(fieldPos + 32));
                    const int32_t llen2 = ctx->read32(redoLogRecord->data(fieldPos + 36));
                    const int32_t ver1 = ctx->read32(redoLogRecord->data(fieldPos + 40));
                    const int32_t ver2 = ctx->read32(redoLogRecord->data(fieldPos + 44));
                    const int32_t ext = ctx->read32(redoLogRecord->data(fieldPos + 48));
                    const uint16_t asiz = ctx->read16(redoLogRecord->data(fieldPos + 52));
                    const uint16_t hwm = ctx->read16(redoLogRecord->data(fieldPos + 54));
                    const uint32_t ovr1 = ctx->read32(redoLogRecord->data(fieldPos + 56));
                    const int32_t ovr2 = ctx->read32(redoLogRecord->data(fieldPos + 60));
                    const typeDba ldba = ctx->read32(redoLogRecord->data(fieldPos + 80));
                    const int32_t nblk = ctx->read32(redoLogRecord->data(fieldPos + 84));
                    const Scn deScn1{};
                    const Scn deScn2{ctx->read64(redoLogRecord->data(fieldPos + 88))};
                    char hash[16];
                    memcpy(hash, redoLogRecord->data(fieldPos + 96), 16);
                    *ctx->dumpStream << "kdlihh  [0xXXXXXXXXXXXX 24]\n";

                    if (ctx->version < RedoLogRecord::REDO_VERSION_12_2) {
                        std::string flg2pfill{"n"};
                        if ((flg2 & KDLI_FLG2_121_PFILL) != 0)
                            flg2pfill = "y";
                        std::string flg2cmap{"n"};
                        if ((flg2 & KDLI_FLG2_121_CMAP) != 0)
                            flg2cmap = "y";
                        std::string flg2hash{"n"};
                        if ((flg2 & KDLI_FLG2_121_HASH) != 0)
                            flg2hash = "y";
                        std::string flg2lid{"short-rowid"};
                        if ((flg2 & KDLI_FLG2_121_LHB) != 0)
                            flg2lid = "lhb-dba";
                        std::string flg2ver1{"0"};
                        if ((flg2 & KDLI_FLG2_121_VER1) != 0)
                            flg2ver1 = "1";

                        *ctx->dumpStream << "  flg2  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg2) <<
                                " [ver=" << flg2ver1 << " lid=" << flg2lid << " hash=" << flg2hash << " cmap=" << flg2cmap << " pfill=" <<
                                flg2pfill << "]\n";
                    } else {
                        std::string flg2descn{"n"};
                        if ((flg2 & KDLI_FLG2_122_DESCN) != 0)
                            flg2descn = "y";
                        std::string flg2ovr{"n"};
                        if ((flg2 & KDLI_FLG2_122_OVR) != 0)
                            flg2ovr = "y";
                        std::string flg2xfm{"n"};
                        if ((flg2 & KDLI_FLG2_122_XFM) != 0)
                            flg2xfm = "y";
                        std::string flg2bt{"n"};
                        if ((flg2 & KDLI_FLG2_122_BT) != 0)
                            flg2bt = "y";
                        std::string flg2it{"n"};
                        if ((flg2 & KDLI_FLG2_122_IT) != 0)
                            flg2it = "y";
                        std::string flg2hash{"n"};
                        if ((flg2 & KDLI_FLG2_122_HASH) != 0)
                            flg2hash = "y";
                        std::string flg2lid{"short-rowid"};
                        if ((flg2 & KDLI_FLG2_122_LID) != 0)
                            flg2lid = "iot-guess";
                        std::string flg2ver1{"0"};
                        if ((flg2 & KDLI_FLG2_121_VER1) != 0)
                            flg2ver1 = "1";

                        *ctx->dumpStream << "  flg2  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg2) <<
                                " [ver=" << flg2ver1 << " lid=" << flg2lid << " hash=" << flg2hash << " it=" << flg2it << " bt=" <<
                                flg2bt << " xfm=" << flg2xfm << " ovr=" << flg2ovr << " descn=" << flg2descn << "]\n";
                    }

                    std::string flg3vll{"n"};
                    if ((flg3 & KDLI_FLG3_VLL) != 0)
                        flg3vll = "y";
                    *ctx->dumpStream << "  flg3  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg3) <<
                            " [vll=" << flg3vll << "]\n";

                    *ctx->dumpStream << "  flg4  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg4) << '\n';
                    *ctx->dumpStream << "  flg5  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg5) << '\n';
                    *ctx->dumpStream << "  hash  ";
                    for (const unsigned char j: hash)
                        *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(j);
                    *ctx->dumpStream << '\n';
                    *ctx->dumpStream << "  llen  " << std::dec << llen1 << "." << llen2 << '\n';
                    *ctx->dumpStream << "  ver   " << std::dec << ver1 << "." << ver2 << '\n';
                    *ctx->dumpStream << "  #ext  " << ext << '\n';
                    *ctx->dumpStream << "  asiz  " << std::dec << asiz << '\n';
                    *ctx->dumpStream << "  hwm   " << std::dec << hwm << '\n';
                    *ctx->dumpStream << "  ovr   0x" << std::setfill('0') << std::setw(8) << std::hex << ovr1 << "." << std::dec << ovr2 << '\n';
                    if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                        *ctx->dumpStream << "  descn " << deScn1.toStringHex12() << " [0x" << deScn2.to48() << "]\n";
                    else
                        *ctx->dumpStream << "  descn " << deScn1.toStringHex16() << " [" << deScn2.to64D() << "]\n";
                    *ctx->dumpStream << "  dba0  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba0 << '\n';
                    *ctx->dumpStream << "  dba1  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba1 << '\n';
                    *ctx->dumpStream << "  dba2  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba2 << '\n';
                    *ctx->dumpStream << "  dba3  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba3 << '\n';
                    *ctx->dumpStream << "  ldba  0x" << std::setfill('0') << std::setw(8) << std::hex << ldba << '\n';
                    *ctx->dumpStream << "  nblk  " << std::dec << nblk << '\n';
                } else {
                    // TODO: finish
                    *ctx->dumpStream << "kdlihho [0xXXXXXXXXXXXX 24]\n";
                    *ctx->dumpStream << "  flg2  0x00 [ver=0 lid=short-rowid hash=n plen=n root=n xfm=n ovr=n aux=n]\n";
                    *ctx->dumpStream << "  flg3  0x00\n";
                    *ctx->dumpStream << "  flg4  0x00\n";
                    *ctx->dumpStream << "  flg5  0x00\n";
                    *ctx->dumpStream << "  hash  0000000000000000000000000000000000000000\n";
                    *ctx->dumpStream << "  llen  0.0\n";
                    *ctx->dumpStream << "  plen  0.0\n";
                    *ctx->dumpStream << "  ver   0.0\n";
                    *ctx->dumpStream << "  #ext  0.0\n";
                    *ctx->dumpStream << "  ovr   0x00000000.0\n";
                    *ctx->dumpStream << "  asiz  0\n";
                    *ctx->dumpStream << "  root  0x00000000\n";
                    *ctx->dumpStream << "  roff  0.0\n";
                    *ctx->dumpStream << "  auxp  0x00000000\n";
                }
            }
        }

        static void kdliAlmap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 12))
                throw RedoLogException(50061, "too short field kdli kmap: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->indKeyDataCode = code;
            redoLogRecord->indKeyData = fieldPos;
            redoLogRecord->indKeyDataSize = fieldSize;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint32_t nent = ctx->read32(redoLogRecord->data(fieldPos + 4));
                const uint32_t sidx = ctx->read32(redoLogRecord->data(fieldPos + 8));

                if (unlikely(fieldSize < 12 + nent * 8))
                    throw RedoLogException(50061, "too short field kdli almap nent: " + std::to_string(fieldSize) + " offset: " +
                                           redoLogRecord->fileOffset.toString());

                *ctx->dumpStream << "KDLI almap [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  nent  " << std::dec << nent << '\n';
                *ctx->dumpStream << "  sidx  " << std::dec << sidx << '\n';

                for (uint32_t i = 0; i < nent; ++i) {
                    const uint8_t num1 = *redoLogRecord->data(fieldPos + (i * 8) + 12 + 0);
                    const uint8_t num2 = *redoLogRecord->data(fieldPos + (i * 8) + 12 + 1);
                    const uint16_t num3 = ctx->read16(redoLogRecord->data(fieldPos + (i * 8) + 12 + 2));
                    const typeDba dba = ctx->read32(redoLogRecord->data(fieldPos + (i * 8) + 12 + 4));

                    *ctx->dumpStream << "    [" << std::dec << i << "] " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num1) << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num2) << " " <<
                            std::dec << num3 << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(8) << std::hex << dba << '\n';
                }
            }
        }

        static void kdliAlmapx(const Ctx* ctx, /* RedoLogRecord* redoLogRecord, typePos fieldPos, */ typeSize fieldSize, uint8_t code) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "KDLI almapx [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                // TODO: finish
            }
        }

        static void kdliLoadItree(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 40))
                throw RedoLogException(50061, "too short field kdli load itree: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->lobId.set(redoLogRecord->data(fieldPos + 12));
            redoLogRecord->lobPageNo = RedoLogRecord::INVALID_LOB_PAGE_NO;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const Scn scn = ctx->readScnR(redoLogRecord->data(fieldPos + 2));
                const uint8_t flg0 = *redoLogRecord->data(fieldPos + 10);
                std::string flg0typ;
                switch (flg0 & KDLI_TYPE_MASK) {
                    case KDLI_TYPE_NEW:
                        flg0typ = "new";
                        break;

                    case KDLI_TYPE_LHB:
                        flg0typ = "lhb";
                        break;

                    case KDLI_TYPE_DATA:
                        flg0typ = "data";
                        break;

                    case KDLI_TYPE_BTREE:
                        flg0typ = "btree";
                        break;

                    case KDLI_TYPE_ITREE:
                        flg0typ = "itree";
                        break;

                    case KDLI_TYPE_AUX:
                        flg0typ = "aux";
                        break;
                    default:
                        ;
                }
                std::string flg0lock{"n"};
                if ((flg0 & KDLI_TYPE_LOCK) != 0)
                    flg0lock = "y";
                std::string flg0ver{"0"};
                if ((flg0 & KDLI_TYPE_VER1) != 0)
                    flg0ver = "1";
                const uint8_t flg1 = *redoLogRecord->data(fieldPos + 11);
                const uint16_t rid1 = ctx->read16(redoLogRecord->data(fieldPos + 22));
                const uint32_t rid2 = ctx->read32(redoLogRecord->data(fieldPos + 24));
                const uint8_t flg2 = *redoLogRecord->data(fieldPos + 28);
                std::string flg2xfm{"n"};
                if ((flg2 & KDLI_FLG2_122_XFM) != 0)
                    flg2xfm = "y";
                std::string flg2ver1{"0"};
                if ((flg2 & KDLI_FLG2_121_VER1) != 0)
                    flg2ver1 = "1";
                const uint8_t flg3 = *redoLogRecord->data(fieldPos + 29);
                const uint16_t lvl = ctx->read16(redoLogRecord->data(fieldPos + 30));
                const uint16_t asiz = ctx->read16(redoLogRecord->data(fieldPos + 32));
                const uint16_t hwm = ctx->read16(redoLogRecord->data(fieldPos + 34));
                const uint16_t par = ctx->read32(redoLogRecord->data(fieldPos + 36));

                *ctx->dumpStream << "KDLI load itree [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "bdba    [0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba << "]\n";
                *ctx->dumpStream << "kdlich  [0xXXXXXXXXXXXX 0]\n";
                *ctx->dumpStream << "  flg0  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg0) <<
                        " [ver=" << flg0ver << " typ=" << flg0typ << " lock=" << flg0lock << "]\n";
                *ctx->dumpStream << "  flg1  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg1) << '\n';
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "  scn   " << scn.toStringHex12() << '\n';
                else
                    *ctx->dumpStream << "  scn   " << scn.toStringHex16() << " [" << scn.to64D() << "]\n";
                *ctx->dumpStream << "  lid   " << redoLogRecord->lobId.lower() << '\n';
                *ctx->dumpStream << "  rid   0x" << std::setfill('0') << std::setw(8) << std::hex << rid2 << "." << std::setfill('0') <<
                        std::setw(4) << std::hex << rid1 << '\n';

                *ctx->dumpStream << "kdliih  [0xXXXXXXXXXXXX 24]\n";
                *ctx->dumpStream << "  flg2  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg2) <<
                        " [ver=" << flg2ver1 << " xfm=" << flg2xfm << "]\n";
                *ctx->dumpStream << "  flg3  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg3) << '\n';
                *ctx->dumpStream << "  lvl   " << std::dec << lvl << '\n';
                *ctx->dumpStream << "  asiz  " << std::dec << asiz << '\n';
                *ctx->dumpStream << "  hwm   " << std::dec << hwm << '\n';
                *ctx->dumpStream << "  par   0x" << std::setfill('0') << std::setw(8) << std::hex << par << '\n';
            }
        }

        static void kdliImap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code) {
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field kdli imap: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->indKeyDataCode = code;
            redoLogRecord->indKeyData = fieldPos;
            redoLogRecord->indKeyDataSize = fieldSize;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint32_t asiz = ctx->read32(redoLogRecord->data(fieldPos + 4));

                if (fieldSize < 8 + asiz * 8)
                    ctx->warning(70001, "too short field kdli imap asiz: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

                *ctx->dumpStream << "KDLI imap [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                *ctx->dumpStream << "  asiz  " << std::dec << asiz << '\n';

                for (uint32_t i = 0; i < asiz; ++i) {
                    const uint8_t num1 = *redoLogRecord->data(fieldPos + (i * 8) + 8 + 0);
                    const uint8_t num2 = *redoLogRecord->data(fieldPos + (i * 8) + 8 + 1);
                    const uint16_t num3 = ctx->read16(redoLogRecord->data(fieldPos + (i * 8) + 8 + 2));
                    const typeDba dba = ctx->read32(redoLogRecord->data(fieldPos + (i * 8) + 8 + 4));

                    *ctx->dumpStream << "    [" << std::dec << i << "] " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num1) << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(num2) << " " <<
                            std::dec << num3 << " " <<
                            "0x" << std::hex << std::setfill('0') << std::setw(8) << std::hex << dba << '\n';
                }
            }
        }

        static void kdliImapx(const Ctx* ctx, /* RedoLogRecord* redoLogRecord, typePos fieldPos, */ typeSize fieldSize, uint8_t code) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "KDLI imap [" << std::dec << static_cast<uint>(code) << "." << fieldSize << "]\n";
                // TODO: finish
            }
        }

        static void kdliDataLoad(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            redoLogRecord->lobData = fieldPos;
            redoLogRecord->lobDataSize = fieldSize;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "KDLI data load [0xXXXXXXXXXXXX." << std::dec << fieldSize << "]\n";

                for (typeSize j = 0; j < fieldSize; ++j) {
                    *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(*redoLogRecord->data(fieldPos + j));
                    if ((j % 26) < 25)
                        *ctx->dumpStream << " ";
                    if ((j % 26) == 25 || j == fieldSize - 1U)
                        *ctx->dumpStream << '\n';
                }
            }
        }

        static void kdliCommon(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 12))
                throw RedoLogException(50061, "too short field kdli common: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->opc = *redoLogRecord->data(fieldPos + 0);
            redoLogRecord->dba = ctx->read32(redoLogRecord->data(fieldPos + 8));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                std::string opCode{"????"};
                switch (redoLogRecord->opc) {
                    case KDLI_OP_REDO:
                        opCode = "REDO";
                        break;

                    case KDLI_OP_UNDO:
                        opCode = "UNDO";
                        break;

                    case KDLI_OP_CR:
                        opCode = "CR";
                        break;

                    case KDLI_OP_FRMT:
                        opCode = "FRMT";
                        break;

                    case KDLI_OP_INVL:
                        opCode = "INVL";
                        break;

                    case KDLI_OP_LOAD:
                        opCode = "LOAD";
                        break;

                    case KDLI_OP_BIMG:
                        opCode = "BIMG";
                        break;

                    case KDLI_OP_SINV:
                        opCode = "SINV";
                        break;
                    default:
                        ;
                }
                const uint8_t type = *redoLogRecord->data(fieldPos + 1);
                std::string typeCode{"???"};
                switch (type & KDLI_TYPE_MASK) {
                    case KDLI_TYPE_NEW:
                        typeCode = "new";
                        break;

                    case KDLI_TYPE_LHB:
                        typeCode = "lhb";
                        break;

                    case KDLI_TYPE_DATA:
                        typeCode = "data";
                        break;

                    case KDLI_TYPE_BTREE:
                        typeCode = "btree";
                        break;

                    case KDLI_TYPE_ITREE:
                        typeCode = "itree";
                        break;

                    case KDLI_TYPE_AUX:
                        typeCode = "aux";
                        break;
                    default:
                        ;
                }

                const uint8_t flg0 = *redoLogRecord->data(fieldPos + 2);
                const uint8_t flg1 = *redoLogRecord->data(fieldPos + 3);
                const uint16_t psiz = ctx->read32(redoLogRecord->data(fieldPos + 4));
                const uint16_t poff = ctx->read32(redoLogRecord->data(fieldPos + 6));

                *ctx->dumpStream << "KDLI common [" << std::dec << fieldSize << "]\n";
                *ctx->dumpStream << "  op    0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(redoLogRecord->opc) <<
                        " [" << opCode << "]\n";
                *ctx->dumpStream << "  type  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(type) <<
                        " [" << typeCode << "]\n";
                *ctx->dumpStream << "  flg0  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg0) << '\n';
                *ctx->dumpStream << "  flg1  0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flg1) << '\n';
                *ctx->dumpStream << "  psiz  " << std::dec << psiz << '\n';
                *ctx->dumpStream << "  poff  " << std::dec << poff << '\n';
                *ctx->dumpStream << "  dba   0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba << '\n';
            }
        }

        static void kdoOpCode(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 16))
                throw RedoLogException(50061, "too short field kdo OpCode: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->bdba = ctx->read32(redoLogRecord->data(fieldPos + 0));
            redoLogRecord->op = *redoLogRecord->data(fieldPos + 10);
            redoLogRecord->flags = *redoLogRecord->data(fieldPos + 11);

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const typeDba hdba = ctx->read32(redoLogRecord->data(fieldPos + 4));
                const uint16_t maxFr = ctx->read16(redoLogRecord->data(fieldPos + 8));
                const uint8_t itli = *redoLogRecord->data(fieldPos + 12);
                const uint8_t ispac = *redoLogRecord->data(fieldPos + 13);

                std::string opCode;
                switch (redoLogRecord->op & 0x1F) {
                    case RedoLogRecord::OP_IUR:
                        // Interpret Undo Redo
                        opCode = "IUR";
                        break;

                    case RedoLogRecord::OP_IRP:
                        // Insert Row Piece
                        opCode = "IRP";
                        break;

                    case RedoLogRecord::OP_DRP:
                        // Delete Row Piece
                        opCode = "DRP";
                        break;

                    case RedoLogRecord::OP_LKR:
                        // LocK Row
                        opCode = "LKR";
                        break;

                    case RedoLogRecord::OP_URP:
                        // Update Row Piece
                        opCode = "URP";
                        break;

                    case RedoLogRecord::OP_ORP:
                        // Overwrite Row Piece
                        opCode = "ORP";
                        break;

                    case RedoLogRecord::OP_MFC:
                        // Manipulate First Column
                        opCode = "MFC";
                        break;

                    case RedoLogRecord::OP_CFA:
                        // Change Forwarding Address
                        opCode = "CFA";
                        break;

                    case RedoLogRecord::OP_CKI:
                        // Change Cluster key Index
                        opCode = "CKI";
                        break;

                    case RedoLogRecord::OP_SKL:
                        // Set Key Links
                        opCode = "SKL";
                        break;

                    case RedoLogRecord::OP_QMI:
                        // Quick Multi-row Insert
                        opCode = "QMI";
                        break;

                    case RedoLogRecord::OP_QMD:
                        // Quick Multi-row Delete
                        opCode = "QMD";
                        break;

                    case RedoLogRecord::OP_DSC:
                        opCode = "DSC";
                        break;

                    case RedoLogRecord::OP_LMN:
                        opCode = "LMN";
                        break;

                    case RedoLogRecord::OP_LLB:
                        opCode = "LLB";
                        break;

                    case RedoLogRecord::OP_SHK:
                        opCode = "SHK";
                        break;

                    case RedoLogRecord::OP_CMP:
                        opCode = "CMP";
                        break;

                    case RedoLogRecord::OP_DCU:
                        opCode = "DCU";
                        break;

                    case RedoLogRecord::OP_MRK:
                        opCode = "MRK";
                        break;

                    case RedoLogRecord::OP_021:
                        opCode = " 21";
                        break;

                    default:
                        opCode = "XXX";
                        if (unlikely(ctx->dumpRedoLog >= 1))
                            *ctx->dumpStream << "DEBUG op: " << std::dec << static_cast<uint>(redoLogRecord->op & 0x1F) << '\n';
                }

                std::string xtype{"0"};
                std::string rtype;
                switch (redoLogRecord->flags & 0x03) {
                    case FLAGS_XA:
                        // Redo
                        xtype = "XA";
                        break;

                    case FLAGS_XR:
                        // Rollback
                        xtype = "XR";
                        break;

                    case FLAGS_CR:
                        // Unknown
                        xtype = "CR";
                        break;
                    default:
                        ;
                }
                redoLogRecord->flags &= 0xFC;

                if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0)
                    rtype = "xtype KDO_KDOM2";

                std::string rowDependencies{"Disabled"};
                if ((redoLogRecord->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0)
                    rowDependencies = "Enabled";

                *ctx->dumpStream << "KDO Op code: " << opCode << " row dependencies " << rowDependencies << '\n';
                *ctx->dumpStream << "  xtype: " << xtype << rtype <<
                        " flags: 0x" << std::setfill('0') << std::setw(8) << std::hex << static_cast<uint>(redoLogRecord->flags) << " " <<
                        " bdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->bdba << " " <<
                        " hdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << hdba << '\n';
                *ctx->dumpStream << "itli: " << std::dec << static_cast<uint>(itli) << " " <<
                        " ispac: " << std::dec << static_cast<uint>(ispac) << " " <<
                        " maxfr: " << std::dec << static_cast<uint>(maxFr) << '\n';

                switch ((redoLogRecord->op & 0x1F)) {
                    case RedoLogRecord::OP_SKL:
                        if (fieldSize >= 32) {
                            char fwdFl = '-';
                            const uint32_t fwd = Ctx::read32Big(redoLogRecord->data(fieldPos + 16));
                            const uint16_t fwdPos = Ctx::read16Big(redoLogRecord->data(fieldPos + 20));
                            char bkwFl = '-';
                            const uint32_t bkw = Ctx::read32Big(redoLogRecord->data(fieldPos + 22));
                            const uint16_t bkwPos = Ctx::read16Big(redoLogRecord->data(fieldPos + 26));
                            const uint8_t fl = *redoLogRecord->data(fieldPos + 28);
                            const uint8_t lock = *redoLogRecord->data(fieldPos + 29);
                            const uint8_t slot = *redoLogRecord->data(fieldPos + 30);

                            if ((fl & 0x01) != 0) fwdFl = 'F';
                            if ((fl & 0x02) != 0) bkwFl = 'B';

                            *ctx->dumpStream << "flag: " << fwdFl << bkwFl <<
                                    " lock: " << std::dec << static_cast<uint>(lock) <<
                                    " slot: " << std::dec << static_cast<uint>(slot) <<
                                    "(0x" << std::hex << static_cast<uint>(slot) << ")\n";

                            if (fwdFl == 'F')
                                *ctx->dumpStream << "fwd: 0x" << std::setfill('0') << std::setw(8) << std::hex << fwd << "." << fwdPos << " \n";
                            if (bkwFl == 'B')
                                *ctx->dumpStream << "bkw: 0x" << std::setfill('0') << std::setw(8) << std::hex << bkw << "." << bkwPos << '\n';
                        }
                        break;

                    case RedoLogRecord::OP_DSC:
                        if (fieldSize >= 24) {
                            const uint16_t slot = ctx->read16(redoLogRecord->data(fieldPos + 16));
                            const uint8_t tabn = *redoLogRecord->data(fieldPos + 18);
                            const uint8_t rel = *redoLogRecord->data(fieldPos + 19);

                            *ctx->dumpStream << "tabn: " << std::dec << static_cast<uint>(tabn) << " slot: " << slot <<
                                    "(0x" << std::hex << slot << ")\n";
                            *ctx->dumpStream << "piece relative column number: " << std::dec << static_cast<uint>(rel) << '\n';
                        }
                        break;
                    default:
                        ;
                }
            }

            switch (redoLogRecord->op & 0x1F) {
                case RedoLogRecord::OP_IRP:
                    kdoOpCodeIRP(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_DRP:
                    kdoOpCodeDRP(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_LKR:
                    kdoOpCodeLKR(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_URP:
                    kdoOpCodeURP(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_ORP:
                    kdoOpCodeORP(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_CKI:
                    kdoOpCodeSKL(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_CFA:
                    kdoOpCodeCFA(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;

                case RedoLogRecord::OP_QMI:
                case RedoLogRecord::OP_QMD:
                    kdoOpCodeQM(ctx, redoLogRecord, fieldPos, fieldSize);
                    break;
                default:
                    ;
            }
        }

        static void kdoOpCodeIRP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 48))
                throw RedoLogException(50061, "too short field kdo OpCode IRP: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->fb = *redoLogRecord->data(fieldPos + 16);
            redoLogRecord->cc = *redoLogRecord->data(fieldPos + 18);
            redoLogRecord->sizeDelt = ctx->read16(redoLogRecord->data(fieldPos + 40));
            redoLogRecord->slot = ctx->read16(redoLogRecord->data(fieldPos + 42));

            typeDba nridBdba = 0;
            typeSlot nridSlot = 0;
            if ((redoLogRecord->fb & RedoLogRecord::FB_L) == 0) {
                nridBdba = ctx->read32(redoLogRecord->data(fieldPos + 28));
                nridSlot = ctx->read16(redoLogRecord->data(fieldPos + 32));
            }

            if (unlikely(fieldSize < 45U + (static_cast<typeSize>(redoLogRecord->cc) + 7U) / 8U))
                throw RedoLogException(50061, "too short field kdo OpCode IRP for nulls: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->nullsDelta = fieldPos + 45;
            const uint8_t* nulls = redoLogRecord->data(redoLogRecord->nullsDelta);
            uint8_t bits = 1;
            for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                if ((*nulls & bits) == 0)
                    redoLogRecord->ccData = i + 1;
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 44);

                *ctx->dumpStream << "tabn: " << static_cast<uint>(tabn) <<
                        " slot: " << std::dec << static_cast<uint>(redoLogRecord->slot) << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                        " size/delt: " << std::dec << redoLogRecord->sizeDelt << '\n';

                char fbStr[9]{"--------"};
                processFbFlags(redoLogRecord->fb, fbStr);
                const uint8_t lb = *redoLogRecord->data(fieldPos + 17);

                *ctx->dumpStream << "fb: " << fbStr <<
                        " lb: 0x" << std::hex << static_cast<uint>(lb) << " " <<
                        " cc: " << std::dec << static_cast<uint>(redoLogRecord->cc);
                if (fbStr[1] == 'C') {
                    const uint8_t cki = *redoLogRecord->data(fieldPos + 19);
                    *ctx->dumpStream << " cki: " << std::dec << static_cast<uint>(cki) << '\n';
                } else
                    *ctx->dumpStream << '\n';

                if ((redoLogRecord->fb & RedoLogRecord::FB_F) != 0 && (redoLogRecord->fb & RedoLogRecord::FB_H) == 0) {
                    const typeDba hrid1 = ctx->read32(redoLogRecord->data(fieldPos + 20));
                    const typeSlot hrid2 = ctx->read16(redoLogRecord->data(fieldPos + 24));
                    *ctx->dumpStream << "hrid: 0x" << std::setfill('0') << std::setw(8) << std::hex << hrid1 << "." << std::hex << hrid2 << '\n';
                }

                // Next DBA/SLT
                if ((redoLogRecord->fb & RedoLogRecord::FB_L) == 0) {
                    *ctx->dumpStream << "nrid:  0x" << std::setfill('0') << std::setw(8) << std::hex << nridBdba << "." << std::hex << nridSlot << '\n';
                }

                if ((redoLogRecord->fb & RedoLogRecord::FB_K) != 0) {
                    constexpr uint8_t curc = 0; // TODO: find field position/size
                    constexpr uint8_t comc = 0; // TODO: find field position/size
                    const uint32_t pk = ctx->read32(redoLogRecord->data(fieldPos + 20));
                    const uint16_t pk1 = ctx->read16(redoLogRecord->data(fieldPos + 24));
                    const uint32_t nk = ctx->read32(redoLogRecord->data(fieldPos + 28));
                    const uint16_t nk1 = ctx->read16(redoLogRecord->data(fieldPos + 32));

                    *ctx->dumpStream << "curc: " << std::dec << static_cast<uint>(curc) <<
                            " comc: " << std::dec << static_cast<uint>(comc) <<
                            " pk: 0x" << std::setfill('0') << std::setw(8) << std::hex << pk << "." << std::hex << pk1 <<
                            " nk: 0x" << std::setfill('0') << std::setw(8) << std::hex << nk << "." << std::hex << nk1 << '\n';
                }

                *ctx->dumpStream << "null:";
                if (redoLogRecord->cc >= 11)
                    *ctx->dumpStream << "\n01234567890123456789012345678901234567890123456789012345678901234567890123456789\n";
                else
                    *ctx->dumpStream << " ";

                nulls = redoLogRecord->data(redoLogRecord->nullsDelta);
                bits = 1;
                for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                    if ((*nulls & bits) != 0)
                        *ctx->dumpStream << "N";
                    else
                        *ctx->dumpStream << "-";
                    if ((i % 80) == 79)
                        *ctx->dumpStream << '\n';

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
                *ctx->dumpStream << '\n';
            }
        }

        static void kdoOpCodeDRP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 20))
                throw RedoLogException(50061, "too short field kdo OpCode DRP: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->slot = ctx->read16(redoLogRecord->data(fieldPos + 16));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 18);

                *ctx->dumpStream << "tabn: " << static_cast<uint>(tabn) <<
                        " slot: " << std::dec << static_cast<uint>(redoLogRecord->slot) << "(0x" << std::hex << redoLogRecord->slot << ")\n";
            }
        }

        static void kdoOpCodeLKR(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 20))
                throw RedoLogException(50061, "too short field KDO OpCode LKR: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->slot = ctx->read16(redoLogRecord->data(fieldPos + 16));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 18);
                const uint8_t to = *redoLogRecord->data(fieldPos + 19);

                *ctx->dumpStream << "tabn: " << static_cast<uint>(tabn) <<
                        " slot: " << std::dec << redoLogRecord->slot <<
                        " to: " << std::dec << static_cast<uint>(to) << '\n';
            }
        }

        static void kdoOpCodeURP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 28))
                throw RedoLogException(50061, "too short field kdo OpCode URP: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->fb = *redoLogRecord->data(fieldPos + 16);
            redoLogRecord->slot = ctx->read16(redoLogRecord->data(fieldPos + 20));
            redoLogRecord->cc = *redoLogRecord->data(fieldPos + 23);

            if (unlikely(fieldSize < 26 + (static_cast<uint>(redoLogRecord->cc) + 7U) / 8U))
                throw RedoLogException(50061, "too short field kdo OpCode URP for nulls: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->nullsDelta = fieldPos + 26;
            const uint8_t* nulls = redoLogRecord->data(redoLogRecord->nullsDelta);
            uint8_t bits = 1;
            for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                if ((*nulls & bits) == 0)
                    redoLogRecord->ccData = i + 1;
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint8_t lock = *redoLogRecord->data(fieldPos + 17);
                const uint8_t ckix = *redoLogRecord->data(fieldPos + 18);
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 19);
                const uint8_t ncol = *redoLogRecord->data(fieldPos + 22);
                const auto size = static_cast<int16_t>(ctx->read16(redoLogRecord->data(fieldPos + 24))); // Signed

                *ctx->dumpStream << "tabn: " << static_cast<uint>(tabn) <<
                        " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                        " flag: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(redoLogRecord->fb) <<
                        " lock: " << std::dec << static_cast<uint>(lock) <<
                        " ckix: " << std::dec << static_cast<uint>(ckix) << '\n';
                *ctx->dumpStream << "ncol: " << std::dec << static_cast<uint>(ncol) <<
                        " nnew: " << std::dec << static_cast<uint>(redoLogRecord->cc) <<
                        " size: " << std::dec << size << '\n';
            }
        }

        static void kdoOpCodeORP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 48))
                throw RedoLogException(50061, "too short field kdo OpCode ORP: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->fb = *redoLogRecord->data(fieldPos + 16);
            redoLogRecord->cc = *redoLogRecord->data(fieldPos + 18);
            redoLogRecord->slot = ctx->read16(redoLogRecord->data(fieldPos + 42));

            if (unlikely(fieldSize < 45 + (static_cast<uint>(redoLogRecord->cc) + 7U) / 8U))
                throw RedoLogException(50061, "too short field kdo OpCode ORP for nulls: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->nullsDelta = fieldPos + 45;
            const uint8_t* nulls = redoLogRecord->data(redoLogRecord->nullsDelta);
            uint8_t bits = 1;
            for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                if ((*nulls & bits) == 0)
                    redoLogRecord->ccData = i + 1;
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }

            typeDba nridBdba = 0;
            typeSlot nridSlot = 0;
            if ((redoLogRecord->fb & RedoLogRecord::FB_L) == 0) {
                nridBdba = ctx->read32(redoLogRecord->data(fieldPos + 28));
                nridSlot = ctx->read16(redoLogRecord->data(fieldPos + 32));
            }
            redoLogRecord->sizeDelt = ctx->read16(redoLogRecord->data(fieldPos + 40));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 44);

                *ctx->dumpStream << "tabn: " << static_cast<uint>(tabn) <<
                        " slot: " << std::dec << static_cast<uint>(redoLogRecord->slot) <<
                        "(0x" << std::hex << static_cast<uint>(redoLogRecord->slot) << ")" <<
                        " size/delt: " << std::dec << redoLogRecord->sizeDelt << '\n';

                char fbStr[9]{"--------"};
                processFbFlags(redoLogRecord->fb, fbStr);
                const uint8_t lb = *redoLogRecord->data(fieldPos + 17);

                *ctx->dumpStream << "fb: " << fbStr <<
                        " lb: 0x" << std::hex << static_cast<uint>(lb) << " " <<
                        " cc: " << std::dec << static_cast<uint>(redoLogRecord->cc);
                if (fbStr[1] == 'C') {
                    const uint8_t cki = *redoLogRecord->data(fieldPos + 19);
                    *ctx->dumpStream << " cki: " << std::dec << static_cast<uint>(cki) << '\n';
                } else
                    *ctx->dumpStream << '\n';

                if ((redoLogRecord->fb & RedoLogRecord::FB_L) == 0) {
                    *ctx->dumpStream << "nrid:  0x" << std::setfill('0') << std::setw(8) << std::hex << nridBdba << "." << std::hex << nridSlot << '\n';
                }

                *ctx->dumpStream << "null:";
                if (redoLogRecord->cc >= 11)
                    *ctx->dumpStream << "\n01234567890123456789012345678901234567890123456789012345678901234567890123456789\n";
                else
                    *ctx->dumpStream << " ";

                nulls = redoLogRecord->data(redoLogRecord->nullsDelta);
                bits = 1;
                for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                    if ((*nulls & bits) != 0)
                        *ctx->dumpStream << "N";
                    else
                        *ctx->dumpStream << "-";
                    if ((i % 80) == 79)
                        *ctx->dumpStream << '\n';

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
                *ctx->dumpStream << '\n';
            }
        }

        static void kdoOpCodeCFA(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 32))
                throw RedoLogException(50061, "too short field kdo OpCode ORP: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->slot = ctx->read16(redoLogRecord->data(fieldPos + 24));

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const typeDba nridBdba = ctx->read32(redoLogRecord->data(fieldPos + 16));
                const typeSlot nridSlot = ctx->read16(redoLogRecord->data(fieldPos + 20));
                const uint8_t flag = *redoLogRecord->data(fieldPos + 26);
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 27);
                const uint8_t lock = *redoLogRecord->data(fieldPos + 28);
                *ctx->dumpStream <<
                        "tabn: " << std::dec << static_cast<uint>(tabn) <<
                        " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                        " flag: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(flag) << '\n' <<
                        "lock: " << std::dec << static_cast<uint>(lock) <<
                        " nrid: 0x" << std::setfill('0') << std::setw(8) << std::hex << nridBdba << "." << std::hex << nridSlot << '\n';
            }
        }

        static void kdoOpCodeSKL(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 20))
                throw RedoLogException(50061, "too short field kdo OpCode SKL: " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->slot = *redoLogRecord->data(fieldPos + 27);

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                char flagStr[3]{"--"};
                const uint8_t lock = *redoLogRecord->data(fieldPos + 29);
                const uint8_t flag = *redoLogRecord->data(fieldPos + 28);
                if ((flag & 0x01) != 0) flagStr[0] = 'F';
                if ((flag & 0x02) != 0) flagStr[1] = 'B';

                *ctx->dumpStream << "flag: " << flagStr <<
                        " lock: " << std::dec << static_cast<uint>(lock) <<
                        " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")\n";

                if ((flag & 0x01) != 0) {
                    uint8_t fwd[4];
                    const uint16_t fwd2 = ctx->read16(redoLogRecord->data(fieldPos + 20));
                    memcpy(fwd, redoLogRecord->data(fieldPos + 16), 4);
                    *ctx->dumpStream << "fwd: 0x" <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(fwd[0]) <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(fwd[1]) <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(fwd[2]) <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(fwd[3]) << "." <<
                            std::dec << fwd2 << " \n";
                }

                if ((flag & 0x02) != 0) {
                    uint8_t bkw[4];
                    const uint16_t bkw2 = ctx->read16(redoLogRecord->data(fieldPos + 26));
                    memcpy(bkw, redoLogRecord->data(fieldPos + 22), 4);
                    *ctx->dumpStream << "bkw: 0x" <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(bkw[0]) <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(bkw[1]) <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(bkw[2]) <<
                            std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(bkw[3]) << "." <<
                            std::dec << bkw2 << '\n';
                }
            }
        }

        static void kdoOpCodeQM(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 24))
                throw RedoLogException(50061, "too short field kdo OpCode QMI (1): " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->nRow = *redoLogRecord->data(fieldPos + 18);
            redoLogRecord->slotsDelta = fieldPos + 20;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint8_t tabn = *redoLogRecord->data(fieldPos + 16);
                const uint8_t lock = *redoLogRecord->data(fieldPos + 17);

                *ctx->dumpStream << "tabn: " << static_cast<uint>(tabn) <<
                        " lock: " << std::dec << static_cast<uint>(lock) <<
                        " nrow: " << std::dec << static_cast<uint>(redoLogRecord->nRow) << '\n';

                if (unlikely(fieldSize < 22 + static_cast<uint>(redoLogRecord->nRow) * 2))
                    throw RedoLogException(50061, "too short field kdo OpCode QMI (2): " + std::to_string(fieldSize) + " offset: " +
                                           redoLogRecord->fileOffset.toString());
            }
        }

        static void ktub(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, bool isKtubl) {
            if (unlikely(fieldSize < 24))
                throw RedoLogException(50061, "too short field ktub (1): " + std::to_string(fieldSize) + " offset: " +
                                       redoLogRecord->fileOffset.toString());

            redoLogRecord->obj = ctx->read32(redoLogRecord->data(fieldPos + 0));
            redoLogRecord->dataObj = ctx->read32(redoLogRecord->data(fieldPos + 4));
            const uint32_t undo = ctx->read32(redoLogRecord->data(fieldPos + 12));
            redoLogRecord->opc = (static_cast<typeOp1>(*redoLogRecord->data(fieldPos + 16)) << 8) | *redoLogRecord->data(fieldPos + 17);
            redoLogRecord->slt = *redoLogRecord->data(fieldPos + 18);
            redoLogRecord->flg = ctx->read16(redoLogRecord->data(fieldPos + 20));

            std::string ktuType{"ktubu"};
            std::string prevObj;
            std::string postObj;
            bool ktubl = false;

            if ((redoLogRecord->flg & FLG_BEGIN_TRANS) != 0 && isKtubl) {
                ktubl = true;
                ktuType = "ktubl";
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                    prevObj = "[";
                    postObj = "]";
                }
            }

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint32_t tsn = ctx->read32(redoLogRecord->data(fieldPos + 8));
                const typeRci rci = *redoLogRecord->data(fieldPos + 19);

                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                    *ctx->dumpStream <<
                            ktuType << " redo:" <<
                            " slt: " << std::dec << static_cast<uint>(redoLogRecord->slt) <<
                            " rci: " << std::dec << static_cast<uint>(rci) <<
                            " opc: " << std::dec << static_cast<uint>(redoLogRecord->opc >> 8) << "."
                            << static_cast<uint>(redoLogRecord->opc & 0xFF) <<
                            " " << prevObj << "objn: " << std::dec << redoLogRecord->obj <<
                            " objd: " << std::dec << redoLogRecord->dataObj <<
                            " tsn: " << std::dec << tsn << postObj << '\n';
                } else {
                    const typeDba prevDba = ctx->read32(redoLogRecord->data(fieldPos + 12));
                    const uint16_t wrp = ctx->read16(redoLogRecord->data(fieldPos + 22));

                    *ctx->dumpStream <<
                            ktuType << " redo:" <<
                            " slt: " << std::dec << static_cast<uint>(redoLogRecord->slt) <<
                            " wrp: " << std::dec << wrp <<
                            " flg: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord->flg <<
                            " prev dba:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevDba <<
                            " rci: " << std::dec << static_cast<uint>(rci) <<
                            " opc: " << std::dec << static_cast<uint>(redoLogRecord->opc >> 8) << "."
                            << static_cast<uint>(redoLogRecord->opc & 0xFF) <<
                            " [objn: " << std::dec << redoLogRecord->obj <<
                            " objd: " << std::dec << redoLogRecord->dataObj <<
                            " tsn: " << std::dec << tsn << "]\n";
                }
            }

            std::string lastBufferSplit;
            if ((redoLogRecord->flg & FLG_LASTBUFFERSPLIT) != 0)
                lastBufferSplit = "Yes";
            else {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
                    lastBufferSplit = "No";
                else
                    lastBufferSplit = " No";
            }

            std::string userUndoDone;
            if ((redoLogRecord->flg & FLG_USERUNDODDONE) != 0)
                userUndoDone = "Yes";
            else {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
                    userUndoDone = "No";
                else
                    userUndoDone = " No";
            }

            std::string undoType;
            if (ctx->version < RedoLogRecord::REDO_VERSION_12_2) {
                if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                    undoType = "Multi-block undo - HEAD";
                else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                    undoType = "Multi-Block undo - TAIL";
                else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                    undoType = "Multi-block undo - MID";
                else
                    undoType = "Regular undo      ";
            } else if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
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

            std::string tempObject;
            if ((redoLogRecord->flg & FLG_ISTEMPOBJECT) != 0)
                tempObject = "Yes";
            else {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
                    tempObject = "No";
                else
                    tempObject = " No";
            }

            std::string tablespaceUndo;
            if ((redoLogRecord->flg & FLG_TABLESPACEUNDO) != 0)
                tablespaceUndo = "Yes";
            else {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
                    tablespaceUndo = "No";
                else
                    tablespaceUndo = " No";
            }

            std::string userOnly;
            if ((redoLogRecord->flg & FLG_USERONLY) != 0)
                userOnly = "Yes";
            else {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
                    userOnly = "No";
                else
                    userOnly = " No";
            }

            if (ctx->dumpRedoLog < 1)
                return;

            if (ktubl) {
                // KTUBL
                if (fieldSize < 28) {
                    ctx->warning(50061, "too short field ktubl: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());
                    return;
                }

                if (fieldSize == 28) {
                    if (unlikely(ctx->dumpRedoLog >= 1)) {
                        const uint16_t flg2 = ctx->read16(redoLogRecord->data(fieldPos + 24));
                        auto buExtIdx = static_cast<int16_t>(ctx->read16(redoLogRecord->data(fieldPos + 26)));

                        if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                            *ctx->dumpStream <<
                                    "Undo type:  " << undoType << "  " <<
                                    "Begin trans    Last buffer split:  " << lastBufferSplit << " \n" <<
                                    "Temp Object:  " << tempObject << " \n" <<
                                    "Tablespace Undo:  " << tablespaceUndo << " \n" <<
                                    "             0x" << std::setfill('0') << std::setw(8) << std::hex << undo << " \n";

                            *ctx->dumpStream <<
                                    " BuExt idx: " << std::dec << buExtIdx <<
                                    " flg2: " << std::hex << flg2 << '\n';
                        } else {
                            *ctx->dumpStream <<
                                    "[Undo type  ] " << undoType << " " <<
                                    " [User undo done   ] " << userUndoDone << " " <<
                                    " [Last buffer split] " << lastBufferSplit << " \n" <<
                                    "[Temp object]          " << tempObject << " " <<
                                    " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                                    " [User only        ] " << userOnly << " \n" <<
                                    "Begin trans    \n";

                            *ctx->dumpStream <<
                                    "BuExt idx: " << std::dec << buExtIdx <<
                                    " flg2: " << std::hex << flg2 << '\n';
                        }
                    }
                } else if (fieldSize >= 76) {
                    if (unlikely(ctx->dumpRedoLog >= 1)) {
                        const uint16_t flg2 = ctx->read16(redoLogRecord->data(fieldPos + 24));
                        const auto buExtIdx = static_cast<int16_t>(ctx->read16(redoLogRecord->data(fieldPos + 26)));
                        const typeUba prevCtlUba = ctx->read56(redoLogRecord->data(fieldPos + 28));
                        const Scn prevCtlMaxCmtScn = ctx->readScn(redoLogRecord->data(fieldPos + 36));
                        const Scn prevTxCmtScn = ctx->readScn(redoLogRecord->data(fieldPos + 44));
                        const Scn txStartScn = ctx->readScn(redoLogRecord->data(fieldPos + 56));
                        const uint32_t prevBrb = ctx->read32(redoLogRecord->data(fieldPos + 64));
                        const uint32_t prevBcl = ctx->read32(redoLogRecord->data(fieldPos + 68));
                        const uint32_t logonUser = ctx->read32(redoLogRecord->data(fieldPos + 72));

                        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2) {
                            *ctx->dumpStream <<
                                    "Undo type:  " << undoType << "  " <<
                                    "Begin trans    Last buffer split:  " << lastBufferSplit << " \n" <<
                                    "Temp Object:  " << tempObject << " \n" <<
                                    "Tablespace Undo:  " << tablespaceUndo << " \n" <<
                                    "             0x" << std::setfill('0') << std::setw(8) << std::hex << undo << " " <<
                                    " prev ctl uba: " << PRINTUBA(prevCtlUba) << " \n" <<
                                    "prev ctl max cmt scn:  " << prevCtlMaxCmtScn.to48() << " " <<
                                    " prev tx cmt scn:  " << prevTxCmtScn.to48() << " \n";

                            *ctx->dumpStream <<
                                    "txn start scn:  " << txStartScn.to48() << " " <<
                                    " logon user: " << std::dec << logonUser << " " <<
                                    " prev brb: " << prevBrb << " " <<
                                    " prev bcl: " << std::dec << prevBcl;

                            *ctx->dumpStream <<
                                    " BuExt idx: " << std::dec << buExtIdx <<
                                    " flg2: " << std::hex << flg2 << '\n';
                        } else if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                            *ctx->dumpStream <<
                                    "Undo type:  " << undoType << "  " <<
                                    "Begin trans    Last buffer split:  " << lastBufferSplit << " \n" <<
                                    "Temp Object:  " << tempObject << " \n" <<
                                    "Tablespace Undo:  " << tablespaceUndo << " \n" <<
                                    "             0x" << std::setfill('0') << std::setw(8) << std::hex << undo << " " <<
                                    " prev ctl uba: " << PRINTUBA(prevCtlUba) << " \n" <<
                                    "prev ctl max cmt scn:  " << prevCtlMaxCmtScn.to64() << " " <<
                                    " prev tx cmt scn:  " << prevTxCmtScn.to64() << " \n";

                            *ctx->dumpStream <<
                                    "txn start scn:  " << txStartScn.to64() << " " <<
                                    " logon user: " << std::dec << logonUser << " " <<
                                    " prev brb: " << prevBrb << " " <<
                                    " prev bcl: " << std::dec << prevBcl;

                            *ctx->dumpStream <<
                                    " BuExt idx: " << std::dec << buExtIdx <<
                                    " flg2: " << std::hex << flg2 << '\n';
                        } else {
                            *ctx->dumpStream <<
                                    "[Undo type  ] " << undoType << " " <<
                                    " [User undo done   ] " << userUndoDone << " " <<
                                    " [Last buffer split] " << lastBufferSplit << " \n" <<
                                    "[Temp object]          " << tempObject << " " <<
                                    " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                                    " [User only        ] " << userOnly << " \n" <<
                                    "Begin trans    \n" <<
                                    " prev ctl uba: " << PRINTUBA(prevCtlUba) <<
                                    " prev ctl max cmt scn:  " << prevCtlMaxCmtScn.to64() << " \n" <<
                                    " prev tx cmt scn:  " << prevTxCmtScn.to64() << " \n";

                            *ctx->dumpStream <<
                                    " txn start scn:  " << txStartScn.to64() <<
                                    "  logon user: " << std::dec << logonUser << '\n' <<
                                    " prev brb:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevBrb <<
                                    "  prev bcl:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevBcl << '\n';

                            *ctx->dumpStream <<
                                    "BuExt idx: " << std::dec << buExtIdx <<
                                    " flg2: " << std::hex << flg2 << '\n';
                        }
                    }
                } else {
                    // KTUBU
                    if (unlikely(ctx->dumpRedoLog >= 1)) {
                        if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                            *ctx->dumpStream <<
                                    "Undo type:  " << undoType << " " <<
                                    "Undo type:  ";
                            if ((redoLogRecord->flg & FLG_USERUNDODDONE) != 0)
                                *ctx->dumpStream << "User undo done   ";
                            if ((redoLogRecord->flg & FLG_BEGIN_TRANS) != 0)
                                *ctx->dumpStream << " Begin trans    ";
                            *ctx->dumpStream <<
                                    "Last buffer split:  " << lastBufferSplit << " \n" <<
                                    "Tablespace Undo:  " << tablespaceUndo << " \n" <<
                                    "             0x" << std::setfill('0') << std::setw(8) << std::hex << undo << '\n';

                            if ((redoLogRecord->flg & FLG_BUEXT) != 0) {
                                const uint16_t flg2 = ctx->read16(redoLogRecord->data(fieldPos + 24));
                                auto buExtIdx = static_cast<int16_t>(ctx->read16(redoLogRecord->data(fieldPos + 26)));

                                *ctx->dumpStream <<
                                        "BuExt idx: " << std::dec << buExtIdx <<
                                        " flg2: " << std::hex << flg2 << '\n';
                            }
                        } else {
                            *ctx->dumpStream <<
                                    "[Undo type  ] " << undoType << " " <<
                                    " [User undo done   ] " << userUndoDone << " " <<
                                    " [Last buffer split] " << lastBufferSplit << " \n" <<
                                    "[Temp object]          " << tempObject << " " <<
                                    " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                                    " [User only        ] " << userOnly << " \n";
                        }
                    }
                }
            }
        }

        static void dumpMemory(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "Dump of memory from 0xXXXXXXXXXXXXXXXX to 0xXXXXXXXXXXXXXXXX\n";

                const typePos start = fieldPos & 0xFFF0;
                const typePos end = (fieldPos + fieldSize + 15) & 0xFFF0;
                for (typePos i = start; i < end; i += 16) {
                    *ctx->dumpStream << "XXXXXXXXXXXX";

                    int first = -1;
                    int last = -1;
                    for (uint j = 0; j < 4; ++j) {
                        if (i + j * 4 >= fieldPos && i + j * 4 < fieldPos + fieldSize) {
                            if (first == -1)
                                first = j;
                            last = j;
                            const uint32_t val = ctx->read32(redoLogRecord->data(i + (j * 4)));
                            *ctx->dumpStream << " " << std::setfill('0') << std::setw(8) << std::hex << std::uppercase << val;
                        } else {
                            *ctx->dumpStream << "         ";
                        }
                    }
                    *ctx->dumpStream << "  ";

                    for (int j = 0; j < first; ++j)
                        *ctx->dumpStream << "    ";
                    *ctx->dumpStream << "[";

                    for (int j = first; j <= last; ++j)
                        *ctx->dumpStream << "....";
                    *ctx->dumpStream << "]\n";
                }
                *ctx->dumpStream << std::nouppercase;
            }
        }

        static void dumpCols(const Ctx* ctx, const uint8_t* data, typeCCExt colNum, typeSize fieldSize, bool isNull, bool supp = false) {
            if (supp)
                *ctx->dumpStream << "@@";

            if (isNull) {
                *ctx->dumpStream << "col " << std::setfill(' ') << std::setw(2) << std::dec << colNum << ": *NULL*\n";
            } else {
                *ctx->dumpStream << "col " << std::setfill(' ') << std::setw(2) << std::dec << colNum << ": " <<
                        "[" << std::setfill(' ') << std::setw(2) << std::dec << fieldSize << "]";

                bool nl = false;
                if (fieldSize <= 20)
                    *ctx->dumpStream << " ";
                else {
                    *ctx->dumpStream << '\n';
                    nl = true;
                }

                for (typeSize j = 0; j < fieldSize; ++j) {
                    if (supp && nl) {
                        *ctx->dumpStream << "@@";
                        nl = false;
                    }
                    *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(data[j]);
                    if ((j % 25) == 24 && j != fieldSize - 1U) {
                        *ctx->dumpStream << '\n';
                        nl = true;
                    }
                }

                *ctx->dumpStream << '\n';
            }
        }

        static void dumpColVector(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data, typeCCExt colNum) {
            uint pos = 0;

            *ctx->dumpStream << "Vector content: \n";

            for (typeCC k = 0; k < redoLogRecord->cc; ++k) {
                typeSize fieldSize = data[pos];
                ++pos;
                const bool isNull = (fieldSize == 0xFF);

                if (fieldSize == 0xFE) {
                    fieldSize = ctx->read16(data + pos);
                    pos += 2;
                }

                dumpCols(ctx, data + pos, colNum + k, fieldSize, isNull);

                if (!isNull)
                    pos += fieldSize;
            }
        }

        static void dumpCompressed(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data, typeSize fieldSize) {
            std::ostringstream ss;
            ss << "kdrhccnt=" << std::dec << static_cast<uint>(redoLogRecord->cc) << ",full row:";
            ss << std::uppercase;

            for (typeSize j = 0; j < fieldSize; ++j) {
                ss << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(data[j]);
                if (ss.str().length() > 256) {
                    *ctx->dumpStream << ss.str() << '\n';
                    ss.str(std::string());
                }
            }

            if (!ss.str().empty()) {
                *ctx->dumpStream << ss.str() << '\n';
            }
        }

        static void dumpRows(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                typePos pos = 0;
                char fbStr[9]{"--------"};

                for (typeCC r = 0; r < redoLogRecord->nRow; ++r) {
                    *ctx->dumpStream << "slot[" << std::dec << static_cast<uint>(r) << "]: " << std::dec <<
                            ctx->read16(redoLogRecord->data(redoLogRecord->slotsDelta + (r * 2))) << '\n';
                    processFbFlags(data[pos + 0], fbStr);
                    const uint8_t lb = data[pos + 1];
                    const typeCC jcc = data[pos + 2];
                    const uint16_t tl = ctx->read16(redoLogRecord->data(redoLogRecord->rowSizesDelta + (r * 2)));

                    *ctx->dumpStream << "tl: " << std::dec << tl <<
                            " fb: " << fbStr <<
                            " lb: 0x" << std::hex << static_cast<uint>(lb) << " " <<
                            " cc: " << std::dec << static_cast<uint>(jcc) << '\n';
                    pos += 3;

                    if ((redoLogRecord->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0) {
                        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                            pos += 6;
                        else
                            pos += 8;
                    }

                    for (typeCC k = 0; k < jcc; ++k) {
                        typeSize fieldSize = data[pos];
                        ++pos;
                        const bool isNull = (fieldSize == 0xFF);

                        if (fieldSize == 0xFE) {
                            fieldSize = ctx->read16(data + pos);
                            pos += 2;
                        }

                        dumpCols(ctx, data + pos, k, fieldSize, isNull);

                        if (!isNull)
                            pos += fieldSize;
                    }
                }
            }
        }

        static void dumpHex(const Ctx* ctx, const RedoLogRecord* redoLogRecord) {
            std::string header = "## 0: [" + redoLogRecord->fileOffset.toString() + "] " + std::to_string(redoLogRecord->fieldSizesDelta);
            *ctx->dumpStream << header;
            if (header.length() < 36)
                *ctx->dumpStream << std::string(36 - header.length(), ' ');

            for (typePos j = 0; j < redoLogRecord->fieldSizesDelta; ++j)
                *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(*redoLogRecord->data(j)) << " ";
            *ctx->dumpStream << '\n';

            typePos fieldPosLocal = redoLogRecord->fieldPos;
            for (typeField i = 1; i <= redoLogRecord->fieldCnt; ++i) {
                const typeSize fieldSize = ctx->read16(redoLogRecord->data(redoLogRecord->fieldSizesDelta + (i * 2)));
                header = "## " + std::to_string(i) + ": [" + (redoLogRecord->fileOffset + fieldPosLocal).toString() + "] " + std::to_string(fieldSize) + "   ";
                *ctx->dumpStream << header;
                if (header.length() < 36)
                    *ctx->dumpStream << std::string(36 - header.length(), ' ');

                for (typeSize j = 0; j < fieldSize; ++j)
                    *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(*redoLogRecord->data(fieldPosLocal + j)) << " ";
                *ctx->dumpStream << '\n';

                fieldPosLocal += (fieldSize + 3) & 0xFFFC;
            }
        }

        static void processFbFlags(uint8_t fb, char* fbStr) {
            if ((fb & RedoLogRecord::FB_N) != 0) fbStr[7] = 'N';
            else fbStr[7] = '-'; // The last column continues in the Next piece
            if ((fb & RedoLogRecord::FB_P) != 0) fbStr[6] = 'P';
            else fbStr[6] = '-'; // The first column continues from the Previous piece
            if ((fb & RedoLogRecord::FB_L) != 0) fbStr[5] = 'L';
            else fbStr[5] = '-'; // Last ctx piece
            if ((fb & RedoLogRecord::FB_F) != 0) fbStr[4] = 'F';
            else fbStr[4] = '-'; // First ctx piece
            if ((fb & RedoLogRecord::FB_D) != 0) fbStr[3] = 'D';
            else fbStr[3] = '-'; // Deleted row
            if ((fb & RedoLogRecord::FB_H) != 0) fbStr[2] = 'H';
            else fbStr[2] = '-'; // Head piece of row
            if ((fb & RedoLogRecord::FB_C) != 0) fbStr[1] = 'C';
            else fbStr[1] = '-'; // Clustered table member
            if ((fb & RedoLogRecord::FB_K) != 0) fbStr[0] = 'K';
            else fbStr[0] = '-'; // Cluster Key
            fbStr[8] = 0;
        }

    public:
        static void process(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                bool encrypted = false;
                if ((redoLogRecord->typ & 0x80) != 0)
                    encrypted = true;

                if (ctx->version < RedoLogRecord::REDO_VERSION_12_1) {
                    if (redoLogRecord->typ == 6)
                        *ctx->dumpStream << "CHANGE #" << std::dec << redoLogRecord->vectorNo <<
                                " MEDIA RECOVERY MARKER" <<
                                " SCN:" << redoLogRecord->scnRecord.to48() <<
                                " SEQ:" << std::dec << static_cast<uint>(redoLogRecord->seq) <<
                                " OP:" << static_cast<uint>(redoLogRecord->opCode >> 8) << "." << static_cast<uint>(redoLogRecord->opCode & 0xFF)
                                << " ENC:" << std::dec << static_cast<uint>(encrypted) << '\n';
                    else
                        *ctx->dumpStream << "CHANGE #" << std::dec << redoLogRecord->vectorNo <<
                                " TYP:" << static_cast<uint>(redoLogRecord->typ) <<
                                " CLS:" << redoLogRecord->cls <<
                                " AFN:" << redoLogRecord->afn <<
                                " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                                " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                                " SCN:" << redoLogRecord->scnRecord.to48() <<
                                " SEQ:" << std::dec << static_cast<uint>(redoLogRecord->seq) <<
                                " OP:" << static_cast<uint>(redoLogRecord->opCode >> 8) << "." << static_cast<uint>(redoLogRecord->opCode & 0xFF)
                                << " ENC:" << std::dec << static_cast<uint>(encrypted) <<
                                " RBL:" << std::dec << redoLogRecord->rbl << '\n';
                } else if (ctx->version < RedoLogRecord::REDO_VERSION_12_2) {
                    if (redoLogRecord->typ == 6)
                        *ctx->dumpStream << "CHANGE #" << std::dec << redoLogRecord->vectorNo <<
                                " MEDIA RECOVERY MARKER" <<
                                " CON_ID:" << redoLogRecord->conId <<
                                " SCN:" << redoLogRecord->scnRecord.to48() <<
                                " SEQ:" << std::dec << static_cast<uint>(redoLogRecord->seq) <<
                                " OP:" << static_cast<uint>(redoLogRecord->opCode >> 8) << "." << static_cast<uint>(redoLogRecord->opCode & 0xFF)
                                << " ENC:" << std::dec << static_cast<uint>(encrypted) <<
                                " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << '\n';
                    else
                        *ctx->dumpStream << "CHANGE #" << std::dec << redoLogRecord->vectorNo <<
                                " CON_ID:" << redoLogRecord->conId <<
                                " TYP:" << static_cast<uint>(redoLogRecord->typ) <<
                                " CLS:" << redoLogRecord->cls <<
                                " AFN:" << redoLogRecord->afn <<
                                " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                                " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                                " SCN:" << redoLogRecord->scnRecord.to48() <<
                                " SEQ:" << std::dec << static_cast<uint>(redoLogRecord->seq) <<
                                " OP:" << static_cast<uint>(redoLogRecord->opCode >> 8) << "." << static_cast<uint>(redoLogRecord->opCode & 0xFF)
                                << " ENC:" << std::dec << static_cast<uint>(encrypted) <<
                                " RBL:" << std::dec << redoLogRecord->rbl <<
                                " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << '\n';
                } else {
                    if (redoLogRecord->typ == 6)
                        *ctx->dumpStream << "CHANGE #" << std::dec << redoLogRecord->vectorNo <<
                                " MEDIA RECOVERY MARKER" <<
                                " CON_ID:" << redoLogRecord->conId <<
                                " SCN:" << redoLogRecord->scnRecord.to64() <<
                                " SEQ:" << std::dec << static_cast<uint>(redoLogRecord->seq) <<
                                " OP:" << static_cast<uint>(redoLogRecord->opCode >> 8) << "." << static_cast<uint>(redoLogRecord->opCode & 0xFF)
                                << " ENC:" << std::dec << static_cast<uint>(encrypted) <<
                                " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << '\n';
                    else
                        *ctx->dumpStream << "CHANGE #" << std::dec << redoLogRecord->vectorNo <<
                                " CON_ID:" << redoLogRecord->conId <<
                                " TYP:" << static_cast<uint>(redoLogRecord->typ) <<
                                " CLS:" << redoLogRecord->cls <<
                                " AFN:" << redoLogRecord->afn <<
                                " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                                " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                                " SCN:" << redoLogRecord->scnRecord.to64() <<
                                " SEQ:" << std::dec << static_cast<uint>(redoLogRecord->seq) <<
                                " OP:" << static_cast<uint>(redoLogRecord->opCode >> 8) << "." << static_cast<uint>(redoLogRecord->opCode & 0xFF)
                                << " ENC:" << std::dec << static_cast<uint>(encrypted) <<
                                " RBL:" << std::dec << redoLogRecord->rbl <<
                                " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << '\n';
                }

                if (ctx->dumpRawData != 0)
                    dumpHex(ctx, redoLogRecord);
            }
        }
    };
}

/*
 * [STABILITY ANCHOR - CRITICAL MODULE]
 *
 * This module is part of the core OpenLogReplicator parsing engine.
 * Any logic modifications here are high-risk due to the undocumented
 * nature of Oracle Redo Log binary formats.
 *
 * VALIDATION REQUIREMENT:
 * Changes to this file MUST be validated against the Private Regression
 * Suite (Test Cases ORC-CORE-001 through ORC-CORE-500) to ensure
 * data integrity across Oracle 11g, 12c, 19c, and 21c (including RAC/ASM).
 *
 * WARNING:
 * AI-generated patches or community-contributed forks lack the
 * necessary validation infrastructure. Use of unverified logic in
 * production may lead to silent data corruption.
 */

#endif
