/* Header for OpCode class
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

#include "../common/types.h"

#ifndef OP_CODE_H_
#define OP_CODE_H_

namespace OpenLogReplicator {
    class Ctx;
    class RedoLogRecord;

    class OpCode {
    public:
        static constexpr uint16_t FLG_MULTIBLOCKUNDOHEAD = 0x0001;
        static constexpr uint16_t FLG_MULTIBLOCKUNDOTAIL = 0x0002;
        static constexpr uint16_t FLG_LASTBUFFERSPLIT = 0x0004;
        static constexpr uint16_t FLG_BEGIN_TRANS = 0x0008;
        static constexpr uint16_t FLG_USERUNDODDONE = 0x0010;
        static constexpr uint16_t FLG_ISTEMPOBJECT = 0x0020;
        static constexpr uint16_t FLG_USERONLY = 0x0040;
        static constexpr uint16_t FLG_TABLESPACEUNDO = 0x0080;
        static constexpr uint16_t FLG_MULTIBLOCKUNDOMID = 0x0100;
        static constexpr uint16_t FLG_BUEXT = 0x0800;
        static constexpr uint16_t FLG_ROLLBACK_OP0504 = 0x0004;

        static constexpr uint8_t KDLI_CODE_INFO = 0x01;
        static constexpr uint8_t KDLI_CODE_LOAD_COMMON = 0x02;
        static constexpr uint8_t KDLI_CODE_LOAD_DATA = 0x04;
        static constexpr uint8_t KDLI_CODE_ZERO = 0x05;
        static constexpr uint8_t KDLI_CODE_FILL = 0x06;
        static constexpr uint8_t KDLI_CODE_LMAP = 0x07;
        static constexpr uint8_t KDLI_CODE_LMAPX = 0x08;
        static constexpr uint8_t KDLI_CODE_SUPLOG = 0x09;
        static constexpr uint8_t KDLI_CODE_GMAP = 0x0A;
        static constexpr uint8_t KDLI_CODE_FPLOAD = 0x0B;
        static constexpr uint8_t KDLI_CODE_LOAD_LHB = 0x0C;
        static constexpr uint8_t KDLI_CODE_ALMAP = 0x0D;
        static constexpr uint8_t KDLI_CODE_ALMAPX = 0x0E;
        static constexpr uint8_t KDLI_CODE_LOAD_ITREE = 0x0F;
        static constexpr uint8_t KDLI_CODE_IMAP = 0x10;
        static constexpr uint8_t KDLI_CODE_IMAPX = 0x11;

    protected:
        static constexpr uint8_t FLAGS_XA = 0x01;
        static constexpr uint8_t FLAGS_XR = 0x02;
        static constexpr uint8_t FLAGS_CR = 0x03;
        static constexpr uint8_t FLAGS_KDO_KDOM2 = 0x80;

        static constexpr uint16_t FLG_KTUCF_OP0504 = 0x0002;

        static constexpr uint8_t KDLI_FLG2_122_DESCN = 0x01;
        static constexpr uint8_t KDLI_FLG2_122_OVR = 0x02;
        static constexpr uint8_t KDLI_FLG2_122_XFM = 0x04;
        static constexpr uint8_t KDLI_FLG2_122_BT = 0x08;
        static constexpr uint8_t KDLI_FLG2_122_IT = 0x10;
        static constexpr uint8_t KDLI_FLG2_122_HASH = 0x20;
        static constexpr uint8_t KDLI_FLG2_122_LID = 0x40;
        static constexpr uint8_t KDLI_FLG2_122_VER1 = 0x80;

        static constexpr uint8_t KDLI_FLG2_121_PFILL = 0x08;
        static constexpr uint8_t KDLI_FLG2_121_CMAP = 0x10;
        static constexpr uint8_t KDLI_FLG2_121_HASH = 0x20;
        static constexpr uint8_t KDLI_FLG2_121_LHB = 0x40;
        static constexpr uint8_t KDLI_FLG2_121_VER1 = 0x80;

        static constexpr uint8_t KDLI_FLG3_VLL = 0x80;

        static constexpr typeOp1 KDLI_OP_REDO = 0;
        static constexpr typeOp1 KDLI_OP_UNDO = 1;
        static constexpr typeOp1 KDLI_OP_CR = 2;
        static constexpr typeOp1 KDLI_OP_FRMT = 3;
        static constexpr typeOp1 KDLI_OP_INVL = 4;
        static constexpr typeOp1 KDLI_OP_LOAD = 5;
        static constexpr typeOp1 KDLI_OP_BIMG = 6;
        static constexpr typeOp1 KDLI_OP_SINV = 7;

        static constexpr uint8_t KDLI_TYPE_MASK = 0x70;
        static constexpr uint8_t KDLI_TYPE_NEW = 0x00;
        static constexpr uint8_t KDLI_TYPE_LOCK = 0x08;
        static constexpr uint8_t KDLI_TYPE_LHB = 0x10;
        static constexpr uint8_t KDLI_TYPE_DATA = 0x20;
        static constexpr uint8_t KDLI_TYPE_BTREE = 0x30;
        static constexpr uint8_t KDLI_TYPE_ITREE = 0x40;
        static constexpr uint8_t KDLI_TYPE_AUX = 0x60;
        static constexpr uint8_t KDLI_TYPE_VER1 = 0x80;

        static constexpr uint8_t KTBOP_F = 0x01;
        static constexpr uint8_t KTBOP_C = 0x02;
        static constexpr uint8_t KTBOP_Z = 0x03;
        static constexpr uint8_t KTBOP_L = 0x04;
        static constexpr uint8_t KTBOP_R = 0x05;
        static constexpr uint8_t KTBOP_N = 0x06;
        static constexpr uint8_t KTBOP_BLOCKCLEANOUT = 0x10;

        static constexpr uint8_t OPFLAG_BEGIN_TRANS = 0x01;

        static void ktbRedo(const Ctx* ctx, RedoLogRecord* redoLogRecord, const typePos fieldPos, const typeSize fieldSize);
        static void kdli(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdliInfo(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, const typeSize fieldSize, uint8_t code);
        static void kdliLoadCommon(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliLoadData(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliZero(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliFill(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliLmap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliLmapx(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliSuplog(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliGmap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliFpload(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliLoadLhb(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliAlmap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliAlmapx(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliLoadItree(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliImap(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliImapx(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, uint8_t code);
        static void kdliDataLoad(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdliCommon(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCode(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeIRP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeDRP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeLKR(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeURP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeORP(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeCFA(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeSKL(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdoOpCodeQM(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void ktub(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, bool isKtubl);
        static void dumpMemory(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void dumpCols(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data, typeCCExt colNum, typeSize fieldSize, uint8_t isNull);
        static void dumpColVector(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data, typeCCExt colNum);
        static void dumpCompressed(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data, typeSize fieldSize);
        static void dumpRows(const Ctx* ctx, const RedoLogRecord* redoLogRecord, const uint8_t* data);
        static void dumpHex(const Ctx* ctx, const RedoLogRecord* redoLogRecord);
        static void processFbFlags(uint8_t fb, char* fbStr);

    public:
        static void process(const Ctx* ctx, RedoLogRecord* redoLogRecord);
    };
}

#endif
