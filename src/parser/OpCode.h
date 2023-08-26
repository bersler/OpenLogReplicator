/* Header for OpCode class
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

#include "../common/types.h"

#ifndef OP_CODE_H_
#define OP_CODE_H_

#define KTBOP_F                 0x01
#define KTBOP_C                 0x02
#define KTBOP_Z                 0x03
#define KTBOP_L                 0x04
#define KTBOP_R                 0x05
#define KTBOP_N                 0x06
#define KTBOP_BLOCKCLEANOUT     0x10

#define FLG_MULTIBLOCKUNDOHEAD  0x0001
#define FLG_MULTIBLOCKUNDOTAIL  0x0002
#define FLG_LASTBUFFERSPLIT     0x0004
#define FLG_BEGIN_TRANS         0x0008
#define FLG_USERUNDODDONE       0x0010
#define FLG_ISTEMPOBJECT        0x0020
#define FLG_USERONLY            0x0040
#define FLG_TABLESPACEUNDO      0x0080
#define FLG_MULTIBLOCKUNDOMID   0x0100
#define FLG_BUEXT               0x0800

#define FLAGS_XA                0x01
#define FLAGS_XR                0x02
#define FLAGS_CR                0x03
#define FLAGS_KDO_KDOM2         0x80

#define FLG_KTUCF_OP0504        0x0002
#define FLG_ROLLBACK_OP0504     0x0004

#define OPFLAG_BEGIN_TRANS      0x01

#define KDLI_OP_REDO            0
#define KDLI_OP_UNDO            1
#define KDLI_OP_CR              2
#define KDLI_OP_FRMT            3
#define KDLI_OP_INVL            4
#define KDLI_OP_LOAD            5
#define KDLI_OP_BIMG            6
#define KDLI_OP_SINV            7

#define KDLI_TYPE_MASK          0x70
#define KDLI_TYPE_NEW           0x00
#define KDLI_TYPE_LOCK          0x08
#define KDLI_TYPE_LHB           0x10
#define KDLI_TYPE_DATA          0x20
#define KDLI_TYPE_BTREE         0x30
#define KDLI_TYPE_ITREE         0x40
#define KDLI_TYPE_AUX           0x60
#define KDLI_TYPE_VER1          0x80

#define KDLI_CODE_INFO          0x01
#define KDLI_CODE_LOAD_COMMON   0x02
#define KDLI_CODE_LOAD_DATA     0x04
#define KDLI_CODE_ZERO          0x05
#define KDLI_CODE_FILL          0x06
#define KDLI_CODE_LMAP          0x07
#define KDLI_CODE_LMAPX         0x08
#define KDLI_CODE_SUPLOG        0x09
#define KDLI_CODE_GMAP          0x0A
#define KDLI_CODE_FPLOAD        0x0B
#define KDLI_CODE_LOAD_LHB      0x0C
#define KDLI_CODE_ALMAP         0x0D
#define KDLI_CODE_ALMAPX        0x0E
#define KDLI_CODE_LOAD_ITREE    0x0F
#define KDLI_CODE_IMAP          0x10
#define KDLI_CODE_IMAPX         0x11

#define KDLI_FLG2_122_DESCN     0x01
#define KDLI_FLG2_122_OVR       0x02
#define KDLI_FLG2_122_XFM       0x04
#define KDLI_FLG2_122_BT        0x08
#define KDLI_FLG2_122_IT        0x10
#define KDLI_FLG2_122_HASH      0x20
#define KDLI_FLG2_122_LID       0x40
#define KDLI_FLG2_122_VER1      0x80

#define KDLI_FLG2_121_PFILL     0x08
#define KDLI_FLG2_121_CMAP      0x10
#define KDLI_FLG2_121_HASH      0x20
#define KDLI_FLG2_121_LHB       0x40
#define KDLI_FLG2_121_VER1      0x80

#define KDLI_FLG3_VLL           0x80

namespace OpenLogReplicator {
    class Ctx;
    class RedoLogRecord;

    class OpCode {
    protected:
        static void ktbRedo(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdli(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdliInfo(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliLoadCommon(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliLoadData(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliZero(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliFill(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliLmap(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliLmapx(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliSuplog(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliGmap(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliFpload(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliLoadLhb(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliAlmap(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliAlmapx(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliLoadItree(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliImap(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliImapx(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, uint8_t code);
        static void kdliDataLoad(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdliCommon(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCode(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeIRP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeDRP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeLKR(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeURP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeORP(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeCFA(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeSKL(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void kdoOpCodeQM(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void ktub(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, bool isKtubl);
        static void dumpMemory(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
        static void dumpCols(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colNum, uint16_t fieldLength, uint8_t isNull);
        static void dumpColVector(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colNum);
        static void dumpCompressed(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint16_t fieldLength);
        static void dumpRows(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data);
        static void dumpVal(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength, const char* msg);
        static void dumpHex(Ctx* ctx, RedoLogRecord* redoLogRecord);
        static void processFbFlags(uint8_t fb, char* fbStr);

    public:

        static void process(Ctx* ctx, RedoLogRecord* redoLogRecord);
    };
}

#endif
