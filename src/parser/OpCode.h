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

#include "../common/Ctx.h"
#include "../common/RuntimeException.h"
#include "../common/RedoLogRecord.h"
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

namespace OpenLogReplicator {
    class OpCode {
    protected:
        static void ktbRedo(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength);
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
        static void dumpCols(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum, uint16_t fieldLength, uint8_t isNull);
        static void dumpColsVector(Ctx* ctx, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum);
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
