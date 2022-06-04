/* Header for OpCode class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef OPCODE_H_
#define OPCODE_H_

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
