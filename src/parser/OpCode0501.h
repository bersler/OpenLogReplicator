/* Header for OpCode0501 class
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

#include "OpCode.h"

#ifndef OP_CODE_05_01_H_
#define OP_CODE_05_01_H_

namespace OpenLogReplicator {
    class OpCode0501 final : public OpCode {
    protected:
        static void init(const Ctx* ctx, RedoLogRecord* redoLogRecord);
        static void ktudb(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kteoputrn(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void kdilk(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void rowDeps(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize);
        static void suppLog(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize);
        static void opc0A16(const Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize);
        static void opc0B01(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize);
        static void opc0D17(const Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize);

    public:
        static void process0501(Ctx* ctx, RedoLogRecord* redoLogRecord);
    };
}

#endif
