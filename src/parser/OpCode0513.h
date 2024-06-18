/* Header for OpCode0513 class
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

#ifndef OP_CODE_05_13_H_
#define OP_CODE_05_13_H_

namespace OpenLogReplicator {
    class Transaction;

    class OpCode0513 : public OpCode {
    protected:
        static void attribute(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, const char* header,
                              const char* name, Transaction* transaction);
        static void attributeSessionSerial(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction);
        static void attributeFlags(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction);
        static void attributeVersion(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction);
        static void attributeAuditSessionId(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction);

    public:
        static void process0513(const Ctx* ctx, RedoLogRecord* redoLogRecord, Transaction* transaction);
    };
}

#endif
