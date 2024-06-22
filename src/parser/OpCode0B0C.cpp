/* Oracle Redo OpCode: 11.12
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
#include "OpCode0B0C.h"

namespace OpenLogReplicator {
    void OpCode0B0C::process0B0C(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0C01);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0C02))
            return;
        // Field: 2
        kdoOpCode(ctx, redoLogRecord, fieldPos, fieldSize);

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_QMD) {
                for (typeCC i = 0; i < redoLogRecord->nRow; ++i)
                    *ctx->dumpStream << "slot[" << i << "]: " << std::dec << ctx->read16(redoLogRecord->data() + redoLogRecord->slotsDelta + i * 2) << '\n';
            }
        }
    }
}
