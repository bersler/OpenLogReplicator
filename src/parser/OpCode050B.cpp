/* Oracle Redo OpCode: 5.11
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
#include "OpCode050B.h"

namespace OpenLogReplicator {
    void OpCode050B::init(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        if (redoLogRecord->fieldCnt >= 1) {
            typePos fieldPos = redoLogRecord->fieldPos;
            typeSize fieldSize = ctx->read16(redoLogRecord->data() + redoLogRecord->fieldSizesDelta + 1 * 2);
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field 5.11: " + std::to_string(fieldSize) + " offset: " +
                                              std::to_string(redoLogRecord->dataOffset));

            redoLogRecord->obj = ctx->read32(redoLogRecord->data() + fieldPos + 0);
            redoLogRecord->dataObj = ctx->read32(redoLogRecord->data() + fieldPos + 4);
        }
    }

    void OpCode050B::process050B(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        init(ctx, redoLogRecord);
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050B01);
        // Field: 1
        if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
            ktub(ctx, redoLogRecord, fieldPos, fieldSize, false);
        else
            ktub(ctx, redoLogRecord, fieldPos, fieldSize, true);
    }
}
