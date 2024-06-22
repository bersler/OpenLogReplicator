/* Oracle Redo OpCode: 24.1
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
#include "OpCode1801.h"

namespace OpenLogReplicator {
    void OpCode1801::process1801(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        bool validDdl = false;

        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180101);
        // Field: 1
        if (unlikely(fieldSize < 18))
            throw RedoLogException(50061, "too short field 24.1.1: " +
                                          std::to_string(fieldSize) + " offset: " + std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->xid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data() + fieldPos + 4)),
                                     ctx->read16(redoLogRecord->data() + fieldPos + 6),
                                     ctx->read32(redoLogRecord->data() + fieldPos + 8));
        // uint16_t type = ctx->read16(redoLogRecord->ctx + fieldPos + 12);
        const uint16_t ddlType = ctx->read16(redoLogRecord->data() + fieldPos + 16);
        // uint16_t seq = ctx->read16(redoLogRecord->ctx + fieldPos + 18);
        // uint16_t cnt = ctx->read16(redoLogRecord->ctx + fieldPos + 20);

        // Temporary object
        if (ddlType != 4 && ddlType != 5 && ddlType != 6 && ddlType != 8 && ddlType != 9 && ddlType != 10)
            validDdl = true;

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180102))
            return;
        // Field: 2

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180103))
            return;
        // Field: 3

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180104))
            return;
        // Field: 4

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180105))
            return;
        // Field: 5

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180106))
            return;
        // Field: 6

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180107))
            return;
        // Field: 7

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180108))
            return;
        // Field: 8

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x180109))
            return;
        // Field: 9

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x18010A))
            return;
        // Field: 10

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x18010B))
            return;
        // Field: 11

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x18010C))
            return;
        // Field: 12

        if (validDdl)
            redoLogRecord->obj = ctx->read32(redoLogRecord->data() + fieldPos + 0);
    }
}
