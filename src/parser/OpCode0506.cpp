/* Oracle Redo OpCode: 5.6
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
#include "OpCode0506.h"

namespace OpenLogReplicator {
    void OpCode0506::init(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        typePos fieldPos = redoLogRecord->fieldPos;
        typeSize fieldSize = ctx->read16(redoLogRecord->data() + redoLogRecord->fieldSizesDelta + 1 * 2);
        if (unlikely(fieldSize < 8))
            throw RedoLogException(50061, "too short field 5.6: " +
                                          std::to_string(fieldSize) + " offset: " + std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->obj = ctx->read32(redoLogRecord->data() + fieldPos + 0);
        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data() + fieldPos + 4);
    }

    void OpCode0506::process0506(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        init(ctx, redoLogRecord);
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050601);
        // Field: 1
        ktub(ctx, redoLogRecord, fieldPos, fieldSize, true);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050602))
            return;
        // Field: 2
        ktuxvoff(ctx, redoLogRecord, fieldPos, fieldSize);
    }

    void OpCode0506::ktuxvoff(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
        if (unlikely(fieldSize < 8))
            throw RedoLogException(50061, "too short field ktuxvoff: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            const uint16_t off = ctx->read16(redoLogRecord->data() + fieldPos + 0);
            const uint16_t flg = ctx->read16(redoLogRecord->data() + fieldPos + 4);

            *ctx->dumpStream << "ktuxvoff: 0x" << std::setfill('0') << std::setw(4) << std::hex << off << " " <<
                            " ktuxvflg: 0x" << std::setfill('0') << std::setw(4) << std::hex << flg << '\n';
        }
    }
}
