/* Oracle Redo OpCode: 5.6
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

#include "../common/RedoLogRecord.h"
#include "OpCode0506.h"

namespace OpenLogReplicator {
    void OpCode0506::init(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        uint64_t fieldPos = redoLogRecord->fieldPos;
        uint16_t fieldLength = ctx->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + 1 * 2);
        if (fieldLength < 8) {
            WARNING("too short field ktub: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        redoLogRecord->obj = ctx->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data + fieldPos + 4);
    }

    void OpCode0506::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        init(ctx, redoLogRecord);
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050601);
        // Field: 1
        ktub(ctx, redoLogRecord, fieldPos, fieldLength, true);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050602))
            return;
        // Field: 2
        ktuxvoff(ctx, redoLogRecord, fieldPos, fieldLength);
    }

    void OpCode0506::ktuxvoff(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (fieldLength < 8) {
            WARNING("too short field ktuxvoff: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset)
            return;
        }

        if (ctx->dumpRedoLog >= 1) {
            uint16_t off = ctx->read16(redoLogRecord->data + fieldPos + 0);
            uint16_t flg = ctx->read16(redoLogRecord->data + fieldPos + 4);

            ctx->dumpStream << "ktuxvoff: 0x" << std::setfill('0') << std::setw(4) << std::hex << off << " " <<
                    " ktuxvflg: 0x" << std::setfill('0') << std::setw(4) << std::hex << flg << std::endl;
        }
    }
}
