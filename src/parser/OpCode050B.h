/* Redo Log OP Code 5.11
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef OP_CODE_05_0B_H_
#define OP_CODE_05_0B_H_

#include "../common/RedoLogRecord.h"
#include "OpCode.h"

namespace OpenLogReplicator {
    class OpCode050B final : public OpCode {
    protected:
        static void init(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            if (redoLogRecord->fieldCnt >= 1) {
                const typePos fieldPos = redoLogRecord->fieldPos;
                const typeSize fieldSize = ctx->read16(redoLogRecord->data(redoLogRecord->fieldSizesDelta + (1 * 2)));
                if (unlikely(fieldSize < 8))
                    throw RedoLogException(50061, "too short field 5.11: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

                redoLogRecord->obj = ctx->read32(redoLogRecord->data(fieldPos + 0));
                redoLogRecord->dataObj = ctx->read32(redoLogRecord->data(fieldPos + 4));
            }
        }

    public:
        static void process050B(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            init(ctx, redoLogRecord);
            process(ctx, redoLogRecord);
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
    };
}

#endif
