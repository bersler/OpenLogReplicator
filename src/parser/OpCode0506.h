/* Redo Log OP Code 5.6
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

#ifndef OP_CODE_05_06_H_
#define OP_CODE_05_06_H_

#include "../common/RedoLogRecord.h"
#include "OpCode.h"

namespace OpenLogReplicator {
    class OpCode0506 final : public OpCode {
    protected:
        static void ktuxvoff(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field ktuxvoff: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                const uint16_t off = ctx->read16(redoLogRecord->data(fieldPos + 0));
                const uint16_t flg = ctx->read16(redoLogRecord->data(fieldPos + 4));

                *ctx->dumpStream << "ktuxvoff: 0x" << std::setfill('0') << std::setw(4) << std::hex << off << " " <<
                        " ktuxvflg: 0x" << std::setfill('0') << std::setw(4) << std::hex << flg << '\n';
            }
        }

        static void init(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            const typePos fieldPos = redoLogRecord->fieldPos;
            const typeSize fieldSize = ctx->read16(redoLogRecord->data(redoLogRecord->fieldSizesDelta + (1 * 2)));
            if (unlikely(fieldSize < 8))
                throw RedoLogException(50061, "too short field 5.6: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            redoLogRecord->obj = ctx->read32(redoLogRecord->data(fieldPos + 0));
            redoLogRecord->dataObj = ctx->read32(redoLogRecord->data(fieldPos + 4));
        }

    public:
        static void process0506(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            init(ctx, redoLogRecord);
            process(ctx, redoLogRecord);
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
    };
}

#endif
