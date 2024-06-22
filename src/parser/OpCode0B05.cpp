/* Oracle Redo OpCode: 11.5
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
#include "OpCode0B05.h"

namespace OpenLogReplicator {
    void OpCode0B05::process0B05(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0501);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0502))
            return;
        // Field: 2
        kdoOpCode(ctx, redoLogRecord, fieldPos, fieldSize);
        const typeCC* nulls = redoLogRecord->data() + redoLogRecord->nullsDelta;

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0503))
            return;
        // Field: 3
        const typeCC* colNums = nullptr;
        if (fieldSize > 0 && redoLogRecord->cc > 0) {
            redoLogRecord->colNumsDelta = fieldPos;
            colNums = redoLogRecord->data() + redoLogRecord->colNumsDelta;
        }

        if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0504);
            // Field: 4
            redoLogRecord->rowData = fieldNum;
            if (unlikely(ctx->dumpRedoLog >= 1))
                dumpColVector(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, ctx->read16(colNums));
        } else if (colNums != nullptr) {
            redoLogRecord->rowData = fieldNum + 1;
            uint8_t bits = 1;

            // Fields: 4 + cc .. 4 + cc - 1
            for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                if (fieldNum >= redoLogRecord->fieldCnt)
                    break;
                if (i < redoLogRecord->ccData)
                    RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0506);

                if (unlikely(fieldSize > 0 && (*nulls & bits) != 0 && i < redoLogRecord->ccData))
                    throw RedoLogException(50061, "too short field 11.5." + std::to_string(fieldNum) + ": " +
                                                  std::to_string(fieldSize) + " offset: " + std::to_string(redoLogRecord->dataOffset));

                if (unlikely(ctx->dumpRedoLog >= 1))
                    dumpCols(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, ctx->read16(colNums), fieldSize, *nulls & bits);

                bits <<= 1;
                colNums += 2;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
        }
    }
}
