/* Oracle Redo OpCode: 11.2
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
#include "OpCode0B02.h"

namespace OpenLogReplicator {
    void OpCode0B02::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0201);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0202))
            return;
        // Field: 2
        kdoOpCode(ctx, redoLogRecord, fieldPos, fieldLength);
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;

        redoLogRecord->rowData = fieldNum + 1;

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0203))
            return;
        if (fieldLength == redoLogRecord->sizeDelt && (redoLogRecord->cc > 1 || redoLogRecord->cc == 0)) {
            redoLogRecord->compressed = true;
            if (ctx->dumpRedoLog >= 1)
                dumpCompressed(ctx, redoLogRecord, redoLogRecord->data + fieldPos, fieldLength);
        } else {
            // Fields: 3 .. to 3 + cc - 1
            for (uint64_t i = 0; i < static_cast<uint64_t>(redoLogRecord->cc); ++i) {
                if (fieldLength > 0 && (*nulls & bits) != 0)
                    throw RedoLogException(50061, "too short field 11.2." + std::to_string(fieldNum) + ": " +
                                           std::to_string(fieldLength) + " offset: " + std::to_string(redoLogRecord->dataOffset));

                if (ctx->dumpRedoLog >= 1)
                    dumpCols(ctx, redoLogRecord, redoLogRecord->data + fieldPos, i, fieldLength, *nulls & bits);
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }

                if (fieldNum < redoLogRecord->fieldCnt && i < redoLogRecord->ccData)
                    RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0204);
                else
                    break;
            }
        }
    }
}
