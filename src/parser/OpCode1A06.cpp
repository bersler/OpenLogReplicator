/* Oracle Redo OpCode: 26.6
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
#include "OpCode1A06.h"

namespace OpenLogReplicator {
    void OpCode1A06::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0601);
        // Field: 1
        if (fieldLength < 12)
            throw RedoLogException(50061, "too short field 26.6.1: " + std::to_string(fieldLength) + " offset: " +
                                   std::to_string(redoLogRecord->dataOffset));

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0602);
        // Field: 2
        if (fieldLength < 32)
            throw RedoLogException(50061, "too short field 26.6.2: " + std::to_string(fieldLength) + " offset: " +
                                   std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->recordDataObj = ctx->read32(redoLogRecord->data + fieldPos + 24);

        OpCode::process(ctx, redoLogRecord);
        fieldPos = 0;
        fieldNum = 0;
        fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0603);
        // Field: 1
        kdliCommon(ctx, redoLogRecord, fieldPos, fieldLength);

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0604);
        // Field: 2
        kdli(ctx, redoLogRecord, fieldPos, fieldLength);

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0605);
        // Field: 3
        kdli(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0606))
            return;
        // Field: 4

        if (redoLogRecord->opc == KDLI_OP_BIMG) {
            kdliDataLoad(ctx, redoLogRecord, fieldPos, fieldLength);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x1A0607))
                return;
        }

        // Field: 4/5 - supplog?
        kdli(ctx, redoLogRecord, fieldPos, fieldLength);
    }
}
