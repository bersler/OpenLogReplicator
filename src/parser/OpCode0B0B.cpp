/* Oracle Redo OpCode: 11.11
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
#include "OpCode0B0B.h"

namespace OpenLogReplicator {
    void OpCode0B0B::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0B01);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0B02))
            return;
        // Field: 2
        kdoOpCode(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0B03))
            return;
        // Field: 3
        redoLogRecord->rowLenghsDelta = fieldPos;
        if (fieldLength < redoLogRecord->nRow * 2)
            throw RedoLogException(50061, "too short field 11.11.3: " + std::to_string(fieldLength) + " offset: " +
                                   std::to_string(redoLogRecord->dataOffset));

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0B04))
            return;
        // Field: 4
        redoLogRecord->rowData = fieldNum;
        dumpRows(ctx, redoLogRecord, redoLogRecord->data + fieldPos);
    }
}
