/* Oracle Redo OpCode: 26.2
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
#include "OpCode1A02.h"

namespace OpenLogReplicator {
    void OpCode1A02::process1A02(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x1A0201);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x1A0202);
        // Field: 2
        kdliCommon(ctx, redoLogRecord, fieldPos, fieldSize);

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x1A0203);
        // Field: 3`
        kdli(ctx, redoLogRecord, fieldPos, fieldSize);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x1A0204))
            return;
        // Field: 4
        kdli(ctx, redoLogRecord, fieldPos, fieldSize);
    }
}
