/* Oracle Redo OpCode: 10.18
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
#include "OpCode0A12.h"

namespace OpenLogReplicator {
    void OpCode0A12::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        if (ctx->dumpRedoLog >= 1) {
            uint64_t count = redoLogRecord->fieldCnt;
            ctx->dumpStream << "index redo (kdxlup): update keydata, count=" << std::dec << count << std::endl;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0A1201);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0202))
            return;
        // Field: 2

        if (ctx->dumpRedoLog >= 1) {
            if (fieldLength < 6)
                return;

            uint16_t itl = ctx->read16(redoLogRecord->data + fieldPos);
            uint16_t sno = ctx->read16(redoLogRecord->data + fieldPos + 2);
            uint16_t rowSize = ctx->read16(redoLogRecord->data + fieldPos + 4);

            ctx->dumpStream << "REDO: SINGLE / -- / -- " << std::endl;
            ctx->dumpStream << "itl: " << std::dec << itl <<
                    ", sno: " << std::dec << sno <<
                    ", row size " << std::dec << rowSize << std::endl;
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0202))
            return;
        // Field: 3

        redoLogRecord->indKeyData = fieldPos;
        redoLogRecord->indKeyDataLength = fieldLength;

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "keydata : (" << std::dec << fieldLength << "): ";

            if (fieldLength > 20)
                ctx->dumpStream << std::endl;

            for (uint64_t j = 0; j < fieldLength; ++j) {
                ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->data[fieldPos + j];
                if ((j % 25) == 24 && j != (uint64_t)fieldLength - 1)
                    ctx->dumpStream << std::endl;
            }
            ctx->dumpStream << std::endl;
        }
    }
}
