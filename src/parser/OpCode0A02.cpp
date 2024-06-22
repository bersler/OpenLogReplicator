/* Oracle Redo OpCode: 10.2
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
#include "OpCode0A02.h"

namespace OpenLogReplicator {
    void OpCode0A02::process0A02(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "index redo (kdxlin):  insert leaf row\n";
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0201);
        // Field: 1
        ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0202))
            return;
        // Field: 2

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            if (fieldSize < 6)
                return;

            const uint8_t itl = redoLogRecord->data()[fieldPos];
            const uint16_t sno = ctx->read16(redoLogRecord->data() + fieldPos + 2);
            const uint16_t rowSize = ctx->read16(redoLogRecord->data() + fieldPos + 4);

            *ctx->dumpStream << "REDO: SINGLE / -- / -- " << '\n';
            *ctx->dumpStream << "itl: " << std::dec << static_cast<uint64_t>(itl) <<
                            ", sno: " << std::dec << sno <<
                            ", row size " << std::dec << rowSize << '\n';
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0202))
            return;
        // Field: 3

        redoLogRecord->indKey = fieldPos;
        redoLogRecord->indKeySize = fieldSize;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "insert key: (" << std::dec << fieldSize << "): ";

            if (fieldSize > 20)
                *ctx->dumpStream << '\n';

            for (typeSize j = 0; j < fieldSize; ++j) {
                *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data()[fieldPos + j]);
                if ((j % 25) == 24 && j != fieldSize - 1U)
                    *ctx->dumpStream << '\n';
            }
            *ctx->dumpStream << '\n';
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0202))
            return;
        // Field: 4

        redoLogRecord->indKeyData = fieldPos;
        redoLogRecord->indKeyDataSize = fieldSize;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "keydata: (" << std::dec << fieldSize << "): ";

            if (fieldSize > 20)
                *ctx->dumpStream << '\n';

            for (typeSize j = 0; j < fieldSize; ++j) {
                *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data()[fieldPos + j]);
                if ((j % 25) == 24 && j != fieldSize - 1U)
                    *ctx->dumpStream << '\n';
            }
            *ctx->dumpStream << '\n';
        }
    }
}
