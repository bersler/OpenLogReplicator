/* Oracle Redo OpCode: 19.1
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
#include "OpCode1301.h"

namespace OpenLogReplicator {
    void OpCode1301::process1301(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x130101);
        // Field: 1

        if (unlikely(fieldSize < 36))
            throw RedoLogException(50061, "too short field 19.1.1: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data() + fieldPos + 0);
        redoLogRecord->recordDataObj = redoLogRecord->dataObj;
        redoLogRecord->lobId.set(redoLogRecord->data() + fieldPos + 4);
        redoLogRecord->lobPageNo = ctx->read32(redoLogRecord->data() + fieldPos + 24);
        redoLogRecord->lobData = fieldPos + 36;
        redoLogRecord->lobDataSize = fieldSize - 36;
        OpCode::process(ctx, redoLogRecord);

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            const uint32_t v2 = ctx->read32(redoLogRecord->data() + fieldPos + 16);
            const uint16_t v1 = ctx->read16(redoLogRecord->data() + fieldPos + 20);
            const typeDba dba = ctx->read32(redoLogRecord->data() + fieldPos + 28);

            *ctx->dumpStream << "Direct Loader block redo entry\n";
            *ctx->dumpStream << "Long field block dump:\n";
            *ctx->dumpStream << "Object Id    " << std::dec << redoLogRecord->dataObj << " \n";
            *ctx->dumpStream << "LobId: " << redoLogRecord->lobId.narrow() <<
                            " PageNo " << std::setfill(' ') << std::setw(8) << std::dec << std::right << redoLogRecord->lobPageNo << " \n";
            *ctx->dumpStream << "Version: 0x" << std::setfill('0') << std::setw(4) << std::hex << v1 <<
                            "." << std::setfill('0') << std::setw(8) << std::hex << v2 <<
                            "  pdba: " << std::setfill(' ') << std::setw(8) << std::dec << std::right << dba << "  \n";

            for (typeSize j = 0; j < fieldSize - 36U; ++j) {
                *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data()[fieldPos + j + 36]) << " ";
                if ((j % 24) == 23 && j != fieldSize - 1U)
                    *ctx->dumpStream << "\n    ";
            }
            *ctx->dumpStream << '\n';
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x130102);
        // Field: 2
        dumpMemory(ctx, redoLogRecord, fieldPos, fieldSize);
    }
}
