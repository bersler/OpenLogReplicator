/* Oracle Redo OpCode: 19.1
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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
    void OpCode1301::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x130101);
        // Field: 1

        if (fieldLength < 36) {
            WARNING("too short field for 19.1: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->recordDataObj = redoLogRecord->dataObj;

        OpCode::process(ctx, redoLogRecord);
        if (ctx->dumpRedoLog >= 1) {
            uint32_t v2 = ctx->read32(redoLogRecord->data + fieldPos + 16);
            uint16_t v1 = ctx->read16(redoLogRecord->data + fieldPos + 20);
            uint32_t pageno = ctx->read32(redoLogRecord->data + fieldPos + 24);
            uint32_t pdba = ctx->read32(redoLogRecord->data + fieldPos + 28);
            uint8_t lobid[10];
            memcpy(lobid, redoLogRecord->data + fieldPos + 4, 10);

            ctx->dumpStream << "Direct Loader block redo entry" << std::endl;
            ctx->dumpStream << "Long field block dump:" << std::endl;
            ctx->dumpStream << "Object Id    " << std::dec << redoLogRecord->dataObj << " " << std::endl;
            ctx->dumpStream << "LobId: " << PRINTLOBID(lobid) <<
                    " PageNo " << std::setfill(' ') << std::setw(8) << std::dec << std::right << pageno << " " << std::endl;
            ctx->dumpStream << "Version: 0x" << std::setfill('0') << std::setw(4) << std::hex << v1 <<
                    "." << std::setfill('0') << std::setw(8) << std::hex << v2 <<
                    "  pdba: " << std::setfill(' ') << std::setw(8) << std::dec << std::right << pdba << "  " << std::endl;

            for (uint64_t j = 0; j < fieldLength - 36; ++j) {
                ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->data[fieldPos + j + 36] << " ";
                if ((j % 24) == 23 && j != (uint64_t)fieldLength - 1)
                    ctx->dumpStream << std::endl << "    ";
            }
            ctx->dumpStream << std::endl;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x130102);
        // Field: 2
        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "Dump of memory from 0xXXXXXXXXXXXXXXXX to 0xXXXXXXXXXXXXXXXX" << std::endl;

            for (uint64_t j = 0; j < fieldLength; j += 16) {
                uint64_t length = 16;
                if (j + length > fieldLength)
                    length = fieldLength - j + 1;

                if (length <= 4) {
                    uint32_t val = ctx->read32(redoLogRecord->data + fieldPos + j);
                    ctx->dumpStream << "XXXXXXXXXXXX          " <<
                            std::setfill('0') << std::setw(8) << std::hex << std::uppercase << val <<
                            "                        [....]" << std::endl;
                }
            }
            ctx->dumpStream << std::nouppercase << std::endl;
        }
    }
}
