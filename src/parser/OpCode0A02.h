/* Redo Log OP Code 10.2
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

#ifndef OP_CODE_0A_02_H_
#define OP_CODE_0A_02_H_

#include "../common/RedoLogRecord.h"
#include "OpCode.h"

namespace OpenLogReplicator {
    class OpCode0A02 final : public OpCode {
    public:
        static void process0A02(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            process(ctx, redoLogRecord);
            typePos fieldPos = 0;
            typeField fieldNum = 0;
            typeSize fieldSize = 0;
            uint16_t keys = 0;

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

                const uint8_t itl = *redoLogRecord->data(fieldPos);
                const uint8_t code = *redoLogRecord->data(fieldPos + 1);
                const uint16_t sno = ctx->read16(redoLogRecord->data(fieldPos + 2));
                const uint16_t rowSize = ctx->read16(redoLogRecord->data(fieldPos + 4));
                std::string codeStr;
                if (code == 0) {
                    codeStr = "SINGLE";
                } else if (code == 0x20) {
                    codeStr = "ARRAY";
                }

                *ctx->dumpStream << "REDO: ";
                if (ctx->version >= RedoLogRecord::REDO_VERSION_18_0)
                    *ctx->dumpStream << "0x" << std::hex << static_cast<uint>(code) << " ";
                *ctx->dumpStream << codeStr << " / -- / -- " << '\n';
                *ctx->dumpStream << "itl: " << std::dec << static_cast<uint>(itl) <<
                        ", sno: " << std::dec << sno <<
                        ", row size " << std::dec << rowSize << '\n';

                if (code == 0x20) {
                    if (fieldSize < 10)
                        return;
                    keys = ctx->read16(redoLogRecord->data(fieldPos + 8));
                    *ctx->dumpStream << "number of keys: " << std::dec << keys << '\n';

                    if (fieldSize < 12 + keys * 2)
                        return;
                    *ctx->dumpStream << "slots: \n";
                    for (uint i = 0; i < keys; ++i) {
                        const uint16_t val = ctx->read16(redoLogRecord->data(fieldPos + 12 + (i * 2)));
                        *ctx->dumpStream << " " << std::dec << val;
                    }
                    *ctx->dumpStream << '\n';
                }
            }

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0203))
                return;
            // Field: 3

            redoLogRecord->indKey = fieldPos;
            redoLogRecord->indKeySize = fieldSize;

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "insert key: (" << std::dec << fieldSize << "): ";

                if (fieldSize > 20)
                    *ctx->dumpStream << '\n';

                for (typeSize j = 0; j < fieldSize; ++j) {
                    *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(*redoLogRecord->data(fieldPos + j));
                    if ((j % 25) == 24 && j != fieldSize - 1U)
                        *ctx->dumpStream << '\n';
                }
                *ctx->dumpStream << '\n';
            }

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0204))
                return;
            // Field: 4

            redoLogRecord->indKeyData = fieldPos;
            redoLogRecord->indKeyDataSize = fieldSize;

            if (RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0A0205) && fieldSize >= keys * 2) {
                // Field: 5

                *ctx->dumpStream << "each key size is: \n";
                for (uint i = 0; i < keys; ++i) {
                    const uint16_t val = ctx->read16(redoLogRecord->data(fieldPos + (i * 2)));
                    *ctx->dumpStream << " " << std::dec << val;
                }
                *ctx->dumpStream << '\n';
            }

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "keydata: (" << std::dec << redoLogRecord->indKeyDataSize << "): ";

                if (redoLogRecord->indKeyDataSize > 20)
                    *ctx->dumpStream << '\n';

                for (typeSize j = 0; j < redoLogRecord->indKeyDataSize; ++j) {
                    *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex <<
                            static_cast<uint>(*redoLogRecord->data(redoLogRecord->indKeyData + j));
                    if ((j % 25) == 24 && j != redoLogRecord->indKeyDataSize - 1U)
                        *ctx->dumpStream << '\n';
                }
                *ctx->dumpStream << '\n';
            }
        }
    };
}

#endif
