/* Oracle Redo OpCode: 5.1
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
#include "OpCode0501.h"

namespace OpenLogReplicator {
    void OpCode0501::init(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;
        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050101))
            return;
        // Field: 1

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050102))
            return;
        // Field: 2
        if (unlikely(fieldSize < 8))
            throw RedoLogException(50061, "too short field 5.1.2: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->obj = ctx->read32(redoLogRecord->data() + fieldPos + 0);
        redoLogRecord->dataObj = ctx->read32(redoLogRecord->data() + fieldPos + 4);
    }

    void OpCode0501::opc0A16(const Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize) {
        kdilk(ctx, redoLogRecord, fieldPos, fieldSize);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050103))
            return;
        // Field: 5

        redoLogRecord->indKey = fieldPos;
        redoLogRecord->indKeySize = fieldSize;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "key :(" << std::dec << fieldSize << "): ";

            if (fieldSize > 20)
                *ctx->dumpStream << '\n';

            for (typeSize j = 0; j < fieldSize; ++j) {
                *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data()[fieldPos + j]);
                if ((j % 25) == 24 && j != fieldSize - 1U)
                    *ctx->dumpStream << '\n';
            }
            *ctx->dumpStream << '\n';
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050104))
            return;
        // Field: 6

        redoLogRecord->indKeyData = fieldPos;
        redoLogRecord->indKeyDataSize = fieldSize;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "keydata/bitmap: (" << std::dec << fieldSize << "): ";

            if (fieldSize > 20)
                *ctx->dumpStream << '\n';

            for (typeSize j = 0; j < fieldSize; ++j) {
                *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data()[fieldPos + j]);
                if ((j % 25) == 24 && j != fieldSize - 1U)
                    *ctx->dumpStream << '\n';
            }
            *ctx->dumpStream << '\n';
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050105))
            return;
        // Field: 7

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "selflock: (" << std::dec << fieldSize << "): ";

            if (fieldSize > 20)
                *ctx->dumpStream << '\n';

            for (typeSize j = 0; j < fieldSize; ++j) {
                *ctx->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord->data()[fieldPos + j]);
                if ((j % 25) == 24 && j != fieldSize - 1U)
                    *ctx->dumpStream << '\n';
            }
            *ctx->dumpStream << '\n';
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050106))
            return;
        // Field: 8

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "bitmap: (" << std::dec << fieldSize << "): ";

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

    void OpCode0501::opc0B01(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize) {
        kdoOpCode(ctx, redoLogRecord, fieldPos, fieldSize);
        const typeCC* colNums = nullptr;
        const typeCC* nulls = redoLogRecord->data() + redoLogRecord->nullsDelta;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_QMD) {
                for (typeCC i = 0; i < redoLogRecord->nRow; ++i)
                    *ctx->dumpStream << "slot[" << i << "]: " << std::dec << ctx->read16(redoLogRecord->data() + redoLogRecord->slotsDelta + i * 2) << '\n';
            }
        }

        if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_URP) {
            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050107);
            // Field: 5
            if (fieldSize > 0 && redoLogRecord->cc > 0) {
                redoLogRecord->colNumsDelta = fieldPos;
                colNums = redoLogRecord->data() + redoLogRecord->colNumsDelta;
            }

            if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
                RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050108);

                redoLogRecord->rowData = fieldPos;
                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    dumpColVector(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, ctx->read16(colNums));
                }
            } else {
                redoLogRecord->rowData = fieldNum + 1;
                uint8_t bits = 1;

                for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                    if ((*nulls & bits) == 0) {
                        RedoLogRecord::skipEmptyFields(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
                        if (fieldNum >= redoLogRecord->fieldCnt)
                            return;
                        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050109);
                    }

                    if (unlikely(ctx->dumpRedoLog >= 1))
                        dumpCols(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, ctx->read16(colNums), fieldSize, *nulls & bits);
                    colNums += 2;
                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }

                if ((redoLogRecord->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0) {
                    RedoLogRecord::skipEmptyFields(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
                    RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05010A);
                    rowDeps(ctx, redoLogRecord, fieldPos, fieldSize);
                }

                suppLog(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
            }

        } else if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_DRP) {
            if ((redoLogRecord->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0) {
                RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05010B);
                rowDeps(ctx, redoLogRecord, fieldPos, fieldSize);
            }

            suppLog(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);

        } else if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_IRP || (redoLogRecord->op & 0x1F) == RedoLogRecord::OP_ORP) {
            if (unlikely(nulls == nullptr))
                throw RedoLogException(50063, "nulls field is missing on offset: " + std::to_string(redoLogRecord->dataOffset));

            if (redoLogRecord->cc > 0) {
                redoLogRecord->rowData = fieldNum + 1;
                if (fieldNum >= redoLogRecord->fieldCnt)
                    return;
                RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05010C);

                if (fieldSize == redoLogRecord->sizeDelt && redoLogRecord->cc > 1) {
                    redoLogRecord->compressed = true;
                    if (unlikely(ctx->dumpRedoLog >= 1))
                        dumpCompressed(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, fieldSize);
                } else {
                    uint8_t bits = 1;
                    for (typeCC i = 0; i < redoLogRecord->cc; ++i) {
                        if (i > 0) {
                            if (fieldNum >= redoLogRecord->fieldCnt)
                                return;
                            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05010D);
                        }
                        if (unlikely(fieldSize > 0 && (*nulls & bits) != 0))
                            throw RedoLogException(50061, "too short field for nulls: " + std::to_string(fieldSize) + " offset: " +
                                                          std::to_string(redoLogRecord->dataOffset));

                        if (unlikely(ctx->dumpRedoLog >= 1))
                            dumpCols(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, i, fieldSize, *nulls & bits);
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                    }
                }
            }

            if ((redoLogRecord->op & RedoLogRecord::OP_ROWDEPENDENCIES) != 0) {
                RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05010E);
                rowDeps(ctx, redoLogRecord, fieldPos, fieldSize);
            }

            suppLog(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);

        } else if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_QMI) {
            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05010F);
            redoLogRecord->rowSizesDelta = fieldPos;

            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050110);
            redoLogRecord->rowData = fieldNum;
            if (unlikely(ctx->dumpRedoLog >= 1))
                dumpRows(ctx, redoLogRecord, redoLogRecord->data() + fieldPos);

        } else if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_LMN) {
            suppLog(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);

        } else if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_LKR) {
            suppLog(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);

        } else if ((redoLogRecord->op & 0x1F) == RedoLogRecord::OP_CFA) {
            suppLog(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
        }
    }

    void OpCode0501::opc0D17(const Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize) {
        if (unlikely(fieldSize < 20))
            throw RedoLogException(50061, "too short field OPC 0D17: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            redoLogRecord->bdba = ctx->read32(redoLogRecord->data() + fieldPos + 0);
            const uint32_t fcls = ctx->read32(redoLogRecord->data() + fieldPos + 4);
            const typeDba l2dba = ctx->read32(redoLogRecord->data() + fieldPos + 8);
            const uint32_t scls = ctx->read32(redoLogRecord->data() + fieldPos + 12);
            const uint32_t offset = ctx->read32(redoLogRecord->data() + fieldPos + 16);

            *ctx->dumpStream << "Undo for Lev1 Bitmap Block\n";
            *ctx->dumpStream << "L1 DBA:  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->bdba <<
                            " L2 DBA:  0x" << std::setfill('0') << std::setw(8) << std::hex << l2dba <<
                            " fcls: " << std::dec << fcls <<
                            " scls: " << std::dec << scls <<
                            " offset: " << std::dec << offset << '\n';
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050111);
        // Field: 4

        if (fieldSize < 8) {
            ctx->warning(70001, "too short field lev1 bitmap block: " + std::to_string(fieldSize) + " offset: " +
                                std::to_string(redoLogRecord->dataOffset));
            return;
        }

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            *ctx->dumpStream << "Redo on Level1 Bitmap Block\n";

            if (fieldSize >= 16) {
                const uint32_t len = ctx->read32(redoLogRecord->data() + fieldPos + 4);
                const uint32_t offset = ctx->read32(redoLogRecord->data() + fieldPos + 12);
                const uint64_t netstate = 0; // Random value observed

                *ctx->dumpStream << "Redo for state change\n";
                *ctx->dumpStream << "Len: " << std::dec << len <<
                                " Offset: " << std::dec << offset <<
                                " newstate: " << std::dec << netstate << '\n';
            }
        }
    }

    void OpCode0501::process0501(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        init(ctx, redoLogRecord);
        OpCode::process(ctx, redoLogRecord);
        typePos fieldPos = 0;
        typeField fieldNum = 0;
        typeSize fieldSize = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050112);
        // Field: 1
        ktudb(ctx, redoLogRecord, fieldPos, fieldSize);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050113))
            return;
        // Field: 2
        ktub(ctx, redoLogRecord, fieldPos, fieldSize, true);

        // Incomplete ctx: don't analyze further
        if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID)) != 0)
            return;

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050114))
            return;
        // Field: 3

        switch (redoLogRecord->opc) {
            case 0x0A16:
                ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

                if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050115))
                    return;
                // Field: 4

                opc0A16(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
                break;

            case 0x0B01:
                ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

                if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050116))
                    return;
                // Field: 4

                opc0B01(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
                break;

            case 0x1A01:
                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    *ctx->dumpStream << "KDLI undo record:\n";
                }
                ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

                if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05011B))
                    return;
                // Field: 4
                kdliCommon(ctx, redoLogRecord, fieldPos, fieldSize);

                if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05011C))
                    return;
                kdli(ctx, redoLogRecord, fieldPos, fieldSize);
                break;

            case 0x0E08:
                kteoputrn(ctx, redoLogRecord, fieldPos, fieldSize);
                break;
        }
    }

    void OpCode0501::ktudb(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
        if (unlikely(fieldSize < 20))
            throw RedoLogException(50061, "too short field ktudb: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        redoLogRecord->xid = typeXid(static_cast<typeUsn>(ctx->read16(redoLogRecord->data() + fieldPos + 8)),
                                     ctx->read16(redoLogRecord->data() + fieldPos + 10),
                                     ctx->read32(redoLogRecord->data() + fieldPos + 12));

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            const uint16_t siz = ctx->read16(redoLogRecord->data() + fieldPos + 0);
            const uint16_t spc = ctx->read16(redoLogRecord->data() + fieldPos + 2);
            const uint16_t flgKtudb = ctx->read16(redoLogRecord->data() + fieldPos + 4);
            const uint16_t seq = ctx->read16(redoLogRecord->data() + fieldPos + 16);
            const uint8_t rec = redoLogRecord->data()[fieldPos + 18];

            *ctx->dumpStream << "ktudb redo:" <<
                            " siz: " << std::dec << siz <<
                            " spc: " << std::dec << spc <<
                            " flg: 0x" << std::setfill('0') << std::setw(4) << std::hex << flgKtudb <<
                            " seq: 0x" << std::setfill('0') << std::setw(4) << seq <<
                            " rec: 0x" << std::setfill('0') << std::setw(2) << static_cast<uint64_t>(rec) << '\n';
            *ctx->dumpStream << "           " <<
                            " xid:  " << redoLogRecord->xid.toString() << "  \n";
        }
    }

    void OpCode0501::kteoputrn(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
        if (unlikely(fieldSize < 4))
            throw RedoLogException(50061, "too short field kteoputrn: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        if (unlikely(ctx->dumpRedoLog >= 2)) {
            const typeObj newDataObj = ctx->read32(redoLogRecord->data() + fieldPos + 0);
            *ctx->dumpStream << "kteoputrn - undo operation for flush for truncate \n";
            *ctx->dumpStream << "newobjd: 0x" << std::hex << newDataObj << " \n";
        }
    }

    void OpCode0501::kdilk(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
        if (unlikely(fieldSize < 20))
            throw RedoLogException(50061, "too short field kdilk: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            const uint8_t code = redoLogRecord->data()[fieldPos + 0];
            const uint8_t itl = redoLogRecord->data()[fieldPos + 1];
            const uint8_t kdxlkflg = redoLogRecord->data()[fieldPos + 2];
            const uint32_t indexid = ctx->read32(redoLogRecord->data() + fieldPos + 4);
            const uint32_t block = ctx->read32(redoLogRecord->data() + fieldPos + 8);
            const auto sdc = static_cast<int32_t>(ctx->read32(redoLogRecord->data() + fieldPos + 12));

            *ctx->dumpStream << "Dump kdilk :" <<
                            " itl=" << std::dec << static_cast<uint64_t>(itl) << ", " <<
                            "kdxlkflg=0x" << std::hex << static_cast<uint64_t>(kdxlkflg) << " " <<
                            "sdc=" << std::dec << sdc << " " <<
                            "indexid=0x" << std::hex << indexid << " " <<
                            "block=0x" << std::setfill('0') << std::setw(8) << std::hex << block << '\n';

            switch (code) {
                case 2:
                case 3:
                    *ctx->dumpStream << "(kdxlpu): purge leaf row\n";
                    break;

                case 4:
                    *ctx->dumpStream << "(kdxlde): mark leaf row deleted\n";
                    break;

                case 5:
                    *ctx->dumpStream << "(kdxlre): restore leaf row (clear leaf delete flags)\n";
                    break;

                case 18:
                    *ctx->dumpStream << "(kdxlup): update keydata in row\n";
                    break;

                default:;
            }

            if (fieldSize >= 24) {
                const uint16_t keySizes = ctx->read16(redoLogRecord->data() + fieldPos + 20);

                if (fieldSize < keySizes * 2 + 24) {
                    ctx->warning(70001, "too short field kdilk key sizes(" + std::to_string(keySizes) + "): " +
                                        std::to_string(fieldSize) + " offset: " + std::to_string(redoLogRecord->dataOffset));
                    return;
                }
                *ctx->dumpStream << "number of keys: " << std::dec << keySizes << " \n";
                *ctx->dumpStream << "key sizes:\n";
                for (uint16_t j = 0; j < keySizes; ++j) {
                    const uint16_t key = ctx->read16(redoLogRecord->data() + fieldPos + 24 + j * 2);
                    *ctx->dumpStream << " " << std::dec << key;
                }
                *ctx->dumpStream << '\n';
            }
        }
    }

    void OpCode0501::rowDeps(const Ctx* ctx, RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize) {
        if (fieldSize < 8)
            ctx->warning(70001, "too short field row dependencies: " + std::to_string(fieldSize) + " offset: " +
                                std::to_string(redoLogRecord->dataOffset));

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            const typeScn dscn = ctx->readScn(redoLogRecord->data() + fieldPos + 0);
            if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                *ctx->dumpStream << "dscn: " << PRINTSCN48(dscn) << '\n';
            else
                *ctx->dumpStream << "dscn: " << PRINTSCN64(dscn) << '\n';
        }
    }

    void OpCode0501::suppLog(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize) {
        typeSize suppLogSize = 0;
        typeField suppLogFieldCnt = 0;
        RedoLogRecord::skipEmptyFields(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize);
        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050117))
            return;

        if (unlikely(fieldSize < 20))
            throw RedoLogException(50061, "too short field supplemental log: " + std::to_string(fieldSize) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        ++suppLogFieldCnt;
        suppLogSize += (fieldSize + 3) & 0xFFFC;
        redoLogRecord->suppLogFb = redoLogRecord->data()[fieldPos + 1];
        redoLogRecord->suppLogCC = ctx->read16(redoLogRecord->data() + fieldPos + 2);
        redoLogRecord->suppLogBefore = ctx->read16(redoLogRecord->data() + fieldPos + 6);
        redoLogRecord->suppLogAfter = ctx->read16(redoLogRecord->data() + fieldPos + 8);

        if (unlikely(ctx->dumpRedoLog >= 2)) {
            const uint8_t suppLogType = redoLogRecord->data()[fieldPos + 0];

            *ctx->dumpStream <<
                            "supp log type: " << std::dec << static_cast<uint64_t>(suppLogType) <<
                            " fb: " << std::dec << static_cast<uint64_t>(redoLogRecord->suppLogFb) <<
                            " cc: " << std::dec << redoLogRecord->suppLogCC <<
                            " before: " << std::dec << redoLogRecord->suppLogBefore <<
                            " after: " << std::dec << redoLogRecord->suppLogAfter << '\n';
        }

        if (fieldSize >= 26) {
            redoLogRecord->suppLogBdba = ctx->read32(redoLogRecord->data() + fieldPos + 20);
            redoLogRecord->suppLogSlot = ctx->read16(redoLogRecord->data() + fieldPos + 24);
            if (unlikely(ctx->dumpRedoLog >= 2)) {
                *ctx->dumpStream <<
                                "supp log bdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->suppLogBdba <<
                                "." << std::hex << redoLogRecord->suppLogSlot << '\n';
            }
        } else {
            redoLogRecord->suppLogBdba = redoLogRecord->bdba;
            redoLogRecord->suppLogSlot = redoLogRecord->slot;
        }

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050118)) {
            ctx->suppLogSize += suppLogSize;
            return;
        }

        redoLogRecord->suppLogNumsDelta = fieldPos;
        const typeCC* colNumsSupp = redoLogRecord->data() + redoLogRecord->suppLogNumsDelta;

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x050119)) {
            ctx->suppLogSize += suppLogSize;
            return;
        }
        ++suppLogFieldCnt;
        suppLogSize += (fieldSize + 3) & 0xFFFC;
        redoLogRecord->suppLogLenDelta = fieldPos;
        redoLogRecord->suppLogRowData = fieldNum + 1;

        for (typeCC i = 0; i < redoLogRecord->suppLogCC; ++i) {
            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05011A);

            ++suppLogFieldCnt;
            suppLogSize += (fieldSize + 3) & 0xFFFC;
            if (unlikely(ctx->dumpRedoLog >= 2))
                dumpCols(ctx, redoLogRecord, redoLogRecord->data() + fieldPos, ctx->read16(colNumsSupp), fieldSize, 0);
            colNumsSupp += 2;
        }

        suppLogSize += ((redoLogRecord->fieldCnt * 2 + 2) & 0xFFFC) - (((redoLogRecord->fieldCnt - suppLogFieldCnt) * 2 + 2) & 0xFFFC);
        ctx->suppLogSize += suppLogSize;
    }
}
