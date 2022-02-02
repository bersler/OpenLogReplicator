/* Oracle Redo OpCode: 5.1
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

#include "OpCode0501.h"
#include "OracleAnalyzer.h"
#include "Reader.h"
#include "RedoLogRecord.h"

namespace OpenLogReplicator {
    OpCode0501::OpCode0501(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord) :
        OpCode(oracleAnalyzer, redoLogRecord) {

        uint64_t fieldPos = 0;
        typeFIELD fieldNum = 0;
        uint16_t fieldLength = 0;
        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050101))
            return;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050102))
            return;
        //field: 2
        if (fieldLength < 8) {
            WARNING("too short field ktub: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->obj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->dataObj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4);
    }

    OpCode0501::~OpCode0501() {
    }

    void OpCode0501::process(void) {
        OpCode::process();
        uint64_t fieldPos = 0;
        typeFIELD fieldNum = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050103);
        //field: 1
        ktudb(fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050104))
            return;
        //field: 2
        ktub(fieldPos, fieldLength, true);

        //incomplete data, don't analyze further
        if (redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID) != 0)
            return;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050105))
            return;
        //field: 3
        if (redoLogRecord->opc == 0x0A16 || redoLogRecord->opc == 0x0B01) {
            ktbRedo(fieldPos, fieldLength);
        } else if (redoLogRecord->opc == 0x0E08) {
            kteoputrn(fieldPos, fieldLength);
        }

        uint8_t* colNums = nullptr;
        uint8_t* nulls = nullptr;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050106))
            return;
        //field: 4

        if (redoLogRecord->opc == 0x0B01) {
            kdoOpCode(fieldPos, fieldLength);
            nulls = redoLogRecord->data + redoLogRecord->nullsDelta;

            if (oracleAnalyzer->dumpRedoLog >= 1) {
                if ((redoLogRecord->op & 0x1F) == OP_QMD) {
                    for (uint64_t i = 0; i < redoLogRecord->nrow; ++i)
                        oracleAnalyzer->dumpStream << "slot[" << i << "]: " << std::dec << oracleAnalyzer->read16(redoLogRecord->data+redoLogRecord->slotsDelta + i * 2) << std::endl;
                }
            }

            if ((redoLogRecord->op & 0x1F) == OP_URP) {
                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050107);
                //field: 5
                if (fieldLength > 0 && redoLogRecord->cc > 0) {
                    redoLogRecord->colNumsDelta = fieldPos;
                    colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
                }

                if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
                    oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050108);

                    redoLogRecord->rowData = fieldPos;
                    if (oracleAnalyzer->dumpRedoLog >= 1) {
                        dumpColsVector(redoLogRecord->data + fieldPos, oracleAnalyzer->read16(colNums), fieldLength);
                    }
                } else {
                    redoLogRecord->rowData = fieldNum + 1;
                    uint8_t bits = 1;

                    for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {
                        if ((*nulls & bits) == 0) {
                            oracleAnalyzer->skipEmptyFields(redoLogRecord, fieldNum, fieldPos, fieldLength);
                            if (fieldNum >= redoLogRecord->fieldCnt)
                                return;
                            oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050109);
                        }

                        if (oracleAnalyzer->dumpRedoLog >= 1)
                            dumpCols(redoLogRecord->data + fieldPos, oracleAnalyzer->read16(colNums), fieldLength, *nulls & bits);
                        colNums += 2;
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                    }

                    if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                        oracleAnalyzer->skipEmptyFields(redoLogRecord, fieldNum, fieldPos, fieldLength);
                        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010A);
                        rowDeps(fieldPos, fieldLength);
                    }

                    suppLog(fieldNum, fieldPos, fieldLength);
                }

            } else if ((redoLogRecord->op & 0x1F) == OP_DRP) {
                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010B);
                    rowDeps(fieldPos, fieldLength);
                }

                suppLog(fieldNum, fieldPos, fieldLength);

            } else if ((redoLogRecord->op & 0x1F) == OP_IRP || (redoLogRecord->op & 0x1F) == OP_ORP) {
                if (nulls == nullptr) {
                    WARNING("nulls field is missing" << " offset: " << redoLogRecord->dataOffset);
                    return;
                }

                if (redoLogRecord->cc > 0) {
                    redoLogRecord->rowData = fieldNum + 1;
                    if (fieldNum >= redoLogRecord->fieldCnt)
                        return;
                    oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010C);

                    if (fieldLength == redoLogRecord->sizeDelt && redoLogRecord->cc > 1) {
                        redoLogRecord->compressed = true;
                        if (oracleAnalyzer->dumpRedoLog >= 1)
                            dumpCompressed(redoLogRecord->data + fieldPos, fieldLength);
                    } else {
                        uint8_t bits = 1;
                        for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {
                            if (i > 0) {
                                if (fieldNum >= redoLogRecord->fieldCnt)
                                    return;
                                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010C);
                            }
                            if (fieldLength > 0 && (*nulls & bits) != 0) {
                                WARNING("length: " << std::dec << fieldLength << " for NULL column offset: " << redoLogRecord->dataOffset);
                            }

                            if (oracleAnalyzer->dumpRedoLog >= 1)
                                dumpCols(redoLogRecord->data + fieldPos, i, fieldLength, *nulls & bits);
                            bits <<= 1;
                            if (bits == 0) {
                                bits = 1;
                                ++nulls;
                            }
                        }
                    }
                }

                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010D);
                    rowDeps(fieldPos, fieldLength);
                }

                suppLog(fieldNum, fieldPos, fieldLength);

            } else if ((redoLogRecord->op & 0x1F) == OP_QMI) {
                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010E);
                redoLogRecord->rowLenghsDelta = fieldPos;

                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05010F);
                redoLogRecord->rowData = fieldNum;
                if (oracleAnalyzer->dumpRedoLog >= 1)
                    dumpRows(redoLogRecord->data + fieldPos);

            } else if ((redoLogRecord->op & 0x1F) == OP_LMN) {
                suppLog(fieldNum, fieldPos, fieldLength);

            } else if ((redoLogRecord->op & 0x1F) == OP_LKR) {
                suppLog(fieldNum, fieldPos, fieldLength);

            } else if ((redoLogRecord->op & 0x1F) == OP_CFA) {
                suppLog(fieldNum, fieldPos, fieldLength);
            }
        } else

        if (redoLogRecord->opc == 0x0A16) {
            kdilk(fieldPos, fieldLength);

            //field: 5
            if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050110))
                return;

            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "key :(" << std::dec << fieldLength << "): ";

                if (fieldLength > 20)
                    oracleAnalyzer->dumpStream << std::endl;

                for (uint64_t j = 0; j < fieldLength; ++j) {
                    oracleAnalyzer->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->data[fieldPos + j];
                    if ((j % 25) == 24 && j != (uint64_t)fieldLength - 1)
                        oracleAnalyzer->dumpStream << std::endl;
                }
                oracleAnalyzer->dumpStream << std::endl;
            }

            //field: 6
            if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050111))
                return;

            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "keydata/bitmap: (" << std::dec << fieldLength << "): ";

                if (fieldLength > 20)
                    oracleAnalyzer->dumpStream << std::endl;

                for (uint64_t j = 0; j < fieldLength; ++j) {
                    oracleAnalyzer->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->data[fieldPos + j];
                    if ((j % 25) == 24 && j != (uint64_t)fieldLength - 1)
                        oracleAnalyzer->dumpStream << std::endl;
                }
                oracleAnalyzer->dumpStream << std::endl;
            }

            //field: 7
            if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050111))
                return;

            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "selflock: (" << std::dec << fieldLength << "): ";

                if (fieldLength > 20)
                    oracleAnalyzer->dumpStream << std::endl;

                for (uint64_t j = 0; j < fieldLength; ++j) {
                    oracleAnalyzer->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->data[fieldPos + j];
                    if ((j % 25) == 24 && j != (uint64_t)fieldLength - 1)
                        oracleAnalyzer->dumpStream << std::endl;
                }
                oracleAnalyzer->dumpStream << std::endl;
            }

            //field: 8
            if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050111))
                return;

            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "bitmap: (" << std::dec << fieldLength << "): ";

                if (fieldLength > 20)
                    oracleAnalyzer->dumpStream << std::endl;

                for (uint64_t j = 0; j < fieldLength; ++j) {
                    oracleAnalyzer->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->data[fieldPos + j];
                    if ((j % 25) == 24 && j != (uint64_t)fieldLength - 1)
                        oracleAnalyzer->dumpStream << std::endl;
                }
                oracleAnalyzer->dumpStream << std::endl;
            }
        }
    }

    void OpCode0501::ktudb(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field ktudb: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->xid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8),
                oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 10),
                oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12));

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint16_t siz = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 0);
            uint16_t spc = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 2);
            uint16_t flgKtudb = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 4);
            uint16_t seq = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);
            uint8_t rec = redoLogRecord->data[fieldPos + 18];

            oracleAnalyzer->dumpStream << "ktudb redo:" <<
                    " siz: " << std::dec << siz <<
                    " spc: " << std::dec << spc <<
                    " flg: 0x" << std::setfill('0') << std::setw(4) << std::hex << flgKtudb <<
                    " seq: 0x" << std::setfill('0') << std::setw(4) << seq <<
                    " rec: 0x" << std::setfill('0') << std::setw(2) << (uint64_t)rec << std::endl;
            oracleAnalyzer->dumpStream << "           " <<
                    " xid:  " << PRINTXID(redoLogRecord->xid) << "  " << std::endl;
        }
    }

    void OpCode0501::kteoputrn(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 4) {
            WARNING("too short field kteoputrn: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }
        if (oracleAnalyzer->dumpRedoLog >= 2) {
            typeOBJ newDataObj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
            oracleAnalyzer->dumpStream << "kteoputrn - undo operation for flush for truncate " << std::endl;
            oracleAnalyzer->dumpStream << "newobjd: 0x" << std::hex << newDataObj << " " << std::endl;
        }
    }

    void OpCode0501::kdilk(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field kdilk: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t code = redoLogRecord->data[fieldPos + 0];
            uint8_t itl = redoLogRecord->data[fieldPos + 1];
            uint8_t kdxlkflg = redoLogRecord->data[fieldPos + 2];
            uint32_t indexid = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4);
            uint32_t block = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 8);
            int32_t sdc = (int32_t)oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12);

            oracleAnalyzer->dumpStream << "Dump kdilk :" <<
                    " itl=" << std::dec << (uint64_t)itl << ", " <<
                    "kdxlkflg=0x" << std::hex << (uint64_t)kdxlkflg << " " <<
                    "sdc=" << std::dec << sdc << " " <<
                    "indexid=0x" << std::hex << indexid << " " <<
                    "block=0x" << std::setfill('0') << std::setw(8) << std::hex << block << std::endl;

            switch (code) {
                case 2:
                    oracleAnalyzer->dumpStream << "(kdxlpu): purge leaf row" << std::endl;
                    break;
                case 3:
                    oracleAnalyzer->dumpStream << "(kdxlpu): purge leaf row" << std::endl;
                    break;
                case 4:
                    oracleAnalyzer->dumpStream << "(kdxlde): mark leaf row deleted" << std::endl;
                    break;
                case 5:
                    oracleAnalyzer->dumpStream << "(kdxlre): restore leaf row (clear leaf delete flags)" << std::endl;
                    break;
                case 18:
                    oracleAnalyzer->dumpStream << "(kdxlup): update keydata in row" << std::endl;
                    break;
            }

            if (fieldLength >= 24) {
                uint32_t keySizes = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 20);

                if (fieldLength < keySizes * 2 + 24) {
                    WARNING("too short field kdilk key sizes(" << std::dec << keySizes << "): " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                    return;
                }
                oracleAnalyzer->dumpStream << "number of keys: " << std::dec << keySizes << " " << std::endl;
                oracleAnalyzer->dumpStream << "key sizes:" << std::endl;
                for (uint64_t j = 0; j < keySizes; ++j) {
                    uint16_t key = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24 + j * 2);
                    oracleAnalyzer->dumpStream << " " << std::dec << key;
                }
                oracleAnalyzer->dumpStream << std::endl;
            }
        }
    }

    void OpCode0501::rowDeps(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 8) {
            WARNING("too short row dependencies: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            typeSCN dscn = oracleAnalyzer->readSCN(redoLogRecord->data + fieldPos + 0);
            if (oracleAnalyzer->version < REDO_VERSION_12_2)
                oracleAnalyzer->dumpStream << "dscn: " << PRINTSCN48(dscn) << std::endl;
            else
                oracleAnalyzer->dumpStream << "dscn: " << PRINTSCN64(dscn) << std::endl;
        }
    }

    void OpCode0501::suppLog(typeFIELD& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength) {
        uint64_t suppLogSize = 0;
        uint64_t suppLogFieldCnt = 0;
        oracleAnalyzer->skipEmptyFields(redoLogRecord, fieldNum, fieldPos, fieldLength);
        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050110))
            return;

        if (fieldLength < 20) {
            WARNING("too short supplemental log: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        ++suppLogFieldCnt;
        suppLogSize += (fieldLength + 3) & 0xFFFC;
        redoLogRecord->suppLogType = redoLogRecord->data[fieldPos + 0];
        redoLogRecord->suppLogFb = redoLogRecord->data[fieldPos + 1];
        redoLogRecord->suppLogCC = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 2);
        redoLogRecord->suppLogBefore = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 6);
        redoLogRecord->suppLogAfter = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8);

        if (oracleAnalyzer->dumpRedoLog >= 2) {
            oracleAnalyzer->dumpStream <<
                    "supp log type: " << std::dec << (uint64_t)redoLogRecord->suppLogType <<
                    " fb: " << std::dec << (uint64_t)redoLogRecord->suppLogFb <<
                    " cc: " << std::dec << redoLogRecord->suppLogCC <<
                    " before: " << std::dec << redoLogRecord->suppLogBefore <<
                    " after: " << std::dec << redoLogRecord->suppLogAfter << std::endl;
        }

        if (fieldLength >= 26) {
            redoLogRecord->suppLogBdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 20);
            redoLogRecord->suppLogSlot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
            if (oracleAnalyzer->dumpRedoLog >= 2) {
                oracleAnalyzer->dumpStream <<
                        "supp log bdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->suppLogBdba <<
                        "." << std::hex << redoLogRecord->suppLogSlot << std::endl;
            }
        } else {
            redoLogRecord->suppLogBdba = redoLogRecord->bdba;
            redoLogRecord->suppLogSlot = redoLogRecord->slot;
        }

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050111)) {
            oracleAnalyzer->suppLogSize += suppLogSize;
            return;
        }

        redoLogRecord->suppLogNumsDelta = fieldPos;
        uint8_t* colNumsSupp = redoLogRecord->data + redoLogRecord->suppLogNumsDelta;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050112)) {
            oracleAnalyzer->suppLogSize += suppLogSize;
            return;
        }
        ++suppLogFieldCnt;
        suppLogSize += (fieldLength + 3) & 0xFFFC;
        redoLogRecord->suppLogLenDelta = fieldPos;
        redoLogRecord->suppLogRowData = fieldNum + 1;

        for (uint64_t i = 0; i < redoLogRecord->suppLogCC; ++i) {
            oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x050113);

            ++suppLogFieldCnt;
            suppLogSize += (fieldLength + 3) & 0xFFFC;
            if (oracleAnalyzer->dumpRedoLog >= 2)
                dumpCols(redoLogRecord->data + fieldPos, oracleAnalyzer->read16(colNumsSupp), fieldLength, 0);
            colNumsSupp += 2;
        }

        suppLogSize += (redoLogRecord->fieldCnt * 2 + 2 & 0xFFFC) - ((redoLogRecord->fieldCnt - suppLogFieldCnt) * 2 + 2 & 0xFFFC);
        oracleAnalyzer->suppLogSize += suppLogSize;
    }
}
