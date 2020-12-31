/* Oracle Redo OpCode: 5.1
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0501::OpCode0501(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyzer, redoLogRecord) {

        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;
        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        if (fieldLength < 8) {
            WARNING("too short field ktub: " << dec << fieldLength);
            return;
        }

        redoLogRecord->obj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->dataObj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4);
    }

    OpCode0501::~OpCode0501() {
    }

    void OpCode0501::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktudb(fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        ktub(fieldPos, fieldLength);

        //incomplete data, don't analyse further
        if (redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID) != 0)
            return;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 3
        if (redoLogRecord->opc == 0x0A16 || redoLogRecord->opc == 0x0B01) {
            ktbRedo(fieldPos, fieldLength);
        } else if (redoLogRecord->opc == 0x0E08) {
            kteoputrn(fieldPos, fieldLength);
        }

        uint8_t *colNums = nullptr, *nulls = nullptr;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 4

        if (redoLogRecord->opc == 0x0B01) {
            kdoOpCode(fieldPos, fieldLength);
            nulls = redoLogRecord->data + redoLogRecord->nullsDelta;

            if (oracleAnalyzer->dumpRedoLog >= 1) {
                if ((redoLogRecord->op & 0x1F) == OP_QMD) {
                    for (uint64_t i = 0; i < redoLogRecord->nrow; ++i)
                        oracleAnalyzer->dumpStream << "slot[" << i << "]: " << dec << oracleAnalyzer->read16(redoLogRecord->data+redoLogRecord->slotsDelta + i * 2) << endl;
                }
            }
        }

        if ((redoLogRecord->op & 0x1F) == OP_URP) {
            oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            //field: 5
            if (fieldLength > 0) {
                redoLogRecord->colNumsDelta = fieldPos;
                colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
            }

            if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);

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
                        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
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
                    oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                    rowDeps(fieldPos, fieldLength);
                }

                suppLog(fieldNum, fieldPos, fieldLength);
            }

        } else if ((redoLogRecord->op & 0x1F) == OP_DRP) {
            if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                rowDeps(fieldPos, fieldLength);
            }

            suppLog(fieldNum, fieldPos, fieldLength);

        } else if ((redoLogRecord->op & 0x1F) == OP_IRP || (redoLogRecord->op & 0x1F) == OP_ORP) {
            if (nulls == nullptr) {
                WARNING("nulls field is missing");
                return;
            }

            redoLogRecord->rowData = fieldNum + 1;
            uint8_t bits = 1;

            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {
                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);

                if (oracleAnalyzer->dumpRedoLog >= 1)
                    dumpCols(redoLogRecord->data + fieldPos, i, fieldLength, *nulls & bits);
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }

            if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                rowDeps(fieldPos, fieldLength);
            }

            suppLog(fieldNum, fieldPos, fieldLength);

        } else if ((redoLogRecord->op & 0x1F) == OP_QMI) {
            oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            redoLogRecord->rowLenghsDelta = fieldPos;

            oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
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
    }

    const char* OpCode0501::getUndoType(void) {
        return "";
    }

    void OpCode0501::ktudb(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field ktudb: " << dec << fieldLength);
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
                    " siz: " << dec << siz <<
                    " spc: " << dec << spc <<
                    " flg: 0x" << setfill('0') << setw(4) << hex << flgKtudb <<
                    " seq: 0x" << setfill('0') << setw(4) << seq <<
                    " rec: 0x" << setfill('0') << setw(2) << (uint64_t)rec << endl;
            oracleAnalyzer->dumpStream << "           " <<
                    " xid:  " << PRINTXID(redoLogRecord->xid) << "  " << endl;
        }
    }

    void OpCode0501::kteoputrn(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 4) {
            WARNING("too short field kteoputrn: " << dec << fieldLength);
            return;
        }
        if (oracleAnalyzer->dumpRedoLog >= 2) {
            typeOBJ newDataObj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
            oracleAnalyzer->dumpStream << "kteoputrn - undo operation for flush for truncate " << endl;
            oracleAnalyzer->dumpStream << "newobjd: 0x" << hex << newDataObj << " " << endl;
        }
    }

    void OpCode0501::rowDeps(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 8) {
            WARNING("too short row dependencies: " << dec << fieldLength);
            return;
        }

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            typeSCN dscn = oracleAnalyzer->readSCN(redoLogRecord->data + fieldPos + 0);
            if (oracleAnalyzer->version < 0x12200)
                oracleAnalyzer->dumpStream << "dscn: " << PRINTSCN48(dscn) << endl;
            else
                oracleAnalyzer->dumpStream << "dscn: " << PRINTSCN64(dscn) << endl;
        }
    }

    void OpCode0501::suppLog(uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        uint64_t suppLogSize = 0;
        uint64_t suppLogFieldCnt = 0;
        oracleAnalyzer->skipEmptyFields(redoLogRecord, fieldNum, fieldPos, fieldLength);
        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;

        if (fieldLength < 20) {
            oracleAnalyzer->dumpStream << "ERROR: too short supplemental log: " << dec << fieldLength << endl;
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
                    "supp log type: " << dec << (uint64_t)redoLogRecord->suppLogType <<
                    " fb: " << dec << (uint64_t)redoLogRecord->suppLogFb <<
                    " cc: " << dec << redoLogRecord->suppLogCC <<
                    " before: " << dec << redoLogRecord->suppLogBefore <<
                    " after: " << dec << redoLogRecord->suppLogAfter << endl;
        }

        if (fieldLength >= 26) {
            redoLogRecord->suppLogBdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 20);
            redoLogRecord->suppLogSlot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
            oracleAnalyzer->dumpStream <<
                    "supp log bdba: 0x" << setfill('0') << setw(8) << hex << redoLogRecord->suppLogBdba <<
                    "." << hex << redoLogRecord->suppLogSlot << endl;
        } else {
            redoLogRecord->suppLogBdba = redoLogRecord->bdba;
            redoLogRecord->suppLogSlot = redoLogRecord->slot;
        }

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength)) {
            oracleAnalyzer->suppLogSize += suppLogSize;
            return;
        }

        redoLogRecord->suppLogNumsDelta = fieldPos;
        uint8_t *colNumsSupp = redoLogRecord->data + redoLogRecord->suppLogNumsDelta;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        ++suppLogFieldCnt;
        suppLogSize += (fieldLength + 3) & 0xFFFC;
        redoLogRecord->suppLogLenDelta = fieldPos;
        redoLogRecord->suppLogRowData = fieldNum + 1;

        for (uint64_t i = 0; i < redoLogRecord->suppLogCC; ++i) {
            oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);

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
