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

#include <iomanip>
#include <iostream>

#include "OpCode0501.h"
#include "OracleAnalyser.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0501::OpCode0501(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord) {

        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;
        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        if (fieldLength < 8) {
            cerr << "ERROR: too short field ktub: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->objn = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->objd = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 4);
    }

    OpCode0501::~OpCode0501() {
    }

    void OpCode0501::process(void) {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        ktudb(fieldPos, fieldLength);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        ktub(fieldPos, fieldLength);

        //incomplete data, don't analyse further
        if (redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID) != 0)
            return;

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 3
        if (redoLogRecord->opc == 0x0A16 || redoLogRecord->opc == 0x0B01) {
            ktbRedo(fieldPos, fieldLength);
        } else if (redoLogRecord->opc == 0x0E08) {
            kteoputrn(fieldPos, fieldLength);
        }

        uint8_t *colNums = nullptr, *nulls = nullptr;

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 4

        if (redoLogRecord->opc == 0x0B01) {
            kdoOpCode(fieldPos, fieldLength);
            nulls = redoLogRecord->data + redoLogRecord->nullsDelta;

            if (oracleAnalyser->dumpRedoLog >= 1) {
                if ((redoLogRecord->op & 0x1F) == OP_QMD) {
                    for (uint64_t i = 0; i < redoLogRecord->nrow; ++i)
                        oracleAnalyser->dumpStream << "slot[" << i << "]: " << dec << oracleAnalyser->read16(redoLogRecord->data+redoLogRecord->slotsDelta + i * 2) << endl;
                }
            }
        }

        if ((redoLogRecord->op & 0x1F) == OP_URP) {
            oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            //field: 5
            if (fieldLength > 0) {
                redoLogRecord->colNumsDelta = fieldPos;
                colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
            }

            if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
                cerr << "DOM fieldNum" << dec << fieldNum << endl;
                oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);

                redoLogRecord->rowData = fieldPos;
                if (oracleAnalyser->dumpRedoLog >= 1) {
                    dumpColsVector(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength);
                }
            } else {
                redoLogRecord->rowData = fieldNum + 1;
                uint8_t bits = 1;

                for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {
                    if ((*nulls & bits) == 0) {
                        oracleAnalyser->skipEmptyFields(redoLogRecord, fieldNum, fieldPos, fieldLength);
                        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                    }

                    if (oracleAnalyser->dumpRedoLog >= 1)
                        dumpCols(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength, *nulls & bits);
                    colNums += 2;
                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }

                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    oracleAnalyser->skipEmptyFields(redoLogRecord, fieldNum, fieldPos, fieldLength);
                    oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                    rowDeps(fieldPos, fieldLength);
                }

                suppLog(fieldNum, fieldPos, fieldLength);
            }

        } else if ((redoLogRecord->op & 0x1F) == OP_DRP) {
            if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                rowDeps(fieldPos, fieldLength);
            }

            suppLog(fieldNum, fieldPos, fieldLength);

        } else if ((redoLogRecord->op & 0x1F) == OP_IRP || (redoLogRecord->op & 0x1F) == OP_ORP) {
            if (nulls == nullptr) {
                cerr << "ERROR: nulls = null" << endl;
                return;
            }

            redoLogRecord->rowData = fieldNum + 1;
            uint8_t bits = 1;

            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {
                oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);

                if (oracleAnalyser->dumpRedoLog >= 1)
                    dumpCols(redoLogRecord->data + fieldPos, i, fieldLength, *nulls & bits);
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }

            if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
                rowDeps(fieldPos, fieldLength);
            }

            suppLog(fieldNum, fieldPos, fieldLength);

        } else if ((redoLogRecord->op & 0x1F) == OP_QMI) {
            oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            redoLogRecord->rowLenghsDelta = fieldPos;

            oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
            redoLogRecord->rowData = fieldNum;
            if (oracleAnalyser->dumpRedoLog >= 1)
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
            cerr << "ERROR: too short field ktudb: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(oracleAnalyser->read16(redoLogRecord->data + fieldPos + 8),
                oracleAnalyser->read16(redoLogRecord->data + fieldPos + 10),
                oracleAnalyser->read32(redoLogRecord->data + fieldPos + 12));

        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint16_t siz = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 0);
            uint16_t spc = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 2);
            uint16_t flgKtudb = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 4);
            uint16_t seq = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 16);
            uint8_t rec = redoLogRecord->data[fieldPos + 18];

            oracleAnalyser->dumpStream << "ktudb redo:" <<
                    " siz: " << dec << siz <<
                    " spc: " << dec << spc <<
                    " flg: 0x" << setfill('0') << setw(4) << hex << flgKtudb <<
                    " seq: 0x" << setfill('0') << setw(4) << seq <<
                    " rec: 0x" << setfill('0') << setw(2) << (uint64_t)rec << endl;
            oracleAnalyser->dumpStream << "           " <<
                    " xid:  " << PRINTXID(redoLogRecord->xid) << "  " << endl;
        }
    }

    void OpCode0501::kteoputrn(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 4) {
            cerr << "ERROR: too short field kteoputrn: " << dec << fieldLength << endl;
            return;
        }
        if (oracleAnalyser->dumpRedoLog >= 2) {
            typeobj newobjd = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
            oracleAnalyser->dumpStream << "kteoputrn - undo operation for flush for truncate " << endl;
            oracleAnalyser->dumpStream << "newobjd: 0x" << hex << newobjd << " " << endl;
        }
    }

    void OpCode0501::rowDeps(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 8) {
            cerr << "ERROR: too short row dependencies: " << dec << fieldLength << endl;
            return;
        }

        if (oracleAnalyser->dumpRedoLog >= 1) {
            typescn dscn = oracleAnalyser->readSCN(redoLogRecord->data + fieldPos + 0);
            if (oracleAnalyser->version < 0x12200)
                oracleAnalyser->dumpStream << "dscn: " << PRINTSCN48(dscn) << endl;
            else
                oracleAnalyser->dumpStream << "dscn: " << PRINTSCN64(dscn) << endl;
        }
    }
}
