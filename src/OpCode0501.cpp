/* Oracle Redo OpCode: 5.1
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
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

        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt && i <= 2; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 2) {
                if (fieldLength < 8) {
                    oracleAnalyser->dumpStream << "ERROR: too short field ktub: " << dec << fieldLength << endl;
                    return;
                }

                redoLogRecord->objn = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
                redoLogRecord->objd = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 4);
            }
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }

    OpCode0501::~OpCode0501() {
    }

    void OpCode0501::process() {
        OpCode::process();
        uint8_t *colNums = nullptr, *nulls = nullptr, bits = 1;
        uint64_t fieldPos = redoLogRecord->fieldPos;
        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
            if (i == 1) {
                ktudb(fieldPos, fieldLength);
            } else if (i == 2) {
                ktub(fieldPos, fieldLength);
            } else if (i > 2 && (redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID)) != 0) {
                //incomplete data
            } else if (i == 3) {
                if (redoLogRecord->opc == 0x0A16 || redoLogRecord->opc == 0x0B01) {
                    ktbRedo(fieldPos, fieldLength);
                } else if (redoLogRecord->opc == 0x0E08) {
                    kteoputrn(fieldPos, fieldLength);
                }
            } else if (i == 4) {
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

            } else if ((redoLogRecord->op & 0x1F) == OP_URP) {
                if (i == 5) {
                    redoLogRecord->colNumsDelta = fieldPos;
                    colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
                } else if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0) {
                    if (i == 6) {
                        if (oracleAnalyser->dumpRedoLog >= 1) {
                            dumpColsVector(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength);
                        }
                    } else {
                        if (i == 7) {
                            suppLog(fieldPos, fieldLength);
                        }
                    }

                } else {
                    if (i > 5 && i <= 5 + (uint64_t)redoLogRecord->cc) {
                        if (oracleAnalyser->dumpRedoLog >= 1) {
                            dumpCols(redoLogRecord->data + fieldPos, oracleAnalyser->read16(colNums), fieldLength, *nulls & bits);
                        }
                        colNums += 2;
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                    } else {
                        if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                            if (i == 6  + (uint64_t)redoLogRecord->cc) {
                                rowDeps(fieldPos, fieldLength);
                            } else {
                                suppLog(fieldPos, fieldLength);
                            }
                        } else {
                            if (i == 6  + (uint64_t)redoLogRecord->cc) {
                                suppLog(fieldPos, fieldLength);
                            }
                        }
                    }
                }
            } else if ((redoLogRecord->op & 0x1F) == OP_DRP) {
                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    if (i == 5)
                        rowDeps(fieldPos, fieldLength);
                    else if (i == 6)
                        suppLog(fieldPos, fieldLength);
                } else {
                    if (i == 5)
                        suppLog(fieldPos, fieldLength);
                }
            } else if ((redoLogRecord->op & 0x1F) == OP_IRP || (redoLogRecord->op & 0x1F) == OP_ORP) {
                if (i > 4 && i <= 4 + (uint64_t)redoLogRecord->cc) {
                    if (nulls == nullptr) {
                        cerr << "ERROR: nulls = null" << endl;
                        return;
                    }
                    if (oracleAnalyser->dumpRedoLog >= 1) {
                        dumpCols(redoLogRecord->data + fieldPos, i - 5, fieldLength, *nulls & bits);
                    }
                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                } else {
                    if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                        if (i == 5 + (uint64_t)redoLogRecord->cc)
                            rowDeps(fieldPos, fieldLength);
                        else if (i == 6 + (uint64_t)redoLogRecord->cc)
                            suppLog(fieldPos, fieldLength);
                    } else {
                        if (i == 5 + (uint64_t)redoLogRecord->cc)
                            suppLog(fieldPos, fieldLength);
                    }
                }

            } else if ((redoLogRecord->op & 0x1F) == OP_QMI) {
                if (i == 5) {
                    redoLogRecord->rowLenghsDelta = fieldPos;
                } else if (i == 6) {
                    if (oracleAnalyser->dumpRedoLog >= 1) {
                        dumpRows(redoLogRecord->data + fieldPos);
                    }
                }

            } else if ((redoLogRecord->op & 0x1F) == OP_CFA) {
                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    if (i == 5)
                        rowDeps(fieldPos, fieldLength);
                    else if (i == 6)
                        suppLog(fieldPos, fieldLength);
                } else {
                    if (i == 5)
                        suppLog(fieldPos, fieldLength);
                }
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }

    const char* OpCode0501::getUndoType() {
        return "";
    }

    void OpCode0501::ktudb(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            oracleAnalyser->dumpStream << "too short field ktudb: " << dec << fieldLength << endl;
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
            oracleAnalyser->dumpStream << "ERROR: too short field kteoputrn: " << dec << fieldLength << endl;
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
            oracleAnalyser->dumpStream << "ERROR: too short row dependencies: " << dec << fieldLength << endl;
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

    void OpCode0501::suppLog(uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            oracleAnalyser->dumpStream << "ERROR: too short supplemental log: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->suppLogType = redoLogRecord->data[fieldPos + 0];
        redoLogRecord->suppLogFb = redoLogRecord->data[fieldPos + 1];
        redoLogRecord->suppLogCC = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 2);
        redoLogRecord->suppLogBefore = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 6);
        redoLogRecord->suppLogAfter = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 8);

        if (oracleAnalyser->dumpRedoLog >= 2) {
            oracleAnalyser->dumpStream <<
                    "supp log type: " << dec << (uint64_t)redoLogRecord->suppLogType <<
                    " fb: " << dec << (uint64_t)redoLogRecord->suppLogFb <<
                    " cc: " << dec << redoLogRecord->suppLogCC <<
                    " before: " << dec << redoLogRecord->suppLogBefore <<
                    " after: " << dec << redoLogRecord->suppLogAfter << endl;
        }

        if (fieldLength >= 26) {
            redoLogRecord->suppLogBdba = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 20);
            redoLogRecord->suppLogSlot = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 24);
            oracleAnalyser->dumpStream <<
                    "supp log bdba: 0x" << setfill('0') << setw(8) << hex << redoLogRecord->suppLogBdba <<
                    "." << hex << redoLogRecord->suppLogSlot << endl;
        }
    }
}
