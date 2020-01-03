/* Oracle Redo OpCode: 5.1
   Copyright (C) 2018-2019 Adam Leszczynski.

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

#include <iostream>
#include <iomanip>
#include "OpCode0501.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0501::OpCode0501(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
            OpCode(oracleEnvironment, redoLogRecord) {

        uint32_t fieldPos = redoLogRecord->fieldPos, fieldLength;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum && i <= 2; ++i) {
            fieldLength = ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i];
            if (i == 2) {
                if (fieldLength < 8) {
                    oracleEnvironment->dumpStream << "ERROR: too short field ktub: " << dec << fieldLength << endl;
                    return;
                }

                redoLogRecord->objn = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 0);
                redoLogRecord->objd = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 4);
            }
            fieldPos += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }

    OpCode0501::~OpCode0501() {
    }

    void OpCode0501::process() {
        OpCode::process();
        uint16_t *colnums;
        uint8_t *nulls = nullptr, bits = 1;
        uint32_t fieldPos = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktudb(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);

            } else if (i == 2) {
                ktub(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                if ((redoLogRecord->flg & 0x8) != 0)
                    ktubl(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                else
                    ktubu(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);

            } else if (i == 3) {
                if (redoLogRecord->opc == 0x0A16 || redoLogRecord->opc == 0x0B01) {
                    ktbRedo(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                }

            } else if (i == 4) {
                if (redoLogRecord->opc == 0x0B01) {
                    kdoOpCode(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                    nulls = redoLogRecord->data + redoLogRecord->nullsDelta;

                    //Quick Multi-row Delete
                    if (oracleEnvironment->dumpLogFile && (redoLogRecord->op & 0x1F) == 0x0C) {
                        for (uint32_t i = 0; i < redoLogRecord->nrow; ++i)
                            oracleEnvironment->dumpStream << "slot[" << i << "]: " << dec << ((uint16_t*)(redoLogRecord->data+redoLogRecord->slotsDelta))[i] << endl;
                    }
                }

            //Update Row Piece
            } else if ((redoLogRecord->op & 0x1F) == 0x05) {
                if (i == 5)
                    colnums = (uint16_t*)(redoLogRecord->data + fieldPos);
                else if (i == 6 && (redoLogRecord->xtype & 0x80) != 0) {
                    if (oracleEnvironment->dumpLogFile)
                        dumpColsVector(redoLogRecord->data + fieldPos, colnums[0], ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                } else if (i > 5 && i <= 5 + (uint32_t)redoLogRecord->cc && (redoLogRecord->xtype & 0x80) == 0) {
                    if (oracleEnvironment->dumpLogFile) {
                        dumpCols(redoLogRecord->data + fieldPos, *colnums, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i], *nulls & bits);
                        ++colnums;
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                    }
                }

            //Insert Row Piece / Overwrite Row Piece
            } else if ((redoLogRecord->op & 0x1F) == 0x02 || (redoLogRecord->op & 0x1F) == 0x06) {
                if (i > 4 && i <= 4 + (uint32_t)redoLogRecord->cc) {
                    if (nulls == nullptr) {
                        cout << "nulls = null" << endl;
                        exit(1);
                    }
                    if (oracleEnvironment->dumpLogFile) {
                        dumpCols(redoLogRecord->data + fieldPos, i - 5, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i], *nulls & bits);
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                    }
                }
            }

            fieldPos += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }

    bool OpCode0501::isKdoUndo() {
        return true;
    }

    const char* OpCode0501::getUndoType() {
        return "";
    }

    void OpCode0501::ktudb(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 20) {
            oracleEnvironment->dumpStream << "too short field ktudb: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->xid = XID(oracleEnvironment->read16(redoLogRecord->data + fieldPos + 8),
                oracleEnvironment->read16(redoLogRecord->data + fieldPos + 10),
                oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12));

        if (oracleEnvironment->dumpLogFile) {
            uint16_t siz = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 0);
            uint16_t spc = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 2);
            uint16_t flg = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 4);
            uint16_t seq = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 16);
            uint8_t rec = redoLogRecord->data[fieldPos + 18];

            oracleEnvironment->dumpStream << "ktudb redo:" <<
                    " siz: " << dec << siz <<
                    " spc: " << dec << spc <<
                    " flg: 0x" << setfill('0') << setw(4) << hex << flg <<
                    " seq: 0x" << setfill('0') << setw(4) << seq <<
                    " rec: 0x" << setfill('0') << setw(2) << (uint32_t)rec << endl;
            oracleEnvironment->dumpStream << "           " <<
                    " xid:  " << PRINTXID(redoLogRecord->xid) << "  " << endl;
        }
    }

    void OpCode0501::ktubl(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 76) {
            oracleEnvironment->dumpStream << "too short field ktubl.5.7: " << dec << fieldLength << endl;
            return;
        }

        if (oracleEnvironment->dumpLogFile) {
            uint32_t x1 = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 24);
            typeuba prevCtlUba = oracleEnvironment->read64(redoLogRecord->data + fieldPos + 28);
            typescn prevCtlMaxCmtScn = oracleEnvironment->readSCN(redoLogRecord->data + fieldPos + 36);
            typescn prevTxCmtScn = oracleEnvironment->readSCN(redoLogRecord->data + fieldPos + 44);
            typescn txStartScn = oracleEnvironment->readSCN(redoLogRecord->data + fieldPos + 56);
            uint32_t prevBrb = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 64);
            uint32_t logonUser = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 72);

            string lastBufferSplit = "No "; //FIXME
            string tempObject = "No "; //FIXME
            string tablespaceUndo = "No "; //FIXME
            uint32_t prevBcl = 0; //FIXME
            uint32_t buExtIdx = 0; // FIXME
            uint32_t flg2 = 0; //FIXME

            oracleEnvironment->dumpStream << "ktubl redo:" <<
                    " slt: " << dec << (uint32_t)redoLogRecord->slt <<
                    " rci: " << dec << (uint32_t)redoLogRecord->rci <<
                    " opc: " << dec << (uint32_t)(redoLogRecord->opc >> 8) << "." << (uint32_t)(redoLogRecord->opc & 0xFF) <<
                    " [objn: " << redoLogRecord->objn << " objd: " << redoLogRecord->objd << " tsn: " << redoLogRecord->tsn << "]" << endl;
            oracleEnvironment->dumpStream << "Undo type:  Regular undo        Begin trans    Last buffer split:  " << lastBufferSplit << endl;
            oracleEnvironment->dumpStream << "Temp Object:  " << tempObject << endl;
            oracleEnvironment->dumpStream << "Tablespace Undo:  " << tablespaceUndo << endl;
            oracleEnvironment->dumpStream << "             0x" << setfill('0') << setw(8) << hex << x1 << " " <<
                    " prev ctl uba: " << PRINTUBA(prevCtlUba) << " " << endl;
            oracleEnvironment->dumpStream << "prev ctl max cmt scn:  " << PRINTSCN(prevCtlMaxCmtScn) << " " <<
                    " prev tx cmt scn:  " << PRINTSCN(prevTxCmtScn) << " " << endl;
            oracleEnvironment->dumpStream << "txn start scn:  " << PRINTSCN(txStartScn) << " " <<
                    " logon user: " << dec << logonUser << " " <<
                    " prev brb: " << prevBrb << " " <<
                    " prev bcl: " << dec << prevBcl <<
                    " BuExt idx: " << dec << buExtIdx <<
                    " flg2: " << dec << flg2 << endl;
        }
    }
}
