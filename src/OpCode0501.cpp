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
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

    OpCode0501::OpCode0501(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
        OpCode(oracleEnvironment, redoLogRecord) {
    }

    OpCode0501::~OpCode0501() {
    }

    uint16_t OpCode0501::getOpCode(void) {
        return 0x0501;
    }

    void OpCode0501::process() {
        uint16_t *colnums;
        uint8_t *nullstmp = nullptr, bits = 1;
        uint32_t fieldPosTmp = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktudb(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);

            } else if (i == 2) {
                ktub(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                if ((redoLogRecord->flg & 0x8) != 0)
                    ktubl(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                else //if (redoLogRecord->opc == 0x0B01)
                    ktubu(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);

            } else if (i == 3) {
                if (redoLogRecord->opc == 0x0B01) {
                    ktbRedo(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                }

            } else if (i == 4) {
                if (redoLogRecord->opc == 0x0B01) {
                    kdoOpCode(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                    nullstmp = redoLogRecord->data + redoLogRecord->nullsDelta;
                    //Quick Multi-row Delete
                    if (oracleEnvironment->dumpLogFile && (redoLogRecord->op & 0x1F) == 0x0C) {
                        for (uint32_t i = 0; i < redoLogRecord->nrow; ++i)
                            oracleEnvironment->dumpStream << "slot[" << i << "]: " << dec << ((uint16_t*)(redoLogRecord->data+redoLogRecord->slotsDelta))[i] << endl;
                    }
                }

            //Update Row Piece
            } else if ((redoLogRecord->op & 0x1F) == 0x05) {
                if (i == 5)
                    colnums = (uint16_t*)(redoLogRecord->data + fieldPosTmp);
                else if (i > 5 && i <= 5 + (uint32_t)redoLogRecord->cc) {
                    if (oracleEnvironment->dumpLogFile) {
                        dumpCols(redoLogRecord->data + fieldPosTmp, *colnums, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i], *nullstmp & bits);
                        ++colnums;
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nullstmp;
                        }
                    }
                }

            //Insert Row Piece / Overwrite Row Piece
            } else if ((redoLogRecord->op & 0x1F) == 0x02 || (redoLogRecord->op & 0x1F) == 0x06) {
                if (i > 4 && i <= 4 + (uint32_t)redoLogRecord->cc) {
                    if (nullstmp == nullptr) {
                        cout << "nullstmp = null" << endl;
                        exit(1);
                    }
                    if (oracleEnvironment->dumpLogFile) {
                        dumpCols(redoLogRecord->data + fieldPosTmp, i - 5, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i], *nullstmp & bits);
                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nullstmp;
                        }
                    }
                }
            }

            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
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
            typescn prevCtlMaxCmtScn = oracleEnvironment->read48(redoLogRecord->data + fieldPos + 36);
            typescn prevTxCmtScn = oracleEnvironment->read48(redoLogRecord->data + fieldPos + 44);
            typescn txStartScn = oracleEnvironment->read48(redoLogRecord->data + fieldPos + 56);
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

    void OpCode0501::parseDelete(uint32_t afn) {
        uint32_t fieldPosTmp = redoLogRecord->fieldPos, fieldPosTmp2;
        uint8_t *nullstmp = redoLogRecord->data + redoLogRecord->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 4; ++i)
            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        fieldPosTmp2 = fieldPosTmp;

        switch (oracleEnvironment->commandBuffer->type) {
        case COMMAND_BUFFER_JSON:
            oracleEnvironment->commandBuffer
                    ->append("{\"operation\":\"delete\", \"table\": \"")
                    ->append(redoLogRecord->object->owner)
                    ->append('.')
                    ->append(redoLogRecord->object->objectName)
                    ->append("\", \"rowid\": \"")
                    ->appendRowid(redoLogRecord->objd, afn, redoLogRecord->bdba & 0xFFFF, redoLogRecord->slot)
                    ->append("\", \"before\": {");
            break;

        case COMMAND_BUFFER_REDIS:
            oracleEnvironment->commandBuffer
                    ->append(redoLogRecord->object->owner)
                    ->append('.')
                    ->append(redoLogRecord->object->objectName)
                    ->append('.');

            for (uint32_t i = 0; i < redoLogRecord->object->columns.size(); ++i) {
                //is PK or table has no PK
                if (redoLogRecord->object->columns[i]->numPk > 0 || redoLogRecord->object->totalPk == 0) {
                    if (prevValue) {
                        oracleEnvironment->commandBuffer
                                ->append('.');
                    } else
                        prevValue = true;

                    if ((*nullstmp & bits) != 0 || ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 5] == 0 || i >= redoLogRecord->cc) {
                        oracleEnvironment->commandBuffer
                            ->append("NULL");
                    } else {
                        oracleEnvironment->commandBuffer
                                ->append('"');

                        appendValue(redoLogRecord->object->columns[i]->typeNo, fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 5]);

                        oracleEnvironment->commandBuffer
                                ->append('"');
                    }
                }

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nullstmp;
                }
                fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 5] + 3) & 0xFFFC;
            }
            fieldPosTmp = fieldPosTmp2;
            nullstmp = redoLogRecord->data + redoLogRecord->nullsDelta;

            oracleEnvironment->commandBuffer
                    ->append(0);
            break;
        }
        prevValue = false;

        for (uint32_t i = 0; i < redoLogRecord->object->columns.size(); ++i) {
            if ((*nullstmp & bits) != 0 || ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 5] == 0 || i >= redoLogRecord->cc) {
                switch (oracleEnvironment->commandBuffer->type) {
                case COMMAND_BUFFER_JSON:
                    break;

                case COMMAND_BUFFER_REDIS:
                    if (prevValue) {
                        oracleEnvironment->commandBuffer
                                ->append(',');
                    } else
                        prevValue = true;

                    oracleEnvironment->commandBuffer
                            ->append("NULL");
                    break;
                }

            } else {
                if (prevValue) {
                    oracleEnvironment->commandBuffer
                            ->append(',');
                } else
                    prevValue = true;

                switch (oracleEnvironment->commandBuffer->type) {
                case COMMAND_BUFFER_JSON:
                    oracleEnvironment->commandBuffer
                            ->append('"')
                            ->append(redoLogRecord->object->columns[i]->columnName)
                            ->append("\": \"");
                    break;

                case COMMAND_BUFFER_REDIS:
                    oracleEnvironment->commandBuffer
                            ->append('"');
                    break;
                }

                appendValue(redoLogRecord->object->columns[i]->typeNo, fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 5]);

                switch (oracleEnvironment->commandBuffer->type) {
                case COMMAND_BUFFER_JSON:
                    oracleEnvironment->commandBuffer
                            ->append('"');
                    break;

                case COMMAND_BUFFER_REDIS:
                    oracleEnvironment->commandBuffer
                            ->append('"');
                    break;
                }
            }

            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nullstmp;
            }
            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 3] + 3) & 0xFFFC;
        }

        switch (oracleEnvironment->commandBuffer->type) {
        case COMMAND_BUFFER_JSON:
            oracleEnvironment->commandBuffer
                    ->append("}}");
            break;

        case COMMAND_BUFFER_REDIS:
            oracleEnvironment->commandBuffer
                    ->append(0);
            break;
        }
    }
}
