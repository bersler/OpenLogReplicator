/* Oracle Redo OpCode: 11.11
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

#include <iostream>
#include <iomanip>
#include "OpCode0B0B.h"
#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0B0B::OpCode0B0B(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
            OpCode(oracleEnvironment, redoLogRecord) {
    }

    OpCode0B0B::~OpCode0B0B() {
    }

    void OpCode0B0B::process() {
        OpCode::process();
        uint32_t fieldPos = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktbRedo(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            } else if (i == 2) {
                kdoOpCode(fieldPos, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            } else if (i == 3) {
                redoLogRecord->rowLenghsDelta = fieldPos;
                if (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] < redoLogRecord->nrow * 2) {
                    oracleEnvironment->dumpStream << "field length list length too short: " << dec << ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] << endl;
                    return;
                }
            } else if (i == 4) {
                if (oracleEnvironment->dumpLogFile) {
                    uint32_t pos = 0;
                    char flStr[9] = "--------";

                    for (uint32_t r = 0; r < redoLogRecord->nrow; ++r) {
                        oracleEnvironment->dumpStream << "slot[" << dec << r << "]: " << dec << ((uint16_t*)(redoLogRecord->data + redoLogRecord->slotsDelta))[r] << endl;
                        uint8_t fl = redoLogRecord->data[fieldPos + pos];
                        uint8_t lb = redoLogRecord->data[fieldPos + pos + 1];
                        uint8_t jcc = redoLogRecord->data[fieldPos + pos + 2];

                        if ((fl & 0x01) == 0x01) flStr[7] = 'N'; else flStr[7] = '-'; //last column continues in Next piece
                        if ((fl & 0x02) == 0x02) flStr[6] = 'P'; else flStr[6] = '-'; //first column continues from Previous piece
                        if ((fl & 0x04) == 0x04) flStr[5] = 'L'; else flStr[5] = '-'; //Last data piece
                        if ((fl & 0x08) == 0x08) flStr[4] = 'F'; else flStr[4] = '-'; //First data piece
                        if ((fl & 0x10) == 0x10) flStr[3] = 'D'; else flStr[3] = '-'; //Deleted row
                        if ((fl & 0x20) == 0x20) flStr[2] = 'H'; else flStr[2] = '-'; //Head piece of row
                        if ((fl & 0x40) == 0x40) flStr[1] = 'C'; else flStr[1] = '-'; //Clustered table member
                        if ((fl & 0x80) == 0x80) flStr[0] = 'K'; else flStr[0] = '-'; //cluster Key

                        oracleEnvironment->dumpStream << "tl: " << dec << ((uint16_t*)(redoLogRecord->data + redoLogRecord->rowLenghsDelta))[r] <<
                                " fb: " << flStr <<
                                " lb: 0x" << hex << (uint32_t)lb << " " <<
                                " cc: " << dec << (uint32_t)jcc << endl;
                        pos += 3;

                        for (uint32_t k = 0; k < jcc; ++k) {
                            uint16_t fieldLength = redoLogRecord->data[fieldPos + pos];
                            ++pos;
                            uint8_t isNull = (fieldLength == 0xFF);

                            if (fieldLength == 0xFE) {
                                fieldLength = oracleEnvironment->read16(redoLogRecord->data + fieldPos + pos);
                                pos += 2;
                            }

                            dumpCols(redoLogRecord->data + fieldPos + pos, k, fieldLength, isNull);

                            if (!isNull)
                                pos += fieldLength;
                        }
                    }
                }
            }

            fieldPos += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }
}
