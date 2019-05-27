/* Oracle Redo OpCode: 11.2
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
#include "OpCode0B02.h"
#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

    OpCode0B02::OpCode0B02(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
            OpCode(oracleEnvironment, redoLogRecord) {
    }

    OpCode0B02::~OpCode0B02() {
    }

    uint16_t OpCode0B02::getOpCode(void) {
        return 0x0B02;
    }

    void OpCode0B02::process() {
        uint8_t *nullstmp, bits = 1;
        uint32_t fieldPosTmp = redoLogRecord->fieldPos;
        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                ktbRedo(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
            } else if (i == 2) {
                kdoOpCode(fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]);
                redoLogRecord->nullsDelta = fieldPosTmp + 45;
                nullstmp = redoLogRecord->data + redoLogRecord->nullsDelta;
            } else if (i > 2 && i <= 2 + (uint32_t)redoLogRecord->cc) {
                if (oracleEnvironment->dumpLogFile) {
                    dumpCols(redoLogRecord->data + fieldPosTmp, i - 3, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i], *nullstmp & bits);
                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nullstmp;
                    }
                }
            }

            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }

    void OpCode0B02::parseInsert(uint32_t objd) {
        uint32_t fieldPosTmp = redoLogRecord->fieldPos, fieldPosTmp2;
        uint8_t *nullstmp = redoLogRecord->data + redoLogRecord->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 2; ++i)
            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        fieldPosTmp2 = fieldPosTmp;

        switch (oracleEnvironment->commandBuffer->type) {
        case COMMAND_BUFFER_JSON:
            oracleEnvironment->commandBuffer
                    ->append("{\"operation\":\"insert\", \"table\": \"")
                    ->append(redoLogRecord->object->owner)
                    ->append('.')
                    ->append(redoLogRecord->object->objectName)
                    ->append("\", \"rowid\": \"")
                    ->appendRowid(objd, redoLogRecord->afn, redoLogRecord->bdba & 0xFFFF, redoLogRecord->slot)
                    ->append("\", \"after\": {");
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

                    if ((*nullstmp & bits) != 0 || ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 3] == 0 || i >= redoLogRecord->cc) {
                        oracleEnvironment->commandBuffer
                            ->append("NULL");
                    } else {
                        oracleEnvironment->commandBuffer
                                ->append('"');

                        appendValue(redoLogRecord->object->columns[i]->typeNo, fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 3]);

                        oracleEnvironment->commandBuffer
                                ->append('"');
                    }
                }

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nullstmp;
                }
                fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 3] + 3) & 0xFFFC;
            }
            fieldPosTmp = fieldPosTmp2;
            nullstmp = redoLogRecord->data + redoLogRecord->nullsDelta;

            oracleEnvironment->commandBuffer
                    ->append(0);
            break;
        }
        prevValue = false;

        for (uint32_t i = 0; i < redoLogRecord->object->columns.size(); ++i) {
            if ((*nullstmp & bits) != 0 || ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 3] == 0 || i >= redoLogRecord->cc) {
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

                appendValue(redoLogRecord->object->columns[i]->typeNo, fieldPosTmp, ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i + 3]);

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
