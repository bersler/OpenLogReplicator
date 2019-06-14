/* Oracle Redo OpCode: 11.4
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
#include "OpCode1801.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

    OpCode1801::OpCode1801(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
            OpCode(oracleEnvironment, redoLogRecord),
            validDDL(false),
            type(0) {
    }

    OpCode1801::~OpCode1801() {
    }

    uint16_t OpCode1801::getOpCode(void) {
        return 0x1801;
    }

    void OpCode1801::process() {
        uint32_t fieldPosTmp = redoLogRecord->fieldPos;

        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                redoLogRecord->xid = XID(oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 4),
                        oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 6),
                        oracleEnvironment->read32(redoLogRecord->data + fieldPosTmp + 8));
                type = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 12);
                uint16_t tmp = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 16);
                //uint16_t seq = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 18);
                //uint16_t cnt = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 20);
                if (type == 1 //create table
                        || type == 12 // drop table
                        || type == 15 // alter table
                        || type == 85 // truncate table
                        || type == 86) // truncate partition
                    validDDL = true;

                //temporary object
                if (tmp == 4 || tmp == 5 || tmp == 6 || tmp == 8 || tmp == 9 || tmp == 10) {
                    validDDL = false;
                }
            } else if (i == 12) {
                if (validDDL)
                    redoLogRecord->objn = oracleEnvironment->read32(redoLogRecord->data + fieldPosTmp + 0);
            }

            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
    }

    void OpCode1801::parseDDL() {
        uint32_t fieldPosTmp = redoLogRecord->fieldPos, len;
        uint16_t seq = 0, cnt = 0;

        for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
            if (i == 1) {
                type = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 12);
                seq = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 18);
                cnt = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 20);
                if (oracleEnvironment->trace >= 1) {
                    cout << "SEQ: " << dec << seq << "/" << dec << cnt << endl;
                }
            } else if (i == 8) {
                //DDL text
                if (oracleEnvironment->trace >= 1) {
                    len = ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i];
                    cout << "DDL[" << dec << len << "]: ";
                    for (uint32_t j = 0; j < len; ++j) {
                        cout << *(redoLogRecord->data + fieldPosTmp + j);
                    }
                    cout << endl;
                }
            } else if (i == 9) {
                //owner
                if (oracleEnvironment->trace >= 1) {
                    len = ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i];
                    cout << "OWNER[" << dec << len << "]: ";
                    for (uint32_t j = 0; j < len; ++j) {
                        cout << *(redoLogRecord->data + fieldPosTmp + j);
                    }
                    cout << endl;
                }
            } else if (i == 10) {
                //table
                if (oracleEnvironment->trace >= 1) {
                    len = ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i];
                    cout << "TABLE[" << len << "]: ";
                    for (uint32_t j = 0; j < len; ++j) {
                        cout << *(redoLogRecord->data + fieldPosTmp + j);
                    }
                    cout << endl;
                }
            } else if (i == 12) {
                redoLogRecord->objn = oracleEnvironment->read32(redoLogRecord->data + fieldPosTmp + 0);
                if (oracleEnvironment->trace >= 1) {
                    cout << "OBJN: " << dec << redoLogRecord->objn << endl;
                }
            }

            fieldPosTmp += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }

        if (type == 85) {
            switch (oracleEnvironment->commandBuffer->type) {
            case COMMAND_BUFFER_JSON:
                oracleEnvironment->commandBuffer
                        ->append("{\"operation\":\"truncate\", \"table\": \"")
                        ->append(redoLogRecord->object->owner)
                        ->append('.')
                        ->append(redoLogRecord->object->objectName)
                        ->append("\"}");
                break;

            case COMMAND_BUFFER_REDIS:
                //not yet supported
                break;
            }
       }
    }

}
