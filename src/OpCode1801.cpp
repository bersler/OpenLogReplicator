/* Oracle Redo OpCode: 18.1
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

#include "OpCode1801.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode1801::OpCode1801(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord),
            validDDL(false),
            type(0) {
    }

    OpCode1801::~OpCode1801() {
    }

    void OpCode1801::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;

        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            if (i == 1) {
                redoLogRecord->xid = XID(oracleAnalyser->read16(redoLogRecord->data + fieldPos + 4),
                        oracleAnalyser->read16(redoLogRecord->data + fieldPos + 6),
                        oracleAnalyser->read32(redoLogRecord->data + fieldPos + 8));
                type = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 12);
                uint16_t tmp = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 16);
                //uint16_t seq = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 18);
                //uint16_t cnt = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 20);
                if (type == 85 // truncate table
                        //|| type == 1 //create table
                        || type == 12 // drop table
                        || type == 15 // alter table
                        || type == 86 // truncate partition
                )
                    validDDL = true;

                //temporary object
                if (tmp == 4 || tmp == 5 || tmp == 6 || tmp == 8 || tmp == 9 || tmp == 10) {
                    validDDL = false;
                }
            } else if (i == 12) {
                if (validDDL && redoLogRecord->scn > oracleAnalyser->databaseScn) {
                    redoLogRecord->objn = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
                    if (type == 12 || type == 15) {
                        OracleObject *obj = oracleAnalyser->checkDict(redoLogRecord->objn, 0);
                        if (obj != nullptr) {
                            if (oracleAnalyser->trace >= TRACE_WARN) {
                                cerr << "WARNING: table " << obj->owner << "." << obj->objectName << " altered, removing from replication" << endl;
                            }
                            if ((oracleAnalyser->disableChecks & DISABLE_CHECK_ALTER_TABLE) == 0) {
                                obj->altered = true;
                            }
                        }
                    }
                }
            }

            fieldPos += (oracleAnalyser->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2) + 3) & 0xFFFC;
        }
    }
}
