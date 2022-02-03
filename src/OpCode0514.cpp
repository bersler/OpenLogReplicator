/* Oracle Redo OpCode: 5.14
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

#include "OpCode0514.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

namespace OpenLogReplicator {
    void OpCode0514::process(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord) {
        OpCode::process(oracleAnalyzer, redoLogRecord);
        uint64_t fieldPos = 0;
        typeFIELD fieldNum = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051401);
        //field: 1
        dumpMsgSessionSerial(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051402))
            return;
        //field: 2
        dumpVal(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength, "transaction name = ");

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051403))
            return;
        //field: 3
        dumpMsgFlags(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051404))
            return;
        //field: 4
        dumpMsgVersion(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051405))
            return;
        //field: 5
        dumpMsgAuditSessionid(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051406))
            return;
        //field: 6

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051407))
            return;
        //field: 7
        dumpVal(oracleAnalyzer, redoLogRecord,  fieldPos, fieldLength, "Client Id = ");

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051408))
            return;
        //field: 8
        dumpVal(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength, "login   username = ");
    }
}
