/* Oracle Redo OpCode: 24.1
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

#include "OpCode1801.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

namespace OpenLogReplicator {
    void OpCode1801::process(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord) {
        bool validDDL = false;
        uint16_t type = 0;

        OpCode::process(oracleAnalyzer, redoLogRecord);
        uint64_t fieldPos = 0;
        typeFIELD fieldNum = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180101);
        //field: 1
        if (fieldLength < 18) {
            WARNING("too short field for 24.1: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }
        redoLogRecord->xid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 4),
                oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 6),
                oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 8));
        type = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 12);
        uint16_t tmp = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);
        //uint16_t seq = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 18);
        //uint16_t cnt = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 20);

        //temporary object
        if (tmp == 4 || tmp == 5 || tmp == 6 || tmp == 8 || tmp == 9 || tmp == 10)
            validDDL = false;
        else
            validDDL = true;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180102))
            return;
        //field: 2

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180103))
            return;
        //field: 3

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180104))
            return;
        //field: 4

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180105))
            return;
        //field: 5

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180106))
            return;
        //field: 6

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180107))
            return;
        //field: 7

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180108))
            return;
        //field: 8

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x180109))
            return;
        //field: 9

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x18010A))
            return;
        //field: 10

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x18010B))
            return;
        //field: 11

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x18010C))
            return;
        //field: 12

        if (validDDL)
            redoLogRecord->obj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
    }
}
