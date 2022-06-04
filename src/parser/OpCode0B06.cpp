/* Oracle Redo OpCode: 11.6
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

#include "OpCode0B06.h"
#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

namespace OpenLogReplicator {
    void OpCode0B06::process(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord) {
        OpCode::process(oracleAnalyzer, redoLogRecord);
        uint64_t fieldPos = 0;
        typeFIELD fieldNum = 0;
        uint16_t fieldLength = 0;

        oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0601);
        //field: 1
        ktbRedo(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0602))
            return;
        //field: 2
        kdoOpCode(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;

        redoLogRecord->rowData = fieldNum + 1;

        if (!oracleAnalyzer->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0603))
            return;
        if (fieldLength == redoLogRecord->sizeDelt && (redoLogRecord->cc > 1 || redoLogRecord->cc == 0)) {
            redoLogRecord->compressed = true;
            if (oracleAnalyzer->dumpRedoLog >= 1)
                dumpCompressed(oracleAnalyzer, redoLogRecord, redoLogRecord->data + fieldPos, fieldLength);
        } else {
            //fields: 3 .. to 3 + cc - 1
            for (uint64_t i = 0; i < (uint64_t)redoLogRecord->cc; ++i) {
                if (fieldLength > 0 && (*nulls & bits) != 0) {
                    WARNING("length: " << std::dec << fieldLength << " for NULL column offset: " << redoLogRecord->dataOffset);
                }

                if (oracleAnalyzer->dumpRedoLog >= 1)
                    dumpCols(oracleAnalyzer, redoLogRecord, redoLogRecord->data + fieldPos, i, fieldLength, *nulls & bits);
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }

                if (fieldNum < redoLogRecord->fieldCnt && i < redoLogRecord->ccData)
                    oracleAnalyzer->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength, 0x0B0604);
                else
                    break;
            }
        }
    }
}
