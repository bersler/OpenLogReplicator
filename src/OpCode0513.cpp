/* Oracle Redo OpCode: 5.13
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
#include "OpCode0513.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleReader.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0513::OpCode0513(OracleReader *oracleReader, RedoLogRecord *redoLogRecord) :
            OpCode(oracleReader, redoLogRecord) {
    }

    OpCode0513::~OpCode0513() {
    }

    void OpCode0513::process() {
        OpCode::process();
        uint64_t fieldPos = redoLogRecord->fieldPos;

        for (uint64_t i = 1; i <= redoLogRecord->fieldCnt; ++i) {
            uint16_t fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);

            if (i == 1) dumpMsgSessionSerial(fieldPos, fieldLength);
            else
            if (i == 2) dumpVal(fieldPos, fieldLength, "current username = ");
            else
            if (i == 3) dumpVal(fieldPos, fieldLength, "login   username = ");
            else
            if (i == 4) dumpVal(fieldPos, fieldLength, "client info      = ");
            else
            if (i == 5) dumpVal(fieldPos, fieldLength, "OS username      = ");
            else
            if (i == 6) dumpVal(fieldPos, fieldLength, "Machine name     = ");
            else
            if (i == 7) dumpVal(fieldPos, fieldLength, "OS terminal      = ");
            else
            if (i == 8) dumpVal(fieldPos, fieldLength, "OS process id    = ");
            else
            if (i == 9) dumpVal(fieldPos, fieldLength, "OS program name  = ");
            else
            if (i == 10) dumpVal(fieldPos, fieldLength, "transaction name = ");
            else
            if (i == 11) dumpMsgFlags(fieldPos, fieldLength);
            else
            if (i == 12) dumpMsgVersion(fieldPos, fieldLength);
            else
            if (i == 13) dumpMsgAuditSessionid(fieldPos, fieldLength);
            else
            if (i == 14) dumpVal(fieldPos, fieldLength, "Client Id  = ");

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
    }

    void OpCode0513::dumpMsgFlags(uint64_t fieldPos, uint64_t fieldLength) {
        uint16_t flags = oracleReader->read16(redoLogRecord->data + fieldPos + 0);
        if ((flags & 0x0001) != 0) oracleReader->dumpStream << "DDL transaction" << endl;
        if ((flags & 0x0002) != 0) oracleReader->dumpStream << "Space Management transaction" << endl;
        if ((flags & 0x0004) != 0) oracleReader->dumpStream << "Recursive transaction" << endl;
        if ((flags & 0x0008) != 0) oracleReader->dumpStream << "Logmnr Internal transaction" << endl;
        if ((flags & 0x0010) != 0) oracleReader->dumpStream << "DB Open in Migrate Mode" << endl;
        if ((flags & 0x0020) != 0) oracleReader->dumpStream << "LSBY ignore" << endl;
        if ((flags & 0x0040) != 0) oracleReader->dumpStream << "LogMiner no tx chunking" << endl;
        if ((flags & 0x0080) != 0) oracleReader->dumpStream << "LogMiner Stealth transaction" << endl;
        if ((flags & 0x0100) != 0) oracleReader->dumpStream << "LSBY preserve" << endl;
        if ((flags & 0x0200) != 0) oracleReader->dumpStream << "LogMiner Marker transaction" << endl;
        if ((flags & 0x0400) != 0) oracleReader->dumpStream << "Transaction in pragama'ed plsql" << endl;
        if ((flags & 0x0800) != 0) oracleReader->dumpStream << "Tx audit CV flags undefined" << endl;
    }

    void OpCode0513::dumpMsgSessionSerial(uint64_t fieldPos, uint64_t fieldLength) {
        if (oracleReader->dumpRedoLog >= 1) {
            uint16_t serialNumber = oracleReader->read16(redoLogRecord->data + fieldPos + 2);
            uint16_t sessionNumber;
            if (oracleReader->version < 19000)
                sessionNumber = oracleReader->read16(redoLogRecord->data + fieldPos + 0);
            else
                sessionNumber = oracleReader->read16(redoLogRecord->data + fieldPos + 4);

            oracleReader->dumpStream <<
                    "session number   = " << dec << sessionNumber << endl <<
                    "serial  number   = " << dec << serialNumber << endl;
        }
    }

    void OpCode0513::dumpMsgVersion(uint64_t fieldPos, uint64_t fieldLength) {
        if (oracleReader->dumpRedoLog >= 1) {
            uint32_t version = oracleReader->read32(redoLogRecord->data + fieldPos + 0);
            oracleReader->dumpStream << "version " << dec << version << endl;
        }
    }

    void OpCode0513::dumpMsgAuditSessionid(uint64_t fieldPos, uint64_t fieldLength) {
        if (oracleReader->dumpRedoLog >= 1) {
            uint32_t auditSessionid = oracleReader->read32(redoLogRecord->data + fieldPos + 0);
            oracleReader->dumpStream << "audit sessionid " << auditSessionid << endl;
        }
    }
}
