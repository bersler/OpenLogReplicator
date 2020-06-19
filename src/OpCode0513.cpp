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

#include <iomanip>
#include <iostream>

#include "OpCode0513.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode0513::OpCode0513(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) :
            OpCode(oracleAnalyser, redoLogRecord) {
    }

    OpCode0513::~OpCode0513() {
    }

    void OpCode0513::process() {
        OpCode::process();
        uint64_t fieldNum = 0, fieldPos = 0;
        uint16_t fieldLength = 0;

        oracleAnalyser->nextField(redoLogRecord, fieldNum, fieldPos, fieldLength);
        //field: 1
        dumpMsgSessionSerial(fieldPos, fieldLength);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2
        dumpVal(fieldPos, fieldLength, "current username = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 3
        dumpVal(fieldPos, fieldLength, "login   username = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 4
        dumpVal(fieldPos, fieldLength, "client info      = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 5
        dumpVal(fieldPos, fieldLength, "OS username      = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 6
        dumpVal(fieldPos, fieldLength, "Machine name     = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 7
        dumpVal(fieldPos, fieldLength, "OS terminal      = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 8
        dumpVal(fieldPos, fieldLength, "OS process id    = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 9
        dumpVal(fieldPos, fieldLength, "OS program name  = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 10
        dumpVal(fieldPos, fieldLength, "transaction name = ");

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 11
        dumpMsgFlags(fieldPos, fieldLength);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 12
        dumpMsgVersion(fieldPos, fieldLength);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 13
        dumpMsgAuditSessionid(fieldPos, fieldLength);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord, fieldNum, fieldPos, fieldLength))
            return;
        //field: 14
        dumpVal(fieldPos, fieldLength, "Client Id  = ");
    }

    void OpCode0513::dumpMsgFlags(uint64_t fieldPos, uint64_t fieldLength) {
        uint16_t flags = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 0);
        if ((flags & 0x0001) != 0) oracleAnalyser->dumpStream << "DDL transaction" << endl;
        if ((flags & 0x0002) != 0) oracleAnalyser->dumpStream << "Space Management transaction" << endl;
        if ((flags & 0x0004) != 0) oracleAnalyser->dumpStream << "Recursive transaction" << endl;
        if ((flags & 0x0008) != 0) oracleAnalyser->dumpStream << "Logmnr Internal transaction" << endl;
        if ((flags & 0x0010) != 0) oracleAnalyser->dumpStream << "DB Open in Migrate Mode" << endl;
        if ((flags & 0x0020) != 0) oracleAnalyser->dumpStream << "LSBY ignore" << endl;
        if ((flags & 0x0040) != 0) oracleAnalyser->dumpStream << "LogMiner no tx chunking" << endl;
        if ((flags & 0x0080) != 0) oracleAnalyser->dumpStream << "LogMiner Stealth transaction" << endl;
        if ((flags & 0x0100) != 0) oracleAnalyser->dumpStream << "LSBY preserve" << endl;
        if ((flags & 0x0200) != 0) oracleAnalyser->dumpStream << "LogMiner Marker transaction" << endl;
        if ((flags & 0x0400) != 0) oracleAnalyser->dumpStream << "Transaction in pragama'ed plsql" << endl;
        if ((flags & 0x0800) != 0) oracleAnalyser->dumpStream << "Tx audit CV flags undefined" << endl;
    }

    void OpCode0513::dumpMsgSessionSerial(uint64_t fieldPos, uint64_t fieldLength) {
        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint16_t serialNumber = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 2);
            uint16_t sessionNumber;
            if (oracleAnalyser->version < 0x19000)
                sessionNumber = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 0);
            else
                sessionNumber = oracleAnalyser->read16(redoLogRecord->data + fieldPos + 4);

            oracleAnalyser->dumpStream <<
                    "session number   = " << dec << sessionNumber << endl <<
                    "serial  number   = " << dec << serialNumber << endl;
        }
    }

    void OpCode0513::dumpMsgVersion(uint64_t fieldPos, uint64_t fieldLength) {
        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint32_t version = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
            oracleAnalyser->dumpStream << "version " << dec << version << endl;
        }
    }

    void OpCode0513::dumpMsgAuditSessionid(uint64_t fieldPos, uint64_t fieldLength) {
        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint32_t auditSessionid = oracleAnalyser->read32(redoLogRecord->data + fieldPos + 0);
            oracleAnalyser->dumpStream << "audit sessionid " << auditSessionid << endl;
        }
    }
}
