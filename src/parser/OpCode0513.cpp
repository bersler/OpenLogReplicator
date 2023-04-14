/* Oracle Redo OpCode: 5.13
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/RedoLogRecord.h"
#include "OpCode0513.h"

namespace OpenLogReplicator {
    void OpCode0513::process(Ctx* ctx, RedoLogRecord* redoLogRecord) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051301);
        // Field: 1
        dumpMsgSessionSerial(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051302))
            return;
        // Field: 2
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "current username = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051303))
            return;
        // Field: 3
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "login   username = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051304))
            return;
        // Field: 4
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "client info      = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051305))
            return;
        // Field: 5
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "OS username      = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051306))
            return;
        // Field: 6
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "Machine name     = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051307))
            return;
        // Field: 7
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "OS terminal      = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051308))
            return;
        // Field: 8
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "OS process id    = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051309))
            return;
        // Field: 9
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "OS program name  = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130A))
            return;
        // Field: 10
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "transaction name = ");

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130B))
            return;
        // Field: 11
        dumpMsgFlags(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130C))
            return;
        // Field: 12
        dumpMsgVersion(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130D))
            return;
        // Field: 13
        dumpMsgAuditSessionid(ctx, redoLogRecord, fieldPos, fieldLength);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130E))
            return;
        // Field: 14
        dumpVal(ctx, redoLogRecord, fieldPos, fieldLength, "Client Id  = ");
    }

    void OpCode0513::dumpMsgFlags(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength __attribute__((unused))) {
        uint16_t flags = ctx->read16(redoLogRecord->data + fieldPos + 0);
        if ((flags & 0x0001) != 0) ctx->dumpStream << "DDL transaction" << std::endl;
        if ((flags & 0x0002) != 0) ctx->dumpStream << "Space Management transaction" << std::endl;
        if ((flags & 0x0004) != 0) ctx->dumpStream << "Recursive transaction" << std::endl;
        if ((flags & 0x0008) != 0) {
            if (ctx->version < REDO_VERSION_19_0) {
                ctx->dumpStream << "Logmnr Internal transaction" << std::endl;
            } else {
                ctx->dumpStream << "LogMiner Internal transaction" << std::endl;
            }
        }
        if ((flags & 0x0010) != 0) ctx->dumpStream << "DB Open in Migrate Mode" << std::endl;
        if ((flags & 0x0020) != 0) ctx->dumpStream << "LSBY ignore" << std::endl;
        if ((flags & 0x0040) != 0) ctx->dumpStream << "LogMiner no tx chunking" << std::endl;
        if ((flags & 0x0080) != 0) ctx->dumpStream << "LogMiner Stealth transaction" << std::endl;
        if ((flags & 0x0100) != 0) ctx->dumpStream << "LSBY preserve" << std::endl;
        if ((flags & 0x0200) != 0) ctx->dumpStream << "LogMiner Marker transaction" << std::endl;
        if ((flags & 0x0400) != 0) ctx->dumpStream << "Transaction in pragama'ed plsql" << std::endl;
        if ((flags & 0x0800) != 0) {
            if (ctx->version < REDO_VERSION_19_0) {
                ctx->dumpStream << "Tx audit CV flags undefined" << std::endl;
            } else {
                ctx->dumpStream << "Disabled Logical Repln. txn." << std::endl;
            }
        }
        if ((flags & 0x1000) != 0) ctx->dumpStream << "Datapump import txn" << std::endl;
        if ((flags & 0x8000) != 0) ctx->dumpStream << "Tx audit CV flags undefined" << std::endl;

        uint16_t flags2 = ctx->read16(redoLogRecord->data + fieldPos + 4);
        if ((flags2 & 0x0001) != 0) ctx->dumpStream << "Federation PDB replay" << std::endl;
        if ((flags2 & 0x0002) != 0) ctx->dumpStream << "PDB DDL replay" << std::endl;
        if ((flags2 & 0x0004) != 0) ctx->dumpStream << "LogMiner SKIP transaction" << std::endl;
        if ((flags2 & 0x0008) != 0) ctx->dumpStream << "SEQ$ update transaction" << std::endl;
    }

    void OpCode0513::dumpMsgSessionSerial(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength) {
        if (ctx->dumpRedoLog >= 1) {
            if (fieldLength < 4) {
                ctx->warning(70001,"too short field session serial: " + std::to_string(fieldLength) + " offset: " +
                             std::to_string(redoLogRecord->dataOffset));
                return;
            }

            uint16_t serialNumber = ctx->read16(redoLogRecord->data + fieldPos + 2);
            uint32_t sessionNumber;
            if (ctx->version < REDO_VERSION_19_0)
                sessionNumber = ctx->read16(redoLogRecord->data + fieldPos + 0);
            else {
                if (fieldLength < 8) {
                    ctx->warning(70001,"too short field session number: " + std::to_string(fieldLength) + " offset: " +
                                 std::to_string(redoLogRecord->dataOffset));
                    return;
                }
                sessionNumber = ctx->read32(redoLogRecord->data + fieldPos + 4);
            }

            ctx->dumpStream <<
                    "session number   = " << std::dec << sessionNumber << std::endl <<
                    "serial  number   = " << std::dec << serialNumber << std::endl;
        }
    }

    void OpCode0513::dumpMsgVersion(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength __attribute__((unused))) {
        if (ctx->dumpRedoLog >= 1) {
            uint32_t version = ctx->read32(redoLogRecord->data + fieldPos + 0);
            ctx->dumpStream << "version " << std::dec << version << std::endl;
        }
    }

    void OpCode0513::dumpMsgAuditSessionid(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t& fieldPos, uint16_t& fieldLength __attribute__((unused))) {
        if (ctx->dumpRedoLog >= 1) {
            uint32_t auditSessionid = ctx->read32(redoLogRecord->data + fieldPos + 0);
            ctx->dumpStream << "audit sessionid " << auditSessionid << std::endl;
        }
    }
}
