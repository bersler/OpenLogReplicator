/* Oracle Redo OpCode: 5.13
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "Transaction.h"

namespace OpenLogReplicator {
    void OpCode0513::attribute(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength, const char* header,
                               const char* name, Transaction* transaction) {
        std::string value(reinterpret_cast<char*>(redoLogRecord->data + fieldPos), fieldLength);
        if (value != "")
            transaction->attributes.insert_or_assign(name, value);

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << header << value << '\n';
        }
    }

    void OpCode0513::process0513(Ctx* ctx, RedoLogRecord* redoLogRecord, Transaction* transaction) {
        OpCode::process(ctx, redoLogRecord);
        uint64_t fieldPos = 0;
        typeField fieldNum = 0;
        uint16_t fieldLength = 0;

        if (transaction == nullptr) {
            ctx->logTrace(TRACE_TRANSACTION, "attributes with no transaction, offset: " + std::to_string(redoLogRecord->dataOffset));
            return;
        }

        RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051301);
        // Field: 1
        attributeSessionSerial(ctx, redoLogRecord, fieldPos, fieldLength, transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051302))
            return;
        // Field: 2
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "current username = ", "current username", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051303))
            return;
        // Field: 3
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "login   username = ", "login username", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051304))
            return;
        // Field: 4
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "client info      = ", "client info", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051305))
            return;
        // Field: 5
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "OS username      = ", "OS username", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051306))
            return;
        // Field: 6
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "Machine name     = ", "machine name", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051307))
            return;
        // Field: 7
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "OS terminal      = ", "OS terminal", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051308))
            return;
        // Field: 8
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "OS process id    = ", "OS process id", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x051309))
            return;
        // Field: 9
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "OS program name  = ", "OS process name", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130A))
            return;
        // Field: 10
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "transaction name = ", "transaction name", transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130B))
            return;
        // Field: 11
        attributeFlags(ctx, redoLogRecord, fieldPos, fieldLength, transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130C))
            return;
        // Field: 12
        attributeVersion(ctx, redoLogRecord, fieldPos, fieldLength, transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130D))
            return;
        // Field: 13
        attributeAuditSessionId(ctx, redoLogRecord, fieldPos, fieldLength, transaction);

        if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldLength, 0x05130E))
            return;
        // Field: 14
        attribute(ctx, redoLogRecord, fieldPos, fieldLength, "Client Id  = ", "client id", transaction);
    }

    void OpCode0513::attributeFlags(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength, Transaction* transaction) {
        if (fieldLength < 2)
            throw RedoLogException(50061, "too short field 5.13.11: " + std::to_string(fieldLength) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        std::string value("true");

        uint16_t flags = ctx->read16(redoLogRecord->data + fieldPos + 0);
        if ((flags & 0x0001) != 0) {
            transaction->attributes.insert_or_assign("DDL transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "DDL transaction\n";
        }

        if ((flags & 0x0002) != 0) {
            transaction->attributes.insert_or_assign("space management transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "Space Management transaction\n";
        }

        if ((flags & 0x0004) != 0) {
            transaction->attributes.insert_or_assign("recursive transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "Recursive transaction\n";
        }

        if ((flags & 0x0008) != 0) {
            transaction->attributes.insert_or_assign("LogMiner internal transaction", value);

            if (ctx->dumpRedoLog >= 1) {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                    ctx->dumpStream << "Logmnr Internal transaction\n";
                } else {
                    ctx->dumpStream << "LogMiner Internal transaction\n";
                }
            }
        }

        if ((flags & 0x0010) != 0) {
            transaction->attributes.insert_or_assign("DB open in migrate mode", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "DB Open in Migrate Mode\n";
        }

        if ((flags & 0x0020) != 0) {
            transaction->attributes.insert_or_assign("LSBY ignore", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "LSBY ignore\n";
        }

        if ((flags & 0x0040) != 0) {
            transaction->attributes.insert_or_assign("LogMiner no tx chunking", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "LogMiner no tx chunking\n";
        }

        if ((flags & 0x0080) != 0) {
            transaction->attributes.insert_or_assign("LogMiner stealth transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "LogMiner Stealth transaction\n";
        }

        if ((flags & 0x0100) != 0) {
            transaction->attributes.insert_or_assign("LSBY preserve", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "LSBY preserve\n";
        }

        if ((flags & 0x0200) != 0) {
            transaction->attributes.insert_or_assign("LogMiner marker transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "LogMiner Marker transaction\n";
        }

        if ((flags & 0x0400) != 0) {
            transaction->attributes.insert_or_assign("transaction in pragama'ed plsql", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "Transaction in pragama'ed plsql\n";
        }

        if ((flags & 0x0800) != 0) {
            transaction->attributes.insert_or_assign("disabled logical repln. txn.", value);

            if (ctx->dumpRedoLog >= 1) {
                if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                    ctx->dumpStream << "Tx audit CV flags undefined\n";
                } else {
                    ctx->dumpStream << "Disabled Logical Repln. txn.\n";
                }
            }
        }

        if ((flags & 0x1000) != 0) {
            transaction->attributes.insert_or_assign("datapump import txn", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "Datapump import txn\n";
        }

        if ((flags & 0x8000) != 0) {
            transaction->attributes.insert_or_assign("txn audit CV flags undefined", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "Tx audit CV flags undefined\n";
        }

        uint16_t flags2 = ctx->read16(redoLogRecord->data + fieldPos + 4);
        if ((flags2 & 0x0001) != 0) {
            transaction->attributes.insert_or_assign("federation PDB replay", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "Federation PDB replay\n";
        }

        if ((flags2 & 0x0002) != 0) {
            transaction->attributes.insert_or_assign("PDB DDL replay", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "PDB DDL replay\n";
        }

        if ((flags2 & 0x0004) != 0) {
            transaction->attributes.insert_or_assign("LogMiner skip transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "LogMiner SKIP transaction\n";
        }

        if ((flags2 & 0x0008) != 0) {
            transaction->attributes.insert_or_assign("SEQ$ update transaction", value);

            if (ctx->dumpRedoLog >= 1)
                ctx->dumpStream << "SEQ$ update transaction\n";
        }
    }

    void OpCode0513::attributeSessionSerial(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength, Transaction* transaction) {
        if (fieldLength < 4) {
            ctx->warning(70001, "too short field session serial: " + std::to_string(fieldLength) + " offset: " +
                                std::to_string(redoLogRecord->dataOffset));
            return;
        }

        uint16_t serialNumber = ctx->read16(redoLogRecord->data + fieldPos + 2);
        uint32_t sessionNumber;
        if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
            sessionNumber = ctx->read16(redoLogRecord->data + fieldPos + 0);
        else {
            if (fieldLength < 8) {
                ctx->warning(70001, "too short field session number: " + std::to_string(fieldLength) + " offset: " +
                                    std::to_string(redoLogRecord->dataOffset));
                return;
            }
            sessionNumber = ctx->read32(redoLogRecord->data + fieldPos + 4);
        }

        std::string value = std::to_string(sessionNumber);
        if (value != "")
            transaction->attributes.insert_or_assign("session number", value);

        value = std::to_string(serialNumber);
        if (value != "")
            transaction->attributes.insert_or_assign("serial number", value);

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream <<
                            "session number   = " << std::dec << sessionNumber << '\n' <<
                            "serial  number   = " << std::dec << serialNumber << '\n';
        }
    }

    void OpCode0513::attributeVersion(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength, Transaction* transaction) {
        if (fieldLength < 4)
            throw RedoLogException(50061, "too short field 5.13.12: " + std::to_string(fieldLength) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        uint32_t version = ctx->read32(redoLogRecord->data + fieldPos + 0);
        std::string value = std::to_string(version);
        if (value != "")
            transaction->attributes.insert_or_assign("version", value);

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "version " << std::dec << version << '\n';
        }
    }

    void OpCode0513::attributeAuditSessionId(Ctx* ctx, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint16_t fieldLength, Transaction* transaction) {
        if (fieldLength < 4)
            throw RedoLogException(50061, "too short field 5.13.13: " + std::to_string(fieldLength) + " offset: " +
                                          std::to_string(redoLogRecord->dataOffset));

        uint32_t auditSessionid = ctx->read32(redoLogRecord->data + fieldPos + 0);
        std::string value = std::to_string(auditSessionid);
        if (value != "")
            transaction->attributes.insert_or_assign("audit sessionid", value);

        if (ctx->dumpRedoLog >= 1) {
            ctx->dumpStream << "audit sessionid " << auditSessionid << '\n';
        }
    }
}
