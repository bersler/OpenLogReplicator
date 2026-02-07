/* Redo Log OP Code 5.19
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef OP_CODE_05_13_H_
#define OP_CODE_05_13_H_

#include "../common/RedoLogRecord.h"
#include "OpCode.h"
#include "Transaction.h"

namespace OpenLogReplicator {
    class Transaction;

    class OpCode0513 : public OpCode {
    protected:
        static void attribute(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, const std::string& header,
                              Attribute::KEY key, Transaction* transaction) {
            const std::string value(reinterpret_cast<const char*>(redoLogRecord->data(fieldPos)), fieldSize);
            if (!value.empty())
                transaction->attributes.insert_or_assign(key, value);

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << header << value << '\n';
            }
        }

        static void attributeSessionSerial(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction) {
            if (unlikely(fieldSize < 4)) {
                ctx->warning(70001, "too short field session serial: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());
                return;
            }

            const uint16_t serialNumber = ctx->read16(redoLogRecord->data(fieldPos + 2));
            uint32_t sessionNumber;
            if (ctx->version < RedoLogRecord::REDO_VERSION_19_0)
                sessionNumber = ctx->read16(redoLogRecord->data(fieldPos + 0));
            else {
                if (fieldSize < 8) {
                    ctx->warning(70001, "too short field session number: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());
                    return;
                }
                sessionNumber = ctx->read32(redoLogRecord->data(fieldPos + 4));
            }

            std::string value = std::to_string(sessionNumber);
            if (!value.empty())
                transaction->attributes.insert_or_assign(Attribute::KEY::SESSION_NUMBER, value);

            value = std::to_string(serialNumber);
            if (!value.empty())
                transaction->attributes.insert_or_assign(Attribute::KEY::SERIAL_NUMBER, value);

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream <<
                        "session number   = " << std::dec << sessionNumber << '\n' <<
                        "serial  number   = " << std::dec << serialNumber << '\n';
            }
        }

        static void attributeFlags(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction) {
            if (unlikely(fieldSize < 2))
                throw RedoLogException(50061, "too short field 5.13.11: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            const std::string value("true");

            const uint16_t flags = ctx->read16(redoLogRecord->data(fieldPos + 0));
            if ((flags & 0x0001) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::DDL_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "DDL transaction\n";
            }

            if ((flags & 0x0002) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::SPACE_MANAGEMENT_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "Space Management transaction\n";
            }

            if ((flags & 0x0004) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::RECURSIVE_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "Recursive transaction\n";
            }

            if ((flags & 0x0008) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LOGMINER_INTERNAL_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                        *ctx->dumpStream << "Logmnr Internal transaction\n";
                    } else {
                        *ctx->dumpStream << "LogMiner Internal transaction\n";
                    }
                }
            }

            if ((flags & 0x0010) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::DB_OPEN_IN_MIGRATE_MODE, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "DB Open in Migrate Mode\n";
            }

            if ((flags & 0x0020) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LSBY_IGNORE, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "LSBY ignore\n";
            }

            if ((flags & 0x0040) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LOGMINER_NO_TX_CHUNKING, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "LogMiner no tx chunking\n";
            }

            if ((flags & 0x0080) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LOGMINER_STEALTH_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "LogMiner Stealth transaction\n";
            }

            if ((flags & 0x0100) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LSBY_PRESERVE, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "LSBY preserve\n";
            }

            if ((flags & 0x0200) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LOGMINER_MARKER_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "LogMiner Marker transaction\n";
            }

            if ((flags & 0x0400) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::TRANSACTION_IN_PRAGMAED_PLSQL, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "Transaction in pragma'ed plsql\n";
            }

            if ((flags & 0x0800) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::DISABLED_LOGICAL_REPLICATION_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1)) {
                    if (ctx->version < RedoLogRecord::REDO_VERSION_19_0) {
                        *ctx->dumpStream << "Tx audit CV flags undefined\n";
                    } else {
                        *ctx->dumpStream << "Disabled Logical Repln. txn.\n";
                    }
                }
            }

            if ((flags & 0x1000) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::DATAPUMP_IMPORT_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "Datapump import txn\n";
            }

            if ((flags & 0x8000) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::TRANSACTION_AUDIT_CV_FLAGS_UNDEFINED, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "Tx audit CV flags undefined\n";
            }

            const uint16_t flags2 = ctx->read16(redoLogRecord->data(fieldPos + 4));
            if ((flags2 & 0x0001) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::FEDERATION_PDB_REPLAY, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "Federation PDB replay\n";
            }

            if ((flags2 & 0x0002) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::PDB_DDL_REPLAY, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "PDB DDL replay\n";
            }

            if ((flags2 & 0x0004) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::LOGMINER_SKIP_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "LogMiner SKIP transaction\n";
            }

            if ((flags2 & 0x0008) != 0) {
                transaction->attributes.insert_or_assign(Attribute::KEY::SEQ_UPDATE_TRANSACTION, value);

                if (unlikely(ctx->dumpRedoLog >= 1))
                    *ctx->dumpStream << "SEQ$ update transaction\n";
            }
        }

        static void attributeVersion(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize, Transaction* transaction) {
            if (unlikely(fieldSize < 4))
                throw RedoLogException(50061, "too short field 5.13.12: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            const uint32_t version = ctx->read32(redoLogRecord->data(fieldPos + 0));
            const std::string value = std::to_string(version);
            if (!value.empty())
                transaction->attributes.insert_or_assign(Attribute::KEY::VERSION, value);

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "version " << std::dec << version << '\n';
            }
        }

        static void attributeAuditSessionId(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typePos fieldPos, typeSize fieldSize,
                                            Transaction* transaction) {
            if (unlikely(fieldSize < 4))
                throw RedoLogException(50061, "too short field 5.13.13: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            const uint32_t auditSessionId = ctx->read32(redoLogRecord->data(fieldPos + 0));
            const std::string value = std::to_string(auditSessionId);
            if (!value.empty())
                transaction->attributes.insert_or_assign(Attribute::KEY::AUDIT_SESSION_ID, value);

            if (unlikely(ctx->dumpRedoLog >= 1)) {
                *ctx->dumpStream << "audit sessionid " << auditSessionId << '\n';
            }
        }

    public:
        static void process0513(const Ctx* ctx, RedoLogRecord* redoLogRecord, Transaction* transaction) {
            process(ctx, redoLogRecord);
            typePos fieldPos = 0;
            typeField fieldNum = 0;
            typeSize fieldSize = 0;

            if (unlikely(transaction == nullptr)) {
                ctx->logTrace(Ctx::TRACE::TRANSACTION, "attributes with no transaction, offset: " + redoLogRecord->fileOffset.toString());
                return;
            }

            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051301);
            // Field: 1
            attributeSessionSerial(ctx, redoLogRecord, fieldPos, fieldSize, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051302))
                return;
            // Field: 2
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "current username = ", Attribute::KEY::CURRENT_USER_NAME, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051303))
                return;
            // Field: 3
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "login   username = ", Attribute::KEY::LOGIN_USER_NAME, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051304))
                return;
            // Field: 4
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "client info      = ", Attribute::KEY::CLIENT_INFO, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051305))
                return;
            // Field: 5
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "OS username      = ", Attribute::KEY::OS_USER_NAME, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051306))
                return;
            // Field: 6
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "Machine name     = ", Attribute::KEY::MACHINE_NAME, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051307))
                return;
            // Field: 7
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "OS terminal      = ", Attribute::KEY::OS_TERMINAL, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051308))
                return;
            // Field: 8
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "OS process id    = ", Attribute::KEY::OS_PROCESS_ID, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x051309))
                return;
            // Field: 9
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "OS program name  = ", Attribute::KEY::OS_PROGRAM_NAME, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05130A))
                return;
            // Field: 10
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "transaction name = ", Attribute::KEY::TRANSACTION_NAME, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05130B))
                return;
            // Field: 11
            attributeFlags(ctx, redoLogRecord, fieldPos, fieldSize, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05130C))
                return;
            // Field: 12
            attributeVersion(ctx, redoLogRecord, fieldPos, fieldSize, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05130D))
                return;
            // Field: 13
            attributeAuditSessionId(ctx, redoLogRecord, fieldPos, fieldSize, transaction);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x05130E))
                return;
            // Field: 14
            attribute(ctx, redoLogRecord, fieldPos, fieldSize, "Client Id  = ", Attribute::KEY::CLIENT_ID, transaction);
        }
    };
}

#endif
