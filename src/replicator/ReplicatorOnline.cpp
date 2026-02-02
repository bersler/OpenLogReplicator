/* Thread reading database redo Logs using online mode
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

#include <algorithm>
#include <regex>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/DbColumn.h"
#include "../common/DbIncarnation.h"
#include "../common/DbTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/table/SysCCol.h"
#include "../common/table/SysCDef.h"
#include "../common/table/SysCol.h"
#include "../common/table/SysDeferredStg.h"
#include "../common/table/SysECol.h"
#include "../common/table/SysLob.h"
#include "../common/table/SysLobCompPart.h"
#include "../common/table/SysLobFrag.h"
#include "../common/table/SysObj.h"
#include "../common/table/SysTab.h"
#include "../common/table/SysTabComPart.h"
#include "../common/table/SysTabPart.h"
#include "../common/table/SysTabSubPart.h"
#include "../common/table/SysTs.h"
#include "../common/table/SysUser.h"
#include "../common/table/XdbTtSet.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXQn.h"
#include "../common/table/XdbXPt.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../parser/TransactionBuffer.h"
#include "../reader/Reader.h"
#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "ReplicatorOnline.h"

namespace OpenLogReplicator {
    ReplicatorOnline::ReplicatorOnline(Ctx * newCtx, void(*newArchGetLog)(Replicator * replicator), Builder * newBuilder, Metadata * newMetadata,
                                       TransactionBuffer * newTransactionBuffer, std::string newAlias, std::string newDatabase, std::string newUser,
                                       std::string newPassword, std::string newConnectString, bool newKeepConnection):
        Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, std::move(newAlias), std::move(newDatabase)),
        keepConnection(newKeepConnection) {
        env = new DatabaseEnvironment(ctx);
        env->initialize();
        conn = new DatabaseConnection(env, std::move(newUser), std::move(newPassword), std::move(newConnectString), false);
    }

    ReplicatorOnline::~ReplicatorOnline() {
        if (conn != nullptr) {
            delete conn;
            conn = nullptr;
        }

        if (env != nullptr) {
            delete env;
            env = nullptr;
        }
    }

    void ReplicatorOnline::loadDatabaseMetadata() {
        if (!checkConnection())
            return;

        Scn currentScn;
        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::GRANTS)) {
            const std::vector<std::string> tables{
                "SYS.V_$ARCHIVED_LOG",
                "SYS.V_$DATABASE",
                "SYS.V_$DATABASE_INCARNATION",
                "SYS.V_$LOG",
                "SYS.V_$LOGFILE",
                "SYS.V_$PARAMETER",
                "SYS.V_$STANDBY_LOG",
                "SYS.V_$TRANSPORTABLE_PLATFORM"
            };
            for (const auto& tableName: tables)
                checkTableForGrants(tableName);
        }

        archReader = readerCreate(0);

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_DATABASE_INFORMATION));
            stmt.createStatement(SQL_GET_DATABASE_INFORMATION);
            uint logMode;
            stmt.defineUInt(1, logMode);
            uint supplementalLogMin;
            stmt.defineUInt(2, supplementalLogMin);
            uint suppLogDbPrimary;
            stmt.defineUInt(3, suppLogDbPrimary);
            uint suppLogDbAll;
            stmt.defineUInt(4, suppLogDbAll);
            uint bigEndian;
            stmt.defineUInt(5, bigEndian);
            std::array < char, 130 > banner{};
            stmt.defineString(6, banner.data(), banner.size());
            std::array < char, 82 > context{};
            stmt.defineString(7, context.data(), context.size());
            stmt.defineUInt(8, currentScn);
            std::array < char, 81 > dbTimezoneStr{};
            stmt.defineString(9, dbTimezoneStr.data(), dbTimezoneStr.size());

            if (stmt.executeQuery() != 0) {
                if (logMode == 0) {
                    ctx->hint("run: SHUTDOWN IMMEDIATE;");
                    ctx->hint("run: STARTUP MOUNT;");
                    ctx->hint("run: ALTER DATABASE ARCHIVELOG;");
                    ctx->hint("run: ALTER DATABASE OPEN;");
                    throw RuntimeException(10021, "database not in ARCHIVELOG mode");
                }

                if (supplementalLogMin == 0) {
                    ctx->hint("run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;");
                    ctx->hint("run: ALTER SYSTEM ARCHIVE LOG CURRENT;");
                    throw RuntimeException(10022, "SUPPLEMENTAL_LOG_DATA_MIN missing");
                }

                if (bigEndian != 0)
                    ctx->setBigEndian();

                metadata->suppLogDbPrimary = suppLogDbPrimary != 0;
                metadata->suppLogDbAll = suppLogDbAll != 0;
                metadata->context = context.data();
                metadata->dbTimezoneStr = dbTimezoneStr.data();
                if (metadata->ctx->dbTimezone != Ctx::BAD_TIMEZONE) {
                    metadata->dbTimezone = metadata->ctx->dbTimezone;
                } else {
                    if (!Data::parseTimezone(dbTimezoneStr.data(), metadata->dbTimezone))
                        throw RuntimeException(10068, "invalid DBTIMEZONE value: " + std::string(dbTimezoneStr.data()));
                }

                // 12+
                metadata->dbId = 0;
                metadata->conId = 0;
                if (memcmp(banner.data(), "Oracle Database 11g", 19) != 0) {
                    checkTableForGrants("SYS.V_$PDBS");

                    ctx->version12 = true;
                    DatabaseStatement stmt2(conn);
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                        ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_CON_INFO));
                    stmt2.createStatement(SQL_GET_CON_INFO);
                    typeConId conId;
                    stmt2.defineInt(1, conId);
                    std::array < char, 81 > conNameChar{};
                    stmt2.defineString(2, conNameChar.data(), conNameChar.size());
                    std::array < char, 81 > conContext{};
                    stmt2.defineString(3, conContext.data(), conContext.size());
                    typeDbId dbId;
                    stmt2.defineUInt(4, dbId);

                    if (stmt2.executeQuery() != 0) {
                        metadata->dbId = dbId;
                        metadata->conId = conId;
                        metadata->conName = conNameChar.data();
                        metadata->context = conContext.data();
                    }
                }

                ctx->info(0, "version: " + std::string(banner.data()) + ", context: " + metadata->context + ", resetlogs: " +
                          std::to_string(metadata->resetlogs) + ", activation: " + std::to_string(metadata->activation) + ", con_id: " +
                          std::to_string(metadata->conId) + ", con_name: " + metadata->conName);
            } else {
                throw RuntimeException(10023, "no data in SYS.V_$DATABASE");
            }
        }

        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::GRANTS) && !standby) {
            const std::vector tables{
                SysCCol::tableName(),
                SysCDef::tableName(),
                SysCol::tableName(),
                SysDeferredStg::tableName(),
                SysECol::tableName(),
                SysLob::tableName(),
                SysLobCompPart::tableName(),
                SysLobFrag::tableName(),
                SysObj::tableName(),
                SysTab::tableName(),
                SysTabComPart::tableName(),
                SysTabSubPart::tableName(),
                SysTs::tableName(),
                SysUser::tableName(),
                XdbTtSet::tableName()
            };
            for (const auto& tableName: tables)
                checkTableForGrantsFlashback(tableName, currentScn);
        }

        metadata->dbRecoveryFileDest = getParameterValue("db_recovery_file_dest");
        if (!metadata->dbRecoveryFileDest.empty() && metadata->dbRecoveryFileDest.back() == '/') {
            while (!metadata->dbRecoveryFileDest.empty() && metadata->dbRecoveryFileDest.back() == '/')
                metadata->dbRecoveryFileDest.pop_back();
            ctx->warning(60026, "stripping trailing '/' from db_recovery_file_dest parameter; new value: " + metadata->dbRecoveryFileDest);
        }
        metadata->logArchiveDest = getParameterValue("log_archive_dest");
        if (!metadata->logArchiveDest.empty() && metadata->logArchiveDest.back() == '/') {
            while (!metadata->logArchiveDest.empty() && metadata->logArchiveDest.back() == '/')
                metadata->logArchiveDest.pop_back();
            ctx->warning(60026, "stripping trailing '/' from log_archive_dest parameter; new value: " + metadata->logArchiveDest);
        }
        metadata->dbBlockChecksum = getParameterValue("db_block_checksum");
        std::transform(metadata->dbBlockChecksum.begin(), metadata->dbBlockChecksum.end(), metadata->dbBlockChecksum.begin(), ::toupper);
        if (metadata->dbRecoveryFileDest.empty())
            metadata->logArchiveFormat = getParameterValue("log_archive_format");
        metadata->nlsCharacterSet = getPropertyValue("NLS_CHARACTERSET");
        metadata->nlsNcharCharacterSet = getPropertyValue("NLS_NCHAR_CHARACTERSET");

        ctx->info(0, "loading character mapping for " + metadata->nlsCharacterSet);
        ctx->info(0, "loading character mapping for " + metadata->nlsNcharCharacterSet);
        metadata->setNlsCharset(metadata->nlsCharacterSet, metadata->nlsNcharCharacterSet);
        metadata->onlineData = true;
    }

    void ReplicatorOnline::positionReader() {
        // Position by time
        if (!metadata->startTime.empty()) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw BootException(10024, "can't position by time for standby database");

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SCN_FROM_TIME));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + metadata->startTime);
            }
            stmt.createStatement(SQL_GET_SCN_FROM_TIME);

            stmt.bindString(1, metadata->startTime);
            Scn firstDataScn;
            stmt.defineUInt(1, firstDataScn);

            if (stmt.executeQuery() == 0)
                throw BootException(10025, "can't find scn for: " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;
        } else if (metadata->startTimeRel > 0) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw BootException(10026, "can't position by relative time for standby database");

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SCN_FROM_TIME_RELATIVE));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + std::to_string(metadata->startTimeRel));
            }
            stmt.createStatement(SQL_GET_SCN_FROM_TIME_RELATIVE);
            stmt.bindUInt(1, metadata->startTimeRel);
            Scn firstDataScn;
            stmt.defineUInt(1, firstDataScn);

            if (stmt.executeQuery() == 0)
                throw BootException(10025, "can't find scn for " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;
        } else if (metadata->firstDataScn == Scn::none() || metadata->firstDataScn == Scn::zero()) {
            // NOW
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_DATABASE_SCN));
            stmt.createStatement(SQL_GET_DATABASE_SCN);
            Scn firstDataScn;
            stmt.defineUInt(1, firstDataScn);

            if (stmt.executeQuery() == 0)
                throw BootException(10029, "can't find database current scn");
            metadata->firstDataScn = firstDataScn;
        }

        // First sequence
        if (metadata->startSequence != Seq::none()) {
            metadata->setSeqFileOffset(metadata->startSequence, FileOffset::zero());
            if (metadata->firstDataScn == Scn::none())
                metadata->firstDataScn = Scn::zero();
        } else {
            DatabaseStatement stmt(conn);
            if (standby) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SEQUENCE_FROM_SCN_STANDBY));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(metadata->resetlogs));
                }
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
                stmt.bindUInt(1, metadata->firstDataScn);
                stmt.bindUInt(2, metadata->firstDataScn);
                stmt.bindUInt(3, metadata->resetlogs);
            } else {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SEQUENCE_FROM_SCN));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(metadata->resetlogs));
                }
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN);
                stmt.bindUInt(1, metadata->firstDataScn);
                stmt.bindUInt(2, metadata->firstDataScn);
                stmt.bindUInt(3, metadata->resetlogs);
            }
            Seq sequence;
            stmt.defineUInt(1, sequence);

            if (stmt.executeQuery() == 0)
                throw BootException(10030, "getting database sequence for scn: " + metadata->firstDataScn.toString());

            metadata->setSeqFileOffset(sequence, FileOffset::zero());
            ctx->info(0, "starting sequence not found - starting with new batch with seq: " + metadata->sequence.toString());
        }

        if (metadata->firstDataScn == Scn::none())
            throw BootException(10031, "getting database scn");
    }

    bool ReplicatorOnline::checkConnection() {
        if (!conn->connected) {
            ctx->info(0, "connecting to the database instance of " + database + " to " + conn->connectString);
        }

        while (!ctx->softShutdown) {
            if (!conn->connected) {
                try {
                    conn->connect();
                } catch (RuntimeException& ex) {
                    ctx->error(ex.code, ex.msg);
                }
            }

            if (conn->connected) {
                try {
                    DatabaseStatement stmt(conn);
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                        ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_CHECK_CONNECTION));
                    stmt.createStatement(SQL_CHECK_CONNECTION);
                    uint dummy;
                    stmt.defineUInt(1, dummy);

                    stmt.executeQuery();
                } catch (RuntimeException& ex) {
                    ctx->error(ex.code, ex.msg);
                    conn->disconnect();
                    contextSet(CONTEXT::SLEEP);
                    ctx->usleepInt(ctx->refreshIntervalUs);
                    contextSet(CONTEXT::CPU);
                    ctx->info(0, "reconnecting to the database instance of " + database + " to " + conn->connectString);
                    continue;
                }

                return true;
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "cannot connect to database, retry in " + std::to_string(ctx->refreshIntervalUs / 1000000) + " sec.");
            contextSet(CONTEXT::SLEEP);
            ctx->usleepInt(ctx->refreshIntervalUs);
            contextSet(CONTEXT::CPU);
        }

        return false;
    }

    void ReplicatorOnline::goStandby() {
        if (!keepConnection)
            conn->disconnect();
    }

    std::string ReplicatorOnline::getParameterValue(std::string parameter) const {
        std::array < char, DbTable::VPARAMETER_LENGTH + 1 > value{};
        DatabaseStatement stmt(conn);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
            ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_PARAMETER));
            ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + parameter);
        }
        stmt.createStatement(SQL_GET_PARAMETER);
        stmt.bindString(1, parameter);
        stmt.defineString(1, value.data(), value.size());

        if (stmt.executeQuery() != 0)
            return {value.data()};

        // No value found
        throw RuntimeException(10032, "can't get parameter value for " + parameter);
    }

    std::string ReplicatorOnline::getPropertyValue(std::string property) const {
        std::array < char, DbTable::VPROPERTY_LENGTH + 1 > value{};
        DatabaseStatement stmt(conn);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
            ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_PROPERTY));
            ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + property);
        }
        stmt.createStatement(SQL_GET_PROPERTY);
        stmt.bindString(1, property);
        stmt.defineString(1, value.data(), value.size());

        if (stmt.executeQuery() != 0)
            return {value.data()};

        // No value found
        throw RuntimeException(10033, "can't get property value for " + property);
    }

    void ReplicatorOnline::checkTableForGrants(const std::string& tableName) {
        try {
            const std::string query("SELECT 1 FROM " + tableName + " WHERE 0 = 1");
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, query);
            stmt.createStatement(query);
            uint dummy;
            stmt.defineUInt(1, dummy);

            stmt.executeQuery();
        } catch (RuntimeException& ex) {
            if (ex.supCode == 1031) {
                if (metadata->conId > 0) {
                    ctx->hint("run: ALTER SESSION SET CONTAINER = " + metadata->conName + ";");
                    ctx->hint("run: GRANT SELECT ON " + tableName + " TO " + conn->user + ";");
                } else {
                    ctx->hint("run: GRANT SELECT ON " + tableName + " TO " + conn->user + ";");
                }
                throw RuntimeException(10034, "grants missing for table " + tableName);
            }
            throw RuntimeException(ex.code, ex.msg);
        }
    }

    void ReplicatorOnline::checkTableForGrantsFlashback(const std::string& tableName, Scn scn) {
        try {
            const std::string query("SELECT 1 FROM " + tableName + " AS OF SCN " + scn.toString() + " WHERE 0 = 1");
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, query);
            stmt.createStatement(query);
            uint dummy;
            stmt.defineUInt(1, dummy);

            stmt.executeQuery();
        } catch (RuntimeException& ex) {
            if (ex.supCode == 1031) {
                if (metadata->conId > 0) {
                    ctx->hint("run: ALTER SESSION SET CONTAINER = " + metadata->conName + ";");
                    ctx->hint("run: GRANT SELECT, FLASHBACK ON " + tableName + " TO " + conn->user + ";");
                } else {
                    ctx->hint("run: GRANT SELECT, FLASHBACK ON " + tableName + " TO " + conn->user + ";");
                }
                throw RuntimeException(10034, "grants missing for table " + tableName);
            }
            if (ex.supCode == 8181)
                throw RuntimeException(10035, "specified SCN number is not a valid system change number");
            throw RuntimeException(ex.code, ex.msg);
        }
    }

    void ReplicatorOnline::verifySchema(Scn currentScn) {
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::VERIFY_SCHEMA))
            return;
        if (!checkConnection())
            return;

        ctx->info(0, "verifying schema for SCN: " + currentScn.toString());

        Schema otherSchema(ctx, metadata->locales);
        try {
            readSystemDictionariesMetadata(&otherSchema, currentScn);
            for (const SchemaElement* element: metadata->schemaElements)
                readSystemDictionaries(&otherSchema, currentScn, element->owner, element->table, element->options);
            std::string errMsg;
            const bool result = metadata->schema->compare(&otherSchema, errMsg);
            if (result) {
                ctx->warning(70000, "schema incorrect: " + errMsg);
            }
        } catch (RuntimeException& e) {
            ctx->error(e.code, e.msg);
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }
    }

    void ReplicatorOnline::createSchema() {
        if (!checkConnection())
            return;

        ctx->info(0, "reading dictionaries for scn: " + metadata->firstDataScn.toString());

        std::unordered_map<typeObj, std::string> tablesUpdated;
        {
            contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_SCHEMA);
            std::unique_lock const lck(metadata->mtxSchema);
            metadata->schema->purgeMetadata();
            metadata->schema->purgeDicts();
            metadata->schema->scn = metadata->firstDataScn;
            metadata->firstSchemaScn = metadata->firstDataScn;
            readSystemDictionariesMetadata(metadata->schema, metadata->firstDataScn);

            for (const SchemaElement* element: metadata->schemaElements)
                createSchemaForTable(metadata->firstDataScn, element->owner, element->table, element->keyList, element->key, element->tagType,
                                     element->tagList, element->tag, element->condition, element->options, tablesUpdated);
            metadata->schema->resetTouched();

            if (unlikely(metadata->ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                metadata->ctx->logTrace(Ctx::TRACE::CHECKPOINT, "schema creation completed, allowing checkpoints");
            metadata->allowCheckpoints();
        }
        contextSet(CONTEXT::CPU);

        for (const auto& [_, tableName]: tablesUpdated)
            ctx->info(0, "- found: " + tableName);
    }

    void ReplicatorOnline::readSystemDictionariesMetadata(Schema* schema, Scn targetScn) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "reading metadata");

        try {
            // Reading SYS.TS$
            DatabaseStatement sysTsStmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TS));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
            }
            sysTsStmt.createStatement(SQL_GET_SYS_TS);
            sysTsStmt.bindUInt(1, targetScn);
            std::array < char, RowId::SIZE + 1 > sysTsRowidStr{};
            sysTsStmt.defineString(1, sysTsRowidStr.data(), sysTsRowidStr.size());
            typeTs sysTsTs;
            sysTsStmt.defineUInt(2, sysTsTs);
            std::array < char, SysTs::NAME_LENGTH + 1 > sysTsName{};
            sysTsStmt.defineString(3, sysTsName.data(), sysTsName.size());
            uint32_t sysTsBlockSize;
            sysTsStmt.defineUInt(4, sysTsBlockSize);

            int sysTsRet = sysTsStmt.executeQuery();
            while (sysTsRet != 0) {
                schema->sysTsPack.addWithKeys(ctx, new SysTs(RowId(sysTsRowidStr), sysTsTs, sysTsName.data(), sysTsBlockSize));
                sysTsRet = sysTsStmt.next();
            }

            // Reading XDB.XDB$TTSET
            DatabaseStatement xdbTtSetStmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_XDB_TTSET));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
            }
            xdbTtSetStmt.createStatement(SQL_GET_XDB_TTSET);
            xdbTtSetStmt.bindUInt(1, targetScn);
            std::array < char, RowId::SIZE + 1 > xdbTtSetRowidStr{};
            xdbTtSetStmt.defineString(1, xdbTtSetRowidStr.data(), xdbTtSetRowidStr.size());
            std::array < char, XdbTtSet::GUID_LENGTH + 1 > xdbTtSetGuid{};
            xdbTtSetStmt.defineString(2, xdbTtSetGuid.data(), xdbTtSetGuid.size());
            std::array < char, XdbTtSet::TOKSUF_LENGTH + 1 > xdbTtSetTokSuf{};
            xdbTtSetStmt.defineString(3, xdbTtSetTokSuf.data(), xdbTtSetTokSuf.size());
            uint64_t xdbTtSetFlags;
            xdbTtSetStmt.defineUInt(4, xdbTtSetFlags);
            uint32_t xdbTtSetObj;
            xdbTtSetStmt.defineUInt(5, xdbTtSetObj);

            int xdbTtSetRet = xdbTtSetStmt.executeQuery();
            while (xdbTtSetRet != 0) {
                schema->xdbTtSetPack.addWithKeys(ctx, new XdbTtSet(RowId(xdbTtSetRowidStr), xdbTtSetGuid.data(), xdbTtSetTokSuf.data(), xdbTtSetFlags,
                                                                   xdbTtSetObj));
                xdbTtSetRet = xdbTtSetStmt.next();
            }

            for (const auto& [_, xdbTtSet]: schema->xdbTtSetPack.mapRowId) {
                auto* xmlCtx = new XmlCtx(ctx, xdbTtSet->tokSuf, xdbTtSet->flags);
                schema->schemaXmlMap.insert_or_assign(xdbTtSet->tokSuf, xmlCtx);

                // Check permissions before reading data
                std::string tableName = "XDB.X$NM" + xdbTtSet->tokSuf;
                checkTableForGrantsFlashback(tableName, targetScn);
                tableName = "XDB.X$PT" + xdbTtSet->tokSuf;
                checkTableForGrantsFlashback(tableName, targetScn);
                tableName = "XDB.X$QN" + xdbTtSet->tokSuf;
                checkTableForGrantsFlashback(tableName, targetScn);

                // Reading XDB.X$NMxxxx
                DatabaseStatement xdbXNmStmt(conn);
                const std::string SQL_GET_XDB_XNM = "SELECT"
                        "   T.ROWID, T.NMSPCURI, T.ID "
                        " FROM"
                        "   XDB.X$NM" + xdbTtSet->tokSuf + " AS OF SCN :i T";
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, SQL_GET_XDB_XNM);
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                }
                xdbXNmStmt.createStatement(SQL_GET_XDB_XNM);
                xdbXNmStmt.bindUInt(1, targetScn);
                std::array < char, RowId::SIZE + 1 > xdbXNmRowidStr{};
                xdbXNmStmt.defineString(1, xdbXNmRowidStr.data(), xdbXNmRowidStr.size());
                std::array < char, XdbXNm::NMSPCURI_LENGTH + 1 > xdbNmNmSpcUri{};
                xdbXNmStmt.defineString(2, xdbNmNmSpcUri.data(), xdbNmNmSpcUri.size());
                std::array < char, XdbXNm::ID_LENGTH + 1 > xdbNmId{};
                xdbXNmStmt.defineString(3, xdbNmId.data(), xdbNmId.size());

                int xdbXNmRet = xdbXNmStmt.executeQuery();
                while (xdbXNmRet != 0) {
                    xmlCtx->xdbXNmPack.addWithKeys(ctx, new XdbXNm(RowId(xdbXNmRowidStr), xdbNmNmSpcUri.data(), xdbNmId.data()));
                    xdbXNmRet = xdbXNmStmt.next();
                }

                // Reading XDB.X$PTxxxx
                DatabaseStatement xdbXPtStmt(conn);
                const std::string SQL_GET_XDB_XPT = "SELECT"
                        "   T.ROWID, T.PATH, T.ID "
                        " FROM"
                        "   XDB.X$PT" + xdbTtSet->tokSuf + " AS OF SCN :i T";
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, SQL_GET_XDB_XPT);
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                }
                xdbXPtStmt.createStatement(SQL_GET_XDB_XPT);
                xdbXPtStmt.bindUInt(1, targetScn);
                std::array < char, RowId::SIZE + 1 > xdbXPtRowidStr{};
                xdbXPtStmt.defineString(1, xdbXPtRowidStr.data(), xdbXPtRowidStr.size());
                std::array < char, XdbXPt::PATH_LENGTH + 1 > xdbPtPath{};
                xdbXPtStmt.defineString(2, xdbPtPath.data(), xdbPtPath.size());
                std::array < char, XdbXPt::ID_LENGTH + 1 > xdbPtId{};
                xdbXPtStmt.defineString(3, xdbPtId.data(), xdbPtId.size());

                int xdbXPtRet = xdbXPtStmt.executeQuery();
                while (xdbXPtRet != 0) {
                    xmlCtx->xdbXPtPack.addWithKeys(ctx, new XdbXPt(RowId(xdbXPtRowidStr), xdbPtPath.data(), xdbPtId.data()));
                    xdbXPtRet = xdbXPtStmt.next();
                }

                // Reading XDB.X$QNxxxx
                DatabaseStatement xdbXQnStmt(conn);
                const std::string SQL_GET_XDB_XQN = "SELECT"
                        "   T.ROWID, T.NMSPCID, T.LOCALNAME, T.FLAGS, T.ID "
                        " FROM"
                        "   XDB.X$QN" + xdbTtSet->tokSuf + " AS OF SCN :i T";
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, SQL_GET_XDB_XQN);
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                }
                xdbXQnStmt.createStatement(SQL_GET_XDB_XQN);
                xdbXQnStmt.bindUInt(1, targetScn);
                std::array < char, RowId::SIZE + 1 > xdbXQnRowidStr{};
                xdbXQnStmt.defineString(1, xdbXQnRowidStr.data(), xdbXQnRowidStr.size());
                std::array < char, XdbXQn::NMSPCID_LENGTH + 1 > xdbXQnNmSpcId{};
                xdbXQnStmt.defineString(2, xdbXQnNmSpcId.data(), xdbXQnNmSpcId.size());
                std::array < char, XdbXQn::LOCALNAME_LENGTH + 1 > xdbXQnLocalName{};
                xdbXQnStmt.defineString(3, xdbXQnLocalName.data(), xdbXQnLocalName.size());
                std::array < char, XdbXQn::FLAGS_LENGTH + 1 > xdbXQnFlags{};
                xdbXQnStmt.defineString(4, xdbXQnFlags.data(), xdbXQnFlags.size());
                std::array < char, XdbXQn::ID_LENGTH + 1 > xdbXQnId{};
                xdbXQnStmt.defineString(5, xdbXQnId.data(), xdbXQnId.size());

                int xdbXQnRet = xdbXQnStmt.executeQuery();
                while (xdbXQnRet != 0) {
                    xmlCtx->xdbXQnPack.addWithKeys(ctx, new XdbXQn(RowId(xdbXQnRowidStr), xdbXQnNmSpcId.data(), xdbXQnLocalName.data(), xdbXQnFlags.data(),
                                                                   xdbXQnId.data()));
                    xdbXQnRet = xdbXQnStmt.next();
                }
            }
        } catch (RuntimeException& ex) {
            if (ex.supCode == 8181) {
                throw BootException(10035, "can't read metadata from flashback, provide a valid starting SCN value");
            }
            throw BootException(ex.code, ex.msg);
        }
    }

    void ReplicatorOnline::readSystemDictionariesDetails(Schema* schema, Scn targetScn, typeUser user, typeObj obj) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "read dictionaries for user: " + std::to_string(user) + ", object: " + std::to_string(obj));

        // Reading SYS.CCOL$
        DatabaseStatement sysCColStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_CCOL_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysCColStmt.createStatement(SQL_GET_SYS_CCOL_OBJ);
            sysCColStmt.bindUInt(1, targetScn);
            sysCColStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_CCOL_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysCColStmt.createStatement(SQL_GET_SYS_CCOL_USER);
            sysCColStmt.bindUInt(1, targetScn);
            sysCColStmt.bindUInt(2, targetScn);
            sysCColStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysCColRowidStr{};
        sysCColStmt.defineString(1, sysCColRowidStr.data(), sysCColRowidStr.size());
        typeCon sysCColCon;
        sysCColStmt.defineUInt(2, sysCColCon);
        typeCol sysCColIntCol;
        sysCColStmt.defineInt(3, sysCColIntCol);
        typeObj sysCColObj;
        sysCColStmt.defineUInt(4, sysCColObj);
        uint64_t sysCColSpare11 = 0;
        sysCColStmt.defineUInt(5, sysCColSpare11);
        uint64_t sysCColSpare12 = 0;
        sysCColStmt.defineUInt(6, sysCColSpare12);

        int sysCColRet = sysCColStmt.executeQuery();
        while (sysCColRet != 0) {
            schema->sysCColPack.addWithKeys(ctx, new SysCCol(RowId(sysCColRowidStr), sysCColCon, sysCColIntCol, sysCColObj, sysCColSpare11, sysCColSpare12));
            schema->touchTable(sysCColObj);
            sysCColSpare11 = 0;
            sysCColSpare12 = 0;
            sysCColRet = sysCColStmt.next();
        }

        // Reading SYS.CDEF$
        DatabaseStatement sysCDefStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_CDEF_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysCDefStmt.createStatement(SQL_GET_SYS_CDEF_OBJ);
            sysCDefStmt.bindUInt(1, targetScn);
            sysCDefStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_CDEF_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysCDefStmt.createStatement(SQL_GET_SYS_CDEF_USER);
            sysCDefStmt.bindUInt(1, targetScn);
            sysCDefStmt.bindUInt(2, targetScn);
            sysCDefStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysCDefRowidStr{};
        sysCDefStmt.defineString(1, sysCDefRowidStr.data(), sysCDefRowidStr.size());
        typeCon sysCDefCon;
        sysCDefStmt.defineUInt(2, sysCDefCon);
        typeObj sysCDefObj;
        sysCDefStmt.defineUInt(3, sysCDefObj);
        uint sysCDefType;
        sysCDefStmt.defineUInt(4, sysCDefType);

        int sysCDefRet = sysCDefStmt.executeQuery();
        while (sysCDefRet != 0) {
            schema->sysCDefPack.addWithKeys(ctx, new SysCDef(RowId(sysCDefRowidStr), sysCDefCon, sysCDefObj, static_cast<SysCDef::CDEFTYPE>(sysCDefType)));
            schema->touchTable(sysCDefObj);
            sysCDefRet = sysCDefStmt.next();
        }

        // Reading SYS.COL$
        DatabaseStatement sysColStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_COL_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysColStmt.createStatement(SQL_GET_SYS_COL_OBJ);
            sysColStmt.bindUInt(1, targetScn);
            sysColStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_COL_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysColStmt.createStatement(SQL_GET_SYS_COL_USER);
            sysColStmt.bindUInt(1, targetScn);
            sysColStmt.bindUInt(2, targetScn);
            sysColStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysColRowidStr{};
        sysColStmt.defineString(1, sysColRowidStr.data(), sysColRowidStr.size());
        typeObj sysColObj;
        sysColStmt.defineUInt(2, sysColObj);
        typeCol sysColCol;
        sysColStmt.defineInt(3, sysColCol);
        typeCol sysColSegCol;
        sysColStmt.defineInt(4, sysColSegCol);
        typeCol sysColIntCol;
        sysColStmt.defineInt(5, sysColIntCol);
        std::array < char, SysCol::NAME_LENGTH + 1 > sysColName{};
        sysColStmt.defineString(6, sysColName.data(), sysColName.size());
        uint sysColType;
        sysColStmt.defineUInt(7, sysColType);
        uint sysColLength;
        sysColStmt.defineUInt(8, sysColLength);
        int sysColPrecision = -1;
        sysColStmt.defineInt(9, sysColPrecision);
        int sysColScale = -1;
        sysColStmt.defineInt(10, sysColScale);
        uint sycColCharsetForm = 0;
        sysColStmt.defineUInt(11, sycColCharsetForm);
        uint sysColCharsetId = 0;
        sysColStmt.defineUInt(12, sysColCharsetId);
        int sysColNull;
        sysColStmt.defineInt(13, sysColNull);
        uint64_t sysColProperty1;
        sysColStmt.defineUInt(14, sysColProperty1);
        uint64_t sysColProperty2;
        sysColStmt.defineUInt(15, sysColProperty2);

        int sysColRet = sysColStmt.executeQuery();
        while (sysColRet != 0) {
            schema->sysColPack.addWithKeys(ctx, new SysCol(RowId(sysColRowidStr), sysColObj, sysColCol, sysColSegCol, sysColIntCol, sysColName.data(),
                                                           static_cast<SysCol::COLTYPE>(sysColType), sysColLength, sysColPrecision, sysColScale,
                                                           sycColCharsetForm, sysColCharsetId, sysColNull, sysColProperty1, sysColProperty2));
            schema->touchTable(sysColObj);
            sysColPrecision = -1;
            sysColScale = -1;
            sycColCharsetForm = 0;
            sysColCharsetId = 0;
            sysColRet = sysColStmt.next();
        }

        // Reading SYS.DEFERRED_STG$
        DatabaseStatement sysDeferredStgStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_DEFERRED_STG_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysDeferredStgStmt.createStatement(SQL_GET_SYS_DEFERRED_STG_OBJ);
            sysDeferredStgStmt.bindUInt(1, targetScn);
            sysDeferredStgStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_DEFERRED_STG_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysDeferredStgStmt.createStatement(SQL_GET_SYS_DEFERRED_STG_USER);
            sysDeferredStgStmt.bindUInt(1, targetScn);
            sysDeferredStgStmt.bindUInt(2, targetScn);
            sysDeferredStgStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysDeferredStgRowidStr{};
        sysDeferredStgStmt.defineString(1, sysDeferredStgRowidStr.data(), sysDeferredStgRowidStr.size());
        typeObj sysDeferredStgObj;
        sysDeferredStgStmt.defineUInt(2, sysDeferredStgObj);
        uint64_t sysDeferredStgFlagsStg1 = 0;
        sysDeferredStgStmt.defineUInt(3, sysDeferredStgFlagsStg1);
        uint64_t sysDeferredStgFlagsStg2 = 0;
        sysDeferredStgStmt.defineUInt(4, sysDeferredStgFlagsStg2);

        int sysDeferredStgRet = sysDeferredStgStmt.executeQuery();
        while (sysDeferredStgRet != 0) {
            schema->sysDeferredStgPack.addWithKeys(ctx, new SysDeferredStg(RowId(sysDeferredStgRowidStr), sysDeferredStgObj,
                                                                           sysDeferredStgFlagsStg1, sysDeferredStgFlagsStg2));
            schema->touchTable(sysDeferredStgObj);
            sysDeferredStgFlagsStg1 = 0;
            sysDeferredStgFlagsStg2 = 0;
            sysDeferredStgRet = sysDeferredStgStmt.next();
        }

        // Reading SYS.ECOL$
        DatabaseStatement sysEColStmt(conn);
        if (ctx->version12) {
            if (obj != 0) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_ECOL_OBJ));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL_OBJ);
                sysEColStmt.bindUInt(1, targetScn);
                sysEColStmt.bindUInt(2, obj);
            } else {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_ECOL_USER));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL_USER);
                sysEColStmt.bindUInt(1, targetScn);
                sysEColStmt.bindUInt(2, targetScn);
                sysEColStmt.bindUInt(3, user);
            }
        } else {
            if (obj != 0) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_ECOL11_OBJ));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL11_OBJ);
                sysEColStmt.bindUInt(1, targetScn);
                sysEColStmt.bindUInt(2, obj);
            } else {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_ECOL11_USER));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL11_USER);
                sysEColStmt.bindUInt(1, targetScn);
                sysEColStmt.bindUInt(2, targetScn);
                sysEColStmt.bindUInt(3, user);
            }
        }

        std::array < char, RowId::SIZE + 1 > sysEColRowidStr{};
        sysEColStmt.defineString(1, sysEColRowidStr.data(), sysEColRowidStr.size());
        typeObj sysEColTabObj;
        sysEColStmt.defineUInt(2, sysEColTabObj);
        typeCol sysEColColNum = 0;
        sysEColStmt.defineInt(3, sysEColColNum);
        typeCol sysEColGuardId = -1;
        sysEColStmt.defineInt(4, sysEColGuardId);

        int sysEColRet = sysEColStmt.executeQuery();
        while (sysEColRet != 0) {
            schema->sysEColPack.addWithKeys(ctx, new SysECol(RowId(sysEColRowidStr), sysEColTabObj, sysEColColNum, sysEColGuardId));
            schema->touchTable(sysEColTabObj);
            sysEColColNum = 0;
            sysEColGuardId = -1;
            sysEColRet = sysEColStmt.next();
        }

        // Reading SYS.LOB$
        DatabaseStatement sysLobStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_LOB_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysLobStmt.createStatement(SQL_GET_SYS_LOB_OBJ);
            sysLobStmt.bindUInt(1, targetScn);
            sysLobStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_LOB_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysLobStmt.createStatement(SQL_GET_SYS_LOB_USER);
            sysLobStmt.bindUInt(1, targetScn);
            sysLobStmt.bindUInt(2, targetScn);
            sysLobStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysLobRowidStr{};
        sysLobStmt.defineString(1, sysLobRowidStr.data(), sysLobRowidStr.size());
        typeObj sysLobObj;
        sysLobStmt.defineUInt(2, sysLobObj);
        typeCol sysLobCol = 0;
        sysLobStmt.defineInt(3, sysLobCol);
        typeCol sysLobIntCol = 0;
        sysLobStmt.defineInt(4, sysLobIntCol);
        typeObj sysLobLObj;
        sysLobStmt.defineUInt(5, sysLobLObj);
        typeTs sysLobTs;
        sysLobStmt.defineUInt(6, sysLobTs);

        int sysLobRet = sysLobStmt.executeQuery();
        while (sysLobRet != 0) {
            schema->sysLobPack.addWithKeys(ctx, new SysLob(RowId(sysLobRowidStr), sysLobObj, sysLobCol, sysLobIntCol, sysLobLObj, sysLobTs));
            schema->touchTable(sysLobObj);
            sysLobRet = sysLobStmt.next();
        }

        // Reading SYS.LOBCOMPPART$
        DatabaseStatement sysLobCompPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_LOB_COMP_PART_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(obj));
            }
            sysLobCompPartStmt.createStatement(SQL_GET_SYS_LOB_COMP_PART_OBJ);
            sysLobCompPartStmt.bindUInt(1, targetScn);
            sysLobCompPartStmt.bindUInt(2, targetScn);
            sysLobCompPartStmt.bindUInt(3, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_LOB_COMP_PART_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM4: " + std::to_string(user));
            }
            sysLobCompPartStmt.createStatement(SQL_GET_SYS_LOB_COMP_PART_USER);
            sysLobCompPartStmt.bindUInt(1, targetScn);
            sysLobCompPartStmt.bindUInt(2, targetScn);
            sysLobCompPartStmt.bindUInt(3, targetScn);
            sysLobCompPartStmt.bindUInt(4, user);
        }

        std::array < char, RowId::SIZE + 1 > sysLobCompPartRowidStr{};
        sysLobCompPartStmt.defineString(1, sysLobCompPartRowidStr.data(), sysLobCompPartRowidStr.size());
        typeObj sysLobCompPartPartObj;
        sysLobCompPartStmt.defineUInt(2, sysLobCompPartPartObj);
        typeObj sysLobCompPartLObj;
        sysLobCompPartStmt.defineUInt(3, sysLobCompPartLObj);

        int sysLobCompPartRet = sysLobCompPartStmt.executeQuery();
        while (sysLobCompPartRet != 0) {
            schema->sysLobCompPartPack.addWithKeys(ctx, new SysLobCompPart(RowId(sysLobCompPartRowidStr), sysLobCompPartPartObj, sysLobCompPartLObj));
            metadata->schema->touchTableLob(sysLobCompPartLObj);
            sysLobCompPartRet = sysLobCompPartStmt.next();
        }

        // Reading SYS.LOBFRAG$
        DatabaseStatement sysLobFragStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_LOB_FRAG_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM4: " + std::to_string(obj));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM5: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM6: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM7: " + std::to_string(obj));
            }
            sysLobFragStmt.createStatement(SQL_GET_SYS_LOB_FRAG_OBJ);
            sysLobFragStmt.bindUInt(1, targetScn);
            sysLobFragStmt.bindUInt(2, targetScn);
            sysLobFragStmt.bindUInt(3, targetScn);
            sysLobFragStmt.bindUInt(4, obj);
            sysLobFragStmt.bindUInt(5, targetScn);
            sysLobFragStmt.bindUInt(6, targetScn);
            sysLobFragStmt.bindUInt(7, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_LOB_FRAG_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM4: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM5: " + std::to_string(user));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM6: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM7: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM8: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM9: " + std::to_string(user));
            }
            sysLobFragStmt.createStatement(SQL_GET_SYS_LOB_FRAG_USER);
            sysLobFragStmt.bindUInt(1, targetScn);
            sysLobFragStmt.bindUInt(2, targetScn);
            sysLobFragStmt.bindUInt(3, targetScn);
            sysLobFragStmt.bindUInt(4, targetScn);
            sysLobFragStmt.bindUInt(5, user);
            sysLobFragStmt.bindUInt(6, targetScn);
            sysLobFragStmt.bindUInt(7, targetScn);
            sysLobFragStmt.bindUInt(8, targetScn);
            sysLobFragStmt.bindUInt(9, user);
        }

        std::array < char, RowId::SIZE + 1 > sysLobFragRowidStr{};
        sysLobFragStmt.defineString(1, sysLobFragRowidStr.data(), sysLobFragRowidStr.size());
        typeObj sysLobFragFragObj;
        sysLobFragStmt.defineUInt(2, sysLobFragFragObj);
        typeObj sysLobFragParentObj;
        sysLobFragStmt.defineUInt(3, sysLobFragParentObj);
        typeTs sysLobFragTs;
        sysLobFragStmt.defineUInt(4, sysLobFragTs);

        int sysLobFragRet = sysLobFragStmt.executeQuery();
        while (sysLobFragRet != 0) {
            schema->sysLobFragPack.addWithKeys(ctx, new SysLobFrag(RowId(sysLobFragRowidStr), sysLobFragFragObj, sysLobFragParentObj, sysLobFragTs));
            metadata->schema->touchTableLobFrag(sysLobFragParentObj);
            metadata->schema->touchTableLob(sysLobFragParentObj);
            sysLobFragRet = sysLobFragStmt.next();
        }

        // Reading SYS.TAB$
        DatabaseStatement sysTabStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TAB_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabStmt.createStatement(SQL_GET_SYS_TAB_OBJ);
            sysTabStmt.bindUInt(1, targetScn);
            sysTabStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TAB_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabStmt.createStatement(SQL_GET_SYS_TAB_USER);
            sysTabStmt.bindUInt(1, targetScn);
            sysTabStmt.bindUInt(2, targetScn);
            sysTabStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysTabRowidStr{};
        sysTabStmt.defineString(1, sysTabRowidStr.data(), sysTabRowidStr.size());
        typeObj sysTabObj;
        sysTabStmt.defineUInt(2, sysTabObj);
        typeDataObj sysTabDataObj = 0;
        sysTabStmt.defineUInt(3, sysTabDataObj);
        typeTs sysTabTs;
        sysTabStmt.defineUInt(4, sysTabTs);
        typeCol sysTabCluCols = 0;
        sysTabStmt.defineInt(5, sysTabCluCols);
        uint64_t sysTabFlags1;
        sysTabStmt.defineUInt(6, sysTabFlags1);
        uint64_t sysTabFlags2;
        sysTabStmt.defineUInt(7, sysTabFlags2);
        uint64_t sysTabProperty1;
        sysTabStmt.defineUInt(8, sysTabProperty1);
        uint64_t sysTabProperty2;
        sysTabStmt.defineUInt(9, sysTabProperty2);

        int sysTabRet = sysTabStmt.executeQuery();
        while (sysTabRet != 0) {
            schema->sysTabPack.addWithKeys(ctx, new SysTab(RowId(sysTabRowidStr), sysTabObj, sysTabDataObj, sysTabTs, sysTabCluCols, sysTabFlags1,
                                                           sysTabFlags2, sysTabProperty1, sysTabProperty2));
            metadata->schema->touchTable(sysTabObj);
            sysTabDataObj = 0;
            sysTabCluCols = 0;
            sysTabRet = sysTabStmt.next();
        }

        // Reading SYS.TABCOMPART$
        DatabaseStatement sysTabComPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TABCOMPART_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabComPartStmt.createStatement(SQL_GET_SYS_TABCOMPART_OBJ);
            sysTabComPartStmt.bindUInt(1, targetScn);
            sysTabComPartStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TABCOMPART_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabComPartStmt.createStatement(SQL_GET_SYS_TABCOMPART_USER);
            sysTabComPartStmt.bindUInt(1, targetScn);
            sysTabComPartStmt.bindUInt(2, targetScn);
            sysTabComPartStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysTabComPartRowidStr{};
        sysTabComPartStmt.defineString(1, sysTabComPartRowidStr.data(), sysTabComPartRowidStr.size());
        typeObj sysTabComPartObj;
        sysTabComPartStmt.defineUInt(2, sysTabComPartObj);
        typeDataObj sysTabComPartDataObj = 0;
        sysTabComPartStmt.defineUInt(3, sysTabComPartDataObj);
        typeObj sysTabComPartBo;
        sysTabComPartStmt.defineUInt(4, sysTabComPartBo);

        int sysTabComPartRet = sysTabComPartStmt.executeQuery();
        while (sysTabComPartRet != 0) {
            schema->sysTabComPartPack.addWithKeys(ctx, new SysTabComPart(RowId(sysTabComPartRowidStr), sysTabComPartObj, sysTabComPartDataObj,
                                                                         sysTabComPartBo));
            metadata->schema->touchTable(sysTabComPartBo);
            sysTabComPartDataObj = 0;
            sysTabComPartRet = sysTabComPartStmt.next();
        }

        // Reading SYS.TABPART$
        DatabaseStatement sysTabPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TABPART_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabPartStmt.createStatement(SQL_GET_SYS_TABPART_OBJ);
            sysTabPartStmt.bindUInt(1, targetScn);
            sysTabPartStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TABPART_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabPartStmt.createStatement(SQL_GET_SYS_TABPART_USER);
            sysTabPartStmt.bindUInt(1, targetScn);
            sysTabPartStmt.bindUInt(2, targetScn);
            sysTabPartStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysTabPartRowidStr{};
        sysTabPartStmt.defineString(1, sysTabPartRowidStr.data(), sysTabPartRowidStr.size());
        typeObj sysTabPartObj;
        sysTabPartStmt.defineUInt(2, sysTabPartObj);
        typeDataObj sysTabPartDataObj = 0;
        sysTabPartStmt.defineUInt(3, sysTabPartDataObj);
        typeObj sysTabPartBo;
        sysTabPartStmt.defineUInt(4, sysTabPartBo);

        int sysTabPartRet = sysTabPartStmt.executeQuery();
        while (sysTabPartRet != 0) {
            schema->sysTabPartPack.addWithKeys(ctx, new SysTabPart(RowId(sysTabPartRowidStr), sysTabPartObj, sysTabPartDataObj, sysTabPartBo));
            metadata->schema->touchTable(sysTabPartBo);
            sysTabPartDataObj = 0;
            sysTabPartRet = sysTabPartStmt.next();
        }

        // Reading SYS.TABSUBPART$
        DatabaseStatement sysTabSubPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TABSUBPART_OBJ));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabSubPartStmt.createStatement(SQL_GET_SYS_TABSUBPART_OBJ);
            sysTabSubPartStmt.bindUInt(1, targetScn);
            sysTabSubPartStmt.bindUInt(2, obj);
        } else {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_TABSUBPART_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabSubPartStmt.createStatement(SQL_GET_SYS_TABSUBPART_USER);
            sysTabSubPartStmt.bindUInt(1, targetScn);
            sysTabSubPartStmt.bindUInt(2, targetScn);
            sysTabSubPartStmt.bindUInt(3, user);
        }

        std::array < char, RowId::SIZE + 1 > sysTabSubPartRowidStr{};
        sysTabSubPartStmt.defineString(1, sysTabSubPartRowidStr.data(), sysTabSubPartRowidStr.size());
        typeObj sysTabSubPartObj;
        sysTabSubPartStmt.defineUInt(2, sysTabSubPartObj);
        typeDataObj sysTabSubPartDataObj = 0;
        sysTabSubPartStmt.defineUInt(3, sysTabSubPartDataObj);
        typeObj sysTabSubPartPobj;
        sysTabSubPartStmt.defineUInt(4, sysTabSubPartPobj);

        int sysTabSubPartRet = sysTabSubPartStmt.executeQuery();
        while (sysTabSubPartRet != 0) {
            schema->sysTabSubPartPack.addWithKeys(ctx, new SysTabSubPart(RowId(sysTabSubPartRowidStr), sysTabSubPartObj, sysTabSubPartDataObj,
                                                                         sysTabSubPartPobj));
            metadata->schema->touchTablePart(sysTabSubPartObj);
            sysTabSubPartDataObj = 0;
            sysTabSubPartRet = sysTabSubPartStmt.next();
        }

        schema->touched = true;
    }

    void ReplicatorOnline::readSystemDictionaries(Schema* schema, Scn targetScn, const std::string& owner, const std::string& table,
                                                  DbTable::OPTIONS options) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "read dictionaries for owner: " + owner + ", table: " + table + ", options: " +
                          std::to_string(static_cast<uint>(options)));

        try {
            std::string ownerRegexp("^" + owner + "$");
            std::string tableRegexp("^" + table + "$");
            const bool single = DbTable::isSystemTable(options);
            DatabaseStatement sysUserStmt(conn);

            // Reading SYS.USER$
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_USER));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + ownerRegexp);
            }
            sysUserStmt.createStatement(SQL_GET_SYS_USER);
            sysUserStmt.bindUInt(1, targetScn);
            sysUserStmt.bindString(2, ownerRegexp);
            std::array < char, RowId::SIZE + 1 > sysUserRowidStr{};
            sysUserStmt.defineString(1, sysUserRowidStr.data(), sysUserRowidStr.size());
            typeUser sysUserUser;
            sysUserStmt.defineUInt(2, sysUserUser);
            std::array < char, SysUser::NAME_LENGTH + 1 > sysUserName{};
            sysUserStmt.defineString(3, sysUserName.data(), sysUserName.size());
            uint64_t sysUserSpare11 = 0;
            sysUserStmt.defineUInt(4, sysUserSpare11);
            uint64_t sysUserSpare12 = 0;
            sysUserStmt.defineUInt(5, sysUserSpare12);

            int sysUserRet = sysUserStmt.executeQuery();
            while (sysUserRet != 0) {
                const RowId sysUserRowid(sysUserRowidStr);
                auto sysUserMapRowIdIt = schema->sysUserPack.mapRowId.find(sysUserRowid);
                if (sysUserMapRowIdIt != schema->sysUserPack.mapRowId.end()) {
                    if (!single) {
                        SysUser* sysUser = sysUserMapRowIdIt->second;
                        if (sysUser->single) {
                            sysUser->single = false;
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                                ctx->logTrace(Ctx::TRACE::SYSTEM, "disabling single option for user " + std::string(sysUserName.data()) + " (" +
                                              std::to_string(sysUserUser) + ")");
                        } else {
                            sysUserSpare11 = 0;
                            sysUserSpare12 = 0;
                            sysUserRet = sysUserStmt.next();
                            continue;
                        }
                    }
                } else
                    schema->sysUserPack.addWithKeys(ctx, new SysUser(sysUserRowid, sysUserUser, sysUserName.data(), sysUserSpare11, sysUserSpare12, single));

                DatabaseStatement sysObjStmt(conn);
                // Reading SYS.OBJ$
                if (!single) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                        ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_OBJ_USER));
                        ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                        ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(sysUserUser));
                    }
                    sysObjStmt.createStatement(SQL_GET_SYS_OBJ_USER);
                    sysObjStmt.bindUInt(1, targetScn);
                    sysObjStmt.bindUInt(2, sysUserUser);
                } else {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                        ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_SYS_OBJ_NAME));
                        ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + targetScn.toString());
                        ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(sysUserUser));
                        ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + table);
                    }
                    sysObjStmt.createStatement(SQL_GET_SYS_OBJ_NAME);
                    sysObjStmt.bindUInt(1, targetScn);
                    sysObjStmt.bindUInt(2, sysUserUser);
                    sysObjStmt.bindString(3, tableRegexp);
                }

                std::array < char, RowId::SIZE + 1 > sysObjRowidStr{};
                sysObjStmt.defineString(1, sysObjRowidStr.data(), sysObjRowidStr.size());
                typeUser sysObjOwner;
                sysObjStmt.defineUInt(2, sysObjOwner);
                typeObj sysObjObj;
                sysObjStmt.defineUInt(3, sysObjObj);
                typeDataObj sysObjDataObj = 0;
                sysObjStmt.defineUInt(4, sysObjDataObj);
                std::array < char, SysObj::NAME_LENGTH + 1 > sysObjName{};
                sysObjStmt.defineString(5, sysObjName.data(), sysObjName.size());
                uint sysObjType = 0;
                sysObjStmt.defineUInt(6, sysObjType);
                uint64_t sysObjFlags1;
                sysObjStmt.defineUInt(7, sysObjFlags1);
                uint64_t sysObjFlags2;
                sysObjStmt.defineUInt(8, sysObjFlags2);

                int sysObjRet = sysObjStmt.executeQuery();
                while (sysObjRet != 0) {
                    const RowId sysObjRowId(sysObjRowidStr);
                    auto sysObjMapRowIdIt = schema->sysObjPack.mapRowId.find(sysObjRowId);
                    if (sysObjMapRowIdIt != schema->sysObjPack.mapRowId.end()) {
                        SysObj* sysObj = sysObjMapRowIdIt->second;
                        if (sysObj->single && !single) {
                            sysObj->single = false;
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                                ctx->logTrace(Ctx::TRACE::SYSTEM, "disabling single option for object " + std::string(sysObjName.data()) + " (owner " +
                                              std::to_string(sysObjOwner) + ")");
                        }

                        sysObjDataObj = 0;
                        sysObjFlags1 = 0;
                        sysObjFlags2 = 0;
                        sysObjRet = sysObjStmt.next();
                        continue;
                    }

                    schema->sysObjPack.addWithKeys(ctx, new SysObj(sysObjRowId, sysObjOwner, sysObjObj, sysObjDataObj, static_cast<SysObj::OBJTYPE>(sysObjType),
                                                                   sysObjName.data(), sysObjFlags1, sysObjFlags2, single));
                    schema->touchTable(sysObjObj);

                    if (single)
                        readSystemDictionariesDetails(schema, targetScn, sysUserUser, sysObjObj);

                    sysObjDataObj = 0;
                    sysObjFlags1 = 0;
                    sysObjFlags2 = 0;
                    sysObjRet = sysObjStmt.next();
                }

                if (!single)
                    readSystemDictionariesDetails(schema, targetScn, sysUserUser, 0);

                sysUserSpare11 = 0;
                sysUserSpare12 = 0;
                sysUserRet = sysUserStmt.next();
            }
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
            throw BootException(10035, "can't read schema from flashback, provide a valid starting SCN value");
        } catch (RuntimeException& ex) {
            if (ex.supCode == 8181) {
                throw BootException(10035, "can't read schema from flashback, provide a valid starting SCN value");
            }
            throw BootException(ex.code, ex.msg);
        }
    }

    void ReplicatorOnline::createSchemaForTable(Scn targetScn, const std::string& owner, const std::string& table, const std::vector<std::string>& keyList,
                                                const std::string& key, SchemaElement::TAG_TYPE tagType, const std::vector<std::string>& tagList,
                                                const std::string& tag,
                                                const std::string& condition, DbTable::OPTIONS options,
                                                std::unordered_map<typeObj, std::string>& tablesUpdated) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "creating table schema for owner: " + owner + " table: " + table + " options: " +
                          std::to_string(static_cast<uint>(options)));

        readSystemDictionaries(metadata->schema, targetScn, owner, table, options);

        metadata->schema->buildMaps(owner, table, keyList, key, tagType, tagList, tag, condition, options, tablesUpdated, metadata->suppLogDbPrimary,
                                    metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                    metadata->defaultCharacterNcharMapId);
    }

    void ReplicatorOnline::updateOnlineRedoLogData() {
        if (!checkConnection())
            return;

        contextSet(CONTEXT::CHKPT, REASON::CHKPT);
        std::unique_lock const lck(metadata->mtxCheckpoint);

        // Reload incarnation ctx
        typeResetlogs const oldResetlogs = metadata->resetlogs;
        for (DbIncarnation* oi: metadata->dbIncarnations)
            delete oi;
        metadata->dbIncarnations.clear();
        metadata->dbIncarnationCurrent = nullptr;

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_DATABASE_ROLE));
            stmt.createStatement(SQL_GET_DATABASE_ROLE);
            std::array < char, 129 > databaseRole{};
            stmt.defineString(1, databaseRole.data(), databaseRole.size());

            if (stmt.executeQuery() != 0) {
                const std::string roleStr(databaseRole.data());
                if (roleStr == "PRIMARY") {
                    if (standby) {
                        standby = false;
                        ctx->info(0, "changed database role to: " + roleStr);
                    }
                } else if (roleStr == "PHYSICAL STANDBY") {
                    if (!standby) {
                        standby = true;
                        ctx->info(0, "changed database role to: " + roleStr);
                    }
                } else {
                    throw RuntimeException(10038, "unknown database role: " + roleStr);
                }
            }
        }

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_DATABASE_INCARNATION));
            stmt.createStatement(SQL_GET_DATABASE_INCARNATION);
            uint32_t incarnation;
            stmt.defineUInt(1, incarnation);
            Scn resetlogsScn;
            stmt.defineUInt(2, resetlogsScn);
            Scn priorResetlogsScn;
            stmt.defineUInt(3, priorResetlogsScn);
            std::array < char, 129 > status{};
            stmt.defineString(4, status.data(), status.size());
            typeResetlogs resetlogs;
            stmt.defineUInt(5, resetlogs);
            uint32_t priorIncarnation;
            stmt.defineUInt(6, priorIncarnation);

            int ret = stmt.executeQuery();
            while (ret != 0) {
                auto* oi = new DbIncarnation(incarnation, resetlogsScn, priorResetlogsScn, status.data(),
                                             resetlogs, priorIncarnation);
                metadata->dbIncarnations.insert(oi);

                // Search prev value
                if (oldResetlogs != 0 && oi->resetlogs == oldResetlogs) {
                    metadata->dbIncarnationCurrent = oi;
                } else if (oi->current && metadata->dbIncarnationCurrent == nullptr) {
                    metadata->dbIncarnationCurrent = oi;
                    metadata->setResetlogs(oi->resetlogs);
                }

                ret = stmt.next();
            }
        }

        // Reload online redo log ctx
        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_LOGFILE_LIST));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + std::to_string(standby ? 1 : 0));
            }
            stmt.createStatement(SQL_GET_LOGFILE_LIST);
            std::string standbyStr{"STANDBY"};
            std::string onlineStr{"ONLINE"};
            if (standby)
                stmt.bindString(1, standbyStr);
            else
                stmt.bindString(1, onlineStr);

            int group = -1;
            stmt.defineInt(1, group);
            std::array < char, 513 > pathStr{};
            stmt.defineString(2, pathStr.data(), pathStr.size());
            Reader* onlineReader = nullptr;
            int lastGroup = -1;

            int ret = stmt.executeQuery();
            while (ret != 0) {
                if (group != lastGroup || onlineReader == nullptr) {
                    onlineReader = readerCreate(group);
                    onlineReader->paths.clear();
                    lastGroup = group;
                }
                const std::string path(pathStr.data());
                onlineReader->paths.push_back(path);
                auto* redoLog = new RedoLog(group, pathStr.data());
                metadata->redoLogs.insert(redoLog);

                ret = stmt.next();
            }

            if (readers.empty()) {
                if (standby)
                    throw RuntimeException(10036, "failed to find standby redo log files");
                throw RuntimeException(10037, "failed to find online redo log files");
            }
        }
        checkOnlineRedoLogs();
        contextSet(CONTEXT::CPU);
    }

    void ReplicatorOnline::archGetLogOnline(Replicator* replicator) {
        auto* replicatorOnline = dynamic_cast<ReplicatorOnline*>(replicator);
        if (replicatorOnline == nullptr)
            return;

        if (!replicatorOnline->checkConnection())
            return;

        {
            DatabaseStatement stmt(replicatorOnline->conn);
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::SQL))) {
                replicator->ctx->logTrace(Ctx::TRACE::SQL, std::string(SQL_GET_ARCHIVE_LOG_LIST));
                replicator->ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " +
                                          replicatorOnline->metadata->sequence.toString());
                replicator->ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(replicator->metadata->resetlogs));
            }

            stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
            stmt.bindUInt(1, replicator->metadata->sequence);
            stmt.bindUInt(2, replicator->metadata->resetlogs);

            std::array < char, 513 > path{};
            stmt.defineString(1, path.data(), path.size());
            Seq sequence;
            stmt.defineUInt(2, sequence);
            Scn firstScn;
            stmt.defineUInt(3, firstScn);
            Scn nextScn;
            stmt.defineUInt(4, nextScn);

            int ret = stmt.executeQuery();
            while (ret != 0) {
                std::string mappedPath(path.data());
                replicator->applyMapping(mappedPath);

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = firstScn;
                parser->nextScn = nextScn;
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
                ret = stmt.next();
            }
        }
        replicator->goStandby();
    }

    std::string ReplicatorOnline::getModeName() const {
        return {"online"};
    }
}
