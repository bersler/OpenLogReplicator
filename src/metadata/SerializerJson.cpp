/* Base class for serialization of metadata to json
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

#include "../common/Ctx.h"
#include "../common/DbIncarnation.h"
#include "../common/DbTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/DataException.h"
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
#include "../common/types/RowId.h"
#include "RedoLog.h"
#include "Metadata.h"
#include "Schema.h"
#include "SchemaElement.h"
#include "SerializerJson.h"

namespace OpenLogReplicator {
    void SerializerJson::serialize(Metadata* metadata, std::ostringstream& ss, bool storeSchema) {
        // Assuming the caller holds all locks
        ss << R"({"database":")";
        Data::writeEscapeValue(ss, metadata->database);
        ss << R"(","scn":)" << metadata->checkpointScn.toString() <<
                R"(,"resetlogs":)" << std::dec << metadata->resetlogs <<
                R"(,"activation":)" << std::dec << metadata->activation <<
                R"(,"time":)" << std::dec << metadata->checkpointTime.getVal() << // not read
                R"(,"seq":)" << metadata->checkpointSequence.toString() <<
                R"(,"offset":)" << metadata->checkpointFileOffset.toString();
        if (metadata->minSequence != Seq::none()) {
            ss << R"(,"min-tran":{)" <<
                    R"("seq":)" << metadata->minSequence.toString() <<
                    R"(,"offset":)" << metadata->minFileOffset.toString() <<
                    R"(,"xid":")" << metadata->minXid.toString() << R"("})";
        }
        ss << R"(,"big-endian":)" << std::dec << (metadata->ctx->isBigEndian() ? 1 : 0) <<
                R"(,"context":")";
        Data::writeEscapeValue(ss, metadata->context);
        ss << R"(","db-id":)" << std::dec << metadata->dbId <<
                R"(,"con-id":)" << std::dec << metadata->conId <<
                R"(,"con-name":")";
        Data::writeEscapeValue(ss, metadata->conName);
        ss << R"(","db-timezone":")";
        Data::writeEscapeValue(ss, metadata->dbTimezoneStr);
        ss << R"(","db-recovery-file-dest":")";
        Data::writeEscapeValue(ss, metadata->dbRecoveryFileDest);
        ss << R"(",)" << R"("db-block-checksum":")";
        Data::writeEscapeValue(ss, metadata->dbBlockChecksum);
        ss << R"(",)" << R"("log-archive-dest":")";
        Data::writeEscapeValue(ss, metadata->logArchiveDest);
        ss << R"(",)" << R"("log-archive-format":")";
        Data::writeEscapeValue(ss, metadata->logArchiveFormat);
        ss << R"(",)" << R"("nls-character-set":")";
        Data::writeEscapeValue(ss, metadata->nlsCharacterSet);
        ss << R"(",)" << R"("nls-nchar-character-set":")";
        Data::writeEscapeValue(ss, metadata->nlsNcharCharacterSet);

        ss << R"(","supp-log-db-primary":)" << (metadata->suppLogDbPrimary ? 1 : 0) <<
                R"(,"supp-log-db-all":)" << (metadata->suppLogDbAll ? 1 : 0) <<
                R"(,)"
        SERIALIZER_ENDL << R"("online-redo":[)";

        int64_t prevGroup = -2;
        for (const RedoLog* redoLog: metadata->redoLogs) {
            if (redoLog->group == 0)
                continue;

            if (prevGroup == -2)
                ss SERIALIZER_ENDL
            <<
            R"({"group":)" << redoLog->group << R"(,"path":[)";
            else
            if (prevGroup != redoLog->group)
                ss << "]},"
            SERIALIZER_ENDL << R"({"group":)" << redoLog->group << R"(,"path":[)";
            else
            ss << ",";

            ss << R"(")";
            Data::writeEscapeValue(ss, redoLog->path);
            ss << R"(")";

            prevGroup = redoLog->group;
        }
        if (prevGroup > 0)
            ss << "]}";

        ss << "],"
        SERIALIZER_ENDL << R"("incarnations":[)";
        bool hasPrev = false;
        for (const DbIncarnation* oi: metadata->dbIncarnations) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;
            ss SERIALIZER_ENDL
            <<
            R"({"incarnation":)" << oi->incarnation <<
                    R"(,"resetlogs-scn":)" << oi->resetlogsScn.toString() <<
                    R"(,"prior-resetlogs-scn":)" << oi->priorResetlogsScn.toString() <<
                    R"(,"status":")";
            Data::writeEscapeValue(ss, oi->status);
            ss << R"(","resetlogs":)" << oi->resetlogs <<
                    R"(,"prior-incarnation":)" << oi->priorIncarnation << "}";
        }

        ss << "],"
        SERIALIZER_ENDL << R"("users":[)";
        hasPrev = false;
        for (const std::string& user: metadata->users) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;
            ss SERIALIZER_ENDL
            <<
            R"(")" << user << R"(")";
        }

        ss << "],"
        SERIALIZER_ENDL;

        // The schema has not changed since the last checkpoint file
        if (!storeSchema) {
            ss << R"("schema-ref-scn":)" << metadata->schema->refScn.toString() << "}";
            return;
        }

        metadata->schema->refScn = metadata->checkpointScn;
        ss << R"("schema-scn":)" << metadata->schema->scn.toString() << ","
        SERIALIZER_ENDL;

        // SYS.CCOL$
        ss << R"("sys-ccol":[)";
        hasPrev = false;
        for (const auto& [_,sysCCol]: metadata->schema->sysCColPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysCCol->rowId <<
                    R"(","con":)" << std::dec << sysCCol->con <<
                    R"(,"int-col":)" << std::dec << sysCCol->intCol <<
                    R"(,"obj":)" << std::dec << sysCCol->obj <<
                    R"(,"spare1":)" << std::dec << sysCCol->spare1.toString() << "}";
        }

        // SYS.CDEF$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-cdef":[)";
        hasPrev = false;
        for (const auto& [_, sysCDef]: metadata->schema->sysCDefPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysCDef->rowId <<
                    R"(","con":)" << std::dec << sysCDef->con <<
                    R"(,"obj":)" << std::dec << sysCDef->obj <<
                    R"(,"type":)" << std::dec << static_cast<uint>(sysCDef->type) << "}";
        }

        // SYS.COL$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-col":[)";
        hasPrev = false;
        for (const auto& [_, sysCol]: metadata->schema->sysColPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysCol->rowId <<
                    R"(","obj":)" << std::dec << sysCol->obj <<
                    R"(,"col":)" << std::dec << sysCol->col <<
                    R"(,"seg-col":)" << std::dec << sysCol->segCol <<
                    R"(,"int-col":)" << std::dec << sysCol->intCol <<
                    R"(,"name":")";
            Data::writeEscapeValue(ss, sysCol->name);
            ss << R"(","type":)" << std::dec << static_cast<uint>(sysCol->type) <<
                    R"(,"length":)" << std::dec << sysCol->length <<
                    R"(,"precision":)" << std::dec << sysCol->precision <<
                    R"(,"scale":)" << std::dec << sysCol->scale <<
                    R"(,"charset-form":)" << std::dec << sysCol->charsetForm <<
                    R"(,"charset-id":)" << std::dec << sysCol->charsetId <<
                    R"(,"null":)" << std::dec << sysCol->null_ <<
                    R"(,"property":)" << sysCol->property.toString() << "}";
        }

        // SYS.DEFERRED_STG$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-deferredstg":[)";
        hasPrev = false;
        for (const auto& [_, sysDeferredStg]: metadata->schema->sysDeferredStgPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysDeferredStg->rowId <<
                    R"(","obj":)" << std::dec << sysDeferredStg->obj <<
                    R"(,"flags-stg":)" << std::dec << sysDeferredStg->flagsStg.toString() << "}";
        }

        // SYS.ECOL$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-ecol":[)";
        hasPrev = false;
        for (const auto& [_, sysECol]: metadata->schema->sysEColPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysECol->rowId <<
                    R"(","tab-obj":)" << std::dec << sysECol->tabObj <<
                    R"(,"col-num":)" << std::dec << sysECol->colNum <<
                    R"(,"guard-id":)" << std::dec << sysECol->guardId << "}";
        }

        // SYS.LOB$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-lob":[)";
        hasPrev = false;
        for (const auto& [_, sysLob]: metadata->schema->sysLobPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysLob->rowId <<
                    R"(","obj":)" << std::dec << sysLob->obj <<
                    R"(,"col":)" << std::dec << sysLob->col <<
                    R"(,"int-col":)" << std::dec << sysLob->intCol <<
                    R"(,"l-obj":)" << std::dec << sysLob->lObj <<
                    R"(,"ts":)" << std::dec << sysLob->ts << "}";
        }

        // SYS.LOBCOMPPART$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-lob-comp-part":[)";
        hasPrev = false;
        for (const auto& [_, sysLobCompPart]: metadata->schema->sysLobCompPartPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysLobCompPart->rowId <<
                    R"(","part-obj":)" << std::dec << sysLobCompPart->partObj <<
                    R"(,"l-obj":)" << std::dec << sysLobCompPart->lObj << "}";
        }

        // SYS.LOBFRAG$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-lob-frag":[)";
        hasPrev = false;
        for (const auto& [_, sysLobFrag]: metadata->schema->sysLobFragPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysLobFrag->rowId <<
                    R"(","frag-obj":)" << std::dec << sysLobFrag->fragObj <<
                    R"(,"parent-obj":)" << std::dec << sysLobFrag->parentObj <<
                    R"(,"ts":)" << std::dec << sysLobFrag->ts << "}";
        }

        // SYS.OBJ$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-obj":[)";
        hasPrev = false;
        for (const auto& [_, sysObj]: metadata->schema->sysObjPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysObj->rowId <<
                    R"(","owner":)" << std::dec << sysObj->owner <<
                    R"(,"obj":)" << std::dec << sysObj->obj <<
                    R"(,"data-obj":)" << std::dec << sysObj->dataObj <<
                    R"(,"name":")";
            Data::writeEscapeValue(ss, sysObj->name);
            ss << R"(","type":)" << std::dec << static_cast<uint>(sysObj->type) <<
                    R"(,"flags":)" << std::dec << sysObj->flags.toString() <<
                    R"(,"single":)" << std::dec << static_cast<uint>(sysObj->single) << "}";
        }

        // SYS.TAB$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-tab":[)";
        hasPrev = false;
        for (const auto& [_, sysTab]: metadata->schema->sysTabPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysTab->rowId <<
                    R"(","obj":)" << std::dec << sysTab->obj <<
                    R"(,"data-obj":)" << std::dec << sysTab->dataObj <<
                    R"(,"ts":)" << std::dec << sysTab->ts <<
                    R"(,"clu-cols":)" << std::dec << sysTab->cluCols <<
                    R"(,"flags":)" << std::dec << sysTab->flags.toString() <<
                    R"(,"property":)" << std::dec << sysTab->property.toString() << "}";
        }

        // SYS.TABCOMPART$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-tabcompart":[)";
        hasPrev = false;
        for (const auto& [_, sysTabComPart]: metadata->schema->sysTabComPartPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysTabComPart->rowId <<
                    R"(","obj":)" << std::dec << sysTabComPart->obj <<
                    R"(,"data-obj":)" << std::dec << sysTabComPart->dataObj <<
                    R"(,"bo":)" << std::dec << sysTabComPart->bo << "}";
        }

        // SYS.TABPART$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-tabpart":[)";
        hasPrev = false;
        for (const auto& [_, sysTabPart]: metadata->schema->sysTabPartPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysTabPart->rowId <<
                    R"(","obj":)" << std::dec << sysTabPart->obj <<
                    R"(,"data-obj":)" << std::dec << sysTabPart->dataObj <<
                    R"(,"bo":)" << std::dec << sysTabPart->bo << "}";
        }

        // SYS.TABSUBPART$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-tabsubpart":[)";
        hasPrev = false;
        for (const auto& [_, sysTabSubPart]: metadata->schema->sysTabSubPartPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysTabSubPart->rowId <<
                    R"(","obj":)" << std::dec << sysTabSubPart->obj <<
                    R"(,"data-obj":)" << std::dec << sysTabSubPart->dataObj <<
                    R"(,"p-obj":)" << std::dec << sysTabSubPart->pObj << "}";
        }

        // SYS.TS$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-ts":[)";
        hasPrev = false;
        for (const auto& [_, sysTs]: metadata->schema->sysTsPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysTs->rowId <<
                    R"(","ts":)" << std::dec << sysTs->ts <<
                    R"(,"name":")";
            Data::writeEscapeValue(ss, sysTs->name);
            ss << R"(","block-size":)" << std::dec << sysTs->blockSize << "}";
        }

        // SYS.USER$
        ss << "],"
        SERIALIZER_ENDL << R"("sys-user":[)";
        hasPrev = false;
        for (const auto& [_, sysUser]: metadata->schema->sysUserPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << sysUser->rowId <<
                    R"(","user":)" << std::dec << sysUser->user <<
                    R"(,"name":")";
            Data::writeEscapeValue(ss, sysUser->name);
            ss << R"(","spare1":)" << std::dec << sysUser->spare1.toString() <<
                    R"(,"single":)" << std::dec << static_cast<uint>(sysUser->single) << "}";
        }

        // XDB.XDB$TTSET
        ss << "],"
        SERIALIZER_ENDL << R"("xdb-ttset":[)";
        hasPrev = false;
        for (const auto& [_, xdbTtSet]: metadata->schema->xdbTtSetPack.mapRowId) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL
            <<
            R"({"row-id":")" << xdbTtSet->rowId <<
                    R"(","guid":")" << std::dec << xdbTtSet->guid <<
                    R"(","toksuf":")";
            Data::writeEscapeValue(ss, xdbTtSet->tokSuf);
            ss << R"(","flags":)" << std::dec << xdbTtSet->flags <<
                    R"(,"obj":)" << std::dec << xdbTtSet->obj << "}";
        }

        for (const auto& [_, xmlCtx]: metadata->schema->schemaXmlMap) {
            // XDB.X$NMxxx
            ss << "],"
            SERIALIZER_ENDL << R"("xdb-xnm)" << xmlCtx->tokSuf << R"(":[)";
            hasPrev = false;
            for (const auto& [_2, xdbXNm]: xmlCtx->xdbXNmPack.mapRowId) {
                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                ss SERIALIZER_ENDL
                <<
                R"({"row-id":")" << xdbXNm->rowId <<
                        R"(","nmspcuri":")";
                Data::writeEscapeValue(ss, xdbXNm->nmSpcUri);
                ss << R"(","id":")" << xdbXNm->id << R"("})";
            }

            // XDB.X$PTxxx
            ss << "],"
            SERIALIZER_ENDL << R"("xdb-xpt)" << xmlCtx->tokSuf << R"(":[)";
            hasPrev = false;
            for (const auto& [_2, xdbXPt]: xmlCtx->xdbXPtPack.mapRowId) {
                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                ss SERIALIZER_ENDL
                <<
                R"({"row-id":")" << xdbXPt->rowId <<
                        R"(","path":")";
                Data::writeEscapeValue(ss, xdbXPt->path);
                ss << R"(","id":")" << xdbXPt->id << R"("})";
            }

            // XDB.X$QNxxx
            ss << "],"
            SERIALIZER_ENDL << R"("xdb-xqn)" << xmlCtx->tokSuf << R"(":[)";
            hasPrev = false;
            for (const auto& [_2, xdbXQn]: xmlCtx->xdbXQnPack.mapRowId) {
                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                ss SERIALIZER_ENDL
                <<
                R"({"row-id":")" << xdbXQn->rowId <<
                        R"(","nmspcid":")";
                Data::writeEscapeValue(ss, xdbXQn->nmSpcId);
                ss << R"(","localname":")";
                Data::writeEscapeValue(ss, xdbXQn->localName);
                ss << R"(","flags":")";
                Data::writeEscapeValue(ss, xdbXQn->flags);
                ss << R"(","id":")" << xdbXQn->id << R"("})";
            }
        }

        ss << "]}";
    }

    bool SerializerJson::deserialize(Metadata* metadata, const std::string& ss, const std::string& fileName, std::vector<std::string>& msgs,
                                     std::unordered_map<typeObj, std::string>& tablesUpdated, bool loadMetadata, bool loadSchema) {
        try {
            rapidjson::Document document;
            if (unlikely(ss.empty() || document.Parse(ss.c_str()).HasParseError()))
                throw DataException(20001, "file: " + fileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                    " - parse error: " + GetParseError_En(document.GetParseError()));


            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> documentChildNames{
                    "activation",
                    "big-endian",
                    "con-id",
                    "con-name",
                    "context",
                    "database",
                    "db-block-checksum",
                    "db-id",
                    "db-recovery-file-dest",
                    "db-timezone",
                    "incarnations",
                    "log-archive-dest",
                    "log-archive-format",
                    "min-tran",
                    "nls-character-set",
                    "nls-nchar-character-set",
                    "offset",
                    "online-redo",
                    "resetlogs",
                    "schema-ref-scn",
                    "schema-scn",
                    "scn",
                    "seq",
                    "supp-log-db-all",
                    "supp-log-db-primary",
                    "sys-ccol",
                    "sys-cdef",
                    "sys-col",
                    "sys-deferredstg",
                    "sys-ecol",
                    "sys-lob",
                    "sys-lob-comp-part",
                    "sys-lob-frag",
                    "sys-obj",
                    "sys-tab",
                    "sys-tabcompart",
                    "sys-tabpart",
                    "sys-tabsubpart",
                    "sys-ts",
                    "sys-user",
                    "time",
                    "users",
                    "xdb-ttset"
                };
                Ctx::checkJsonFields(fileName, document, documentChildNames);
            }
            std::unique_lock const lckCheckpoint(metadata->mtxCheckpoint);
            std::unique_lock const lckSchema(metadata->mtxSchema);

            if (loadMetadata) {
                metadata->checkpointScn = Ctx::getJsonFieldU64(fileName, document, "scn");

                if (document.HasMember("min-tran")) {
                    const rapidjson::Value& minTranJson = Ctx::getJsonFieldO(fileName, document, "min-tran");
                    if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                        static const std::vector<std::string> minTranJsonChildNames{
                            "offset",
                            "seq",
                            "xid"
                        };
                        Ctx::checkJsonFields(fileName, minTranJson, minTranJsonChildNames);
                    }

                    metadata->sequence = Ctx::getJsonFieldU32(fileName, minTranJson, "seq");
                    metadata->fileOffset = FileOffset(Ctx::getJsonFieldU64(fileName, minTranJson, "offset"));
                } else {
                    metadata->sequence = Ctx::getJsonFieldU32(fileName, document, "seq");
                    metadata->fileOffset = FileOffset(Ctx::getJsonFieldU64(fileName, document, "offset"));
                }

                if (unlikely(!metadata->fileOffset.matchesBlockSize(Ctx::MIN_BLOCK_SIZE)))
                    throw DataException(20006, "file: " + fileName + " - invalid offset: " + metadata->fileOffset.toString() +
                                        " is not a multiplication of " + std::to_string(Ctx::MIN_BLOCK_SIZE));

                metadata->minSequence = Seq::none();
                metadata->minFileOffset = FileOffset::zero();
                metadata->minXid = Xid::zero();
                metadata->lastCheckpointScn = Scn::none();
                metadata->lastSequence = Seq::none();
                metadata->lastCheckpointFileOffset = FileOffset::zero();
                metadata->lastCheckpointTime = 0;
                metadata->lastCheckpointBytes = 0;

                if (!metadata->onlineData) {
                    // Database metadata
                    const std::string newDatabase = Ctx::getJsonFieldS(fileName, Ctx::JSON_PARAMETER_LENGTH, document, "database");
                    if (metadata->database.empty()) {
                        metadata->database = newDatabase;
                    } else if (metadata->database != newDatabase) {
                        throw DataException(20001, "file: " + fileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                            " - parse error of field \"database\", invalid value: " + newDatabase + ", expected value: " +
                                            metadata->database);
                    }
                    metadata->resetlogs = Ctx::getJsonFieldU32(fileName, document, "resetlogs");
                    metadata->activation = Ctx::getJsonFieldU32(fileName, document, "activation");
                    const int bigEndian = Ctx::getJsonFieldI(fileName, document, "big-endian");
                    if (bigEndian == 1)
                        metadata->ctx->setBigEndian();
                    metadata->context = Ctx::getJsonFieldS(fileName, DbTable::VCONTEXT_LENGTH, document, "context");
                    metadata->dbId = Ctx::getJsonFieldU32(fileName, document, "db-id");
                    metadata->conId = Ctx::getJsonFieldI16(fileName, document, "con-id");
                    metadata->conName = Ctx::getJsonFieldS(fileName, DbTable::VCONTEXT_LENGTH, document, "con-name");
                    if (document.HasMember("db-timezone"))
                        metadata->dbTimezoneStr = Ctx::getJsonFieldS(fileName, DbTable::VCONTEXT_LENGTH, document, "db-timezone");
                    else
                        metadata->dbTimezoneStr = "+00:00";
                    if (metadata->ctx->dbTimezone != Ctx::BAD_TIMEZONE) {
                        metadata->dbTimezone = metadata->ctx->dbTimezone;
                    } else {
                        if (unlikely(!Data::parseTimezone(metadata->dbTimezoneStr, metadata->dbTimezone)))
                            throw DataException(20001, "file: " + fileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                                " - parse error of field \"db-timezone\", invalid value: " + metadata->dbTimezoneStr);
                    }
                    metadata->dbRecoveryFileDest = Ctx::getJsonFieldS(fileName, DbTable::VPARAMETER_LENGTH, document, "db-recovery-file-dest");
                    metadata->dbBlockChecksum = Ctx::getJsonFieldS(fileName, DbTable::VPARAMETER_LENGTH, document, "db-block-checksum");
                    if (!metadata->logArchiveFormatCustom)
                        metadata->logArchiveFormat = Ctx::getJsonFieldS(fileName, DbTable::VPARAMETER_LENGTH, document, "log-archive-format");
                    metadata->logArchiveDest = Ctx::getJsonFieldS(fileName, DbTable::VPARAMETER_LENGTH, document, "log-archive-dest");
                    metadata->nlsCharacterSet = Ctx::getJsonFieldS(fileName, DbTable::VPROPERTY_LENGTH, document, "nls-character-set");
                    metadata->nlsNcharCharacterSet = Ctx::getJsonFieldS(fileName, DbTable::VPROPERTY_LENGTH, document,
                                                                        "nls-nchar-character-set");
                    metadata->setNlsCharset(metadata->nlsCharacterSet, metadata->nlsNcharCharacterSet);
                    metadata->suppLogDbPrimary = Ctx::getJsonFieldU64(fileName, document, "supp-log-db-primary") != 0;
                    metadata->suppLogDbAll = Ctx::getJsonFieldU64(fileName, document, "supp-log-db-all") != 0;

                    const rapidjson::Value& onlineRedoJson = Ctx::getJsonFieldA(fileName, document, "online-redo");
                    for (rapidjson::SizeType i = 0; i < onlineRedoJson.Size(); ++i) {
                        if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                            static const std::vector<std::string> onlineRedoChildNames{
                                "group",
                                "path"
                            };
                            Ctx::checkJsonFields(fileName, onlineRedoJson[i], onlineRedoChildNames);
                        }

                        const int group = Ctx::getJsonFieldI(fileName, onlineRedoJson[i], "group");
                        const rapidjson::Value& path = Ctx::getJsonFieldA(fileName, onlineRedoJson[i], "path");

                        for (rapidjson::SizeType j = 0; j < path.Size(); ++j) {
                            const rapidjson::Value& pathVal = path[j];
                            auto* redoLog = new RedoLog(group, pathVal.GetString());
                            metadata->redoLogs.insert(redoLog);
                        }
                    }

                    const rapidjson::Value& incarnationsJson = Ctx::getJsonFieldA(fileName, document, "incarnations");
                    for (rapidjson::SizeType i = 0; i < incarnationsJson.Size(); ++i) {
                        if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                            static const std::vector<std::string> incarnationsChildNames{
                                "incarnation",
                                "prior-incarnation",
                                "prior-resetlogs-scn",
                                "resetlogs",
                                "resetlogs-scn",
                                "status"
                            };
                            Ctx::checkJsonFields(fileName, incarnationsJson[i], incarnationsChildNames);
                        }

                        const uint32_t incarnation = Ctx::getJsonFieldU32(fileName, incarnationsJson[i], "incarnation");
                        const auto resetlogsScn = Scn(Ctx::getJsonFieldU64(fileName, incarnationsJson[i], "resetlogs-scn"));
                        const auto priorResetlogsScn = Scn(Ctx::getJsonFieldU64(fileName, incarnationsJson[i], "prior-resetlogs-scn"));
                        const std::string status = Ctx::getJsonFieldS(fileName, 128, incarnationsJson[i], "status");
                        const typeResetlogs resetlogs = Ctx::getJsonFieldU32(fileName, incarnationsJson[i], "resetlogs");
                        const uint32_t priorIncarnation = Ctx::getJsonFieldU32(fileName, incarnationsJson[i], "prior-incarnation");

                        auto* oi = new DbIncarnation(incarnation, resetlogsScn, priorResetlogsScn, status, resetlogs, priorIncarnation);
                        metadata->dbIncarnations.insert(oi);

                        if (oi->current)
                            metadata->dbIncarnationCurrent = oi;
                        else
                            metadata->dbIncarnationCurrent = nullptr;
                    }
                }

                if (!metadata->ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA)) {
                    std::set<std::string> users;
                    const rapidjson::Value& usersJson = Ctx::getJsonFieldA(fileName, document, "users");
                    for (rapidjson::SizeType i = 0; i < usersJson.Size(); ++i) {
                        const rapidjson::Value& userJson = usersJson[i];
                        users.insert(userJson.GetString());
                    }

                    for (const auto& user: metadata->users) {
                        if (unlikely(users.find(user) == users.end()))
                            throw DataException(20007, "file: " + fileName + " - " + user + " is missing");
                    }
                    for (const auto& user: users) {
                        if (unlikely(metadata->users.find(user) == metadata->users.end()))
                            throw DataException(20007, "file: " + fileName + " - " + user + " is redundant");
                    }
                    users.clear();
                }
            }

            if (loadSchema) {
                // Schema referenced to other checkpoint file
                if (document.HasMember("schema-ref-scn")) {
                    metadata->schema->scn = Scn::none();
                    metadata->schema->refScn = Ctx::getJsonFieldU64(fileName, document, "schema-ref-scn");
                } else {
                    metadata->schema->scn = Ctx::getJsonFieldU64(fileName, document, "schema-scn");
                    metadata->schema->refScn = Scn::none();

                    deserializeSysUser(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-user"));
                    deserializeSysObj(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-obj"));
                    deserializeSysCol(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-col"));
                    deserializeSysCCol(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-ccol"));
                    deserializeSysCDef(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-cdef"));
                    deserializeSysDeferredStg(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-deferredstg"));
                    deserializeSysECol(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-ecol"));
                    deserializeSysLob(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-lob"));
                    deserializeSysLobCompPart(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-lob-comp-part"));
                    deserializeSysLobFrag(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-lob-frag"));
                    deserializeSysTab(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-tab"));
                    deserializeSysTabPart(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-tabpart"));
                    deserializeSysTabComPart(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-tabcompart"));
                    deserializeSysTabSubPart(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-tabsubpart"));
                    deserializeSysTs(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "sys-ts"));
                    // allow continuing
                    if (document.HasMember("xdb-ttset"))
                        deserializeXdbTtSet(metadata, fileName, Ctx::getJsonFieldA(fileName, document, "xdb-ttset"));

                    for (const auto& [key, xdbTtSet]: metadata->schema->xdbTtSetPack.mapRowId) {
                        auto* xmlCtx = new XmlCtx(metadata->ctx, xdbTtSet->tokSuf, xdbTtSet->flags);
                        metadata->schema->schemaXmlMap.insert_or_assign(xdbTtSet->tokSuf, xmlCtx);

                        std::string field = "xdb-xnm" + xdbTtSet->tokSuf;
                        deserializeXdbXNm(metadata, xmlCtx, fileName, Ctx::getJsonFieldA(fileName, document, field.c_str()));
                        field = "xdb-xpt" + xdbTtSet->tokSuf;
                        deserializeXdbXPt(metadata, xmlCtx, fileName, Ctx::getJsonFieldA(fileName, document, field.c_str()));
                        field = "xdb-xqn" + xdbTtSet->tokSuf;
                        deserializeXdbXQn(metadata, xmlCtx, fileName, Ctx::getJsonFieldA(fileName, document, field.c_str()));
                    }
                    metadata->schema->touched = true;
                }

                // Loading schema from configuration file
                metadata->buildMaps(msgs, tablesUpdated);
                metadata->schema->resetTouched();
                metadata->schema->loaded = true;
                return true;
            }
        } catch (DataException& ex) {
            metadata->ctx->error(ex.code, ex.msg);
            return false;
        }
        return true;
    }

    void SerializerJson::deserializeSysCCol(const Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysCColJson) {
        for (rapidjson::SizeType i = 0; i < sysCColJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysCColChildNames{
                    "con",
                    "int-col",
                    "obj",
                    "row-id",
                    "spare1"
                };
                Ctx::checkJsonFields(fileName, sysCColJson[i], sysCColChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysCColJson[i], "row-id");
            const typeCon con = Ctx::getJsonFieldU32(fileName, sysCColJson[i], "con");
            const typeCol intCol = Ctx::getJsonFieldI16(fileName, sysCColJson[i], "int-col");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysCColJson[i], "obj");
            const rapidjson::Value& spare1Json = Ctx::getJsonFieldA(fileName, sysCColJson[i], "spare1");
            if (unlikely(spare1Json.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - spare1 should be an array with 2 elements");
            const uint64_t spare11 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 0);
            const uint64_t spare12 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 1);

            metadata->schema->sysCColPack.addWithKeys(metadata->ctx, new SysCCol(RowId(rowIdStr), con, intCol, obj, spare11, spare12));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysCDef(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysCDefJson) {
        for (rapidjson::SizeType i = 0; i < sysCDefJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysCDefChildNames{
                    "con",
                    "obj",
                    "row-id",
                    "type"
                };
                Ctx::checkJsonFields(fileName, sysCDefJson[i], sysCDefChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysCDefJson[i], "row-id");
            const typeCon con = Ctx::getJsonFieldU32(fileName, sysCDefJson[i], "con");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysCDefJson[i], "obj");
            const auto type = static_cast<SysCDef::CDEFTYPE>(Ctx::getJsonFieldU16(fileName, sysCDefJson[i], "type"));

            metadata->schema->sysCDefPack.addWithKeys(metadata->ctx, new SysCDef(RowId(rowIdStr), con, obj, type));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysCol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysColJson) {
        for (rapidjson::SizeType i = 0; i < sysColJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysColChildNames{
                    "charset-form",
                    "charset-id",
                    "col",
                    "int-col",
                    "length",
                    "name",
                    "null",
                    "obj",
                    "precision",
                    "property",
                    "row-id",
                    "scale",
                    "seg-col",
                    "type"
                };
                Ctx::checkJsonFields(fileName, sysColJson[i], sysColChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysColJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysColJson[i], "obj");
            const typeCol col = Ctx::getJsonFieldI16(fileName, sysColJson[i], "col");
            const typeCol segCol = Ctx::getJsonFieldI16(fileName, sysColJson[i], "seg-col");
            const typeCol intCol = Ctx::getJsonFieldI16(fileName, sysColJson[i], "int-col");
            const std::string name_ = Ctx::getJsonFieldS(fileName, SysCol::NAME_LENGTH, sysColJson[i], "name");
            const auto type = static_cast<SysCol::COLTYPE>(Ctx::getJsonFieldU16(fileName, sysColJson[i], "type"));
            const uint length = Ctx::getJsonFieldU(fileName, sysColJson[i], "length");
            const int precision = Ctx::getJsonFieldI(fileName, sysColJson[i], "precision");
            const int scale = Ctx::getJsonFieldI(fileName, sysColJson[i], "scale");
            const uint charsetForm = Ctx::getJsonFieldU(fileName, sysColJson[i], "charset-form");
            const uint charsetId = Ctx::getJsonFieldU(fileName, sysColJson[i], "charset-id");
            const int null_ = Ctx::getJsonFieldI(fileName, sysColJson[i], "null");
            const rapidjson::Value& propertyJson = Ctx::getJsonFieldA(fileName, sysColJson[i], "property");
            if (unlikely(propertyJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - property should be an array with 2 elements");
            const uint64_t property1 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 0);
            const uint64_t property2 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 1);

            metadata->schema->sysColPack.addWithKeys(metadata->ctx, new SysCol(RowId(rowIdStr), obj, col, segCol, intCol, name_, type, length, precision,
                                                                               scale, charsetForm, charsetId, null_, property1, property2));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysDeferredStg(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysDeferredStgJson) {
        for (rapidjson::SizeType i = 0; i < sysDeferredStgJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysDeferredStgChildNames{
                    "flags-stg",
                    "obj",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, sysDeferredStgJson[i], sysDeferredStgChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysDeferredStgJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysDeferredStgJson[i], "obj");

            const rapidjson::Value& flagsStgJson = Ctx::getJsonFieldA(fileName, sysDeferredStgJson[i], "flags-stg");
            if (unlikely(flagsStgJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - flags-stg should be an array with 2 elements");
            const uint64_t flagsStg1 = Ctx::getJsonFieldU64(fileName, flagsStgJson, "flags-stg", 0);
            const uint64_t flagsStg2 = Ctx::getJsonFieldU64(fileName, flagsStgJson, "flags-stg", 1);

            metadata->schema->sysDeferredStgPack.addWithKeys(metadata->ctx, new SysDeferredStg(RowId(rowIdStr), obj, flagsStg1, flagsStg2));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysECol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysEColJson) {
        for (rapidjson::SizeType i = 0; i < sysEColJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysEColChildNames{
                    "col-num",
                    "guard-id",
                    "row-id",
                    "tab-obj"
                };
                Ctx::checkJsonFields(fileName, sysEColJson[i], sysEColChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysEColJson[i], "row-id");
            const typeObj tabObj = Ctx::getJsonFieldU32(fileName, sysEColJson[i], "tab-obj");
            const typeCol colNum = Ctx::getJsonFieldI16(fileName, sysEColJson[i], "col-num");
            const typeCol guardId = Ctx::getJsonFieldI16(fileName, sysEColJson[i], "guard-id");

            metadata->schema->sysEColPack.addWithKeys(metadata->ctx, new SysECol(RowId(rowIdStr), tabObj, colNum, guardId));
            metadata->schema->touchTable(tabObj);
        }
    }

    void SerializerJson::deserializeSysLob(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobJson) {
        for (rapidjson::SizeType i = 0; i < sysLobJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysLobChildNames{
                    "col",
                    "int-col",
                    "l-obj",
                    "obj",
                    "row-id",
                    "ts"
                };
                Ctx::checkJsonFields(fileName, sysLobJson[i], sysLobChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysLobJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysLobJson[i], "obj");
            const typeCol col = Ctx::getJsonFieldI16(fileName, sysLobJson[i], "col");
            const typeCol intCol = Ctx::getJsonFieldI16(fileName, sysLobJson[i], "int-col");
            const typeObj lObj = Ctx::getJsonFieldU32(fileName, sysLobJson[i], "l-obj");
            const uint32_t ts = Ctx::getJsonFieldU32(fileName, sysLobJson[i], "ts");

            metadata->schema->sysLobPack.addWithKeys(metadata->ctx, new SysLob(RowId(rowIdStr), obj, col, intCol, lObj, ts));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysLobCompPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobCompPartJson) {
        for (rapidjson::SizeType i = 0; i < sysLobCompPartJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysLobCompPartChildNames{
                    "l-obj",
                    "part-obj",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, sysLobCompPartJson[i], sysLobCompPartChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysLobCompPartJson[i], "row-id");
            const typeObj partObj = Ctx::getJsonFieldU32(fileName, sysLobCompPartJson[i], "part-obj");
            const typeObj lObj = Ctx::getJsonFieldU32(fileName, sysLobCompPartJson[i], "l-obj");

            metadata->schema->sysLobCompPartPack.addWithKeys(metadata->ctx, new SysLobCompPart(RowId(rowIdStr), partObj, lObj));
            metadata->schema->touchTableLob(lObj);
        }
    }

    void SerializerJson::deserializeSysLobFrag(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobFragJson) {
        for (rapidjson::SizeType i = 0; i < sysLobFragJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysLobFragChildNames{
                    "frag-obj",
                    "parent-obj",
                    "row-id",
                    "ts"
                };
                Ctx::checkJsonFields(fileName, sysLobFragJson[i], sysLobFragChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysLobFragJson[i], "row-id");
            const typeObj fragObj = Ctx::getJsonFieldU32(fileName, sysLobFragJson[i], "frag-obj");
            const typeObj parentObj = Ctx::getJsonFieldU32(fileName, sysLobFragJson[i], "parent-obj");
            const uint32_t ts = Ctx::getJsonFieldU32(fileName, sysLobFragJson[i], "ts");

            metadata->schema->sysLobFragPack.addWithKeys(metadata->ctx, new SysLobFrag(RowId(rowIdStr), fragObj, parentObj, ts));
            metadata->schema->touchTableLobFrag(parentObj);
            metadata->schema->touchTableLob(parentObj);
        }
    }

    void SerializerJson::deserializeSysObj(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysObjJson) {
        for (rapidjson::SizeType i = 0; i < sysObjJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysObjChildNames{
                    "data-obj",
                    "flags",
                    "name",
                    "obj",
                    "owner",
                    "row-id",
                    "single",
                    "type"
                };
                Ctx::checkJsonFields(fileName, sysObjJson[i], sysObjChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysObjJson[i], "row-id");
            const typeUser owner = Ctx::getJsonFieldU32(fileName, sysObjJson[i], "owner");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysObjJson[i], "obj");
            const typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysObjJson[i], "data-obj");
            const auto type = static_cast<SysObj::OBJTYPE>(Ctx::getJsonFieldU16(fileName, sysObjJson[i], "type"));
            const std::string name_ = Ctx::getJsonFieldS(fileName, SysObj::NAME_LENGTH, sysObjJson[i], "name");

            const rapidjson::Value& flagsJson = Ctx::getJsonFieldA(fileName, sysObjJson[i], "flags");
            if (unlikely(flagsJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - flags should be an array with 2 elements");
            const uint64_t flags1 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 0);
            const uint64_t flags2 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 1);
            const uint64_t single = Ctx::getJsonFieldU64(fileName, sysObjJson[i], "single");

            metadata->schema->sysObjPack.addWithKeys(metadata->ctx, new SysObj(RowId(rowIdStr), owner, obj, dataObj, type, name_, flags1, flags2,
                                                                               single != 0U));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysTab(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabJson) {
        for (rapidjson::SizeType i = 0; i < sysTabJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysTabChildNames{
                    "clu-cols",
                    "data-obj",
                    "flags",
                    "obj",
                    "property",
                    "row-id",
                    "ts"
                };
                Ctx::checkJsonFields(fileName, sysTabJson[i], sysTabChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysTabJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "obj");
            const typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "data-obj");
            typeTs ts = 0;
            if (sysTabJson[i].HasMember("ts"))
                ts = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "ts");
            // typeTs ts = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "ts");
            const typeCol cluCols = Ctx::getJsonFieldI16(fileName, sysTabJson[i], "clu-cols");

            const rapidjson::Value& flagsJson = Ctx::getJsonFieldA(fileName, sysTabJson[i], "flags");
            if (unlikely(flagsJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - flags should be an array with 2 elements");
            const uint64_t flags1 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 0);
            const uint64_t flags2 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 1);

            const rapidjson::Value& propertyJson = Ctx::getJsonFieldA(fileName, sysTabJson[i], "property");
            if (unlikely(propertyJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - property should be an array with 2 elements");
            const uint64_t property1 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 0);
            const uint64_t property2 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 1);

            metadata->schema->sysTabPack.addWithKeys(metadata->ctx, new SysTab(RowId(rowIdStr), obj, dataObj, ts, cluCols, flags1, flags2, property1,
                                                                               property2));
            metadata->schema->touchTable(obj);
        }
    }

    void SerializerJson::deserializeSysTabComPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabComPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabComPartJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysTabComPartChildNames{
                    "bo",
                    "data-obj",
                    "obj",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, sysTabComPartJson[i], sysTabComPartChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysTabComPartJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabComPartJson[i], "obj");
            const typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabComPartJson[i], "data-obj");
            const typeObj bo = Ctx::getJsonFieldU32(fileName, sysTabComPartJson[i], "bo");

            metadata->schema->sysTabComPartPack.addWithKeys(metadata->ctx, new SysTabComPart(RowId(rowIdStr), obj, dataObj, bo));
            metadata->schema->touchTable(bo);
        }
    }

    void SerializerJson::deserializeSysTabPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabPartJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysTabPartChildNames{
                    "bo",
                    "data-obj",
                    "obj",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, sysTabPartJson[i], sysTabPartChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysTabPartJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabPartJson[i], "obj");
            const typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabPartJson[i], "data-obj");
            const typeObj bo = Ctx::getJsonFieldU32(fileName, sysTabPartJson[i], "bo");

            metadata->schema->sysTabPartPack.addWithKeys(metadata->ctx, new SysTabPart(RowId(rowIdStr), obj, dataObj, bo));
            metadata->schema->touchTable(bo);
        }
    }

    void SerializerJson::deserializeSysTabSubPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabSubPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabSubPartJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysTabSubPartChildNames{
                    "data-obj",
                    "obj",
                    "p-obj",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, sysTabSubPartJson[i], sysTabSubPartChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysTabSubPartJson[i], "row-id");
            const typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabSubPartJson[i], "obj");
            const typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabSubPartJson[i], "data-obj");
            const typeObj pObj = Ctx::getJsonFieldU32(fileName, sysTabSubPartJson[i], "p-obj");

            metadata->schema->sysTabSubPartPack.addWithKeys(metadata->ctx, new SysTabSubPart(RowId(rowIdStr), obj, dataObj, pObj));
            metadata->schema->touchTablePart(obj);
        }
    }

    void SerializerJson::deserializeSysTs(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTsJson) {
        for (rapidjson::SizeType i = 0; i < sysTsJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysTsChildNames{
                    "block-size",
                    "name",
                    "row-id",
                    "ts"
                };
                Ctx::checkJsonFields(fileName, sysTsJson[i], sysTsChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysTsJson[i], "row-id");
            const typeTs ts = Ctx::getJsonFieldU32(fileName, sysTsJson[i], "ts");
            const std::string name_ = Ctx::getJsonFieldS(fileName, SysTs::NAME_LENGTH, sysTsJson[i], "name");
            const uint32_t blockSize = Ctx::getJsonFieldU32(fileName, sysTsJson[i], "block-size");

            metadata->schema->sysTsPack.addWithKeys(metadata->ctx, new SysTs(RowId(rowIdStr), ts, name_, blockSize));
        }
    }

    void SerializerJson::deserializeSysUser(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysUserJson) {
        for (rapidjson::SizeType i = 0; i < sysUserJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sysUserChildNames{
                    "name",
                    "row-id",
                    "single",
                    "spare1",
                    "user"
                };
                Ctx::checkJsonFields(fileName, sysUserJson[i], sysUserChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, sysUserJson[i], "row-id");
            const typeUser user = Ctx::getJsonFieldU32(fileName, sysUserJson[i], "user");
            const std::string name_ = Ctx::getJsonFieldS(fileName, SysUser::NAME_LENGTH, sysUserJson[i], "name");

            const rapidjson::Value& spare1Json = Ctx::getJsonFieldA(fileName, sysUserJson[i], "spare1");
            if (unlikely(spare1Json.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - spare1 should be an array with 2 elements");
            const uint64_t spare11 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 0);
            const uint64_t spare12 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 1);
            const uint64_t single = Ctx::getJsonFieldU64(fileName, sysUserJson[i], "single");

            metadata->schema->sysUserPack.addWithKeys(metadata->ctx, new SysUser(RowId(rowIdStr), user, name_, spare11, spare12, single != 0U));
        }
    }

    void SerializerJson::deserializeXdbTtSet(Metadata* metadata, const std::string& fileName, const rapidjson::Value& xdbTtSetJson) {
        for (rapidjson::SizeType i = 0; i < xdbTtSetJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> xdbTtSetChildNames{
                    "flags",
                    "guid",
                    "obj",
                    "row-id",
                    "toksuf"
                };
                Ctx::checkJsonFields(fileName, xdbTtSetJson[i], xdbTtSetChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, xdbTtSetJson[i], "row-id");
            const std::string guid = Ctx::getJsonFieldS(fileName, XdbTtSet::GUID_LENGTH, xdbTtSetJson[i], "guid");
            const std::string tokSuf = Ctx::getJsonFieldS(fileName, XdbTtSet::TOKSUF_LENGTH, xdbTtSetJson[i], "toksuf");
            const uint64_t flags = Ctx::getJsonFieldU64(fileName, xdbTtSetJson[i], "flags");
            const uint32_t obj = Ctx::getJsonFieldU32(fileName, xdbTtSetJson[i], "obj");

            metadata->schema->xdbTtSetPack.addWithKeys(metadata->ctx, new XdbTtSet(RowId(rowIdStr), guid, tokSuf, flags, obj));
        }
    }

    void SerializerJson::deserializeXdbXNm(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXNmJson) {
        for (rapidjson::SizeType i = 0; i < xdbXNmJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> xdbXNmChildNames{
                    "id",
                    "nmspcuri",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, xdbXNmJson[i], xdbXNmChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, xdbXNmJson[i], "row-id");
            const std::string nmSpcUri = Ctx::getJsonFieldS(fileName, XdbXNm::NMSPCURI_LENGTH, xdbXNmJson[i], "nmspcuri");
            const std::string id = Ctx::getJsonFieldS(fileName, XdbXNm::ID_LENGTH, xdbXNmJson[i], "id");

            xmlCtx->xdbXNmPack.addWithKeys(metadata->ctx, new XdbXNm(RowId(rowIdStr), nmSpcUri, id));
        }
    }

    void SerializerJson::deserializeXdbXPt(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXPtJson) {
        for (rapidjson::SizeType i = 0; i < xdbXPtJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> xdbXPtChildNames{
                    "id",
                    "path",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, xdbXPtJson[i], xdbXPtChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, xdbXPtJson[i], "row-id");
            const std::string path = Ctx::getJsonFieldS(fileName, XdbXPt::PATH_LENGTH, xdbXPtJson[i], "path");
            const std::string id = Ctx::getJsonFieldS(fileName, XdbXPt::ID_LENGTH, xdbXPtJson[i], "id");

            xmlCtx->xdbXPtPack.addWithKeys(metadata->ctx, new XdbXPt(RowId(rowIdStr), path, id));
        }
    }

    void SerializerJson::deserializeXdbXQn(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXQnJson) {
        for (rapidjson::SizeType i = 0; i < xdbXQnJson.Size(); ++i) {
            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> xdbXQnChildNames{
                    "flags",
                    "id",
                    "localname",
                    "nmspcid",
                    "row-id"
                };
                Ctx::checkJsonFields(fileName, xdbXQnJson[i], xdbXQnChildNames);
            }

            const std::string rowIdStr = Ctx::getJsonFieldS(fileName, RowId::SIZE, xdbXQnJson[i], "row-id");
            const std::string nmSpcId = Ctx::getJsonFieldS(fileName, XdbXQn::NMSPCID_LENGTH, xdbXQnJson[i], "nmspcid");
            const std::string localName = Ctx::getJsonFieldS(fileName, XdbXQn::LOCALNAME_LENGTH, xdbXQnJson[i], "localname");
            const std::string flags = Ctx::getJsonFieldS(fileName, XdbXQn::FLAGS_LENGTH, xdbXQnJson[i], "flags");
            const std::string id = Ctx::getJsonFieldS(fileName, XdbXQn::ID_LENGTH, xdbXQnJson[i], "id");

            xmlCtx->xdbXQnPack.addWithKeys(metadata->ctx, new XdbXQn(RowId(rowIdStr), nmSpcId, localName, flags, id));
        }
    }
}
