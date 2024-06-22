/* Base class for serialization of metadata to json
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

#include "../common/Ctx.h"
#include "../common/OracleIncarnation.h"
#include "../common/OracleTable.h"
#include "../common/typeRowId.h"
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
#include "RedoLog.h"
#include "Metadata.h"
#include "Schema.h"
#include "SchemaElement.h"
#include "SerializerJson.h"

namespace OpenLogReplicator {
    SerializerJson::SerializerJson() :
            Serializer() {
    }

    SerializerJson::~SerializerJson() = default;

    void SerializerJson::serialize(Metadata* metadata, std::ostringstream& ss, bool storeSchema) {
        // Assuming the caller holds all locks
        ss << R"({"database":")";
        Ctx::writeEscapeValue(ss, metadata->database);
        ss << R"(","scn":)" << std::dec << metadata->checkpointScn <<
           R"(,"resetlogs":)" << std::dec << metadata->resetlogs <<
           R"(,"activation":)" << std::dec << metadata->activation <<
           R"(,"time":)" << std::dec << metadata->checkpointTime.getVal() << // not read
           R"(,"seq":)" << std::dec << metadata->checkpointSequence <<
           R"(,"offset":)" << std::dec << metadata->checkpointOffset;
        if (metadata->minSequence != Ctx::ZERO_SEQ) {
            ss << R"(,"min-tran":{)" <<
               R"("seq":)" << std::dec << metadata->minSequence <<
               R"(,"offset":)" << std::dec << metadata->minOffset <<
               R"(,"xid":")" << metadata->minXid.toString() << R"("})";
        }
        ss << R"(,"big-endian":)" << std::dec << (metadata->ctx->isBigEndian() ? 1 : 0) <<
           R"(,"context":")";
        Ctx::writeEscapeValue(ss, metadata->context);
        ss << R"(","con-id":)" << std::dec << metadata->conId <<
           R"(,"con-name":")";
        Ctx::writeEscapeValue(ss, metadata->conName);
        ss << R"(","db-timezone":")";
        Ctx::writeEscapeValue(ss, metadata->dbTimezoneStr);
        ss << R"(","db-recovery-file-dest":")";
        Ctx::writeEscapeValue(ss, metadata->dbRecoveryFileDest);
        ss << R"(",)" << R"("db-block-checksum":")";
        Ctx::writeEscapeValue(ss, metadata->dbBlockChecksum);
        ss << R"(",)" << R"("log-archive-dest":")";
        Ctx::writeEscapeValue(ss, metadata->logArchiveDest);
        ss << R"(",)" << R"("log-archive-format":")";
        Ctx::writeEscapeValue(ss, metadata->logArchiveFormat);
        ss << R"(",)" << R"("nls-character-set":")";
        Ctx::writeEscapeValue(ss, metadata->nlsCharacterSet);
        ss << R"(",)" << R"("nls-nchar-character-set":")";
        Ctx::writeEscapeValue(ss, metadata->nlsNcharCharacterSet);

        ss << R"(","supp-log-db-primary":)" << (metadata->suppLogDbPrimary ? 1 : 0) <<
           R"(,"supp-log-db-all":)" << (metadata->suppLogDbAll ? 1 : 0) <<
           R"(,)" SERIALIZER_ENDL << R"("online-redo":[)";

        int64_t prevGroup = -2;
        for (const RedoLog* redoLog: metadata->redoLogs) {
            if (redoLog->group == 0)
                continue;

            if (prevGroup == -2)
                ss SERIALIZER_ENDL << R"({"group":)" << redoLog->group << R"(,"path":[)";
            else if (prevGroup != redoLog->group)
                ss << "]}," SERIALIZER_ENDL << R"({"group":)" << redoLog->group << R"(,"path":[)";
            else
                ss << ",";

            ss << R"(")";
            Ctx::writeEscapeValue(ss, redoLog->path);
            ss << R"(")";

            prevGroup = redoLog->group;
        }
        if (prevGroup > 0)
            ss << "]}";

        ss << "]," SERIALIZER_ENDL << R"("incarnations":[)";
        bool hasPrev = false;
        for (const OracleIncarnation* oi: metadata->oracleIncarnations) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;
            ss SERIALIZER_ENDL << R"({"incarnation":)" << oi->incarnation <<
                               R"(,"resetlogs-scn":)" << oi->resetlogsScn <<
                               R"(,"prior-resetlogs-scn":)" << oi->priorResetlogsScn <<
                               R"(,"status":")";
            Ctx::writeEscapeValue(ss, oi->status);
            ss << R"(","resetlogs":)" << oi->resetlogs <<
               R"(,"prior-incarnation":)" << oi->priorIncarnation << "}";
        }

        ss << "]," SERIALIZER_ENDL << R"("users":[)";
        hasPrev = false;
        for (const std::string& user: metadata->users) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;
            ss SERIALIZER_ENDL << R"(")" << user << R"(")";
        }

        ss << "]," SERIALIZER_ENDL;

        // The schema has not changed since the last checkpoint file
        if (!storeSchema) {
            ss << R"("schema-ref-scn":)" << metadata->schema->refScn << "}";
            return;
        }

        metadata->schema->refScn = metadata->checkpointScn;
        ss << R"("schema-scn":)" << metadata->schema->scn << "," SERIALIZER_ENDL;

        // SYS.CCOL$
        ss << R"("sys-ccol":[)";
        hasPrev = false;
        for (auto sysCColMapRowIdIt: metadata->schema->sysCColMapRowId) {
            SysCCol* sysCCol = sysCColMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysCCol->rowId <<
                               R"(","con":)" << std::dec << sysCCol->con <<
                               R"(,"int-col":)" << std::dec << sysCCol->intCol <<
                               R"(,"obj":)" << std::dec << sysCCol->obj <<
                               R"(,"spare1":)" << std::dec << sysCCol->spare1.toString() << "}";
        }

        // SYS.CDEF$
        ss << "]," SERIALIZER_ENDL << R"("sys-cdef":[)";
        hasPrev = false;
        for (auto sysCDefMapRowIdIt: metadata->schema->sysCDefMapRowId) {
            const SysCDef* sysCDef = sysCDefMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysCDef->rowId <<
                               R"(","con":)" << std::dec << sysCDef->con <<
                               R"(,"obj":)" << std::dec << sysCDef->obj <<
                               R"(,"type":)" << std::dec << sysCDef->type << "}";
        }

        // SYS.COL$
        ss << "]," SERIALIZER_ENDL << R"("sys-col":[)";
        hasPrev = false;
        for (auto sysColMapRowIdIt: metadata->schema->sysColMapRowId) {
            SysCol* sysCol = sysColMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysCol->rowId <<
                               R"(","obj":)" << std::dec << sysCol->obj <<
                               R"(,"col":)" << std::dec << sysCol->col <<
                               R"(,"seg-col":)" << std::dec << sysCol->segCol <<
                               R"(,"int-col":)" << std::dec << sysCol->intCol <<
                               R"(,"name":")";
            Ctx::writeEscapeValue(ss, sysCol->name);
            ss << R"(","type":)" << std::dec << sysCol->type <<
               R"(,"length":)" << std::dec << sysCol->length <<
               R"(,"precision":)" << std::dec << sysCol->precision <<
               R"(,"scale":)" << std::dec << sysCol->scale <<
               R"(,"charset-form":)" << std::dec << sysCol->charsetForm <<
               R"(,"charset-id":)" << std::dec << sysCol->charsetId <<
               R"(,"null":)" << std::dec << sysCol->null_ <<
               R"(,"property":)" << sysCol->property.toString() << "}";
        }

        // SYS.DEFERRED_STG$
        ss << "]," SERIALIZER_ENDL << R"("sys-deferredstg":[)";
        hasPrev = false;
        for (auto sysDeferredStgMapRowIdIt: metadata->schema->sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysDeferredStg->rowId <<
                               R"(","obj":)" << std::dec << sysDeferredStg->obj <<
                               R"(,"flags-stg":)" << std::dec << sysDeferredStg->flagsStg.toString() << "}";
        }

        // SYS.ECOL$
        ss << "]," SERIALIZER_ENDL << R"("sys-ecol":[)";
        hasPrev = false;
        for (auto sysEColMapRowIdIt: metadata->schema->sysEColMapRowId) {
            const SysECol* sysECol = sysEColMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysECol->rowId <<
                               R"(","tab-obj":)" << std::dec << sysECol->tabObj <<
                               R"(,"col-num":)" << std::dec << sysECol->colNum <<
                               R"(,"guard-id":)" << std::dec << sysECol->guardId << "}";
        }

        // SYS.LOB$
        ss << "]," SERIALIZER_ENDL << R"("sys-lob":[)";
        hasPrev = false;
        for (auto sysLobMapRowIdIt: metadata->schema->sysLobMapRowId) {
            const SysLob* sysLob = sysLobMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysLob->rowId <<
                               R"(","obj":)" << std::dec << sysLob->obj <<
                               R"(,"col":)" << std::dec << sysLob->col <<
                               R"(,"int-col":)" << std::dec << sysLob->intCol <<
                               R"(,"l-obj":)" << std::dec << sysLob->lObj <<
                               R"(,"ts":)" << std::dec << sysLob->ts << "}";
        }

        // SYS.LOBCOMPPART$
        ss << "]," SERIALIZER_ENDL << R"("sys-lob-comp-part":[)";
        hasPrev = false;
        for (auto sysLobCompPartMapRowIdIt: metadata->schema->sysLobCompPartMapRowId) {
            const SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysLobCompPart->rowId <<
                               R"(","part-obj":)" << std::dec << sysLobCompPart->partObj <<
                               R"(,"l-obj":)" << std::dec << sysLobCompPart->lObj << "}";
        }

        // SYS.LOBFRAG$
        ss << "]," SERIALIZER_ENDL << R"("sys-lob-frag":[)";
        hasPrev = false;
        for (auto sysLobFragMapRowIdIt: metadata->schema->sysLobFragMapRowId) {
            const SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysLobFrag->rowId <<
                               R"(","frag-obj":)" << std::dec << sysLobFrag->fragObj <<
                               R"(,"parent-obj":)" << std::dec << sysLobFrag->parentObj <<
                               R"(,"ts":)" << std::dec << sysLobFrag->ts << "}";
        }

        // SYS.OBJ$
        ss << "]," SERIALIZER_ENDL << R"("sys-obj":[)";
        hasPrev = false;
        for (auto sysObjMapRowIdIt: metadata->schema->sysObjMapRowId) {
            SysObj* sysObj = sysObjMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysObj->rowId <<
                               R"(","owner":)" << std::dec << sysObj->owner <<
                               R"(,"obj":)" << std::dec << sysObj->obj <<
                               R"(,"data-obj":)" << std::dec << sysObj->dataObj <<
                               R"(,"name":")";
            Ctx::writeEscapeValue(ss, sysObj->name);
            ss << R"(","type":)" << std::dec << sysObj->type <<
               R"(,"flags":)" << std::dec << sysObj->flags.toString() <<
               R"(,"single":)" << std::dec << static_cast<uint64_t>(sysObj->single) << "}";
        }

        // SYS.TAB$
        ss << "]," SERIALIZER_ENDL << R"("sys-tab":[)";
        hasPrev = false;
        for (auto sysTabMapRowIdIt: metadata->schema->sysTabMapRowId) {
            SysTab* sysTab = sysTabMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysTab->rowId <<
                               R"(","obj":)" << std::dec << sysTab->obj <<
                               R"(,"data-obj":)" << std::dec << sysTab->dataObj <<
                               R"(,"ts":)" << std::dec << sysTab->ts <<
                               R"(,"clu-cols":)" << std::dec << sysTab->cluCols <<
                               R"(,"flags":)" << std::dec << sysTab->flags.toString() <<
                               R"(,"property":)" << std::dec << sysTab->property.toString() << "}";
        }

        // SYS.TABCOMPART$
        ss << "]," SERIALIZER_ENDL << R"("sys-tabcompart":[)";
        hasPrev = false;
        for (auto sysTabComPartMapRowIdIt: metadata->schema->sysTabComPartMapRowId) {
            const SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysTabComPart->rowId <<
                               R"(","obj":)" << std::dec << sysTabComPart->obj <<
                               R"(,"data-obj":)" << std::dec << sysTabComPart->dataObj <<
                               R"(,"bo":)" << std::dec << sysTabComPart->bo << "}";
        }

        // SYS.TABPART$
        ss << "]," SERIALIZER_ENDL << R"("sys-tabpart":[)";
        hasPrev = false;
        for (auto sysTabPartMapRowIdIt: metadata->schema->sysTabPartMapRowId) {
            const SysTabPart* sysTabPart = sysTabPartMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysTabPart->rowId <<
                               R"(","obj":)" << std::dec << sysTabPart->obj <<
                               R"(,"data-obj":)" << std::dec << sysTabPart->dataObj <<
                               R"(,"bo":)" << std::dec << sysTabPart->bo << "}";
        }

        // SYS.TABSUBPART$
        ss << "]," SERIALIZER_ENDL << R"("sys-tabsubpart":[)";
        hasPrev = false;
        for (auto sysTabSubPartMapRowIdIt: metadata->schema->sysTabSubPartMapRowId) {
            const SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysTabSubPart->rowId <<
                               R"(","obj":)" << std::dec << sysTabSubPart->obj <<
                               R"(,"data-obj":)" << std::dec << sysTabSubPart->dataObj <<
                               R"(,"p-obj":)" << std::dec << sysTabSubPart->pObj << "}";
        }

        // SYS.TS$
        ss << "]," SERIALIZER_ENDL << R"("sys-ts":[)";
        hasPrev = false;
        for (auto sysTsMapRowIdIt: metadata->schema->sysTsMapRowId) {
            const SysTs* sysTs = sysTsMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysTs->rowId <<
                               R"(","ts":)" << std::dec << sysTs->ts <<
                               R"(,"name":")";
            Ctx::writeEscapeValue(ss, sysTs->name);
            ss << R"(","block-size":)" << std::dec << sysTs->blockSize << "}";
        }

        // SYS.USER$
        ss << "]," SERIALIZER_ENDL << R"("sys-user":[)";
        hasPrev = false;
        for (auto sysUserMapRowIdIt: metadata->schema->sysUserMapRowId) {
            SysUser* sysUser = sysUserMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysUser->rowId <<
                               R"(","user":)" << std::dec << sysUser->user <<
                               R"(,"name":")";
            Ctx::writeEscapeValue(ss, sysUser->name);
            ss << R"(","spare1":)" << std::dec << sysUser->spare1.toString() <<
               R"(,"single":)" << std::dec << static_cast<uint64_t>(sysUser->single) << "}";
        }

        // XDB.XDB$TTSET
        ss << "]," SERIALIZER_ENDL << R"("xdb-ttset":[)";
        hasPrev = false;
        for (auto xdbTtSetMapRowIdIt: metadata->schema->xdbTtSetMapRowId) {
            const XdbTtSet* xdbTtSet = xdbTtSetMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << xdbTtSet->rowId <<
                               R"(","guid":")" << std::dec << xdbTtSet->guid <<
                               R"(","toksuf":")";
            Ctx::writeEscapeValue(ss, xdbTtSet->tokSuf);
            ss << R"(","flags":)" << std::dec << xdbTtSet->flags <<
               R"(,"obj":)" << std::dec << xdbTtSet->obj << "}";
        }

        for (const auto& schemaXmlIt: metadata->schema->schemaXmlMap) {
            const XmlCtx* xmlCtx = schemaXmlIt.second;

            // XDB.X$NMxxx
            ss << "]," SERIALIZER_ENDL << R"("xdb-xnm)" << xmlCtx->tokSuf << R"(":[)";
            hasPrev = false;
            for (auto xdbXNmMapRowIdIt: xmlCtx->xdbXNmMapRowId) {
                const XdbXNm* xdbXNm = xdbXNmMapRowIdIt.second;

                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                ss SERIALIZER_ENDL << R"({"row-id":")" << xdbXNm->rowId <<
                                   R"(","nmspcuri":")";
                Ctx::writeEscapeValue(ss, xdbXNm->nmSpcUri);
                ss << R"(","id":")" << xdbXNm->id << R"("})";
            }

            // XDB.X$PTxxx
            ss << "]," SERIALIZER_ENDL << R"("xdb-xpt)" << xmlCtx->tokSuf << R"(":[)";
            hasPrev = false;
            for (auto xdbXPtMapRowIdIt: xmlCtx->xdbXPtMapRowId) {
                const XdbXPt* xdbXPt = xdbXPtMapRowIdIt.second;

                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                ss SERIALIZER_ENDL << R"({"row-id":")" << xdbXPt->rowId <<
                                   R"(","path":")";
                Ctx::writeEscapeValue(ss, xdbXPt->path);
                ss << R"(","id":")" << xdbXPt->id << R"("})";
            }

            // XDB.X$QNxxx
            ss << "]," SERIALIZER_ENDL << R"("xdb-xqn)" << xmlCtx->tokSuf << R"(":[)";
            hasPrev = false;
            for (auto xdbXQnMapRowIdIt: xmlCtx->xdbXQnMapRowId) {
                const XdbXQn* xdbXQn = xdbXQnMapRowIdIt.second;

                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                ss SERIALIZER_ENDL << R"({"row-id":")" << xdbXQn->rowId <<
                                   R"(","nmspcid":")";
                Ctx::writeEscapeValue(ss, xdbXQn->nmSpcId);
                ss << R"(","localname":")";
                Ctx::writeEscapeValue(ss, xdbXQn->localName);
                ss << R"(","flags":")";
                Ctx::writeEscapeValue(ss, xdbXQn->flags);
                ss << R"(","id":")" << xdbXQn->id << R"("})";
            }
        }

        ss << "]}";
    }

    bool SerializerJson::deserialize(Metadata* metadata, const std::string& ss, const std::string& fileName, std::vector<std::string>& msgs, bool loadMetadata,
                                     bool loadSchema) {
        try {
            rapidjson::Document document;
            if (unlikely(ss.length() == 0 || document.Parse(ss.c_str()).HasParseError()))
                throw DataException(20001, "file: " + fileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                           " - parse error: " + GetParseError_En(document.GetParseError()));

            {
                if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                    static const char* documentChildNames[] = {"scn", "min-tran", "seq", "offset", "database", "resetlogs",
                                                               "activation", "time", "big-endian", "context", "con-id", "con-name",
                                                               "db-timezone", "db-recovery-file-dest", "db-block-checksum",
                                                               "log-archive-format", "log-archive-dest", "nls-character-set",
                                                               "nls-nchar-character-set", "supp-log-db-primary", "supp-log-db-all",
                                                               "online-redo", "incarnations", "users", "schema-ref-scn", "schema-scn",
                                                               "sys-user", "sys-obj", "sys-col",  "sys-ccol", "sys-cdef",
                                                               "sys-deferredstg", "sys-ecol", "sys-lob", "sys-lob-comp-part",
                                                               "sys-lob-frag", "sys-tab", "sys-tabpart", "sys-tabcompart",
                                                               "sys-tabsubpart", "sys-ts", "xdb-ttset", nullptr};
                    Ctx::checkJsonFields(fileName, document, documentChildNames);
                }
                std::unique_lock<std::mutex> lckCheckpoint(metadata->mtxCheckpoint);
                std::unique_lock<std::mutex> lckSchema(metadata->mtxSchema);

                if (loadMetadata) {
                    metadata->checkpointScn = Ctx::getJsonFieldU64(fileName, document, "scn");

                    if (document.HasMember("min-tran")) {
                        const rapidjson::Value& minTranJson = Ctx::getJsonFieldO(fileName, document, "min-tran");
                        if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                            static const char* minTranJsonChildNames[] = {"seq", "offset", "xid", nullptr};
                            Ctx::checkJsonFields(fileName, minTranJson, minTranJsonChildNames);
                        }

                        metadata->sequence = Ctx::getJsonFieldU32(fileName, minTranJson, "seq");
                        metadata->offset = Ctx::getJsonFieldU64(fileName, minTranJson, "offset");
                    } else {
                        metadata->sequence = Ctx::getJsonFieldU32(fileName, document, "seq");
                        metadata->offset = Ctx::getJsonFieldU64(fileName, document, "offset");
                    }

                    if (unlikely((metadata->offset & 511) != 0))
                        throw DataException(20006, "file: " + fileName + " - invalid offset: " + std::to_string(metadata->offset) +
                                                   " is not a multiplication of 512");

                    metadata->minSequence = Ctx::ZERO_SEQ;
                    metadata->minOffset = 0;
                    metadata->minXid = 0;
                    metadata->lastCheckpointScn = Ctx::ZERO_SCN;
                    metadata->lastSequence = Ctx::ZERO_SEQ;
                    metadata->lastCheckpointOffset = 0;
                    metadata->lastCheckpointTime = 0;
                    metadata->lastCheckpointBytes = 0;

                    if (!metadata->onlineData) {
                        // Database metadata
                        metadata->database = Ctx::getJsonFieldS(fileName, Ctx::JSON_PARAMETER_LENGTH, document, "database");
                        metadata->resetlogs = Ctx::getJsonFieldU32(fileName, document, "resetlogs");
                        metadata->activation = Ctx::getJsonFieldU32(fileName, document, "activation");
                        int64_t bigEndian = Ctx::getJsonFieldU64(fileName, document, "big-endian");
                        if (bigEndian == 1)
                            metadata->ctx->setBigEndian();
                        metadata->context = Ctx::getJsonFieldS(fileName, OracleTable::VCONTEXT_LENGTH, document, "context");
                        metadata->conId = Ctx::getJsonFieldI16(fileName, document, "con-id");
                        metadata->conName = Ctx::getJsonFieldS(fileName, OracleTable::VCONTEXT_LENGTH, document, "con-name");
                        if (document.HasMember("db-timezone"))
                            metadata->dbTimezoneStr = Ctx::getJsonFieldS(fileName, OracleTable::VCONTEXT_LENGTH, document, "db-timezone");
                        else
                            metadata->dbTimezoneStr = "+00:00";
                        if (metadata->ctx->dbTimezone != Ctx::BAD_TIMEZONE) {
                            metadata->dbTimezone = metadata->ctx->dbTimezone;
                        } else {
                            if (unlikely(!metadata->ctx->parseTimezone(metadata->dbTimezoneStr.c_str(), metadata->dbTimezone)))
                                throw DataException(20001, "file: " + fileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                                           " - parse error of field \"db-timezone\", invalid value: " + metadata->dbTimezoneStr);
                        }
                        metadata->dbRecoveryFileDest = Ctx::getJsonFieldS(fileName, OracleTable::VPARAMETER_LENGTH, document, "db-recovery-file-dest");
                        metadata->dbBlockChecksum = Ctx::getJsonFieldS(fileName, OracleTable::VPARAMETER_LENGTH, document, "db-block-checksum");
                        if (!metadata->logArchiveFormatCustom)
                            metadata->logArchiveFormat = Ctx::getJsonFieldS(fileName, OracleTable::VPARAMETER_LENGTH, document, "log-archive-format");
                        metadata->logArchiveDest = Ctx::getJsonFieldS(fileName, OracleTable::VPARAMETER_LENGTH, document, "log-archive-dest");
                        metadata->nlsCharacterSet = Ctx::getJsonFieldS(fileName, OracleTable::VPROPERTY_LENGTH, document, "nls-character-set");
                        metadata->nlsNcharCharacterSet = Ctx::getJsonFieldS(fileName, OracleTable::VPROPERTY_LENGTH, document,
                                                                            "nls-nchar-character-set");
                        metadata->setNlsCharset(metadata->nlsCharacterSet, metadata->nlsNcharCharacterSet);
                        metadata->suppLogDbPrimary = Ctx::getJsonFieldU64(fileName, document, "supp-log-db-primary");
                        metadata->suppLogDbAll = Ctx::getJsonFieldU64(fileName, document, "supp-log-db-all");

                        const rapidjson::Value& onlineRedoJson = Ctx::getJsonFieldA(fileName, document, "online-redo");
                        for (rapidjson::SizeType i = 0; i < onlineRedoJson.Size(); ++i) {
                            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                                static const char* onlineRedoChildNames[] = {"group", "path", nullptr};
                                Ctx::checkJsonFields(fileName, onlineRedoJson[i], onlineRedoChildNames);
                            }

                            int64_t group = Ctx::getJsonFieldI64(fileName, onlineRedoJson[i], "group");
                            const rapidjson::Value& path = Ctx::getJsonFieldA(fileName, onlineRedoJson[i], "path");

                            for (rapidjson::SizeType j = 0; j < path.Size(); ++j) {
                                const rapidjson::Value& pathVal = path[j];
                                auto redoLog = new RedoLog(group, pathVal.GetString());
                                metadata->redoLogs.insert(redoLog);
                            }
                        }

                        const rapidjson::Value& incarnationsJson = Ctx::getJsonFieldA(fileName, document, "incarnations");
                        for (rapidjson::SizeType i = 0; i < incarnationsJson.Size(); ++i) {
                            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                                static const char* incarnationsChildNames[] = {"incarnation", "resetlogs-scn", "prior-resetlogs-scn",
                                                                               "status", "resetlogs", "prior-incarnation", nullptr};
                                Ctx::checkJsonFields(fileName, incarnationsJson[i], incarnationsChildNames);
                            }

                            uint32_t incarnation = Ctx::getJsonFieldU32(fileName, incarnationsJson[i], "incarnation");
                            typeScn resetlogsScn = Ctx::getJsonFieldU64(fileName, incarnationsJson[i], "resetlogs-scn");
                            typeScn priorResetlogsScn = Ctx::getJsonFieldU64(fileName, incarnationsJson[i], "prior-resetlogs-scn");
                            const char* status = Ctx::getJsonFieldS(fileName, 128, incarnationsJson[i], "status");
                            typeResetlogs resetlogs = Ctx::getJsonFieldU32(fileName, incarnationsJson[i], "resetlogs");
                            uint32_t priorIncarnation = Ctx::getJsonFieldU32(fileName, incarnationsJson[i], "prior-incarnation");

                            auto oi = new OracleIncarnation(incarnation, resetlogsScn, priorResetlogsScn,
                                                            status, resetlogs, priorIncarnation);
                            metadata->oracleIncarnations.insert(oi);

                            if (oi->current)
                                metadata->oracleIncarnationCurrent = oi;
                            else
                                metadata->oracleIncarnationCurrent = nullptr;
                        }
                    }

                    if (!metadata->ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                        std::set<std::string> users;
                        const rapidjson::Value& usersJson = Ctx::getJsonFieldA(fileName, document, "users");
                        for (rapidjson::SizeType i = 0; i < usersJson.Size(); ++i) {
                            const rapidjson::Value& userJson = usersJson[i];;
                            users.insert(userJson.GetString());
                        }

                        for (auto& user: metadata->users) {
                            if (unlikely(users.find(user) == users.end()))
                                throw DataException(20007, "file: " + fileName + " - " + user + " is missing");
                        }
                        for (auto& user: users) {
                            if (unlikely(metadata->users.find(user) == metadata->users.end()))
                                throw DataException(20007, "file: " + fileName + " - " + user + " is redundant");
                        }
                        users.clear();
                    }
                }

                if (loadSchema) {
                    // Schema referenced to other checkpoint file
                    if (document.HasMember("schema-ref-scn")) {
                        metadata->schema->scn = Ctx::ZERO_SCN;
                        metadata->schema->refScn = Ctx::getJsonFieldU64(fileName, document, "schema-ref-scn");

                    } else {
                        metadata->schema->scn = Ctx::getJsonFieldU64(fileName, document, "schema-scn");
                        metadata->schema->refScn = Ctx::ZERO_SCN;

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

                        for (auto ttSetIt: metadata->schema->xdbTtSetMapRowId) {
                            XmlCtx* xmlCtx = new XmlCtx(metadata->ctx, ttSetIt.second->tokSuf, ttSetIt.second->flags);
                            metadata->schema->schemaXmlMap.insert_or_assign(ttSetIt.second->tokSuf, xmlCtx);

                            std::string field = "xdb-xnm" + ttSetIt.second->tokSuf;
                            deserializeXdbXNm(metadata, xmlCtx, fileName, Ctx::getJsonFieldA(fileName, document, field.c_str()));
                            field = "xdb-xpt" + ttSetIt.second->tokSuf;
                            deserializeXdbXPt(metadata, xmlCtx, fileName, Ctx::getJsonFieldA(fileName, document, field.c_str()));
                            field = "xdb-xqn" + ttSetIt.second->tokSuf;
                            deserializeXdbXQn(metadata, xmlCtx, fileName, Ctx::getJsonFieldA(fileName, document, field.c_str()));
                        }
                    }

                    for (const SchemaElement* element: metadata->schemaElements) {
                        if (metadata->ctx->logLevel >= Ctx::LOG_LEVEL_DEBUG)
                            msgs.push_back("- creating table schema for owner: " + element->owner + " table: " + element->table + " options: " +
                                           std::to_string(element->options));

                        metadata->schema->buildMaps(element->owner, element->table, element->keys, element->keysStr, element->conditionStr, element->options,
                                                    msgs, metadata->suppLogDbPrimary, metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                                    metadata->defaultCharacterNcharMapId);
                    }

                    metadata->schema->resetTouched();
                    metadata->schema->loaded = true;
                    return true;
                }
            }
        } catch (DataException& ex) {
            metadata->ctx->error(ex.code, ex.msg);
            return false;
        }
        return true;
    }

    void SerializerJson::deserializeSysCCol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysCColJson) {
        for (rapidjson::SizeType i = 0; i < sysCColJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysCColChildNames[] = {"row-id", "con", "int-col", "obj", "spare1", nullptr};
                Ctx::checkJsonFields(fileName, sysCColJson[i], sysCColChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysCColJson[i], "row-id");
            typeCon con = Ctx::getJsonFieldU32(fileName, sysCColJson[i], "con");
            typeCol intCol = Ctx::getJsonFieldI16(fileName, sysCColJson[i], "int-col");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysCColJson[i], "obj");
            const rapidjson::Value& spare1Json = Ctx::getJsonFieldA(fileName, sysCColJson[i], "spare1");
            if (unlikely(spare1Json.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - spare1 should be an array with 2 elements");
            uint64_t spare11 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 0);
            uint64_t spare12 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 1);

            metadata->schema->dictSysCColAdd(rowIdStr, con, intCol, obj, spare11, spare12);
        }
    }

    void SerializerJson::deserializeSysCDef(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysCDefJson) {
        for (rapidjson::SizeType i = 0; i < sysCDefJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysCDefChildNames[] = {"row-id", "con", "obj", "type", nullptr};
                Ctx::checkJsonFields(fileName, sysCDefJson[i], sysCDefChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysCDefJson[i], "row-id");
            typeCon con = Ctx::getJsonFieldU32(fileName, sysCDefJson[i], "con");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysCDefJson[i], "obj");
            typeType type = Ctx::getJsonFieldU16(fileName, sysCDefJson[i], "type");

            metadata->schema->dictSysCDefAdd(rowIdStr, con, obj, type);
        }
    }

    void SerializerJson::deserializeSysCol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysColJson) {
        for (rapidjson::SizeType i = 0; i < sysColJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysColChildNames[] = {"row-id", "obj", "col", "seg-col", "int-col", "name", "type",
                                                         "length", "precision", "scale", "charset-form", "charset-id", "null",
                                                         "property", nullptr};
                Ctx::checkJsonFields(fileName, sysColJson[i], sysColChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysColJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysColJson[i], "obj");
            typeCol col = Ctx::getJsonFieldI16(fileName, sysColJson[i], "col");
            typeCol segCol = Ctx::getJsonFieldI16(fileName, sysColJson[i], "seg-col");
            typeCol intCol = Ctx::getJsonFieldI16(fileName, sysColJson[i], "int-col");
            const char* name_ = Ctx::getJsonFieldS(fileName, SysCol::NAME_LENGTH, sysColJson[i], "name");
            typeType type = Ctx::getJsonFieldU16(fileName, sysColJson[i], "type");
            uint64_t length = Ctx::getJsonFieldU64(fileName, sysColJson[i], "length");
            int64_t precision = Ctx::getJsonFieldI64(fileName, sysColJson[i], "precision");
            int64_t scale = Ctx::getJsonFieldI64(fileName, sysColJson[i], "scale");
            uint64_t charsetForm = Ctx::getJsonFieldU64(fileName, sysColJson[i], "charset-form");
            uint64_t charsetId = Ctx::getJsonFieldU64(fileName, sysColJson[i], "charset-id");
            int64_t null_ = Ctx::getJsonFieldI64(fileName, sysColJson[i], "null");
            const rapidjson::Value& propertyJson = Ctx::getJsonFieldA(fileName, sysColJson[i], "property");
            if (unlikely(propertyJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - property should be an array with 2 elements");
            uint64_t property1 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 0);
            uint64_t property2 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 1);

            metadata->schema->dictSysColAdd(rowIdStr, obj, col, segCol, intCol, name_, type, length, precision, scale, charsetForm, charsetId,
                                            null_, property1, property2);
        }
    }

    void SerializerJson::deserializeSysDeferredStg(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysDeferredStgJson) {
        for (rapidjson::SizeType i = 0; i < sysDeferredStgJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysDeferredStgChildNames[] = {"row-id", "obj", "flags-stg", nullptr};
                Ctx::checkJsonFields(fileName, sysDeferredStgJson[i], sysDeferredStgChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysDeferredStgJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysDeferredStgJson[i], "obj");

            const rapidjson::Value& flagsStgJson = Ctx::getJsonFieldA(fileName, sysDeferredStgJson[i], "flags-stg");
            if (unlikely(flagsStgJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - flags-stg should be an array with 2 elements");
            uint64_t flagsStg1 = Ctx::getJsonFieldU64(fileName, flagsStgJson, "flags-stg", 0);
            uint64_t flagsStg2 = Ctx::getJsonFieldU64(fileName, flagsStgJson, "flags-stg", 1);

            metadata->schema->dictSysDeferredStgAdd(rowIdStr, obj, flagsStg1, flagsStg2);
        }
    }

    void SerializerJson::deserializeSysECol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysEColJson) {
        for (rapidjson::SizeType i = 0; i < sysEColJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysEColChildNames[] = {"row-id", "tab-obj", "col-num", "guard-id", nullptr};
                Ctx::checkJsonFields(fileName, sysEColJson[i], sysEColChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysEColJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysEColJson[i], "tab-obj");
            typeCol colNum = Ctx::getJsonFieldI16(fileName, sysEColJson[i], "col-num");
            typeCol guardId = Ctx::getJsonFieldI16(fileName, sysEColJson[i], "guard-id");

            metadata->schema->dictSysEColAdd(rowIdStr, obj, colNum, guardId);
        }
    }

    void SerializerJson::deserializeSysLob(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobJson) {
        for (rapidjson::SizeType i = 0; i < sysLobJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysLobChildNames[] = {"row-id", "obj", "col", "int-col", "l-obj", "ts", nullptr};
                Ctx::checkJsonFields(fileName, sysLobJson[i], sysLobChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysLobJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysLobJson[i], "obj");
            typeCol col = Ctx::getJsonFieldI16(fileName, sysLobJson[i], "col");
            typeCol intCol = Ctx::getJsonFieldI16(fileName, sysLobJson[i], "int-col");
            typeObj lObj = Ctx::getJsonFieldU32(fileName, sysLobJson[i], "l-obj");
            uint32_t ts = Ctx::getJsonFieldU32(fileName, sysLobJson[i], "ts");

            metadata->schema->dictSysLobAdd(rowIdStr, obj, col, intCol, lObj, ts);
        }
    }

    void SerializerJson::deserializeSysLobCompPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobCompPartJson) {
        for (rapidjson::SizeType i = 0; i < sysLobCompPartJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysLobCompPartChildNames[] = {"row-id", "part-obj", "l-obj", nullptr};
                Ctx::checkJsonFields(fileName, sysLobCompPartJson[i], sysLobCompPartChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysLobCompPartJson[i], "row-id");
            typeObj partObj = Ctx::getJsonFieldU32(fileName, sysLobCompPartJson[i], "part-obj");
            typeObj lObj = Ctx::getJsonFieldU32(fileName, sysLobCompPartJson[i], "l-obj");

            metadata->schema->dictSysLobCompPartAdd(rowIdStr, partObj, lObj);
        }
    }

    void SerializerJson::deserializeSysLobFrag(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobFragJson) {
        for (rapidjson::SizeType i = 0; i < sysLobFragJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysLobFragChildNames[] = {"row-id", "frag-obj", "parent-obj", "ts", nullptr};
                Ctx::checkJsonFields(fileName, sysLobFragJson[i], sysLobFragChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysLobFragJson[i], "row-id");
            typeObj fragObj = Ctx::getJsonFieldU32(fileName, sysLobFragJson[i], "frag-obj");
            typeObj parentObj = Ctx::getJsonFieldU32(fileName, sysLobFragJson[i], "parent-obj");
            uint32_t ts = Ctx::getJsonFieldU32(fileName, sysLobFragJson[i], "ts");

            metadata->schema->dictSysLobFragAdd(rowIdStr, fragObj, parentObj, ts);
        }
    }

    void SerializerJson::deserializeSysObj(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysObjJson) {
        for (rapidjson::SizeType i = 0; i < sysObjJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysObjChildNames[] = {"row-id", "owner", "obj", "data-obj", "type", "name", "flags",
                                                         "single", nullptr};
                Ctx::checkJsonFields(fileName, sysObjJson[i], sysObjChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysObjJson[i], "row-id");
            typeUser owner = Ctx::getJsonFieldU32(fileName, sysObjJson[i], "owner");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysObjJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysObjJson[i], "data-obj");
            typeType type = Ctx::getJsonFieldU16(fileName, sysObjJson[i], "type");
            const char* name_ = Ctx::getJsonFieldS(fileName, SysObj::NAME_LENGTH, sysObjJson[i], "name");

            const rapidjson::Value& flagsJson = Ctx::getJsonFieldA(fileName, sysObjJson[i], "flags");
            if (unlikely(flagsJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - flags should be an array with 2 elements");
            uint64_t flags1 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 0);
            uint64_t flags2 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 1);
            uint64_t single = Ctx::getJsonFieldU64(fileName, sysObjJson[i], "single");

            metadata->schema->dictSysObjAdd(rowIdStr, owner, obj, dataObj, type, name_, flags1, flags2, single != 0u);
        }
    }

    void SerializerJson::deserializeSysTab(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabJson) {
        for (rapidjson::SizeType i = 0; i < sysTabJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysTabChildNames[] = {"row-id", "obj", "data-obj", "ts", "clu-cols", "flags", "property",
                                                         nullptr};
                Ctx::checkJsonFields(fileName, sysTabJson[i], sysTabChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysTabJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "data-obj");
            typeTs ts = 0;
            if (sysTabJson[i].HasMember("ts"))
                ts = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "ts");
            // typeTs ts = Ctx::getJsonFieldU32(fileName, sysTabJson[i], "ts");
            typeCol cluCols = Ctx::getJsonFieldI16(fileName, sysTabJson[i], "clu-cols");

            const rapidjson::Value& flagsJson = Ctx::getJsonFieldA(fileName, sysTabJson[i], "flags");
            if (unlikely(flagsJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - flags should be an array with 2 elements");
            uint64_t flags1 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 0);
            uint64_t flags2 = Ctx::getJsonFieldU64(fileName, flagsJson, "flags", 1);

            const rapidjson::Value& propertyJson = Ctx::getJsonFieldA(fileName, sysTabJson[i], "property");
            if (unlikely(propertyJson.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - property should be an array with 2 elements");
            uint64_t property1 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 0);
            uint64_t property2 = Ctx::getJsonFieldU64(fileName, propertyJson, "property", 1);

            metadata->schema->dictSysTabAdd(rowIdStr, obj, dataObj, ts, cluCols, flags1, flags2, property1, property2);
        }
    }

    void SerializerJson::deserializeSysTabComPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabComPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabComPartJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysTabComPartChildNames[] = {"row-id", "obj", "data-obj", "bo", nullptr};
                Ctx::checkJsonFields(fileName, sysTabComPartJson[i], sysTabComPartChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysTabComPartJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabComPartJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabComPartJson[i], "data-obj");
            typeObj bo = Ctx::getJsonFieldU32(fileName, sysTabComPartJson[i], "bo");

            metadata->schema->dictSysTabComPartAdd(rowIdStr, obj, dataObj, bo);
        }
    }

    void SerializerJson::deserializeSysTabPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabPartJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysTabPartChildNames[] = {"row-id", "obj", "data-obj", "bo", nullptr};
                Ctx::checkJsonFields(fileName, sysTabPartJson[i], sysTabPartChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysTabPartJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabPartJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabPartJson[i], "data-obj");
            typeObj bo = Ctx::getJsonFieldU32(fileName, sysTabPartJson[i], "bo");

            metadata->schema->dictSysTabPartAdd(rowIdStr, obj, dataObj, bo);
        }
    }

    void SerializerJson::deserializeSysTabSubPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabSubPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabSubPartJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysTabSubPartChildNames[] = {"row-id", "obj", "data-obj", "p-obj", nullptr};
                Ctx::checkJsonFields(fileName, sysTabSubPartJson[i], sysTabSubPartChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysTabSubPartJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(fileName, sysTabSubPartJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(fileName, sysTabSubPartJson[i], "data-obj");
            typeObj pObj = Ctx::getJsonFieldU32(fileName, sysTabSubPartJson[i], "p-obj");

            metadata->schema->dictSysTabSubPartAdd(rowIdStr, obj, dataObj, pObj);
        }
    }

    void SerializerJson::deserializeSysTs(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTsJson) {
        for (rapidjson::SizeType i = 0; i < sysTsJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysTsChildNames[] = {"row-id", "ts", "name", "block-size", nullptr};
                Ctx::checkJsonFields(fileName, sysTsJson[i], sysTsChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysTsJson[i], "row-id");
            typeTs ts = Ctx::getJsonFieldU32(fileName, sysTsJson[i], "ts");
            const char* name_ = Ctx::getJsonFieldS(fileName, SysTs::NAME_LENGTH, sysTsJson[i], "name");
            uint32_t blockSize = Ctx::getJsonFieldU32(fileName, sysTsJson[i], "block-size");

            metadata->schema->dictSysTsAdd(rowIdStr, ts, name_, blockSize);
        }
    }

    void SerializerJson::deserializeSysUser(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysUserJson) {
        for (rapidjson::SizeType i = 0; i < sysUserJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* sysUserChildNames[] = {"row-id", "user", "name", "spare1", "single", nullptr};
                Ctx::checkJsonFields(fileName, sysUserJson[i], sysUserChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, sysUserJson[i], "row-id");
            typeUser user = Ctx::getJsonFieldU32(fileName, sysUserJson[i], "user");
            const char* name_ = Ctx::getJsonFieldS(fileName, SysUser::NAME_LENGTH, sysUserJson[i], "name");

            const rapidjson::Value& spare1Json = Ctx::getJsonFieldA(fileName, sysUserJson[i], "spare1");
            if (unlikely(spare1Json.Size() != 2))
                throw DataException(20005, "file: " + fileName + " - spare1 should be an array with 2 elements");
            uint64_t spare11 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 0);
            uint64_t spare12 = Ctx::getJsonFieldU64(fileName, spare1Json, "spare1", 1);
            uint64_t single = Ctx::getJsonFieldU64(fileName, sysUserJson[i], "single");

            metadata->schema->dictSysUserAdd(rowIdStr, user, name_, spare11, spare12, single != 0u);
        }
    }

    void SerializerJson::deserializeXdbTtSet(Metadata* metadata, const std::string& fileName, const rapidjson::Value& xdbTtSetJson) {
        for (rapidjson::SizeType i = 0; i < xdbTtSetJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* xdbTtSetChildNames[] = {"row-id", "guid", "toksuf", "flags", "obj", nullptr};
                Ctx::checkJsonFields(fileName, xdbTtSetJson[i], xdbTtSetChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, xdbTtSetJson[i], "row-id");
            const char* guid = Ctx::getJsonFieldS(fileName, XdbTtSet::GUID_LENGTH, xdbTtSetJson[i], "guid");
            const char* tokSuf = Ctx::getJsonFieldS(fileName, XdbTtSet::TOKSUF_LENGTH, xdbTtSetJson[i], "toksuf");
            uint64_t flags = Ctx::getJsonFieldU64(fileName, xdbTtSetJson[i], "flags");
            uint32_t obj = Ctx::getJsonFieldU32(fileName, xdbTtSetJson[i], "obj");

            metadata->schema->dictXdbTtSetAdd(rowIdStr, guid, tokSuf, flags, obj);
        }
    }

    void SerializerJson::deserializeXdbXNm(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXNmJson) {
        for (rapidjson::SizeType i = 0; i < xdbXNmJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* xdbXNmChildNames[] = {"row-id", "nmspcuri", "id", nullptr};
                Ctx::checkJsonFields(fileName, xdbXNmJson[i], xdbXNmChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, xdbXNmJson[i], "row-id");
            const char* nmSpcUri = Ctx::getJsonFieldS(fileName, XdbXNm::NMSPCURI_LENGTH, xdbXNmJson[i], "nmspcuri");
            const char* id = Ctx::getJsonFieldS(fileName, XdbXNm::ID_LENGTH, xdbXNmJson[i], "id");

            metadata->schema->dictXdbXNmAdd(xmlCtx, rowIdStr, nmSpcUri, id);
        }
    }

    void SerializerJson::deserializeXdbXPt(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXPtJson) {
        for (rapidjson::SizeType i = 0; i < xdbXPtJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* xdbXPtChildNames[] = {"row-id", "path", "id", nullptr};
                Ctx::checkJsonFields(fileName, xdbXPtJson[i], xdbXPtChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, xdbXPtJson[i], "row-id");
            const char* path = Ctx::getJsonFieldS(fileName, XdbXPt::PATH_LENGTH, xdbXPtJson[i], "path");
            const char* id = Ctx::getJsonFieldS(fileName, XdbXPt::ID_LENGTH, xdbXPtJson[i], "id");

            metadata->schema->dictXdbXPtAdd(xmlCtx, rowIdStr, path, id);
        }
    }

    void SerializerJson::deserializeXdbXQn(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXQnJson) {
        for (rapidjson::SizeType i = 0; i < xdbXQnJson.Size(); ++i) {
            if (!metadata->ctx->disableChecksSet(Ctx::DISABLE_CHECKS_JSON_TAGS)) {
                static const char* xdbXQnChildNames[] = {"row-id", "nmspcid", "localname", "flags", "id", nullptr};
                Ctx::checkJsonFields(fileName, xdbXQnJson[i], xdbXQnChildNames);
            }

            const char* rowIdStr = Ctx::getJsonFieldS(fileName, typeRowId::SIZE, xdbXQnJson[i], "row-id");
            const char* nmSpcId = Ctx::getJsonFieldS(fileName, XdbXQn::NMSPCID_LENGTH, xdbXQnJson[i], "nmspcid");
            const char* localName = Ctx::getJsonFieldS(fileName, XdbXQn::LOCALNAME_LENGTH, xdbXQnJson[i], "localname");
            const char* flags = Ctx::getJsonFieldS(fileName, XdbXQn::FLAGS_LENGTH, xdbXQnJson[i], "flags");
            const char* id = Ctx::getJsonFieldS(fileName, XdbXQn::ID_LENGTH, xdbXQnJson[i], "id");

            metadata->schema->dictXdbXQnAdd(xmlCtx, rowIdStr, nmSpcId, localName, flags, id);
        }
    }
}
