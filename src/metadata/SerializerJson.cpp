/* Base class for serialization of metadata to json
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

#include "../common/Ctx.h"
#include "../common/OracleIncarnation.h"
#include "../common/DataException.h"
#include "../common/SysCCol.h"
#include "../common/SysCDef.h"
#include "../common/SysCol.h"
#include "../common/SysDeferredStg.h"
#include "../common/SysECol.h"
#include "../common/SysLob.h"
#include "../common/SysLobCompPart.h"
#include "../common/SysLobFrag.h"
#include "../common/SysObj.h"
#include "../common/SysTab.h"
#include "../common/SysTabComPart.h"
#include "../common/SysTabPart.h"
#include "../common/SysTabSubPart.h"
#include "../common/SysUser.h"
#include "../common/typeRowId.h"
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
        ss << R"({"database":")";
        Ctx::writeEscapeValue(ss, metadata->database);
        ss << R"(","scn":)" << std::dec << metadata->checkpointScn <<
                R"(,"resetlogs":)" << std::dec << metadata->resetlogs <<
                R"(,"activation":)" << std::dec << metadata->activation <<
                R"(,"time":)" << std::dec << metadata->checkpointTime.getVal() <<
                R"(,"seq":)" << std::dec << metadata->sequence <<
                R"(,"offset":)" << std::dec << metadata->checkpointOffset;
        if (metadata->minSequence != ZERO_SEQ) {
            ss << R"(,"min-tran":{)" <<
                    R"("seq":)" << std::dec << metadata->minSequence <<
                    R"(,"offset":)" << std::dec << metadata->minOffset <<
                    R"(,"xid:":")" << metadata->minXid << R"("})";
        }
        ss << R"(,"big-endian":)" << std::dec << (metadata->ctx->isBigEndian() ? 1 : 0) <<
                R"(,"context":")";
        Ctx::writeEscapeValue(ss, metadata->context);
        ss << R"(","con-id":)" << std::dec << metadata->conId <<
                R"(,"con-name":")";
        Ctx::writeEscapeValue(ss, metadata->conName);
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

        ss << R"(","supp-log-db-primary":)" << (metadata->suppLogDbPrimary?1:0) <<
                R"(,"supp-log-db-all":)" << (metadata->suppLogDbAll?1:0) <<
                R"(,)" SERIALIZER_ENDL << R"("online-redo":[)";

        int64_t prevGroup = -2;
        for (RedoLog* redoLog: metadata->redoLogs) {
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
        for (OracleIncarnation* oi : metadata->oracleIncarnations) {
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

        // Schema did not change since last checkpoint file
        if (!storeSchema) {
            ss << R"("schema-ref-scn":)" << metadata->schema->refScn << "}";
            return;
        }

        metadata->schema->refScn = metadata->checkpointScn;
        ss << R"("schema-scn":)" << metadata->schema->scn << "," SERIALIZER_ENDL;

        // SYS.CCOL$
        ss << R"("sys-ccol":[)";
        hasPrev = false;
        for (auto sysCColMapRowIdIt : metadata->schema->sysCColMapRowId) {
            SysCCol* sysCCol = sysCColMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysCCol->rowId <<
                    R"(","con":)" << std::dec << sysCCol->con <<
                    R"(,"int-col":)" << std::dec << sysCCol->intCol <<
                    R"(,"obj":)" << std::dec << sysCCol->obj <<
                    R"(,"spare1":)" << std::dec << sysCCol->spare1 << "}";
        }

        // SYS.CDEF$
        ss << "]," SERIALIZER_ENDL << R"("sys-cdef":[)";
        hasPrev = false;
        for (auto sysCDefMapRowIdIt : metadata->schema->sysCDefMapRowId) {
            SysCDef* sysCDef = sysCDefMapRowIdIt.second;

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
        for (auto sysColMapRowIdIt : metadata->schema->sysColMapRowId) {
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
            Ctx::writeEscapeValue(ss,  sysCol->name);
            ss << R"(","type":)" << std::dec << sysCol->type <<
                    R"(,"length":)" << std::dec << sysCol->length <<
                    R"(,"precision":)" << std::dec << sysCol->precision <<
                    R"(,"scale":)" << std::dec << sysCol->scale <<
                    R"(,"charset-form":)" << std::dec << sysCol->charsetForm <<
                    R"(,"charset-id":)" << std::dec << sysCol->charsetId <<
                    R"(,"null":)" << std::dec << sysCol->null_ <<
                    R"(,"property":)" << sysCol->property << "}";
        }

        // SYS.DEFERRED_STG$
        ss << "]," SERIALIZER_ENDL << R"("sys-deferredstg":[)";
        hasPrev = false;
        for (auto sysDeferredStgMapRowIdIt : metadata->schema->sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysDeferredStg->rowId <<
                    R"(","obj":)" << std::dec << sysDeferredStg->obj <<
                    R"(,"flags-stg":)" << std::dec << sysDeferredStg->flagsStg << "}";
        }

        // SYS.ECOL$
        ss << "]," SERIALIZER_ENDL << R"("sys-ecol":[)";
        hasPrev = false;
        for (auto sysEColMapRowIdIt : metadata->schema->sysEColMapRowId) {
            SysECol* sysECol = sysEColMapRowIdIt.second;

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
        for (auto sysLobMapRowIdIt : metadata->schema->sysLobMapRowId) {
            SysLob* sysLob = sysLobMapRowIdIt.second;

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
        for (auto sysLobCompPartMapRowIdIt : metadata->schema->sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt.second;

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
        for (auto sysLobFragMapRowIdIt : metadata->schema->sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt.second;

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
        for (auto sysObjMapRowIdIt : metadata->schema->sysObjMapRowId) {
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
                    R"(,"flags":)" << std::dec << sysObj->flags <<
                    R"(,"single":)" << std::dec << static_cast<uint64_t>(sysObj->single) << "}";
        }

        // SYS.TAB$
        ss << "]," SERIALIZER_ENDL << R"("sys-tab":[)";
        hasPrev = false;
        for (auto sysTabMapRowIdIt : metadata->schema->sysTabMapRowId) {
            SysTab* sysTab = sysTabMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysTab->rowId <<
                    R"(","obj":)" << std::dec << sysTab->obj <<
                    R"(,"data-obj":)" << std::dec << sysTab->dataObj <<
                    R"(,"clu-cols":)" << std::dec << sysTab->cluCols <<
                    R"(,"flags":)" << std::dec << sysTab->flags <<
                    R"(,"property":)" << std::dec << sysTab->property << "}";
        }

        // SYS.TABCOMPART$
        ss << "]," SERIALIZER_ENDL << R"("sys-tabcompart":[)";
        hasPrev = false;
        for (auto sysTabComPartMapRowIdIt : metadata->schema->sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt.second;

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
        for (auto sysTabPartMapRowIdIt : metadata->schema->sysTabPartMapRowId) {
            SysTabPart* sysTabPart = sysTabPartMapRowIdIt.second;

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
        for (auto sysTabSubPartMapRowIdIt : metadata->schema->sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt.second;

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
        for (auto sysTsMapRowIdIt : metadata->schema->sysTsMapRowId) {
            SysTs* sysTs = sysTsMapRowIdIt.second;

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
        for (auto sysUserMapRowIdIt : metadata->schema->sysUserMapRowId) {
            SysUser* sysUser = sysUserMapRowIdIt.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SERIALIZER_ENDL << R"({"row-id":")" << sysUser->rowId <<
                    R"(","user":)" << std::dec << sysUser->user <<
                    R"(,"name":")";
            Ctx::writeEscapeValue(ss, sysUser->name);
            ss << R"(","spare1":)" << std::dec << sysUser->spare1 <<
                    R"(,"single":)" << std::dec << static_cast<uint64_t>(sysUser->single) << "}";
        }

        ss << "]}";
    }

    bool SerializerJson::deserialize(Metadata* metadata, const std::string& ss, const std::string& name, std::set<std::string>& msgs, bool loadMetadata,
                                     bool loadSchema) {
        try {
            rapidjson::Document document;
            if (ss.length() == 0 || document.Parse(ss.c_str()).HasParseError())
                throw DataException("parsing " + name + " at offset: " + std::to_string(document.GetErrorOffset()) + ", message: " +
                        GetParseError_En(document.GetParseError()));

            {
                std::unique_lock<std::mutex> lck(metadata->mtx);

                if (loadMetadata) {
                    metadata->checkpointScn = Ctx::getJsonFieldU64(name, document, "scn");

                    if (document.HasMember("min-tran")) {
                        const rapidjson::Value& minTranJson = Ctx::getJsonFieldO(name, document, "min-tran");
                        metadata->sequence = Ctx::getJsonFieldU32(name, minTranJson, "seq");
                        metadata->offset = Ctx::getJsonFieldU64(name, minTranJson, "offset");
                    } else {
                        metadata->sequence = Ctx::getJsonFieldU32(name, document, "seq");
                        metadata->offset = Ctx::getJsonFieldU64(name, document, "offset");
                    }

                    if ((metadata->offset & 511) != 0)
                        throw DataException("invalid offset for: " + name + " - " + std::to_string(metadata->offset) +
                                " is not a multiplication of 512 - skipping file");

                    metadata->minSequence = ZERO_SEQ;
                    metadata->minOffset = 0;
                    metadata->minXid = 0;
                    metadata->lastCheckpointScn = ZERO_SCN;
                    metadata->lastSequence = ZERO_SEQ;
                    metadata->lastCheckpointOffset = 0;
                    metadata->lastCheckpointTime = 0;
                    metadata->lastCheckpointBytes = 0;

                    if (!metadata->onlineData) {
                        // Database metadata
                        metadata->database = Ctx::getJsonFieldS(name, JSON_PARAMETER_LENGTH, document, "database");
                        metadata->resetlogs = Ctx::getJsonFieldU32(name, document, "resetlogs");
                        metadata->activation = Ctx::getJsonFieldU32(name, document, "activation");
                        int64_t bigEndian = Ctx::getJsonFieldU64(name, document, "big-endian");
                        if (bigEndian == 1)
                            metadata->ctx->setBigEndian();
                        metadata->context = Ctx::getJsonFieldS(name, VCONTEXT_LENGTH, document, "context");
                        metadata->conId = Ctx::getJsonFieldI16(name, document, "con-id");
                        metadata->conName = Ctx::getJsonFieldS(name, VCONTEXT_LENGTH, document, "con-name");
                        metadata->dbRecoveryFileDest = Ctx::getJsonFieldS(name, VPARAMETER_LENGTH, document, "db-recovery-file-dest");
                        metadata->dbBlockChecksum = Ctx::getJsonFieldS(name, VPARAMETER_LENGTH, document, "db-block-checksum");
                        if (!metadata->logArchiveFormatCustom)
                            metadata->logArchiveFormat = Ctx::getJsonFieldS(name, VPARAMETER_LENGTH, document, "log-archive-format");
                        metadata->logArchiveDest = Ctx::getJsonFieldS(name, VPARAMETER_LENGTH, document, "log-archive-dest");
                        metadata->nlsCharacterSet = Ctx::getJsonFieldS(name, VPROPERTY_LENGTH, document, "nls-character-set");
                        metadata->nlsNcharCharacterSet = Ctx::getJsonFieldS(name, VPROPERTY_LENGTH, document,
                                                                            "nls-nchar-character-set");
                        metadata->setNlsCharset(metadata->nlsCharacterSet, metadata->nlsNcharCharacterSet);
                        metadata->suppLogDbPrimary = Ctx::getJsonFieldU64(name, document, "supp-log-db-primary");
                        metadata->suppLogDbAll = Ctx::getJsonFieldU64(name, document, "supp-log-db-all");

                        const rapidjson::Value& onlineRedoJson = Ctx::getJsonFieldA(name, document, "online-redo");
                        for (rapidjson::SizeType i = 0; i < onlineRedoJson.Size(); ++i) {
                            int64_t group = Ctx::getJsonFieldI64(name, onlineRedoJson[i], "group");
                            const rapidjson::Value& path = Ctx::getJsonFieldA(name, onlineRedoJson[i], "path");

                            for (rapidjson::SizeType j = 0; j < path.Size(); ++j) {
                                const rapidjson::Value& pathVal = path[j];
                                auto redoLog = new RedoLog(group, pathVal.GetString());
                                metadata->redoLogs.insert(redoLog);
                            }
                        }

                        const rapidjson::Value& incarnationsJson = Ctx::getJsonFieldA(name, document, "incarnations");
                        for (rapidjson::SizeType i = 0; i < incarnationsJson.Size(); ++i) {
                            uint32_t incarnation = Ctx::getJsonFieldU32(name, incarnationsJson[i], "incarnation");
                            typeScn resetlogsScn = Ctx::getJsonFieldU64(name, incarnationsJson[i], "resetlogs-scn");
                            typeScn priorResetlogsScn = Ctx::getJsonFieldU64(name, incarnationsJson[i], "prior-resetlogs-scn");
                            const char* status = Ctx::getJsonFieldS(name, 128, incarnationsJson[i], "status");
                            typeResetlogs resetlogs = Ctx::getJsonFieldU32(name, incarnationsJson[i], "resetlogs");
                            uint32_t priorIncarnation = Ctx::getJsonFieldU32(name, incarnationsJson[i], "prior-incarnation");

                            auto oi = new OracleIncarnation(incarnation, resetlogsScn, priorResetlogsScn,
                                                            status, resetlogs, priorIncarnation);
                            metadata->oracleIncarnations.insert(oi);

                            if (oi->current)
                                metadata->oracleIncarnationCurrent = oi;
                            else
                                metadata->oracleIncarnationCurrent = nullptr;
                        }
                    }

                    const rapidjson::Value& usersJson = Ctx::getJsonFieldA(name, document, "users");
                    for (rapidjson::SizeType i = 0; i < usersJson.Size(); ++i) {
                        const rapidjson::Value& userJson = usersJson[i];;
                        metadata->users.insert(userJson.GetString());
                    }
                }

                if (loadSchema) {
                    // Schema referenced to other checkpoint file
                    if (document.HasMember("schema-ref-scn")) {
                        metadata->schema->scn = ZERO_SCN;
                        metadata->schema->refScn = Ctx::getJsonFieldU64(name, document, "schema-ref-scn");

                    } else {
                        metadata->schema->scn = Ctx::getJsonFieldU64(name, document, "schema-scn");
                        metadata->schema->refScn = ZERO_SCN;

                        deserializeSysUser(metadata, name, Ctx::getJsonFieldA(name, document, "sys-user"));
                        deserializeSysObj(metadata, name, Ctx::getJsonFieldA(name, document, "sys-obj"));
                        deserializeSysCol(metadata, name, Ctx::getJsonFieldA(name, document, "sys-col"));
                        deserializeSysCCol(metadata, name, Ctx::getJsonFieldA(name, document, "sys-ccol"));
                        deserializeSysCDef(metadata, name, Ctx::getJsonFieldA(name, document, "sys-cdef"));
                        deserializeSysDeferredStg(metadata, name, Ctx::getJsonFieldA(name, document, "sys-deferredstg"));
                        deserializeSysECol(metadata, name, Ctx::getJsonFieldA(name, document, "sys-ecol"));
                        deserializeSysLob(metadata, name, Ctx::getJsonFieldA(name, document, "sys-lob"));
                        deserializeSysLobCompPart(metadata, name, Ctx::getJsonFieldA(name, document, "sys-lob-comp-part"));
                        deserializeSysLobFrag(metadata, name, Ctx::getJsonFieldA(name, document, "sys-lob-frag"));
                        deserializeSysTab(metadata, name, Ctx::getJsonFieldA(name, document, "sys-tab"));
                        deserializeSysTabPart(metadata, name, Ctx::getJsonFieldA(name, document, "sys-tabpart"));
                        deserializeSysTabComPart(metadata, name, Ctx::getJsonFieldA(name, document, "sys-tabcompart"));
                        deserializeSysTabSubPart(metadata, name, Ctx::getJsonFieldA(name, document, "sys-tabsubpart"));
                        deserializeSysTs(metadata, name, Ctx::getJsonFieldA(name, document, "sys-ts"));
                    }

                    for (SchemaElement *element: metadata->schemaElements) {
                        if (metadata->ctx->trace >= TRACE_DEBUG)
                            msgs.insert(
                                    "- creating table schema for owner: " + element->owner + " table: " + element->table +
                                    " options: " + std::to_string(element->options));

                        if ((metadata->ctx->flags & REDO_FLAGS_ADAPTIVE_SCHEMA) == 0) {
                            if ((element->options & OPTIONS_SYSTEM_TABLE) == 0 && metadata->users.find(element->owner) == metadata->users.end())
                                throw DataException("owner '" + std::string(element->owner) + "' is missing in schema file: " + name +
                                                    " - recreate schema file (delete old file and force creation of new)");
                        }

                        metadata->schema->buildMaps(element->owner, element->table, element->keys, element->keysStr,
                                                    element->options, msgs, metadata->suppLogDbPrimary,
                                                    metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                                    metadata->defaultCharacterNcharMapId);
                    }

                    metadata->schema->resetTouched();
                    metadata->schema->loaded = true;
                    metadata->allowedCheckpoints = true;
                    return true;
                }
            }
        } catch (DataException& ex) {
            ERROR(ex.msg)
            return false;
        }
        return true;
    }

    void SerializerJson::deserializeSysCCol(Metadata* metadata, const std::string& name, const rapidjson::Value& sysCColJson) {
        for (rapidjson::SizeType i = 0; i < sysCColJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysCColJson[i], "row-id");
            typeCon con = Ctx::getJsonFieldU32(name, sysCColJson[i], "con");
            typeCol intCol = Ctx::getJsonFieldI16(name, sysCColJson[i], "int-col");
            typeObj obj = Ctx::getJsonFieldU32(name, sysCColJson[i], "obj");
            const rapidjson::Value& spare1Json = Ctx::getJsonFieldA(name, sysCColJson[i], "spare1");
            if (spare1Json.Size() != 2)
                throw DataException("bad JSON in " + name + ", spare1 should be an array with 2 elements");
            uint64_t spare11 = Ctx::getJsonFieldU64(name, spare1Json, "spare1", 0);
            uint64_t spare12 = Ctx::getJsonFieldU64(name, spare1Json, "spare1", 1);

            metadata->schema->dictSysCColAdd(rowId, con, intCol, obj, spare11, spare12);
        }
    }

    void SerializerJson::deserializeSysCDef(Metadata* metadata, const std::string& name, const rapidjson::Value& sysCDefJson) {
        for (rapidjson::SizeType i = 0; i < sysCDefJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysCDefJson[i], "row-id");
            typeCon con = Ctx::getJsonFieldU32(name, sysCDefJson[i], "con");
            typeObj obj = Ctx::getJsonFieldU32(name, sysCDefJson[i], "obj");
            typeType type = Ctx::getJsonFieldU16(name, sysCDefJson[i], "type");

            metadata->schema->dictSysCDefAdd(rowId, con, obj, type);
        }
    }

    void SerializerJson::deserializeSysCol(Metadata* metadata, const std::string& name, const rapidjson::Value& sysColJson) {
        for (rapidjson::SizeType i = 0; i < sysColJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysColJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysColJson[i], "obj");
            typeCol col = Ctx::getJsonFieldI16(name, sysColJson[i], "col");
            typeCol segCol = Ctx::getJsonFieldI16(name, sysColJson[i], "seg-col");
            typeCol intCol = Ctx::getJsonFieldI16(name, sysColJson[i], "int-col");
            const char* name_ = Ctx::getJsonFieldS(name, SYS_COL_NAME_LENGTH, sysColJson[i], "name");
            typeType type = Ctx::getJsonFieldU16(name, sysColJson[i], "type");
            uint64_t length = Ctx::getJsonFieldU64(name, sysColJson[i], "length");
            int64_t precision = Ctx::getJsonFieldI64(name, sysColJson[i], "precision");
            int64_t scale = Ctx::getJsonFieldI64(name, sysColJson[i], "scale");
            uint64_t charsetForm = Ctx::getJsonFieldU64(name, sysColJson[i], "charset-form");
            uint64_t charsetId = Ctx::getJsonFieldU64(name, sysColJson[i], "charset-id");
            int64_t null_ = Ctx::getJsonFieldI64(name, sysColJson[i], "null");
            const rapidjson::Value& propertyJson = Ctx::getJsonFieldA(name, sysColJson[i], "property");
            if (propertyJson.Size() != 2)
                throw DataException("bad JSON in " + name + ", property should be an array with 2 elements");
            uint64_t property1 = Ctx::getJsonFieldU64(name, propertyJson, "property", 0);
            uint64_t property2 = Ctx::getJsonFieldU64(name, propertyJson, "property", 1);

            metadata->schema->dictSysColAdd(rowId, obj, col, segCol, intCol, name_, type, length, precision, scale, charsetForm, charsetId,
                                            null_ != 0, property1, property2);
        }
    }

    void SerializerJson::deserializeSysDeferredStg(Metadata* metadata, const std::string& name, const rapidjson::Value& sysDeferredStgJson) {
        for (rapidjson::SizeType i = 0; i < sysDeferredStgJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysDeferredStgJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysDeferredStgJson[i], "obj");

            const rapidjson::Value& flagsStgJson = Ctx::getJsonFieldA(name, sysDeferredStgJson[i], "flags-stg");
            if (flagsStgJson.Size() != 2)
                throw DataException("bad JSON in " + name + ", flags-stg should be an array with 2 elements");
            uint64_t flagsStg1 = Ctx::getJsonFieldU64(name, flagsStgJson, "flags-stg", 0);
            uint64_t flagsStg2 = Ctx::getJsonFieldU64(name, flagsStgJson, "flags-stg", 1);

            metadata->schema->dictSysDeferredStgAdd(rowId, obj, flagsStg1, flagsStg2);
        }
    }

    void SerializerJson::deserializeSysECol(Metadata* metadata, const std::string& name, const rapidjson::Value& sysEColJson) {
        for (rapidjson::SizeType i = 0; i < sysEColJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysEColJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysEColJson[i], "tab-obj");
            typeCol colNum = Ctx::getJsonFieldI16(name, sysEColJson[i], "col-num");
            typeCol guardId = Ctx::getJsonFieldI16(name, sysEColJson[i], "guard-id");

            metadata->schema->dictSysEColAdd(rowId, obj, colNum, guardId);
        }
    }

    void SerializerJson::deserializeSysLob(Metadata* metadata, const std::string& name, const rapidjson::Value& sysLobJson) {
        for (rapidjson::SizeType i = 0; i < sysLobJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysLobJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysLobJson[i], "obj");
            typeCol col = Ctx::getJsonFieldI16(name, sysLobJson[i], "col");
            typeCol intCol = Ctx::getJsonFieldI16(name, sysLobJson[i], "int-col");
            typeObj lObj = Ctx::getJsonFieldU32(name, sysLobJson[i], "l-obj");
            uint32_t ts = Ctx::getJsonFieldU32(name, sysLobJson[i], "ts");

            metadata->schema->dictSysLobAdd(rowId, obj, col, intCol, lObj, ts);
        }
    }

    void SerializerJson::deserializeSysLobCompPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysLobCompPartJson) {
        for (rapidjson::SizeType i = 0; i < sysLobCompPartJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysLobCompPartJson[i], "row-id");
            typeObj partObj = Ctx::getJsonFieldU32(name, sysLobCompPartJson[i], "part-obj");
            typeObj lObj = Ctx::getJsonFieldU32(name, sysLobCompPartJson[i], "l-obj");

            metadata->schema->dictSysLobCompPartAdd(rowId, partObj, lObj);
        }
    }

    void SerializerJson::deserializeSysLobFrag(Metadata* metadata, const std::string& name, const rapidjson::Value& sysLobFragJson) {
        for (rapidjson::SizeType i = 0; i < sysLobFragJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysLobFragJson[i], "row-id");
            typeObj fragObj = Ctx::getJsonFieldU32(name, sysLobFragJson[i], "frag-obj");
            typeObj parentObj = Ctx::getJsonFieldU32(name, sysLobFragJson[i], "parent-obj");
            uint32_t ts = Ctx::getJsonFieldU32(name, sysLobFragJson[i], "ts");

            metadata->schema->dictSysLobFragAdd(rowId, fragObj, parentObj, ts);
        }
    }

    void SerializerJson::deserializeSysObj(Metadata* metadata, const std::string& name, const rapidjson::Value& sysObjJson) {
        for (rapidjson::SizeType i = 0; i < sysObjJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysObjJson[i], "row-id");
            typeUser owner = Ctx::getJsonFieldU32(name, sysObjJson[i], "owner");
            typeObj obj = Ctx::getJsonFieldU32(name, sysObjJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(name, sysObjJson[i], "data-obj");
            typeType type = Ctx::getJsonFieldU16(name, sysObjJson[i], "type");
            const char* name_ = Ctx::getJsonFieldS(name, SYS_OBJ_NAME_LENGTH, sysObjJson[i], "name");

            const rapidjson::Value& flagsJson = Ctx::getJsonFieldA(name, sysObjJson[i], "flags");
            if (flagsJson.Size() != 2)
                throw DataException("bad Json in " + name + ", flags should be an array with 2 elements");
            uint64_t flags1 = Ctx::getJsonFieldU64(name, flagsJson, "flags", 0);
            uint64_t flags2 = Ctx::getJsonFieldU64(name, flagsJson, "flags", 1);
            uint64_t single = Ctx::getJsonFieldU64(name, sysObjJson[i], "single");

            metadata->schema->dictSysObjAdd(rowId, owner, obj, dataObj, type, name_, flags1, flags2, single != 0u);
        }
    }

    void SerializerJson::deserializeSysTab(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabJson) {
        for (rapidjson::SizeType i = 0; i < sysTabJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysTabJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysTabJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(name, sysTabJson[i], "data-obj");
            typeCol cluCols = Ctx::getJsonFieldI16(name, sysTabJson[i], "clu-cols");

            const rapidjson::Value& flagsJson = Ctx::getJsonFieldA(name, sysTabJson[i], "flags");
            if (flagsJson.Size() != 2)
                throw DataException("bad JSON in " + name + ", flags should be an array with 2 elements");
            uint64_t flags1 = Ctx::getJsonFieldU64(name, flagsJson, "flags", 0);
            uint64_t flags2 = Ctx::getJsonFieldU64(name, flagsJson, "flags", 1);

            const rapidjson::Value& propertyJson = Ctx::getJsonFieldA(name, sysTabJson[i], "property");
            if (propertyJson.Size() != 2)
                throw DataException("bad Json in " + name + ", property should be an array with 2 elements");
            uint64_t property1 = Ctx::getJsonFieldU64(name, propertyJson, "property", 0);
            uint64_t property2 = Ctx::getJsonFieldU64(name, propertyJson, "property", 1);

            metadata->schema->dictSysTabAdd(rowId, obj, dataObj, cluCols, flags1, flags2, property1, property2);
        }
    }

    void SerializerJson::deserializeSysTabComPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabComPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabComPartJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysTabComPartJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysTabComPartJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(name, sysTabComPartJson[i], "data-obj");
            typeObj bo = Ctx::getJsonFieldU32(name, sysTabComPartJson[i], "bo");

            metadata->schema->dictSysTabComPartAdd(rowId, obj, dataObj, bo);
        }
    }

    void SerializerJson::deserializeSysTabPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabPartJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysTabPartJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysTabPartJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(name, sysTabPartJson[i], "data-obj");
            typeObj bo = Ctx::getJsonFieldU32(name, sysTabPartJson[i], "bo");

            metadata->schema->dictSysTabPartAdd(rowId, obj, dataObj, bo);
        }
    }

    void SerializerJson::deserializeSysTabSubPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabSubPartJson) {
        for (rapidjson::SizeType i = 0; i < sysTabSubPartJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysTabSubPartJson[i], "row-id");
            typeObj obj = Ctx::getJsonFieldU32(name, sysTabSubPartJson[i], "obj");
            typeDataObj dataObj = Ctx::getJsonFieldU32(name, sysTabSubPartJson[i], "data-obj");
            typeObj pObj = Ctx::getJsonFieldU32(name, sysTabSubPartJson[i], "p-obj");

            metadata->schema->dictSysTabSubPartAdd(rowId, obj, dataObj, pObj);
        }
    }

    void SerializerJson::deserializeSysTs(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTsJson) {
        for (rapidjson::SizeType i = 0; i < sysTsJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysTsJson[i], "row-id");
            typeTs ts = Ctx::getJsonFieldU32(name, sysTsJson[i], "ts");
            const char* name_ = Ctx::getJsonFieldS(name, SYS_TS_NAME_LENGTH, sysTsJson[i], "name");
            uint32_t blockSize = Ctx::getJsonFieldU32(name, sysTsJson[i], "block-size");

            metadata->schema->dictSysTsAdd(rowId, ts, name_, blockSize);
        }
    }

    void SerializerJson::deserializeSysUser(Metadata* metadata, const std::string& name, const rapidjson::Value& sysUserJson) {
        for (rapidjson::SizeType i = 0; i < sysUserJson.Size(); ++i) {
            const char* rowId = Ctx::getJsonFieldS(name, ROWID_LENGTH, sysUserJson[i], "row-id");
            typeUser user = Ctx::getJsonFieldU32(name, sysUserJson[i], "user");
            const char* name_ = Ctx::getJsonFieldS(name, SYS_USER_NAME_LENGTH, sysUserJson[i], "name");

            const rapidjson::Value& spare1Json = Ctx::getJsonFieldA(name, sysUserJson[i], "spare1");
            if (spare1Json.Size() != 2)
                throw DataException("bad JSON in " + name + ", spare1 should be an array with 2 elements");
            uint64_t spare11 = Ctx::getJsonFieldU64(name, spare1Json, "spare1", 0);
            uint64_t spare12 = Ctx::getJsonFieldU64(name, spare1Json, "spare1", 1);
            uint64_t single = Ctx::getJsonFieldU64(name, sysUserJson[i], "single");

            metadata->schema->dictSysUserAdd(rowId, user, name_, spare11, spare12, single != 0u, true);
        }
    }
}
