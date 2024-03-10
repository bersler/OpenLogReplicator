/* Base class for handling of schema
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

#include <cstring>
#include <vector>
#include <regex>

#include "../common/Ctx.h"
#include "../common/OracleColumn.h"
#include "../common/OracleLob.h"
#include "../common/OracleTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/DataException.h"
#include "../locales/Locales.h"
#include "Schema.h"
#include "SchemaElement.h"

namespace OpenLogReplicator {
    Schema::Schema(Ctx* newCtx, Locales* newLocales) :
            ctx(newCtx),
            locales(newLocales),
            sysUserAdaptive(sysUserRowId, 0, "", 0, 0, false),
            sysCColTmp(nullptr),
            sysCDefTmp(nullptr),
            sysColTmp(nullptr),
            sysDeferredStgTmp(nullptr),
            sysEColTmp(nullptr),
            sysLobTmp(nullptr),
            sysLobCompPartTmp(nullptr),
            sysLobFragTmp(nullptr),
            sysObjTmp(nullptr),
            sysTabTmp(nullptr),
            sysTabComPartTmp(nullptr),
            sysTabPartTmp(nullptr),
            sysTabSubPartTmp(nullptr),
            sysTsTmp(nullptr),
            sysUserTmp(nullptr),
            xdbTtSetTmp(nullptr),
            xdbXNmTmp(nullptr),
            xdbXPtTmp(nullptr),
            xdbXQnTmp(nullptr),
            scn(ZERO_SCN),
            refScn(ZERO_SCN),
            loaded(false),
            xmlCtxDefault(nullptr),
            columnTmp(nullptr),
            lobTmp(nullptr),
            tableTmp(nullptr),
            touched(false) {
    }

    Schema::~Schema() {
        if (sysCColTmp != nullptr) {
            delete sysCColTmp;
            sysCColTmp = nullptr;
        }

        if (sysCColTmp != nullptr) {
            delete sysCColTmp;
            sysCColTmp = nullptr;
        }

        if (sysCDefTmp != nullptr) {
            delete sysCDefTmp;
            sysCDefTmp = nullptr;
        }

        if (sysColTmp != nullptr) {
            delete sysColTmp;
            sysColTmp = nullptr;
        }

        if (sysDeferredStgTmp != nullptr) {
            delete sysDeferredStgTmp;
            sysDeferredStgTmp = nullptr;
        }

        if (sysEColTmp != nullptr) {
            delete sysEColTmp;
            sysEColTmp = nullptr;
        }

        if (sysLobTmp != nullptr) {
            delete sysLobTmp;
            sysLobTmp = nullptr;
        }

        if (sysLobCompPartTmp != nullptr) {
            delete sysLobCompPartTmp;
            sysLobCompPartTmp = nullptr;
        }

        if (sysLobFragTmp != nullptr) {
            delete sysLobFragTmp;
            sysLobFragTmp = nullptr;
        }

        if (sysObjTmp != nullptr) {
            delete sysObjTmp;
            sysObjTmp = nullptr;
        }

        if (sysTabTmp != nullptr) {
            delete sysTabTmp;
            sysTabTmp = nullptr;
        }

        if (sysTabComPartTmp != nullptr) {
            delete sysTabComPartTmp;
            sysTabComPartTmp = nullptr;
        }

        if (sysTabPartTmp != nullptr) {
            delete sysTabPartTmp;
            sysTabPartTmp = nullptr;
        }

        if (sysTabSubPartTmp != nullptr) {
            delete sysTabSubPartTmp;
            sysTabSubPartTmp = nullptr;
        }

        if (sysTsTmp != nullptr) {
            delete sysTsTmp;
            sysTsTmp = nullptr;
        }

        if (sysUserTmp != nullptr) {
            delete sysUserTmp;
            sysUserTmp = nullptr;
        }

        if (xdbTtSetTmp != nullptr) {
            delete xdbTtSetTmp;
            xdbTtSetTmp = nullptr;
        }

        if (xdbXNmTmp != nullptr) {
            delete xdbXNmTmp;
            xdbXNmTmp = nullptr;
        }

        if (xdbXPtTmp != nullptr) {
            delete xdbXPtTmp;
            xdbXPtTmp = nullptr;
        }

        if (xdbXQnTmp != nullptr) {
            delete xdbXQnTmp;
            xdbXQnTmp = nullptr;
        }

        purgeMetadata();
        purgeDicts();
    }

    void Schema::purgeMetadata() {
        if (columnTmp != nullptr) {
            delete columnTmp;
            columnTmp = nullptr;
        }

        if (lobTmp != nullptr) {
            delete lobTmp;
            lobTmp = nullptr;
        }

        if (tableTmp != nullptr) {
            delete tableTmp;
            tableTmp = nullptr;
        }

        while (!tableMap.empty()) {
            auto tableMapTt = tableMap.cbegin();
            OracleTable* table = tableMapTt->second;
            removeTableFromDict(table);
            delete table;
        }

        if (!lobPartitionMap.empty())
            ctx->error(50029, "schema lob partition map not empty, left: " + std::to_string(lobPartitionMap.size()) + " at exit");
        lobPartitionMap.clear();
        if (!lobIndexMap.empty())
            ctx->error(50029, "schema lob index map not empty, left: " + std::to_string(lobIndexMap.size()) + " at exit");
        lobIndexMap.clear();
        if (!tablePartitionMap.empty())
            ctx->error(50029, "schema table partition map not empty, left: " + std::to_string(tablePartitionMap.size()) + " at exit");
        tablePartitionMap.clear();

        tablesTouched.clear();
        identifiersTouched.clear();
    }

    void Schema::purgeDicts() {
        // SYS.CCOL$
        while (!sysCColMapRowId.empty()) {
            auto sysCColMapRowIdIt = sysCColMapRowId.cbegin();
            SysCCol* sysCCol = sysCColMapRowIdIt->second;
            dictSysCColDrop(sysCCol);
            delete sysCCol;
        }
        if (!sysCColMapKey.empty())
            ctx->error(50029, "key map SYS.CCOL$ not empty, left: " + std::to_string(sysCColMapKey.size()) + " at exit");

        // SYS.CDEF$
        while (!sysCDefMapRowId.empty()) {
            auto sysCDefMapRowIdIt = sysCDefMapRowId.cbegin();
            SysCDef* sysCDef = sysCDefMapRowIdIt->second;
            dictSysCDefDrop(sysCDef);
            delete sysCDef;
        }
        if (!sysCDefMapCon.empty())
            ctx->error(50029, "con# map SYS.CDEF$ not empty, left: " + std::to_string(sysCDefMapCon.size()) + " at exit");
        if (!sysCDefMapKey.empty())
            ctx->error(50029, "key map SYS.CDEF$ not empty, left: " + std::to_string(sysCDefMapKey.size()) + " at exit");

        // SYS.COL$
        while (!sysColMapRowId.empty()) {
            auto sysColMapRowIdIt = sysColMapRowId.cbegin();
            SysCol* sysCol = sysColMapRowIdIt->second;
            dictSysColDrop(sysCol);
            delete sysCol;
        }
        if (!sysColMapSeg.empty())
            ctx->error(50029, "seg# map SYS.COL$ not empty, left: " + std::to_string(sysColMapSeg.size()) + " at exit");

        // SYS.DEFERRED_STG$
        while (!sysDeferredStgMapRowId.empty()) {
            auto sysDeferredStgMapRowIdIt = sysDeferredStgMapRowId.cbegin();
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt->second;
            dictSysDeferredStgDrop(sysDeferredStg);
            delete sysDeferredStg;
        }
        if (!sysDeferredStgMapObj.empty())
            ctx->error(50029, "obj map SYS.DEFERRED_STG$ not empty, left: " + std::to_string(sysDeferredStgMapObj.size()) + " at exit");

        // SYS.ECOL$
        while (!sysEColMapRowId.empty()) {
            auto sysEColMapRowIdIt = sysEColMapRowId.cbegin();
            SysECol* sysECol = sysEColMapRowIdIt->second;
            dictSysEColDrop(sysECol);
            delete sysECol;
        }
        if (!sysEColMapKey.empty())
            ctx->error(50029, "key map SYS.ECOL$ not empty, left: " + std::to_string(sysEColMapKey.size()) + " at exit");

        // SYS.LOB$
        while (!sysLobMapRowId.empty()) {
            auto sysLobMapRowIdIt = sysLobMapRowId.cbegin();
            SysLob* sysLob = sysLobMapRowIdIt->second;
            dictSysLobDrop(sysLob);
            delete sysLob;
        }
        if (!sysLobMapLObj.empty())
            ctx->error(50029, "lobj# map SYS.LOB$ not empty, left: " + std::to_string(sysLobMapLObj.size()) + " at exit");
        if (!sysLobMapKey.empty())
            ctx->error(50029, "key map SYS.LOB$ not empty, left: " + std::to_string(sysLobMapKey.size()) + " at exit");

        // SYS.LOBCOMPPART$
        while (!sysLobCompPartMapRowId.empty()) {
            auto sysLobCompPartMapRowIdIt = sysLobCompPartMapRowId.cbegin();
            SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt->second;
            dictSysLobCompPartDrop(sysLobCompPart);
            delete sysLobCompPart;
        }
        if (!sysLobCompPartMapPartObj.empty())
            ctx->error(50029, "partobj# map SYS.LOBCOMPPART$ not empty, left: " + std::to_string(sysLobCompPartMapPartObj.size()) +
                              " at exit");
        if (!sysLobCompPartMapKey.empty())
            ctx->error(50029, "key map SYS.LOBCOMPPART$ not empty, left: " + std::to_string(sysLobCompPartMapKey.size()) + " at exit");

        // SYS.LOBFRAG$
        while (!sysLobFragMapRowId.empty()) {
            auto sysLobFragMapRowIdIt = sysLobFragMapRowId.cbegin();
            SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt->second;
            dictSysLobFragDrop(sysLobFrag);
            delete sysLobFrag;
        }
        if (!sysLobFragMapKey.empty())
            ctx->error(50029, "key map SYS.LOBFRAG$ not empty, left: " + std::to_string(sysLobFragMapKey.size()) + " at exit");

        // SYS.OBJ$
        while (!sysObjMapRowId.empty()) {
            auto sysObjMapRowIdIt = sysObjMapRowId.cbegin();
            SysObj* sysObj = sysObjMapRowIdIt->second;
            dictSysObjDrop(sysObj);
            delete sysObj;
        }
        if (!sysObjMapName.empty())
            ctx->error(50029, "name map SYS.OBJ$ not empty, left: " + std::to_string(sysObjMapName.size()) + " at exit");
        if (!sysObjMapObj.empty())
            ctx->error(50029, "obj# map SYS.OBJ$ not empty, left: " + std::to_string(sysObjMapObj.size()) + " at exit");

        // SYS.TAB$
        while (!sysTabMapRowId.empty()) {
            auto sysTabMapRowIdIt = sysTabMapRowId.cbegin();
            SysTab* sysTab = sysTabMapRowIdIt->second;
            dictSysTabDrop(sysTab);
            delete sysTab;
        }
        if (!sysTabMapObj.empty())
            ctx->error(50029, "obj# map SYS.TAB$ not empty, left: " + std::to_string(sysTabMapObj.size()) + " at exit");

        // SYS.TABCOMPART$
        while (!sysTabComPartMapRowId.empty()) {
            auto sysTabComPartMapRowIdIt = sysTabComPartMapRowId.cbegin();
            SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt->second;
            dictSysTabComPartDrop(sysTabComPart);
            delete sysTabComPart;
        }
        if (!sysTabComPartMapObj.empty())
            ctx->error(50029, "obj# map SYS.TABCOMPART$ not empty, left: " + std::to_string(sysTabComPartMapObj.size()) + " at exit");
        if (!sysTabComPartMapKey.empty())
            ctx->error(50029, "key map SYS.TABCOMPART$ not empty, left: " + std::to_string(sysTabComPartMapKey.size()) + " at exit");

        // SYS.TABPART$
        while (!sysTabPartMapRowId.empty()) {
            auto sysTabPartMapRowIdIt = sysTabPartMapRowId.cbegin();
            SysTabPart* sysTabPart = sysTabPartMapRowIdIt->second;
            dictSysTabPartDrop(sysTabPart);
            delete sysTabPart;
        }
        if (!sysTabPartMapKey.empty())
            ctx->error(50029, "key map SYS.TABPART$ not empty, left: " + std::to_string(sysTabPartMapKey.size()) + " at exit");

        // SYS.TABSUBPART$
        while (!sysTabSubPartMapRowId.empty()) {
            auto sysTabSubPartMapRowIdIt = sysTabSubPartMapRowId.cbegin();
            SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt->second;
            dictSysTabSubPartDrop(sysTabSubPart);
            delete sysTabSubPart;
        }
        if (!sysTabSubPartMapKey.empty())
            ctx->error(50029, "key map SYS.TABSUBPART$ not empty, left: " + std::to_string(sysTabSubPartMapKey.size()) + " at exit");

        // SYS.TS$
        while (!sysTsMapRowId.empty()) {
            auto sysTsMapRowIdIt = sysTsMapRowId.cbegin();
            SysTs* sysTs = sysTsMapRowIdIt->second;
            dictSysTsDrop(sysTs);
            delete sysTs;
        }
        if (!sysTsMapTs.empty())
            ctx->error(50029, "ts# map SYS.TS$ not empty, left: " + std::to_string(sysTsMapTs.size()) + " at exit");

        // SYS.USER$
        while (!sysUserMapRowId.empty()) {
            auto sysUserMapRowIdIt = sysUserMapRowId.cbegin();
            SysUser* sysUser = sysUserMapRowIdIt->second;
            dictSysUserDrop(sysUser);
            delete sysUser;
        }
        if (!sysUserMapUser.empty())
            ctx->error(50029, "user# map SYS.USER$ not empty, left: " + std::to_string(sysUserMapUser.size()) + " at exit");

        // XDB.XDB$TTSET
        while (!xdbTtSetMapRowId.empty()) {
            auto xdbTtSetMapRowIdIt = xdbTtSetMapRowId.cbegin();
            XdbTtSet* xdbTtSet = xdbTtSetMapRowIdIt->second;
            dictXdbTtSetDrop(xdbTtSet);
            delete xdbTtSet;
        }
        if (!xdbTtSetMapTs.empty())
            ctx->error(50029, "guid map XDB.XDB$TTSET not empty, left: " + std::to_string(xdbTtSetMapTs.size()) + " at exit");

        while (!schemaXmlMap.empty()) {
            auto schemaXmlMapIt = schemaXmlMap.cbegin();
            XmlCtx* xmlCtx = schemaXmlMapIt->second;
            xmlCtx->purgeDicts();
            schemaXmlMap.erase(schemaXmlMapIt);
            delete xmlCtx;
        }

        resetTouched();
    }

    bool Schema::compareSysCCol(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysCColMapRowIdIt: sysCColMapRowId) {
            SysCCol* sysCCol = sysCColMapRowIdIt.second;
            auto sysCColMapRowIdIt2 = otherSchema->sysCColMapRowId.find(sysCCol->rowId);
            if (sysCColMapRowIdIt2 == otherSchema->sysCColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CCOL$ lost ROWID: " + sysCCol->rowId.toString());
                return false;
            } else if (*sysCCol != *(sysCColMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.CCOL$ differs ROWID: " + sysCCol->rowId.toString());
                return false;
            }
        }

        for (const auto sysCColMapRowIdIt: otherSchema->sysCColMapRowId) {
            SysCCol* sysCCol = sysCColMapRowIdIt.second;
            auto sysCColMapRowIdIt2 = sysCColMapRowId.find(sysCCol->rowId);
            if (sysCColMapRowIdIt2 == sysCColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CCOL$ lost ROWID: " + sysCCol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysCDef(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysCDefMapRowIdIt: sysCDefMapRowId) {
            SysCDef* sysCDef = sysCDefMapRowIdIt.second;
            auto sysCDefMapRowIdIt2 = otherSchema->sysCDefMapRowId.find(sysCDef->rowId);
            if (sysCDefMapRowIdIt2 == otherSchema->sysCDefMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CDEF$ lost ROWID: " + sysCDef->rowId.toString());
                return false;
            } else if (*sysCDef != *(sysCDefMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.CDEF$ differs ROWID: " + sysCDef->rowId.toString());
                return false;
            }
        }

        for (const auto sysCDefMapRowIdIt: otherSchema->sysCDefMapRowId) {
            SysCDef* sysCDef = sysCDefMapRowIdIt.second;
            auto sysCDefMapRowIdIt2 = sysCDefMapRowId.find(sysCDef->rowId);
            if (sysCDefMapRowIdIt2 == sysCDefMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CDEF$ lost ROWID: " + sysCDef->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysCol(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysColMapRowIdIt: sysColMapRowId) {
            SysCol* sysCol = sysColMapRowIdIt.second;
            auto sysColMapRowIdIt2 = otherSchema->sysColMapRowId.find(sysCol->rowId);
            if (sysColMapRowIdIt2 == otherSchema->sysColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.COL$ lost ROWID: " + sysCol->rowId.toString());
                return false;
            } else if (*sysCol != *(sysColMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.COL$ differs ROWID: " + sysCol->rowId.toString());
                return false;
            }
        }

        for (const auto sysColMapRowIdIt: otherSchema->sysColMapRowId) {
            SysCol* sysCol = sysColMapRowIdIt.second;
            auto sysColMapRowIdIt2 = sysColMapRowId.find(sysCol->rowId);
            if (sysColMapRowIdIt2 == sysColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.COL$ lost ROWID: " + sysCol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysDeferredStg(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysDeferredStgMapRowIdIt: sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt.second;
            auto sysDeferredStgMapRowIdIt2 = otherSchema->sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
            if (sysDeferredStgMapRowIdIt2 == otherSchema->sysDeferredStgMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ lost ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            } else if (*sysDeferredStg != *(sysDeferredStgMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ differs ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            }
        }

        for (const auto sysDeferredStgMapRowIdIt: otherSchema->sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt.second;
            auto sysDeferredStgMapRowIdIt2 = sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
            if (sysDeferredStgMapRowIdIt2 == sysDeferredStgMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ lost ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysECol(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysEColMapRowIdIt: sysEColMapRowId) {
            SysECol* sysECol = sysEColMapRowIdIt.second;
            auto sysEColMapRowIdIt2 = otherSchema->sysEColMapRowId.find(sysECol->rowId);
            if (sysEColMapRowIdIt2 == otherSchema->sysEColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.ECOL$ lost ROWID: " + sysECol->rowId.toString());
                return false;
            } else if (*sysECol != *(sysEColMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.ECOL$ differs ROWID: " + sysECol->rowId.toString());
                return false;
            }
        }

        for (const auto sysEColMapRowIdIt: otherSchema->sysEColMapRowId) {
            SysECol* sysECol = sysEColMapRowIdIt.second;
            auto sysEColMapRowIdIt2 = sysEColMapRowId.find(sysECol->rowId);
            if (sysEColMapRowIdIt2 == sysEColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.ECOL$ lost ROWID: " + sysECol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLob(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysLobMapRowIdIt: sysLobMapRowId) {
            SysLob* sysLob = sysLobMapRowIdIt.second;
            auto sysLobMapRowIdIt2 = otherSchema->sysLobMapRowId.find(sysLob->rowId);
            if (sysLobMapRowIdIt2 == otherSchema->sysLobMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOB$ lost ROWID: " + sysLob->rowId.toString());
                return false;
            } else if (*sysLob != *(sysLobMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.LOB$ differs ROWID: " + sysLob->rowId.toString());
                return false;
            }
        }

        for (const auto sysLobMapRowIdIt: otherSchema->sysLobMapRowId) {
            SysLob* sysLob = sysLobMapRowIdIt.second;
            auto sysLobMapRowIdIt2 = sysLobMapRowId.find(sysLob->rowId);
            if (sysLobMapRowIdIt2 == sysLobMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOB$ lost ROWID: " + sysLob->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLobCompPart(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysLobCompPartMapRowIdIt: sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt.second;
            auto sysLobCompPartMapRowIdIt2 = otherSchema->sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
            if (sysLobCompPartMapRowIdIt2 == otherSchema->sysLobCompPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ lost ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            } else if (*sysLobCompPart != *(sysLobCompPartMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ differs ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            }
        }

        for (const auto sysLobCompPartMapRowIdIt: otherSchema->sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt.second;
            auto sysLobCompPartMapRowIdIt2 = sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
            if (sysLobCompPartMapRowIdIt2 == sysLobCompPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ lost ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLobFrag(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysLobFragMapRowIdIt: sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt.second;
            auto sysLobFragMapRowIdIt2 = otherSchema->sysLobFragMapRowId.find(sysLobFrag->rowId);
            if (sysLobFragMapRowIdIt2 == otherSchema->sysLobFragMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ lost ROWID: " + sysLobFrag->rowId.toString());
                return false;
            } else if (*sysLobFrag != *(sysLobFragMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ differs ROWID: " + sysLobFrag->rowId.toString());
                return false;
            }
        }

        for (const auto sysLobFragMapRowIdIt: otherSchema->sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt.second;
            auto sysLobFragMapRowIdIt2 = sysLobFragMapRowId.find(sysLobFrag->rowId);
            if (sysLobFragMapRowIdIt2 == sysLobFragMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ lost ROWID: " + sysLobFrag->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysObj(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysObjMapRowIdIt: sysObjMapRowId) {
            SysObj* sysObj = sysObjMapRowIdIt.second;
            auto sysObjMapRowIdIt2 = otherSchema->sysObjMapRowId.find(sysObj->rowId);
            if (sysObjMapRowIdIt2 == otherSchema->sysObjMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.OBJ$ lost ROWID: " + sysObj->rowId.toString());
                return false;
            } else if (*sysObj != *(sysObjMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.OBJ$ differs ROWID: " + sysObj->rowId.toString());
                return false;
            }
        }

        for (const auto sysObjMapRowIdIt: otherSchema->sysObjMapRowId) {
            SysObj* sysObj = sysObjMapRowIdIt.second;
            auto sysObjMapRowIdIt2 = sysObjMapRowId.find(sysObj->rowId);
            if (sysObjMapRowIdIt2 == sysObjMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.OBJ$ lost ROWID: " + sysObj->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTab(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysTabMapRowIdIt: sysTabMapRowId) {
            SysTab* sysTab = sysTabMapRowIdIt.second;
            auto sysTabMapRowIdIt2 = otherSchema->sysTabMapRowId.find(sysTab->rowId);
            if (sysTabMapRowIdIt2 == otherSchema->sysTabMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TAB$ lost ROWID: " + sysTab->rowId.toString());
                return false;
            } else if (*sysTab != *(sysTabMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.TAB$ differs ROWID: " + sysTab->rowId.toString());
                return false;
            }
        }

        for (const auto sysTabMapRowIdIt: otherSchema->sysTabMapRowId) {
            SysTab* sysTab = sysTabMapRowIdIt.second;
            auto sysTabMapRowIdIt2 = sysTabMapRowId.find(sysTab->rowId);
            if (sysTabMapRowIdIt2 == sysTabMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TAB$ lost ROWID: " + sysTab->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabComPart(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysTabComPartMapRowIdIt: sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt.second;
            auto sysTabComPartMapRowIdIt2 = otherSchema->sysTabComPartMapRowId.find(sysTabComPart->rowId);
            if (sysTabComPartMapRowIdIt2 == otherSchema->sysTabComPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ lost ROWID: " + sysTabComPart->rowId.toString());
                return false;
            } else if (*sysTabComPart != *(sysTabComPartMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ differs ROWID: " + sysTabComPart->rowId.toString());
                return false;
            }
        }

        for (const auto sysTabComPartMapRowIdIt: otherSchema->sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt.second;
            auto sysTabComPartMapRowIdIt2 = sysTabComPartMapRowId.find(sysTabComPart->rowId);
            if (sysTabComPartMapRowIdIt2 == sysTabComPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ lost ROWID: " + sysTabComPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabPart(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysTabPartMapRowIdIt: sysTabPartMapRowId) {
            SysTabPart* sysTabPart = sysTabPartMapRowIdIt.second;
            auto sysTabPartMapRowIdIt2 = otherSchema->sysTabPartMapRowId.find(sysTabPart->rowId);
            if (sysTabPartMapRowIdIt2 == otherSchema->sysTabPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABPART$ lost ROWID: " + sysTabPart->rowId.toString());
                return false;
            } else if (*sysTabPart != *(sysTabPartMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.TABPART$ differs ROWID: " + sysTabPart->rowId.toString());
                return false;
            }
        }

        for (const auto sysTabPartMapRowIdIt: otherSchema->sysTabPartMapRowId) {
            SysTabPart* sysTabPart = sysTabPartMapRowIdIt.second;
            auto sysTabPartMapRowIdIt2 = sysTabPartMapRowId.find(sysTabPart->rowId);
            if (sysTabPartMapRowIdIt2 == sysTabPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABPART$ lost ROWID: " + sysTabPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabSubPart(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysTabSubPartMapRowIdIt: sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt.second;
            auto sysTabSubPartMapRowIdIt2 = otherSchema->sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
            if (sysTabSubPartMapRowIdIt2 == otherSchema->sysTabSubPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ lost ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            } else if (*sysTabSubPart != *(sysTabSubPartMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ differs ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            }
        }

        for (const auto sysTabSubPartMapRowIdIt: otherSchema->sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt.second;
            auto sysTabSubPartMapRowIdIt2 = sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
            if (sysTabSubPartMapRowIdIt2 == sysTabSubPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ lost ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTs(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysTsMapRowIdIt: sysTsMapRowId) {
            SysTs* sysTs = sysTsMapRowIdIt.second;
            auto sysTsMapRowIdIt2 = otherSchema->sysTsMapRowId.find(sysTs->rowId);
            if (sysTsMapRowIdIt2 == otherSchema->sysTsMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TS$ lost ROWID: " + sysTs->rowId.toString());
                return false;
            } else if (*sysTs != *(sysTsMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.TS$ differs ROWID: " + sysTs->rowId.toString());
                return false;
            }
        }

        for (const auto sysTsMapRowIdIt: otherSchema->sysTsMapRowId) {
            SysTs* sysTs = sysTsMapRowIdIt.second;
            auto sysTsMapRowIdIt2 = sysTsMapRowId.find(sysTs->rowId);
            if (sysTsMapRowIdIt2 == sysTsMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TS$ lost ROWID: " + sysTs->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysUser(Schema* otherSchema, std::string& msgs) const {
        for (const auto sysUserMapRowIdIt: sysUserMapRowId) {
            SysUser* sysUser = sysUserMapRowIdIt.second;
            auto sysUserMapRowIdIt2 = otherSchema->sysUserMapRowId.find(sysUser->rowId);
            if (sysUserMapRowIdIt2 == otherSchema->sysUserMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.USER$ lost ROWID: " + sysUser->rowId.toString());
                return false;
            } else if (*sysUser != *(sysUserMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: SYS.USER$ differs ROWID: " + sysUser->rowId.toString());
                return false;
            }
        }

        for (const auto sysUserMapRowIdIt: otherSchema->sysUserMapRowId) {
            SysUser* sysUser = sysUserMapRowIdIt.second;
            auto sysUserMapRowIdIt2 = sysUserMapRowId.find(sysUser->rowId);
            if (sysUserMapRowIdIt2 == sysUserMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.USER$ lost ROWID: " + sysUser->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareXdbTtSet(Schema* otherSchema, std::string& msgs) const {
        for (const auto xdbTtSetMapRowIdIt: xdbTtSetMapRowId) {
            XdbTtSet* xdbTtSet = xdbTtSetMapRowIdIt.second;
            auto xdbTtSetMapRowIdIt2 = otherSchema->xdbTtSetMapRowId.find(xdbTtSet->rowId);
            if (xdbTtSetMapRowIdIt2 == otherSchema->xdbTtSetMapRowId.end()) {
                msgs.assign("schema mismatch: XDB.XDB$TTSET lost ROWID: " + xdbTtSet->rowId.toString());
                return false;
            } else if (*xdbTtSet != *(xdbTtSetMapRowIdIt2->second)) {
                msgs.assign("schema mismatch: XDB.XDB$TTSET differs ROWID: " + xdbTtSet->rowId.toString());
                return false;
            }
        }

        for (const auto xdbTtSetMapRowIdIt: otherSchema->xdbTtSetMapRowId) {
            XdbTtSet* xdbTtSet = xdbTtSetMapRowIdIt.second;
            auto xdbTtSetMapRowIdIt2 = xdbTtSetMapRowId.find(xdbTtSet->rowId);
            if (xdbTtSetMapRowIdIt2 == xdbTtSetMapRowId.end()) {
                msgs.assign("schema mismatch: XDB.XDB$TTSET lost ROWID: " + xdbTtSet->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareXdbXNm(Schema* otherSchema, std::string& msgs) const {
        for (const auto& schemaXmlMapIter: schemaXmlMap) {
            XmlCtx* xmlCtx = schemaXmlMapIter.second;
            auto otherXmlCtxIt = otherSchema->schemaXmlMap.find(schemaXmlMapIter.first);
            if (otherXmlCtxIt == otherSchema->schemaXmlMap.end())
                return false;
            XmlCtx* otherXmlCtx = otherXmlCtxIt->second;

            for (auto xdbXNmMapRowIdIt: xmlCtx->xdbXNmMapRowId) {
                XdbXNm* xdbXNm = xdbXNmMapRowIdIt.second;
                auto xdbXNmMapRowIdIt2 = otherXmlCtx->xdbXNmMapRowId.find(xdbXNm->rowId);
                if (xdbXNmMapRowIdIt2 == otherXmlCtx->xdbXNmMapRowId.end()) {
                    msgs.assign("schema mismatch: XDB.X$NM" + schemaXmlMapIter.first + " lost ROWID: " + xdbXNm->rowId.toString());
                    return false;
                } else if (*xdbXNm != *(xdbXNmMapRowIdIt2->second)) {
                    msgs.assign("schema mismatch: XDB.X$NM" + schemaXmlMapIter.first + " differs ROWID: " + xdbXNm->rowId.toString());
                    return false;
                }
            }

            for (const auto xdbXNmMapRowIdIt: otherXmlCtx->xdbXNmMapRowId) {
                XdbXNm* xdbXNm = xdbXNmMapRowIdIt.second;
                auto xdbXNmMapRowIdIt2 = xmlCtx->xdbXNmMapRowId.find(xdbXNm->rowId);
                if (xdbXNmMapRowIdIt2 == xmlCtx->xdbXNmMapRowId.end()) {
                    msgs.assign("schema mismatch: XDB.X$NM" + schemaXmlMapIter.first + " lost ROWID: " + xdbXNm->rowId.toString());
                    return false;
                }
            }
        }
        return true;
    }

    bool Schema::compareXdbXQn(Schema* otherSchema, std::string& msgs) const {
        for (const auto& schemaXmlMapIter: schemaXmlMap) {
            XmlCtx* xmlCtx = schemaXmlMapIter.second;
            auto otherXmlCtxIt = otherSchema->schemaXmlMap.find(schemaXmlMapIter.first);
            if (otherXmlCtxIt == otherSchema->schemaXmlMap.end())
                return false;
            XmlCtx* otherXmlCtx = otherXmlCtxIt->second;

            for (auto xdbXQnMapRowIdIt: xmlCtx->xdbXQnMapRowId) {
                XdbXQn* xdbXQn = xdbXQnMapRowIdIt.second;
                auto xdbXQnMapRowIdIt2 = otherXmlCtx->xdbXQnMapRowId.find(xdbXQn->rowId);
                if (xdbXQnMapRowIdIt2 == otherXmlCtx->xdbXQnMapRowId.end()) {
                    msgs.assign("schema mismatch: XDB.X$QN" + schemaXmlMapIter.first + " lost ROWID: " + xdbXQn->rowId.toString());
                    return false;
                } else if (*xdbXQn != *(xdbXQnMapRowIdIt2->second)) {
                    msgs.assign("schema mismatch: XDB.X$QN" + schemaXmlMapIter.first + " differs ROWID: " + xdbXQn->rowId.toString());
                    return false;
                }
            }

            for (const auto xdbXQnMapRowIdIt: otherXmlCtx->xdbXQnMapRowId) {
                XdbXQn* xdbXQn = xdbXQnMapRowIdIt.second;
                auto xdbXQnMapRowIdIt2 = xmlCtx->xdbXQnMapRowId.find(xdbXQn->rowId);
                if (xdbXQnMapRowIdIt2 == xmlCtx->xdbXQnMapRowId.end()) {
                    msgs.assign("schema mismatch: XDB.X$QN" + schemaXmlMapIter.first + " lost ROWID: " + xdbXQn->rowId.toString());
                    return false;
                }
            }
        }
        return true;
    }

    bool Schema::compareXdbXPt(Schema* otherSchema, std::string& msgs) const {
        for (const auto& schemaXmlMapIter: schemaXmlMap) {
            XmlCtx* xmlCtx = schemaXmlMapIter.second;
            auto otherXmlCtxIt = otherSchema->schemaXmlMap.find(schemaXmlMapIter.first);
            if (otherXmlCtxIt == otherSchema->schemaXmlMap.end())
                return false;
            XmlCtx* otherXmlCtx = otherXmlCtxIt->second;

            for (auto xdbXPtMapRowIdIt: xmlCtx->xdbXPtMapRowId) {
                XdbXPt* xdbXPt = xdbXPtMapRowIdIt.second;
                auto xdbXPtMapRowIdIt2 = otherXmlCtx->xdbXPtMapRowId.find(xdbXPt->rowId);
                if (xdbXPtMapRowIdIt2 == otherXmlCtx->xdbXPtMapRowId.end()) {
                    msgs.assign("schema mismatch: XDB.X$PT" + schemaXmlMapIter.first + " lost ROWID: " + xdbXPt->rowId.toString());
                    return false;
                } else if (*xdbXPt != *(xdbXPtMapRowIdIt2->second)) {
                    msgs.assign("schema mismatch: XDB.X$PT" + schemaXmlMapIter.first + " differs ROWID: " + xdbXPt->rowId.toString());
                    return false;
                }
            }

            for (const auto xdbXPtMapRowIdIt: otherXmlCtx->xdbXPtMapRowId) {
                XdbXPt* xdbXPt = xdbXPtMapRowIdIt.second;
                auto xdbXPtMapRowIdIt2 = xmlCtx->xdbXPtMapRowId.find(xdbXPt->rowId);
                if (xdbXPtMapRowIdIt2 == xmlCtx->xdbXPtMapRowId.end()) {
                    msgs.assign("schema mismatch: XDB.X$PT" + schemaXmlMapIter.first + " lost ROWID: " + xdbXPt->rowId.toString());
                    return false;
                }
            }
        }
        return true;
    }

    bool Schema::compare(Schema* otherSchema, std::string& msgs) const {
        if (!compareSysCCol(otherSchema, msgs)) return false;
        if (!compareSysCDef(otherSchema, msgs)) return false;
        if (!compareSysCol(otherSchema, msgs)) return false;
        if (!compareSysDeferredStg(otherSchema, msgs)) return false;
        if (!compareSysECol(otherSchema, msgs)) return false;
        if (!compareSysLob(otherSchema, msgs)) return false;
        if (!compareSysLobCompPart(otherSchema, msgs)) return false;
        if (!compareSysLobFrag(otherSchema, msgs)) return false;
        if (!compareSysObj(otherSchema, msgs)) return false;
        if (!compareSysTab(otherSchema, msgs)) return false;
        if (!compareSysTabComPart(otherSchema, msgs)) return false;
        if (!compareSysTabPart(otherSchema, msgs)) return false;
        if (!compareSysTabSubPart(otherSchema, msgs)) return false;
        if (!compareSysTs(otherSchema, msgs)) return false;
        if (!compareSysUser(otherSchema, msgs)) return false;
        if (!compareXdbTtSet(otherSchema, msgs)) return false;
        if (!compareXdbXNm(otherSchema, msgs)) return false;
        if (!compareXdbXPt(otherSchema, msgs)) return false;
        if (!compareXdbXQn(otherSchema, msgs)) return false;

        msgs.assign("");
        return true;
    }

    void Schema::dictSysCColAdd(const char* rowIdStr, typeCon con, typeCol intCol, typeObj obj, uint64_t spare11, uint64_t spare12) {
        typeRowId rowId(rowIdStr);
        if (sysCColMapRowId.find(rowId) != sysCColMapRowId.end())
            throw DataException(50023, "duplicate SYS.CCOL$ value: (rowid: " + rowId.toString() + ")");

        sysCColTmp = new SysCCol(rowId, con, intCol, obj, spare11, spare12);
        dictSysCColAdd(sysCColTmp);
        sysCColTmp = nullptr;
    }

    void Schema::dictSysCDefAdd(const char* rowIdStr, typeCon con, typeObj obj, typeType type) {
        typeRowId rowId(rowIdStr);
        if (sysCDefMapRowId.find(rowId) != sysCDefMapRowId.end())
            throw DataException(50023, "duplicate SYS.CDEF$ value: (rowid: " + rowId.toString() + ")");

        sysCDefTmp = new SysCDef(rowId, con, obj, type);
        dictSysCDefAdd(sysCDefTmp);
        sysCDefTmp = nullptr;
    }

    void Schema::dictSysColAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type,
                               uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null_,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysColMapRowId.find(rowId) != sysColMapRowId.end())
            throw DataException(50023, "duplicate SYS.COL$ value: (rowid: " + rowId.toString() + ")");

        if (strlen(name) > SYS_COL_NAME_LENGTH)
            throw DataException(50025, "value of SYS.COL$ too long for NAME (value: '" + std::string(name) + "', length: " +
                                       std::to_string(strlen(name)) + ")");

        if (segCol > 1000)
            throw DataException(50025, "value of SYS.COL$ too big for SEGCOL# (value: " + std::to_string(segCol) + ")");

        sysColTmp = new SysCol(rowId, obj, col, segCol, intCol, name, type, length,
                               precision, scale, charsetForm, charsetId, null_, property1,
                               property2);
        dictSysColAdd(sysColTmp);
        sysColTmp = nullptr;
    }

    void Schema::dictSysDeferredStgAdd(const char* rowIdStr, typeObj obj, uint64_t flagsStg1, uint64_t flagsStg2) {
        typeRowId rowId(rowIdStr);
        if (sysDeferredStgMapRowId.find(rowId) != sysDeferredStgMapRowId.end())
            throw DataException(50023, "duplicate SYS.DEFERRED_STG$ value: (rowid: " + rowId.toString() + ")");

        sysDeferredStgTmp = new SysDeferredStg(rowId, obj, flagsStg1, flagsStg2);
        dictSysDeferredStgAdd(sysDeferredStgTmp);
        sysDeferredStgTmp = nullptr;
    }

    void Schema::dictSysEColAdd(const char* rowIdStr, typeObj tabObj, typeCol colNum, typeCol guardId) {
        typeRowId rowId(rowIdStr);
        if (sysEColMapRowId.find(rowId) != sysEColMapRowId.end())
            throw DataException(50023, "duplicate SYS.ECOL$ value: (rowid: " + rowId.toString() + ")");

        sysEColTmp = new SysECol(rowId, tabObj, colNum, guardId);
        dictSysEColAdd(sysEColTmp);
        sysEColTmp = nullptr;
    }

    void Schema::dictSysLobAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol intCol, typeObj lObj, typeTs ts) {
        typeRowId rowId(rowIdStr);
        if (sysLobMapRowId.find(rowId) != sysLobMapRowId.end())
            throw DataException(50023, "duplicate SYS.LOB$ value: (rowid: " + rowId.toString() + ")");

        sysLobTmp = new SysLob(rowId, obj, col, intCol, lObj, ts);
        dictSysLobAdd(sysLobTmp);
        sysLobTmp = nullptr;
    }

    void Schema::dictSysLobCompPartAdd(const char* rowIdStr, typeObj partObj, typeObj lObj) {
        typeRowId rowId(rowIdStr);
        if (sysLobCompPartMapRowId.find(rowId) != sysLobCompPartMapRowId.end())
            throw DataException(50023, "duplicate SYS.LOBCOMPPART$ value: (rowid: " + rowId.toString() + ")");

        sysLobCompPartTmp = new SysLobCompPart(rowId, partObj, lObj);
        dictSysLobCompPartAdd(sysLobCompPartTmp);
        sysLobCompPartTmp = nullptr;
    }

    void Schema::dictSysLobFragAdd(const char* rowIdStr, typeObj fragObj, typeObj parentObj, typeTs ts) {
        typeRowId rowId(rowIdStr);
        if (sysLobFragMapRowId.find(rowId) != sysLobFragMapRowId.end())
            throw DataException(50023, "duplicate SYS.LOBFRAG$ value: (rowid: " + rowId.toString() + ")");

        sysLobFragTmp = new SysLobFrag(rowId, fragObj, parentObj, ts);
        dictSysLobFragAdd(sysLobFragTmp);
        sysLobFragTmp = nullptr;
    }

    bool Schema::dictSysObjAdd(const char* rowIdStr, typeUser owner, typeObj obj, typeDataObj dataObj, typeType type, const char* name,
                               uint64_t flags1, uint64_t flags2, bool single) {
        typeRowId rowId(rowIdStr);
        auto sysObjMapRowIdIt = sysObjMapRowId.find(rowId);
        if (sysObjMapRowIdIt != sysObjMapRowId.end()) {
            SysObj* sysObj = sysObjMapRowIdIt->second;

            if (sysObj->single) {
                if (!single) {
                    sysObj->single = false;
                    if (ctx->trace & TRACE_SYSTEM)
                        ctx->logTrace(TRACE_SYSTEM, "disabling single option for object " + std::string(name) + " (owner " +
                                                    std::to_string(owner) + ")");
                }

                return true;
            }

            return false;
        }

        if (strlen(name) > SYS_OBJ_NAME_LENGTH)
            throw DataException(50025, "value of SYS.OBJ$ too long for NAME (value: '" + std::string(name) + "', length: " +
                                       std::to_string(strlen(name)) + ")");
        sysObjTmp = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2,
                               single);
        dictSysObjAdd(sysObjTmp);
        sysObjTmp = nullptr;

        return true;
    }

    void Schema::dictSysTabAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeTs ts, typeCol cluCols, uint64_t flags1, uint64_t flags2,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end())
            throw DataException(50023, "duplicate SYS.TAB$ value: (rowid: " + rowId.toString() + ")");

        sysTabTmp = new SysTab(rowId, obj, dataObj, ts, cluCols, flags1, flags2,
                               property1, property2);
        dictSysTabAdd(sysTabTmp);
        sysTabTmp = nullptr;
    }

    void Schema::dictSysTabComPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo) {
        typeRowId rowId(rowIdStr);
        if (sysTabComPartMapRowId.find(rowId) != sysTabComPartMapRowId.end())
            throw DataException(50023, "duplicate SYS.TABCOMPART$ value: (rowid: " + rowId.toString() + ")");

        sysTabComPartTmp = new SysTabComPart(rowId, obj, dataObj, bo);
        dictSysTabComPartAdd(sysTabComPartTmp);
        sysTabComPartTmp = nullptr;
    }

    void Schema::dictSysTabPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo) {
        typeRowId rowId(rowIdStr);
        if (sysTabPartMapRowId.find(rowId) != sysTabPartMapRowId.end())
            throw DataException(50023, "duplicate SYS.TABPART$ value: (rowid: " + rowId.toString() + ")");

        sysTabPartTmp = new SysTabPart(rowId, obj, dataObj, bo);
        dictSysTabPartAdd(sysTabPartTmp);
        sysTabPartTmp = nullptr;
    }

    void Schema::dictSysTabSubPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj pObj) {
        typeRowId rowId(rowIdStr);
        if (sysTabSubPartMapRowId.find(rowId) != sysTabSubPartMapRowId.end())
            throw DataException(50023, "duplicate SYS.TABSUBPART$ value: (rowid: " + rowId.toString() + ")");

        sysTabSubPartTmp = new SysTabSubPart(rowId, obj, dataObj, pObj);
        dictSysTabSubPartAdd(sysTabSubPartTmp);
        sysTabSubPartTmp = nullptr;
    }

    void Schema::dictSysTsAdd(const char* rowIdStr, typeTs ts, const char* name, uint32_t blockSize) {
        typeRowId rowId(rowIdStr);
        if (sysTsMapRowId.find(rowId) != sysTsMapRowId.end())
            throw DataException(50023, "duplicate SYS.TS$ value: (rowid: " + rowId.toString() + ")");

        sysTsTmp = new SysTs(rowId, ts, name, blockSize);
        dictSysTsAdd(sysTsTmp);
        sysTsTmp = nullptr;
    }

    bool Schema::dictSysUserAdd(const char* rowIdStr, typeUser user, const char* name, uint64_t spare11, uint64_t spare12, bool single) {
        typeRowId rowId(rowIdStr);

        auto sysUserMapRowIdIt = sysUserMapRowId.find(rowId);
        if (sysUserMapRowIdIt != sysUserMapRowId.end()) {
            SysUser* sysUser = sysUserMapRowIdIt->second;
            if (sysUser->single) {
                if (!single) {
                    sysUser->single = false;
                    if (ctx->trace & TRACE_SYSTEM)
                        ctx->logTrace(TRACE_SYSTEM, "disabling single option for user " + std::string(name) + " (" +
                                                    std::to_string(user) + ")");
                }

                return true;
            }

            return false;
        }

        if (strlen(name) > SYS_USER_NAME_LENGTH)
            throw DataException(50025, "value of SYS.USER$ too long for NAME (value: '" + std::string(name) + "', length: " +
                                       std::to_string(strlen(name)) + ")");
        sysUserTmp = new SysUser(rowId, user, name, spare11, spare12, single);
        dictSysUserAdd(sysUserTmp);
        sysUserTmp = nullptr;

        return true;
    }

    void Schema::dictXdbTtSetAdd(const char* rowIdStr, const char* guid, const char* tokSuf, uint64_t flags, typeObj obj) {
        typeRowId rowId(rowIdStr);
        if (xdbTtSetMapRowId.find(rowId) != xdbTtSetMapRowId.end())
            throw DataException(50023, "duplicate SYS.TS$ value: (rowid: " + rowId.toString() + ")");

        xdbTtSetTmp = new XdbTtSet(rowId, guid, tokSuf, flags, obj);
        dictXdbTtSetAdd(xdbTtSetTmp);
        xdbTtSetTmp = nullptr;
    }

    void Schema::dictXdbXNmAdd(XmlCtx* xmlCtx, const char* rowIdStr, const char* nmSpcUri, const char* id) {
        typeRowId rowId(rowIdStr);
        if (xmlCtx->xdbXNmMapRowId.find(rowId) != xmlCtx->xdbXNmMapRowId.end())
            throw DataException(50023, "duplicate XDB.X$NM" + xmlCtx->tokSuf + " value: (rowid: " + rowId.toString() + ")");

        xdbXNmTmp = new XdbXNm(rowId, nmSpcUri, id);
        xmlCtx->dictXdbXNmAdd(xdbXNmTmp);
        xdbXNmTmp = nullptr;
    }

    void Schema::dictXdbXPtAdd(XmlCtx* xmlCtx, const char* rowIdStr, const char* path, const char* id) {
        typeRowId rowId(rowIdStr);
        if (xmlCtx->xdbXPtMapRowId.find(rowId) != xmlCtx->xdbXPtMapRowId.end())
            throw DataException(50023, "duplicate XDB.X$PT" + xmlCtx->tokSuf + " value: (rowid: " + rowId.toString() + ")");

        xdbXPtTmp = new XdbXPt(rowId, path, id);
        xmlCtx->dictXdbXPtAdd(xdbXPtTmp);
        xdbXPtTmp = nullptr;
    }

    void Schema::dictXdbXQnAdd(XmlCtx* xmlCtx, const char* rowIdStr, const char* nmSpcId, const char* localName, const char* flags, const char* id) {
        typeRowId rowId(rowIdStr);
        if (xmlCtx->xdbXQnMapRowId.find(rowId) != xmlCtx->xdbXQnMapRowId.end())
            throw DataException(50023, "duplicate XDB.X$QN" + xmlCtx->tokSuf + " value: (rowid: " + rowId.toString() + ")");

        xdbXQnTmp = new XdbXQn(rowId, nmSpcId, localName, flags, id);
        xmlCtx->dictXdbXQnAdd(xdbXQnTmp);
        xdbXQnTmp = nullptr;
    }

    void Schema::dictSysCColAdd(SysCCol* sysCCol) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.CCOL$ (ROWID: " + sysCCol->rowId.toString() +
                                        ", CON#: " + std::to_string(sysCCol->con) +
                                        ", INTCOL#: " + std::to_string(sysCCol->intCol) +
                                        ", OBJ#: " + std::to_string(sysCCol->obj) +
                                        ", SPARE1: " + sysCCol->spare1.toString() + ")");

        SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
        auto sysCColMapKeyIt = sysCColMapKey.find(sysCColKey);
        if (sysCColMapKeyIt != sysCColMapKey.end())
            throw DataException(50024, "duplicate SYS.CCOL$ value for unique (OBJ#: " + std::to_string(sysCCol->obj) + ", INTCOL#: " +
                                       std::to_string(sysCCol->intCol) + ", CON#: " + std::to_string(sysCCol->con) + ")");

        sysCColMapRowId.insert_or_assign(sysCCol->rowId, sysCCol);
        sysCColMapKey.insert_or_assign(sysCColKey, sysCCol);
        sysCColSetTouched.insert(sysCCol);
        touchTable(sysCCol->obj);
        touched = true;
    }

    void Schema::dictSysCDefAdd(SysCDef* sysCDef) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.CDEF$ (ROWID: " + sysCDef->rowId.toString() +
                                        ", CON#: " + std::to_string(sysCDef->con) +
                                        ", OBJ#: " + std::to_string(sysCDef->obj) +
                                        ", TYPE: " + std::to_string(sysCDef->type) + ")");

        SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
        auto sysCDefMapKeyIt = sysCDefMapKey.find(sysCDefKey);
        if (sysCDefMapKeyIt != sysCDefMapKey.end())
            throw DataException(50024, "duplicate SYS.CDEF$ value for unique (OBJ#: " + std::to_string(sysCDef->obj) + ", CON#: " +
                                       std::to_string(sysCDef->con) + ")");

        auto sysCDefMapConIt = sysCDefMapCon.find(sysCDef->con);
        if (sysCDefMapConIt != sysCDefMapCon.end())
            throw DataException(50024, "duplicate SYS.CDEF$ value for unique (CON#: " + std::to_string(sysCDef->con) + ")");

        sysCDefMapRowId.insert_or_assign(sysCDef->rowId, sysCDef);
        sysCDefMapKey.insert_or_assign(sysCDefKey, sysCDef);
        sysCDefMapCon.insert_or_assign(sysCDef->con, sysCDef);
        sysCDefSetTouched.insert(sysCDef);
        touchTable(sysCDef->obj);
        touched = true;
    }

    void Schema::dictSysColAdd(SysCol* sysCol) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.COL$ (ROWID: " + sysCol->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysCol->obj) +
                                        ", COL#: " + std::to_string(sysCol->col) +
                                        ", SEGCOL#: " + std::to_string(sysCol->segCol) +
                                        ", INTCOL#: " + std::to_string(sysCol->intCol) +
                                        ", NAME: '" + sysCol->name +
                                        "', TYPE#: " + std::to_string(sysCol->type) +
                                        ", LENGTH: " + std::to_string(sysCol->length) +
                                        ", PRECISION#: " + std::to_string(sysCol->precision) +
                                        ", SCALE: " + std::to_string(sysCol->scale) +
                                        ", CHARSETFORM: " + std::to_string(sysCol->charsetForm) +
                                        ", CHARSETID: " + std::to_string(sysCol->charsetId) +
                                        ", NULL$: " + std::to_string(sysCol->null_) +
                                        ", PROPERTY: " + sysCol->property.toString() + ")");

        SysColSeg sysColSeg(sysCol->obj, sysCol->segCol, sysCol->rowId);
        auto sysColMapSegIt = sysColMapSeg.find(sysColSeg);
        if (sysColMapSegIt != sysColMapSeg.end())
            throw DataException(50024, "duplicate SYS.COL$ value for unique (OBJ#: " + std::to_string(sysCol->obj) + ", SEGCOL#: " +
                                       std::to_string(sysCol->segCol) + ", ROWID: " + sysCol->rowId.toString() + ")");

        sysColMapRowId.insert_or_assign(sysCol->rowId, sysCol);
        sysColMapSeg.insert_or_assign(sysColSeg, sysCol);
        sysColSetTouched.insert(sysCol);
        touchTable(sysCol->obj);
        touched = true;
    }

    void Schema::dictSysDeferredStgAdd(SysDeferredStg* sysDeferredStg) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.DEFERRED_STG$ (ROWID: " + sysDeferredStg->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysDeferredStg->obj) +
                                        ", FLAGS_STG: " + sysDeferredStg->flagsStg.toString() + ")");

        auto sysDeferredStgMapObjIt = sysDeferredStgMapObj.find(sysDeferredStg->obj);
        if (sysDeferredStgMapObjIt != sysDeferredStgMapObj.end())
            throw DataException(50024, "duplicate SYS.DEFERRED_STG$ value for unique (OBJ#: " + std::to_string(sysDeferredStg->obj) +
                                       ")");

        sysDeferredStgMapRowId.insert_or_assign(sysDeferredStg->rowId, sysDeferredStg);
        sysDeferredStgMapObj.insert_or_assign(sysDeferredStg->obj, sysDeferredStg);
        sysDeferredStgSetTouched.insert(sysDeferredStg);
        touchTable(sysDeferredStg->obj);
        touched = true;
    }

    void Schema::dictSysEColAdd(SysECol* sysECol) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.ECOL$ (ROWID: " + sysECol->rowId.toString() +
                                        ", TABOBJ#: " + std::to_string(sysECol->tabObj) +
                                        ", COLNUM: " + std::to_string(sysECol->colNum) +
                                        ", GUARD_ID: " + std::to_string(sysECol->guardId) + ")");

        SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
        auto sysEColMapKeyIt = sysEColMapKey.find(sysEColKey);
        if (sysEColMapKeyIt != sysEColMapKey.end())
            throw DataException(50024, "duplicate SYS.ECOL$ value for unique (TABOBJ#: " + std::to_string(sysECol->tabObj) + ", COLNUM: " +
                                       std::to_string(sysECol->colNum) + ")");

        sysEColMapRowId.insert_or_assign(sysECol->rowId, sysECol);
        sysEColMapKey.insert_or_assign(sysEColKey, sysECol);
        sysEColSetTouched.insert(sysECol);
        touchTable(sysECol->tabObj);
        touched = true;
    }

    void Schema::dictSysLobAdd(SysLob* sysLob) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.LOB$ (ROWID: " + sysLob->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysLob->obj) +
                                        ", COL#: " + std::to_string(sysLob->col) +
                                        ", INTCOL#: " + std::to_string(sysLob->intCol) +
                                        ", LOBJ#: " + std::to_string(sysLob->lObj) +
                                        ", TS#: " + std::to_string(sysLob->ts) + ")");

        SysLobKey sysLobKey(sysLob->obj, sysLob->intCol);
        auto sysLobMapKeyIt = sysLobMapKey.find(sysLobKey);
        if (sysLobMapKeyIt != sysLobMapKey.end())
            throw DataException(50024, "duplicate SYS.LOB$ value for unique (OBJ#: " + std::to_string(sysLob->obj) + ", INTCOL#: " +
                                       std::to_string(sysLob->intCol) + ")");

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLob->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            throw DataException(50024, "duplicate SYS.LOB$ value for unique (LOBJ#: " + std::to_string(sysLob->lObj) + ")");

        sysLobMapRowId.insert_or_assign(sysLob->rowId, sysLob);
        sysLobMapKey.insert_or_assign(sysLobKey, sysLob);
        sysLobMapLObj.insert_or_assign(sysLob->lObj, sysLob);
        sysLobSetTouched.insert(sysLob);
        touchTable(sysLob->obj);
        touched = true;
    }

    void Schema::dictSysLobCompPartAdd(SysLobCompPart* sysLobCompPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.LOBCOMPPART$ (ROWID: " + sysLobCompPart->rowId.toString() +
                                        ", PARTOBJ#: " + std::to_string(sysLobCompPart->partObj) +
                                        ", LOBJ#: " + std::to_string(sysLobCompPart->lObj) + ")");

        SysLobCompPartKey sysLobCompPartKey(sysLobCompPart->lObj, sysLobCompPart->partObj);
        auto sysLobCompPartMapKeyIt = sysLobCompPartMapKey.find(sysLobCompPartKey);
        if (sysLobCompPartMapKeyIt != sysLobCompPartMapKey.end())
            throw DataException(50024, "duplicate SYS.LOBCOMPPART$ value for unique (LOBJ#: " + std::to_string(sysLobCompPart->lObj) +
                                       ", PARTOBJ#: " + std::to_string(sysLobCompPart->partObj) + ")");

        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobCompPart->partObj);
        if (sysLobCompPartMapPartObjIt != sysLobCompPartMapPartObj.end())
            throw DataException(50024, "duplicate SYS.LOBCOMPPART$ value for unique (PARTOBJ#: " +
                                       std::to_string(sysLobCompPart->partObj) + ")");

        sysLobCompPartMapRowId.insert_or_assign(sysLobCompPart->rowId, sysLobCompPart);
        sysLobCompPartMapKey.insert_or_assign(sysLobCompPartKey, sysLobCompPart);
        sysLobCompPartMapPartObj.insert_or_assign(sysLobCompPart->partObj, sysLobCompPart);
        sysLobCompPartSetTouched.insert(sysLobCompPart);
        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPart->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            touchTable(sysLobMapLObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysLobFragAdd(SysLobFrag* sysLobFrag) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.LOBFRAG$ (ROWID: " + sysLobFrag->rowId.toString() +
                                        ", FRAGOBJ#: " + std::to_string(sysLobFrag->fragObj) +
                                        ", PARENTOBJ#: " + std::to_string(sysLobFrag->parentObj) +
                                        ", TS#: " + std::to_string(sysLobFrag->ts) + ")");

        SysLobFragKey sysLobFragKey(sysLobFrag->parentObj, sysLobFrag->fragObj);
        auto sysLobFragMapKeyIt = sysLobFragMapKey.find(sysLobFragKey);
        if (sysLobFragMapKeyIt != sysLobFragMapKey.end())
            throw DataException(50024, "duplicate SYS.LOBFRAG$ value for unique (PARENTOBJ#: " + std::to_string(sysLobFrag->parentObj) +
                                       ", PARTOBJ#: " + std::to_string(sysLobFrag->parentObj) + ")");

        sysLobFragMapRowId.insert_or_assign(sysLobFrag->rowId, sysLobFrag);
        sysLobFragMapKey.insert_or_assign(sysLobFragKey, sysLobFrag);
        sysLobFragSetTouched.insert(sysLobFrag);
        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobFrag->parentObj);
        if (sysLobCompPartMapPartObjIt != sysLobCompPartMapPartObj.end()) {
            auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPartMapPartObjIt->second->lObj);
            if (sysLobMapLObjIt != sysLobMapLObj.end())
                touchTable(sysLobMapLObjIt->second->obj);
        }
        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobFrag->parentObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            touchTable(sysLobMapLObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysObjAdd(SysObj* sysObj) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.OBJ$ (ROWID: " + sysObj->rowId.toString() +
                                        ", OWNER#: " + std::to_string(sysObj->owner) +
                                        ", OBJ#: " + std::to_string(sysObj->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysObj->dataObj) +
                                        ", TYPE#: " + std::to_string(sysObj->type) +
                                        ", NAME: '" + sysObj->name +
                                        "', FLAGS: " + sysObj->flags.toString() + ")");

        SysObjNameKey sysObjNameKey(sysObj->owner, sysObj->name.c_str(), sysObj->obj, sysObj->dataObj);
        auto sysObjMapNameIt = sysObjMapName.find(sysObjNameKey);
        if (sysObjMapNameIt != sysObjMapName.end())
            throw DataException(50024, "duplicate SYS.OBJ$ value for unique (OWNER#: " + std::to_string(sysObj->owner) + ", NAME: '" +
                                       sysObj->name + "', OBJ#: " + std::to_string(sysObj->obj) + ", DATAOBJ#: " + std::to_string(sysObj->dataObj) + ")");

        auto sysObjMapObjIt = sysObjMapObj.find(sysObj->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            throw DataException(50024, "duplicate SYS.OBJ$ value for unique (OBJ#: " + std::to_string(sysObj->obj) + ")");

        sysObjMapRowId.insert_or_assign(sysObj->rowId, sysObj);
        sysObjMapName.insert_or_assign(sysObjNameKey, sysObj);
        sysObjMapObj.insert_or_assign(sysObj->obj, sysObj);
        sysObjSetTouched.insert(sysObj);
        touchTable(sysObj->obj);
        touched = true;
    }

    void Schema::dictSysTabAdd(SysTab* sysTab) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.TAB$ (ROWID: " + sysTab->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTab->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTab->dataObj) +
                                        ", CLUCOLS: " + std::to_string(sysTab->cluCols) +
                                        ", FLAGS: " + sysTab->flags.toString() +
                                        ", PROPERTY: " + sysTab->property.toString() + ")");

        auto sysTabMapObjIt = sysTabMapObj.find(sysTab->obj);
        if (sysTabMapObjIt != sysTabMapObj.end())
            throw DataException(50024, "duplicate SYS.TAB$ value for unique (OBJ#: " + std::to_string(sysTab->obj) + ")");

        sysTabMapRowId.insert_or_assign(sysTab->rowId, sysTab);
        sysTabMapObj.insert_or_assign(sysTab->obj, sysTab);
        sysTabSetTouched.insert(sysTab);
        touchTable(sysTab->obj);
        touched = true;
    }

    void Schema::dictSysTabComPartAdd(SysTabComPart* sysTabComPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.TABCOMPART$ (ROWID: " + sysTabComPart->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTabComPart->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTabComPart->dataObj) +
                                        ", BO#: " + std::to_string(sysTabComPart->bo) + ")");

        SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
        auto sysTabComPartMapKeyIt = sysTabComPartMapKey.find(sysTabComPartKey);
        if (sysTabComPartMapKeyIt != sysTabComPartMapKey.end())
            throw DataException(50024, "duplicate SYS.TABCOMPART$ value for unique (BO#: " + std::to_string(sysTabComPart->bo) +
                                       ", OBJ#: " + std::to_string(sysTabComPart->obj) + ")");

        auto sysTabComPartMapObjIt = sysTabComPartMapObj.find(sysTabComPart->obj);
        if (sysTabComPartMapObjIt != sysTabComPartMapObj.end())
            throw DataException(50024, "duplicate SYS.TABCOMPART$ value for unique (OBJ#: " + std::to_string(sysTabComPart->obj) + ")");

        sysTabComPartMapRowId.insert_or_assign(sysTabComPart->rowId, sysTabComPart);
        sysTabComPartMapKey.insert_or_assign(sysTabComPartKey, sysTabComPart);
        sysTabComPartMapObj.insert_or_assign(sysTabComPart->obj, sysTabComPart);
        sysTabComPartSetTouched.insert(sysTabComPart);
        touchTable(sysTabComPart->bo);
        touched = true;
    }

    void Schema::dictSysTabPartAdd(SysTabPart* sysTabPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.TABPART$ (ROWID: " + sysTabPart->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTabPart->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTabPart->dataObj) +
                                        ", BO#: " + std::to_string(sysTabPart->bo) + ")");

        SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
        auto sysTabPartMapKeyIt = sysTabPartMapKey.find(sysTabPartKey);
        if (sysTabPartMapKeyIt != sysTabPartMapKey.end())
            throw DataException(50024, "duplicate SYS.TABPART$ value for unique (BO#: " + std::to_string(sysTabPart->bo) + ", OBJ#: " +
                                       std::to_string(sysTabPart->obj) + ")");

        sysTabPartMapRowId.insert_or_assign(sysTabPart->rowId, sysTabPart);
        sysTabPartMapKey.insert_or_assign(sysTabPartKey, sysTabPart);
        sysTabPartSetTouched.insert(sysTabPart);
        touchTable(sysTabPart->bo);
        touched = true;
    }

    void Schema::dictSysTabSubPartAdd(SysTabSubPart* sysTabSubPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.TABSUBPART$ (ROWID: " + sysTabSubPart->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTabSubPart->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTabSubPart->dataObj) +
                                        ", POBJ#: " + std::to_string(sysTabSubPart->pObj) + ")");

        SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
        auto sysTabSubPartMapKeyIt = sysTabSubPartMapKey.find(sysTabSubPartKey);
        if (sysTabSubPartMapKeyIt != sysTabSubPartMapKey.end())
            throw DataException(50024, "duplicate SYS.TABSUBPART$ value for unique (POBJ#: " + std::to_string(sysTabSubPart->pObj) +
                                       ", OBJ#: " + std::to_string(sysTabSubPart->obj) + ")");

        sysTabSubPartMapRowId.insert_or_assign(sysTabSubPart->rowId, sysTabSubPart);
        sysTabSubPartMapKey.insert_or_assign(sysTabSubPartKey, sysTabSubPart);
        sysTabSubPartSetTouched.insert(sysTabSubPart);
        auto sysObjMapObjIt = sysObjMapObj.find(sysTabSubPart->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            touchTable(sysObjMapObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysTsAdd(SysTs* sysTs) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.TS$ (ROWID: " + sysTs->rowId.toString() +
                                        ", TS#: " + std::to_string(sysTs->ts) +
                                        ", NAME: '" + sysTs->name +
                                        "', BLOCKSIZE: " + std::to_string(sysTs->blockSize) + ")");

        auto sysTsMapTsIt = sysTsMapTs.find(sysTs->ts);
        if (sysTsMapTsIt != sysTsMapTs.end())
            throw DataException(50024, "duplicate SYS.TS$ value for unique (TS#: " + std::to_string(sysTs->ts) + ")");

        sysTsMapRowId.insert_or_assign(sysTs->rowId, sysTs);
        sysTsMapTs.insert_or_assign(sysTs->ts, sysTs);
        touched = true;
    }

    void Schema::dictSysUserAdd(SysUser* sysUser) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert SYS.USER$ (ROWID: " + sysUser->rowId.toString() +
                                        ", USER#: " + std::to_string(sysUser->user) +
                                        ", NAME: '" + sysUser->name +
                                        "', SPARE1: " + sysUser->spare1.toString() + ")");

        auto sysUserMapUserIt = sysUserMapUser.find(sysUser->user);
        if (sysUserMapUserIt != sysUserMapUser.end())
            throw DataException(50024, "duplicate SYS.USER$ value for unique (USER#: " + std::to_string(sysUser->user) + ")");

        sysUserMapRowId.insert_or_assign(sysUser->rowId, sysUser);
        sysUserMapUser.insert_or_assign(sysUser->user, sysUser);
        sysUserSetTouched.insert(sysUser);
        touched = true;
    }

    void Schema::dictXdbTtSetAdd(XdbTtSet* xdbTtSet) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert XDB.XDB$TTSET (ROWID: " + xdbTtSet->rowId.toString() +
                                        ", GUID: '" + xdbTtSet->guid +
                                        "', TOKSUF: '" + xdbTtSet->tokSuf +
                                        "', FLAGS: '" + std::to_string(xdbTtSet->flags) +
                                        "', OBJ#: " + std::to_string(xdbTtSet->obj) + ")");

        auto xdbTtSetMapTsIt = xdbTtSetMapTs.find(xdbTtSet->tokSuf);
        if (xdbTtSetMapTsIt != xdbTtSetMapTs.end())
            throw DataException(50024, "duplicate XDB.XDB$TTSET value for unique (TOKSUF: '" + xdbTtSet->tokSuf + "')");

        xdbTtSetMapRowId.insert_or_assign(xdbTtSet->rowId, xdbTtSet);
        xdbTtSetMapTs.insert_or_assign(xdbTtSet->tokSuf, xdbTtSet);
        touched = true;
    }

    void Schema::dictXdbXNmAdd(const std::string& tokSuf, XdbXNm* xdbXNm) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert XDB.X$NM" + tokSuf + " (ROWID: " + xdbXNm->rowId.toString() +
                                        ", NMSPCURI: '" + xdbXNm->nmSpcUri +
                                        "', ID: '" + xdbXNm->id + "')");

        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$NM" + tokSuf + " table, insert failed");
        schemaXmlMapIt->second->dictXdbXNmAdd(xdbXNm);
        touched = true;
    }

    void Schema::dictXdbXPtAdd(const std::string& tokSuf, XdbXPt* xdbXPt) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert XDB.X$PT" + tokSuf + " (ROWID: " + xdbXPt->rowId.toString() +
                                        ", PATH: '" + xdbXPt->path +
                                        "', ID: '" + xdbXPt->id + "')");

        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$PT" + tokSuf + " table, insert failed");
        schemaXmlMapIt->second->dictXdbXPtAdd(xdbXPt);
        touched = true;
    }

    void Schema::dictXdbXQnAdd(const std::string& tokSuf, XdbXQn* xdbXQn) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "insert XDB.X$QN" + tokSuf + " (ROWID: " + xdbXQn->rowId.toString() +
                                        ", NMSPCID: '" + xdbXQn->nmSpcId +
                                        "', LOCALNAME: '" + xdbXQn->localName +
                                        "', FLAGS: '" + xdbXQn->flags +
                                        "', ID: '" + xdbXQn->id + "')");

        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$QN" + tokSuf + " table, insert failed");
        schemaXmlMapIt->second->dictXdbXQnAdd(xdbXQn);
        touched = true;
    }

    void Schema::dictSysCColDrop(SysCCol* sysCCol) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.CCOL$ (ROWID: " + sysCCol->rowId.toString() +
                                        ", CON#: " + std::to_string(sysCCol->con) +
                                        ", INTCOL#: " + std::to_string(sysCCol->intCol) +
                                        ", OBJ#: " + std::to_string(sysCCol->obj) +
                                        ", SPARE1: " + sysCCol->spare1.toString() + ")");
        auto sysCColMapRowIdIt = sysCColMapRowId.find(sysCCol->rowId);
        if (sysCColMapRowIdIt == sysCColMapRowId.end())
            return;
        sysCColMapRowId.erase(sysCColMapRowIdIt);

        SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
        auto sysCColMapKeyIt = sysCColMapKey.find(sysCColKey);
        if (sysCColMapKeyIt != sysCColMapKey.end())
            sysCColMapKey.erase(sysCColMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.CCOL$ (OBJ#: " + std::to_string(sysCCol->obj) + ", INTCOL#: " +
                                std::to_string(sysCCol->intCol) + ", CON#: " + std::to_string(sysCCol->con) + ")");

        touchTable(sysCCol->obj);
        touched = true;
    }

    void Schema::dictSysCDefDrop(SysCDef* sysCDef) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.CDEF$ (ROWID: " + sysCDef->rowId.toString() +
                                        ", CON#: " + std::to_string(sysCDef->con) +
                                        ", OBJ#: " + std::to_string(sysCDef->obj) +
                                        ", TYPE: " + std::to_string(sysCDef->type) + ")");
        auto sysCDefMapRowIdIt = sysCDefMapRowId.find(sysCDef->rowId);
        if (sysCDefMapRowIdIt == sysCDefMapRowId.end())
            return;
        sysCDefMapRowId.erase(sysCDefMapRowIdIt);

        SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
        auto sysCDefMapKeyIt = sysCDefMapKey.find(sysCDefKey);
        if (sysCDefMapKeyIt != sysCDefMapKey.end())
            sysCDefMapKey.erase(sysCDefMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.CDEF$ (OBJ#: " + std::to_string(sysCDef->obj) + ", CON#: " +
                                std::to_string(sysCDef->con) + ")");

        auto sysCDefMapConIt = sysCDefMapCon.find(sysCDef->con);
        if (sysCDefMapConIt != sysCDefMapCon.end())
            sysCDefMapCon.erase(sysCDefMapConIt);
        else
            ctx->warning(50030, "missing index for SYS.CDEF$ (CON#: " + std::to_string(sysCDef->con) + ")");

        touchTable(sysCDef->obj);
        touched = true;
    }

    void Schema::dictSysColDrop(SysCol* sysCol) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.COL$ (ROWID: " + sysCol->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysCol->obj) +
                                        ", COL#: " + std::to_string(sysCol->col) +
                                        ", SEGCOL#: " + std::to_string(sysCol->segCol) +
                                        ", INTCOL#: " + std::to_string(sysCol->intCol) +
                                        ", NAME: '" + sysCol->name +
                                        "', TYPE#: " + std::to_string(sysCol->type) +
                                        ", LENGTH: " + std::to_string(sysCol->length) +
                                        ", PRECISION#: " + std::to_string(sysCol->precision) +
                                        ", SCALE: " + std::to_string(sysCol->scale) +
                                        ", CHARSETFORM: " + std::to_string(sysCol->charsetForm) +
                                        ", CHARSETID: " + std::to_string(sysCol->charsetId) +
                                        ", NULL$: " + std::to_string(sysCol->null_) +
                                        ", PROPERTY: " + sysCol->property.toString() + ")");
        auto sysColMapRowIdIt = sysColMapRowId.find(sysCol->rowId);
        if (sysColMapRowIdIt == sysColMapRowId.end())
            return;
        sysColMapRowId.erase(sysColMapRowIdIt);

        SysColSeg sysColSeg(sysCol->obj, sysCol->segCol, sysCol->rowId);
        auto sysColMapSegIt = sysColMapSeg.find(sysColSeg);
        if (sysColMapSegIt != sysColMapSeg.end())
            sysColMapSeg.erase(sysColMapSegIt);
        else
            ctx->warning(50030, "missing index for SYS.COL$ (OBJ#: " + std::to_string(sysCol->obj) + ", SEGCOL#: " +
                                std::to_string(sysCol->segCol) + ", ROWID: " + sysCol->rowId.toString() + ")");

        touchTable(sysCol->obj);
        touched = true;
    }

    void Schema::dictSysDeferredStgDrop(SysDeferredStg* sysDeferredStg) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.DEFERRED_STG$ (ROWID: " + sysDeferredStg->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysDeferredStg->obj) +
                                        ", FLAGS_STG: " + sysDeferredStg->flagsStg.toString() + ")");
        auto sysDeferredStgMapRowIdIt = sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
        if (sysDeferredStgMapRowIdIt == sysDeferredStgMapRowId.end())
            return;
        sysDeferredStgMapRowId.erase(sysDeferredStgMapRowIdIt);

        auto sysDeferredStgMapObjIt = sysDeferredStgMapObj.find(sysDeferredStg->obj);
        if (sysDeferredStgMapObjIt != sysDeferredStgMapObj.end())
            sysDeferredStgMapObj.erase(sysDeferredStgMapObjIt);
        else
            ctx->warning(50030, "missing index for SYS.DEFERRED_STG$ (OBJ#: " + std::to_string(sysDeferredStg->obj) + ")");

        touchTable(sysDeferredStg->obj);
        touched = true;
    }

    void Schema::dictSysEColDrop(SysECol* sysECol) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.ECOL$ (ROWID: " + sysECol->rowId.toString() +
                                        ", TABOBJ#: " + std::to_string(sysECol->tabObj) +
                                        ", COLNUM: " + std::to_string(sysECol->colNum) +
                                        ", GUARD_ID: " + std::to_string(sysECol->guardId) + ")");
        auto sysEColMapRowIdIt = sysEColMapRowId.find(sysECol->rowId);
        if (sysEColMapRowIdIt == sysEColMapRowId.end())
            return;
        sysEColMapRowId.erase(sysEColMapRowIdIt);

        SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
        auto sysEColMapKeyIt = sysEColMapKey.find(sysEColKey);
        if (sysEColMapKeyIt != sysEColMapKey.end())
            sysEColMapKey.erase(sysEColMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.ECOL$ (TABOBJ#: " + std::to_string(sysECol->tabObj) + ", COLNUM#: " +
                                std::to_string(sysECol->colNum) + ")");

        touchTable(sysECol->tabObj);
        touched = true;
    }

    void Schema::dictSysLobDrop(SysLob* sysLob) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.LOB$ (ROWID: " + sysLob->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysLob->obj) +
                                        ", COL#: " + std::to_string(sysLob->col) +
                                        ", INTCOL#: " + std::to_string(sysLob->intCol) +
                                        ", LOBJ#: " + std::to_string(sysLob->lObj) +
                                        ", TS#: " + std::to_string(sysLob->ts) + ")");
        auto sysLobMapRowIdIt = sysLobMapRowId.find(sysLob->rowId);
        if (sysLobMapRowIdIt == sysLobMapRowId.end())
            return;
        sysLobMapRowId.erase(sysLobMapRowIdIt);

        SysLobKey sysLobKey(sysLob->obj, sysLob->intCol);
        auto sysLobMapKeyIt = sysLobMapKey.find(sysLobKey);
        if (sysLobMapKeyIt != sysLobMapKey.end())
            sysLobMapKey.erase(sysLobMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.LOB$ (OBJ#: " + std::to_string(sysLob->obj) + ", INTCOL#: " +
                                std::to_string(sysLob->intCol) + ")");

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLob->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            sysLobMapLObj.erase(sysLobMapLObjIt);
        else
            ctx->warning(50030, "missing index for SYS.LOB$ (LOBJ#: " + std::to_string(sysLob->lObj) + ")");

        touchTable(sysLob->obj);
        touched = true;
    }

    void Schema::dictSysLobCompPartDrop(SysLobCompPart* sysLobCompPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.LOBCOMPPART$ (ROWID: " + sysLobCompPart->rowId.toString() +
                                        ", PARTOBJ#: " + std::to_string(sysLobCompPart->partObj) +
                                        ", LOBJ#: " + std::to_string(sysLobCompPart->lObj) + ")");
        auto sysLobCompPartMapRowIdIt = sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
        if (sysLobCompPartMapRowIdIt == sysLobCompPartMapRowId.end())
            return;
        sysLobCompPartMapRowId.erase(sysLobCompPartMapRowIdIt);

        SysLobCompPartKey sysLobCompPartKey(sysLobCompPart->lObj, sysLobCompPart->partObj);
        auto sysLobCompPartMapKeyIt = sysLobCompPartMapKey.find(sysLobCompPartKey);
        if (sysLobCompPartMapKeyIt != sysLobCompPartMapKey.end())
            sysLobCompPartMapKey.erase(sysLobCompPartMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.LOBCOMPPART$ (LOBJ#: " + std::to_string(sysLobCompPart->lObj) + ", PARTOBJ#: " +
                                std::to_string(sysLobCompPart->partObj) + ")");

        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobCompPart->partObj);
        if (sysLobCompPartMapPartObjIt != sysLobCompPartMapPartObj.end())
            sysLobCompPartMapPartObj.erase(sysLobCompPartMapPartObjIt);
        else
            ctx->warning(50030, "missing index for SYS.LOBCOMPPART$ (PARTOBJ#: " + std::to_string(sysLobCompPart->partObj) + ")");

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPart->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            touchTable(sysLobMapLObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysLobFragDrop(SysLobFrag* sysLobFrag) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.LOBFRAG$ (ROWID: " + sysLobFrag->rowId.toString() +
                                        ", FRAGOBJ#: " + std::to_string(sysLobFrag->fragObj) +
                                        ", PARENTOBJ#: " + std::to_string(sysLobFrag->parentObj) +
                                        ", TS#: " + std::to_string(sysLobFrag->ts) + ")");
        auto sysLobFragMapRowIdIt = sysLobFragMapRowId.find(sysLobFrag->rowId);
        if (sysLobFragMapRowIdIt == sysLobFragMapRowId.end())
            return;
        sysLobFragMapRowId.erase(sysLobFragMapRowIdIt);

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobFrag->parentObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            touchTable(sysLobMapLObjIt->second->obj);

        SysLobFragKey sysLobFragKey(sysLobFrag->parentObj, sysLobFrag->fragObj);
        auto sysLobFragMapKeyIt = sysLobFragMapKey.find(sysLobFragKey);
        if (sysLobFragMapKeyIt != sysLobFragMapKey.end())
            sysLobFragMapKey.erase(sysLobFragMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.LOBFRAG$ (PARENTOBJ#: " + std::to_string(sysLobFrag->parentObj) + ", FRAGOBJ#: " +
                                std::to_string(sysLobFrag->fragObj) + ")");

        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobFrag->parentObj);
        if (sysLobCompPartMapPartObjIt != sysLobCompPartMapPartObj.end()) {
            sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPartMapPartObjIt->second->lObj);
            if (sysLobMapLObjIt != sysLobMapLObj.end())
                touchTable(sysLobMapLObjIt->second->obj);
        }
        touched = true;
    }

    void Schema::dictSysObjDrop(SysObj* sysObj) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.OBJ$ (ROWID: " + sysObj->rowId.toString() +
                                        ", OWNER#: " + std::to_string(sysObj->owner) +
                                        ", OBJ#: " + std::to_string(sysObj->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysObj->dataObj) +
                                        ", TYPE#: " + std::to_string(sysObj->type) +
                                        ", NAME: '" + sysObj->name +
                                        "', FLAGS: " + sysObj->flags.toString() + ")");
        auto sysObjMapRowIdIt = sysObjMapRowId.find(sysObj->rowId);
        if (sysObjMapRowIdIt == sysObjMapRowId.end())
            return;
        sysObjMapRowId.erase(sysObjMapRowIdIt);

        SysObjNameKey sysObjNameKey(sysObj->owner, sysObj->name.c_str(), sysObj->obj, sysObj->dataObj);
        auto sysObjMapNameIt = sysObjMapName.find(sysObjNameKey);
        if (sysObjMapNameIt != sysObjMapName.end())
            sysObjMapName.erase(sysObjMapNameIt);
        else
            ctx->warning(50030, "missing index for SYS.OBJ$ (OWNER#: " + std::to_string(sysObj->owner) + ", NAME: '" +
                                sysObj->name + "', OBJ#: " + std::to_string(sysObj->obj) + ", DATAOBJ#: " + std::to_string(sysObj->dataObj) + ")");

        auto sysObjMapObjIt = sysObjMapObj.find(sysObj->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            sysObjMapObj.erase(sysObjMapObjIt);
        else
            ctx->warning(50030, "missing index for SYS.OBJ$ (OBJ#: " + std::to_string(sysObj->obj) + ")");

        touchTable(sysObj->obj);
        touched = true;
    }

    void Schema::dictSysTabDrop(SysTab* sysTab) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.TAB$ (ROWID: " + sysTab->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTab->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTab->dataObj) +
                                        ", CLUCOLS: " + std::to_string(sysTab->cluCols) +
                                        ", FLAGS: " + sysTab->flags.toString() +
                                        ", PROPERTY: " + sysTab->property.toString() + ")");
        auto sysTabMapRowIdIt = sysTabMapRowId.find(sysTab->rowId);
        if (sysTabMapRowIdIt == sysTabMapRowId.end())
            return;
        sysTabMapRowId.erase(sysTabMapRowIdIt);

        auto sysTabMapObjIt = sysTabMapObj.find(sysTab->obj);
        if (sysTabMapObjIt != sysTabMapObj.end())
            sysTabMapObj.erase(sysTab->obj);
        else
            ctx->warning(50030, "missing index for SYS.TAB$ (OBJ#: " + std::to_string(sysTab->obj) + ")");

        touchTable(sysTab->obj);
        touched = true;
    }

    void Schema::dictSysTabComPartDrop(SysTabComPart* sysTabComPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.TABCOMPART$ (ROWID: " + sysTabComPart->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTabComPart->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTabComPart->dataObj) +
                                        ", BO#: " + std::to_string(sysTabComPart->bo) + ")");
        auto sysTabComPartMapRowIdIt = sysTabComPartMapRowId.find(sysTabComPart->rowId);
        if (sysTabComPartMapRowIdIt == sysTabComPartMapRowId.end())
            return;
        sysTabComPartMapRowId.erase(sysTabComPartMapRowIdIt);

        SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
        auto sysTabComPartMapKeyIt = sysTabComPartMapKey.find(sysTabComPartKey);
        if (sysTabComPartMapKeyIt != sysTabComPartMapKey.end())
            sysTabComPartMapKey.erase(sysTabComPartMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.TABCOMPART$ (BO#: " + std::to_string(sysTabComPart->bo) + ", OBJ#: " +
                                std::to_string(sysTabComPart->obj) + ")");

        auto sysTabComPartMapObjIt = sysTabComPartMapObj.find(sysTabComPart->obj);
        if (sysTabComPartMapObjIt != sysTabComPartMapObj.end())
            sysTabComPartMapObj.erase(sysTabComPartMapObjIt);
        else
            ctx->warning(50030, "missing index for SYS.TABCOMPART$ (OBJ#: " + std::to_string(sysTabComPart->obj) + ")");

        touchTable(sysTabComPart->bo);
        touched = true;
    }

    void Schema::dictSysTabPartDrop(SysTabPart* sysTabPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.TABPART$ (ROWID: " + sysTabPart->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTabPart->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTabPart->dataObj) +
                                        ", BO#: " + std::to_string(sysTabPart->bo) + ")");
        auto sysTabPartMapRowIdIt = sysTabPartMapRowId.find(sysTabPart->rowId);
        if (sysTabPartMapRowIdIt == sysTabPartMapRowId.end())
            return;
        sysTabPartMapRowId.erase(sysTabPartMapRowIdIt);

        SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
        auto sysTabPartMapKeyIt = sysTabPartMapKey.find(sysTabPartKey);
        if (sysTabPartMapKeyIt != sysTabPartMapKey.end())
            sysTabPartMapKey.erase(sysTabPartMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.TABPART$ (BO#: " + std::to_string(sysTabPart->bo) + ", OBJ#: " +
                                std::to_string(sysTabPart->obj) + ")");

        touchTable(sysTabPart->bo);
        touched = true;
    }

    void Schema::dictSysTabSubPartDrop(SysTabSubPart* sysTabSubPart) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.TABSUBPART$ (ROWID: " + sysTabSubPart->rowId.toString() +
                                        ", OBJ#: " + std::to_string(sysTabSubPart->obj) +
                                        ", DATAOBJ#: " + std::to_string(sysTabSubPart->dataObj) +
                                        ", POBJ#: " + std::to_string(sysTabSubPart->pObj) + ")");
        auto sysTabSubPartMapRowIdIt = sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
        if (sysTabSubPartMapRowIdIt == sysTabSubPartMapRowId.end())
            return;
        sysTabSubPartMapRowId.erase(sysTabSubPartMapRowIdIt);

        SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
        auto sysTabSubPartMapKeyIt = sysTabSubPartMapKey.find(sysTabSubPartKey);
        if (sysTabSubPartMapKeyIt != sysTabSubPartMapKey.end())
            sysTabSubPartMapKey.erase(sysTabSubPartMapKeyIt);
        else
            ctx->warning(50030, "missing index for SYS.TABSUBPART$ (POBJ#: " + std::to_string(sysTabSubPart->pObj) + ", OBJ#: " +
                                std::to_string(sysTabSubPart->obj) + ")");

        auto sysObjMapObjIt = sysObjMapObj.find(sysTabSubPart->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            touchTable(sysObjMapObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysTsDrop(SysTs* sysTs) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.TS$ (ROWID: " + sysTs->rowId.toString() +
                                        ", TS#: " + std::to_string(sysTs->ts) +
                                        ", NAME: '" + sysTs->name +
                                        "', BLOCKSIZE: " + std::to_string(sysTs->blockSize) + ")");
        auto sysTsMapRowIdIt = sysTsMapRowId.find(sysTs->rowId);
        if (sysTsMapRowIdIt == sysTsMapRowId.end())
            return;
        sysTsMapRowId.erase(sysTsMapRowIdIt);

        auto sysTsMapTsIt = sysTsMapTs.find(sysTs->ts);
        if (sysTsMapTsIt != sysTsMapTs.end())
            sysTsMapTs.erase(sysTsMapTsIt);
        else
            ctx->warning(50030, "missing index for SYS.TS$ (TS#: " + std::to_string(sysTs->ts) + ")");
        touched = true;
    }

    void Schema::dictSysUserDrop(SysUser* sysUser) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete SYS.USER$ (ROWID: " + sysUser->rowId.toString() +
                                        ", USER#: " + std::to_string(sysUser->user) +
                                        ", NAME: '" + sysUser->name +
                                        "', SPARE1: " + sysUser->spare1.toString() + ")");
        auto sysUserMapRowIdIt = sysUserMapRowId.find(sysUser->rowId);
        if (sysUserMapRowIdIt == sysUserMapRowId.end())
            return;
        sysUserMapRowId.erase(sysUserMapRowIdIt);

        auto sysUserMapUserIt = sysUserMapUser.find(sysUser->user);
        if (sysUserMapUserIt != sysUserMapUser.end())
            sysUserMapUser.erase(sysUserMapUserIt);
        else
            ctx->warning(50030, "missing index for SYS.USER$ (USER#: " + std::to_string(sysUser->user) + ")");
        touched = true;
    }

    void Schema::dictXdbTtSetDrop(XdbTtSet* xdbTtSet) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete XDB.XDB$TTSET (ROWID: " + xdbTtSet->rowId.toString() +
                                        ", GUID: '" + xdbTtSet->guid +
                                        "', TOKSUF: '" + xdbTtSet->tokSuf +
                                        "', FLAGS: " + std::to_string(xdbTtSet->flags) +
                                        ", OBJ#: " + std::to_string(xdbTtSet->obj) + ")");
        auto xdbTtSetMapRowIdIt = xdbTtSetMapRowId.find(xdbTtSet->rowId);
        if (xdbTtSetMapRowIdIt == xdbTtSetMapRowId.end())
            return;
        xdbTtSetMapRowId.erase(xdbTtSetMapRowIdIt);

        auto xdbTtSetMapTsIt = xdbTtSetMapTs.find(xdbTtSet->tokSuf);
        if (xdbTtSetMapTsIt != xdbTtSetMapTs.end())
            xdbTtSetMapTs.erase(xdbTtSetMapTsIt);
        else
            ctx->warning(50030, "missing index for XDB.XDB$TTSET (TOKSUF: '" + xdbTtSet->tokSuf + "')");
        touched = true;
    }

    void Schema::dictXdbXNmDrop(const std::string& tokSuf, XdbXNm* xdbXNm) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete XDB.X$NM" + tokSuf + " (ROWID: " + xdbXNm->rowId.toString() +
                                        ", NMSPCURI: '" + xdbXNm->nmSpcUri +
                                        "', ID: '" + xdbXNm->id + "')");

        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$NM" + tokSuf + " table, delete failed");
        schemaXmlMapIt->second->dictXdbXNmDrop(xdbXNm);
        touched = true;
    }

    void Schema::dictXdbXPtDrop(const std::string& tokSuf, XdbXPt* xdbXPt) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete XDB.X$PT" + tokSuf + " (ROWID: " + xdbXPt->rowId.toString() +
                                        ", PATH: '" + xdbXPt->path +
                                        "', ID: '" + xdbXPt->id + "')");

        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$PT" + tokSuf + " table, delete failed");
        schemaXmlMapIt->second->dictXdbXPtDrop(xdbXPt);
        touched = true;
    }

    void Schema::dictXdbXQnDrop(const std::string& tokSuf, XdbXQn* xdbXQn) {
        if (ctx->trace & TRACE_SYSTEM)
            ctx->logTrace(TRACE_SYSTEM, "delete XDB.X$QN" + tokSuf + " (ROWID: " + xdbXQn->rowId.toString() +
                                        ", NMSPCID '" + xdbXQn->nmSpcId +
                                        "', LOCALNAME: '" + xdbXQn->localName +
                                        "', FLAGS: '" + xdbXQn->flags +
                                        "', ID: '" + xdbXQn->id + "')");

        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$QN" + tokSuf + " table, delete failed");
        schemaXmlMapIt->second->dictXdbXQnDrop(xdbXQn);
        touched = true;
    }

    SysCCol* Schema::dictSysCColFind(typeRowId rowId) {
        auto sysCColMapRowIdIt = sysCColMapRowId.find(rowId);
        if (sysCColMapRowIdIt != sysCColMapRowId.end())
            return sysCColMapRowIdIt->second;
        else
            return nullptr;
    }

    SysCDef* Schema::dictSysCDefFind(typeRowId rowId) {
        auto sysCDefMapRowIdIt = sysCDefMapRowId.find(rowId);
        if (sysCDefMapRowIdIt != sysCDefMapRowId.end())
            return sysCDefMapRowIdIt->second;
        else
            return nullptr;
    }

    SysCol* Schema::dictSysColFind(typeRowId rowId) {
        auto sysColMapRowIdIt = sysColMapRowId.find(rowId);
        if (sysColMapRowIdIt != sysColMapRowId.end())
            return sysColMapRowIdIt->second;
        else
            return nullptr;
    }

    SysDeferredStg* Schema::dictSysDeferredStgFind(typeRowId rowId) {
        auto sysDeferredStgMapRowIdIt = sysDeferredStgMapRowId.find(rowId);
        if (sysDeferredStgMapRowIdIt != sysDeferredStgMapRowId.end())
            return sysDeferredStgMapRowIdIt->second;
        else
            return nullptr;
    }

    SysECol* Schema::dictSysEColFind(typeRowId rowId) {
        auto sysEColMapRowIdIt = sysEColMapRowId.find(rowId);
        if (sysEColMapRowIdIt != sysEColMapRowId.end())
            return sysEColMapRowIdIt->second;
        else
            return nullptr;
    }

    SysLob* Schema::dictSysLobFind(typeRowId rowId) {
        auto sysLobMapRowIdIt = sysLobMapRowId.find(rowId);
        if (sysLobMapRowIdIt != sysLobMapRowId.end())
            return sysLobMapRowIdIt->second;
        else
            return nullptr;
    }

    SysLobCompPart* Schema::dictSysLobCompPartFind(typeRowId rowId) {
        auto sysLobCompPartMapRowIdIt = sysLobCompPartMapRowId.find(rowId);
        if (sysLobCompPartMapRowIdIt != sysLobCompPartMapRowId.end())
            return sysLobCompPartMapRowIdIt->second;
        else
            return nullptr;
    }

    SysLobFrag* Schema::dictSysLobFragFind(typeRowId rowId) {
        auto sysLobFragMapRowIdIt = sysLobFragMapRowId.find(rowId);
        if (sysLobFragMapRowIdIt != sysLobFragMapRowId.end())
            return sysLobFragMapRowIdIt->second;
        else
            return nullptr;
    }

    SysObj* Schema::dictSysObjFind(typeRowId rowId) {
        auto sysObjMapRowIdIt = sysObjMapRowId.find(rowId);
        if (sysObjMapRowIdIt != sysObjMapRowId.end())
            return sysObjMapRowIdIt->second;
        else
            return nullptr;
    }

    SysTab* Schema::dictSysTabFind(typeRowId rowId) {
        auto sysTabMapRowIdIt = sysTabMapRowId.find(rowId);
        if (sysTabMapRowIdIt != sysTabMapRowId.end())
            return sysTabMapRowIdIt->second;
        else
            return nullptr;
    }

    SysTabComPart* Schema::dictSysTabComPartFind(typeRowId rowId) {
        auto sysTabComPartMapRowIdIt = sysTabComPartMapRowId.find(rowId);
        if (sysTabComPartMapRowIdIt != sysTabComPartMapRowId.end())
            return sysTabComPartMapRowIdIt->second;
        else
            return nullptr;
    }

    SysTabPart* Schema::dictSysTabPartFind(typeRowId rowId) {
        auto sysTabPartMapRowIdIt = sysTabPartMapRowId.find(rowId);
        if (sysTabPartMapRowIdIt != sysTabPartMapRowId.end())
            return sysTabPartMapRowIdIt->second;
        else
            return nullptr;
    }

    SysTabSubPart* Schema::dictSysTabSubPartFind(typeRowId rowId) {
        auto sysTabSubPartMapRowIdIt = sysTabSubPartMapRowId.find(rowId);
        if (sysTabSubPartMapRowIdIt != sysTabSubPartMapRowId.end())
            return sysTabSubPartMapRowIdIt->second;
        else
            return nullptr;
    }

    SysTs* Schema::dictSysTsFind(typeRowId rowId) {
        auto sysTsMapRowIdIt = sysTsMapRowId.find(rowId);
        if (sysTsMapRowIdIt != sysTsMapRowId.end())
            return sysTsMapRowIdIt->second;
        else
            return nullptr;
    }

    SysUser* Schema::dictSysUserFind(typeRowId rowId) {
        auto sysUserMapRowIdIt = sysUserMapRowId.find(rowId);
        if (sysUserMapRowIdIt != sysUserMapRowId.end())
            return sysUserMapRowIdIt->second;
        else
            return nullptr;
    }

    XdbTtSet* Schema::dictXdbTtSetFind(typeRowId rowId) {
        auto xdbTtSetMapRowIdIt = xdbTtSetMapRowId.find(rowId);
        if (xdbTtSetMapRowIdIt != xdbTtSetMapRowId.end())
            return xdbTtSetMapRowIdIt->second;
        else
            return nullptr;
    }

    XdbXNm* Schema::dictXdbXNmFind(const std::string& tokSuf, typeRowId rowId) {
        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$NM" + tokSuf + " table, find failed");
        XmlCtx* xmlCtx = schemaXmlMapIt->second;

        return xmlCtx->dictXdbXNmFind(rowId);
    }

    XdbXPt* Schema::dictXdbXPtFind(const std::string& tokSuf, typeRowId rowId) {
        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$PT" + tokSuf + " table, find failed");
        XmlCtx* xmlCtx = schemaXmlMapIt->second;

        return xmlCtx->dictXdbXPtFind(rowId);
    }

    XdbXQn* Schema::dictXdbXQnFind(const std::string& tokSuf, typeRowId rowId) {
        auto schemaXmlMapIt = schemaXmlMap.find(tokSuf);
        if (schemaXmlMapIt == schemaXmlMap.end())
            throw DataException(50068, "missing XDB.X$QN" + tokSuf + " table, find failed");
        XmlCtx* xmlCtx = schemaXmlMapIt->second;

        return xmlCtx->dictXdbXQnFind(rowId);
    }

    void Schema::touchTable(typeObj obj) {
        if (obj == 0)
            return;

        identifiersTouched.insert(obj);
        auto tableMapIt = tableMap.find(obj);
        if (tableMapIt == tableMap.end())
            return;

        auto tablesTouchedIt = tablesTouched.find(tableMapIt->second);
        if (tablesTouchedIt != tablesTouched.end())
            return;

        tablesTouched.insert(tableMapIt->second);
    }

    OracleTable* Schema::checkTableDict(typeObj obj) const {
        const auto tablePartitionMapIt = tablePartitionMap.find(obj);
        if (tablePartitionMapIt != tablePartitionMap.end())
            return tablePartitionMapIt->second;

        return nullptr;
    }

    bool Schema::checkTableDictUncommitted(typeObj obj, std::string& owner, std::string& table) const {
        const auto objIt = sysObjMapObj.find(obj);
        if (objIt == sysObjMapObj.end())
            return false;
        SysObj* sysObj = objIt->second;

        const auto userIt = sysUserMapUser.find(sysObj->owner);
        if (userIt == sysUserMapUser.end())
            return false;
        const SysUser* sysUser = userIt->second;

        table = sysObj->name;
        owner = sysUser->name;
        return true;
    }

    OracleLob* Schema::checkLobDict(typeDataObj dataObj) const {
        const auto lobPartitionMapIt = lobPartitionMap.find(dataObj);
        if (lobPartitionMapIt != lobPartitionMap.end())
            return lobPartitionMapIt->second;

        return nullptr;
    }

    OracleLob* Schema::checkLobIndexDict(typeDataObj dataObj) const {
        const auto lobIndexMapIt = lobIndexMap.find(dataObj);
        if (lobIndexMapIt != lobIndexMap.end())
            return lobIndexMapIt->second;

        return nullptr;
    }

    void Schema::addTableToDict(OracleTable* table) {
        if (tableMap.find(table->obj) != tableMap.end())
            throw DataException(50031, "can't add table (obj: " + std::to_string(table->obj) + ", dataobj: " + std::to_string(table->dataObj) + ")");

        tableMap.insert_or_assign(table->obj, table);

        for (auto lob: table->lobs) {
            for (auto dataObj: lob->lobIndexes) {
                if (lobIndexMap.find(dataObj) == lobIndexMap.end())
                    lobIndexMap.insert_or_assign(dataObj, lob);
                else
                    throw DataException(50032, "can't add lob index element (dataobj: " + std::to_string(dataObj) + ")");
            }

            for (auto dataObj: lob->lobPartitions) {
                if (lobPartitionMap.find(dataObj) == lobPartitionMap.end())
                    lobPartitionMap.insert_or_assign(dataObj, lob);
            }
        }

        if (tablePartitionMap.find(table->obj) == tablePartitionMap.end())
            tablePartitionMap.insert_or_assign(table->obj, table);
        else
            throw DataException(50033, "can't add partition (obj: " + std::to_string(table->obj) + ", dataobj: " +
                                       std::to_string(table->dataObj) + ")");

        for (typeObj2 objx: table->tablePartitions) {
            typeObj obj = objx >> 32;
            typeDataObj dataObj = objx & 0xFFFFFFFF;

            if (tablePartitionMap.find(obj) == tablePartitionMap.end())
                tablePartitionMap.insert_or_assign(obj, table);
            else
                throw DataException(50034, "can't add partition element (obj: " + std::to_string(obj) + ", dataobj: " +
                                           std::to_string(dataObj) + ")");
        }
    }

    void Schema::removeTableFromDict(OracleTable* table) {
        auto tablePartitionMapIt = tablePartitionMap.find(table->obj);
        if (tablePartitionMapIt != tablePartitionMap.end())
            tablePartitionMap.erase(tablePartitionMapIt);
        else
            throw DataException(50035, "can't remove partition (obj: " + std::to_string(table->obj) + ", dataobj: " +
                                       std::to_string(table->dataObj) + ")");

        for (typeObj2 objx: table->tablePartitions) {
            typeObj obj = objx >> 32;
            typeDataObj dataObj = objx & 0xFFFFFFFF;

            tablePartitionMapIt = tablePartitionMap.find(obj);
            if (tablePartitionMapIt != tablePartitionMap.end())
                tablePartitionMap.erase(tablePartitionMapIt);
            else
                throw DataException(50036, "can't remove table partition element (obj: " + std::to_string(obj) + ", dataobj: " +
                                           std::to_string(dataObj) + ")");
        }

        for (const auto lob: table->lobs) {
            for (auto dataObj: lob->lobIndexes) {
                auto lobIndexMapIt = lobIndexMap.find(dataObj);
                if (lobIndexMapIt != lobIndexMap.end())
                    lobIndexMap.erase(lobIndexMapIt);
                else
                    throw DataException(50037, "can't remove lob index element (dataobj: " + std::to_string(dataObj) + ")");
            }

            for (auto dataObj: lob->lobPartitions) {
                auto lobPartitionMapIt = lobPartitionMap.find(dataObj);
                if (lobPartitionMapIt != lobPartitionMap.end())
                    lobPartitionMap.erase(lobPartitionMapIt);
            }
        }

        auto tableMapIt = tableMap.find(table->obj);
        if (tableMapIt != tableMap.end())
            tableMap.erase(tableMapIt);
        else
            throw DataException(50038, "can't remove table (obj: " + std::to_string(table->obj) + ", dataobj: " +
                                       std::to_string(table->dataObj) + ")");
    }

    void Schema::dropUnusedMetadata(const std::set<std::string>& users, const std::vector<SchemaElement*>& schemaElements, std::vector<std::string>& msgs) {
        for (OracleTable* table: tablesTouched) {
            msgs.push_back(table->owner + "." + table->name + " (dataobj: " + std::to_string(table->dataObj) + ", obj: " +
                           std::to_string(table->obj) + ") ");
            removeTableFromDict(table);
            delete table;
        }
        tablesTouched.clear();

        // SYS.USER$
        for (auto sysUser: sysUserSetTouched) {
            if (users.find(sysUser->name) != users.end())
                continue;

            dictSysUserDrop(sysUser);
            delete sysUser;
        }

        // SYS.OBJ$
        if (!FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA)) {
            // delete objects owned by users that are not in the list of users
            for (auto sysObj: sysObjSetTouched) {
                auto sysUserMapUserIt = sysUserMapUser.find(sysObj->owner);
                if (sysUserMapUserIt != sysUserMapUser.end()) {
                    SysUser* sysUser = sysUserMapUserIt->second;
                    if (sysUser->name == "SYS") {
                        if (!sysUser->single)
                            continue;
                    } else if (sysUser->name == "XDB") {
                        if (!sysUser->single)
                            continue;
                    } else
                        continue;

                    // SYS or XDB user, check if matches list of system table
                    for (SchemaElement* element: schemaElements) {
                        std::regex regexOwner(element->owner);
                        std::regex regexTable(element->table);

                        // matches, keep it
                        if (regex_match(sysUser->name, regexOwner) && regex_match(sysObj->name, regexTable))
                            continue;
                    }
                }

                dictSysObjDrop(sysObj);
                delete sysObj;
            }
        }

        // SYS.CCOL$
        for (auto sysCCol: sysCColSetTouched) {
            if (sysObjMapObj.find(sysCCol->obj) != sysObjMapObj.end())
                continue;

            dictSysCColDrop(sysCCol);
            delete sysCCol;
        }

        // SYS.CDEF$
        for (auto sysCDef: sysCDefSetTouched) {
            if (sysObjMapObj.find(sysCDef->obj) != sysObjMapObj.end())
                continue;

            dictSysCDefDrop(sysCDef);
            delete sysCDef;
        }

        // SYS.COL$
        for (auto sysCol: sysColSetTouched) {
            if (sysObjMapObj.find(sysCol->obj) != sysObjMapObj.end())
                continue;

            dictSysColDrop(sysCol);
            delete sysCol;
        }

        // SYS.DEFERRED_STG$
        for (auto sysDeferredStg: sysDeferredStgSetTouched) {
            if (sysObjMapObj.find(sysDeferredStg->obj) != sysObjMapObj.end())
                continue;

            dictSysDeferredStgDrop(sysDeferredStg);
            delete sysDeferredStg;
        }

        // SYS.ECOL$
        for (auto sysECol: sysEColSetTouched) {
            if (sysObjMapObj.find(sysECol->tabObj) != sysObjMapObj.end())
                continue;

            dictSysEColDrop(sysECol);
            delete sysECol;
        }

        // SYS.LOB$
        for (auto sysLob: sysLobSetTouched) {
            if (sysObjMapObj.find(sysLob->obj) != sysObjMapObj.end())
                continue;

            dictSysLobDrop(sysLob);
            delete sysLob;
        }

        // SYS.LOBCOMPPART$
        for (auto sysLobCompPart: sysLobCompPartSetTouched) {
            if (sysLobMapLObj.find(sysLobCompPart->lObj) != sysLobMapLObj.end())
                continue;

            dictSysLobCompPartDrop(sysLobCompPart);
            delete sysLobCompPart;
        }

        // SYS.LOBFRAG$
        for (auto sysLobFrag: sysLobFragSetTouched) {
            if (sysLobCompPartMapPartObj.find(sysLobFrag->parentObj) != sysLobCompPartMapPartObj.end())
                continue;
            if (sysLobMapLObj.find(sysLobFrag->parentObj) != sysLobMapLObj.end())
                continue;

            dictSysLobFragDrop(sysLobFrag);
            delete sysLobFrag;
        }

        // SYS.TAB$
        for (auto sysTab: sysTabSetTouched) {
            if (sysObjMapObj.find(sysTab->obj) != sysObjMapObj.end())
                continue;

            dictSysTabDrop(sysTab);
            delete sysTab;
        }

        // SYS.TABCOMPART$
        for (auto sysTabComPart: sysTabComPartSetTouched) {
            if (sysObjMapObj.find(sysTabComPart->obj) != sysObjMapObj.end())
                continue;

            dictSysTabComPartDrop(sysTabComPart);
            delete sysTabComPart;
        }

        // SYS.TABPART$
        for (auto sysTabPart: sysTabPartSetTouched) {
            if (sysObjMapObj.find(sysTabPart->bo) != sysObjMapObj.end())
                continue;

            dictSysTabPartDrop(sysTabPart);
            delete sysTabPart;
        }

        // SYS.TABSUBPART$
        for (auto sysTabSubPart: sysTabSubPartSetTouched) {
            if (sysObjMapObj.find(sysTabSubPart->obj) != sysObjMapObj.end())
                continue;

            dictSysTabSubPartDrop(sysTabSubPart);
            delete sysTabSubPart;
        }
    }

    void Schema::resetTouched() {
        tablesTouched.clear();
        identifiersTouched.clear();
        sysCColSetTouched.clear();
        sysCDefSetTouched.clear();
        sysColSetTouched.clear();
        sysDeferredStgSetTouched.clear();
        sysEColSetTouched.clear();
        sysLobSetTouched.clear();
        sysLobCompPartSetTouched.clear();
        sysLobFragSetTouched.clear();
        sysObjSetTouched.clear();
        sysTabSetTouched.clear();
        sysTabComPartSetTouched.clear();
        sysTabPartSetTouched.clear();
        sysTabSubPartSetTouched.clear();
        sysUserSetTouched.clear();
        touched = false;
    }

    void Schema::updateXmlCtx() {
        if ((ctx->flags & REDO_FLAGS_EXPERIMENTAL_XMLTYPE) != 0) {
            xmlCtxDefault = nullptr;
            auto schemaXmlMapIt = schemaXmlMap.begin();
            while (schemaXmlMapIt != schemaXmlMap.end()) {
                if (schemaXmlMapIt->second->flags == 0) {
                    xmlCtxDefault = schemaXmlMapIt->second;
                    break;
                }
                ++schemaXmlMapIt;
            }
            if (xmlCtxDefault == nullptr)
                throw DataException(50069, "no active XML context found");
        }

    }

    void Schema::buildMaps(const std::string& owner, const std::string& table, const std::vector<std::string>& keys, const std::string& keysStr,
                           const std::string& conditionStr, typeOptions options, std::vector<std::string>& msgs, bool suppLogDbPrimary, bool suppLogDbAll,
                           uint64_t defaultCharacterMapId, uint64_t defaultCharacterNcharMapId) {
        std::regex regexOwner(owner);
        std::regex regexTable(table);
        char sysLobConstraintName[26] = "SYS_LOB0000000000C00000$$";

        for (auto obj: identifiersTouched) {
            auto sysObjMapObjTouchedIt = sysObjMapObj.find(obj);
            if (sysObjMapObjTouchedIt == sysObjMapObj.end())
                continue;
            SysObj* sysObj = sysObjMapObjTouchedIt->second;

            if (sysObj->isDropped() || !sysObj->isTable() || !regex_match(sysObj->name, regexTable))
                continue;

            SysUser* sysUser = nullptr;
            auto sysUserMapUserIt = sysUserMapUser.find(sysObj->owner);
            if (sysUserMapUserIt == sysUserMapUser.end()) {
                if (!FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA))
                    continue;
                sysUserAdaptive.name = "USER_" + std::to_string(sysObj->obj);
                sysUser = &sysUserAdaptive;
            } else {
                sysUser = sysUserMapUserIt->second;
                if (!regex_match(sysUser->name, regexOwner))
                    continue;
            }

            // Table already added with another rule
            if (tableMap.find(sysObj->obj) != tableMap.end()) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - already added (skipped)");
                continue;
            }

            // Object without SYS.TAB$
            auto sysTabMapObjIt = sysTabMapObj.find(sysObj->obj);
            if (sysTabMapObjIt == sysTabMapObj.end()) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - SYS.TAB$ entry missing (skipped)");
                continue;
            }
            SysTab* sysTab = sysTabMapObjIt->second;

            // Skip binary objects
            if (sysTab->isBinary()) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - binary (skipped");
                continue;
            }

            // Skip Index Organized Tables (IOT)
            if (sysTab->isIot()) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - IOT (skipped)");
                continue;
            }

            // Skip temporary tables
            if (sysObj->isTemporary()) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - temporary table (skipped)");
                continue;
            }

            // Skip nested tables
            if (sysTab->isNested()) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - nested table (skipped)");
                continue;
            }

            bool compressed = false;
            if (sysTab->isPartitioned())
                compressed = false;
            else if (sysTab->isInitial()) {
                SysDeferredStg* sysDeferredStg = sysDeferredStgMapObj[sysObj->obj];
                if (sysDeferredStg != nullptr)
                    compressed = sysDeferredStg->isCompressed();
            }

            // Skip compressed tables
            if (compressed) {
                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - compressed table (skipped)");
                continue;
            }

            typeCol keysCnt = 0;
            bool suppLogTablePrimary = false;
            bool suppLogTableAll = false;
            bool supLogColMissing = false;

            tableTmp = new OracleTable(sysObj->obj, sysTab->dataObj, sysObj->owner, sysTab->cluCols,
                                       options, sysUser->name, sysObj->name);

            uint64_t lobPartitions = 0;
            uint64_t lobIndexes = 0;
            std::ostringstream lobIndexesList;
            std::ostringstream lobList;
            uint64_t tablePartitions = 0;

            if (sysTab->isPartitioned()) {
                SysTabPartKey sysTabPartKey(sysObj->obj, 0);
                for (auto sysTabPartMapKeyIt = sysTabPartMapKey.upper_bound(sysTabPartKey);
                     sysTabPartMapKeyIt != sysTabPartMapKey.end() && sysTabPartMapKeyIt->first.bo == sysObj->obj; ++sysTabPartMapKeyIt) {

                    const SysTabPart* sysTabPart = sysTabPartMapKeyIt->second;
                    tableTmp->addTablePartition(sysTabPart->obj, sysTabPart->dataObj);
                    ++tablePartitions;
                }

                SysTabComPartKey sysTabComPartKey(sysObj->obj, 0);
                for (auto sysTabComPartMapKeyIt = sysTabComPartMapKey.upper_bound(sysTabComPartKey);
                     sysTabComPartMapKeyIt != sysTabComPartMapKey.end() && sysTabComPartMapKeyIt->first.bo == sysObj->obj; ++sysTabComPartMapKeyIt) {

                    SysTabSubPartKey sysTabSubPartKeyFirst(sysTabComPartMapKeyIt->second->obj, 0);
                    for (auto sysTabSubPartMapKeyIt = sysTabSubPartMapKey.upper_bound(sysTabSubPartKeyFirst);
                         sysTabSubPartMapKeyIt != sysTabSubPartMapKey.end() && sysTabSubPartMapKeyIt->first.pObj == sysTabComPartMapKeyIt->second->obj;
                         ++sysTabSubPartMapKeyIt) {

                        const SysTabSubPart* sysTabSubPart = sysTabSubPartMapKeyIt->second;
                        tableTmp->addTablePartition(sysTabSubPart->obj, sysTabSubPart->dataObj);
                        ++tablePartitions;
                    }
                }
            }

            if (!DISABLE_CHECKS(DISABLE_CHECKS_SUPPLEMENTAL_LOG) && (options & OPTIONS_SYSTEM_TABLE) == 0 &&
                !suppLogDbAll && !sysUser->isSuppLogAll()) {

                SysCDefKey sysCDefKeyFirst(sysObj->obj, 0);
                for (auto sysCDefMapKeyIt = sysCDefMapKey.upper_bound(sysCDefKeyFirst);
                     sysCDefMapKeyIt != sysCDefMapKey.end() && sysCDefMapKeyIt->first.obj == sysObj->obj;
                     ++sysCDefMapKeyIt) {
                    const SysCDef* sysCDef = sysCDefMapKeyIt->second;
                    if (sysCDef->isSupplementalLogPK())
                        suppLogTablePrimary = true;
                    else if (sysCDef->isSupplementalLogAll())
                        suppLogTableAll = true;
                }
            }

            typeRowId rowId;
            SysColSeg sysColSegFirst(sysObj->obj, 0, rowId);
            for (auto sysColMapSegIt = sysColMapSeg.upper_bound(sysColSegFirst); sysColMapSegIt != sysColMapSeg.end() &&
                                                                                 sysColMapSegIt->first.obj == sysObj->obj; ++sysColMapSegIt) {
                SysCol* sysCol = sysColMapSegIt->second;
                if (sysCol->segCol == 0)
                    continue;

                uint64_t charmapId = 0;
                typeCol numPk = 0;
                typeCol numSup = 0;
                typeCol guardSeg = -1;

                SysEColKey sysEColKey(sysObj->obj, sysCol->segCol);
                auto sysEColIt = sysEColMapKey.find(sysEColKey);
                if (sysEColIt != sysEColMapKey.end())
                    guardSeg = sysEColIt->second->guardId;

                if (sysCol->charsetForm == 1) {
                    if (sysCol->type == SYS_COL_TYPE_CLOB) {
                        charmapId = defaultCharacterNcharMapId;
                    } else
                        charmapId = defaultCharacterMapId;
                } else if (sysCol->charsetForm == 2)
                    charmapId = defaultCharacterNcharMapId;
                else
                    charmapId = sysCol->charsetId;

                if (sysCol->type == SYS_COL_TYPE_VARCHAR || sysCol->type == SYS_COL_TYPE_CHAR || sysCol->type == SYS_COL_TYPE_CLOB) {
                    auto characterMapIt = locales->characterMap.find(charmapId);
                    if (characterMapIt == locales->characterMap.end()) {
                        ctx->hint("check in database for name: SELECT NLS_CHARSET_NAME(" + std::to_string(charmapId) + ") FROM DUAL;");
                        throw DataException(50026, "table " + std::string(sysUser->name) + "." + sysObj->name +
                                                   " - unsupported character set id: " + std::to_string(charmapId) + " for column: " + sysCol->name);
                    }
                }

                SysCColKey sysCColKeyFirst(sysObj->obj, sysCol->intCol, 0);
                for (auto sysCColMapKeyIt = sysCColMapKey.upper_bound(sysCColKeyFirst);
                     sysCColMapKeyIt != sysCColMapKey.end() && sysCColMapKeyIt->first.obj == sysObj->obj && sysCColMapKeyIt->first.intCol == sysCol->intCol;
                     ++sysCColMapKeyIt) {
                    SysCCol* sysCCol = sysCColMapKeyIt->second;

                    // Count the number of PKs the column is part of
                    auto sysCDefMapConIt = sysCDefMapCon.find(sysCCol->con);
                    if (sysCDefMapConIt == sysCDefMapCon.end()) {
                        ctx->warning(70005, "data in SYS.CDEF$ missing for CON#: " + std::to_string(sysCCol->con));
                        continue;
                    }
                    const SysCDef* sysCDef = sysCDefMapConIt->second;
                    if (sysCDef->isPK())
                        ++numPk;

                    // Supplemental logging
                    if (sysCCol->spare1.isZero() && sysCDef->isSupplementalLog())
                        ++numSup;
                }

                // Part of defined primary key
                if (!keys.empty()) {
                    // Manually defined pk overlaps with table pk
                    if (numPk > 0 && (suppLogTablePrimary || sysUser->isSuppLogPrimary() || suppLogDbPrimary))
                        numSup = 1;
                    numPk = 0;
                    for (const auto& key: keys) {
                        if (strcmp(sysCol->name.c_str(), key.c_str()) == 0) {
                            numPk = 1;
                            ++keysCnt;
                            if (numSup == 0)
                                supLogColMissing = true;
                            break;
                        }
                    }
                } else {
                    if (numPk > 0 && numSup == 0)
                        supLogColMissing = true;
                }

                if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                    msgs.push_back("- col: " + std::to_string(sysCol->segCol) + ": " + sysCol->name + " (pk: " + std::to_string(numPk) + ", S: " +
                                   std::to_string(numSup) + ", G: " + std::to_string(guardSeg) + ")");

                bool xmlType = false;
                // For system-generated columns, check column name from base column
                std::string columnName = sysCol->name;
                if (sysCol->isSystemGenerated()) {
                    //typeRowId rid2(0, 0, 0);
                    //SysColSeg sysColSegFirst2(sysObj->obj - 1, 0, rid2);
                    for (auto sysColMapSegIt2 = sysColMapSeg.upper_bound(sysColSegFirst); sysColMapSegIt2 != sysColMapSeg.end() &&
                                                                                          sysColMapSegIt2->first.obj <= sysObj->obj; ++sysColMapSegIt2) {
                        const SysCol* sysCol2 = sysColMapSegIt2->second;
                        if (sysCol->col == sysCol2->col && sysCol2->segCol == 0) {
                            columnName = sysCol2->name;
                            xmlType = true;
                            break;
                        }
                    }
                }

                columnTmp = new OracleColumn(sysCol->col, guardSeg, sysCol->segCol, columnName,
                                             sysCol->type, sysCol->length, sysCol->precision, sysCol->scale,
                                             numPk, charmapId, sysCol->isNullable(), sysCol->isHidden() &&
                                                                                     !(xmlType && FLAG(REDO_FLAGS_EXPERIMENTAL_XMLTYPE)),
                                             sysCol->isStoredAsLob(), sysCol->isSystemGenerated(), sysCol->isNested(),
                                             sysCol->isUnused(), sysCol->isAdded(), sysCol->isGuard(), xmlType);

                tableTmp->addColumn(columnTmp);
                columnTmp = nullptr;
            }

            if ((options & OPTIONS_SYSTEM_TABLE) == 0) {
                SysLobKey sysLobKeyFirst(sysObj->obj, 0);
                for (auto sysLobMapKeyIt = sysLobMapKey.upper_bound(sysLobKeyFirst);
                     sysLobMapKeyIt != sysLobMapKey.end() && sysLobMapKeyIt->first.obj == sysObj->obj; ++sysLobMapKeyIt) {

                    SysLob* sysLob = sysLobMapKeyIt->second;

                    auto sysObjMapObjIt = sysObjMapObj.find(sysLob->lObj);
                    if (sysObjMapObjIt == sysObjMapObj.end())
                        throw DataException(50027, "table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find obj for lob " +
                                                   std::to_string(sysLob->lObj));
                    typeObj lobDataObj = sysObjMapObjIt->second->dataObj;

                    if (ctx->logLevel >= LOG_LEVEL_DEBUG)
                        msgs.push_back("- lob: " + std::to_string(sysLob->col) + ":" + std::to_string(sysLob->intCol) + ":" +
                                       std::to_string(lobDataObj) + ":" + std::to_string(sysLob->lObj));

                    lobTmp = new OracleLob(tableTmp, sysLob->obj, lobDataObj, sysLob->lObj, sysLob->col,
                                           sysLob->intCol);

                    // Indexes
                    std::ostringstream str;
                    str << "SYS_IL" << std::setw(10) << std::setfill('0') << sysObj->obj << "C" << std::setw(5)
                        << std::setfill('0') << sysLob->intCol << "$$";
                    std::string lobIndexName = str.str();

                    SysObjNameKey sysObjNameKeyFirst(sysObj->owner, lobIndexName.c_str(), 0, 0);
                    for (auto sysObjMapNameIt = sysObjMapName.upper_bound(sysObjNameKeyFirst);
                         sysObjMapNameIt != sysObjMapName.end() &&
                         sysObjMapNameIt->first.name == lobIndexName &&
                         sysObjMapNameIt->first.owner == sysObj->owner; ++sysObjMapNameIt) {

                        if (sysObjMapNameIt->first.dataObj == 0)
                            continue;

                        lobTmp->addIndex(sysObjMapNameIt->first.dataObj);
                        if ((ctx->trace & TRACE_LOB) != 0)
                            lobIndexesList << " " << std::dec << sysObjMapNameIt->first.dataObj << "/" << sysObjMapNameIt->second->obj;
                        ++lobIndexes;
                    }

                    if (lobTmp->lobIndexes.size() == 0) {
                        ctx->warning(60021, "missing LOB index for LOB (OBJ#: " + std::to_string(sysObj->obj) + ", DATAOBJ#: " +
                                            std::to_string(sysLob->lObj) + ", COL#: " + std::to_string(sysLob->intCol) + ")");
                    }

                    // Partitioned lob
                    if (sysTab->isPartitioned()) {
                        // Partitions
                        SysLobFragKey sysLobFragKey(sysLob->lObj, 0);
                        for (auto sysLobFragMapKeyIt = sysLobFragMapKey.upper_bound(sysLobFragKey);
                             sysLobFragMapKeyIt != sysLobFragMapKey.end() &&
                             sysLobFragMapKeyIt->first.parentObj == sysLob->lObj; ++sysLobFragMapKeyIt) {

                            SysLobFrag* sysLobFrag = sysLobFragMapKeyIt->second;
                            auto sysObjMapObjIt2 = sysObjMapObj.find(sysLobFrag->fragObj);
                            if (sysObjMapObjIt2 == sysObjMapObj.end())
                                throw DataException(50028, "table " + std::string(sysUser->name) + "." + sysObj->name +
                                                           " couldn't find obj for lob frag " + std::to_string(sysLobFrag->fragObj));
                            typeObj lobFragDataObj = sysObjMapObjIt2->second->dataObj;

                            lobTmp->addPartition(lobFragDataObj, getLobBlockSize(sysLobFrag->ts));
                            ++lobPartitions;
                        }

                        // Subpartitions
                        SysLobCompPartKey sysLobCompPartKey(sysLob->lObj, 0);
                        for (auto sysLobCompPartMapKeyIt = sysLobCompPartMapKey.upper_bound(sysLobCompPartKey);
                             sysLobCompPartMapKeyIt != sysLobCompPartMapKey.end() &&
                             sysLobCompPartMapKeyIt->first.lObj == sysLob->lObj; ++sysLobCompPartMapKeyIt) {

                            const SysLobCompPart* sysLobCompPart = sysLobCompPartMapKeyIt->second;

                            SysLobFragKey sysLobFragKey2(sysLobCompPart->partObj, 0);
                            for (auto sysLobFragMapKeyIt = sysLobFragMapKey.upper_bound(sysLobFragKey2);
                                 sysLobFragMapKeyIt != sysLobFragMapKey.end() &&
                                 sysLobFragMapKeyIt->first.parentObj == sysLobCompPart->partObj; ++sysLobFragMapKeyIt) {

                                SysLobFrag* sysLobFrag = sysLobFragMapKeyIt->second;
                                auto sysObjMapObjIt2 = sysObjMapObj.find(sysLobFrag->fragObj);
                                if (sysObjMapObjIt2 == sysObjMapObj.end())
                                    throw DataException(50028, "table " + std::string(sysUser->name) + "." + sysObj->name +
                                                               " couldn't find obj for lob frag " + std::to_string(sysLobFrag->fragObj));
                                typeObj lobFragDataObj = sysObjMapObjIt2->second->dataObj;

                                lobTmp->addPartition(lobFragDataObj, getLobBlockSize(sysLobFrag->ts));
                                ++lobPartitions;
                            }
                        }
                    }

                    lobTmp->addPartition(lobTmp->dataObj, getLobBlockSize(sysLob->ts));
                    tableTmp->addLob(lobTmp);
                    if ((ctx->trace & TRACE_LOB) != 0)
                        lobList << " " << std::dec << lobTmp->obj << "/" << lobTmp->dataObj << "/" << std::dec << lobTmp->lObj;
                    lobTmp = nullptr;
                }

                // 0123456 7890123456 7 89012 34
                // SYS_LOB xxxxxxxxxx C yyyyy $$
                typeObj obj2 = sysObj->obj;
                for (uint j = 0; j < 10; ++j) {
                    sysLobConstraintName[16 - j] = (obj2 % 10) + '0';
                    obj2 /= 10;
                }

                SysObjNameKey sysObjNameKeyName(sysObj->owner, sysLobConstraintName, 0, 0);
                for (auto sysObjMapNameIt = sysObjMapName.upper_bound(sysObjNameKeyName); sysObjMapNameIt != sysObjMapName.end();
                     ++sysObjMapNameIt) {
                    SysObj* sysObjLob = sysObjMapNameIt->second;
                    const char* colStr = sysObjLob->name.c_str();

                    if (sysObjLob->name.length() != 25 || memcmp(colStr, sysLobConstraintName, 18) != 0 || colStr[23] != '$' || colStr[24] != '$')
                        continue;

                    // Decode column id
                    typeCol col = 0;
                    for (uint j = 0; j < 5; ++j) {
                        col += colStr[18 + j] - '0';
                        col *= 10;
                    }

                    // FIXME: potentially slow for tables with large number of LOB columns
                    OracleLob* oracleLob = nullptr;
                    for (auto lobIt: tableTmp->lobs) {
                        if (lobIt->intCol == col) {
                            oracleLob = lobIt;
                            break;
                        }
                    }

                    if (oracleLob == nullptr) {
                        lobTmp = new OracleLob(tableTmp, sysObj->obj, 0, 0, col, col);
                        tableTmp->addLob(lobTmp);
                        oracleLob = lobTmp;
                        lobTmp = nullptr;
                    }

                    oracleLob->addPartition(sysObjLob->dataObj, getLobBlockSize(sysTab->ts));
                }
            }

            // Check if a table has all listed columns
            if (static_cast<typeCol>(keys.size()) != keysCnt)
                throw DataException(10041, "table " + std::string(sysUser->name) + "." + sysObj->name + " - couldn't find all column sets (" +
                                           keysStr + ")");

            std::ostringstream ss;
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << std::dec << sysTab->dataObj << ", obj: " << std::dec << sysObj->obj <<
               ", columns: " << std::dec << tableTmp->maxSegCol << ", lobs: " << std::dec << tableTmp->totalLobs << lobList.str() <<
               ", lob-idx: " << std::dec << lobIndexes << lobIndexesList.str() << ")";
            if (sysTab->isClustered())
                ss << ", part of cluster";
            if (sysTab->isPartitioned())
                ss << ", partitioned(table: " << std::dec << tablePartitions << ", lob: " << lobPartitions << ")";
            if (sysTab->isDependencies())
                ss << ", row dependencies";
            if (sysTab->isRowMovement())
                ss << ", row movement enabled";

            if (!DISABLE_CHECKS(DISABLE_CHECKS_SUPPLEMENTAL_LOG) && (options & OPTIONS_SYSTEM_TABLE) == 0) {
                // Use a default primary key
                if (keys.empty()) {
                    if (tableTmp->totalPk == 0)
                        ss << ", primary key missing";
                    else if (!suppLogTablePrimary && !suppLogTableAll && !sysUser->isSuppLogPrimary() && !sysUser->isSuppLogAll() &&
                             !suppLogDbPrimary && !suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name <<
                           " ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;";
                    // User defined primary key
                } else {
                    if (!suppLogTableAll && !sysUser->isSuppLogAll() && !suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG GROUP GRP" <<
                           std::dec << sysObj->obj << " (" << keysStr << ") ALWAYS;";
                }
            }
            msgs.push_back(ss.str());

            tableTmp->setConditionStr(conditionStr);
            addTableToDict(tableTmp);
            tableTmp = nullptr;
        }
    }

    uint16_t Schema::getLobBlockSize(typeTs ts) {
        auto sysTsMapTsIt = sysTsMapTs.find(ts);
        if (sysTsMapTsIt != sysTsMapTs.end()) {
            uint32_t pageSize = sysTsMapTsIt->second->blockSize;
            if (pageSize == 8192)
                return 8132;
            else if (pageSize == 16384)
                return 16264;
            else if (pageSize == 32768)
                return 32528;
            else
                ctx->warning(60022, "missing TS#: " + std::to_string(ts) + ", BLOCKSIZE: " + std::to_string(pageSize) + ")");
        } else
            ctx->warning(60022, "missing TS#: " + std::to_string(ts) + ")");

        // Default value?
        return 8132;
    }
}
