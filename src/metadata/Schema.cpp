/* Base class for handling of schema
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

#include <list>
#include <regex>

#include "../common/Ctx.h"
#include "../common/DataException.h"
#include "../common/OracleColumn.h"
#include "../common/OracleLob.h"
#include "../common/OracleTable.h"
#include "../common/SysCol.h"
#include "../common/SysDeferredStg.h"
#include "../common/SysTab.h"
#include "../locales/Locales.h"
#include "Schema.h"

namespace OpenLogReplicator {
    Schema::Schema(Ctx* newCtx, Locales* newLocales) :
            ctx(newCtx),
            locales(newLocales),
            sysUserAdaptive(sysUserRowId, 0, "", 0, 0, false),
            scn(ZERO_SCN),
            refScn(ZERO_SCN),
            loaded(false),
            schemaColumn(nullptr),
            schemaLob(nullptr),
            schemaTable(nullptr) {
    }

    Schema::~Schema() {
        purge();
    }

    void Schema::purge() {
        scn = ZERO_SCN;
        if (schemaColumn != nullptr) {
            delete schemaColumn;
            schemaColumn = nullptr;
        }

        if (schemaLob != nullptr) {
            delete schemaLob;
            schemaLob = nullptr;
        }

        if (schemaTable != nullptr) {
            delete schemaTable;
            schemaTable = nullptr;
        }

        while (!tableMap.empty()) {
            auto tableMapTt = tableMap.cbegin();
            OracleTable* table = tableMapTt->second;
            removeTableFromDict(table);
            delete table;
        }

        if (!lobPartitionMap.empty()) {
            ERROR("schema lob partition map not empty, left: " << std::dec << lobPartitionMap.size())
        }
        lobPartitionMap.clear();
        if (!lobIndexMap.empty()) {
            ERROR("schema lob index map not empty, left: " << std::dec << lobIndexMap.size())
        }
        lobIndexMap.clear();
        if (!tablePartitionMap.empty()) {
            ERROR("schema table partition map not empty, left: " << std::dec << tablePartitionMap.size())
        }
        tablePartitionMap.clear();

        //SYS.CCOL$
        while (!sysCColMapRowId.empty()) {
            auto sysCColMapRowIdIt = sysCColMapRowId.cbegin();
            SysCCol* sysCCol = sysCColMapRowIdIt->second;
            dictSysCColDrop(sysCCol);
            delete sysCCol;
        }
        if (!sysCColMapKey.empty()) {
            ERROR("SYS.CCOL$ map key not empty on shutdown")
        }

        //SYS.CDEF$
        while (!sysCDefMapRowId.empty()) {
            auto sysCDefMapRowIdIt = sysCDefMapRowId.cbegin();
            SysCDef *sysCDef = sysCDefMapRowIdIt->second;
            dictSysCDefDrop(sysCDef);
            delete sysCDef;
        }
        if (!sysCDefMapCon.empty()) {
            ERROR("SYS.CDEF$ map con# not empty on shutdown")
        }
        if (!sysCDefMapKey.empty()) {
            ERROR("SYS.CDEF$ map key not empty on shutdown")
        }

        //SYS.COL$
        while (!sysColMapRowId.empty()) {
            auto sysColMapRowIdIt = sysColMapRowId.cbegin();
            SysCol* sysCol = sysColMapRowIdIt->second;
            dictSysColDrop(sysCol);
            delete sysCol;
        }
        if (!sysColMapSeg.empty()) {
            ERROR("SYS.COL$ map seg# not empty on shutdown")
        }

        //SYS.DEFERRED_STG$
        while (!sysDeferredStgMapRowId.empty()) {
            auto sysDeferredStgMapRowIdIt = sysDeferredStgMapRowId.cbegin();
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt->second;
            dictSysDeferredStgDrop(sysDeferredStg);
            delete sysDeferredStg;
        }
        if (!sysDeferredStgMapObj.empty()) {
            ERROR("SYS.DEFERRED_STG$ map obj# not empty on shutdown")
        }

        //SYS.ECOL$
        while (!sysEColMapRowId.empty()) {
            auto sysEColMapRowIdIt = sysEColMapRowId.cbegin();
            SysECol* sysECol = sysEColMapRowIdIt->second;
            dictSysEColDrop(sysECol);
            delete sysECol;
        }
        if (!sysEColMapKey.empty()) {
            ERROR("SYS.ECOL$ map key not empty on shutdown")
        }

        //SYS.LOB$
        while (!sysLobMapRowId.empty()) {
            auto sysLobMapRowIdIt = sysLobMapRowId.cbegin();
            SysLob* sysLob = sysLobMapRowIdIt->second;
            dictSysLobDrop(sysLob);
            delete sysLob;
        }
        if (!sysLobMapLObj.empty()) {
            ERROR("SYS.LOB map lobj# not empty on shutdown")
        }
        if (!sysLobMapKey.empty()) {
            ERROR("SYS.LOB$ map key not empty on shutdown")
        }

        //SYS.LOBCOMPPART$
        while (!sysLobCompPartMapRowId.empty()) {
            auto sysLobCompPartMapRowIdIt = sysLobCompPartMapRowId.cbegin();
            SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt->second;
            dictSysLobCompPartDrop(sysLobCompPart);
            delete sysLobCompPart;
        }
        if (!sysLobCompPartMapPartObj.empty()) {
            ERROR("SYS.LOBCOMPPART$ map partobj# not empty on shutdown")
        }
        if (!sysLobCompPartMapKey.empty()) {
            ERROR("SYS.LOBCOMPPART$ map key not empty on shutdown")
        }

        //SYS.LOBFRAG$
        while (!sysLobFragMapRowId.empty()) {
            auto sysLobFragMapRowIdIt = sysLobFragMapRowId.cbegin();
            SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt->second;
            dictSysLobFragDrop(sysLobFrag);
            delete sysLobFrag;
        }
        if (!sysLobFragMapKey.empty()) {
            ERROR("SYS.LOBFRAG$ map key not empty on shutdown")
        }

        //SYS.OBJ$
        while (!sysObjMapRowId.empty()) {
            auto sysObjMapRowIdIt = sysObjMapRowId.cbegin();
            SysObj* sysObj = sysObjMapRowIdIt->second;
            dictSysObjDrop(sysObj);
            delete sysObj;
        }
        if (!sysObjMapName.empty()) {
            ERROR("SYS.OBJ$ map name not empty on shutdown")
        }
        if (!sysObjMapObj.empty()) {
            ERROR("SYS.OBJ$ map obj# not empty on shutdown")
        }

        //SYS.TAB$
        while (!sysTabMapRowId.empty()) {
            auto sysTabMapRowIdIt = sysTabMapRowId.cbegin();
            SysTab* sysTab = sysTabMapRowIdIt->second;
            dictSysTabDrop(sysTab);
            delete sysTab;
        }
        if (!sysTabMapObj.empty()) {
            ERROR("SYS.TAB$ map obj# not empty on shutdown")
        }

        //SYS.TABCOMPART$
        while (!sysTabComPartMapRowId.empty()) {
            auto sysTabComPartMapRowIdIt = sysTabComPartMapRowId.cbegin();
            SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt->second;
            dictSysTabComPartDrop(sysTabComPart);
            delete sysTabComPart;
        }
        if (!sysTabComPartMapObj.empty()) {
            ERROR("SYS.TABCOMPART$ map obj# not empty on shutdown")
        }
        if (!sysTabComPartMapKey.empty()) {
            ERROR("SYS.TABCOMPART$ map key not empty on shutdown")
        }

        //SYS.TABPART$
        while (!sysTabPartMapRowId.empty()) {
            auto sysTabPartMapRowIdIt = sysTabPartMapRowId.cbegin();
            SysTabPart* sysTabPart = sysTabPartMapRowIdIt->second;
            dictSysTabPartDrop(sysTabPart);
            delete sysTabPart;
        }
        if (!sysTabPartMapKey.empty()) {
            ERROR("SYS.TABPART$ map key not empty on shutdown")
        }

        //SYS.TABSUBPART$
        while(!sysTabSubPartMapRowId.empty()) {
            auto sysTabSubPartMapRowIdIt = sysTabSubPartMapRowId.cbegin();
            SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt->second;
            dictSysTabSubPartDrop(sysTabSubPart);
            delete sysTabSubPart;
        }
        if (!sysTabSubPartMapKey.empty()) {
            ERROR("SYS.TABSUBPART$ map key not empty on shutdown")
        }

        //SYS.TS$
        while (!sysTsMapRowId.empty()) {
            auto sysTsMapRowIdIt = sysTsMapRowId.cbegin();
            SysTs* sysTs = sysTsMapRowIdIt->second;
            dictSysTsDrop(sysTs);
            delete sysTs;
        }
        if (!sysTsMapTs.empty()) {
            ERROR("SYS.TS$ map ts# not empty on shutdown")
        }

        //SYS.USER$
        while (!sysUserMapRowId.empty()) {
            auto sysUserMapRowIdIt = sysUserMapRowId.cbegin();
            SysUser* sysUser = sysUserMapRowIdIt->second;
            dictSysUserDrop(sysUser);
            delete sysUser;
        }
        if (!sysUserMapUser.empty()) {
            ERROR("SYS.USER$ map user# not empty on shutdown")
        }

        resetTouched();
    }

    bool Schema::compareSysCCol(Schema* otherSchema, std::string& msgs) {
        for (auto sysCColMapRowIdIt : sysCColMapRowId) {
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
        for (auto sysCColMapRowIdIt : otherSchema->sysCColMapRowId) {
            SysCCol* sysCCol = sysCColMapRowIdIt.second;
            auto sysCColMapRowIdIt2 = sysCColMapRowId.find(sysCCol->rowId);
            if (sysCColMapRowIdIt2 == sysCColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CCOL$ lost ROWID: " + sysCCol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysCDef(Schema* otherSchema, std::string& msgs) {
        for (auto sysCDefMapRowIdIt : sysCDefMapRowId) {
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
        for (auto sysCDefMapRowIdIt : otherSchema->sysCDefMapRowId) {
            SysCDef* sysCDef = sysCDefMapRowIdIt.second;
            auto sysCDefMapRowIdIt2 = sysCDefMapRowId.find(sysCDef->rowId);
            if (sysCDefMapRowIdIt2 == sysCDefMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CDEF$ lost ROWID: " + sysCDef->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysCol(Schema* otherSchema, std::string& msgs) {
        for (auto sysColMapRowIdIt : sysColMapRowId) {
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
        for (auto sysColMapRowIdIt : otherSchema->sysColMapRowId) {
            SysCol* sysCol = sysColMapRowIdIt.second;
            auto sysColMapRowIdIt2 = sysColMapRowId.find(sysCol->rowId);
            if (sysColMapRowIdIt2 == sysColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.COL$ lost ROWID: " + sysCol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysDeferredStg(Schema* otherSchema, std::string& msgs) {
        for (auto sysDeferredStgMapRowIdIt : sysDeferredStgMapRowId) {
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
        for (auto sysDeferredStgMapRowIdIt : otherSchema->sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = sysDeferredStgMapRowIdIt.second;
            auto sysDeferredStgMapRowIdIt2 = sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
            if (sysDeferredStgMapRowIdIt2 == sysDeferredStgMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ lost ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysECol(Schema* otherSchema, std::string& msgs) {
        for (auto sysEColMapRowIdIt : sysEColMapRowId) {
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
        for (auto sysEColMapRowIdIt : otherSchema->sysEColMapRowId) {
            SysECol* sysECol = sysEColMapRowIdIt.second;
            auto sysEColMapRowIdIt2 = sysEColMapRowId.find(sysECol->rowId);
            if (sysEColMapRowIdIt2 == sysEColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.ECOL$ lost ROWID: " + sysECol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLob(Schema* otherSchema, std::string& msgs) {
        for (auto sysLobMapRowIdIt : sysLobMapRowId) {
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
        for (auto sysLobMapRowIdIt : otherSchema->sysLobMapRowId) {
            SysLob* sysLob = sysLobMapRowIdIt.second;
            auto sysLobMapRowIdIt2 = sysLobMapRowId.find(sysLob->rowId);
            if (sysLobMapRowIdIt2 == sysLobMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOB$ lost ROWID: " + sysLob->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLobCompPart(Schema* otherSchema, std::string& msgs) {
        for (auto sysLobCompPartMapRowIdIt : sysLobCompPartMapRowId) {
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
        for (auto sysLobCompPartMapRowIdIt : otherSchema->sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = sysLobCompPartMapRowIdIt.second;
            auto sysLobCompPartMapRowIdIt2 = sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
            if (sysLobCompPartMapRowIdIt2 == sysLobCompPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ lost ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLobFrag(Schema* otherSchema, std::string& msgs) {
        for (auto sysLobFragMapRowIdIt : sysLobFragMapRowId) {
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
        for (auto sysLobFragMapRowIdIt : otherSchema->sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = sysLobFragMapRowIdIt.second;
            auto sysLobFragMapRowIdIt2 = sysLobFragMapRowId.find(sysLobFrag->rowId);
            if (sysLobFragMapRowIdIt2 == sysLobFragMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ lost ROWID: " + sysLobFrag->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysObj(Schema* otherSchema, std::string& msgs) {
        for (auto sysObjMapRowIdIt : sysObjMapRowId) {
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
        for (auto sysObjMapRowIdIt : otherSchema->sysObjMapRowId) {
            SysObj* sysObj = sysObjMapRowIdIt.second;
            auto sysObjMapRowIdIt2 = sysObjMapRowId.find(sysObj->rowId);
            if (sysObjMapRowIdIt2 == sysObjMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.OBJ$ lost ROWID: " + sysObj->rowId.toString());
                return false;
            }
        }        return true;
    }

    bool Schema::compareSysTab(Schema* otherSchema, std::string& msgs) {
        for (auto sysTabMapRowIdIt : sysTabMapRowId) {
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
        for (auto sysTabMapRowIdIt : otherSchema->sysTabMapRowId) {
            SysTab* sysTab = sysTabMapRowIdIt.second;
            auto sysTabMapRowIdIt2 = sysTabMapRowId.find(sysTab->rowId);
            if (sysTabMapRowIdIt2 == sysTabMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TAB$ lost ROWID: " + sysTab->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabComPart(Schema* otherSchema, std::string& msgs) {
        for (auto sysTabComPartMapRowIdIt : sysTabComPartMapRowId) {
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
        for (auto sysTabComPartMapRowIdIt : otherSchema->sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = sysTabComPartMapRowIdIt.second;
            auto sysTabComPartMapRowIdIt2 = sysTabComPartMapRowId.find(sysTabComPart->rowId);
            if (sysTabComPartMapRowIdIt2 == sysTabComPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ lost ROWID: " + sysTabComPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabPart(Schema* otherSchema, std::string& msgs) {
        for (auto sysTabPartMapRowIdIt : sysTabPartMapRowId) {
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
        for (auto sysTabPartMapRowIdIt : otherSchema->sysTabPartMapRowId) {
            SysTabPart* sysTabPart = sysTabPartMapRowIdIt.second;
            auto sysTabPartMapRowIdIt2 = sysTabPartMapRowId.find(sysTabPart->rowId);
            if (sysTabPartMapRowIdIt2 == sysTabPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABPART$ lost ROWID: " + sysTabPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabSubPart(Schema* otherSchema, std::string& msgs) {
        for (auto sysTabSubPartMapRowIdIt : sysTabSubPartMapRowId) {
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
        for (auto sysTabSubPartMapRowIdIt : otherSchema->sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = sysTabSubPartMapRowIdIt.second;
            auto sysTabSubPartMapRowIdIt2 = sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
            if (sysTabSubPartMapRowIdIt2 == sysTabSubPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ lost ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTs(Schema* otherSchema, std::string& msgs) {
        for (auto sysTsMapRowIdIt : sysTsMapRowId) {
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
        for (auto sysTsMapRowIdIt : otherSchema->sysTsMapRowId) {
            SysTs* sysTs = sysTsMapRowIdIt.second;
            auto sysTsMapRowIdIt2 = sysTsMapRowId.find(sysTs->rowId);
            if (sysTsMapRowIdIt2 == sysTsMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TS$ lost ROWID: " + sysTs->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysUser(Schema* otherSchema, std::string& msgs) {
        for (auto sysUserMapRowIdIt : sysUserMapRowId) {
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
        for (auto sysUserMapRowIdIt : otherSchema->sysUserMapRowId) {
            SysUser* sysUser = sysUserMapRowIdIt.second;
            auto sysUserMapRowIdIt2 = sysUserMapRowId.find(sysUser->rowId);
            if (sysUserMapRowIdIt2 == sysUserMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.USER$ lost ROWID: " + sysUser->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compare(Schema* otherSchema, std::string& msgs) {
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

        msgs.assign("");
        return true;
    }

    void Schema::dictSysCColAdd(const char* rowIdStr, typeCon con, typeCol intCol, typeObj obj, uint64_t spare11, uint64_t spare12) {
        typeRowId rowId(rowIdStr);
        if (sysCColMapRowId.find(rowId) != sysCColMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.CCOL$: (rowid: " << rowId << ")")
            return;
        }

        auto sysCCol = new SysCCol(rowId, con, intCol, obj, spare11, spare12);
        dictSysCColAdd(sysCCol);
    }

    void Schema::dictSysCDefAdd(const char* rowIdStr, typeCon con, typeObj obj, typeType type) {
        typeRowId rowId(rowIdStr);
        if (sysCDefMapRowId.find(rowId) != sysCDefMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.CDEF$: (rowid: " << rowId << ")")
            return;
        }

        auto sysCDef = new SysCDef(rowId, con, obj, type);
        dictSysCDefAdd(sysCDef);
    }

    void Schema::dictSysColAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type,
                               uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, bool null_,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysColMapRowId.find(rowId) != sysColMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.COL$: (rowid: " << rowId << ")")
            return;
        }

        if (strlen(name) > SYS_COL_NAME_LENGTH)
            throw DataException("SYS.COL$ too long value for NAME (value: '" + std::string(name) + "', length: " +
                    std::to_string(strlen(name)) + ")");
        auto sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length,
                                 precision, scale, charsetForm, charsetId, null_, property1,
                                 property2);
        dictSysColAdd(sysCol);
    }

    void Schema::dictSysDeferredStgAdd(const char* rowIdStr, typeObj obj, uint64_t flagsStg1, uint64_t flagsStg2) {
        typeRowId rowId(rowIdStr);
        if (sysDeferredStgMapRowId.find(rowId) != sysDeferredStgMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.DEFERRED_STG$: (rowid: " << rowId << ")")
            return;
        }

        auto sysDeferredStg = new SysDeferredStg(rowId, obj, flagsStg1, flagsStg2);
        dictSysDeferredStgAdd(sysDeferredStg);
    }

    void Schema::dictSysEColAdd(const char* rowIdStr, typeObj tabObj, typeCol colNum, typeCol guardId) {
        typeRowId rowId(rowIdStr);
        if (sysEColMapRowId.find(rowId) != sysEColMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.ECOL$: (rowid: " << rowId << ")")
            return;
        }

        auto sysECol = new SysECol(rowId, tabObj, colNum, guardId);
        dictSysEColAdd(sysECol);
    }

    void Schema::dictSysLobAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol intCol, typeObj lObj, typeTs ts) {
        typeRowId rowId(rowIdStr);
        if (sysLobMapRowId.find(rowId) != sysLobMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.LOB$: (rowid: " << rowId << ")")
            return;
        }

        auto sysLob = new SysLob(rowId, obj, col, intCol, lObj, ts);
        dictSysLobAdd(sysLob);
    }

    void Schema::dictSysLobCompPartAdd(const char* rowIdStr, typeObj partObj, typeObj lObj) {
        typeRowId rowId(rowIdStr);
        if (sysLobCompPartMapRowId.find(rowId) != sysLobCompPartMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.LOBCOMPPART$: (rowid: " << rowId << ")")
            return;
        }

        auto sysLobCompPart = new SysLobCompPart(rowId, partObj, lObj);
        dictSysLobCompPartAdd(sysLobCompPart);
    }

    void Schema::dictSysLobFragAdd(const char* rowIdStr, typeObj fragObj, typeObj parentObj, typeTs ts) {
        typeRowId rowId(rowIdStr);
        if (sysLobFragMapRowId.find(rowId) != sysLobFragMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.LOBFRAG$: (rowid: " << rowId << ")")
            return;
        }

        auto sysLobFrag = new SysLobFrag(rowId, fragObj, parentObj, ts);
        dictSysLobFragAdd(sysLobFrag);
    }

    bool Schema::dictSysObjAdd(const char* rowIdStr, typeUser owner, typeObj obj, typeDataObj dataObj, typeType type, const char* name,
                               uint64_t flags1, uint64_t flags2, bool single) {
        typeRowId rowId(rowIdStr);

        auto sysObjMapRowIdIt = sysObjMapRowId.find(rowId);
        if (sysObjMapRowIdIt != sysObjMapRowId.end()) {
            SysObj* sysObj = sysObjMapRowIdIt->second;
            if (!single) {
                if (sysObj->single) {
                    sysObj->single = false;
                    TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for object " << name << " (owner " << std::dec << owner << ")")
                } else {
                    ERROR("SYSTEM: duplicate SYS.OBJ$: (rowid: " << rowId << ")")
                }
            }
            return false;
        }

        if (strlen(name) > SYS_OBJ_NAME_LENGTH)
            throw DataException("SYS.OBJ$ too long value for NAME (value: '" + std::string(name) + "', length: " +
                    std::to_string(strlen(name)) + ")");
        auto sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2, single);
        dictSysObjAdd(sysObj);

        return true;
    }

    void Schema::dictSysTabAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeCol cluCols, uint64_t flags1, uint64_t flags2,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.TAB$: (rowid: " << rowId << ")")
            return;
        }

        auto sysTab = new SysTab(rowId, obj, dataObj, cluCols, flags1, flags2, property1,
                                 property2);
        dictSysTabAdd(sysTab);
    }

    void Schema::dictSysTabComPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo) {
        typeRowId rowId(rowIdStr);
        if (sysTabComPartMapRowId.find(rowId) != sysTabComPartMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.TABCOMPART$: (rowid: " << rowId << ")")
            return;
        }

        auto sysTabComPart = new SysTabComPart(rowId, obj, dataObj, bo);
        dictSysTabComPartAdd(sysTabComPart);
    }

    void Schema::dictSysTabPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo) {
        typeRowId rowId(rowIdStr);
        if (sysTabPartMapRowId.find(rowId) != sysTabPartMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.TABPART$: (rowid: " << rowId << ")")
            return;
        }

        auto sysTabPart = new SysTabPart(rowId, obj, dataObj, bo);
        dictSysTabPartAdd(sysTabPart);
    }

    void Schema::dictSysTabSubPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj pObj) {
        typeRowId rowId(rowIdStr);
        if (sysTabSubPartMapRowId.find(rowId) != sysTabSubPartMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.TABSUBPART$: (rowid: " << rowId << ")")
            return;
        }

        auto sysTabSubPart = new SysTabSubPart(rowId, obj, dataObj, pObj);
        dictSysTabSubPartAdd(sysTabSubPart);
    }

    void Schema::dictSysTsAdd(const char* rowIdStr, typeTs ts, const char* name, uint32_t blockSize) {
        typeRowId rowId(rowIdStr);
        if (sysTsMapRowId.find(rowId) != sysTsMapRowId.end()) {
            ERROR("SYSTEM: duplicate SYS.TS$: (rowid: " << rowId << ")")
            return;
        }

        auto sysTs = new SysTs(rowId, ts, name, blockSize);
        dictSysTsAdd(sysTs);
    }

    bool Schema::dictSysUserAdd(const char* rowIdStr, typeUser user, const char* name, uint64_t spare11, uint64_t spare12, bool single, bool showError) {
        typeRowId rowId(rowIdStr);

        auto sysUserMapRowIdIt = sysUserMapRowId.find(rowId);
        if (sysUserMapRowIdIt != sysUserMapRowId.end()) {
            SysUser* sysUser = sysUserMapRowIdIt->second;
            if (sysUser->single) {
                if (!single) {
                    sysUser->single = false;
                    TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for user " << name << " (" << std::dec << user << ")")
                } else if (!showError) {
                    ERROR("SYSTEM: duplicate SYS.USER$: (rowid: " << rowId << ")")
                }
                return true;
            }

            return false;
        }

        if (strlen(name) > SYS_USER_NAME_LENGTH)
            throw DataException("SYS.USER$ too long value for NAME (value: '" + std::string(name) + "', length: " +
                    std::to_string(strlen(name)) + ")");
        auto sysUser = new SysUser(rowId, user, name, spare11, spare12, single);
        dictSysUserAdd(sysUser);

        return true;
    }

    void Schema::dictSysCColAdd(SysCCol* sysCCol) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: INSERT SYS.CCOL$ (ROWID: " << sysCCol->rowId <<
                ", CON#: " << std::dec << sysCCol->con <<
                ", INTCOL#: " << sysCCol->intCol <<
                ", OBJ#: " << sysCCol->obj <<
                ", SPARE1: " << sysCCol->spare1 << ")")
        sysCColMapRowId[sysCCol->rowId] = sysCCol;

        SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
        auto sysCColMapKeyIt = sysCColMapKey.find(sysCColKey);
        if (sysCColMapKeyIt == sysCColMapKey.end())
            sysCColMapKey[sysCColKey] = sysCCol;
        else {
            ERROR("SYS.CCOL$ duplicate value for unique (OBJ#: " + std::to_string(sysCCol->obj) + ", INTCOL#: " + std::to_string(sysCCol->intCol) +
                    ", CON#: " + std::to_string(sysCCol->con) + ")")
        }

        sysCColSetTouched.insert(sysCCol);
        touchTable(sysCCol->obj);
        touched = true;
    }

    void Schema::dictSysCDefAdd(SysCDef* sysCDef) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.CDEF$ (ROWID: " << sysCDef->rowId <<
                ", CON#: " << std::dec << sysCDef->con <<
                ", OBJ#: " << sysCDef->obj <<
                ", TYPE: " << sysCDef->type << ")")
        sysCDefMapRowId[sysCDef->rowId] = sysCDef;

        SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
        auto sysCDefMapKeyIt = sysCDefMapKey.find(sysCDefKey);
        if (sysCDefMapKeyIt == sysCDefMapKey.end())
            sysCDefMapKey[sysCDefKey] = sysCDef;
        else {
            ERROR("SYS.CDEF$ duplicate value for unique (OBJ#: " + std::to_string(sysCDef->obj) + ", CON#: " + std::to_string(sysCDef->con) + ")")
        }

        auto sysCDefMapConIt = sysCDefMapCon.find(sysCDef->con);
        if (sysCDefMapConIt == sysCDefMapCon.end())
            sysCDefMapCon[sysCDef->con] = sysCDef;
        else {
            ERROR("SYS.CDEF$ duplicate value for unique (CON#: " + std::to_string(sysCDef->con) + ")")
        }

        sysCDefSetTouched.insert(sysCDef);
        touchTable(sysCDef->obj);
        touched = true;
    }

    void Schema::dictSysColAdd(SysCol* sysCol) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.COL$ (ROWID: " << sysCol->rowId <<
                ", OBJ#: " << std::dec << sysCol->obj <<
                ", COL#: " << sysCol->col <<
                ", SEGCOL#: " << sysCol->segCol <<
                ", INTCOL#: " << sysCol->intCol <<
                ", NAME: '" << sysCol->name <<
                "', TYPE#: " << sysCol->type <<
                ", LENGTH: " << sysCol->length <<
                ", PRECISION#: " << sysCol->precision <<
                ", SCALE: " << sysCol->scale <<
                ", CHARSETFORM: " << sysCol->charsetForm <<
                ", CHARSETID: " << sysCol->charsetId <<
                ", NULL$: " << sysCol->null_ <<
                ", PROPERTY: " << sysCol->property << ")")
        sysColMapRowId[sysCol->rowId] = sysCol;

        if (sysCol->segCol > 0) {
            SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
            auto sysColMapSegIt = sysColMapSeg.find(sysColSeg);
            if (sysColMapSegIt == sysColMapSeg.end())
                sysColMapSeg[sysColSeg] = sysCol;
            else {
                ERROR("SYS.COL$ duplicate value for unique (OBJ#: " + std::to_string(sysCol->obj) + ", SEGCOL#: " + std::to_string(sysCol->segCol) +
                        ")")
            }
        }

        sysColSetTouched.insert(sysCol);
        touchTable(sysCol->obj);
        touched = true;
    }

    void Schema::dictSysDeferredStgAdd(SysDeferredStg* sysDeferredStg) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.DEFERRED_STG$ (ROWID: " << sysDeferredStg->rowId <<
                ", OBJ#: " << std::dec << sysDeferredStg->obj <<
                ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")")
        sysDeferredStgMapRowId[sysDeferredStg->rowId] = sysDeferredStg;

        auto sysDeferredStgMapObjIt = sysDeferredStgMapObj.find(sysDeferredStg->obj);
        if (sysDeferredStgMapObjIt == sysDeferredStgMapObj.end())
            sysDeferredStgMapObj[sysDeferredStg->obj] = sysDeferredStg;
        else {
            ERROR("SYS.DEFERRED_STG$ duplicate value for unique (OBJ#: " + std::to_string(sysDeferredStg->obj) + ")")
        }

        sysDeferredStgSetTouched.insert(sysDeferredStg);
        touchTable(sysDeferredStg->obj);
        touched = true;
    }

    void Schema::dictSysEColAdd(SysECol* sysECol) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.ECOL$ (ROWID: " << sysECol->rowId <<
                ", TABOBJ#: " << std::dec << sysECol->tabObj <<
                ", COLNUM: " << sysECol->colNum <<
                ", GUARD_ID: " << sysECol->guardId << ")")
        sysEColMapRowId[sysECol->rowId] = sysECol;

        SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
        auto sysEColMapKeyIt = sysEColMapKey.find(sysEColKey);
        if (sysEColMapKeyIt == sysEColMapKey.end())
            sysEColMapKey[sysEColKey] = sysECol;
        else {
            ERROR("SYS.ECOL$ duplicate value for unique (TABOBJ#: " + std::to_string(sysECol->tabObj) + ", COLNUM: " +
                    std::to_string(sysECol->colNum) + ")")
        }

        sysEColSetTouched.insert(sysECol);
        touchTable(sysECol->tabObj);
        touched = true;
    }

    void Schema::dictSysLobAdd(SysLob* sysLob) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.LOB$ (ROWID: " << sysLob->rowId <<
                ", OBJ#: " << std::dec << sysLob->obj <<
                ", COL#: " << sysLob->col <<
                ", INTCOL#: " << sysLob->intCol <<
                ", LOBJ#: " << sysLob->lObj <<
                ", TS#: " << sysLob->ts << ")")
        sysLobMapRowId[sysLob->rowId] = sysLob;

        SysLobKey sysLobKey(sysLob->obj, sysLob->intCol);
        auto sysLobMapKeyIt = sysLobMapKey.find(sysLobKey);
        if (sysLobMapKeyIt == sysLobMapKey.end())
            sysLobMapKey[sysLobKey] = sysLob;
        else {
            ERROR("SYS.LOB$ duplicate value for unique (OBJ#: " + std::to_string(sysLob->obj) + ", INTCOL#: " + std::to_string(sysLob->intCol) + ")")
        }

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLob->lObj);
        if (sysLobMapLObjIt == sysLobMapLObj.end())
            sysLobMapLObj[sysLob->lObj] = sysLob;
        else {
            ERROR("SYS.LOB$ duplicate value for unique (LOBJ#: " + std::to_string(sysLob->lObj) + ")")
        }

        sysLobSetTouched.insert(sysLob);
        touchTable(sysLob->obj);
        touched = true;
    }

    void Schema::dictSysLobCompPartAdd(SysLobCompPart* sysLobCompPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.LOBCOMPPART$ (ROWID: " << sysLobCompPart->rowId <<
                ", PARTOBJ#: " << std::dec << sysLobCompPart->partObj <<
                ", LOBJ#: " << sysLobCompPart->lObj << ")")
        sysLobCompPartMapRowId[sysLobCompPart->rowId] = sysLobCompPart;

        SysLobCompPartKey sysLobCompPartKey(sysLobCompPart->lObj, sysLobCompPart->partObj);
        auto sysLobCompPartMapKeyIt = sysLobCompPartMapKey.find(sysLobCompPartKey);
        if (sysLobCompPartMapKeyIt == sysLobCompPartMapKey.end())
            sysLobCompPartMapKey[sysLobCompPartKey] = sysLobCompPart;
        else {
            ERROR("SYS.LOB$ duplicate value for unique (LOBJ#: " + std::to_string(sysLobCompPart->lObj) + ", PARTOBJ#: " +
                    std::to_string(sysLobCompPart->partObj) + ")")
        }

        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobCompPart->partObj);
        if (sysLobCompPartMapPartObjIt == sysLobCompPartMapPartObj.end())
            sysLobCompPartMapPartObj[sysLobCompPart->partObj] = sysLobCompPart;
        else {
            ERROR("SYS.LOBCOMPPART$ duplicate value for unique (PARTOBJ#: " + std::to_string(sysLobCompPart->partObj) + ")")
        }

        sysLobCompPartSetTouched.insert(sysLobCompPart);
        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPart->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            touchTable(sysLobMapLObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysLobFragAdd(SysLobFrag* sysLobFrag) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.LOBFRAG$ (ROWID: " << sysLobFrag->rowId <<
                ", FRAGOBJ#: " << std::dec << sysLobFrag->fragObj <<
                ", PARENTOBJ#: " << sysLobFrag->parentObj <<
                ", TS#: " << sysLobFrag->ts << ")")
        sysLobFragMapRowId[sysLobFrag->rowId] = sysLobFrag;

        SysLobFragKey sysLobFragKey(sysLobFrag->parentObj, sysLobFrag->fragObj);
        auto sysLobFragMapKeyIt = sysLobFragMapKey.find(sysLobFragKey);
        if (sysLobFragMapKeyIt == sysLobFragMapKey.end())
            sysLobFragMapKey[sysLobFragKey] = sysLobFrag;
        else {
            ERROR("SYS.LOBFRAG$ duplicate value for unique (PARENTOBJ#: " + std::to_string(sysLobFrag->parentObj) + ", PARTOBJ#: " +
                  std::to_string(sysLobFrag->parentObj) + ")")
        }

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
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.OBJ$ (ROWID: " << sysObj->rowId <<
                ", OWNER#: " << std::dec << sysObj->owner <<
                ", OBJ#: " << sysObj->obj <<
                ", DATAOBJ#: " << sysObj->dataObj <<
                ", TYPE#: " << sysObj->type <<
                ", NAME: '" << sysObj->name <<
                "', FLAGS: " << sysObj->flags << ")")
        sysObjMapRowId[sysObj->rowId] = sysObj;

        SysObjNameKey sysObjNameKey(sysObj->owner, sysObj->name.c_str(), sysObj->obj, sysObj->dataObj);
        auto sysObjMapNameIt = sysObjMapName.find(sysObjNameKey);
        if (sysObjMapNameIt == sysObjMapName.end())
            sysObjMapName[sysObjNameKey] = sysObj;
        else {
            ERROR("SYS.OBJ$ duplicate value for unique (OWNER#: " + std::to_string(sysObj->owner) + ", NAME: '" + sysObj->name +
                    "', OBJ#: " + std::to_string(sysObj->obj) + ", DATAOBJ#: " + std::to_string(sysObj->dataObj) + ")")
        }

        auto sysObjMapObjIt = sysObjMapObj.find(sysObj->obj);
        if (sysObjMapObjIt == sysObjMapObj.end())
            sysObjMapObj[sysObj->obj] = sysObj;
        else {
            ERROR("SYS.OBJ$ duplicate value for unique (OBJ#: " + std::to_string(sysObj->obj) + ")")
        }

        sysObjSetTouched.insert(sysObj);
        touchTable(sysObj->obj);
        touched = true;
    }

    void Schema::dictSysTabAdd(SysTab* sysTab) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.TAB$ (ROWID: " << sysTab->rowId <<
                ", OBJ#: " << std::dec << sysTab->obj <<
                ", DATAOBJ#: " << sysTab->dataObj <<
                ", CLUCOLS: " << sysTab->cluCols <<
                ", FLAGS: " << sysTab->flags <<
                ", PROPERTY: " << sysTab->property << ")")
        sysTabMapRowId[sysTab->rowId] = sysTab;

        auto sysTabMapObjIt = sysTabMapObj.find(sysTab->obj);
        if (sysTabMapObjIt == sysTabMapObj.end())
            sysTabMapObj[sysTab->obj] = sysTab;
        else {
            ERROR("SYS.TAB$ duplicate value for unique (OBJ#: " + std::to_string(sysTab->obj) + ")")
        }

        sysTabSetTouched.insert(sysTab);
        touchTable(sysTab->obj);
        touched = true;
    }

    void Schema::dictSysTabComPartAdd(SysTabComPart* sysTabComPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.TABCOMPART$ (ROWID: " << sysTabComPart->rowId <<
                ", OBJ#: " << std::dec << sysTabComPart->obj <<
                ", DATAOBJ#: " << sysTabComPart->dataObj <<
                ", BO#: " << sysTabComPart->bo << ")")
        sysTabComPartMapRowId[sysTabComPart->rowId] = sysTabComPart;

        SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
        auto sysTabComPartMapKeyIt = sysTabComPartMapKey.find(sysTabComPartKey);
        if (sysTabComPartMapKeyIt == sysTabComPartMapKey.end())
            sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
        else {
            ERROR("SYS.TABCOMPART$ duplicate value for unique (BO#: " + std::to_string(sysTabComPart->bo) + ", OBJ#: " +
                    std::to_string(sysTabComPart->obj) + ")")
        }

        auto sysTabComPartMapObjIt = sysTabComPartMapObj.find(sysTabComPart->obj);
        if (sysTabComPartMapObjIt == sysTabComPartMapObj.end())
            sysTabComPartMapObj[sysTabComPart->obj] = sysTabComPart;
        else {
            ERROR("SYS.TABCOMPART$ duplicate value for unique (OBJ#: " + std::to_string(sysTabComPart->obj) + ")")
        }

        sysTabComPartSetTouched.insert(sysTabComPart);
        touchTable(sysTabComPart->bo);
        touched = true;
    }

    void Schema::dictSysTabPartAdd(SysTabPart* sysTabPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.TABPART$ (ROWID: " << sysTabPart->rowId <<
                ", OBJ#: " << std::dec << sysTabPart->obj <<
                ", DATAOBJ#: " << sysTabPart->dataObj <<
                ", BO#: " << sysTabPart->bo << ")")
        sysTabPartMapRowId[sysTabPart->rowId] = sysTabPart;

        SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
        auto sysTabPartMapKeyIt = sysTabPartMapKey.find(sysTabPartKey);
        if (sysTabPartMapKeyIt == sysTabPartMapKey.end())
            sysTabPartMapKey[sysTabPartKey] = sysTabPart;
        else {
            ERROR("SYS.TABPART$ duplicate value for unique (BO#: " + std::to_string(sysTabPart->bo) + ", OBJ#: " +
                    std::to_string(sysTabPart->obj) + ")")
        }

        sysTabPartSetTouched.insert(sysTabPart);
        touchTable(sysTabPart->bo);
        touched = true;
    }

    void Schema::dictSysTabSubPartAdd(SysTabSubPart* sysTabSubPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.TABSUBPART$ (ROWID: " << sysTabSubPart->rowId <<
                ", OBJ#: " << std::dec << sysTabSubPart->obj <<
                ", DATAOBJ#: " << sysTabSubPart->dataObj <<
                ", POBJ#: " << sysTabSubPart->pObj << ")")
        sysTabSubPartMapRowId[sysTabSubPart->rowId] = sysTabSubPart;

        SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
        auto sysTabSubPartMapKeyIt = sysTabSubPartMapKey.find(sysTabSubPartKey);
        if (sysTabSubPartMapKeyIt == sysTabSubPartMapKey.end())
            sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;
        else {
            ERROR("SYS.TABSUBPART$ duplicate value for unique (POBJ#: " + std::to_string(sysTabSubPart->pObj) + ", OBJ#: " +
                    std::to_string(sysTabSubPart->obj) + ")")
        }

        sysTabSubPartSetTouched.insert(sysTabSubPart);
        auto sysObjMapObjIt = sysObjMapObj.find(sysTabSubPart->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            touchTable(sysObjMapObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysTsAdd(SysTs* sysTs) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.TS$ (ROWID: " << sysTs->rowId <<
                ", TS#: " << std::dec << sysTs->ts <<
                ", NAME: '" << sysTs->name <<
                "', BLOCKSIZE: " << sysTs->blockSize << ")")
        sysTsMapRowId[sysTs->rowId] = sysTs;

        auto sysTsMapTsIt = sysTsMapTs.find(sysTs->ts);
        if (sysTsMapTsIt == sysTsMapTs.end())
            sysTsMapTs[sysTs->ts] = sysTs;
        else {
            ERROR("SYS.TS$ duplicate value for unique (TS#: " + std::to_string(sysTs->ts) + ")")
        }

        touched = true;
    }

    void Schema::dictSysUserAdd(SysUser* sysUser) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: insert SYS.USER$ (ROWID: " << sysUser->rowId <<
                ", USER#: " << std::dec << sysUser->user <<
                ", NAME: " << sysUser->name <<
                ", SPARE1: " << sysUser->spare1 << ")")
        sysUserMapRowId[sysUser->rowId] = sysUser;

        auto sysUserMapUserIt = sysUserMapUser.find(sysUser->user);
        if (sysUserMapUserIt == sysUserMapUser.end())
            sysUserMapUser[sysUser->user] = sysUser;
        else {
            ERROR("SYS.USER$ duplicate value for unique (USER#: " + std::to_string(sysUser->user) + ")")
        }

        sysUserSetTouched.insert(sysUser);
        touched = true;
    }

    void Schema::dictSysCColDrop(SysCCol* sysCCol) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.CCOL$ (ROWID: " << sysCCol->rowId <<
                ", CON#: " << std::dec << sysCCol->con <<
                ", INTCOL#: " << sysCCol->intCol <<
                ", OBJ#: " << sysCCol->obj <<
                ", SPARE1: " << sysCCol->spare1 << ")")
        auto sysCColMapRowIdIt = sysCColMapRowId.find(sysCCol->rowId);
        if (sysCColMapRowIdIt == sysCColMapRowId.end())
            return;
        sysCColMapRowId.erase(sysCColMapRowIdIt);

        SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
        auto sysCColMapKeyIt = sysCColMapKey.find(sysCColKey);
        if (sysCColMapKeyIt != sysCColMapKey.end())
            sysCColMapKey.erase(sysCColMapKeyIt);
        else {
            ERROR("SYS.CCOL$ missing index for (OBJ#: " + std::to_string(sysCCol->obj) + ", INTCOL#:" + std::to_string(sysCCol->intCol) +
                    ", CON#: " + std::to_string(sysCCol->con) + ")")
        }

        touchTable(sysCCol->obj);
        touched = true;
    }

    void Schema::dictSysCDefDrop(SysCDef* sysCDef) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.CDEF$ (ROWID: " << sysCDef->rowId <<
                ", CON#: " << std::dec << sysCDef->con <<
                ", OBJ#: " << sysCDef->obj <<
                ", TYPE: " << sysCDef->type << ")")
        auto sysCDefMapRowIdIt = sysCDefMapRowId.find(sysCDef->rowId);
        if (sysCDefMapRowIdIt == sysCDefMapRowId.end())
            return;
        sysCDefMapRowId.erase(sysCDefMapRowIdIt);

        SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
        auto sysCDefMapKeyIt = sysCDefMapKey.find(sysCDefKey);
        if (sysCDefMapKeyIt != sysCDefMapKey.end())
            sysCDefMapKey.erase(sysCDefMapKeyIt);
        else {
            ERROR("SYS.CDEF$ missing index for (OBJ#: " + std::to_string(sysCDef->obj) + ", CON#:" + std::to_string(sysCDef->con) + ")")
        }

        auto sysCDefMapConIt = sysCDefMapCon.find(sysCDef->con);
        if (sysCDefMapConIt != sysCDefMapCon.end())
            sysCDefMapCon.erase(sysCDefMapConIt);
        else {
            ERROR("SYS.CDEF$ missing index for (CON#: " + std::to_string(sysCDef->con) + ")")
        }

        touchTable(sysCDef->obj);
        touched = true;
    }

    void Schema::dictSysColDrop(SysCol* sysCol) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.COL$ (ROWID: " << sysCol->rowId <<
                ", OBJ#: " << std::dec << sysCol->obj <<
                ", COL#: " << sysCol->col <<
                ", SEGCOL#: " << sysCol->segCol <<
                ", INTCOL#: " << sysCol->intCol <<
                ", NAME: '" << sysCol->name <<
                "', TYPE#: " << sysCol->type <<
                ", LENGTH: " << sysCol->length <<
                ", PRECISION#: " << sysCol->precision <<
                ", SCALE: " << sysCol->scale <<
                ", CHARSETFORM: " << sysCol->charsetForm <<
                ", CHARSETID: " << sysCol->charsetId <<
                ", NULL$: " << sysCol->null_ <<
                ", PROPERTY: " << sysCol->property << ")")
        auto sysColMapRowIdIt = sysColMapRowId.find(sysCol->rowId);
        if (sysColMapRowIdIt == sysColMapRowId.end())
            return;
        sysColMapRowId.erase(sysColMapRowIdIt);

        if (sysCol->segCol > 0) {
            SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
            auto sysColMapSegIt = sysColMapSeg.find(sysColSeg);
            if (sysColMapSegIt != sysColMapSeg.end())
                sysColMapSeg.erase(sysColMapSegIt);
            else {
                ERROR("SYS.COL$ missing index for (OBJ#: " + std::to_string(sysCol->obj) + ", SEGCOL#: " + std::to_string(sysCol->segCol) + ")")
            }
        }

        touchTable(sysCol->obj);
        touched = true;
    }

    void Schema::dictSysDeferredStgDrop(SysDeferredStg* sysDeferredStg) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.DEFERRED_STG$ (ROWID: " << sysDeferredStg->rowId <<
                ", OBJ#: " << std::dec << sysDeferredStg->obj <<
                ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")")
        auto sysDeferredStgMapRowIdIt = sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
        if (sysDeferredStgMapRowIdIt == sysDeferredStgMapRowId.end())
            return;
        sysDeferredStgMapRowId.erase(sysDeferredStgMapRowIdIt);

        auto sysDeferredStgMapObjIt = sysDeferredStgMapObj.find(sysDeferredStg->obj);
        if (sysDeferredStgMapObjIt != sysDeferredStgMapObj.end())
            sysDeferredStgMapObj.erase(sysDeferredStgMapObjIt);
        else {
            ERROR("SYS.DEFERRED_STG$ missing index for (OBJ#: " + std::to_string(sysDeferredStg->obj) + ")")
        }

        touchTable(sysDeferredStg->obj);
        touched = true;
    }

    void Schema::dictSysEColDrop(SysECol* sysECol) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.ECOL$ (ROWID: " << sysECol->rowId <<
                ", TABOBJ#: " << std::dec << sysECol->tabObj <<
                ", COLNUM: " << sysECol->colNum <<
                ", GUARD_ID: " << sysECol->guardId << ")")
        auto sysEColMapRowIdIt = sysEColMapRowId.find(sysECol->rowId);
        if (sysEColMapRowIdIt == sysEColMapRowId.end())
            return;
        sysEColMapRowId.erase(sysEColMapRowIdIt);

        SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
        auto sysEColMapKeyIt = sysEColMapKey.find(sysEColKey);
        if (sysEColMapKeyIt != sysEColMapKey.end())
            sysEColMapKey.erase(sysEColMapKeyIt);
        else {
            ERROR("SYS.ECOL$ missing index for (TABOBJ#: " + std::to_string(sysECol->tabObj) + ", COLNUM#: " + std::to_string(sysECol->colNum) + ")")
        }

        touchTable(sysECol->tabObj);
        touched = true;
    }

    void Schema::dictSysLobDrop(SysLob* sysLob) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.LOB$ (ROWID: " << sysLob->rowId <<
                ", OBJ#: " << std::dec << sysLob->obj <<
                ", COL#: " << sysLob->col <<
                ", INTCOL#: " << sysLob->intCol <<
                ", LOBJ#: " << sysLob->lObj <<
                ", TS#: " << sysLob->ts << ")")
        auto sysLobMapRowIdIt = sysLobMapRowId.find(sysLob->rowId);
        if (sysLobMapRowIdIt == sysLobMapRowId.end())
            return;
        sysLobMapRowId.erase(sysLobMapRowIdIt);

        SysLobKey sysLobKey(sysLob->obj, sysLob->intCol);
        auto sysLobMapKeyIt = sysLobMapKey.find(sysLobKey);
        if (sysLobMapKeyIt != sysLobMapKey.end())
            sysLobMapKey.erase(sysLobMapKeyIt);
        else {
            ERROR("SYS.LOB$ missing index for (OBJ#: " + std::to_string(sysLob->obj) + ", INTCOL#: " + std::to_string(sysLob->intCol) + ")")
        }

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLob->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            sysLobMapLObj.erase(sysLobMapLObjIt);
        else {
            ERROR("SYS.LOB$ missing index for (LOBJ#: " + std::to_string(sysLob->lObj) + ")")
        }

        touchTable(sysLob->obj);
        touched = true;
    }

    void Schema::dictSysLobCompPartDrop(SysLobCompPart* sysLobCompPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.LOBCOMPPART$ (ROWID: " << sysLobCompPart->rowId <<
                ", PARTOBJ#: " << std::dec << sysLobCompPart->partObj <<
                ", LOBJ#: " << sysLobCompPart->lObj << ")")
        auto sysLobCompPartMapRowIdIt = sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
        if (sysLobCompPartMapRowIdIt == sysLobCompPartMapRowId.end())
            return;
        sysLobCompPartMapRowId.erase(sysLobCompPartMapRowIdIt);

        SysLobCompPartKey sysLobCompPartKey(sysLobCompPart->lObj, sysLobCompPart->partObj);
        auto sysLobCompPartMapKeyIt = sysLobCompPartMapKey.find(sysLobCompPartKey);
        if (sysLobCompPartMapKeyIt != sysLobCompPartMapKey.end())
            sysLobCompPartMapKey.erase(sysLobCompPartMapKeyIt);
        else {
            ERROR("SYS.LOBCOMPPART$ missing index for (LOBJ#: " + std::to_string(sysLobCompPart->lObj) + ", PARTOBJ#: " +
                    std::to_string(sysLobCompPart->partObj) + ")")
        }

        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobCompPart->partObj);
        if (sysLobCompPartMapPartObjIt != sysLobCompPartMapPartObj.end())
            sysLobCompPartMapPartObj.erase(sysLobCompPartMapPartObjIt);
        else {
            ERROR("SYS.LOBCOMPPART$ missing index for (PARTOBJ#: " + std::to_string(sysLobCompPart->partObj) + ")")
        }

        auto sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPart->lObj);
        if (sysLobMapLObjIt != sysLobMapLObj.end())
            touchTable(sysLobMapLObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysLobFragDrop(SysLobFrag* sysLobFrag) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.LOBFRAG$ (ROWID: " << sysLobFrag->rowId <<
                ", FRAGOBJ#: " << std::dec << sysLobFrag->fragObj <<
                ", PARENTOBJ#: " << sysLobFrag->parentObj <<
                ", TS#: " << sysLobFrag->ts << ")")
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
        else {
            ERROR("SYS.LOBFRAG$ missing index for (PARENTOBJ#: " + std::to_string(sysLobFrag->parentObj) + ", FRAGOBJ#: " +
                    std::to_string(sysLobFrag->fragObj) + ")")
        }

        auto sysLobCompPartMapPartObjIt = sysLobCompPartMapPartObj.find(sysLobFrag->parentObj);
        if (sysLobCompPartMapPartObjIt != sysLobCompPartMapPartObj.end()) {
            sysLobMapLObjIt = sysLobMapLObj.find(sysLobCompPartMapPartObjIt->second->lObj);
            if (sysLobMapLObjIt != sysLobMapLObj.end())
                touchTable(sysLobMapLObjIt->second->obj);
        }
        touched = true;
    }

    void Schema::dictSysObjDrop(SysObj* sysObj) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.OBJ$ (ROWID: " << sysObj->rowId <<
                ", OWNER#: " << std::dec << sysObj->owner <<
                ", OBJ#: " << sysObj->obj <<
                ", DATAOBJ#: " << sysObj->dataObj <<
                ", TYPE#: " << sysObj->type <<
                ", NAME: '" << sysObj->name <<
                "', FLAGS: " << sysObj->flags << ")")
        auto sysObjMapRowIdIt = sysObjMapRowId.find(sysObj->rowId);
        if (sysObjMapRowIdIt == sysObjMapRowId.end())
            return;
        sysObjMapRowId.erase(sysObjMapRowIdIt);

        SysObjNameKey sysObjNameKey(sysObj->owner, sysObj->name.c_str(), sysObj->obj, sysObj->dataObj);
        auto sysObjMapNameIt = sysObjMapName.find(sysObjNameKey);
        if (sysObjMapNameIt != sysObjMapName.end())
            sysObjMapName.erase(sysObjMapNameIt);
        else {
            ERROR("SYS.OBJ$ missing index for (OWNER#: " + std::to_string(sysObj->owner) + ", NAME: '" + sysObj->name +
                    "', OBJ#: " + std::to_string(sysObj->obj) + ", DATAOBJ#: " + std::to_string(sysObj->dataObj) + ")")
        }

        auto sysObjMapObjIt = sysObjMapObj.find(sysObj->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            sysObjMapObj.erase(sysObjMapObjIt);
        else {
            ERROR("SYS.OBJ$ missing index for (OBJ#: " + std::to_string(sysObj->obj) + ")")
        }

        touchTable(sysObj->obj);
        touched = true;
    }

    void Schema::dictSysTabDrop(SysTab* sysTab) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.TAB$ (ROWID: " << sysTab->rowId <<
                ", OBJ#: " << std::dec << sysTab->obj <<
                ", DATAOBJ#: " << sysTab->dataObj <<
                ", CLUCOLS: " << sysTab->cluCols <<
                ", FLAGS: " << sysTab->flags <<
                ", PROPERTY: " << sysTab->property << ")")
        auto sysTabMapRowIdIt = sysTabMapRowId.find(sysTab->rowId);
        if (sysTabMapRowIdIt == sysTabMapRowId.end())
            return;
        sysTabMapRowId.erase(sysTabMapRowIdIt);

        auto sysTabMapObjIt = sysTabMapObj.find(sysTab->obj);
        if (sysTabMapObjIt != sysTabMapObj.end())
            sysTabMapObj.erase(sysTab->obj);
        else {
            ERROR("SYS.TAB$ missing index for (OBJ#: " + std::to_string(sysTab->obj) + ")")
        }

        touchTable(sysTab->obj);
        touched = true;
    }

    void Schema::dictSysTabComPartDrop(SysTabComPart* sysTabComPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.TABCOMPART$ (ROWID: " << sysTabComPart->rowId <<
                ", OBJ#: " << std::dec << sysTabComPart->obj <<
                ", DATAOBJ#: " << sysTabComPart->dataObj <<
                ", BO#: " << sysTabComPart->bo << ")")
        auto sysTabComPartMapRowIdIt = sysTabComPartMapRowId.find(sysTabComPart->rowId);
        if (sysTabComPartMapRowIdIt == sysTabComPartMapRowId.end())
            return;
        sysTabComPartMapRowId.erase(sysTabComPartMapRowIdIt);

        SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
        auto sysTabComPartMapKeyIt = sysTabComPartMapKey.find(sysTabComPartKey);
        if (sysTabComPartMapKeyIt != sysTabComPartMapKey.end())
            sysTabComPartMapKey.erase(sysTabComPartMapKeyIt);
        else {
            ERROR("SYS.TABCOMPART$ missing index for (BO#: " + std::to_string(sysTabComPart->bo) + ", OBJ#: " +
                    std::to_string(sysTabComPart->obj) + ")")
        }

        auto sysTabComPartMapObjIt = sysTabComPartMapObj.find(sysTabComPart->obj);
        if (sysTabComPartMapObjIt != sysTabComPartMapObj.end())
            sysTabComPartMapObj.erase(sysTabComPartMapObjIt);
        else {
            ERROR("SYS.TABCOMPART$ missing index for (OBJ#: " + std::to_string(sysTabComPart->obj) + ")")
        }

        touchTable(sysTabComPart->bo);
        touched = true;
    }

    void Schema::dictSysTabPartDrop(SysTabPart* sysTabPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.TABPART$ (ROWID: " << sysTabPart->rowId <<
                ", OBJ#: " << std::dec << sysTabPart->obj <<
                ", DATAOBJ#: " << sysTabPart->dataObj <<
                ", BO#: " << sysTabPart->bo << ")")
        auto sysTabPartMapRowIdIt = sysTabPartMapRowId.find(sysTabPart->rowId);
        if (sysTabPartMapRowIdIt == sysTabPartMapRowId.end())
            return;
        sysTabPartMapRowId.erase(sysTabPartMapRowIdIt);

        SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
        auto sysTabPartMapKeyIt = sysTabPartMapKey.find(sysTabPartKey);
        if (sysTabPartMapKeyIt != sysTabPartMapKey.end())
            sysTabPartMapKey.erase(sysTabPartMapKeyIt);
        else {
            ERROR("SYS.TABPART$ missing index for (BO#: " + std::to_string(sysTabPart->bo) + ", OBJ#: " + std::to_string(sysTabPart->obj) + ")")
        }

        touchTable(sysTabPart->bo);
        touched = true;
    }

    void Schema::dictSysTabSubPartDrop(SysTabSubPart* sysTabSubPart) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.TABSUBPART$ (ROWID: " << sysTabSubPart->rowId <<
                ", OBJ#: " << std::dec << sysTabSubPart->obj <<
                ", DATAOBJ#: " << sysTabSubPart->dataObj <<
                ", POBJ#: " << sysTabSubPart->pObj << ")")
        auto sysTabSubPartMapRowIdIt = sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
        if (sysTabSubPartMapRowIdIt == sysTabSubPartMapRowId.end())
            return;
        sysTabSubPartMapRowId.erase(sysTabSubPartMapRowIdIt);

        SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
        auto sysTabSubPartMapKeyIt = sysTabSubPartMapKey.find(sysTabSubPartKey);
        if (sysTabSubPartMapKeyIt != sysTabSubPartMapKey.end())
            sysTabSubPartMapKey.erase(sysTabSubPartMapKeyIt);
        else {
            ERROR("SYS.TABSUBPART$ missing index for (POBJ#: " + std::to_string(sysTabSubPart->pObj) + ", OBJ#: " +
                    std::to_string(sysTabSubPart->obj) + ")")
        }

        auto sysObjMapObjIt = sysObjMapObj.find(sysTabSubPart->obj);
        if (sysObjMapObjIt != sysObjMapObj.end())
            touchTable(sysObjMapObjIt->second->obj);
        touched = true;
    }

    void Schema::dictSysTsDrop(SysTs* sysTs) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.TS$ (ROWID: " << sysTs->rowId <<
                ", TS#: " << std::dec << sysTs->ts <<
                ", NAME: '" << sysTs->name <<
                "', BLOCKSIZE: " << sysTs->blockSize << ")")
        auto sysTsMapRowIdIt = sysTsMapRowId.find(sysTs->rowId);
        if (sysTsMapRowIdIt == sysTsMapRowId.end())
            return;
        sysTsMapRowId.erase(sysTsMapRowIdIt);

        auto sysTsMapTsIt = sysTsMapTs.find(sysTs->ts);
        if (sysTsMapTsIt != sysTsMapTs.end())
            sysTsMapTs.erase(sysTsMapTsIt);
        else {
            ERROR("SYS.TS$ missing index for (TS#: " + std::to_string(sysTs->ts) + ")");
        }

        touched = true;
    }

    void Schema::dictSysUserDrop(SysUser* sysUser) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete SYS.USER$ (ROWID: " << sysUser->rowId <<
                ", USER#: " << std::dec << sysUser->user <<
                ", NAME: " << sysUser->name <<
                ", SPARE1: " << sysUser->spare1 << ")")
        auto sysUserMapRowIdIt = sysUserMapRowId.find(sysUser->rowId);
        if (sysUserMapRowIdIt == sysUserMapRowId.end())
            return;
        sysUserMapRowId.erase(sysUserMapRowIdIt);

        auto sysUserMapUserIt = sysUserMapUser.find(sysUser->user);
        if (sysUserMapUserIt != sysUserMapUser.end())
            sysUserMapUser.erase(sysUserMapUserIt);
        else {
            ERROR("SYS.USER$ missing index for (USER#: " + std::to_string(sysUser->user) + ")")
        }

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

    OracleTable* Schema::checkTableDict(typeObj obj) {
        auto tablePartitionMapIt = tablePartitionMap.find(obj);
        if (tablePartitionMapIt != tablePartitionMap.end())
            return tablePartitionMapIt->second;

        return nullptr;
    }

    OracleLob* Schema::checkLobDict(typeDataObj dataObj) {
        auto lobPartitionMapIt = lobPartitionMap.find(dataObj);
        if (lobPartitionMapIt != lobPartitionMap.end())
            return lobPartitionMapIt->second;

        return nullptr;
    }

    OracleLob* Schema::checkLobIndexDict(typeDataObj dataObj) {
        auto lobIndexMapIt = lobIndexMap.find(dataObj);
        if (lobIndexMapIt != lobIndexMap.end())
            return lobIndexMapIt->second;

        return nullptr;
    }

    void Schema::addTableToDict(OracleTable* table) {
        if (tableMap.find(table->obj) != tableMap.end()) {
            ERROR("can't add table (obj: " + std::to_string(table->obj) + ", dataobj: " + std::to_string(table->dataObj) + ")")
            return;
        }
        tableMap[table->obj] = table;

        for (auto lob : table->lobs) {
            for (auto dataObj : lob->lobIndexes) {
                if (lobIndexMap.find(dataObj) == lobIndexMap.end())
                    lobIndexMap[dataObj] = lob;
                else {
                    ERROR("can't add lob index element (dataobj: " + std::to_string(dataObj) + ")")
                }
            }

            for (auto dataObj : lob->lobPartitions) {
                if (lobPartitionMap.find(dataObj) == lobPartitionMap.end())
                    lobPartitionMap[dataObj] = lob;
                else {
                    ERROR("can't remove lob partition element (dataobj: " + std::to_string(dataObj) + ")")
                }
            }
        }

        if (tablePartitionMap.find(table->obj) == tablePartitionMap.end())
            tablePartitionMap[table->obj] = table;
        else {
            ERROR("can't add partition (obj: " + std::to_string(table->obj) + ", dataobj: " + std::to_string(table->dataObj) + ")")
        }

        for (typeObj2 objx : table->tablePartitions) {
            typeObj obj = objx >> 32;
            typeDataObj dataObj = objx & 0xFFFFFFFF;

            if (tablePartitionMap.find(obj) == tablePartitionMap.end())
                tablePartitionMap[obj] = table;
            else {
                ERROR("can't add partition element (obj: " + std::to_string(obj) + ", dataobj: " + std::to_string(dataObj) + ")")
            }
        }
    }

    void Schema::removeTableFromDict(OracleTable* table) {
        auto tablePartitionMapIt = tablePartitionMap.find(table->obj);
        if (tablePartitionMapIt != tablePartitionMap.end())
            tablePartitionMap.erase(tablePartitionMapIt);
        else {
            ERROR("can't remove partition (obj: " + std::to_string(table->obj) + ", dataobj: " + std::to_string(table->dataObj) + ")")
        }

        for (typeObj2 objx : table->tablePartitions) {
            typeObj obj = objx >> 32;
            typeDataObj dataObj = objx & 0xFFFFFFFF;

            tablePartitionMapIt = tablePartitionMap.find(obj);
            if (tablePartitionMapIt != tablePartitionMap.end())
                tablePartitionMap.erase(tablePartitionMapIt);
            else {
                ERROR("can't remove table partition element (obj: " + std::to_string(obj) + ", dataobj: " + std::to_string(dataObj) + ")")
            }
        }

        for (auto lob : table->lobs) {
            for (auto dataObj : lob->lobIndexes) {
                auto lobIndexMapIt = lobIndexMap.find(dataObj);
                if (lobIndexMapIt != lobIndexMap.end())
                    lobIndexMap.erase(lobIndexMapIt);
                else {
                    ERROR("can't remove lob index element (dataobj: " + std::to_string(dataObj) + ")")
                }
            }

            for (auto dataObj : lob->lobPartitions) {
                auto lobPartitionMapIt = lobPartitionMap.find(dataObj);
                if (lobPartitionMapIt != lobPartitionMap.end())
                    lobPartitionMap.erase(lobPartitionMapIt);
                else {
                    ERROR("can't remove lob partition element (dataobj: " + std::to_string(dataObj) + ")")
                }
            }
        }

        auto tableMapIt = tableMap.find(table->obj);
        if (tableMapIt != tableMap.end())
            tableMap.erase(tableMapIt);
        else {
            ERROR("can't remove table (obj: " + std::to_string(table->obj) + ", dataobj: " + std::to_string(table->dataObj) + ")")
        }
    }

    void Schema::dropUnusedMetadata(const std::set<std::string>& users, std::set<std::string>& msgs) {
        for (OracleTable* table : tablesTouched) {
            msgs.insert(table->owner + "." + table->name + " (dataobj: " + std::to_string(table->dataObj) + ", obj: " +
                        std::to_string(table->obj) + ") ");
            removeTableFromDict(table);
            delete table;
        }
        tablesTouched.clear();

        //SYS.USER$
        for (auto sysUser : sysUserSetTouched) {
            if (users.find(sysUser->name) != users.end())
                continue;

            dictSysUserDrop(sysUser);
            delete sysUser;
        }

        //SYS.OBJ$
        for (auto sysObj: sysObjSetTouched) {
            auto sysUserMapUserIt = sysUserMapUser.find(sysObj->owner);
            if (sysUserMapUserIt != sysUserMapUser.end())
                continue;

            if (!FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA))
                continue;

            dictSysObjDrop(sysObj);
            delete sysObj;
        }

        //SYS.CCOL$
        for (auto sysCCol: sysCColSetTouched) {
            if (sysObjMapObj.find(sysCCol->obj) != sysObjMapObj.end())
                continue;

            dictSysCColDrop(sysCCol);
            delete sysCCol;
        }

        //SYS.CDEF$
        for (auto sysCDef: sysCDefSetTouched) {
            if (sysObjMapObj.find(sysCDef->obj) != sysObjMapObj.end())
                continue;

            dictSysCDefDrop(sysCDef);
            delete sysCDef;
        }

        //SYS.COL$
        for (auto sysCol: sysColSetTouched) {
            if (sysObjMapObj.find(sysCol->obj) != sysObjMapObj.end())
                continue;

            dictSysColDrop(sysCol);
            delete sysCol;
        }

        //SYS.DEFERRED_STG$
        for (auto sysDeferredStg: sysDeferredStgSetTouched) {
            if (sysObjMapObj.find(sysDeferredStg->obj) != sysObjMapObj.end())
                continue;

            dictSysDeferredStgDrop(sysDeferredStg);
            delete sysDeferredStg;
        }

        //SYS.ECOL$
        for (auto sysECol: sysEColSetTouched) {
            if (sysObjMapObj.find(sysECol->tabObj) != sysObjMapObj.end())
                continue;

            dictSysEColDrop(sysECol);
            delete sysECol;
        }

        //SYS.LOB$
        for (auto sysLob: sysLobSetTouched) {
            if (sysObjMapObj.find(sysLob->obj) != sysObjMapObj.end())
                continue;

            dictSysLobDrop(sysLob);
            delete sysLob;
        }

        //SYS.LOBCOMPPART$
        for (auto sysLobCompPart: sysLobCompPartSetTouched) {
            if (sysLobMapLObj.find(sysLobCompPart->lObj) != sysLobMapLObj.end())
                continue;

            dictSysLobCompPartDrop(sysLobCompPart);
            delete sysLobCompPart;
        }

        //SYS.LOBFRAG$
        for (auto sysLobFrag: sysLobFragSetTouched) {
            if (sysLobCompPartMapPartObj.find(sysLobFrag->parentObj) != sysLobCompPartMapPartObj.end())
                continue;
            if (sysLobMapLObj.find(sysLobFrag->parentObj) != sysLobMapLObj.end())
                continue;

            dictSysLobFragDrop(sysLobFrag);
            delete sysLobFrag;
        }

        //SYS.TAB$
        for (auto sysTab: sysTabSetTouched) {
            if (sysObjMapObj.find(sysTab->obj) != sysObjMapObj.end())
                continue;

            dictSysTabDrop(sysTab);
            delete sysTab;
        }

        //SYS.TABCOMPART$
        for (auto sysTabComPart: sysTabComPartSetTouched) {
            if (sysObjMapObj.find(sysTabComPart->obj) != sysObjMapObj.end())
                continue;

            dictSysTabComPartDrop(sysTabComPart);
            delete sysTabComPart;
        }

        //SYS.TABPART$
        for (auto sysTabPart: sysTabPartSetTouched) {
            if (sysObjMapObj.find(sysTabPart->bo) != sysObjMapObj.end())
                continue;

            dictSysTabPartDrop(sysTabPart);
            delete sysTabPart;
        }

        //SYS.TABSUBPART$
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

    void Schema::buildMaps(const std::string& owner, const std::string& table, const std::vector<std::string>& keys, const std::string& keysStr,
                           typeOptions options, std::set<std::string>& msgs, bool suppLogDbPrimary, bool suppLogDbAll,
                           uint64_t defaultCharacterMapId, uint64_t defaultCharacterNcharMapId) {
        uint64_t tabCnt = 0;
        std::regex regexOwner(owner);
        std::regex regexTable(table);

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
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - already added (skipped)");
                continue;
            }

            // Object without SYS.TAB$
            auto sysTabMapObjIt = sysTabMapObj.find(sysObj->obj);
            if (sysTabMapObjIt == sysTabMapObj.end()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - SYS.TAB$ entry missing (skipped)");
                continue;
            }
            SysTab* sysTab = sysTabMapObjIt->second;

            // Skip binary objects
            if (sysTab->isBinary()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - binary (skipped");
                continue;
            }

            // Skip Index Organized Tables (IOT)
            if (sysTab->isIot()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - IOT (skipped)");
                continue;
            }

            // Skip temporary tables
            if (sysObj->isTemporary()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - temporary table (skipped)");
                continue;
            }

            // Skip nested tables
            if (sysTab->isNested()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - nested table (skipped)");
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
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - compressed table (skipped)");
                continue;
            }

            typeCol keysCnt = 0;
            bool suppLogTablePrimary = false;
            bool suppLogTableAll = false;
            bool supLogColMissing = false;

            schemaTable = new OracleTable(sysObj->obj, sysTab->dataObj, sysObj->owner, sysTab->cluCols,
                                          options, sysUser->name, sysObj->name);
            ++tabCnt;

            uint64_t lobPartitions = 0;
            uint64_t lobIndexes = 0;
            std::ostringstream lobIndexesList;
            std::ostringstream lobList;
            uint64_t tablePartitions = 0;

            if (sysTab->isPartitioned()) {
                SysTabPartKey sysTabPartKey(sysObj->obj, 0);
                for (auto sysTabPartMapKeyIt = sysTabPartMapKey.upper_bound(sysTabPartKey);
                     sysTabPartMapKeyIt != sysTabPartMapKey.end() && sysTabPartMapKeyIt->first.bo == sysObj->obj; ++sysTabPartMapKeyIt) {

                    SysTabPart* sysTabPart = sysTabPartMapKeyIt->second;
                    schemaTable->addTablePartition(sysTabPart->obj, sysTabPart->dataObj);
                    ++tablePartitions;
                }

                SysTabComPartKey sysTabComPartKey(sysObj->obj, 0);
                for (auto sysTabComPartMapKeyIt = sysTabComPartMapKey.upper_bound(sysTabComPartKey);
                     sysTabComPartMapKeyIt != sysTabComPartMapKey.end() && sysTabComPartMapKeyIt->first.bo == sysObj->obj; ++sysTabComPartMapKeyIt) {

                    SysTabSubPartKey sysTabSubPartKeyFirst(sysTabComPartMapKeyIt->second->obj, 0);
                    for (auto sysTabSubPartMapKeyIt = sysTabSubPartMapKey.upper_bound(sysTabSubPartKeyFirst);
                         sysTabSubPartMapKeyIt != sysTabSubPartMapKey.end() && sysTabSubPartMapKeyIt->first.pObj == sysTabComPartMapKeyIt->second->obj;
                                ++sysTabSubPartMapKeyIt) {

                        SysTabSubPart* sysTabSubPart = sysTabSubPartMapKeyIt->second;
                        schemaTable->addTablePartition(sysTabSubPart->obj, sysTabSubPart->dataObj);
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
                    SysCDef* sysCDef = sysCDefMapKeyIt->second;
                    if (sysCDef->isSupplementalLogPK())
                        suppLogTablePrimary = true;
                    else if (sysCDef->isSupplementalLogAll())
                        suppLogTableAll = true;
                }
            }

            SysColSeg sysColSegFirst(sysObj->obj, 0);
            for (auto sysColMapSegIt = sysColMapSeg.upper_bound(sysColSegFirst); sysColMapSegIt != sysColMapSeg.end() &&
                    sysColMapSegIt->first.obj == sysObj->obj; ++sysColMapSegIt) {
                SysCol* sysCol = sysColMapSegIt->second;

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
                        ERROR("HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << std::dec << charmapId << ") FROM DUAL;")
                        throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " - unsupported character set id: " +
                                std::to_string(charmapId) + " for column: " + sysObj->name + "." + sysCol->name);
                    }
                }

                SysCColKey sysCColKeyFirst(sysObj->obj, sysCol->intCol, 0);
                for (auto sysCColMapKeyIt = sysCColMapKey.upper_bound(sysCColKeyFirst);
                     sysCColMapKeyIt != sysCColMapKey.end() && sysCColMapKeyIt->first.obj == sysObj->obj && sysCColMapKeyIt->first.intCol == sysCol->intCol;
                     ++sysCColMapKeyIt) {
                    SysCCol* sysCCol = sysCColMapKeyIt->second;

                    // Count number of PK the column is part of
                    auto sysCDefMapConIt = sysCDefMapCon.find(sysCCol->con);
                    if (sysCDefMapConIt == sysCDefMapCon.end()) {
                        WARNING("SYS.CDEF$ missing for CON#: " << sysCCol->con)
                        continue;
                    }
                    SysCDef* sysCDef = sysCDefMapConIt->second;
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
                    for (auto key : keys) {
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

                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert("- col: " + std::to_string(sysCol->segCol) + ": " + sysCol->name + " (pk: " + std::to_string(numPk) + ", S: " +
                                std::to_string(numSup) + ", G: " + std::to_string(guardSeg) + ")");

                schemaColumn = new OracleColumn(sysCol->col, guardSeg, sysCol->segCol, sysCol->name,
                                                sysCol->type,sysCol->length, sysCol->precision, sysCol->scale,
                                                numPk,charmapId, sysCol->isNullable(), sysCol->isInvisible(),
                                                sysCol->isStoredAsLob(), sysCol->isConstraint(), sysCol->isNested(),
                                                sysCol->isUnused(), sysCol->isAdded(), sysCol->isGuard());

                schemaTable->addColumn(schemaColumn);
                schemaColumn = nullptr;
            }

            if ((options & OPTIONS_SYSTEM_TABLE) == 0) {
                SysLobKey sysLobKeyFirst(sysObj->obj, 0);
                for (auto sysLobMapKeyIt = sysLobMapKey.upper_bound(sysLobKeyFirst);
                     sysLobMapKeyIt != sysLobMapKey.end() && sysLobMapKeyIt->first.obj == sysObj->obj; ++sysLobMapKeyIt) {
                    SysLob* sysLob = sysLobMapKeyIt->second;

                    auto sysObjMapObjIt = sysObjMapObj.find(sysLob->lObj);
                    if (sysObjMapObjIt == sysObjMapObj.end())
                        throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find obj for lob " +
                                std::to_string(sysLob->lObj));
                    typeObj lobDataObj = sysObjMapObjIt->second->dataObj;

                    if (ctx->trace >= TRACE_DEBUG)
                        msgs.insert("- lob: " + std::to_string(sysLob->col) + ":" + std::to_string(sysLob->intCol) + ":" +
                                std::to_string(lobDataObj) + ":" + std::to_string(sysLob->lObj));

                    schemaLob = new OracleLob(schemaTable, sysLob->obj, lobDataObj, sysLob->lObj, sysLob->col,
                                              sysLob->intCol);

                    // indexes
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

                        schemaLob->addIndex(sysObjMapNameIt->first.dataObj);
                        if ((ctx->trace2 & TRACE2_LOB) != 0)
                            lobIndexesList << " " << std::dec << sysObjMapNameIt->first.dataObj << "/" << sysObjMapNameIt->second->obj;
                        ++lobIndexes;
                    }

                    if (schemaLob->lobIndexes.size() == 0) {
                        WARNING("missing LOB index for LOB (OBJ#:" + std::to_string(sysObj->obj) + ", DATAOBJ#" +
                                std::to_string(sysLob->lObj) + ", COL#:" +
                                std::to_string(sysLob->intCol) + ")")
                    }

                    // partitioned lob
                    if (sysTab->isPartitioned()) {
                        // partitions
                        SysLobFragKey sysLobFragKey(sysLob->lObj, 0);
                        for (auto sysLobFragMapKeyIt = sysLobFragMapKey.upper_bound(sysLobFragKey);
                                sysLobFragMapKeyIt != sysLobFragMapKey.end() &&
                                sysLobFragMapKeyIt->first.parentObj == sysLob->lObj; ++sysLobFragMapKeyIt) {

                            SysLobFrag* sysLobFrag = sysLobFragMapKeyIt->second;
                            auto sysObjMapObjIt2 = sysObjMapObj.find(sysLobFrag->fragObj);
                            if (sysObjMapObjIt2 == sysObjMapObj.end())
                                throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find obj for lob frag " +
                                                    std::to_string(sysLobFrag->fragObj));
                            typeObj lobFragDataObj = sysObjMapObjIt2->second->dataObj;

                            schemaLob->addPartition(lobFragDataObj, getLobBlockSize(sysLobFrag->ts));
                            ++lobPartitions;
                        }

                        // subpartitions
                        SysLobCompPartKey sysLobCompPartKey(sysLob->lObj, 0);
                        for (auto sysLobCompPartMapKeyIt = sysLobCompPartMapKey.upper_bound(sysLobCompPartKey);
                                sysLobCompPartMapKeyIt != sysLobCompPartMapKey.end() &&
                                sysLobCompPartMapKeyIt->first.lObj == sysLob->lObj; ++sysLobCompPartMapKeyIt) {

                            SysLobCompPart* sysLobCompPart = sysLobCompPartMapKeyIt->second;

                            SysLobFragKey sysLobFragKey2(sysLobCompPart->partObj, 0);
                            for (auto sysLobFragMapKeyIt = sysLobFragMapKey.upper_bound(sysLobFragKey2);
                                    sysLobFragMapKeyIt != sysLobFragMapKey.end() &&
                                    sysLobFragMapKeyIt->first.parentObj == sysLobCompPart->partObj; ++sysLobFragMapKeyIt) {

                                SysLobFrag* sysLobFrag = sysLobFragMapKeyIt->second;
                                auto sysObjMapObjIt2 = sysObjMapObj.find(sysLobFrag->fragObj);
                                if (sysObjMapObjIt2 == sysObjMapObj.end())
                                    throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name +
                                                        " couldn't find obj for lob frag " + std::to_string(sysLobFrag->fragObj));
                                typeObj lobFragDataObj = sysObjMapObjIt2->second->dataObj;

                                schemaLob->addPartition(lobFragDataObj, getLobBlockSize(sysLobFrag->ts));
                                ++lobPartitions;
                            }
                        }
                    }

                    schemaLob->addPartition(schemaLob->dataObj, getLobBlockSize(sysLob->ts));
                    schemaTable->addLob(schemaLob);
                    if ((ctx->trace2 & TRACE2_LOB) != 0)
                        lobList << " " << std::dec <<  schemaLob->obj << "/" << schemaLob->dataObj << "/" << std::dec << schemaLob->lObj;
                    schemaLob = nullptr;
                }
            }

            // Check if table has all listed columns
            if (static_cast<typeCol>(keys.size()) != keysCnt)
                throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find all column set (" + keysStr + ")");

            std::ostringstream ss;
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << std::dec << sysTab->dataObj << ", obj: " << std::dec << sysObj->obj <<
                    ", columns: " << std::dec << schemaTable->maxSegCol << ", lobs: " << std::dec << schemaTable->totalLobs << lobList.str() <<
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
                // Use default primary key
                if (keys.empty()) {
                    if (schemaTable->totalPk == 0)
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
            msgs.insert(ss.str());

            addTableToDict(schemaTable);
            schemaTable = nullptr;
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
            else {
                WARNING("missing TS#: " + std::to_string(ts) + ", BLOCKSIZE: " + std::to_string(pageSize) + ")")
            }
        } else {
            WARNING("missing TS#: " + std::to_string(ts) + ")")
        }

        return 8132; // default value?
    }
}
