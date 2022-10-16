/* Base class for handling of schema
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/ConfigurationException.h"
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
            sysUserAdaptive(sysUserRowId, 0, "", 0, 0, false, false),
            scn(ZERO_SCN),
            refScn(ZERO_SCN),
            loaded(false),
            schemaColumn(nullptr),
            schemaLob(nullptr),
            schemaTable(nullptr),
            sysCColTouched(false),
            sysCDefTouched(false),
            sysColTouched(false),
            sysDeferredStgTouched(false),
            sysEColTouched(false),
            sysLobTouched(false),
            sysLobCompPartTouched(false),
            sysLobFragTouched(false),
            sysObjTouched(false),
            sysTabTouched(false),
            sysTabComPartTouched(false),
            sysTabPartTouched(false),
            sysTabSubPartTouched(false),
            sysUserTouched(false),
            touched(false) {
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

        for (auto it : tableMap) {
            OracleTable* table = it.second;
            delete table;
        }
        tableMap.clear();
        lobMap.clear();
        lobPartitionMap.clear();
        lobIndexMap.clear();
        lobPageMap.clear();
        tablePartitionMap.clear();

        for (auto it : sysCColMapRowId) {
            SysCCol* sysCCol = it.second;
            delete sysCCol;
        }
        sysCColMapRowId.clear();
        sysCColMapKey.clear();

        for (auto it : sysCDefMapRowId) {
            SysCDef* sysCDef = it.second;
            delete sysCDef;
        }
        sysCDefMapRowId.clear();
        sysCDefMapCon.clear();
        sysCDefMapKey.clear();

        for (auto it : sysColMapRowId) {
            SysCol* sysCol = it.second;
            delete sysCol;
        }
        sysColMapRowId.clear();
        sysColMapKey.clear();
        sysColMapSeg.clear();

        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;
            delete sysDeferredStg;
        }
        sysDeferredStgMapRowId.clear();
        sysDeferredStgMapObj.clear();

        for (auto it : sysEColMapRowId) {
            SysECol* sysECol = it.second;
            delete sysECol;
        }
        sysEColMapRowId.clear();
        sysEColMapKey.clear();

        for (auto it : sysLobMapRowId) {
            SysLob* sysLob = it.second;
            delete sysLob;
        }
        sysLobMapRowId.clear();
        sysLobMapLObj.clear();
        sysLobMapKey.clear();

        for (auto it : sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = it.second;
            delete sysLobCompPart;
        }
        sysLobCompPartMapRowId.clear();
        sysLobCompPartMapPartObj.clear();
        sysLobCompPartMapKey.clear();

        for (auto it : sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = it.second;
            delete sysLobFrag;
        }
        sysLobFragMapRowId.clear();
        sysLobFragMapKey.clear();

        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;
            delete sysObj;
        }
        sysObjMapRowId.clear();
        sysObjMapName.clear();
        sysObjMapObj.clear();

        for (auto it : sysTabMapRowId) {
            SysTab* sysTab = it.second;
            delete sysTab;
        }
        sysTabMapRowId.clear();
        sysTabMapObj.clear();

        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = it.second;
            delete sysTabComPart;
        }
        sysTabComPartMapRowId.clear();
        sysTabComPartMapObj.clear();
        sysTabComPartMapKey.clear();

        for (auto it : sysTabPartMapRowId) {
            SysTabPart* sysTabPart = it.second;
            delete sysTabPart;
        }
        sysTabPartMapRowId.clear();
        sysTabPartMapKey.clear();

        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = it.second;
            delete sysTabSubPart;
        }
        sysTabSubPartMapRowId.clear();
        sysTabSubPartMapKey.clear();

        for (auto it : sysTsMapRowId) {
            SysTs* sysTs = it.second;
            delete sysTs;
        }
        sysTsMapRowId.clear();
        sysTsMapTs.clear();

        for (auto it : sysUserMapRowId) {
            SysUser* sysUser = it.second;
            delete sysUser;
        }
        sysUserMapRowId.clear();
        sysUserMapUser.clear();

        lobsTouched.clear();
        lobPartitionsTouched.clear();
        tablesTouched.clear();
        tablePartitionsTouched.clear();
        usersTouched.clear();
    }

    bool Schema::compareSysCCol(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysCColMapRowId) {
            SysCCol* sysCCol = it.second;
            auto sysCColIt = otherSchema->sysCColMapRowId.find(sysCCol->rowId);
            if (sysCColIt == otherSchema->sysCColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CCOL$ lost ROWID: " + sysCCol->rowId.toString());
                return false;
            } else if (*sysCCol != *(sysCColIt->second)) {
                msgs.assign("schema mismatch: SYS.CCOL$ differs ROWID: " + sysCCol->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysCColMapRowId) {
            SysCCol* sysCCol = it.second;
            auto sysCColIt = sysCColMapRowId.find(sysCCol->rowId);
            if (sysCColIt == sysCColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CCOL$ lost ROWID: " + sysCCol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysCDef(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysCDefMapRowId) {
            SysCDef* sysCDef = it.second;
            auto sysCDefIt = otherSchema->sysCDefMapRowId.find(sysCDef->rowId);
            if (sysCDefIt == otherSchema->sysCDefMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CDEF$ lost ROWID: " + sysCDef->rowId.toString());
                return false;
            } else if (*sysCDef != *(sysCDefIt->second)) {
                msgs.assign("schema mismatch: SYS.CDEF$ differs ROWID: " + sysCDef->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysCDefMapRowId) {
            SysCDef* sysCDef = it.second;
            auto sysCDefIt = sysCDefMapRowId.find(sysCDef->rowId);
            if (sysCDefIt == sysCDefMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.CDEF$ lost ROWID: " + sysCDef->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysCol(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysColMapRowId) {
            SysCol* sysCol = it.second;
            auto sysColIt = otherSchema->sysColMapRowId.find(sysCol->rowId);
            if (sysColIt == otherSchema->sysColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.COL$ lost ROWID: " + sysCol->rowId.toString());
                return false;
            } else if (*sysCol != *(sysColIt->second)) {
                msgs.assign("schema mismatch: SYS.COL$ differs ROWID: " + sysCol->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysColMapRowId) {
            SysCol* sysCol = it.second;
            auto sysColIt = sysColMapRowId.find(sysCol->rowId);
            if (sysColIt == sysColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.COL$ lost ROWID: " + sysCol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysDeferredStg(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;
            auto sysDeferredStgIt = otherSchema->sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
            if (sysDeferredStgIt == otherSchema->sysDeferredStgMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ lost ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            } else if (*sysDeferredStg != *(sysDeferredStgIt->second)) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ differs ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;
            auto sysDeferredStgIt = sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
            if (sysDeferredStgIt == sysDeferredStgMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.DEFERRED_STG$ lost ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysECol(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysEColMapRowId) {
            SysECol* sysECol = it.second;
            auto sysEColIt = otherSchema->sysEColMapRowId.find(sysECol->rowId);
            if (sysEColIt == otherSchema->sysEColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.ECOL$ lost ROWID: " + sysECol->rowId.toString());
                return false;
            } else if (*sysECol != *(sysEColIt->second)) {
                msgs.assign("schema mismatch: SYS.ECOL$ differs ROWID: " + sysECol->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysEColMapRowId) {
            SysECol* sysECol = it.second;
            auto sysEColIt = sysEColMapRowId.find(sysECol->rowId);
            if (sysEColIt == sysEColMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.ECOL$ lost ROWID: " + sysECol->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLob(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysLobMapRowId) {
            SysLob* sysLob = it.second;
            auto sysLobIt = otherSchema->sysLobMapRowId.find(sysLob->rowId);
            if (sysLobIt == otherSchema->sysLobMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOB$ lost ROWID: " + sysLob->rowId.toString());
                return false;
            } else if (*sysLob != *(sysLobIt->second)) {
                msgs.assign("schema mismatch: SYS.LOB$ differs ROWID: " + sysLob->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysLobMapRowId) {
            SysLob* sysLob = it.second;
            auto sysLobIt = sysLobMapRowId.find(sysLob->rowId);
            if (sysLobIt == sysLobMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOB$ lost ROWID: " + sysLob->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLobCompPart(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = it.second;
            auto sysLobCompPartIt = otherSchema->sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
            if (sysLobCompPartIt == otherSchema->sysLobCompPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ lost ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            } else if (*sysLobCompPart != *(sysLobCompPartIt->second)) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ differs ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = it.second;
            auto sysLobCompPartIt = sysLobCompPartMapRowId.find(sysLobCompPart->rowId);
            if (sysLobCompPartIt == sysLobCompPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBCOMPPART$ lost ROWID: " + sysLobCompPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysLobFrag(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = it.second;
            auto sysLobFragIt = otherSchema->sysLobFragMapRowId.find(sysLobFrag->rowId);
            if (sysLobFragIt == otherSchema->sysLobFragMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ lost ROWID: " + sysLobFrag->rowId.toString());
                return false;
            } else if (*sysLobFrag != *(sysLobFragIt->second)) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ differs ROWID: " + sysLobFrag->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = it.second;
            auto sysLobFragIt = sysLobFragMapRowId.find(sysLobFrag->rowId);
            if (sysLobFragIt == sysLobFragMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.LOBFRAG$ lost ROWID: " + sysLobFrag->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysObj(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;
            auto sysObjIt = otherSchema->sysObjMapRowId.find(sysObj->rowId);
            if (sysObjIt == otherSchema->sysObjMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.OBJ$ lost ROWID: " + sysObj->rowId.toString());
                return false;
            } else if (*sysObj != *(sysObjIt->second)) {
                msgs.assign("schema mismatch: SYS.OBJ$ differs ROWID: " + sysObj->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysObjMapRowId) {
            SysObj* sysObj = it.second;
            auto sysObjIt = sysObjMapRowId.find(sysObj->rowId);
            if (sysObjIt == sysObjMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.OBJ$ lost ROWID: " + sysObj->rowId.toString());
                return false;
            }
        }        return true;
    }

    bool Schema::compareSysTab(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysTabMapRowId) {
            SysTab* sysTab = it.second;
            auto sysTabIt = otherSchema->sysTabMapRowId.find(sysTab->rowId);
            if (sysTabIt == otherSchema->sysTabMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TAB$ lost ROWID: " + sysTab->rowId.toString());
                return false;
            } else if (*sysTab != *(sysTabIt->second)) {
                msgs.assign("schema mismatch: SYS.TAB$ differs ROWID: " + sysTab->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysTabMapRowId) {
            SysTab* sysTab = it.second;
            auto sysTabIt = sysTabMapRowId.find(sysTab->rowId);
            if (sysTabIt == sysTabMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TAB$ lost ROWID: " + sysTab->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabComPart(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = it.second;
            auto sysTabComPartIt = otherSchema->sysTabComPartMapRowId.find(sysTabComPart->rowId);
            if (sysTabComPartIt == otherSchema->sysTabComPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ lost ROWID: " + sysTabComPart->rowId.toString());
                return false;
            } else if (*sysTabComPart != *(sysTabComPartIt->second)) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ differs ROWID: " + sysTabComPart->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = it.second;
            auto sysTabComPartIt = sysTabComPartMapRowId.find(sysTabComPart->rowId);
            if (sysTabComPartIt == sysTabComPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABCOMPART$ lost ROWID: " + sysTabComPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabPart(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysTabPartMapRowId) {
            SysTabPart* sysTabPart = it.second;
            auto sysTabPartIt = otherSchema->sysTabPartMapRowId.find(sysTabPart->rowId);
            if (sysTabPartIt == otherSchema->sysTabPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABPART$ lost ROWID: " + sysTabPart->rowId.toString());
                return false;
            } else if (*sysTabPart != *(sysTabPartIt->second)) {
                msgs.assign("schema mismatch: SYS.TABPART$ differs ROWID: " + sysTabPart->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysTabPartMapRowId) {
            SysTabPart* sysTabPart = it.second;
            auto sysTabPartIt = sysTabPartMapRowId.find(sysTabPart->rowId);
            if (sysTabPartIt == sysTabPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABPART$ lost ROWID: " + sysTabPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTabSubPart(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = it.second;
            auto sysTabSubPartIt = otherSchema->sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
            if (sysTabSubPartIt == otherSchema->sysTabSubPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ lost ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            } else if (*sysTabSubPart != *(sysTabSubPartIt->second)) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ differs ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = it.second;
            auto sysTabSubPartIt = sysTabSubPartMapRowId.find(sysTabSubPart->rowId);
            if (sysTabSubPartIt == sysTabSubPartMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TABSUBPART$ lost ROWID: " + sysTabSubPart->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysTs(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysTsMapRowId) {
            SysTs* sysTs = it.second;
            auto sysTsIt = otherSchema->sysTsMapRowId.find(sysTs->rowId);
            if (sysTsIt == otherSchema->sysTsMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TS$ lost ROWID: " + sysTs->rowId.toString());
                return false;
            } else if (*sysTs != *(sysTsIt->second)) {
                msgs.assign("schema mismatch: SYS.TS$ differs ROWID: " + sysTs->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysTsMapRowId) {
            SysTs* sysTs = it.second;
            auto sysTsIt = sysTsMapRowId.find(sysTs->rowId);
            if (sysTsIt == sysTsMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.TS$ lost ROWID: " + sysTs->rowId.toString());
                return false;
            }
        }
        return true;
    }

    bool Schema::compareSysUser(Schema* otherSchema, std::string& msgs) {
        for (auto it : sysUserMapRowId) {
            SysUser* sysUser = it.second;
            auto sysUserIt = otherSchema->sysUserMapRowId.find(sysUser->rowId);
            if (sysUserIt == otherSchema->sysUserMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.USER$ lost ROWID: " + sysUser->rowId.toString());
                return false;
            } else if (*sysUser != *(sysUserIt->second)) {
                msgs.assign("schema mismatch: SYS.USER$ differs ROWID: " + sysUser->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysUserMapRowId) {
            SysUser* sysUser = it.second;
            auto sysUserIt = sysUserMapRowId.find(sysUser->rowId);
            if (sysUserIt == sysUserMapRowId.end()) {
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

    void Schema::refreshIndexesSysCCol() {
        if (!sysCColTouched)
            return;
        sysCColMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysCColMapRowId) {
            SysCCol* sysCCol = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysCCol->obj) != sysObjMapObj.end()) {
                SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
                sysCColMapKey[sysCColKey] = sysCCol;
                if (sysCCol->touched) {
                    touchTable(sysCCol->obj);
                    sysCCol->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CCOL$ (ROWID: " << it.first <<
                    ", CON#: " << std::dec << sysCCol->con <<
                    ", INTCOL#: " << sysCCol->intCol <<
                    ", OBJ#: " << sysCCol->obj <<
                    ", SPARE1: " << sysCCol->spare1 << ")")
            removeRowId.push_back(it.first);
            delete sysCCol;
        }

        for (typeRowId rowId: removeRowId)
            sysCColMapRowId.erase(rowId);
        sysCColTouched = false;
    }

    void Schema::refreshIndexesSysCDef() {
        if (!sysCDefTouched)
            return;
        sysCDefMapKey.clear();
        sysCDefMapCon.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysCDefMapRowId) {
            SysCDef* sysCDef = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysCDef->obj) != sysObjMapObj.end()) {
                SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
                sysCDefMapKey[sysCDefKey] = sysCDef;
                sysCDefMapCon[sysCDef->con] = sysCDef;
                if (sysCDef->touched) {
                    touchTable(sysCDef->obj);
                    sysCDef->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CDEF$ (ROWID: " << it.first <<
                    ", CON#: " << std::dec << sysCDef->con <<
                    ", OBJ#: " << sysCDef->obj <<
                    ", TYPE: " << sysCDef->type << ")")
            removeRowId.push_back(it.first);
            delete sysCDef;
        }

        for (typeRowId rowId: removeRowId)
            sysCDefMapRowId.erase(rowId);
        sysCDefTouched = false;
    }

    void Schema::refreshIndexesSysCol() {
        if (!sysColTouched)
            return;
        sysColMapKey.clear();
        sysColMapSeg.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysColMapRowId) {
            SysCol* sysCol = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysCol->obj) != sysObjMapObj.end()) {
                SysColKey sysColKey(sysCol->obj, sysCol->intCol);
                sysColMapKey[sysColKey] = sysCol;
                SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
                sysColMapSeg[sysColSeg] = sysCol;
                if (sysCol->touched) {
                    touchTable(sysCol->obj);
                    sysCol->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage COL$ (ROWID: " << it.first <<
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
            removeRowId.push_back(it.first);
            delete sysCol;
        }

        for (typeRowId rowId: removeRowId)
            sysColMapRowId.erase(rowId);
        sysColTouched = false;
    }

    void Schema::refreshIndexesSysDeferredStg() {
        if (!sysDeferredStgTouched)
            return;
        sysDeferredStgMapObj.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysDeferredStg->obj) != sysObjMapObj.end()) {
                sysDeferredStgMapObj[sysDeferredStg->obj] = sysDeferredStg;
                if (sysDeferredStg->touched) {
                    touchTable(sysDeferredStg->obj);
                    sysDeferredStg->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage DEFERRED_STG$ (ROWID: " << it.first <<
                    ", OBJ#: " << std::dec << sysDeferredStg->obj <<
                    ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")")
            removeRowId.push_back(it.first);
            delete sysDeferredStg;
        }

        for (typeRowId rowId: removeRowId)
            sysDeferredStgMapRowId.erase(rowId);
        sysDeferredStgTouched = false;
    }

    void Schema::refreshIndexesSysECol() {
        if (!sysEColTouched)
            return;
        sysEColMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysEColMapRowId) {
            SysECol* sysECol = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysECol->tabObj) != sysObjMapObj.end()) {
                SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
                sysEColMapKey[sysEColKey] = sysECol;
                if (sysECol->touched) {
                    touchTable(sysECol->tabObj);
                    sysECol->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage ECOL$ (ROWID: " << it.first <<
                    ", TABOBJ#: " << std::dec << sysECol->tabObj <<
                    ", COLNUM: " << sysECol->colNum <<
                    ", GUARD_ID: " << sysECol->guardId << ")")
            removeRowId.push_back(it.first);
            delete sysECol;
        }

        for (typeRowId rowId: removeRowId)
            sysEColMapRowId.erase(rowId);
        sysEColTouched = false;
    }


    void Schema::refreshIndexesSysLob() {
        if (!sysLobTouched)
            return;
        sysLobMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysLobMapRowId) {
            SysLob* sysLob = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysLob->obj) != sysObjMapObj.end()) {
                SysLobKey sysLobKey(sysLob->obj, sysLob->intCol);
                sysLobMapKey[sysLobKey] = sysLob;
                sysLobMapLObj[sysLob->lObj] = sysLob;
                if (sysLob->touched) {
                    touchTable(sysLob->obj);
                    sysLob->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage LOB$ (ROWID: " << it.first <<
                    ", OBJ#: " << std::dec << sysLob->obj <<
                    ", COL#: " << sysLob->col <<
                    ", INTCOL#: " << sysLob->intCol <<
                    ", LOBJ#: " << sysLob->lObj <<
                    ", TS#: " << sysLob->ts << ")")
            removeRowId.push_back(it.first);
            delete sysLob;
        }

        for (typeRowId rowId: removeRowId)
            sysLobMapRowId.erase(rowId);
        sysLobTouched = false;
    }

    void Schema::refreshIndexesSysLobCompPart() {
        if (!sysLobCompPartTouched)
            return;
        sysLobCompPartMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysLobCompPartMapRowId) {
            SysLobCompPart* sysLobCompPart = it.second;
            SysLob* sysLob = nullptr;

            // Find SYS.LOB$
            auto it2 = sysLobMapLObj.find(sysLobCompPart->lObj);
            if (it2 != sysLobMapLObj.end())
                sysLob = it2->second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || (sysLob != nullptr && sysObjMapObj.find(sysLob->obj) != sysObjMapObj.end())) {
                SysLobCompPartKey sysLobCompPartKey(sysLobCompPart->lObj, sysLobCompPart->partObj);
                sysLobCompPartMapKey[sysLobCompPartKey] = sysLobCompPart;
                sysLobCompPartMapPartObj[sysLobCompPart->partObj] = sysLobCompPart;
                if (sysLobCompPart->touched) {
                    if (sysLob != nullptr)
                        touchTable(sysLob->obj);
                    sysLobCompPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage LOBCOMPPART$ (ROWID: " << it.first <<
                    ", PARTOBJ#: " << std::dec << sysLobCompPart->partObj <<
                    ", LOBJ#: " << sysLobCompPart->lObj << ")")
            removeRowId.push_back(it.first);
            delete sysLobCompPart;
        }

        for (typeRowId rowId: removeRowId)
            sysLobCompPartMapRowId.erase(rowId);
        sysLobCompPartTouched = false;
    }

    void Schema::refreshIndexesSysLobFrag() {
        if (!sysLobFragTouched)
            return;
        sysLobFragMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysLobFragMapRowId) {
            SysLobFrag* sysLobFrag = it.second;
            SysLob* sysLob = nullptr;
            SysLobCompPart* sysLobCompPart = nullptr;

            // Find SYS.LOB$
            auto it1 = sysLobMapLObj.find(sysLobFrag->parentObj);
            if (it1 != sysLobMapLObj.end())
                sysLob = it1->second;
            else {
                // Find SYS.LOBCOMPPART$
                auto it2 = sysLobCompPartMapPartObj.find(sysLobFrag->parentObj);
                if (it2 != sysLobCompPartMapPartObj.end())
                    sysLobCompPart = it2->second;

                if (sysLobCompPart != nullptr) {
                    // Find SYS.LOB$
                    auto it3 = sysLobMapLObj.find(sysLobCompPart->lObj);
                    if (it3 != sysLobMapLObj.end())
                        sysLob = it3->second;
                }
            }

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || (sysLob != nullptr && sysObjMapObj.find(sysLob->obj) != sysObjMapObj.end())) {
                SysLobFragKey sysLobFragKey(sysLobFrag->parentObj, sysLobFrag->fragObj);
                sysLobFragMapKey[sysLobFragKey] = sysLobFrag;
                if (sysLobFrag->touched) {
                    if (sysLob != nullptr)
                        touchTable(sysLob->obj);
                    sysLobFrag->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage LOBFRAG$ (ROWID: " << it.first <<
                    ", FRAGOBJ#: " << std::dec << sysLobFrag->fragObj <<
                    ", PARENTOBJ#: " << sysLobFrag->parentObj << ")")
            removeRowId.push_back(it.first);
            delete sysLobFrag;
        }

        for (typeRowId rowId: removeRowId)
            sysLobFragMapRowId.erase(rowId);
        sysLobFragTouched = false;
    }

    void Schema::refreshIndexesSysObj() {
        if (!sysObjTouched)
            return;
        sysObjMapName.clear();
        sysObjMapObj.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                SysObjNameKey sysObjNameKey(sysObj->owner, sysObj->name.c_str(), sysObj->obj);
                sysObjMapName[sysObjNameKey] = sysObj;
                sysObjMapObj[sysObj->obj] = sysObj;
                if (sysObj->touched) {
                    touchTable(sysObj->obj);
                    sysObj->touched = false;
                }
                continue;
            }

            auto sysUserIt = sysUserMapUser.find(sysObj->owner);
            if (sysUserIt != sysUserMapUser.end()) {
                SysUser* sysUser = sysUserIt->second;
                if (!sysUser->single || sysObj->single) {
                    SysObjNameKey sysObjNameKey(sysObj->owner, sysObj->name.c_str(), sysObj->obj);
                    sysObjMapName[sysObjNameKey] = sysObj;
                    sysObjMapObj[sysObj->obj] = sysObj;
                    if (sysObj->touched) {
                        touchTable(sysObj->obj);
                        sysObj->touched = false;
                    }
                    continue;
                }
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage OBJ$ (ROWID: " << it.first <<
                    ", OWNER#: " << std::dec << sysObj->owner <<
                    ", OBJ#: " << sysObj->obj <<
                    ", DATAOBJ#: " << sysObj->dataObj <<
                    ", TYPE#: " << sysObj->type <<
                    ", NAME: '" << sysObj->name <<
                    "', FLAGS: " << sysObj->flags << ")")
            removeRowId.push_back(it.first);
            delete sysObj;
        }

        for (typeRowId rowId: removeRowId)
            sysObjMapRowId.erase(rowId);
        sysObjTouched = false;
    }

    void Schema::refreshIndexesSysTab() {
        if (!sysTabTouched)
            return;
        sysTabMapObj.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysTabMapRowId) {
            SysTab* sysTab = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysTab->obj) != sysObjMapObj.end()) {
                sysTabMapObj[sysTab->obj] = sysTab;
                if (sysTab->touched) {
                    touchTable(sysTab->obj);
                    sysTab->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TAB$ (ROWID: " << it.first <<
                    ", OBJ#: " << std::dec << sysTab->obj <<
                    ", DATAOBJ#: " << sysTab->dataObj <<
                    ", CLUCOLS: " << sysTab->cluCols <<
                    ", FLAGS: " << sysTab->flags <<
                    ", PROPERTY: " << sysTab->property << ")")
            removeRowId.push_back(it.first);
            delete sysTab;
        }

        for (typeRowId rowId: removeRowId)
            sysTabMapRowId.erase(rowId);
        sysTabTouched = false;
    }

    void Schema::refreshIndexesSysTabComPart() {
        if (!sysTabComPartTouched)
            return;
        sysTabComPartMapKey.clear();
        sysTabComPartMapObj.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = it.second;

            sysTabComPartMapObj[sysTabComPart->obj] = sysTabComPart;
            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysTabComPart->bo) != sysObjMapObj.end()) {
                SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
                sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
                if (sysTabComPart->touched) {
                    touchTable(sysTabComPart->bo);
                    sysTabComPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABCOMPART$ (ROWID: " << it.first <<
                    ", OBJ#: " << std::dec << sysTabComPart->obj <<
                    ", DATAOBJ#: " << sysTabComPart->dataObj <<
                    ", BO#: " << sysTabComPart->bo << ")")
            removeRowId.push_back(it.first);
            delete sysTabComPart;
        }

        for (typeRowId rowId: removeRowId)
            sysTabComPartMapRowId.erase(rowId);
        sysTabComPartTouched = false;
    }

    void Schema::refreshIndexesSysTabPart() {
        if (!sysTabPartTouched)
            return;
        sysTabPartMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysTabPartMapRowId) {
            SysTabPart* sysTabPart = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysTabPart->obj) != sysObjMapObj.end()) {
                SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
                sysTabPartMapKey[sysTabPartKey] = sysTabPart;
                if (sysTabPart->touched) {
                    touchTable(sysTabPart->bo);
                    sysTabPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABPART$ (ROWID: " << it.first <<
                    ", OBJ#: " << std::dec << sysTabPart->obj <<
                    ", DATAOBJ#: " << sysTabPart->dataObj <<
                    ", BO#: " << sysTabPart->bo << ")")
            removeRowId.push_back(it.first);
            delete sysTabPart;
        }

        for (typeRowId rowId: removeRowId)
            sysTabPartMapRowId.erase(rowId);
        sysTabPartTouched = false;
    }

    void Schema::refreshIndexesSysTabSubPart() {
        if (!sysTabSubPartTouched)
            return;
        sysTabSubPartMapKey.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = it.second;
            SysTabComPart* sysTabComPart = nullptr;

            // Find SYS.TABCOMPART$
            auto it2 = sysTabComPartMapObj.find(sysTabSubPart->pObj);
            if (it2 != sysTabComPartMapObj.end())
                sysTabComPart = it2->second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || (sysTabComPart != nullptr && sysObjMapObj.find(sysTabComPart->bo) != sysObjMapObj.end())) {
                SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
                sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;
                if (sysTabSubPart->touched) {
                    if (sysTabComPart != nullptr)
                        touchTable(sysTabComPart->bo);
                    sysTabSubPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABSUBPART$ (ROWID: " << it.first <<
                    ", OBJ#: " << std::dec << sysTabSubPart->obj <<
                    ", DATAOBJ#: " << sysTabSubPart->dataObj <<
                    ", POBJ#: " << sysTabSubPart->pObj << ")")
            removeRowId.push_back(it.first);
            delete sysTabSubPart;
        }

        for (typeRowId rowId: removeRowId)
            sysTabSubPartMapRowId.erase(rowId);
        sysTabSubPartTouched = false;
    }

    void Schema::refreshIndexesSysTs() {
        if (!sysTsTouched)
            return;
        sysTsMapTs.clear();

        for (auto it : sysTsMapRowId) {
            SysTs* sysTs = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysTsMapTs.find(sysTs->ts) != sysTsMapTs.end()) {
                sysTsMapTs[sysTs->ts] = sysTs;
                continue;
            }
        }

        sysTsTouched = false;
    }

    void Schema::refreshIndexesSysUser(std::set<std::string>& users) {
        if (!sysUserTouched)
            return;
        sysUserMapUser.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysUserMapRowId) {
            SysUser* sysUser = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysUser->single || users.find(sysUser->name) != users.end()) {
                sysUserMapUser[sysUser->user] = sysUser;
                if (sysUser->touched) {
                    touchUser(sysUser->user);
                    sysUser->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage USER$ (ROWID: " << it.first <<
                    ", USER#: " << std::dec << sysUser->user <<
                    ", NAME: " << sysUser->name <<
                    ", SPARE1: " << sysUser->spare1 << ")")
            removeRowId.push_back(it.first);
            delete sysUser;
        }

        for (typeRowId rowId: removeRowId)
            sysUserMapRowId.erase(rowId);
        sysUserTouched = false;
    }

    void Schema::refreshIndexes(std::set<std::string>& users) {
        refreshIndexesSysUser(users);
        refreshIndexesSysObj();
        refreshIndexesSysCCol();
        refreshIndexesSysCDef();
        refreshIndexesSysCol();
        refreshIndexesSysDeferredStg();
        refreshIndexesSysECol();
        refreshIndexesSysLob();
        refreshIndexesSysLobCompPart();
        refreshIndexesSysLobFrag();
        refreshIndexesSysTab();
        refreshIndexesSysTabComPart();
        refreshIndexesSysTabPart();
        refreshIndexesSysTabSubPart();
        refreshIndexesSysTs();
        touched = false;
    }

    bool Schema::dictSysCColAdd(const char* rowIdStr, typeCon con, typeCol intCol, typeObj obj, uint64_t spare11, uint64_t spare12) {
        typeRowId rowId(rowIdStr);
        if (sysCColMapRowId.find(rowId) != sysCColMapRowId.end())
            return false;

        auto* sysCCol = new SysCCol(rowId, con, intCol, obj, spare11, spare12, false);
        sysCColMapRowId[rowId] = sysCCol;
        SysCColKey sysCColKey(obj, intCol, con);
        sysCColMapKey[sysCColKey] = sysCCol;

        return true;
    }

    bool Schema::dictSysCDefAdd(const char* rowIdStr, typeCon con, typeObj obj, typeType type) {
        typeRowId rowId(rowIdStr);
        if (sysCDefMapRowId.find(rowId) != sysCDefMapRowId.end())
            return false;

        auto* sysCDef = new SysCDef(rowId, con, obj, type, false);
        sysCDefMapRowId[rowId] = sysCDef;
        sysCDefMapCon[con] = sysCDef;
        SysCDefKey sysCDefKey(obj, con);
        sysCDefMapKey[sysCDefKey] = sysCDef;

        return true;
    }

    bool Schema::dictSysColAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type,
                               uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, bool null_,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysColMapRowId.find(rowId) != sysColMapRowId.end())
            return false;

        if (strlen(name) > SYS_COL_NAME_LENGTH)
            throw DataException("SYS.COL$ too long value for NAME (value: '" + std::string(name) + "', length: " +
                    std::to_string(strlen(name)) + ")");
        auto* sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length,
                                  precision, scale, charsetForm, charsetId, null_, property1,
                                  property2, false);
        sysColMapRowId[rowId] = sysCol;
        SysColKey sysColKey(obj, intCol);
        sysColMapKey[sysColKey] = sysCol;
        SysColSeg sysColSeg(obj, segCol);
        sysColMapSeg[sysColSeg] = sysCol;

        return true;
    }

    bool Schema::dictSysDeferredStgAdd(const char* rowIdStr, typeObj obj, uint64_t flagsStg1, uint64_t flagsStg2) {
        typeRowId rowId(rowIdStr);
        if (sysDeferredStgMapRowId.find(rowId) != sysDeferredStgMapRowId.end())
            return false;

        auto* sysDeferredStg = new SysDeferredStg(rowId, obj, flagsStg1, flagsStg2, false);
        sysDeferredStgMapRowId[rowId] = sysDeferredStg;
        sysDeferredStgMapObj[obj] = sysDeferredStg;

        return true;
    }

    bool Schema::dictSysEColAdd(const char* rowIdStr, typeObj tabObj, typeCol colNum, typeCol guardId) {
        typeRowId rowId(rowIdStr);
        if (sysEColMapRowId.find(rowId) != sysEColMapRowId.end())
            return false;

        auto* sysECol = new SysECol(rowId, tabObj, colNum, guardId, false);
        sysEColMapRowId[rowId] = sysECol;
        SysEColKey sysEColKey(tabObj, colNum);
        sysEColMapKey[sysEColKey] = sysECol;

        return true;
    }

    bool Schema::dictSysLobAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol intCol, typeObj lObj, typeTs ts) {
        typeRowId rowId(rowIdStr);
        if (sysLobMapRowId.find(rowId) != sysLobMapRowId.end())
            return false;

        auto* sysLob = new SysLob(rowId, obj, col, intCol, lObj, ts, false);
        sysLobMapRowId[rowId] = sysLob;
        SysLobKey sysLobKey(obj, intCol);
        sysLobMapKey[sysLobKey] = sysLob;
        sysLobMapLObj[lObj] = sysLob;

        return true;
    }

    bool Schema::dictSysLobCompPartAdd(const char* rowIdStr, typeObj partObj, typeObj lObj) {
        typeRowId rowId(rowIdStr);
        if (sysLobCompPartMapRowId.find(rowId) != sysLobCompPartMapRowId.end())
            return false;

        auto* sysLobCompPart = new SysLobCompPart(rowId, partObj, lObj, false);
        sysLobCompPartMapRowId[rowId] = sysLobCompPart;
        SysLobCompPartKey sysLobCompPartKey(lObj, partObj);
        sysLobCompPartMapKey[sysLobCompPartKey] = sysLobCompPart;
        sysLobCompPartMapPartObj[partObj] = sysLobCompPart;

        return true;
    }

    bool Schema::dictSysLobFragAdd(const char* rowIdStr, typeObj fragObj, typeObj parentObj, typeTs ts) {
        typeRowId rowId(rowIdStr);
        if (sysLobFragMapRowId.find(rowId) != sysLobFragMapRowId.end())
            return false;

        auto* sysLobFrag = new SysLobFrag(rowId, fragObj, parentObj, ts, false);
        sysLobFragMapRowId[rowId] = sysLobFrag;
        SysLobFragKey sysLobFragKey(parentObj, fragObj);
        sysLobFragMapKey[sysLobFragKey] = sysLobFrag;

        return true;
    }

    bool Schema::dictSysObjAdd(const char* rowIdStr, typeUser owner, typeObj obj, typeDataObj dataObj, typeType type, const char* name,
                               uint64_t flags1, uint64_t flags2, bool single) {
        typeRowId rowId(rowIdStr);

        auto sysObjIt = sysObjMapRowId.find(rowId);
        if (sysObjIt != sysObjMapRowId.end()) {
            SysObj* sysObj = sysObjIt->second;
            if (!single && sysObj->single) {
                sysObj->single = false;
                TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for object " << name << " (owner " << std::dec << owner << ")")
            }
            return false;
        }

        if (strlen(name) > SYS_OBJ_NAME_LENGTH)
            throw DataException("SYS.OBJ$ too long value for NAME (value: '" + std::string(name) + "', length: " +
                    std::to_string(strlen(name)) + ")");
        auto* sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2,
                                  single, false);
        sysObjMapRowId[rowId] = sysObj;
        SysObjNameKey sysObjNameKey(owner, name, obj);
        sysObjMapName[sysObjNameKey] = sysObj;
        sysObjMapObj[obj] = sysObj;

        return true;
    }

    bool Schema::dictSysTabAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeCol cluCols, uint64_t flags1, uint64_t flags2,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end())
            return false;

        auto* sysTab = new SysTab(rowId, obj, dataObj, cluCols, flags1, flags2, property1,
                                  property2, false);
        sysTabMapRowId[rowId] = sysTab;
        sysTabMapObj[obj] = sysTab;

        return true;
    }

    bool Schema::dictSysTabComPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo) {
        typeRowId rowId(rowIdStr);
        if (sysTabComPartMapRowId.find(rowId) != sysTabComPartMapRowId.end())
            return false;

        auto* sysTabComPart = new SysTabComPart(rowId, obj, dataObj, bo, false);
        sysTabComPartMapRowId[rowId] = sysTabComPart;
        SysTabComPartKey sysTabComPartKey(bo, obj);
        sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
        sysTabComPartMapObj[sysTabComPart->obj] = sysTabComPart;

        return true;
    }

    bool Schema::dictSysTabPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo) {
        typeRowId rowId(rowIdStr);
        if (sysTabPartMapRowId.find(rowId) != sysTabPartMapRowId.end())
            return false;

        auto* sysTabPart = new SysTabPart(rowId, obj, dataObj, bo, false);
        sysTabPartMapRowId[rowId] = sysTabPart;
        SysTabPartKey sysTabPartKey(bo, obj);
        sysTabPartMapKey[sysTabPartKey] = sysTabPart;

        return true;
    }

    bool Schema::dictSysTabSubPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj pObj) {
        typeRowId rowId(rowIdStr);
        if (sysTabSubPartMapRowId.find(rowId) != sysTabSubPartMapRowId.end())
            return false;

        auto* sysTabSubPart = new SysTabSubPart(rowId, obj, dataObj, pObj, false);
        sysTabSubPartMapRowId[rowId] = sysTabSubPart;
        SysTabSubPartKey sysTabSubPartKey(pObj, obj);
        sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;

        return true;
    }

    bool Schema::dictSysTsAdd(const char* rowIdStr, typeTs ts, const char* name, uint32_t blockSize) {
        typeRowId rowId(rowIdStr);
        if (sysTsMapRowId.find(rowId) != sysTsMapRowId.end())
            return false;

        auto* sysTs = new SysTs(rowId, ts, name, blockSize, false);
        sysTsMapRowId[rowId] = sysTs;
        sysTsMapTs[ts] = sysTs;

        return true;
    }

    bool Schema::dictSysUserAdd(const char* rowIdStr, typeUser user, const char* name, uint64_t spare11, uint64_t spare12, bool single) {
        typeRowId rowId(rowIdStr);

        auto sysUserIt = sysUserMapRowId.find(rowId);
        if (sysUserIt != sysUserMapRowId.end()) {
            SysUser* sysUser = sysUserIt->second;
            if (sysUser->single) {
                if (!single) {
                    sysUser->single = false;
                    TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for user " << name << " (" << std::dec << user << ")")
                }
                return true;
            }

            return false;
        }

        if (strlen(name) > SYS_USER_NAME_LENGTH)
            throw DataException("SYS.USER$ too long value for NAME (value: '" + std::string(name) + "', length: " +
                    std::to_string(strlen(name)) + ")");
        auto* sysUser = new SysUser(rowId, user, name, spare11, spare12, single, false);
        sysUserMapRowId[rowId] = sysUser;
        sysUserMapUser[user] = sysUser;

        return true;
    }

    void Schema::dictSysCColDrop(typeRowId rowId) {
        auto sysCColIt = sysCColMapRowId.find(rowId);
        if (sysCColIt == sysCColMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing CCOL$ (ROWID: " << rowId << ")")
            return;
        }
        SysCCol* sysCCol = sysCColIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete CCOL$ (ROWID: " << rowId <<
                ", CON#: " << std::dec << sysCCol->con <<
                ", INTCOL#: " << sysCCol->intCol <<
                ", OBJ#: " << sysCCol->obj <<
                ", SPARE1: " << sysCCol->spare1 << ")")
        touched = true;
        sysCColMapRowId.erase(rowId);
        sysCColTouched = true;
        touchTable(sysCCol->obj);
        delete sysCCol;
    }

    void Schema::dictSysCDefDrop(typeRowId rowId) {
        auto sysCDefIt = sysCDefMapRowId.find(rowId);
        if (sysCDefIt == sysCDefMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing CDEF$ (ROWID: " << rowId << ")")
            return;
        }
        SysCDef* sysCDef = sysCDefIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete CDEF$ (ROWID: " << rowId <<
                ", CON#: " << std::dec << sysCDef->con <<
                ", OBJ#: " << sysCDef->obj <<
                ", TYPE: " << sysCDef->type << ")")
        touched = true;
        sysCDefMapRowId.erase(rowId);
        sysCDefTouched = true;
        touchTable(sysCDef->obj);
        delete sysCDef;
    }

    void Schema::dictSysColDrop(typeRowId rowId) {
        auto sysColIt = sysColMapRowId.find(rowId);
        if (sysColIt == sysColMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing COL$ (ROWID: " << rowId << ")")
            return;
        }
        SysCol* sysCol = sysColIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete COL$ (ROWID: " << rowId <<
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
        touched = true;
        sysColMapRowId.erase(rowId);
        sysColTouched = true;
        touchTable(sysCol->obj);
        delete sysCol;
    }

    void Schema::dictSysDeferredStgDrop(typeRowId rowId) {
        auto sysDeferredStgIt = sysDeferredStgMapRowId.find(rowId);
        if (sysDeferredStgIt == sysDeferredStgMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing DEFERRED_STG$ (ROWID: " << rowId << ")")
            return;
        }
        SysDeferredStg* sysDeferredStg = sysDeferredStgIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete DEFERRED_STG$ (ROWID: " << rowId <<
                ", OBJ#: " << std::dec << sysDeferredStg->obj <<
                ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")")
        touched = true;
        sysDeferredStgMapRowId.erase(rowId);
        sysDeferredStgTouched = true;
        touchTable(sysDeferredStg->obj);
        delete sysDeferredStg;
    }

    void Schema::dictSysEColDrop(typeRowId rowId) {
        auto sysEColIt = sysEColMapRowId.find(rowId);
        if (sysEColIt == sysEColMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing ECOL$ (ROWID: " << rowId << ")")
            return;
        }
        SysECol* sysECol = sysEColIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete ECOL$ (ROWID: " << rowId <<
                ", TABOBJ#: " << std::dec << sysECol->tabObj <<
                ", COLNUM: " << sysECol->colNum <<
                ", GUARD_ID: " << sysECol->guardId << ")")
        touched = true;
        sysEColMapRowId.erase(rowId);
        sysEColTouched = true;
        touchTable(sysECol->tabObj);
        delete sysECol;
    }

    void Schema::dictSysLobDrop(typeRowId rowId) {
        auto sysLobIt = sysLobMapRowId.find(rowId);
        if (sysLobIt == sysLobMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing LOB$ (ROWID: " << rowId << ")")
            return;
        }
        SysLob* sysLob = sysLobIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete LOB$ (ROWID: " << rowId <<
                ", OBJ#: " << std::dec << sysLob->obj <<
                ", COL#: " << sysLob->col <<
                ", INTCOL#: " << sysLob->intCol <<
                ", LOBJ#: " << sysLob->lObj <<
                ", TS#: " << sysLob->ts << ")")
        touched = true;
        sysLobMapRowId.erase(rowId);
        sysLobTouched = true;
        touchLob(sysLob->obj);
        delete sysLob;
    }

    void Schema::dictSysLobCompPartDrop(typeRowId rowId) {
        auto sysLobCompPartIt = sysLobCompPartMapRowId.find(rowId);
        if (sysLobCompPartIt == sysLobCompPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing LOBCOMPPART$ (ROWID: " << rowId << ")")
            return;
        }
        SysLobCompPart* sysLobCompPart = sysLobCompPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete LOBCOMPPART$ (ROWID: " << rowId <<
                ", PARTOBJ#: " << std::dec << sysLobCompPart->partObj <<
                ", LOBJ#: " << sysLobCompPart->lObj << ")")
        touched = true;
        sysLobCompPartMapRowId.erase(rowId);
        sysLobCompPartTouched = true;
        touchLob(sysLobCompPart->lObj);
        delete sysLobCompPart;
    }

    void Schema::dictSysLobFragDrop(typeRowId rowId) {
        auto sysLobFragIt = sysLobFragMapRowId.find(rowId);
        if (sysLobFragIt == sysLobFragMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing LOBFRAG$ (ROWID: " << rowId << ")")
            return;
        }
        SysLobFrag* sysLobFrag = sysLobFragIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete LOBFRAG$ (ROWID: " << rowId <<
                ", FRAGOBJ#: " << std::dec << sysLobFrag->fragObj <<
                ", PARENTOBJ#: " << sysLobFrag->parentObj <<
                ", TS#: " << sysLobFrag->ts << ")")
        touched = true;
        sysLobFragMapRowId.erase(rowId);
        sysLobFragTouched = true;
        touchLob(sysLobFrag->parentObj);
        delete sysLobFrag;
    }

    void Schema::dictSysObjDrop(typeRowId rowId) {
        auto sysObjIt = sysObjMapRowId.find(rowId);
        if (sysObjIt == sysObjMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing OBJ$ (ROWID: " << rowId << ")")
            return;
        }
        SysObj* sysObj = sysObjIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete OBJ$ (ROWID: " << rowId <<
                ", OWNER#: " << std::dec << sysObj->owner <<
                ", OBJ#: " << sysObj->obj <<
                ", DATAOBJ#: " << sysObj->dataObj <<
                ", TYPE#: " << sysObj->type <<
                ", NAME: '" << sysObj->name <<
                "', FLAGS: " << sysObj->flags << ")")
        touched = true;
        sysObjMapRowId.erase(rowId);
        sysObjTouched = true;
        touchTable(sysObj->obj);
        delete sysObj;
    }

    void Schema::dictSysTabDrop(typeRowId rowId) {
        auto sysTabIt = sysTabMapRowId.find(rowId);
        if (sysTabIt == sysTabMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing TAB$ (ROWID: " << rowId << ")")
            return;
        }
        SysTab* sysTab = sysTabIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete TAB$ (ROWID: " << rowId <<
                ", OBJ#: " << std::dec << sysTab->obj <<
                ", DATAOBJ#: " << sysTab->dataObj <<
                ", CLUCOLS: " << sysTab->cluCols <<
                ", FLAGS: " << sysTab->flags <<
                ", PROPERTY: " << sysTab->property << ")")
        touched = true;
        sysTabMapRowId.erase(rowId);
        sysTabTouched = true;
        touchTable(sysTab->obj);
        delete sysTab;
    }

    void Schema::dictSysTabComPartDrop(typeRowId rowId) {
        auto sysTabComPartIt = sysTabComPartMapRowId.find(rowId);
        if (sysTabComPartIt == sysTabComPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing TABCOMPART$ (ROWID: " << rowId << ")")
            return;
        }
        SysTabComPart* sysTabComPart = sysTabComPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete TABCOMPART$ (ROWID: " << rowId <<
                ", OBJ#: " << std::dec << sysTabComPart->obj <<
                ", DATAOBJ#: " << sysTabComPart->dataObj <<
                ", BO#: " << sysTabComPart->bo << ")")
        touched = true;
        sysTabComPartMapRowId.erase(rowId);
        sysTabComPartTouched = true;
        touchTable(sysTabComPart->bo);
        delete sysTabComPart;
    }

    void Schema::dictSysTabPartDrop(typeRowId rowId) {
        auto sysTabPartIt = sysTabPartMapRowId.find(rowId);
        if (sysTabPartIt == sysTabPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing TABPART$ (ROWID: " << rowId << ")")
            return;
        }
        SysTabPart* sysTabPart = sysTabPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete TABPART$ (ROWID: " << rowId <<
                ", OBJ#: " << std::dec << sysTabPart->obj <<
                ", DATAOBJ#: " << sysTabPart->dataObj <<
                ", BO#: " << sysTabPart->bo << ")")
        touched = true;
        sysTabPartMapRowId.erase(rowId);
        sysTabPartTouched = true;
        touchTable(sysTabPart->bo);
        delete sysTabPart;
    }

    void Schema::dictSysTabSubPartDrop(typeRowId rowId) {
        auto sysTabSubPartIt = sysTabSubPartMapRowId.find(rowId);
        if (sysTabSubPartIt == sysTabSubPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing TABSUBPART$ (ROWID: " << rowId << ")")
            return;
        }
        SysTabSubPart* sysTabSubPart = sysTabSubPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete TABSUBPART$ (ROWID: " << rowId <<
                ", OBJ#: " << std::dec << sysTabSubPart->obj <<
                ", DATAOBJ#: " << sysTabSubPart->dataObj <<
                ", POBJ#: " << sysTabSubPart->pObj << ")")
        touched = true;
        sysTabSubPartMapRowId.erase(rowId);
        sysTabSubPartTouched = true;
        touchTablePartition(sysTabSubPart->pObj);
        delete sysTabSubPart;
    }

    void Schema::dictSysTsDrop(typeRowId rowId) {
        auto sysTsIt = sysTsMapRowId.find(rowId);
        if (sysTsIt == sysTsMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing TS$ (ROWID: " << rowId << ")")
            return;
        }
        SysTs* sysTs = sysTsIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete TS$ (ROWID: " << rowId <<
                ", TS#: " << std::dec << sysTs->ts <<
                ", NAME: '" << sysTs->name <<
                "', BLOCKSIZE: " << sysTs->blockSize << ")")
        touched = true;
        sysTsMapRowId.erase(rowId);
        sysTsTouched = true;
        delete sysTs;
    }

    void Schema::dictSysUserDrop(typeRowId rowId) {
        auto sysUserIt = sysUserMapRowId.find(rowId);
        if (sysUserIt == sysUserMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing USER$ (ROWID: " << rowId << ")")
            return;
        }
        SysUser* sysUser = sysUserIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete USER$ (ROWID: " << rowId <<
                ", USER#: " << std::dec << sysUser->user <<
                ", NAME: " << sysUser->name <<
                ", SPARE1: " << sysUser->spare1 << ")")
        touched = true;
        sysUserMapRowId.erase(rowId);
        sysUserTouched = true;
        touchUser(sysUser->user);
        delete sysUser;
    }

    SysCCol* Schema::dictSysCColFind(typeRowId rowId) {
        auto sysCColIt = sysCColMapRowId.find(rowId);
        if (sysCColIt != sysCColMapRowId.end())
            return sysCColIt->second;
        else
            return nullptr;
    }

    SysCDef* Schema::dictSysCDefFind(typeRowId rowId) {
        auto sysCDefIt = sysCDefMapRowId.find(rowId);
        if (sysCDefIt != sysCDefMapRowId.end())
            return sysCDefIt->second;
        else
            return nullptr;
    }

    SysCol* Schema::dictSysColFind(typeRowId rowId) {
        auto sysColIt = sysColMapRowId.find(rowId);
        if (sysColIt != sysColMapRowId.end())
            return sysColIt->second;
        else
            return nullptr;
    }

    SysDeferredStg* Schema::dictSysDeferredStgFind(typeRowId rowId) {
        auto sysDeferredStgIt = sysDeferredStgMapRowId.find(rowId);
        if (sysDeferredStgIt != sysDeferredStgMapRowId.end())
            return sysDeferredStgIt->second;
        else
            return nullptr;
    }

    SysECol* Schema::dictSysEColFind(typeRowId rowId) {
        auto sysEColIt = sysEColMapRowId.find(rowId);
        if (sysEColIt != sysEColMapRowId.end())
            return sysEColIt->second;
        else
            return nullptr;
    }

    SysLob* Schema::dictSysLobFind(typeRowId rowId) {
        auto sysLobIt = sysLobMapRowId.find(rowId);
        if (sysLobIt != sysLobMapRowId.end())
            return sysLobIt->second;
        else
            return nullptr;
    }

    SysLobCompPart* Schema::dictSysLobCompPartFind(typeRowId rowId) {
        auto sysLobCompPartIt = sysLobCompPartMapRowId.find(rowId);
        if (sysLobCompPartIt != sysLobCompPartMapRowId.end())
            return sysLobCompPartIt->second;
        else
            return nullptr;
    }

    SysLobFrag* Schema::dictSysLobFragFind(typeRowId rowId) {
        auto sysLobFragIt = sysLobFragMapRowId.find(rowId);
        if (sysLobFragIt != sysLobFragMapRowId.end())
            return sysLobFragIt->second;
        else
            return nullptr;
    }

    SysObj* Schema::dictSysObjFind(typeRowId rowId) {
        auto sysObjIt = sysObjMapRowId.find(rowId);
        if (sysObjIt != sysObjMapRowId.end())
            return sysObjIt->second;
        else
            return nullptr;
    }

    SysTab* Schema::dictSysTabFind(typeRowId rowId) {
        auto sysTabIt = sysTabMapRowId.find(rowId);
        if (sysTabIt != sysTabMapRowId.end())
            return sysTabIt->second;
        else
            return nullptr;
    }

    SysTabComPart* Schema::dictSysTabComPartFind(typeRowId rowId) {
        auto sysTabComPartIt = sysTabComPartMapRowId.find(rowId);
        if (sysTabComPartIt != sysTabComPartMapRowId.end())
            return sysTabComPartIt->second;
        else
            return nullptr;
    }

    SysTabPart* Schema::dictSysTabPartFind(typeRowId rowId) {
        auto sysTabPartIt = sysTabPartMapRowId.find(rowId);
        if (sysTabPartIt != sysTabPartMapRowId.end())
            return sysTabPartIt->second;
        else
            return nullptr;
    }

    SysTabSubPart* Schema::dictSysTabSubPartFind(typeRowId rowId) {
        auto sysTabSubPartIt = sysTabSubPartMapRowId.find(rowId);
        if (sysTabSubPartIt != sysTabSubPartMapRowId.end())
            return sysTabSubPartIt->second;
        else
            return nullptr;
    }

    SysTs* Schema::dictSysTsFind(typeRowId rowId) {
        auto sysTsIt = sysTsMapRowId.find(rowId);
        if (sysTsIt != sysTsMapRowId.end())
            return sysTsIt->second;
        else
            return nullptr;
    }

    SysUser* Schema::dictSysUserFind(typeRowId rowId) {
        auto sysUserIt = sysUserMapRowId.find(rowId);
        if (sysUserIt != sysUserMapRowId.end())
            return sysUserIt->second;
        else
            return nullptr;
    }

    void Schema::touchLob(typeObj obj) {
        if (obj == 0)
            return;

        if (lobsTouched.find(obj) == lobsTouched.end())
            lobsTouched.insert(obj);
    }

    void Schema::touchLobPartition(typeObj obj) {
        if (obj == 0)
            return;

        if (lobPartitionsTouched.find(obj) == lobPartitionsTouched.end())
            lobPartitionsTouched.insert(obj);
    }

    void Schema::touchTable(typeObj obj) {
        if (obj == 0)
            return;

        if (tablesTouched.find(obj) == tablesTouched.end())
            tablesTouched.insert(obj);
    }

    void Schema::touchTablePartition(typeObj obj) {
        if (obj == 0)
            return;

        if (tablePartitionsTouched.find(obj) == tablePartitionsTouched.end())
            tablePartitionsTouched.insert(obj);
    }

    void Schema::touchUser(typeUser user) {
        if (user == 0)
            return;

        if (usersTouched.find(user) == usersTouched.end())
            usersTouched.insert(user);
    }

    OracleTable* Schema::checkTableDict(typeObj obj) {
        auto it = tablePartitionMap.find(obj);
        if (it != tablePartitionMap.end())
            return it->second;

        return nullptr;
    }

    OracleLob* Schema::checkLobDict(typeObj obj) {
        auto it = lobPartitionMap.find(obj);
        if (it != lobPartitionMap.end())
            return it->second;

        return nullptr;
    }

    OracleLob* Schema::checkLobIndexDict(typeObj obj) {
        auto it = lobIndexMap.find(obj);
        if (it != lobIndexMap.end())
            return it->second;

        return nullptr;
    }

    uint32_t Schema::checkLobPageSize(typeObj obj) {
        auto it = lobPageMap.find(obj);
        if (it != lobPageMap.end())
            return it->second;

        return 8132; // default value?
    }

    void Schema::addTableToDict(OracleTable* table) {
        if (tableMap.find(table->obj) != tableMap.end())
            throw ConfigurationException("can't add table (obj: " + std::to_string(table->obj) + ", dataObj: " +
                    std::to_string(table->dataObj) + ")");
        tableMap[table->obj] = table;

        if (tablePartitionMap.find(table->obj) != tablePartitionMap.end())
            throw ConfigurationException("can't add partition (obj: " + std::to_string(table->obj) + ", dataObj: " +
                    std::to_string(table->dataObj) + ")");
        tablePartitionMap[table->obj] = table;

        for (typeObj2 objx : table->tablePartitions) {
            typeObj obj = objx >> 32;
            typeDataObj dataObj = objx & 0xFFFFFFFF;

            if (tablePartitionMap.find(obj) != tablePartitionMap.end())
                throw ConfigurationException("can't add partition element (obj: " + std::to_string(obj) + ", dataObj: " +
                        std::to_string(dataObj) + ")");
            tablePartitionMap[obj] = table;
        }
    }

    void Schema::removeTableFromDict(OracleTable* table) {
        if (tablePartitionMap.find(table->obj) == tablePartitionMap.end())
            throw ConfigurationException("can't remove partition (obj: " + std::to_string(table->obj) + ", dataObj: " +
                    std::to_string(table->dataObj) + ")");
        tablePartitionMap.erase(table->obj);

        for (typeObj2 objx : table->tablePartitions) {
            typeObj obj = objx >> 32;
            typeDataObj dataObj = objx & 0xFFFFFFFF;

            if (tablePartitionMap.find(obj) == tablePartitionMap.end())
                throw ConfigurationException("can't remove table partition element (obj: " + std::to_string(obj) + ", dataObj: " +
                        std::to_string(dataObj) + ")");
            tablePartitionMap.erase(obj);
        }

        for (OracleLob* lob : table->lobs) {
            if (lobMap.find(lob->lObj) == lobMap.end())
                throw ConfigurationException("can't remove lob element (obj: " + std::to_string(lob->obj) + ", intCol: " +
                        std::to_string(lob->intCol) + ", lObjL " + std::to_string(lob->lObj) + ")");
            lobMap.erase(lob->lObj);
        }

        for (typeObj obj : table->lobPartitions) {
            if (lobPartitionMap.find(obj) == lobPartitionMap.end())
                throw ConfigurationException("can't remove lob partition element (obj: " + std::to_string(obj) + ")");
            lobPartitionMap.erase(obj);
        }

        for (typeObj obj : table->lobIndexes) {
            if (lobIndexMap.find(obj) == lobIndexMap.end())
                throw ConfigurationException("can't remove lob index element (obj: " + std::to_string(obj) + ")");
            lobIndexMap.erase(obj);
            lobPageMap.erase(obj);
        }

        if (tableMap.find(table->obj) == tableMap.end())
            throw ConfigurationException("can't remove table (obj: " + std::to_string(table->obj) + ", dataObj: " +
                    std::to_string(table->dataObj) + ")");
        tableMap.erase(table->obj);
        delete table;
    }

    void Schema::addLobToDict(OracleLob* lob, uint16_t pageSize) {
        if (lobMap.find(lob->lObj) != lobMap.end())
            throw ConfigurationException("can't add lob (obj: " + std::to_string(lob->obj) + ", intCol: " + std::to_string(lob->intCol) +
                    ", lObj: " + std::to_string(lob->lObj) + ")");
        lobMap[lob->lObj] = lob;

        if (lobPartitionMap.find(lob->lObj) != lobPartitionMap.end())
            throw ConfigurationException("can't add lob partition (obj: " + std::to_string(lob->obj) + ", intCol: " +
                    std::to_string(lob->intCol) + ", lObj: " + std::to_string(lob->lObj) + ")");
        schemaTable->addLobPartition(lob->lObj);
        lobPartitionMap[lob->lObj] = lob;
        lobPageMap[lob->lObj] = pageSize;
    }

    void Schema::rebuildMaps(std::set<std::string> &msgs) {
        for (typeUser user : usersTouched) {
            for (auto tableMapIt : tableMap) {
                OracleTable* table = tableMapIt.second;
                if (table->user == user)
                    touchTable(table->obj);
            }
        }
        usersTouched.clear();

        for (typeObj obj : lobPartitionsTouched) {
            auto lobPartitionMapIt = lobPartitionMap.find(obj);
            if (lobPartitionMapIt != lobPartitionMap.end()) {
                OracleLob* lob = lobPartitionMapIt->second;
                touchTable(lob->obj);
            }
        }
        lobPartitionsTouched.clear();

        for (typeObj obj : lobsTouched) {
            auto lobMapIt = lobMap.find(obj);
            if (lobMapIt != lobMap.end()) {
                OracleLob* lob = lobMapIt->second;
                touchTable(lob->obj);
            }
        }
        lobsTouched.clear();

        for (typeObj obj : tablePartitionsTouched) {
            auto partitionMapIt = tablePartitionMap.find(obj);
            if (partitionMapIt != tablePartitionMap.end()) {
                OracleTable* table = partitionMapIt->second;
                touchTable(table->obj);
            }
        }
        tablePartitionsTouched.clear();

        for (typeObj obj : tablesTouched) {
            auto tableMapIt = tableMap.find(obj);
            if (tableMapIt != tableMap.end()) {
                OracleTable* table = tableMapIt->second;
                msgs.insert(table->owner + "." + table->name + " (dataobj: " + std::to_string(table->dataObj) +
                            ", obj: " + std::to_string(table->obj) + ") ");
                removeTableFromDict(table);
            }
        }
        tablesTouched.clear();
    }

    void Schema::buildMaps(std::string& owner, std::string& table, std::vector<std::string>& keys, std::string& keysStr, typeOptions options,
                           std::set<std::string> &msgs, bool suppLogDbPrimary, bool suppLogDbAll,
                           uint64_t defaultCharacterMapId, uint64_t defaultCharacterNcharMapId) {
        uint64_t tabCnt = 0;
        std::regex regexOwner(owner);
        std::regex regexTable(table);

        for (auto itObj : sysObjMapRowId) {
            SysObj* sysObj = itObj.second;
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
            uint64_t tablePartitions = 0;

            if (sysTab->isPartitioned()) {
                SysTabPartKey sysTabPartKey(sysObj->obj, 0);
                for (auto itTabPart = sysTabPartMapKey.upper_bound(sysTabPartKey);
                     itTabPart != sysTabPartMapKey.end() && itTabPart->first.bo == sysObj->obj; ++itTabPart) {

                    SysTabPart* sysTabPart = itTabPart->second;
                    schemaTable->addTablePartition(sysTabPart->obj, sysTabPart->dataObj);
                    ++tablePartitions;
                }

                SysTabComPartKey sysTabComPartKey(sysObj->obj, 0);
                for (auto itTabComPart = sysTabComPartMapKey.upper_bound(sysTabComPartKey);
                     itTabComPart != sysTabComPartMapKey.end() && itTabComPart->first.bo == sysObj->obj; ++itTabComPart) {

                    SysTabSubPartKey sysTabSubPartKeyFirst(itTabComPart->second->obj, 0);
                    for (auto itTabSubPart = sysTabSubPartMapKey.upper_bound(sysTabSubPartKeyFirst);
                         itTabSubPart != sysTabSubPartMapKey.end() && itTabSubPart->first.pObj == itTabComPart->second->obj; ++itTabSubPart) {

                        SysTabSubPart* sysTabSubPart = itTabSubPart->second;
                        schemaTable->addTablePartition(sysTabSubPart->obj, sysTabSubPart->dataObj);
                        ++tablePartitions;
                    }
                }
            }

            if (!DISABLE_CHECKS(DISABLE_CHECKS_SUPPLEMENTAL_LOG) && (options & OPTIONS_SYSTEM_TABLE) == 0 &&
                !suppLogDbAll && !sysUser->isSuppLogAll()) {

                SysCDefKey sysCDefKeyFirst(sysObj->obj, 0);
                for (auto itCDef = sysCDefMapKey.upper_bound(sysCDefKeyFirst);
                     itCDef != sysCDefMapKey.end() && itCDef->first.obj == sysObj->obj;
                     ++itCDef) {
                    SysCDef* sysCDef = itCDef->second;
                    if (sysCDef->isSupplementalLogPK())
                        suppLogTablePrimary = true;
                    else if (sysCDef->isSupplementalLogAll())
                        suppLogTableAll = true;
                }
            }

            SysColSeg sysColSegFirst(sysObj->obj, 0);
            for (auto itCol = sysColMapSeg.upper_bound(sysColSegFirst); itCol != sysColMapSeg.end() && itCol->first.obj == sysObj->obj;
                    ++itCol) {
                SysCol* sysCol = itCol->second;
                if (sysCol->segCol == 0)
                    continue;

                uint64_t charmapId = 0;
                typeCol numPk = 0;
                typeCol numSup = 0;
                typeCol guardSeg = -1;

                SysEColKey sysEColKey(sysObj->obj, sysCol->segCol);
                SysECol* sysECol = sysEColMapKey[sysEColKey];
                if (sysECol != nullptr)
                    guardSeg = sysECol->guardId;

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
                    auto it = locales->characterMap.find(charmapId);
                    if (it == locales->characterMap.end()) {
                        ERROR("HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << std::dec << charmapId << ") FROM DUAL;")
                        throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " - unsupported character set id: " +
                                std::to_string(charmapId) + " for column: " + sysObj->name + "." + sysCol->name);
                    }
                }

                SysCColKey sysCColKeyFirst(sysObj->obj, sysCol->intCol, 0);
                for (auto itCCol = sysCColMapKey.upper_bound(sysCColKeyFirst);
                     itCCol != sysCColMapKey.end() && itCCol->first.obj == sysObj->obj && itCCol->first.intCol == sysCol->intCol;
                     ++itCCol) {
                    SysCCol* sysCCol = itCCol->second;

                    // Count number of PK the column is part of
                    auto it = sysCDefMapCon.find(sysCCol->con);
                    if (it == sysCDefMapCon.end()) {
                        WARNING("SYS.CDEF$ missing for CON: " << sysCCol->con)
                        continue;
                    }
                    SysCDef* sysCDef = it->second;
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
                    for (auto & key : keys) {
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

                schemaColumn = new OracleColumn(sysCol->col, guardSeg, sysCol->segCol, sysCol->name, sysCol->type,
                                                sysCol->length, sysCol->precision, sysCol->scale, numPk,
                                                charmapId, (sysCol->null_ == 0), sysCol->isInvisible(),
                                                sysCol->isStoredAsLob(), sysCol->isConstraint(), sysCol->isNested(),
                                                sysCol->isUnused(), sysCol->isAdded(), sysCol->isGuard());

                schemaTable->addColumn(schemaColumn);
                schemaColumn = nullptr;
            }

            if ((options & OPTIONS_SYSTEM_TABLE) == 0) {
                SysLobKey sysLobKeyFirst(sysObj->obj, 0);
                for (auto itLob = sysLobMapKey.upper_bound(sysLobKeyFirst);
                     itLob != sysLobMapKey.end() && itLob->first.obj == sysObj->obj; ++itLob) {
                    SysLob* sysLob = itLob->second;

                    if (ctx->trace >= TRACE_DEBUG)
                        msgs.insert(
                                "- lob: " + std::to_string(sysLob->col) + ":" + std::to_string(sysLob->intCol) + ":" +
                                std::to_string(sysLob->lObj));

                    schemaLob = new OracleLob(schemaTable, sysLob->obj, sysLob->col, sysLob->intCol, sysLob->lObj);

                    // indexes
                    std::ostringstream str;
                    str << "SYS_IL" << std::setw(10) << std::setfill('0') << sysObj->obj << "C" << std::setw(5)
                        << std::setfill('0') << sysLob->intCol << "$$";
                    std::string lobIndexName = str.str();

                    SysObjNameKey sysObjNameKeyFirst(sysObj->owner, lobIndexName.c_str(), 0);
                    for (auto objNameLobIt = sysObjMapName.upper_bound(sysObjNameKeyFirst);
                         objNameLobIt != sysObjMapName.end() &&
                         objNameLobIt->first.name == lobIndexName &&
                         objNameLobIt->first.owner == sysObj->owner; ++objNameLobIt) {
                        schemaLob->addIndex(objNameLobIt->first.obj);
                        schemaTable->addLobIndex(objNameLobIt->first.obj);
                        lobIndexMap[objNameLobIt->first.obj] = schemaLob;
                        ++lobIndexes;
                    }

                    if (schemaLob->lobIndexes.size() == 0) {
                        WARNING("missing LOB index for LOB (OBJ#:" + std::to_string(sysObj->obj) + ", OBJ#" +
                                std::to_string(sysLob->lObj) + ", COL#:" +
                                std::to_string(sysLob->intCol) + ")")
                    }

                    // partitioned lob
                    if (sysTab->isPartitioned()) {
                        // partitions
                        SysLobFragKey sysLobFragKey(sysLob->lObj, 0);
                        for (auto itLobFrag = sysLobFragMapKey.upper_bound(sysLobFragKey);
                             itLobFrag != sysLobFragMapKey.end() &&
                             itLobFrag->first.parentObj == sysLob->lObj; ++itLobFrag) {

                            SysLobFrag* sysLobFrag = itLobFrag->second;
                            schemaTable->addLobPartition(sysLobFrag->fragObj);
                            lobPartitionMap[sysLobFrag->fragObj] = schemaLob;
                            lobPageMap[sysLobFrag->fragObj] = getLobBlockSize(sysLobFrag->ts);
                            ++lobPartitions;
                        }

                        // subpartitions
                        SysLobCompPartKey sysLobCompPartKey(sysLob->lObj, 0);
                        for (auto itLobCompPart = sysLobCompPartMapKey.upper_bound(sysLobCompPartKey);
                             itLobCompPart != sysLobCompPartMapKey.end() &&
                             itLobCompPart->first.lObj == sysLob->lObj; ++itLobCompPart) {

                            SysLobCompPart* sysLobCompPart = itLobCompPart->second;

                            SysLobFragKey sysLobFragKey2(sysLobCompPart->partObj, 0);
                            for (auto itLobFrag = sysLobFragMapKey.upper_bound(sysLobFragKey2);
                                 itLobFrag != sysLobFragMapKey.end() &&
                                 itLobFrag->first.parentObj == sysLobCompPart->partObj; ++itLobFrag) {

                                SysLobFrag* sysLobFrag = itLobFrag->second;
                                schemaTable->addLobPartition(sysLobFrag->fragObj);
                                lobPartitionMap[sysLobFrag->fragObj] = schemaLob;
                                ++lobPartitions;
                            }
                        }
                    }

                    addLobToDict(schemaLob, getLobBlockSize(sysLob->ts));
                    schemaTable->addLob(schemaLob);
                    schemaLob = nullptr;
                }
            }

            // Check if table has all listed columns
            if ((typeCol)keys.size() != keysCnt)
                throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find all column set (" + keysStr + ")");

            std::stringstream ss;
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << std::dec << sysTab->dataObj << ", obj: " << std::dec << sysObj->obj <<
                    ", columns: " << std::dec << schemaTable->maxSegCol << ", lobs: " << std::dec << schemaTable->totalLobs << ", lob-idx: " <<
                    std::dec << lobIndexes << ")";
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
        auto sysTsIt = sysTsMapTs.find(ts);
        if (sysTsIt != sysTsMapTs.end()) {
            uint32_t pageSize = sysTsIt->second->blockSize;
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
