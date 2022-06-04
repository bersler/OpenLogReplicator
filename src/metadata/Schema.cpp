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
#include "../common/OracleObject.h"
#include "../common/SysCCol.h"
#include "../common/SysCDef.h"
#include "../common/SysCol.h"
#include "../common/SysDeferredStg.h"
#include "../common/SysECol.h"
#include "../common/SysObj.h"
#include "../common/SysTab.h"
#include "../common/SysTabComPart.h"
#include "../common/SysTabPart.h"
#include "../common/SysTabSubPart.h"
#include "../common/SysUser.h"
#include "../locales/Locales.h"
#include "Schema.h"

namespace OpenLogReplicator {
    Schema::Schema(Ctx* ctx, Locales* locales) :
            ctx(ctx),
            locales(locales),
            sysUserAdaptive(sysUserRowId, 0, "", 0, 0, false, false),
            scn(ZERO_SCN),
            refScn(ZERO_SCN),
            loaded(false),
            schemaObject(nullptr),
            schemaColumn(nullptr),
            sysCColTouched(false),
            sysCDefTouched(false),
            sysColTouched(false),
            sysDeferredStgTouched(false),
            sysEColTouched(false),
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
    };

    void Schema::purge() {
        scn = ZERO_SCN;
        if (schemaObject != nullptr) {
            delete schemaObject;
            schemaObject = nullptr;
        }

        if (schemaColumn != nullptr) {
            delete schemaColumn;
            schemaColumn = nullptr;
        }

        for (auto it : objectMap) {
            OracleObject* object = it.second;
            delete object;
        }
        objectMap.clear();

        partitionMap.clear();

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

        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;
            delete sysObj;
        }
        sysObjMapRowId.clear();
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

        for (auto it : sysUserMapRowId) {
            SysUser* sysUser = it.second;
            delete sysUser;
        }
        sysUserMapRowId.clear();
        sysUserMapUser.clear();
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
                msgs.assign("schema mismatch: SYS.DEFERREDSTG$ lost ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            } else if (*sysDeferredStg != *(sysDeferredStgIt->second)) {
                msgs.assign("schema mismatch: SYS.DEFERREDSTG$ differs ROWID: " + sysDeferredStg->rowId.toString());
                return false;
            }
        }
        for (auto it : otherSchema->sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;
            auto sysDeferredStgIt = sysDeferredStgMapRowId.find(sysDeferredStg->rowId);
            if (sysDeferredStgIt == sysDeferredStgMapRowId.end()) {
                msgs.assign("schema mismatch: SYS.DEFERREDSTG$ lost ROWID: " + sysDeferredStg->rowId.toString());
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
        if (!compareSysCCol(otherSchema, msgs) ||
            !compareSysCDef(otherSchema, msgs) ||
            !compareSysCol(otherSchema, msgs) ||
            !compareSysDeferredStg(otherSchema, msgs) ||
            !compareSysECol(otherSchema, msgs) ||
            !compareSysObj(otherSchema, msgs) ||
            !compareSysTab(otherSchema, msgs) ||
            !compareSysTabComPart(otherSchema, msgs) ||
            !compareSysTabPart(otherSchema, msgs) ||
            !compareSysTabSubPart(otherSchema, msgs) ||
            !compareSysUser(otherSchema, msgs))
            return false;

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
                    touchObj(sysCCol->obj);
                    sysCCol->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CCOL$ (rowid: " << it.first << ", CON#: " << std::dec << sysCCol->con << ", INTCOL#: " <<
                                                                  sysCCol->intCol << ", OBJ#: " << sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")")
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
                    touchObj(sysCDef->obj);
                    sysCDef->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CDEF$ (rowid: " << it.first << ", CON#: " << std::dec << sysCDef->con << ", OBJ#: " <<
                                                                  sysCDef->obj << ", type: " << sysCDef->type << ")")
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
                    touchObj(sysCol->obj);
                    sysCol->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage COL$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysCol->obj << ", COL#: " << sysCol->col <<
                                                                 ", SEGCOL#: " << sysCol->segCol << ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " <<
                                                                 sysCol->type << ", LENGTH: " << sysCol->length << ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale <<
                                                                 ", CHARSETFORM: " << sysCol->charsetForm << ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ <<
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
                    touchObj(sysDeferredStg->obj);
                    sysDeferredStg->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage DEFERRED_STG$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysDeferredStg->obj <<
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
                    touchObj(sysECol->tabObj);
                    sysECol->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage ECOL$ (rowid: " << it.first << ", TABOBJ#: " << std::dec << sysECol->tabObj << ", COLNUM: " <<
                                                                  sysECol->colNum << ", GUARD_ID: " << sysECol->guardId << ")")
            removeRowId.push_back(it.first);
            delete sysECol;
        }

        for (typeRowId rowId: removeRowId)
            sysEColMapRowId.erase(rowId);
        sysEColTouched = false;
    }

    void Schema::refreshIndexesSysObj() {
        if (!sysObjTouched)
            return;
        sysObjMapObj.clear();

        std::list<typeRowId> removeRowId;
        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                sysObjMapObj[sysObj->obj] = sysObj;
                if (sysObj->touched) {
                    touchObj(sysObj->obj);
                    sysObj->touched = false;
                }
                continue;
            }

            auto sysUserIt = sysUserMapUser.find(sysObj->owner);
            if (sysUserIt != sysUserMapUser.end()) {
                SysUser* sysUser = sysUserIt->second;
                if (!sysUser->single || sysObj->single) {
                    sysObjMapObj[sysObj->obj] = sysObj;
                    if (sysObj->touched) {
                        touchObj(sysObj->obj);
                        sysObj->touched = false;
                    }
                    continue;
                }
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage OBJ$ (rowid: " << it.first << ", OWNER#: " << std::dec << sysObj->owner << ", OBJ#: " <<
                                                                 sysObj->obj << ", DATAOBJ#: " << sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name <<
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
                    touchObj(sysTab->obj);
                    sysTab->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TAB$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTab->obj << ", DATAOBJ#: " <<
                                                                 sysTab->dataObj << ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " << sysTab->flags << ", PROPERTY: " << sysTab->property << ")")
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
            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysTabComPart->obj) != sysObjMapObj.end()) {
                SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
                sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
                if (sysTabComPart->touched) {
                    touchObj(sysTabComPart->bo);
                    sysTabComPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABCOMPART$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTabComPart->obj << ", DATAOBJ#: " <<
                                                                        sysTabComPart->dataObj << ", BO#: " << sysTabComPart->bo << ")")
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
                    touchObj(sysTabPart->bo);
                    sysTabPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABPART$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTabPart->obj << ", DATAOBJ#: " <<
                                                                     sysTabPart->dataObj << ", BO#: " << sysTabPart->bo << ")")
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

            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) || sysObjMapObj.find(sysTabSubPart->obj) != sysObjMapObj.end()) {
                SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
                sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;
                if (sysTabSubPart->touched) {
                    //find SYS.TABCOMPART$
                    auto it2 = sysTabComPartMapObj.find(sysTabSubPart->pObj);
                    if (it2 != sysTabComPartMapObj.end()) {
                        SysTabComPart* sysTabComPart = it2->second;
                        touchObj(sysTabComPart->bo);
                    }
                    sysTabSubPart->touched = false;
                }
                continue;
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABSUBPART$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTabSubPart->obj << ", DATAOBJ#: " <<
                                                                        sysTabSubPart->dataObj << ", POBJ#: " << sysTabSubPart->pObj << ")")
            removeRowId.push_back(it.first);
            delete sysTabSubPart;
        }

        for (typeRowId rowId: removeRowId)
            sysTabSubPartMapRowId.erase(rowId);
        sysTabSubPartTouched = false;
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

            TRACE(TRACE2_SYSTEM, "SYSTEM: garbage USER$ (rowid: " << it.first << ", USER#: " << std::dec << sysUser->user << ", NAME: " <<
                                                                  sysUser->name << ", SPARE1: " << sysUser->spare1 << ")")
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
        refreshIndexesSysObj();
        refreshIndexesSysTab();
        refreshIndexesSysTabComPart();
        refreshIndexesSysTabPart();
        refreshIndexesSysTabSubPart();
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

        if (strlen(name) > SYSCOL_NAME_LENGTH)
            throw DataException("SYS.COL$ too long value for NAME (value: '" + std::string(name) + "', length: " + std::to_string(strlen(name)) + ")");
        auto* sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId,
                                  null_, property1, property2, false);
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

        if (strlen(name) > SYSOBJ_NAME_LENGTH)
            throw DataException("SYS.OBJ$ too long value for NAME (value: '" + std::string(name) + "', length: " + std::to_string(strlen(name)) + ")");
        auto* sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2, single, false);
        sysObjMapRowId[rowId] = sysObj;
        sysObjMapObj[obj] = sysObj;

        return true;
    }

    bool Schema::dictSysTabAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeCol cluCols, uint64_t flags1, uint64_t flags2,
                               uint64_t property1, uint64_t property2) {
        typeRowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end())
            return false;

        auto* sysTab = new SysTab(rowId, obj, dataObj, cluCols, flags1, flags2, property1, property2, false);
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

        if (strlen(name) > SYSUSER_NAME_LENGTH)
            throw DataException("SYS.USER$ too long value for NAME (value: '" + std::string(name) + "', length: " + std::to_string(strlen(name)) + ")");
        auto* sysUser = new SysUser(rowId, user, name, spare11, spare12, single, false);
        sysUserMapRowId[rowId] = sysUser;
        sysUserMapUser[user] = sysUser;

        return true;
    }

    void Schema::dictSysCColDrop(typeRowId rowId) {
        auto sysCColIt = sysCColMapRowId.find(rowId);
        if (sysCColIt == sysCColMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysCCol* sysCCol = sysCColIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << std::dec << sysCCol->con << ", INTCOL#: " << sysCCol->intCol << ", OBJ#: " <<
                                                      sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")")
        touched = true;
        sysCColMapRowId.erase(rowId);
        sysCColTouched = true;
        touchObj(sysCCol->obj);
        delete sysCCol;
    }

    void Schema::dictSysCDefDrop(typeRowId rowId) {
        auto sysCDefIt = sysCDefMapRowId.find(rowId);
        if (sysCDefIt == sysCDefMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysCDef* sysCDef = sysCDefIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << std::dec << sysCDef->con << ", OBJ#: " << sysCDef->obj << ", type: " << sysCDef->type << ")")
        touched = true;
        sysCDefMapRowId.erase(rowId);
        sysCDefTouched = true;
        touchObj(sysCDef->obj);
        delete sysCDef;
    }

    void Schema::dictSysColDrop(typeRowId rowId) {
        auto sysColIt = sysColMapRowId.find(rowId);
        if (sysColIt == sysColMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysCol* sysCol = sysColIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << std::dec << sysCol->obj << ", COL#: " << sysCol->col << ", SEGCOL#: " << sysCol->segCol <<
                                                      ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " << sysCol->type << ", LENGTH: " << sysCol->length <<
                                                      ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale << ", CHARSETFORM: " << sysCol->charsetForm <<
                                                      ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ << ", PROPERTY: " << sysCol->property << ")")
        touched = true;
        sysColMapRowId.erase(rowId);
        sysColTouched = true;
        touchObj(sysCol->obj);
        delete sysCol;
    }

    void Schema::dictSysDeferredStgDrop(typeRowId rowId) {
        auto sysDeferredStgIt = sysDeferredStgMapRowId.find(rowId);
        if (sysDeferredStgIt == sysDeferredStgMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysDeferredStg* sysDeferredStg = sysDeferredStgIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << std::dec << sysDeferredStg->obj << ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")")
        touched = true;
        sysDeferredStgMapRowId.erase(rowId);
        sysDeferredStgTouched = true;
        touchObj(sysDeferredStg->obj);
        delete sysDeferredStg;
    }

    void Schema::dictSysEColDrop(typeRowId rowId) {
        auto sysEColIt = sysEColMapRowId.find(rowId);
        if (sysEColIt == sysEColMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysECol* sysECol = sysEColIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (TABOBJ#: " << std::dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum <<
                                                         ", GUARD_ID: " << sysECol->guardId << ")")
        touched = true;
        sysEColMapRowId.erase(rowId);
        sysEColTouched = true;
        touchObj(sysECol->tabObj);
        delete sysECol;
    }

    void Schema::dictSysObjDrop(typeRowId rowId) {
        auto sysObjIt = sysObjMapRowId.find(rowId);
        if (sysObjIt == sysObjMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysObj* sysObj = sysObjIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OWNER#: " << std::dec << sysObj->owner << ", OBJ#: " << sysObj->obj <<
                                                        ", DATAOBJ#: " << sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name << "', FLAGS: " << sysObj->flags << ")")
        touched = true;
        sysObjMapRowId.erase(rowId);
        sysObjTouched = true;
        touchObj(sysObj->obj);
        delete sysObj;
    }

    void Schema::dictSysTabDrop(typeRowId rowId) {
        auto sysTabIt = sysTabMapRowId.find(rowId);
        if (sysTabIt == sysTabMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysTab* sysTab = sysTabIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << std::dec << sysTab->obj << ", DATAOBJ#: " << sysTab->dataObj <<
                                                      ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " << sysTab->flags << ", PROPERTY: " << sysTab->property << ")")
        touched = true;
        sysTabMapRowId.erase(rowId);
        sysTabTouched = true;
        touchObj(sysTab->obj);
        delete sysTab;
    }

    void Schema::dictSysTabComPartDrop(typeRowId rowId) {
        auto sysTabComPartIt = sysTabComPartMapRowId.find(rowId);
        if (sysTabComPartIt == sysTabComPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysTabComPart* sysTabComPart = sysTabComPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << std::dec << sysTabComPart->obj << ", DATAOBJ#: " << sysTabComPart->dataObj <<
                                                      ", BO#: " << sysTabComPart->bo << ")")
        touched = true;
        sysTabComPartMapRowId.erase(rowId);
        sysTabComPartTouched = true;
        touchObj(sysTabComPart->bo);
        delete sysTabComPart;
    }

    void Schema::dictSysTabPartDrop(typeRowId rowId) {
        auto sysTabPartIt = sysTabPartMapRowId.find(rowId);
        if (sysTabPartIt == sysTabPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysTabPart* sysTabPart = sysTabPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << std::dec << sysTabPart->obj << ", DATAOBJ#: " << sysTabPart->dataObj <<
                                                      ", BO#: " << sysTabPart->bo << ")")
        touched = true;
        sysTabPartMapRowId.erase(rowId);
        sysTabPartTouched = true;
        touchObj(sysTabPart->bo);
        delete sysTabPart;
    }

    void Schema::dictSysTabSubPartDrop(typeRowId rowId) {
        auto sysTabSubPartIt = sysTabSubPartMapRowId.find(rowId);
        if (sysTabSubPartIt == sysTabSubPartMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysTabSubPart* sysTabSubPart = sysTabSubPartIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << std::dec << sysTabSubPart->obj << ", DATAOBJ#: " << sysTabSubPart->dataObj <<
                                                      ", POBJ#: " << sysTabSubPart->pObj << ")")
        touched = true;
        sysTabSubPartMapRowId.erase(rowId);
        sysTabSubPartTouched = true;
        touchPart(sysTabSubPart->pObj);
        delete sysTabSubPart;
    }

    void Schema::dictSysUserDrop(typeRowId rowId) {
        auto sysUserIt = sysUserMapRowId.find(rowId);
        if (sysUserIt == sysUserMapRowId.end()) {
            TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
            return;
        }
        SysUser* sysUser = sysUserIt->second;
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete (USER#: " << std::dec << sysUser->user << ", NAME: " << sysUser->name <<
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

    SysUser* Schema::dictSysUserFind(typeRowId rowId) {
        auto sysUserIt = sysUserMapRowId.find(rowId);
        if (sysUserIt != sysUserMapRowId.end())
            return sysUserIt->second;
        else
            return nullptr;
    }

    void Schema::touchObj(typeObj obj) {
        if (obj == 0)
            return;

        if (objectsTouched.find(obj) == objectsTouched.end())
            objectsTouched.insert(obj);
    }

    void Schema::touchPart(typeObj obj) {
        if (obj == 0)
            return;

        if (partitionsTouched.find(obj) == partitionsTouched.end())
            partitionsTouched.insert(obj);
    }

    void Schema::touchUser(typeUser user) {
        if (user == 0)
            return;

        if (usersTouched.find(user) == usersTouched.end())
            usersTouched.insert(user);
    }

    OracleObject* Schema::checkDict(typeObj obj, typeDataObj dataObj __attribute__((unused))) {
        auto it = partitionMap.find(obj);
        if (it == partitionMap.end())
            return nullptr;
        return it->second;
    }

    void Schema::addToDict(OracleObject* object) {
        if (objectMap.find(object->obj) != objectMap.end())
            throw ConfigurationException("can't add object (obj: " + std::to_string(object->obj) + ", dataObj: " +
                                         std::to_string(object->dataObj) + ")");
        objectMap[object->obj] = object;

        if (partitionMap.find(object->obj) != partitionMap.end())
            throw ConfigurationException("can't add partition (obj: " + std::to_string(object->obj) + ", dataObj: " +
                                         std::to_string(object->dataObj) + ")");
        partitionMap[object->obj] = object;

        for (typeObj2 objx : object->partitions) {
            typeObj partitionObj = objx >> 32;
            typeDataObj partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap.find(partitionObj) != partitionMap.end())
                throw ConfigurationException("can't add partition element (obj: " + std::to_string(partitionObj) + ", dataObj: " +
                                             std::to_string(partitionDataObj) + ")");
            partitionMap[partitionObj] = object;
        }
    }

    void Schema::removeFromDict(OracleObject* object) {
        if (partitionMap.find(object->obj) == partitionMap.end())
            throw ConfigurationException("can't remove partition (obj: " + std::to_string(object->obj) + ", dataObj: " +
                                         std::to_string(object->dataObj) + ")");
        partitionMap.erase(object->obj);

        for (typeObj2 objx : object->partitions) {
            typeObj partitionObj = objx >> 32;
            typeDataObj partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap.find(partitionObj) == partitionMap.end())
                throw ConfigurationException("can't remove partition element (obj: " + std::to_string(partitionObj) + ", dataObj: " +
                                             std::to_string(partitionDataObj) + ")");
            partitionMap.erase(partitionObj);
        }
    }

    void Schema::rebuildMaps(std::set<std::string> &msgs) {
        for (typeUser user : usersTouched) {
            for (auto it = objectMap.cbegin(); it != objectMap.cend() ; ) {
                OracleObject* object = it->second;
                if (object->user == user) {
                    removeFromDict(object);
                    msgs.insert(object->owner + "." + object->name + " (dataobj: " + std::to_string(object->dataObj) +
                                ", obj: " + std::to_string(object->obj) + ") ");
                    objectMap.erase(it++);
                    delete object;
                } else {
                    ++it;
                }
            }
        }
        usersTouched.clear();

        for (typeObj obj : partitionsTouched) {
            auto objectMapIt = partitionMap.find(obj);
            if (objectMapIt != partitionMap.end()) {
                OracleObject* object = objectMapIt->second;
                touchObj(object->obj);
            }
        }
        partitionsTouched.clear();

        for (typeObj obj : objectsTouched) {
            auto objectMapIt = objectMap.find(obj);
            if (objectMapIt != objectMap.end()) {
                OracleObject* object = objectMapIt->second;
                removeFromDict(object);
                msgs.insert(object->owner + "." + object->name + " (dataobj: " + std::to_string(object->dataObj) +
                            ", obj: " + std::to_string(object->obj) + ") ");
                objectMap.erase(obj);
                delete object;
            }
        }
        objectsTouched.clear();
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

            //table already added with another rule
            if (objectMap.find(sysObj->obj) != objectMap.end()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - already added (skipped)");
                continue;
            }

            //object without SYS.TAB$
            auto sysTabMapObjIt = sysTabMapObj.find(sysObj->obj);
            if (sysTabMapObjIt == sysTabMapObj.end()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - SYS.TAB$ entry missing (skipped)");
                continue;
            }
            SysTab* sysTab = sysTabMapObjIt->second;

            //skip binary objects
            if (sysTab->isBinary()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - binary (skipped");
                continue;
            }

            //skip Index Organized Tables (IOT)
            if (sysTab->isIot()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - IOT (skipped)");
                continue;
            }

            //skip temporary tables
            if (sysObj->isTemporary()) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - temporary table (skipped)");
                continue;
            }

            //skip nested tables
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
            //skip compressed tables
            if (compressed) {
                if (ctx->trace >= TRACE_DEBUG)
                    msgs.insert(sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - compressed table (skipped)");
                continue;
            }

            typeCol keysCnt = 0;
            bool suppLogTablePrimary = false;
            bool suppLogTableAll = false;
            bool supLogColMissing = false;

            schemaObject = new OracleObject(sysObj->obj, sysTab->dataObj, sysObj->owner, sysTab->cluCols,
                                            options, sysUser->name, sysObj->name);
            ++tabCnt;

            uint64_t partitions = 0;
            if (sysTab->isPartitioned()) {
                SysTabPartKey sysTabPartKey(sysObj->obj, 0);
                for (auto itTabPart = sysTabPartMapKey.upper_bound(sysTabPartKey);
                     itTabPart != sysTabPartMapKey.end() && itTabPart->first.bo == sysObj->obj; ++itTabPart) {

                    SysTabPart* sysTabPart = itTabPart->second;
                    schemaObject->addPartition(sysTabPart->obj, sysTabPart->dataObj);
                    ++partitions;
                }

                SysTabComPartKey sysTabComPartKey(sysObj->obj, 0);
                for (auto itTabComPart = sysTabComPartMapKey.upper_bound(sysTabComPartKey);
                     itTabComPart != sysTabComPartMapKey.end() && itTabComPart->first.bo == sysObj->obj; ++itTabComPart) {

                    SysTabSubPartKey sysTabSubPartKeyFirst(itTabComPart->second->obj, 0);
                    for (auto itTabSubPart = sysTabSubPartMapKey.upper_bound(sysTabSubPartKeyFirst);
                         itTabSubPart != sysTabSubPartMapKey.end() && itTabSubPart->first.pObj == itTabComPart->second->obj; ++itTabSubPart) {

                        SysTabSubPart* sysTabSubPart = itTabSubPart->second;
                        schemaObject->addPartition(sysTabSubPart->obj, sysTabSubPart->dataObj);
                        ++partitions;
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
            for (auto itCol = sysColMapSeg.upper_bound(sysColSegFirst);
                 itCol != sysColMapSeg.end() && itCol->first.obj == sysObj->obj; ++itCol) {

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

                if (sysCol->charsetForm == 1)
                    charmapId = defaultCharacterMapId;
                else if (sysCol->charsetForm == 2)
                    charmapId = defaultCharacterNcharMapId;
                else
                    charmapId = sysCol->charsetId;

                //check character set for char and varchar2
                if (sysCol->type == 1 || sysCol->type == 96) {
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

                    auto it = sysCDefMapCon.find(sysCCol->con);
                    //count number of PK the column is part of
                    if (it == sysCDefMapCon.end()) {
                        WARNING("SYS.CDEF$ missing for CON: " << sysCCol->con)
                        continue;
                    }
                    SysCDef* sysCDef = it->second;
                    if (sysCDef->isPK())
                        ++numPk;

                    //supplemental logging
                    if (sysCCol->spare1.isZero() && sysCDef->isSupplementalLog())
                        ++numSup;
                }

                //part of defined primary key
                if (!keys.empty()) {
                    //manually defined pk overlaps with table pk
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
                                                sysCol->length, sysCol->precision, sysCol->scale, numPk, charmapId,
                                                (sysCol->null_ == 0), sysCol->isInvisible(), sysCol->isStoredAsLob(),
                                                sysCol->isConstraint(), sysCol->isNested(), sysCol->isUnused(),
                                                sysCol->isAdded(), sysCol->isGuard());

                schemaObject->addColumn(schemaColumn);
                schemaColumn = nullptr;
            }

            //check if table has all listed columns
            if ((typeCol)keys.size() != keysCnt)
                throw DataException("table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find all column set (" + keysStr + ")");

            std::stringstream ss;
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << std::dec << sysTab->dataObj << ", obj: " << std::dec << sysObj->obj
               << ", columns: " << std::dec << schemaObject->maxSegCol << ")";
            if (sysTab->isClustered())
                ss << ", part of cluster";
            if (sysTab->isPartitioned())
                ss << ", partitioned(" << std::dec << partitions << ")";
            if (sysTab->isDependencies())
                ss << ", row dependencies";
            if (sysTab->isRowMovement())
                ss << ", row movement enabled";

            if (!DISABLE_CHECKS(DISABLE_CHECKS_SUPPLEMENTAL_LOG) && (options & OPTIONS_SYSTEM_TABLE) == 0) {
                //use default primary key
                if (keys.empty()) {
                    if (schemaObject->totalPk == 0)
                        ss << ", primary key missing";
                    else if (!suppLogTablePrimary && !suppLogTableAll && !sysUser->isSuppLogPrimary() && !sysUser->isSuppLogAll() &&
                             !suppLogDbPrimary && !suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;";
                    //user defined primary key
                } else {
                    if (!suppLogTableAll && !sysUser->isSuppLogAll() && !suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG GROUP GRP" << std::dec << sysObj->obj << " (" << keysStr << ") ALWAYS;";
                }
            }
            msgs.insert(ss.str());

            addToDict(schemaObject);
            schemaObject = nullptr;
        }
    }
}
