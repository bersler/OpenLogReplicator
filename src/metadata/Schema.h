/* Header for Schema class
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

#ifndef SCHEMA_H_
#define SCHEMA_H_

#include <list>
#include <map>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <unordered_map>
#include <vector>

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
#include "../common/table/TablePack.h"
#include "../common/table/XdbTtSet.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXPt.h"
#include "../common/table/XdbXQn.h"
#include "../common/types/RowId.h"
#include "../common/types/Types.h"
#include "../common/types/Xid.h"
#include "SchemaElement.h"

namespace OpenLogReplicator {
    class Ctx;
    class DbColumn;
    class DbLob;
    class DbTable;
    class Locales;
    class XmlCtx;

    class Schema final {
    protected:
        Ctx* ctx;
        Locales* locales;
        RowId sysUserRowId;
        SysUser sysUserAdaptive;

        void addTableToDict(DbTable* table);
        void removeTableFromDict(const DbTable* table);
        [[nodiscard]] uint16_t getLobBlockSize(typeTs ts) const;

    public:
        Scn scn{Scn::none()};
        Scn refScn{Scn::none()};
        bool loaded{false};

        std::unordered_map<typeDataObj, DbLob*> lobPartitionMap;
        std::unordered_map<typeDataObj, DbLob*> lobIndexMap;
        std::unordered_map<typeObj, DbTable*> tableMap;
        std::unordered_map<typeObj, DbTable*> tablePartitionMap;
        XmlCtx* xmlCtxDefault{nullptr};
        DbColumn* columnTmp{nullptr};
        DbLob* lobTmp{nullptr};
        DbTable* tableTmp{nullptr};
        std::set<DbTable*> tablesTouched;
        std::set<typeObj> identifiersTouched;
        bool touched{false};

        TablePack<SysCCol, SysCColKey> sysCColPack;
        TablePack<SysCDef, SysCDefKey, SysCDefCon> sysCDefPack;
        TablePack<SysCol, SysColSeg> sysColPack;
        TablePack<SysDeferredStg, TabRowIdKeyDefault, SysDeferredStgObj> sysDeferredStgPack;
        TablePack<SysECol, TabRowIdKeyDefault, SysEColKey> sysEColPack;
        TablePack<SysLob, SysLobKey, SysLobLObj> sysLobPack;
        TablePack<SysLobCompPart, SysLobCompPartKey, SysLobCompPartPartObj> sysLobCompPartPack;
        TablePack<SysLobFrag, SysLobFragKey> sysLobFragPack;
        TablePack<SysObj, SysObjNameKey, SysObjObj> sysObjPack;
        TablePack<SysTab, TabRowIdKeyDefault, SysTabObj> sysTabPack;
        TablePack<SysTabComPart, SysTabComPartKey, SysTabComPartObj> sysTabComPartPack;
        TablePack<SysTabPart, SysTabPartKey> sysTabPartPack;
        TablePack<SysTabSubPart, SysTabSubPartKey> sysTabSubPartPack;
        TablePack<SysTs, TabRowIdKeyDefault, SysTsTs> sysTsPack;
        TablePack<SysUser, TabRowIdKeyDefault, SysUserUser> sysUserPack;
        TablePack<XdbTtSet, TabRowIdKeyDefault, XdbTtSetTokSuf> xdbTtSetPack;

        // XDB.X$yyxxx
        std::map<std::string, XmlCtx*> schemaXmlMap;

        Schema(Ctx* newCtx, Locales* newLocales);
        ~Schema();

        void purgeMetadata();
        void purgeDicts();
        [[nodiscard]] bool compare(Schema* otherSchema, std::string& msgs) const;

        void touchTable(typeObj obj);
        void touchTableLob(typeObj lobObj);
        void touchTableLobFrag(typeObj lobFragObj);
        void touchTablePart(typeObj obj);
        [[nodiscard]] DbTable* checkTableDict(typeObj obj) const;
        [[nodiscard]] bool checkTableDictUncommitted(typeObj obj, std::string& owner, std::string& table) const;
        [[nodiscard]] DbLob* checkLobDict(typeDataObj dataObj) const;
        [[nodiscard]] DbLob* checkLobIndexDict(typeDataObj dataObj) const;
        void dropUnusedMetadata(const std::set<std::string>& users, const std::vector<SchemaElement*>& schemaElements, std::unordered_map<typeObj,
                                std::string>& tablesDropped);
        void buildMaps(const std::string& owner, const std::string& table, const std::vector<std::string>& keyList, const std::string& key,
                       SchemaElement::TAG_TYPE tagType, const std::vector<std::string>& tagList, const std::string& tag, const std::string& condition,
                       DbTable::OPTIONS options, std::unordered_map<typeObj, std::string>& tablesUpdated, bool suppLogDbPrimary, bool suppLogDbAll,
                       uint64_t defaultCharacterMapId, uint64_t defaultCharacterNcharMapId);
        void resetTouched();
        void updateXmlCtx();
    };
}

#endif
