/* Header for Schema class
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

#include <map>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <unordered_map>
#include <vector>

#include "../common/SysCol.h"
#include "../common/SysCCol.h"
#include "../common/SysCDef.h"
#include "../common/SysECol.h"
#include "../common/SysLob.h"
#include "../common/SysLobCompPart.h"
#include "../common/SysLobFrag.h"
#include "../common/SysObj.h"
#include "../common/SysTabComPart.h"
#include "../common/SysTabPart.h"
#include "../common/SysTabSubPart.h"
#include "../common/SysTs.h"
#include "../common/SysUser.h"
#include "../common/typeXid.h"
#include "../common/types.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

namespace OpenLogReplicator {
    class Ctx;
    class Locales;
    class OracleColumn;
    class OracleLob;
    class OracleTable;
    class SysColSeg;
    class SysDeferredStg;
    class SysTab;
    class SysUser;

    class Schema {
    protected:
        Ctx* ctx;
        Locales* locales;
        typeRowId sysUserRowId;
        SysUser sysUserAdaptive;
        bool compareSysCCol(Schema* otherSchema, std::string& msgs);
        bool compareSysCDef(Schema* otherSchema, std::string& msgs);
        bool compareSysCol(Schema* otherSchema, std::string& msgs);
        bool compareSysDeferredStg(Schema* otherSchema, std::string& msgs);
        bool compareSysECol(Schema* otherSchema, std::string& msgs);
        bool compareSysLob(Schema* otherSchema, std::string& msgs);
        bool compareSysLobCompPart(Schema* otherSchema, std::string& msgs);
        bool compareSysLobFrag(Schema* otherSchema, std::string& msgs);
        bool compareSysObj(Schema* otherSchema, std::string& msgs);
        bool compareSysTab(Schema* otherSchema, std::string& msgs);
        bool compareSysTabComPart(Schema* otherSchema, std::string& msgs);
        bool compareSysTabPart(Schema* otherSchema, std::string& msgs);
        bool compareSysTabSubPart(Schema* otherSchema, std::string& msgs);
        bool compareSysTs(Schema* otherSchema, std::string& msgs);
        bool compareSysUser(Schema* otherSchema, std::string& msgs);
        void refreshIndexesSysCCol();
        void refreshIndexesSysCDef();
        void refreshIndexesSysCol();
        void refreshIndexesSysDeferredStg();
        void refreshIndexesSysECol();
        void refreshIndexesSysLob();
        void refreshIndexesSysLobCompPart();
        void refreshIndexesSysLobFrag();
        void refreshIndexesSysObj();
        void refreshIndexesSysTab();
        void refreshIndexesSysTabComPart();
        void refreshIndexesSysTabPart();
        void refreshIndexesSysTabSubPart();
        void refreshIndexesSysTs();
        void refreshIndexesSysUser(const std::set<std::string>& users);

    public:
        typeScn scn;
        typeScn refScn;
        bool loaded;

        std::unordered_map<typeObj, OracleLob*> lobMap;
        std::unordered_map<typeObj, OracleLob*> lobPartitionMap;
        std::unordered_map<typeObj, OracleLob*> lobIndexMap;
        std::unordered_map<typeObj, uint16_t> lobPageMap;
        std::unordered_map<typeObj, OracleTable*> tableMap;
        std::unordered_map<typeObj, OracleTable*> tablePartitionMap;
        OracleColumn* schemaColumn;
        OracleLob* schemaLob;
        OracleTable* schemaTable;

        // SYS.CCOL$
        std::map<typeRowId, SysCCol*> sysCColMapRowId;
        std::map<SysCColKey, SysCCol*> sysCColMapKey;

        // SYS.CDEF$
        std::map<typeRowId, SysCDef*> sysCDefMapRowId;
        std::map<SysCDefKey, SysCDef*> sysCDefMapKey;
        std::unordered_map<typeCon, SysCDef*> sysCDefMapCon;

        // SYS.COL$
        std::map<typeRowId, SysCol*> sysColMapRowId;
        std::map<SysColKey, SysCol*> sysColMapKey;
        std::map<SysColSeg, SysCol*> sysColMapSeg;

        // SYS.DEFERRED_STG$
        std::map<typeRowId, SysDeferredStg*> sysDeferredStgMapRowId;
        std::unordered_map<typeObj, SysDeferredStg*> sysDeferredStgMapObj;

        // SYS.ECOL$
        std::map<typeRowId, SysECol*> sysEColMapRowId;
        std::unordered_map<SysEColKey, SysECol*> sysEColMapKey;

        // SYS.LOB$
        std::map<typeRowId, SysLob*> sysLobMapRowId;
        std::unordered_map<typeObj, SysLob*> sysLobMapLObj;
        std::map<SysLobKey, SysLob*> sysLobMapKey;

        // SYS.LOBCOMPPART$
        std::map<typeRowId, SysLobCompPart*> sysLobCompPartMapRowId;
        std::unordered_map<typeObj, SysLobCompPart*> sysLobCompPartMapPartObj;
        std::map<SysLobCompPartKey, SysLobCompPart*> sysLobCompPartMapKey;

        // SYS.LOBFRAG$
        std::map<typeRowId, SysLobFrag*> sysLobFragMapRowId;
        std::map<SysLobFragKey, SysLobFrag*> sysLobFragMapKey;

        // SYS.OBJ$
        std::map<typeRowId, SysObj*> sysObjMapRowId;
        std::map<SysObjNameKey, SysObj*> sysObjMapName;
        std::unordered_map<typeObj, SysObj*> sysObjMapObj;

        // SYS.TAB$
        std::map<typeRowId, SysTab*> sysTabMapRowId;
        std::unordered_map<typeObj, SysTab*> sysTabMapObj;

        // SYS.TABCOMPART$
        std::map<typeRowId, SysTabComPart*> sysTabComPartMapRowId;
        std::unordered_map<typeObj, SysTabComPart*> sysTabComPartMapObj;
        std::map<SysTabComPartKey, SysTabComPart*> sysTabComPartMapKey;

        // SYS.TABPART$
        std::map<typeRowId, SysTabPart*> sysTabPartMapRowId;
        std::map<SysTabPartKey, SysTabPart*> sysTabPartMapKey;

        // SYS.TABSUBPART$
        std::map<typeRowId, SysTabSubPart*> sysTabSubPartMapRowId;
        std::map<SysTabSubPartKey, SysTabSubPart*> sysTabSubPartMapKey;

        // SYS.TS$
        std::map<typeRowId, SysTs*> sysTsMapRowId;
        std::unordered_map<typeTs, SysTs*> sysTsMapTs;

        // SYS.USER$
        std::map<typeRowId, SysUser*> sysUserMapRowId;
        std::unordered_map<typeUser, SysUser*> sysUserMapUser;

        std::set<typeObj> lobsTouched;
        std::set<typeObj> lobPartitionsTouched;
        std::set<typeObj> tablesTouched;
        std::set<typeObj> tablePartitionsTouched;
        std::set<typeUser> usersTouched;

        bool sysCColTouched;
        bool sysCDefTouched;
        bool sysColTouched;
        bool sysDeferredStgTouched;
        bool sysEColTouched;
        bool sysLobTouched;
        bool sysLobCompPartTouched;
        bool sysLobFragTouched;
        bool sysObjTouched;
        bool sysTabTouched;
        bool sysTabComPartTouched;
        bool sysTabPartTouched;
        bool sysTabSubPartTouched;
        bool sysTsTouched;
        bool sysUserTouched;
        bool touched;

        Schema(Ctx* newCtx, Locales* newLocales);
        virtual ~Schema();

        void purge();
        void refreshIndexes(const std::set<std::string>& users);
        [[nodiscard]] bool compare(Schema* otherSchema, std::string& msgs);
        bool dictSysCColAdd(const char* rowIdStr, typeCon con, typeCol intCol, typeObj obj, uint64_t spare11, uint64_t spare12);
        bool dictSysCDefAdd(const char* rowIdStr, typeCon con, typeObj obj, typeType type);
        bool dictSysColAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type, uint64_t length,
                           int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, bool null_, uint64_t property1, uint64_t property2);
        bool dictSysDeferredStgAdd(const char* rowIdStr, typeObj obj, uint64_t flagsStg1, uint64_t flagsStg2);
        bool dictSysEColAdd(const char* rowIdStr, typeObj tabObj, typeCol colNum, typeCol guardId);
        bool dictSysLobAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol intCol, typeObj lObj, typeTs ts);
        bool dictSysLobCompPartAdd(const char* rowIdStr, typeObj partObj, typeObj lObj);
        bool dictSysLobFragAdd(const char* rowIdStr, typeObj fragObj, typeObj parentObj, typeTs ts);
        bool dictSysObjAdd(const char* rowIdStr, typeUser owner, typeObj obj, typeDataObj dataObj, typeType type, const char* name, uint64_t flags1,
                           uint64_t flags2, bool single);
        bool dictSysTabAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeCol cluCols, uint64_t flags1, uint64_t flags2, uint64_t property1,
                           uint64_t property2);
        bool dictSysTabComPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo);
        bool dictSysTabPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo);
        bool dictSysTabSubPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj pObj);
        bool dictSysTsAdd(const char* rowIdStr, typeTs ts, const char* name, uint32_t blockSize);
        bool dictSysUserAdd(const char* rowIdStr, typeUser user, const char* name, uint64_t spare11, uint64_t spare12, bool single);
        void dictSysCColDrop(typeRowId rowId);
        void dictSysCDefDrop(typeRowId rowId);
        void dictSysColDrop(typeRowId rowId);
        void dictSysDeferredStgDrop(typeRowId rowId);
        void dictSysEColDrop(typeRowId rowId);
        void dictSysLobDrop(typeRowId rowId);
        void dictSysLobCompPartDrop(typeRowId rowId);
        void dictSysLobFragDrop(typeRowId rowId);
        void dictSysObjDrop(typeRowId rowId);
        void dictSysTabDrop(typeRowId rowId);
        void dictSysTabComPartDrop(typeRowId rowId);
        void dictSysTabPartDrop(typeRowId rowId);
        void dictSysTabSubPartDrop(typeRowId rowId);
        void dictSysTsDrop(typeRowId rowId);
        void dictSysUserDrop(typeRowId rowId);
        [[nodiscard]] SysCCol* dictSysCColFind(typeRowId rowId);
        [[nodiscard]] SysCDef* dictSysCDefFind(typeRowId rowId);
        [[nodiscard]] SysCol* dictSysColFind(typeRowId rowId);
        [[nodiscard]] SysDeferredStg* dictSysDeferredStgFind(typeRowId rowId);
        [[nodiscard]] SysECol* dictSysEColFind(typeRowId rowId);
        [[nodiscard]] SysLob* dictSysLobFind(typeRowId rowId);
        [[nodiscard]] SysLobCompPart* dictSysLobCompPartFind(typeRowId rowId);
        [[nodiscard]] SysLobFrag* dictSysLobFragFind(typeRowId rowId);
        [[nodiscard]] SysObj* dictSysObjFind(typeRowId rowId);
        [[nodiscard]] SysTab* dictSysTabFind(typeRowId rowId);
        [[nodiscard]] SysTabComPart* dictSysTabComPartFind(typeRowId rowId);
        [[nodiscard]] SysTabPart* dictSysTabPartFind(typeRowId rowId);
        [[nodiscard]] SysTabSubPart* dictSysTabSubPartFind(typeRowId rowId);
        [[nodiscard]] SysTs* dictSysTsFind(typeRowId rowId);
        [[nodiscard]] SysUser* dictSysUserFind(typeRowId rowId);
        void touchLob(typeObj obj);
        void touchLobPartition(typeObj obj);
        void touchTable(typeObj obj);
        void touchTablePartition(typeObj obj);
        void touchUser(typeUser user);
        [[nodiscard]] OracleTable* checkTableDict(typeObj obj);
        [[nodiscard]] OracleLob* checkLobDict(typeObj obj);
        [[nodiscard]] OracleLob* checkLobIndexDict(typeObj obj);
        [[nodiscard]] uint32_t checkLobPageSize(typeObj obj);
        void addTableToDict(OracleTable* table);
        void removeTableFromDict(OracleTable* table);
        void addLobToDict(OracleLob* lob, uint16_t pageSize);
        void rebuildMaps(std::set<std::string>& msgs);
        void buildMaps(const std::string& owner, const std::string& table, const std::vector<std::string>& keys, const std::string& keysStr,
                       typeOptions options, std::set<std::string>& msgs, bool suppLogDbPrimary, bool suppLogDbAll, uint64_t defaultCharacterMapId,
                       uint64_t defaultCharacterNcharMapId);
        uint16_t getLobBlockSize(typeTs ts);
    };
}

#endif
