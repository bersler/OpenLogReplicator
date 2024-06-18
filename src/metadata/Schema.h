/* Header for Schema class
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

#include <list>
#include <map>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <unordered_map>
#include <vector>

#include "../common/typeRowId.h"
#include "../common/typeXid.h"
#include "../common/types.h"
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
#include "../common/table/XdbXPt.h"
#include "../common/table/XdbXQn.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

namespace OpenLogReplicator {
    class Ctx;
    class Locales;
    class OracleColumn;
    class OracleLob;
    class OracleTable;
    class SchemaElement;
    class SysDeferredStg;
    class SysTab;
    class SysTs;
    class XdbTtSet;
    class XdbXNm;
    class XdbXQn;
    class XdbXPt;
    class XmlCtx;

    class Schema final {
    protected:
        Ctx* ctx;
        Locales* locales;
        typeRowId sysUserRowId;
        SysUser sysUserAdaptive;

        // temporary objects
        SysCCol* sysCColTmp;
        SysCDef* sysCDefTmp;
        SysCol* sysColTmp;
        SysDeferredStg* sysDeferredStgTmp;
        SysECol* sysEColTmp;
        SysLob* sysLobTmp;
        SysLobCompPart* sysLobCompPartTmp;
        SysLobFrag* sysLobFragTmp;
        SysObj* sysObjTmp;
        SysTab* sysTabTmp;
        SysTabComPart* sysTabComPartTmp;
        SysTabPart* sysTabPartTmp;
        SysTabSubPart* sysTabSubPartTmp;
        SysTs* sysTsTmp;
        SysUser* sysUserTmp;
        XdbTtSet* xdbTtSetTmp;
        XdbXNm* xdbXNmTmp;
        XdbXPt* xdbXPtTmp;
        XdbXQn* xdbXQnTmp;

        bool compareSysCCol(Schema* otherSchema, std::string& msgs) const;
        bool compareSysCDef(Schema* otherSchema, std::string& msgs) const;
        bool compareSysCol(Schema* otherSchema, std::string& msgs) const;
        bool compareSysDeferredStg(Schema* otherSchema, std::string& msgs) const;
        bool compareSysECol(Schema* otherSchema, std::string& msgs) const;
        bool compareSysLob(Schema* otherSchema, std::string& msgs) const;
        bool compareSysLobCompPart(Schema* otherSchema, std::string& msgs) const;
        bool compareSysLobFrag(Schema* otherSchema, std::string& msgs) const;
        bool compareSysObj(Schema* otherSchema, std::string& msgs) const;
        bool compareSysTab(Schema* otherSchema, std::string& msgs) const;
        bool compareSysTabComPart(Schema* otherSchema, std::string& msgs) const;
        bool compareSysTabPart(Schema* otherSchema, std::string& msgs) const;
        bool compareSysTabSubPart(Schema* otherSchema, std::string& msgs) const;
        bool compareSysTs(Schema* otherSchema, std::string& msgs) const;
        bool compareSysUser(Schema* otherSchema, std::string& msgs) const;
        bool compareXdbTtSet(Schema* otherSchema, std::string& msgs) const;
        bool compareXdbXNm(Schema* otherSchema, std::string& msgs) const;
        bool compareXdbXQn(Schema* otherSchema, std::string& msgs) const;
        bool compareXdbXPt(Schema* otherSchema, std::string& msgs) const;
        void addTableToDict(OracleTable* table);
        void removeTableFromDict(OracleTable* table);
        uint16_t getLobBlockSize(typeTs ts) const;

    public:
        typeScn scn;
        typeScn refScn;
        bool loaded;

        std::unordered_map<typeDataObj, OracleLob*> lobPartitionMap;
        std::unordered_map<typeDataObj, OracleLob*> lobIndexMap;
        std::unordered_map<typeObj, OracleTable*> tableMap;
        std::unordered_map<typeObj, OracleTable*> tablePartitionMap;
        XmlCtx* xmlCtxDefault;
        OracleColumn* columnTmp;
        OracleLob* lobTmp;
        OracleTable* tableTmp;
        std::set<OracleTable*> tablesTouched;
        std::set<typeObj> identifiersTouched;
        bool touched;

        // SYS.CCOL$
        std::map<typeRowId, SysCCol*> sysCColMapRowId;
        std::map<SysCColKey, SysCCol*> sysCColMapKey;
        std::set<SysCCol*> sysCColSetTouched;

        // SYS.CDEF$
        std::map<typeRowId, SysCDef*> sysCDefMapRowId;
        std::map<SysCDefKey, SysCDef*> sysCDefMapKey;
        std::unordered_map<typeCon, SysCDef*> sysCDefMapCon;
        std::set<SysCDef*> sysCDefSetTouched;

        // SYS.COL$
        std::map<typeRowId, SysCol*> sysColMapRowId;
        std::map<SysColSeg, SysCol*> sysColMapSeg;
        std::set<SysCol*> sysColSetTouched;

        // SYS.DEFERRED_STG$
        std::map<typeRowId, SysDeferredStg*> sysDeferredStgMapRowId;
        std::unordered_map<typeObj, SysDeferredStg*> sysDeferredStgMapObj;
        std::set<SysDeferredStg*> sysDeferredStgSetTouched;

        // SYS.ECOL$
        std::map<typeRowId, SysECol*> sysEColMapRowId;
        std::unordered_map<SysEColKey, SysECol*> sysEColMapKey;
        std::set<SysECol*> sysEColSetTouched;

        // SYS.LOB$
        std::map<typeRowId, SysLob*> sysLobMapRowId;
        std::unordered_map<typeObj, SysLob*> sysLobMapLObj;
        std::map<SysLobKey, SysLob*> sysLobMapKey;
        std::set<SysLob*> sysLobSetTouched;

        // SYS.LOBCOMPPART$
        std::map<typeRowId, SysLobCompPart*> sysLobCompPartMapRowId;
        std::unordered_map<typeObj, SysLobCompPart*> sysLobCompPartMapPartObj;
        std::map<SysLobCompPartKey, SysLobCompPart*> sysLobCompPartMapKey;
        std::set<SysLobCompPart*> sysLobCompPartSetTouched;

        // SYS.LOBFRAG$
        std::map<typeRowId, SysLobFrag*> sysLobFragMapRowId;
        std::map<SysLobFragKey, SysLobFrag*> sysLobFragMapKey;
        std::set<SysLobFrag*> sysLobFragSetTouched;

        // SYS.OBJ$
        std::map<typeRowId, SysObj*> sysObjMapRowId;
        std::map<SysObjNameKey, SysObj*> sysObjMapName;
        std::unordered_map<typeObj, SysObj*> sysObjMapObj;
        std::set<SysObj*> sysObjSetTouched;

        // SYS.TAB$
        std::map<typeRowId, SysTab*> sysTabMapRowId;
        std::unordered_map<typeObj, SysTab*> sysTabMapObj;
        std::set<SysTab*> sysTabSetTouched;

        // SYS.TABCOMPART$
        std::map<typeRowId, SysTabComPart*> sysTabComPartMapRowId;
        std::unordered_map<typeObj, SysTabComPart*> sysTabComPartMapObj;
        std::map<SysTabComPartKey, SysTabComPart*> sysTabComPartMapKey;
        std::set<SysTabComPart*> sysTabComPartSetTouched;

        // SYS.TABPART$
        std::map<typeRowId, SysTabPart*> sysTabPartMapRowId;
        std::map<SysTabPartKey, SysTabPart*> sysTabPartMapKey;
        std::set<SysTabPart*> sysTabPartSetTouched;

        // SYS.TABSUBPART$
        std::map<typeRowId, SysTabSubPart*> sysTabSubPartMapRowId;
        std::map<SysTabSubPartKey, SysTabSubPart*> sysTabSubPartMapKey;
        std::set<SysTabSubPart*> sysTabSubPartSetTouched;

        // SYS.TS$
        std::map<typeRowId, SysTs*> sysTsMapRowId;
        std::unordered_map<typeTs, SysTs*> sysTsMapTs;

        // SYS.USER$
        std::map<typeRowId, SysUser*> sysUserMapRowId;
        std::unordered_map<typeUser, SysUser*> sysUserMapUser;
        std::set<SysUser*> sysUserSetTouched;

        // XDB.XDB$TTSET
        std::map<typeRowId, XdbTtSet*> xdbTtSetMapRowId;
        std::unordered_map<std::string, XdbTtSet*> xdbTtSetMapTs;

        // XDB.X$yyxxx
        std::map<std::string, XmlCtx*> schemaXmlMap;

        Schema(Ctx* newCtx, Locales* newLocales);
        virtual ~Schema();

        void purgeMetadata();
        void purgeDicts();
        [[nodiscard]] bool compare(Schema* otherSchema, std::string& msgs) const;
        void dictSysCColAdd(const char* rowIdStr, typeCon con, typeCol intCol, typeObj obj, uint64_t spare11, uint64_t spare12);
        void dictSysCDefAdd(const char* rowIdStr, typeCon con, typeObj obj, typeType type);
        void dictSysColAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type, uint64_t length,
                           int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null_, uint64_t property1, uint64_t property2);
        void dictSysDeferredStgAdd(const char* rowIdStr, typeObj obj, uint64_t flagsStg1, uint64_t flagsStg2);
        void dictSysEColAdd(const char* rowIdStr, typeObj tabObj, typeCol colNum, typeCol guardId);
        void dictSysLobAdd(const char* rowIdStr, typeObj obj, typeCol col, typeCol intCol, typeObj lObj, typeTs ts);
        void dictSysLobCompPartAdd(const char* rowIdStr, typeObj partObj, typeObj lObj);
        void dictSysLobFragAdd(const char* rowIdStr, typeObj fragObj, typeObj parentObj, typeTs ts);
        bool dictSysObjAdd(const char* rowIdStr, typeUser owner, typeObj obj, typeDataObj dataObj, typeType type, const char* name, uint64_t flags1,
                           uint64_t flags2, bool single);
        void dictSysTabAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeTs ts, typeCol cluCols, uint64_t flags1, uint64_t flags2,
                           uint64_t property1, uint64_t property2);
        void dictSysTabComPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo);
        void dictSysTabPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj bo);
        void dictSysTabSubPartAdd(const char* rowIdStr, typeObj obj, typeDataObj dataObj, typeObj pObj);
        void dictSysTsAdd(const char* rowIdStr, typeTs ts, const char* name, uint32_t blockSize);
        bool dictSysUserAdd(const char* rowIdStr, typeUser user, const char* name, uint64_t spare11, uint64_t spare12, bool single);
        void dictXdbTtSetAdd(const char* rowIdStr, const char* guid, const char* tokSuf, uint64_t flags, typeObj obj);
        void dictXdbXNmAdd(XmlCtx* xmlCtx, const char* rowIdStr, const char* nmSpcUri, const char* id);
        void dictXdbXPtAdd(XmlCtx* xmlCtx, const char* rowIdStr, const char* path, const char* id);
        void dictXdbXQnAdd(XmlCtx* xmlCtx, const char* rowIdStr, const char* nmSpcId, const char* localName, const char* flags, const char* id);

        void dictSysCColAdd(SysCCol* sysCCol);
        void dictSysCDefAdd(SysCDef* sysCDef);
        void dictSysColAdd(SysCol* sysCol);
        void dictSysDeferredStgAdd(SysDeferredStg* sysDeferredStg);
        void dictSysEColAdd(SysECol* sysECol);
        void dictSysLobAdd(SysLob* sysLob);
        void dictSysLobCompPartAdd(SysLobCompPart* sysLobCompPart);
        void dictSysLobFragAdd(SysLobFrag* sysLobFrag);
        void dictSysObjAdd(SysObj* sysObj);
        void dictSysTabAdd(SysTab* sysTab);
        void dictSysTabComPartAdd(SysTabComPart* sysTabComPart);
        void dictSysTabPartAdd(SysTabPart* sysTabPart);
        void dictSysTabSubPartAdd(SysTabSubPart* sysTabSubPart);
        void dictSysTsAdd(SysTs* sysTs);
        void dictSysUserAdd(SysUser* sysUser);
        void dictXdbTtSetAdd(XdbTtSet* xdbTtSet);
        void dictXdbXNmAdd(const std::string& tokSuf, XdbXNm* xdbXNm);
        void dictXdbXPtAdd(const std::string& tokSuf, XdbXPt* xdbXPt);
        void dictXdbXQnAdd(const std::string& tokSuf, XdbXQn* xdbXQn);

        void dictSysCColDrop(SysCCol* sysCCol);
        void dictSysCDefDrop(SysCDef* sysCDef);
        void dictSysColDrop(SysCol* sysCol);
        void dictSysDeferredStgDrop(SysDeferredStg* sysDeferredStg);
        void dictSysEColDrop(SysECol* sysECol);
        void dictSysLobDrop(SysLob* sysLob);
        void dictSysLobCompPartDrop(SysLobCompPart* sysLobCompPart);
        void dictSysLobFragDrop(SysLobFrag* sysLobFrag);
        void dictSysObjDrop(SysObj* sysObj);
        void dictSysTabDrop(SysTab* sysTab);
        void dictSysTabComPartDrop(SysTabComPart* sysTabComPart);
        void dictSysTabPartDrop(SysTabPart* sysTabPart);
        void dictSysTabSubPartDrop(SysTabSubPart* sysTabSubPart);
        void dictSysTsDrop(SysTs* sysTs);
        void dictSysUserDrop(SysUser* sysUser);
        void dictXdbTtSetDrop(XdbTtSet* xdbTtSet);
        void dictXdbXNmDrop(const std::string& tokSuf, XdbXNm* xdbXNm);
        void dictXdbXPtDrop(const std::string& tokSuf, XdbXPt* xdbXPt);
        void dictXdbXQnDrop(const std::string& tokSuf, XdbXQn* xdbXQn);

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
        [[nodiscard]] XdbTtSet* dictXdbTtSetFind(typeRowId rowId);
        [[nodiscard]] XdbXNm* dictXdbXNmFind(const std::string& tokSuf, typeRowId rowId);
        [[nodiscard]] XdbXPt* dictXdbXPtFind(const std::string& tokSuf, typeRowId rowId);
        [[nodiscard]] XdbXQn* dictXdbXQnFind(const std::string& tokSuf, typeRowId rowId);

        void touchTable(typeObj obj);
        [[nodiscard]] OracleTable* checkTableDict(typeObj obj) const;
        [[nodiscard]] bool checkTableDictUncommitted(typeObj obj, std::string& owner, std::string& table) const;
        [[nodiscard]] OracleLob* checkLobDict(typeDataObj dataObj) const;
        [[nodiscard]] OracleLob* checkLobIndexDict(typeDataObj dataObj) const;
        void dropUnusedMetadata(const std::set<std::string>& users, const std::vector<SchemaElement*>& schemaElements, std::vector<std::string>& msgs);
        void buildMaps(const std::string& owner, const std::string& table, const std::vector<std::string>& keys, const std::string& keysStr,
                       const std::string& conditionStr, typeOptions options, std::vector<std::string>& msgs, bool suppLogDbPrimary, bool suppLogDbAll,
                       uint64_t defaultCharacterMapId, uint64_t defaultCharacterNcharMapId);
        void resetTouched();
        void updateXmlCtx();
    };
}

#endif
