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

#include <functional>
#include <map>
#include <unordered_map>
#include <set>
#include <vector>

#include "SysCCol.h"
#include "SysCDef.h"
#include "SysCol.h"
#include "SysDeferredStg.h"
#include "SysECol.h"
#include "SysObj.h"
#include "SysTab.h"
#include "SysTabComPart.h"
#include "SysTabPart.h"
#include "SysTabSubPart.h"
#include "SysUser.h"
#include "SystemTransaction.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

#define SCHEMA_ENDL     <<std::endl

namespace OpenLogReplicator {
    class OracleAnalyzer;
    class OracleColumn;
    class OracleObject;
    class SchemaElement;

    class Schema {
    protected:
        OracleAnalyzer* oracleAnalyzer;
        std::stringstream& writeEscapeValue(std::stringstream& ss, std::string& str);
        std::unordered_map<typeOBJ, OracleObject*> objectMap;
        std::unordered_map<typeOBJ, OracleObject*> partitionMap;

        //SYS.CCOL$
        std::map<RowId, SysCCol*> sysCColMapRowId;
        std::map<SysCColKey, SysCCol*> sysCColMapKey;

        //SYS.CDEF$
        std::map<RowId, SysCDef*> sysCDefMapRowId;
        std::map<SysCDefKey, SysCDef*> sysCDefMapKey;
        std::unordered_map<typeCON, SysCDef*> sysCDefMapCon;

        //SYS.COL$
        std::map<RowId, SysCol*> sysColMapRowId;
        std::map<SysColKey, SysCol*> sysColMapKey;
        std::map<SysColSeg, SysCol*> sysColMapSeg;

        //SYS.DEFERREDSTG$
        std::map<RowId, SysDeferredStg*> sysDeferredStgMapRowId;
        std::unordered_map<typeOBJ, SysDeferredStg*> sysDeferredStgMapObj;

        //SYS.ECOL$
        std::map<RowId, SysECol*> sysEColMapRowId;
        std::unordered_map<SysEColKey, SysECol*> sysEColMapKey;

        //SYS.OBJ$
        std::map<RowId, SysObj*> sysObjMapRowId;
        std::unordered_map<typeOBJ, SysObj*> sysObjMapObj;

        //SYS.TAB$
        std::map<RowId, SysTab*> sysTabMapRowId;
        std::unordered_map<typeOBJ, SysTab*> sysTabMapObj;

        //SYS.TABCOMPART$
        std::map<RowId, SysTabComPart*> sysTabComPartMapRowId;
        std::unordered_map<typeOBJ, SysTabComPart*> sysTabComPartMapObj;
        std::map<SysTabComPartKey, SysTabComPart*> sysTabComPartMapKey;

        //SYS.TABPART$
        std::map<RowId, SysTabPart*> sysTabPartMapRowId;
        std::map<SysTabPartKey, SysTabPart*> sysTabPartMapKey;

        //SYS.TABSUBPART$
        std::map<RowId, SysTabSubPart*> sysTabSubPartMapRowId;
        std::map<SysTabSubPartKey, SysTabSubPart*> sysTabSubPartMapKey;

        //SYS.USER$
        std::map<RowId, SysUser*> sysUserMapRowId;
        std::unordered_map<typeUSER, SysUser*> sysUserMapUser;

        std::set<typeOBJ> partitionsTouched;
        std::set<typeOBJ> objectsTouched;
        std::set<typeUSER> usersTouched;
        OracleObject* schemaObject;
        OracleColumn* schemaColumn;
        std::vector<SchemaElement*> elements;
        std::set<std::string> users;
        std::set<typeSCN> schemaScnList;
        bool touched;
        bool sysCColTouched;
        bool sysCDefTouched;
        bool sysColTouched;
        bool sysDeferredStgTouched;
        bool sysEColTouched;
        bool sysObjTouched;
        bool sysTabTouched;
        bool sysTabComPartTouched;
        bool sysTabPartTouched;
        bool sysTabSubPartTouched;
        bool sysUserTouched;
        bool savedDeleted;

    public:
        Schema(OracleAnalyzer* oracleAnalyzer);
        virtual ~Schema();

        void dropSchema(void);
        bool readSchema(void);
        bool readSchema(std::string& jsonName, typeSCN fileScn);
        void writeSchema(void);
        OracleObject* checkDict(typeOBJ obj, typeDATAOBJ dataObj);
        void addToDict(OracleObject* object);
        void removeFromDict(OracleObject* object);
        bool refreshIndexes(void);
        void rebuildMaps(void);
        void buildMaps(std::string& owner, std::string& table, std::vector<std::string>& keys, std::string& keysStr, typeOPTIONS options, bool output);
        SchemaElement* addElement(const char* owner, const char* table, typeOPTIONS options);
        bool dictSysCColAdd(const char* rowIdStr, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare11, uint64_t spare12);
        bool dictSysCDefAdd(const char* rowIdStr, typeCON con, typeOBJ obj, typeTYPE type);
        bool dictSysColAdd(const char* rowIdStr, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char* name,
                typeTYPE type, uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId,
                bool null_, uint64_t property1, uint64_t property2);
        bool dictSysDeferredStgAdd(const char* rowIdStr, typeOBJ obj, uint64_t flagsStg1, uint64_t flagsStg2);
        bool dictSysEColAdd(const char* rowIdStr, typeOBJ tabObj, typeCOL colNum, typeCOL guardId);
        bool dictSysObjAdd(const char* rowIdStr, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char* name,
                uint64_t flags1, uint64_t flags2, bool single);
        bool dictSysTabAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeCOL cluCols, uint64_t flags1, uint64_t flags2,
                uint64_t property1, uint64_t property2);
        bool dictSysTabComPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabSubPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj);
        bool dictSysUserAdd(const char* rowIdStr, typeUSER user, const char* name, uint64_t spare11, uint64_t spare12, bool single);
        void touchObj(typeOBJ obj);
        void touchPart(typeOBJ obj);
        void touchUser(typeUSER user);
        bool checkNameCase(const char* name);

        friend class SystemTransaction;
        friend class OracleAnalyzer;
        friend class OracleAnalyzerOnline;
    };
}

#endif
