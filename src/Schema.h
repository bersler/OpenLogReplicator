/* Header for Schema class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "SysSeg.h"
#include "SysTab.h"
#include "SysTabComPart.h"
#include "SysTabPart.h"
#include "SysTabSubPart.h"
#include "SysUser.h"
#include "SystemTransaction.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzer;
    class OracleObject;
    class SchemaElement;

    class Schema {
    protected:
        stringstream& writeEscapeValue(stringstream &ss, string &str);
        unordered_map<typeOBJ, OracleObject*> objectMap;
        unordered_map<typeOBJ, OracleObject*> partitionMap;

        //SYS.CCOL$
        unordered_map<RowId, SysCCol*> sysCColMapRowId;
        map<SysCColKey, SysCCol*> sysCColMapKey;
        bool sysCColKeyTouched;

        //SYS.CDEF$
        unordered_map<RowId, SysCDef*> sysCDefMapRowId;
        map<SysCDefKey, SysCDef*> sysCDefMapKey;
        bool sysCDefKeyTouched;
        unordered_map<typeCON, SysCDef*> sysCDefMapCon;
        bool sysCDefConTouched;

        //SYS.COL$
        unordered_map<RowId, SysCol*> sysColMapRowId;
        map<SysColKey, SysCol*> sysColMapKey;
        bool sysColKeyTouched;
        map<SysColSeg, SysCol*> sysColMapSeg;
        bool sysColSegTouched;

        //SYS.DEFERREDSTG$
        unordered_map<RowId, SysDeferredStg*> sysDeferredStgMapRowId;
        unordered_map<typeOBJ, SysDeferredStg*> sysDeferredStgMapObj;
        bool sysDeferredStgObjTouched;

        //SYS.ECOL$
        unordered_map<RowId, SysECol*> sysEColMapRowId;
        unordered_map<SysEColKey, SysECol*> sysEColMapKey;
        bool sysEColKeyTouched;

        //SYS.OBJ$
        unordered_map<RowId, SysObj*> sysObjMapRowId;
        unordered_map<typeOBJ, SysObj*> sysObjMapObj;
        bool sysObjObjTouched;

        //SYS.SEG$
        unordered_map<RowId, SysSeg*> sysSegMapRowId;
        unordered_map<SysSegKey, SysSeg*> sysSegMapKey;
        bool sysSegKeyTouched;

        //SYS.TAB$
        unordered_map<RowId, SysTab*> sysTabMapRowId;
        unordered_map<typeOBJ, SysTab*> sysTabMapObj;
        bool sysTabObjTouched;
        unordered_map<SysTabKey, SysTab*> sysTabMapKey;
        bool sysTabKeyTouched;

        //SYS.TABCOMPART$
        unordered_map<RowId, SysTabComPart*> sysTabComPartMapRowId;
        map<SysTabComPartKey, SysTabComPart*> sysTabComPartMapKey;
        bool sysTabComPartKeyTouched;

        //SYS.TABPART$
        unordered_map<RowId, SysTabPart*> sysTabPartMapRowId;
        map<SysTabPartKey, SysTabPart*> sysTabPartMapKey;
        bool sysTabPartKeyTouched;

        //SYS.TABSUBPART$
        unordered_map<RowId, SysTabSubPart*> sysTabSubPartMapRowId;
        map<SysTabSubPartKey, SysTabSubPart*> sysTabSubPartMapKey;
        bool sysTabSubPartKeyTouched;

        //SYS.USER$
        unordered_map<RowId, SysUser*> sysUserMapRowId;
        unordered_map<typeUSER, SysUser*> sysUserMapUser;
        bool sysUserUserTouched;

        set<typeOBJ> objectsTouched;
        set<typeUSER> usersTouched;
        bool touched;

    public:
        OracleObject *schemaObject;
        vector<SchemaElement*> elements;

        Schema();
        virtual ~Schema();

        bool readSchema(OracleAnalyzer *oracleAnalyzer);
        void writeSchema(OracleAnalyzer *oracleAnalyzer);
        bool readSys(OracleAnalyzer *oracleAnalyzer);
        void writeSys(OracleAnalyzer *oracleAnalyzer);
        OracleObject *checkDict(typeOBJ obj, typeDATAOBJ dataObj);
        void addToDict(OracleObject *object);
        void removeFromDict(OracleObject *object);
        void refreshIndexes(void);
        void rebuildMaps(OracleAnalyzer *oracleAnalyzer);
        void buildMaps(string &owner, string &table, vector<string> &keys, string &keysStr, uint64_t options, OracleAnalyzer *oracleAnalyzer, bool output);
        SchemaElement* addElement(void);
        SchemaElement* addElement(const char *owner, const char *table, uint64_t options);
        bool dictSysCColAdd(const char *rowIdStr, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare11, uint64_t spare12);
        bool dictSysCDefAdd(const char *rowIdStr, typeCON con, typeOBJ obj, typeTYPE type);
        bool dictSysColAdd(const char *rowIdStr, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char *name,
                typeTYPE type, uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId,
                bool null_, uint64_t property1, uint64_t property2);
        bool dictSysDeferredStgAdd(const char *rowIdStr, typeOBJ obj, uint64_t flagsStg1, uint64_t flagsStg2);
        bool dictSysEColAdd(const char *rowIdStr, typeOBJ tabObj, typeCOL colNum, uint32_t guardId);
        bool dictSysObjAdd(const char *rowIdStr, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char *name,
                uint64_t flags1, uint64_t flags2);
        bool dictSysSegAdd(const char *rowIdStr, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare11, uint64_t spare12);
        bool dictSysTabAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block,
                typeCOL cluCols, uint64_t flags1, uint64_t flags2, uint64_t property1, uint64_t property2);
        bool dictSysTabComPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabSubPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj);
        bool dictSysUserAdd(const char *rowIdStr, typeUSER user, const char *name, uint64_t spare11, uint64_t spare12, bool trackDDL);
        void touchObj(typeOBJ obj);
        void touchUser(typeUSER user);

        friend class SystemTransaction;
    };
}

#endif
