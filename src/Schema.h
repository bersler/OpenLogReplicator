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

#include <unordered_map>
#include <vector>

#include "RowId.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzer;
    class OracleObject;
    class SchemaElement;

    struct SysUser {
        RowId rowId;
        typeUSER user;      //pk
        string name;
        uint64_t spare1;
    };

    struct SysObj {
        RowId rowId;
        typeUSER owner;
        typeOBJ obj;       //pk
        typeDATAOBJ dataObj;
        typeTYPE type;
        string name;
        uint32_t flags;
    };

    struct SysCol {
        RowId rowId;
        typeOBJ obj;       //pk
        typeCOL col;
        typeCOL segCol;
        typeCOL intCol;     //pk
        string name;
        typeTYPE type;
        uint64_t length;
        int64_t precision;
        int64_t scale;
        uint64_t charsetForm;
        uint64_t charsetId;
        int64_t null;
        uintX_t property;
    };

    struct SysCCol {
        RowId rowId;
        typeCON con;        //pk
        typeCOL intCol;     //pk
        typeOBJ obj;
        uint64_t spare1;
    };

    struct SysCDef {
        RowId rowId;
        typeCON con;        //pk
        typeOBJ obj;
        typeTYPE type;
    };

    struct SysDeferredStg {
        RowId rowId;
        typeOBJ obj;       //pk
        uint64_t flagsStg;
    };

    struct SysECol {
        RowId rowId;
        typeOBJ obj;
        uint32_t colNum;
        uint32_t guardId;
    };

    struct SysSeg {
        RowId rowId;
        uint32_t file;
        uint32_t block;
        uint32_t ts;
        uint64_t spare1;
    };

    struct SysTab {
        RowId rowId;
        typeOBJ obj;       //pk
        typeDATAOBJ dataObj;
        uint32_t ts;
        uint32_t file;
        uint32_t block;
        typeCOL cluCols;
        uint64_t flags;
        uintX_t property;
    };

    struct SysTabPart {
        RowId rowId;
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeOBJ bo;
    };

    struct SysTabComPart {
        RowId rowId;
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeOBJ bo;
    };

    struct SysTabSubPart {
        RowId rowId;
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeOBJ pObj;
    };

    class Schema {
    protected:
        stringstream& writeEscapeValue(stringstream &ss, string &str);
        unordered_map<typeOBJ, OracleObject*> objectMap;
        unordered_map<typeOBJ, OracleObject*> partitionMap;

        unordered_map<RowId, SysUser*> sysUserMap;
        unordered_map<RowId, SysObj*> sysObjMap;
        unordered_map<RowId, SysCol*> sysColMap;
        unordered_map<RowId, SysCCol*> sysCColMap;
        unordered_map<RowId, SysCDef*> sysCDefMap;
        unordered_map<RowId, SysDeferredStg*> sysDeferredStgMap;
        unordered_map<RowId, SysECol*> sysEColMap;
        unordered_map<RowId, SysSeg*> sysSegMap;
        unordered_map<RowId, SysTab*> sysTabMap;
        unordered_map<RowId, SysTabPart*> sysTabPartMap;
        unordered_map<RowId, SysTabComPart*> sysTabComPartMap;
        unordered_map<RowId, SysTabSubPart*> sysTabSubPartMap;

    public:
        OracleObject *object;
        vector<SchemaElement*> elements;

        Schema();
        virtual ~Schema();

        bool readSchema(OracleAnalyzer *oracleAnalyzer);
        void writeSchema(OracleAnalyzer *oracleAnalyzer);
        bool readSys(OracleAnalyzer *oracleAnalyzer);
        void writeSys(OracleAnalyzer *oracleAnalyzer);
        OracleObject *checkDict(typeOBJ obj, typeDATAOBJ dataObj);
        void addToDict(OracleObject *object);
        SchemaElement* addElement(void);
        bool dictSysUserAdd(const char *rowIdStr, typeUSER user, const char *name, uint64_t spare1);
        bool dictSysObjAdd(const char *rowIdStr, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char *name, uint32_t flags);
        bool dictSysColAdd(const char *rowIdStr, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char *name, typeTYPE type, uint64_t length,
                int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null, uint64_t property1, uint64_t property2);
        bool dictSysCColAdd(const char *rowIdStr, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare1);
        bool dictSysCDefAdd(const char *rowIdStr, typeCON con, typeOBJ obj, typeTYPE type);
        bool dictSysDeferredStgAdd(const char *rowIdStr, typeOBJ obj, uint64_t flagsStg);
        bool dictSysEColAdd(const char *rowIdStr, typeOBJ obj, uint32_t colNum, uint32_t guardId);
        bool dictSysSegAdd(const char *rowIdStr, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare1);
        bool dictSysTabAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block, typeCOL cluCols,
                uint64_t flags, uint64_t property1, uint64_t property2);
        bool dictSysTabPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabComPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabSubPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj);
    };
}

#endif
