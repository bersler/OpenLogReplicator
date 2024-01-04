/* Header for SystemTransaction class
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

#include <set>

#include "../common/types.h"
#include "../common/typeIntX.h"
#include "../common/typeRowId.h"
#include "../common/typeXid.h"

#ifndef SYSTEM_TRANSACTION_H_
#define SYSTEM_TRANSACTION_H_

namespace OpenLogReplicator {
    class Ctx;
    class OracleTable;
    class Builder;
    class Metadata;
    class SysCCol;
    class SysCDef;
    class SysCol;
    class SysDeferredStg;
    class SysECol;
    class SysLob;
    class SysLobCompPart;
    class SysLobFrag;
    class SysObj;
    class SysTab;
    class SysTabComPart;
    class SysTabPart;
    class SysTabSubPart;
    class SysTs;
    class SysUser;
    class XdbTtSet;
    class XdbXNm;
    class XdbXPt;
    class XdbXQn;

    class SystemTransaction final {
    protected:
        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;

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

        void updateNumber16(int16_t& val, int16_t defVal, typeCol column, const OracleTable* table, uint64_t offset);
        void updateNumber16u(uint16_t& val, uint16_t devVal, typeCol column, const OracleTable* table, uint64_t offset);
        void updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, const OracleTable* table, uint64_t offset);
        void updateNumber64(int64_t& val, int64_t defVal, typeCol column, const OracleTable* table, uint64_t offset);
        void updateNumber64u(uint64_t& val, uint64_t devVal, typeCol column, const OracleTable* table, uint64_t offset);
        void updateNumberXu(typeIntX& val, typeCol column, const OracleTable* table, uint64_t offset);
        void updateRaw(std::string& val, uint64_t maxLength, typeCol column, const OracleTable* table, uint64_t offset);
        void updateString(std::string& val, uint64_t maxLength, typeCol column, const OracleTable* table, uint64_t offset);

        void processInsertSysCCol(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysCDef(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysCol(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysDeferredStg(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysECol(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysLob(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysLobCompPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysLobFrag(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysObj(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTab(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTabComPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTabPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTabSubPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTs(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysUser(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbTtSet(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbXNm(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbXPt(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbXQn(const OracleTable* table, typeRowId rowId, uint64_t offset);

        void processUpdateSysCCol(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysCDef(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysCol(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysDeferredStg(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysECol(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysLob(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysLobCompPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysLobFrag(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysObj(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTab(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTabComPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTabPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTabSubPart(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTs(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysUser(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbTtSet(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbXNm(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbXPt(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbXQn(const OracleTable* table, typeRowId rowId, uint64_t offset);

        void processDeleteSysCCol(typeRowId rowId, uint64_t offset);
        void processDeleteSysCDef(typeRowId rowId, uint64_t offset);
        void processDeleteSysCol(typeRowId rowId, uint64_t offset);
        void processDeleteSysDeferredStg(typeRowId rowId, uint64_t offset);
        void processDeleteSysECol(typeRowId rowId, uint64_t offset);
        void processDeleteSysLob(typeRowId rowId, uint64_t offset);
        void processDeleteSysLobCompPart(typeRowId rowId, uint64_t offset);
        void processDeleteSysLobFrag(typeRowId rowId, uint64_t offset);
        void processDeleteSysObj(typeRowId rowId, uint64_t offset);
        void processDeleteSysTab(typeRowId rowId, uint64_t offset);
        void processDeleteSysTabComPart(typeRowId rowId, uint64_t offset);
        void processDeleteSysTabPart(typeRowId rowId, uint64_t offset);
        void processDeleteSysTabSubPart(typeRowId rowId, uint64_t offset);
        void processDeleteSysTs(typeRowId rowId, uint64_t offset);
        void processDeleteSysUser(typeRowId rowId, uint64_t offset);
        void processDeleteXdbTtSet(typeRowId rowId, uint64_t offset);
        void processDeleteXdbXNm(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processDeleteXdbXPt(const OracleTable* table, typeRowId rowId, uint64_t offset);
        void processDeleteXdbXQn(const OracleTable* table, typeRowId rowId, uint64_t offset);

    public:
        SystemTransaction(Builder* newBuilder, Metadata* newMetadata);
        ~SystemTransaction();

        void processInsert(const OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void processUpdate(const OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void processDelete(const OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void commit(typeScn scn);
    };
}

#endif
