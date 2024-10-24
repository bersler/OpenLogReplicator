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
    class Builder;
    class DbTable;
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

        void updateNumber(int& val, int defVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumberu(uint& val, uint devVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumber16(int16_t& val, int16_t defVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumber16u(uint16_t& val, uint16_t devVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumber64(int64_t& val, int64_t defVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumber64u(uint64_t& val, uint64_t devVal, typeCol column, const DbTable* table, uint64_t offset);
        void updateNumberXu(typeIntX& val, typeCol column, const DbTable* table, uint64_t offset);
        void updateRaw(std::string& val, uint maxLength, typeCol column, const DbTable* table, uint64_t offset);
        void updateString(std::string& val, uint maxLength, typeCol column, const DbTable* table, uint64_t offset);

        void processInsertSysCCol(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysCDef(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysCol(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysDeferredStg(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysECol(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysLob(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysLobCompPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysLobFrag(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysObj(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTab(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTabComPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTabPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTabSubPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysTs(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertSysUser(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbTtSet(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbXNm(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbXPt(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processInsertXdbXQn(const DbTable* table, typeRowId rowId, uint64_t offset);

        void processUpdateSysCCol(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysCDef(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysCol(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysDeferredStg(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysECol(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysLob(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysLobCompPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysLobFrag(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysObj(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTab(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTabComPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTabPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTabSubPart(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysTs(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateSysUser(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbTtSet(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbXNm(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbXPt(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processUpdateXdbXQn(const DbTable* table, typeRowId rowId, uint64_t offset);

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
        void processDeleteXdbXNm(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processDeleteXdbXPt(const DbTable* table, typeRowId rowId, uint64_t offset);
        void processDeleteXdbXQn(const DbTable* table, typeRowId rowId, uint64_t offset);

    public:
        SystemTransaction(Builder* newBuilder, Metadata* newMetadata);
        ~SystemTransaction();

        void processInsert(const DbTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void processUpdate(const DbTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void processDelete(const DbTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void commit(typeScn scn);
    };
}

#endif
