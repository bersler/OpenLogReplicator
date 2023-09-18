/* Header for SystemTransaction class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

    class SystemTransaction final {
    protected:
        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;
        SysCCol* sysCCol;
        SysCDef* sysCDef;
        SysCol* sysCol;
        SysDeferredStg* sysDeferredStg;
        SysECol* sysECol;
        SysLob* sysLob;
        SysLobCompPart* sysLobCompPart;
        SysLobFrag* sysLobFrag;
        SysObj* sysObj;
        SysTab* sysTab;
        SysTabComPart* sysTabComPart;
        SysTabPart* sysTabPart;
        SysTabSubPart* sysTabSubPart;
        SysTs* sysTs;
        SysUser* sysUser;

        void updateNumber16(int16_t& val, int16_t defVal, typeCol column, OracleTable* table, uint64_t offset);
        void updateNumber16u(uint16_t& val, uint16_t devVal, typeCol column, OracleTable* table, uint64_t offset);
        void updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, OracleTable* table, uint64_t offset);
        void updateNumber64(int64_t& val, int64_t defVal, typeCol column, OracleTable* table, uint64_t offset);
        void updateNumber64u(uint64_t& val, uint64_t devVal, typeCol column, OracleTable* table, uint64_t offset);
        void updateNumberXu(typeIntX& val, typeCol column, OracleTable* table, uint64_t offset);
        void updateString(std::string& val, uint64_t maxLength, typeCol column, OracleTable* table, uint64_t offset);

        void processInsertCCol(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertCDef(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertCol(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertDeferredStg(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertECol(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertLob(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertLobCompPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertLobFrag(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertObj(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertTab(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertTabComPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertTabPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertTabSubPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertTs(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processInsertUser(OracleTable* table, typeRowId& rowId, uint64_t offset);

        void processUpdateCCol(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateCDef(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateCol(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateDeferredStg(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateECol(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateLob(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateLobCompPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateLobFrag(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateObj(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateTab(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateTabComPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateTabPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateTabSubPart(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateTs(OracleTable* table, typeRowId& rowId, uint64_t offset);
        void processUpdateUser(OracleTable* table, typeRowId& rowId, uint64_t offset);

        void processDeleteCCol(typeRowId& rowId, uint64_t offset);
        void processDeleteCDef(typeRowId& rowId, uint64_t offset);
        void processDeleteCol(typeRowId& rowId, uint64_t offset);
        void processDeleteDeferredStg(typeRowId& rowId, uint64_t offset);
        void processDeleteECol(typeRowId& rowId, uint64_t offset);
        void processDeleteLob(typeRowId& rowId, uint64_t offset);
        void processDeleteLobCompPart(typeRowId& rowId, uint64_t offset);
        void processDeleteLobFrag(typeRowId& rowId, uint64_t offset);
        void processDeleteObj(typeRowId& rowId, uint64_t offset);
        void processDeleteTab(typeRowId& rowId, uint64_t offset);
        void processDeleteTabComPart(typeRowId& rowId, uint64_t offset);
        void processDeleteTabPart(typeRowId& rowId, uint64_t offset);
        void processDeleteTabSubPart(typeRowId& rowId, uint64_t offset);
        void processDeleteTs(typeRowId& rowId, uint64_t offset);
        void processDeleteUser(typeRowId& rowId, uint64_t offset);

    public:
        SystemTransaction(Builder* newBuilder, Metadata* newMetadata);
        ~SystemTransaction();

        void processInsert(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void processUpdate(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void processDelete(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset);
        void commit(typeScn scn);
    };
}

#endif
