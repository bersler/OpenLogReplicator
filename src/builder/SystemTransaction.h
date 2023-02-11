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

    class SystemTransaction {
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

        void updateNumber16(int16_t& val, int16_t defVal, typeCol column, OracleTable* table);
        void updateNumber16u(uint16_t& val, uint16_t devVal, typeCol column, OracleTable* table);
        void updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, OracleTable* table);
        void updateNumber64(int64_t& val, int64_t defVal, typeCol column, OracleTable* table);
        void updateNumber64u(uint64_t& val, uint64_t devVal, typeCol column, OracleTable* table);
        void updateNumberXu(typeIntX& val, typeCol column, OracleTable* table);
        void updateString(std::string& val, uint64_t maxLength, typeCol column, OracleTable* table);

        void processInsertCCol(OracleTable* table, typeRowId& rowId);
        void processInsertCDef(OracleTable* table, typeRowId& rowId);
        void processInsertCol(OracleTable* table, typeRowId& rowId);
        void processInsertDeferredStg(OracleTable* table, typeRowId& rowId);
        void processInsertECol(OracleTable* table, typeRowId& rowId);
        void processInsertLob(OracleTable* table, typeRowId& rowId);
        void processInsertLobCompPart(OracleTable* table, typeRowId& rowId);
        void processInsertLobFrag(OracleTable* table, typeRowId& rowId);
        void processInsertObj(OracleTable* table, typeRowId& rowId);
        void processInsertTab(OracleTable* table, typeRowId& rowId);
        void processInsertTabComPart(OracleTable* table, typeRowId& rowId);
        void processInsertTabPart(OracleTable* table, typeRowId& rowId);
        void processInsertTabSubPart(OracleTable* table, typeRowId& rowId);
        void processInsertTs(OracleTable* table, typeRowId& rowId);
        void processInsertUser(OracleTable* table, typeRowId& rowId);

        void processUpdateCCol(OracleTable* table, typeRowId& rowId);
        void processUpdateCDef(OracleTable* table, typeRowId& rowId);
        void processUpdateCol(OracleTable* table, typeRowId& rowId);
        void processUpdateDeferredStg(OracleTable* table, typeRowId& rowId);
        void processUpdateECol(OracleTable* table, typeRowId& rowId);
        void processUpdateLob(OracleTable* table, typeRowId& rowId);
        void processUpdateLobCompPart(OracleTable* table, typeRowId& rowId);
        void processUpdateLobFrag(OracleTable* table, typeRowId& rowId);
        void processUpdateObj(OracleTable* table, typeRowId& rowId);
        void processUpdateTab(OracleTable* table, typeRowId& rowId);
        void processUpdateTabComPart(OracleTable* table, typeRowId& rowId);
        void processUpdateTabPart(OracleTable* table, typeRowId& rowId);
        void processUpdateTabSubPart(OracleTable* table, typeRowId& rowId);
        void processUpdateTs(OracleTable* table, typeRowId& rowId);
        void processUpdateUser(OracleTable* table, typeRowId& rowId);

        void processDeleteCCol(typeRowId& rowId);
        void processDeleteCDef(typeRowId& rowId);
        void processDeleteCol(typeRowId& rowId);
        void processDeleteDeferredStg(typeRowId& rowId);
        void processDeleteECol(typeRowId& rowId);
        void processDeleteLob(typeRowId& rowId);
        void processDeleteLobCompPart(typeRowId& rowId);
        void processDeleteLobFrag(typeRowId& rowId);
        void processDeleteObj(typeRowId& rowId);
        void processDeleteTab(typeRowId& rowId);
        void processDeleteTabComPart(typeRowId& rowId);
        void processDeleteTabPart(typeRowId& rowId);
        void processDeleteTabSubPart(typeRowId& rowId);
        void processDeleteTs(typeRowId& rowId);
        void processDeleteUser(typeRowId& rowId);

    public:
        SystemTransaction(Builder* newBuilder, Metadata* newMetadata);
        ~SystemTransaction();

        void processInsert(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot);
        void processUpdate(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot);
        void processDelete(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot);
        void commit(typeScn scn);
    };
}

#endif
