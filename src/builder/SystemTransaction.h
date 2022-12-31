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

        bool updateNumber16(int16_t& val, int16_t defVal, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateNumber16u(uint16_t& val, uint16_t devVal, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateNumber64(int64_t& val, int64_t defVal, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateNumber64u(uint64_t& val, uint64_t devVal, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateNumberXu(typeIntX& val, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateObj(typeObj& val, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updatePartition(typeObj& val, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateLob(typeObj& val, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateUser(typeUser& val, typeCol column, OracleTable* table, typeRowId& rowId);
        bool updateString(std::string& val, uint64_t maxLength, typeCol column, OracleTable* table, typeRowId& rowId);
        void processInsertCCol(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertCDef(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertCol(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertDeferredStg(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertECol(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertLob(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertLobCompPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertLobFrag(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertObj(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertTab(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertTabComPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertTabPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertTabSubPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertTs(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processInsertUser(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateCCol(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateCDef(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateCol(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateDeferredStg(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateECol(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateLob(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateLobCompPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateLobFrag(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateObj(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateTab(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateTabComPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateTabPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateTabSubPart(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateTs(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);
        void processUpdateUser(OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeRowId& rowId);

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
