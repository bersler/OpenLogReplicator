/* Header for SystemTransaction class
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

#include <set>

#include "../common/types.h"
#include "../common/typeINTX.h"
#include "../common/typeRowId.h"
#include "../common/typeXid.h"

#ifndef SYSTEM_TRANSACTION_H_
#define SYSTEM_TRANSACTION_H_

namespace OpenLogReplicator {
    class Ctx;
    class OracleObject;
    class Builder;
    class Metadata;
    class SysCCol;
    class SysCDef;
    class SysCol;
    class SysDeferredStg;
    class SysECol;
    class SysObj;
    class SysTab;
    class SysTabComPart;
    class SysTabPart;
    class SysTabSubPart;
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
        SysObj* sysObj;
        SysTab* sysTab;
        SysTabComPart* sysTabComPart;
        SysTabPart* sysTabPart;
        SysTabSubPart* sysTabSubPart;
        SysUser* sysUser;

        bool updateNumber16(int16_t& val, int16_t defVal, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateNumber16u(uint16_t& val, uint16_t devVal, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateNumber64(int64_t& val, int64_t defVal, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateNumber64u(uint64_t& val, uint64_t devVal, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateNumberXu(typeINTX& val, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateObj(typeObj& val, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updatePart(typeObj& val, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateUser(typeUser& val, typeCol column, OracleObject* object, typeRowId& rowId);
        bool updateString(std::string& val, uint64_t maxLength, typeCol column, OracleObject* object, typeRowId& rowId);

    public:
        SystemTransaction(Builder* newBuilder, Metadata* newMetadata);
        ~SystemTransaction();

        void processInsert(OracleObject* object, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid);
        void processUpdate(OracleObject* object, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid);
        void processDelete(OracleObject* object, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid);
        void commit(typeScn scn);
    };
}

#endif
