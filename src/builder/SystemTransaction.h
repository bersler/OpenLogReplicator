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
#include "../common/table/SysCol.h"
#include "../common/table/TablePack.h"

#ifndef SYSTEM_TRANSACTION_H_
#define SYSTEM_TRANSACTION_H_

namespace OpenLogReplicator {
    class Ctx;
    class Builder;
    class DbTable;
    class Metadata;
    class SysCCol;
    class SysCDef;
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

    using Pack = TablePack<TabRowId, TabRowIdKey, TabRowIdUnorderedKey>;
    class SystemTransaction final {
    protected:
        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;

        template<class VALUE, SysCol::COLTYPE COLTYPE> void updateValue(VALUE& val, typeCol column, const DbTable* table, uint64_t offset, int defVal = 0,
                uint maxLength = 0);
        template<class TABLE> void updateValues(const DbTable* table, TABLE* row, typeCol column, uint64_t offset);
        template<class TABLE, class TABLEKEY, class TABLEUNORDEREDKEY>
                void updateAllValues(TablePack<TABLE, TABLEKEY, TABLEUNORDEREDKEY>* pack, const DbTable* table, TABLE* row, uint64_t offset);

        XmlCtx* findMatchingXmlCtx(const DbTable* table) const;

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
