/* Definition of schema SYS.OBJ$
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

#include "SysObj.h"

namespace OpenLogReplicator {
    SysObj::SysObj(RowId& rowId, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char* name,
            uint64_t flags1, uint64_t flags2, bool single, bool touched) :
            rowId(rowId),
            owner(owner),
            obj(obj),
            dataObj(dataObj),
            type(type),
            name(name),
            single(single),
            touched(touched),
            saved(false) {
        flags.set(flags1, flags2);
    }

    bool SysObj::isTable(void) {
        return (type == SYSOBJ_TYPE_TABLE);
    }

    bool SysObj::isTemporary(void) {
        return flags.isSet64(2)         //temporary
                || flags.isSet64(16)    //secondary
                || flags.isSet64(32);   //in-memory temp
    }

    bool SysObj::isDropped(void) {
        return flags.isSet64(128);
    }
}
