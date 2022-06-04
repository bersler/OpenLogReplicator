/* Definition of schema SYS.OBJ$
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

#include "SysObj.h"

namespace OpenLogReplicator {
    SysObj::SysObj(typeRowId& rowId, typeUser owner, typeObj obj, typeDataObj dataObj, typeType type, const char* name,
                   uint64_t flags1, uint64_t flags2, bool single, bool touched) :
            rowId(rowId),
            owner(owner),
            obj(obj),
            dataObj(dataObj),
            type(type),
            name(name),
            flags(flags1, flags2),
            single(single),
            touched(touched) {
    }

    bool SysObj::operator!=(const SysObj& other) const {
        return other.rowId != rowId || other.owner != owner || other.obj != obj || other.dataObj != dataObj || other.type != type || other.name != name ||
                other.flags != flags;
    }

    bool SysObj::isTable() const {
        return (type == SYSOBJ_TYPE_TABLE);
    }

    bool SysObj::isTemporary() {
        return flags.isSet64(SYSOBJ_FLAGS_TEMPORARY)
                || flags.isSet64(SYSOBJ_FLAGS_SECONDARY)
                || flags.isSet64(SYSOBJ_FLAGS_IN_MEMORY_TEMP);
    }

    bool SysObj::isDropped() {
        return flags.isSet64(SYSOBJ_FLAGS_DROPPED);
    }
}
