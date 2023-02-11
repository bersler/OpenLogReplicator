/* Definition of schema SYS.OBJ$
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

#include "SysObj.h"

namespace OpenLogReplicator {
    SysObjNameKey::SysObjNameKey(typeUser newOwner, const char* newName, typeObj newObj, typeDataObj newDataObj) :
            owner(newOwner),
            name(newName),
            obj(newObj),
            dataObj(newDataObj) {
    }

    bool SysObjNameKey::operator<(const SysObjNameKey& other) const {
        if (other.owner > owner)
            return true;
        if (other.owner < owner)
            return false;
        int cmp = other.name.compare(name);
        if (cmp > 0)
            return true;
        if (cmp < 0)
            return false;
        if (other.obj > obj)
            return true;
        if (other.obj < obj)
            return false;
        if (other.dataObj > dataObj)
            return true;
        if (other.dataObj < dataObj)
            return false;
        return false;
    }

    SysObj::SysObj(typeRowId& newRowId, typeUser newOwner, typeObj newObj, typeDataObj newDataObj, typeType newType, const char* newName, uint64_t newFlags1,
                   uint64_t newFlags2, bool newSingle) :
            rowId(newRowId),
            owner(newOwner),
            obj(newObj),
            dataObj(newDataObj),
            type(newType),
            name(newName),
            flags(newFlags1, newFlags2),
            single(newSingle) {
    }

    bool SysObj::operator!=(const SysObj& other) const {
        return (other.rowId != rowId) || (other.owner != owner) || (other.obj != obj) || (other.dataObj != dataObj) || (other.type != type) ||
                (other.name != name) || (other.flags != flags);
    }

    bool SysObj::isLob() const {
        return (type == SYS_OBJ_TYPE_LOB);
    }

    bool SysObj::isTable() const {
        return (type == SYS_OBJ_TYPE_TABLE);
    }

    bool SysObj::isTemporary() const {
        return flags.isSet64(SYS_OBJ_FLAGS_TEMPORARY) ||
                flags.isSet64(SYS_OBJ_FLAGS_SECONDARY) ||
                flags.isSet64(SYS_OBJ_FLAGS_IN_MEMORY_TEMP);
    }

    bool SysObj::isDropped() const {
        return flags.isSet64(SYS_OBJ_FLAGS_DROPPED);
    }
}
