/* Definition of schema SYS.LOBFRAG$
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

#include "SysLobFrag.h"

namespace OpenLogReplicator {
    SysLobFragKey::SysLobFragKey(typeObj newParentObj, typeObj newFragObj) :
            parentObj(newParentObj),
            fragObj(newFragObj) {
    }

    bool SysLobFragKey::operator<(const SysLobFragKey& other) const {
        if (other.parentObj > parentObj)
            return true;
        if (other.parentObj < parentObj)
            return false;
        if (other.fragObj > fragObj)
            return true;
        return false;
    }

    SysLobFrag::SysLobFrag(typeRowId& newRowId, typeObj newFragObj, typeObj newParentObj, typeTs newTs, bool newTouched) :
            rowId(newRowId),
            fragObj(newFragObj),
            parentObj(newParentObj),
            ts(newTs),
            touched(newTouched) {
    }

    bool SysLobFrag::operator!=(const SysLobFrag& other) const {
        return (other.rowId != rowId) || (other.fragObj != fragObj) || (other.parentObj != parentObj) || (other.ts != ts);
    }
}
