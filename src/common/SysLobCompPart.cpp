/* Definition of schema SYS.LOBCOMPPART$
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

#include "SysLobCompPart.h"

namespace OpenLogReplicator {
    SysLobCompPartKey::SysLobCompPartKey(typeObj newLObj, typeObj newPartObj) :
            lObj(newLObj),
            partObj(newPartObj) {
    }

    bool SysLobCompPartKey::operator<(const SysLobCompPartKey& other) const {
        if (other.lObj > lObj)
            return true;
        if (other.lObj < lObj)
            return false;
        if (other.partObj > partObj)
            return true;
        return false;
    }

    SysLobCompPart::SysLobCompPart(typeRowId& newRowId, typeObj newPartObj, typeObj newLObj, bool newTouched) :
            rowId(newRowId),
            partObj(newPartObj),
            lObj(newLObj),
            touched(newTouched) {
    }

    bool SysLobCompPart::operator!=(const SysLobCompPart& other) const {
        return (other.rowId != rowId) || (other.partObj != partObj) || (other.lObj != lObj);
    }
}
