/* Definition of schema SYS.LOB$
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

#include "SysLob.h"

namespace OpenLogReplicator {
    SysLobKey::SysLobKey(typeObj newObj, typeCol newIntCol) :
            obj(newObj),
            intCol(newIntCol) {
    }

    bool SysLobKey::operator<(const SysLobKey& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj == obj && other.intCol > intCol)
            return true;
        return false;
    }

    SysLob::SysLob(typeRowId& newRowId, typeObj newObj, typeCol newCol, typeCol newIntCol, typeObj newLObj, bool newTouched) :
            rowId(newRowId),
            obj(newObj),
            col(newCol),
            intCol(newIntCol),
            lObj(newLObj),
            touched(newTouched) {
    }

    bool SysLob::operator!=(const SysLob& other) const {
        return other.rowId != rowId || other.obj != obj || other.col != col || other.intCol != intCol || other.lObj != lObj;
    }
}
