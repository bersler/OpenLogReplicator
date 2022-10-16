/* Definition of schema SYS.TABSUBPART$
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

#include "SysTabSubPart.h"

namespace OpenLogReplicator {
    SysTabSubPartKey::SysTabSubPartKey(typeObj newPObj, typeObj newObj) :
            pObj(newPObj),
            obj(newObj) {
    }

    bool SysTabSubPartKey::operator<(const SysTabSubPartKey& other) const {
        if (other.pObj > pObj)
            return true;
        if (other.pObj < pObj)
            return false;
        if (other.obj > obj)
            return true;
        return false;
    }

    SysTabSubPart::SysTabSubPart(typeRowId& newRowId, typeObj newObj, typeDataObj newDataObj, typeObj newPObj, bool newTouched) :
            rowId(newRowId),
            obj(newObj),
            dataObj(newDataObj),
            pObj(newPObj),
            touched(newTouched) {
    }

    bool SysTabSubPart::operator!=(const SysTabSubPart& other) const {
        return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.pObj != pObj);
    }
}
