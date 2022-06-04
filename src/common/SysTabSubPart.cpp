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
    SysTabSubPartKey::SysTabSubPartKey(typeObj pObj, typeObj obj) :
            pObj(pObj),
            obj(obj) {
    }

    bool SysTabSubPartKey::operator<(const SysTabSubPartKey& other) const {
        if (other.pObj > pObj)
            return true;
        if (other.pObj == pObj && other.obj > obj)
            return true;
        return false;
    }

    SysTabSubPart::SysTabSubPart(typeRowId& rowId, typeObj obj, typeDataObj dataObj, typeObj pObj, bool touched) :
            rowId(rowId),
            obj(obj),
            dataObj(dataObj),
            pObj(pObj),
            touched(touched) {
    }

    bool SysTabSubPart::operator!=(const SysTabSubPart& other) const {
        return other.rowId != rowId || other.obj != obj || other.dataObj != dataObj || other.pObj != pObj;
    }
}
