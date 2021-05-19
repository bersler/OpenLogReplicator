/* Definition of schema SYS.TABPART$
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

#include "SysTabPart.h"

namespace OpenLogReplicator {
    SysTabPartKey::SysTabPartKey() :
            bo(0),
            obj(0) {
    }

    SysTabPartKey::SysTabPartKey(typeOBJ bo, typeOBJ obj) :
            bo(bo),
            obj(obj) {
    }


    bool SysTabPartKey::operator<(const SysTabPartKey& other) const {
        if (other.bo > bo)
            return true;
        if (other.bo == bo && other.obj > obj)
            return true;
        return false;
    }

    SysTabPart::SysTabPart(RowId &rowId, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo, bool touched) :
            rowId(rowId),
            obj(obj),
            dataObj(dataObj),
            bo(bo),
            touched(touched) {
    }
}
