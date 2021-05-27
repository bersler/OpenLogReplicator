/* Definition of schema SYS.CDEF$
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

#include "SysCDef.h"

namespace OpenLogReplicator {
    SysCDefKey::SysCDefKey() :
            obj(0),
            con(0) {
    }

    SysCDefKey::SysCDefKey(typeOBJ obj, typeCON con) :
            obj(obj),
            con(con) {
    }

    bool SysCDefKey::operator<(const SysCDefKey& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj == obj && other.con > con)
            return true;
        return false;
    }

    SysCDef::SysCDef(RowId &rowId, typeCON con, typeOBJ obj, typeTYPE type, bool touched) :
            rowId(rowId),
            con(con),
            obj(obj),
            type(type),
            touched(touched),
            saved(false) {
    }

    bool SysCDef::isPK(void) {
        return (type == 2);
    }

    bool SysCDef::isSupplementalLog(void) {
        return (type == 12);
    }

    bool SysCDef::isSupplementalLogPK(void) {
        return (type == 14);
    }

    bool SysCDef::isSupplementalLogAll(void) {
        return (type == 17);
    }
}
