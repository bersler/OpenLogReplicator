/* Definition of schema SYS.CDEF$
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

#include "SysCDef.h"

namespace OpenLogReplicator {
    SysCDefKey::SysCDefKey(typeObj newObj, typeCon newCon) :
            obj(newObj),
            con(newCon) {
    }

    bool SysCDefKey::operator<(const SysCDefKey& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj == obj && other.con > con)
            return true;
        return false;
    }

    SysCDef::SysCDef(typeRowId& newRowId, typeCon newCon, typeObj newObj, typeType newType, bool newTouched) :
            rowId(newRowId),
            con(newCon),
            obj(newObj),
            type(newType),
            touched(newTouched) {
    }

    bool SysCDef::operator!=(const SysCDef& other) const {
        return other.rowId != rowId || other.con != con || other.obj != obj || other.type != type;
    }

    bool SysCDef::isPK() const {
        return (type == SYS_CDEF_TYPE_PK);
    }

    bool SysCDef::isSupplementalLog() const {
        return (type == SYS_CDEF_TYPE_SUPPLEMENTAL_LOG);
    }

    bool SysCDef::isSupplementalLogPK() const {
        return (type == SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_PK);
    }

    bool SysCDef::isSupplementalLogAll() const {
        return (type == SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_ALL);
    }
}
