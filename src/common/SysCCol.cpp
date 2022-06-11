/* Definition of schema SYS.CCOL$
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

#include "SysCCol.h"

namespace OpenLogReplicator {
    SysCColKey::SysCColKey(typeObj newObj, typeCol newIntCol, typeCon newCon) :
            obj(newObj),
            intCol(newIntCol),
            con(newCon) {
    }

    bool SysCColKey::operator<(const SysCColKey& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj == obj) {
            if (other.intCol > intCol)
                return true;
            if (other.intCol == intCol) {
                if (other.con > con)
                    return true;
            }
        }
        return false;
    }

    bool SysCCol::operator!=(const SysCCol& other) const {
        return other.rowId != rowId || other.con != con || other.intCol != intCol || other.obj != obj || other.spare1 != spare1;
    }

    SysCCol::SysCCol(typeRowId& newRowId, typeCon newCon, typeCol newIntCol, typeObj newObj, uint64_t newSpare11, uint64_t newSpare12, bool newTouched) :
                rowId(newRowId),
                con(newCon),
                intCol(newIntCol),
                obj(newObj),
                spare1(newSpare11, newSpare12),
                touched(newTouched) {
    }
}
