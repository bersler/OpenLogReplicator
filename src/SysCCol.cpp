/* Definition of schema SYS.CCOL$
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

#include "SysCCol.h"

namespace OpenLogReplicator {
    SysCColKey::SysCColKey() :
            con(0),
            intCol(0),
            obj(0) {
    }

    SysCColKey::SysCColKey(typeOBJ obj, typeCOL intCol, typeCON con) :
            obj(obj),
            intCol(intCol),
            con(con) {
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

    SysCCol::SysCCol(RowId &rowId, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare11, uint64_t spare12) :
                rowId(rowId),
                con(con),
                intCol(intCol),
                obj(obj) {
        spare1.set(spare11, spare12);
    }
}
