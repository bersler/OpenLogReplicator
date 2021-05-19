/* Definition of schema SYS.ECOL$
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

#include "SysECol.h"

namespace OpenLogReplicator {
    SysEColKey::SysEColKey() :
            tabObj(0),
            colNum(0) {
    }

    SysEColKey::SysEColKey(typeOBJ tabObj, typeCOL colNum) :
            tabObj(tabObj),
            colNum(colNum) {
    }


    bool SysEColKey::operator==(const SysEColKey& other) const {
        return (other.tabObj == tabObj) &&
                (other.colNum == colNum);
    }

    bool SysEColKey::operator!=(const SysEColKey& other) const {
        return (other.tabObj != tabObj) ||
                (other.colNum != colNum);
    }

    SysECol::SysECol(RowId &rowId, typeOBJ tabObj, typeCOL colNum, uint32_t guardId, bool touched) :
            rowId(rowId),
            tabObj(tabObj),
            colNum(colNum),
            guardId(guardId),
            touched(touched) {
    }
}

namespace std {
    size_t std::hash<OpenLogReplicator::SysEColKey>::operator()(const OpenLogReplicator::SysEColKey &sysEColKey) const {
        return hash<typeOBJ>()(sysEColKey.tabObj) ^
                hash<typeCOL>()(sysEColKey.colNum);
    }
}
