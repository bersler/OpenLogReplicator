/* Definition of schema SYS.ECOL$
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../types.h"
#include "../typeRowId.h"

#ifndef SYS_ECOL_H_
#define SYS_ECOL_H_

namespace OpenLogReplicator {
    class SysEColKey final {
    public:
        SysEColKey(typeObj newTabObj, typeCol newColNum) :
                tabObj(newTabObj),
                colNum(newColNum) {
        }

        bool operator!=(const SysEColKey& other) const {
            return (other.tabObj != tabObj) || (other.colNum != colNum);
        }

        bool operator==(const SysEColKey& other) const {
            return (other.tabObj == tabObj) && (other.colNum == colNum);
        }

        typeObj tabObj;
        typeCol colNum;
    };

    class SysECol final {
    public:
        SysECol(typeRowId& newRowId, typeObj newTabObj, typeCol newColNum, typeCol newGuardId) :
                rowId(newRowId),
                tabObj(newTabObj),
                colNum(newColNum),
                guardId(newGuardId) {
        }

        bool operator!=(const SysECol& other) const {
            return (other.rowId != rowId) || (other.tabObj != tabObj) || (other.colNum != colNum) || (other.guardId != guardId);
        }

        typeRowId rowId;
        typeObj tabObj;
        typeCol colNum;            // NULL
        typeCol guardId;           // NULL
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::SysEColKey> {
        size_t operator()(const OpenLogReplicator::SysEColKey& sysEColKey) const {
            return hash<typeObj>()(sysEColKey.tabObj) ^ hash<typeCol>()(sysEColKey.colNum);
        }
    };
}

#endif
