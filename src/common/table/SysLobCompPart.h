/* Definition of schema SYS.LOBCOMPPART$
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

#ifndef SYS_LOB_COMP_PART_H_
#define SYS_LOB_COMP_PART_H_

namespace OpenLogReplicator {
    class SysLobCompPartKey final {
    public:
        SysLobCompPartKey(typeObj newLObj, typeObj newPartObj) :
                lObj(newLObj),
                partObj(newPartObj) {
        }

        bool operator<(const SysLobCompPartKey& other) const {
            if (lObj < other.lObj)
                return true;
            if (other.lObj < lObj)
                return false;
            if (partObj < other.partObj)
                return true;
            return false;
        }

        typeObj lObj;
        typeObj partObj;
    };

    class SysLobCompPart final {
    public:
        SysLobCompPart(typeRowId& newRowId, typeObj newPartObj, typeObj newLObj) :
                rowId(newRowId),
                partObj(newPartObj),
                lObj(newLObj) {
        }

        bool operator!=(const SysLobCompPart& other) const {
            return (other.rowId != rowId) || (other.partObj != partObj) || (other.lObj != lObj);
        }

        typeRowId rowId;
        typeObj partObj;
        typeObj lObj;
    };
}

#endif
