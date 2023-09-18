/* Definition of schema SYS.LOBFRAG$
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "types.h"
#include "typeRowId.h"

#ifndef SYS_LOB_FRAG_H_
#define SYS_LOB_FRAG_H_

namespace OpenLogReplicator {
    class SysLobFragKey final {
    public:
        SysLobFragKey(typeObj newParentObj, typeObj newFragObj) :
                parentObj(newParentObj),
                fragObj(newFragObj) {
        }

        bool operator<(const SysLobFragKey& other) const {
            if (parentObj < other.parentObj)
                return true;
            if (other.parentObj < parentObj)
                return false;
            if (fragObj < other.fragObj)
                return true;
            return false;
        }

        typeObj parentObj;
        typeObj fragObj;
    };

    class SysLobFrag final {
    public:
        SysLobFrag(typeRowId& newRowId, typeObj newFragObj, typeObj newParentObj, typeTs newTs) :
                rowId(newRowId),
                fragObj(newFragObj),
                parentObj(newParentObj),
                ts(newTs) {
        }

        bool operator!=(const SysLobFrag& other) const {
            return (other.rowId != rowId) || (other.fragObj != fragObj) || (other.parentObj != parentObj) || (other.ts != ts);
        }

        typeRowId rowId;
        typeObj fragObj;
        typeObj parentObj;
        typeTs ts;
    };
}

#endif
