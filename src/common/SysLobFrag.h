/* Header for SysLobFrag class
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

#include "types.h"
#include "typeRowId.h"

#ifndef SYS_LOB_FRAG_H_
#define SYS_LOB_FRAG_H_

namespace OpenLogReplicator {
    class SysLobFragKey {
    public:
        SysLobFragKey(typeObj newParentObj, typeObj newFragObj);

        bool operator<(const SysLobFragKey& other) const;

        typeObj parentObj;
        typeObj fragObj;
    };

    class SysLobFrag {
    public:
        SysLobFrag(typeRowId& newRowId, typeObj newFragObj, typeObj newParentObj, typeTs newTs, bool newTouched);

        bool operator!=(const SysLobFrag& other) const;

        typeRowId rowId;
        typeObj fragObj;
        typeObj parentObj;
        typeTs ts;
        bool touched;
    };
}

#endif
