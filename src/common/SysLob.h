/* Header for SysLob class
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
#include "typeINTX.h"
#include "typeRowId.h"

#ifndef SYS_LOB_H_
#define SYS_LOB_H_

namespace OpenLogReplicator {
    class SysLobKey {
    public:
        SysLobKey(typeObj newObj, typeCol newIntCol);

        bool operator<(const SysLobKey& other) const;

        typeObj obj;
        typeCol intCol;
    };

    class SysLob {
    public:
        SysLob(typeRowId& newRowId, typeObj newObj, typeCol newCol, typeCol newIntCol, typeObj newLObj, bool newTouched);

        bool operator!=(const SysLob& other) const;

        typeRowId rowId;
        typeObj obj;
        typeCol col;
        typeCol intCol;
        typeObj lObj;
        bool touched;
    };
}

#endif
