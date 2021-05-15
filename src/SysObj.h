/* Header for SysObj class
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

#include "RowId.h"

#ifndef SYSOBJ_H_
#define SYSOBJ_H_

using namespace std;

namespace OpenLogReplicator {
    class SysObj {
    public:
        SysObj(RowId &rowId, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char *name,
                uint64_t flags1, uint64_t flags2);
        bool isTable(void);
        bool isTemporary(void);
        bool isDropped(void);

        RowId rowId;
        typeUSER owner;
        typeOBJ obj;
        typeDATAOBJ dataObj;        //NULL
        typeTYPE type;
        string name;
        uintX_t flags;             //NULL
    };
}

#endif
