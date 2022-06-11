/* Header for SysTabComPart class
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

#ifndef SYSTABCOMPART_H_
#define SYSTABCOMPART_H_

namespace OpenLogReplicator {
    class SysTabComPartKey {
    public:
        SysTabComPartKey(typeObj newBo, typeObj newObj);

        bool operator<(const SysTabComPartKey& other) const;

        typeObj bo;
        typeObj obj;
    };

    class SysTabComPart {
    public:
        SysTabComPart(typeRowId& newRowId, typeObj newObj, typeDataObj newDataObj, typeObj newBo, bool newTouched);

        bool operator!=(const SysTabComPart& other) const;

        typeRowId rowId;
        typeObj obj;
        typeDataObj dataObj;        //NULL
        typeObj bo;
        bool touched;
    };
}

#endif
