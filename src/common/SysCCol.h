/* Header for SysCCol class
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
#include "typeIntX.h"
#include "typeRowId.h"

#ifndef SYS_CCOL_H_
#define SYS_CCOL_H_

namespace OpenLogReplicator {
    class SysCColKey {
    public:
        SysCColKey(typeObj newObj, typeCol newIntCol, typeCon newCon);

        bool operator<(const SysCColKey& other) const;

        typeObj obj;
        typeCol intCol;
        typeCon con;
    };

    class SysCCol {
    public:
        SysCCol(typeRowId& newRowId, typeCon newCon, typeCol newIntCol, typeObj newObj, uint64_t newSpare11, uint64_t newSpare12, bool newTouched);

        bool operator!=(const SysCCol& other) const;

        typeRowId rowId;
        typeCon con;
        typeCol intCol;
        typeObj obj;
        typeIntX spare1;            // NULL
        bool touched;
    };
}

#endif
