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

#include "RowId.h"

#ifndef SYSCCOL_H_
#define SYSCCOL_H_

#define SYSCCOL_ROWID_LENGTH    18

namespace OpenLogReplicator {
    class SysCColKey {
    public:
        SysCColKey();
        SysCColKey(typeOBJ obj, typeCOL intCol, typeCON con);
        bool operator<(const SysCColKey& other) const;

        typeOBJ obj;
        typeCOL intCol;
        typeCON con;
    };

    class SysCCol {
    public:
        SysCCol(RowId& rowId, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare11, uint64_t spare12, bool touched);

        RowId rowId;
        typeCON con;
        typeCOL intCol;
        typeOBJ obj;
        uintX_t spare1;            //NULL
        bool touched;
        bool saved;
    };
}

#endif
