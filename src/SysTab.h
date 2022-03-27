/* Header for SysTab class
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

#ifndef SYSTAB_H_
#define SYSTAB_H_

#define SYSTAB_ROWID_LENGTH     18

namespace OpenLogReplicator {
    class SysTab {
    public:
        SysTab(RowId& rowId, typeOBJ obj, typeDATAOBJ dataObj, typeCOL cluCols, uint64_t flags1, uint64_t flags2, uint64_t property1,
                uint64_t property2, bool touched);
        bool operator!=(const SysTab& other) const;
        bool isBinary(void);
        bool isClustered(void);
        bool isIot(void);
        bool isPartitioned(void);
        bool isNested(void);
        bool isRowMovement(void);
        bool isDependencies(void);
        bool isInitial(void);

        RowId rowId;
        typeOBJ obj;
        typeDATAOBJ dataObj;        //NULL
        typeCOL cluCols;            //NULL
        uintX_t flags;
        uintX_t property;
        bool touched;
        bool saved;
    };
}

#endif
