/* Header for SysCDef class
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

#ifndef SYSCDEF_H_
#define SYSCDEF_H_

#define SYSCDEF_ROWID_LENGTH    18

namespace OpenLogReplicator {
    class SysCDefKey {
    public:
        SysCDefKey();
        SysCDefKey(typeOBJ obj, typeCON intCon);
        bool operator<(const SysCDefKey& other) const;

        typeOBJ obj;
        typeCON con;
    };

    class SysCDef {
    public:
        SysCDef(RowId& rowId, typeCON con, typeOBJ obj, typeTYPE type, bool touched);
        bool isPK(void);
        bool isSupplementalLog(void);
        bool isSupplementalLogPK(void);
        bool isSupplementalLogAll(void);

        RowId rowId;
        typeCON con;
        typeOBJ obj;
        typeTYPE type;
        bool touched;
        bool saved;
    };
}

#endif
