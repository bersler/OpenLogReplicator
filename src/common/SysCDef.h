/* Header for SysCDef class
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

#ifndef SYSCDEF_H_
#define SYSCDEF_H_

#define SYSCDEF_TYPE_PK                     2
#define SYSCDEF_TYPE_SUPPLEMENTAL_LOG       12
#define SYSCDEF_TYPE_SUPPLEMENTAL_LOG_PK    14
#define SYSCDEF_TYPE_SUPPLEMENTAL_LOG_ALL   17

namespace OpenLogReplicator {
    class SysCDefKey {
    public:
        SysCDefKey(typeObj newObj, typeCon newIntCon);

        bool operator<(const SysCDefKey& other) const;

        typeObj obj;
        typeCon con;
    };

    class SysCDef {
    public:
        SysCDef(typeRowId& newRowId, typeCon newCon, typeObj newObj, typeType newType, bool newTouched);

        [[nodiscard]] bool operator!=(const SysCDef& other) const;
        [[nodiscard]] bool isPK() const;
        [[nodiscard]] bool isSupplementalLog() const;
        [[nodiscard]] bool isSupplementalLogPK() const;
        [[nodiscard]] bool isSupplementalLogAll() const;

        typeRowId rowId;
        typeCon con;
        typeObj obj;
        typeType type;
        bool touched;
    };
}

#endif
