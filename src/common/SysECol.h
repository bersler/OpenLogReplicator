/* Header for SysECol class
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

#ifndef SYSECOL_H_
#define SYSECOL_H_

namespace OpenLogReplicator {
    class SysEColKey {
    public:
        SysEColKey(typeObj tabObj, typeCol colNum);

        bool operator!=(const SysEColKey& other) const;
        bool operator==(const SysEColKey& other) const;

        typeObj tabObj;
        typeCol colNum;
    };

    class SysECol {
    public:
        SysECol(typeRowId& rowId, typeObj tabObj, typeCol colNum, typeCol guardId, bool touched);

        bool operator!=(const SysECol& other) const;

        typeRowId rowId;
        typeObj tabObj;
        typeCol colNum;            //NULL
        typeCol guardId;           //NULL
        bool touched;
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::SysEColKey> {
        size_t operator()(const OpenLogReplicator::SysEColKey& sysEColKey) const;
    };
}

#endif
