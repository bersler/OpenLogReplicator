/* Header for SysECol class
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

#ifndef SYSECOL_H_
#define SYSECOL_H_

using namespace std;

namespace OpenLogReplicator {
    class SysEColKey {
    public:
        SysEColKey();
        SysEColKey(typeOBJ tabObj, typeCOL colNum);
        bool operator!=(const SysEColKey& other) const;
        bool operator==(const SysEColKey& other) const;

        typeOBJ tabObj;
        typeCOL colNum;
    };

    class SysECol {
    public:
        SysECol(RowId &rowId, typeOBJ tabObj, typeCOL colNum, typeCOL guardId, bool touched);

        RowId rowId;
        typeOBJ tabObj;
        typeCOL colNum;            //NULL
        typeCOL guardId;           //NULL
        bool touched;
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::SysEColKey> {
        size_t operator()(const OpenLogReplicator::SysEColKey &sysEColKey) const;
    };
}

#endif
