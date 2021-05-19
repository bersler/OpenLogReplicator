/* Header for SysTabSubPart class
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

#ifndef SYSTABSUBPART_H_
#define SYSTABSUBPART_H_

using namespace std;

namespace OpenLogReplicator {
    class SysTabSubPartKey {
    public:
        SysTabSubPartKey();
        SysTabSubPartKey(typeOBJ pObj, typeOBJ obj);
        bool operator<(const SysTabSubPartKey& other) const;

        typeOBJ pObj;
        typeOBJ obj;
    };

    class SysTabSubPart {
    public:
        SysTabSubPart(RowId &rowId, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj, bool touched);

        RowId rowId;
        typeOBJ obj;
        typeDATAOBJ dataObj;        //NULL
        typeOBJ pObj;
        bool touched;
    };
}

#endif
