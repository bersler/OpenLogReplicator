/* Header for SysDeferredStg class
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

#ifndef SYSDEFERREDSTG_H_
#define SYSDEFERREDSTG_H_

using namespace std;

namespace OpenLogReplicator {
    class SysDeferredStg {
    public:
        SysDeferredStg(RowId &rowId, typeOBJ obj, uint64_t flagsStg1, uint64_t flagsStg2, bool touched);
        bool isCompressed(void);

        RowId rowId;
        typeOBJ obj;
        uintX_t flagsStg;          //NULL
        bool touched;
    };
}

#endif
