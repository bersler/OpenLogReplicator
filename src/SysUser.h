/* Header for SysUser class
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

#ifndef SYSUSER_H_
#define SYSUSER_H_

#define SYSUSER_ROWID_LENGTH     18
#define SYSUSER_NAME_LENGTH      128

using namespace std;

namespace OpenLogReplicator {
    class SysUser {
    public:
        SysUser(RowId& rowId, typeUSER user, const char* name, uint64_t spare11, uint64_t spare12, bool single, bool touched);
        bool isSuppLogPrimary(void);
        bool isSuppLogAll(void);

        RowId rowId;
        typeUSER user;
        string name;
        uintX_t spare1;            //NULL
        bool single;
        bool touched;
        bool saved;
    };
}

#endif
