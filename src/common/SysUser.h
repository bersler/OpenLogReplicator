/* Header for SysUser class
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
#include "typeINTX.h"
#include "typeRowId.h"

#ifndef SYSUSER_H_
#define SYSUSER_H_

#define SYSUSER_NAME_LENGTH                 128
#define SYSUSER_SPARE1_SUPP_LOG_PRIMARY     1
#define SYSUSER_SPARE1_SUPP_LOG_ALL         8

namespace OpenLogReplicator {
    class SysUser {
    public:
        SysUser(typeRowId& rowId, typeUser user, const char* name, uint64_t spare11, uint64_t spare12, bool single, bool touched);

        bool operator!=(const SysUser& other) const;
        [[nodiscard]] bool isSuppLogPrimary();
        [[nodiscard]] bool isSuppLogAll();

        typeRowId rowId;
        typeUser user;
        std::string name;
        typeINTX spare1;            //NULL
        bool single;
        bool touched;
    };
}

#endif
