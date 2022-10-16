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
#include "typeIntX.h"
#include "typeRowId.h"

#ifndef SYS_USER_H_
#define SYS_USER_H_

#define SYS_USER_NAME_LENGTH                 128
#define SYS_USER_SPARE1_SUPP_LOG_PRIMARY     1
#define SYS_USER_SPARE1_SUPP_LOG_ALL         8

namespace OpenLogReplicator {
    class SysUser {
    public:
        SysUser(typeRowId& newRowId, typeUser newUser, const char* newName, uint64_t newSpare11, uint64_t newSpare12, bool newSingle, bool newTouched);

        bool operator!=(const SysUser& other) const;
        [[nodiscard]] bool isSuppLogPrimary();
        [[nodiscard]] bool isSuppLogAll();

        typeRowId rowId;
        typeUser user;
        std::string name;
        typeIntX spare1;            // NULL
        bool single;
        bool touched;
    };
}

#endif
