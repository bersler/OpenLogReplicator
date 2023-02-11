/* Definition of schema SYS.USER$
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "SysUser.h"

namespace OpenLogReplicator {
    SysUser::SysUser(typeRowId& newRowId, typeUser newUser, const char* newName, uint64_t newSpare11, uint64_t newSpare12, bool newSingle) :
            rowId(newRowId),
            user(newUser),
            name(newName),
            spare1(newSpare11, newSpare12),
            single(newSingle) {
    }

    bool SysUser::operator!=(const SysUser& other) const {
        return (other.rowId != rowId) || (other.user != user) || (other.name != name) || (other.spare1 != spare1);
    }

    bool SysUser::isSuppLogPrimary() {
        return spare1.isSet64(SYS_USER_SPARE1_SUPP_LOG_PRIMARY);
    }

    bool SysUser::isSuppLogAll() {
        return spare1.isSet64(SYS_USER_SPARE1_SUPP_LOG_ALL);
    }
}
