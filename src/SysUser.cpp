/* Definition of schema SYS.USER$
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

#include "SysUser.h"

namespace OpenLogReplicator {
    SysUser::SysUser(RowId &rowId, typeUSER user, const char *name, uint64_t spare11, uint64_t spare12, bool trackDDL, bool touched) :
            rowId(rowId),
            user(user),
            name(name),
            trackDDL(trackDDL),
            touched(touched) {
        spare1.set(spare11, spare12);
    }

    bool SysUser::isSuppLogPrimary(void) {
        return spare1.isSet64(1);
    }

    bool SysUser::isSuppLogAll(void) {
        return spare1.isSet64(8);
    }
}
