/* Definition of schema SYS.DEFERREDSTG$
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

#include "SysDeferredStg.h"

namespace OpenLogReplicator {
    SysDeferredStg::SysDeferredStg(RowId& rowId, typeOBJ obj, uint64_t flagsStg1, uint64_t flagsStg2, bool touched) :
            rowId(rowId),
            obj(obj),
            touched(touched),
            saved(false) {
        flagsStg.set(flagsStg1, flagsStg2);
    }

    bool SysDeferredStg::operator!=(const SysDeferredStg& other) const {
        if (other.rowId != rowId || other.obj != obj || other.flagsStg != flagsStg)
            return true;
        return false;
    }

    bool SysDeferredStg::isCompressed(void) {
        return flagsStg.isSet64(4);
    }
}
