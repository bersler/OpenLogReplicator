/* Definition of schema SYS.TABPART$
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

#include "types.h"
#include "typeRowId.h"

#ifndef SYS_TAB_PART_H_
#define SYS_TAB_PART_H_

namespace OpenLogReplicator {
    class SysTabPartKey {
    public:
        SysTabPartKey(typeObj newBo, typeObj newObj) :
                bo(newBo),
                obj(newObj) {
        }

        bool operator<(const SysTabPartKey& other) const {
            if (bo < other.bo)
                return true;
            if (other.bo < bo)
                return false;
            if (obj < other.obj)
                return true;
            return false;
        }

        typeObj bo;
        typeObj obj;
    };

    class SysTabPart {
    public:
        SysTabPart(typeRowId& newRowId, typeObj newObj, typeDataObj newDataObj, typeObj newBo) :
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                bo(newBo) {
        }

        bool operator!=(const SysTabPart& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.bo != bo);
        }

        typeRowId rowId;
        typeObj obj;
        typeDataObj dataObj;        // NULL
        typeObj bo;
    };
}

#endif
