/* Definition of schema SYS.TABPART$
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef SYS_TAB_PART_H_
#define SYS_TAB_PART_H_

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysTabPart final {
    public:
        RowId rowId;
        typeObj obj{0};
        typeDataObj dataObj{0}; // NULL
        typeObj bo{0};

        SysTabPart(RowId newRowId, typeObj newObj, typeDataObj newDataObj, typeObj newBo):
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                bo(newBo) {}

        explicit SysTabPart(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysTabPart& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.bo != bo);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.TABPART$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", DATAOBJ#: " + std::to_string(dataObj) + ", BO#: " + std::to_string(bo);
        }

        [[nodiscard]] static constexpr bool dependentTable() {
            return true;
        }

        [[nodiscard]] static constexpr bool dependentTableLob() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLobFrag() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTablePart() {
            return false;
        }

        [[nodiscard]] typeObj getDependentTable() const {
            return bo;
        }
    };

    class SysTabPartKey final {
    public:
        typeObj bo;
        typeObj obj;

        SysTabPartKey(typeObj newBo, typeObj newObj):
                bo(newBo),
                obj(newObj) {}

        explicit SysTabPartKey(const SysTabPart* sysTabPart):
                bo(sysTabPart->bo),
                obj(sysTabPart->obj) {}

        bool operator<(const SysTabPartKey other) const {
            if (bo < other.bo)
                return true;
            if (other.bo < bo)
                return false;
            if (obj < other.obj)
                return true;
            return false;
        }
    };
}

#endif
