/* Definition of schema SYS.TABSUBPART$
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

#ifndef SYS_TAB_SUB_PART_H_
#define SYS_TAB_SUB_PART_H_

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysTabSubPart final {
    public:
        RowId rowId;
        typeObj obj{0};
        typeDataObj dataObj{0}; // NULL
        typeObj pObj{0};

        SysTabSubPart(RowId newRowId, typeObj newObj, typeDataObj newDataObj, typeObj newPObj):
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                pObj(newPObj) {}

        explicit SysTabSubPart(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysTabSubPart& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.pObj != pObj);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.TABSUBPART$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", DATAOBJ#: " + std::to_string(dataObj) + ", POBJ#: " +
                    std::to_string(pObj);
        }

        [[nodiscard]] static constexpr bool dependentTable() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLob() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLobFrag() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTablePart() {
            return true;
        }

        [[nodiscard]] typeObj getDependentTablePart() const {
            return obj;
        }
    };

    class SysTabSubPartKey final {
    public:
        typeObj pObj;
        typeObj obj;

        SysTabSubPartKey(typeObj newPObj, typeObj newObj):
                pObj(newPObj),
                obj(newObj) {}

        explicit SysTabSubPartKey(const SysTabSubPart* sysTabSubPart):
                pObj(sysTabSubPart->pObj),
                obj(sysTabSubPart->obj) {}

        bool operator<(const SysTabSubPartKey other) const {
            if (pObj < other.pObj)
                return true;
            if (other.pObj < pObj)
                return false;
            if (obj < other.obj)
                return true;
            return false;
        }
    };
}

#endif
