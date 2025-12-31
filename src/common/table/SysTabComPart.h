/* Definition of schema SYS.TABCOMPART$
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

#ifndef SYS_TAB_COM_PART_H_
#define SYS_TAB_COM_PART_H_

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysTabComPart final {
    public:
        RowId rowId;
        typeObj obj{0};
        typeDataObj dataObj{0}; // NULL
        typeObj bo{0};

        SysTabComPart(RowId newRowId, typeObj newObj, typeDataObj newDataObj, typeObj newBo):
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                bo(newBo) {}

        explicit SysTabComPart(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysTabComPart& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.bo != bo);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.TABCOMPART$";
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

    class SysTabComPartKey final {
    public:
        typeObj bo;
        typeObj obj;

        SysTabComPartKey(typeObj newBo, typeObj newObj):
                bo(newBo),
                obj(newObj) {}

        explicit SysTabComPartKey(SysTabComPart* sysTabComPart):
                bo(sysTabComPart->bo),
                obj(sysTabComPart->obj) {}

        bool operator<(SysTabComPartKey other) const {
            if (bo < other.bo)
                return true;
            if (other.bo < bo)
                return false;
            if (obj < other.obj)
                return true;
            return false;
        }
    };

    class SysTabComPartObj final {
    public:
        typeObj obj;

        explicit SysTabComPartObj(typeObj newObj):
                obj(newObj) {}

        explicit SysTabComPartObj(const SysTabComPart* sysTabComPart):
                obj(sysTabComPart->obj) {}

        bool operator!=(const SysTabComPartObj other) const {
            return (other.obj != obj);
        }

        bool operator==(const SysTabComPartObj other) const {
            return (other.obj == obj);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysTabComPartObj> {
    size_t operator()(const OpenLogReplicator::SysTabComPartObj sysTabComPartObj) const noexcept {
        return hash<typeObj>()(sysTabComPartObj.obj);
    }
};

#endif
