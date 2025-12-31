/* Definition of schema SYS.LOBCOMPPART$
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

#ifndef SYS_LOB_COMP_PART_H_
#define SYS_LOB_COMP_PART_H_

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysLobCompPart final {
    public:
        RowId rowId;
        typeObj partObj{0};
        typeObj lObj{0};

        SysLobCompPart(RowId newRowId, typeObj newPartObj, typeObj newLObj):
                rowId(newRowId),
                partObj(newPartObj),
                lObj(newLObj) {}

        explicit SysLobCompPart(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysLobCompPart& other) const {
            return (other.rowId != rowId) || (other.partObj != partObj) || (other.lObj != lObj);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.LOBCOMPPART$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", PARTOBJ#: " + std::to_string(partObj) + ", LOBJ#: " + std::to_string(lObj);
        }

        [[nodiscard]] static constexpr bool dependentTable() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLob() {
            return true;
        }

        [[nodiscard]] static constexpr bool dependentTableLobFrag() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTablePart() {
            return false;
        }

        [[nodiscard]] typeObj getDependentTableLob() const {
            return lObj;
        }
    };

    class SysLobCompPartKey final {
    public:
        typeObj lObj;
        typeObj partObj;

        SysLobCompPartKey(typeObj newLObj, typeObj newPartObj):
                lObj(newLObj),
                partObj(newPartObj) {}

        explicit SysLobCompPartKey(const SysLobCompPart* sysLobCompPart):
                lObj(sysLobCompPart->lObj),
                partObj(sysLobCompPart->partObj) {}

        bool operator<(SysLobCompPartKey other) const {
            if (lObj < other.lObj)
                return true;
            if (other.lObj < lObj)
                return false;
            if (partObj < other.partObj)
                return true;
            return false;
        }
    };

    class SysLobCompPartPartObj final {
    public:
        typeObj partObj;

        explicit SysLobCompPartPartObj(typeObj newPartObj):
                partObj(newPartObj) {}

        explicit SysLobCompPartPartObj(const SysLobCompPart* sysLobCompPart):
                partObj(sysLobCompPart->partObj) {}

        bool operator!=(const SysLobCompPartPartObj other) const {
            return (other.partObj != partObj);
        }

        bool operator==(const SysLobCompPartPartObj other) const {
            return (other.partObj == partObj);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysLobCompPartPartObj> {
    size_t operator()(const OpenLogReplicator::SysLobCompPartPartObj sysLobCompPartPartObj) const noexcept {
        return hash<typeObj>()(sysLobCompPartPartObj.partObj);
    }
};

#endif
