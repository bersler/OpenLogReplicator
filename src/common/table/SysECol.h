/* Definition of schema SYS.ECOL$
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

#ifndef SYS_ECOL_H_
#define SYS_ECOL_H_

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysECol final {
    public:
        RowId rowId;
        typeObj tabObj{0};
        typeCol colNum{0};   // NULL
        typeCol guardId{-1}; // NULL

        SysECol(RowId newRowId, typeObj newTabObj, typeCol newColNum, typeCol newGuardId):
                rowId(newRowId),
                tabObj(newTabObj),
                colNum(newColNum),
                guardId(newGuardId) {}

        explicit SysECol(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysECol& other) const {
            return (other.rowId != rowId) || (other.tabObj != tabObj) || (other.colNum != colNum) || (other.guardId != guardId);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.ECOL$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", TABOBJ#: " + std::to_string(tabObj) + ", COLNUM: " + std::to_string(colNum) + ", GUARD_ID: " +
                    std::to_string(guardId);
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
            return tabObj;
        }
    };

    class SysEColKey final {
    public:
        typeObj tabObj;
        typeCol colNum;

        SysEColKey(typeObj newTabObj, typeCol newColNum):
                tabObj(newTabObj),
                colNum(newColNum) {}

        explicit SysEColKey(const SysECol* sysECol):
                tabObj(sysECol->tabObj),
                colNum(sysECol->colNum) {}

        bool operator!=(const SysEColKey other) const {
            return (other.tabObj != tabObj) || (other.colNum != colNum);
        }

        bool operator==(const SysEColKey other) const {
            return (other.tabObj == tabObj) && (other.colNum == colNum);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysEColKey> {
    size_t operator()(const OpenLogReplicator::SysEColKey sysEColKey) const noexcept {
        return hash<typeObj>()(sysEColKey.tabObj) ^ hash<typeCol>()(sysEColKey.colNum);
    }
};

#endif
