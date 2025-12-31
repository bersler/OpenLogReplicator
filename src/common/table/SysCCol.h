/* Definition of schema SYS.CCOL$
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

#ifndef SYS_CCOL_H_
#define SYS_CCOL_H_

#include "../Ctx.h"
#include "../types/IntX.h"
#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysCCol final : public TabRowId {
    public:
        typeCon con{0};
        typeCol intCol{0};
        typeObj obj{0};
        IntX spare1{0, 0}; // NULL

        SysCCol(RowId newRowId, typeCon newCon, typeCol newIntCol, typeObj newObj, uint64_t newSpare11, uint64_t newSpare12):
                TabRowId(newRowId),
                con(newCon),
                intCol(newIntCol),
                obj(newObj),
                spare1(newSpare11, newSpare12) {}

        explicit SysCCol(RowId newRowId):
                TabRowId(newRowId) {}

        bool operator!=(const SysCCol& other) const {
            return (other.rowId != rowId) || (other.con != con) || (other.intCol != intCol) || (other.obj != obj) || (other.spare1 != spare1);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.CCOL$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", CON#: " + std::to_string(con) + ", INTCOL#: " + std::to_string(intCol) + ", OBJ#: " +
                    std::to_string(obj) + ", SPARE1: " + spare1.toString();
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
            return obj;
        }
    };

    class SysCColKey final : public TabRowIdKey {
    public:
        typeObj obj;
        typeCon con;
        typeCol intCol;

        SysCColKey(typeObj newObj, typeCon newCon, typeCol newIntCol):
                obj(newObj),
                con(newCon),
                intCol(newIntCol) {}

        explicit SysCColKey(const SysCCol* sysCCol):
                obj(sysCCol->obj),
                con(sysCCol->con),
                intCol(sysCCol->intCol) {}

        bool operator<(const SysCColKey other) const {
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (intCol < other.intCol)
                return true;
            if (other.intCol < intCol)
                return false;
            if (con < other.con)
                return true;
            return false;
        }
    };
}

#endif
