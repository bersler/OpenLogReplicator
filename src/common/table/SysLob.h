/* Definition of schema SYS.LOB$
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../types.h"
#include "../typeRowId.h"

#ifndef SYS_LOB_H_
#define SYS_LOB_H_

namespace OpenLogReplicator {
    class SysLob final {
    public:
        typeRowId rowId;
        typeObj obj;
        typeCol col;
        typeCol intCol;
        typeObj lObj;
        typeTs ts;

        SysLob(typeRowId newRowId, typeObj newObj, typeCol newCol, typeCol newIntCol, typeObj newLObj, typeTs newTs) :
                rowId(newRowId),
                obj(newObj),
                col(newCol),
                intCol(newIntCol),
                lObj(newLObj),
                ts(newTs) {
        }

        explicit SysLob(typeRowId newRowId) :
                rowId(newRowId),
                obj(0),
                col(0),
                intCol(0),
                lObj(0),
                ts(0) {
        }

        bool operator!=(const SysLob& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.col != col) || (other.intCol != intCol) || (other.lObj != lObj) || (other.ts != ts);
        }

        static std::string tableName() {
            return "SYS.LOB$";
        }

        std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", COL#: " + std::to_string(col) + ", INTCOL#: " + std::to_string(intCol) +
                   ", LOBJ#: " + std::to_string(lObj) + ", TS#: " + std::to_string(ts);
        }

        static constexpr bool dependentTable() {
            return true;
        }

        static constexpr bool dependentTableLob() {
            return false;
        }

        static constexpr bool dependentTableLobFrag() {
            return false;
        }

        static constexpr bool dependentTablePart() {
            return false;
        }

        typeObj getDependentTable() const {
            return obj;
        }
    };

    class SysLobKey final {
    public:
        typeObj obj;
        typeCol intCol;

        SysLobKey(typeObj newObj, typeCol newIntCol) :
                obj(newObj),
                intCol(newIntCol) {
        }

        explicit SysLobKey(const SysLob* sysLob) :
                obj(sysLob->obj),
                intCol(sysLob->intCol) {
        }

        bool operator<(const SysLobKey other) const {
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (intCol < other.intCol)
                return true;
            return false;
        }
    };

    class SysLobLObj final {
    public:
        typeObj lObj;

        explicit SysLobLObj(typeObj newLObj) :
                lObj(newLObj) {
        }

        explicit SysLobLObj(const SysLob* sysLob) :
                lObj(sysLob->lObj) {
        }

        bool operator!=(const SysLobLObj other) const {
            return (other.lObj != lObj);
        }

        bool operator==(const SysLobLObj other) const {
            return (other.lObj == lObj);
        }
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::SysLobLObj> {
        size_t operator()(const OpenLogReplicator::SysLobLObj sysLobLObj) const {
            return hash<typeObj>()(sysLobLObj.lObj);
        }
    };
}

#endif
