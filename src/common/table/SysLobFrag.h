/* Definition of schema SYS.LOBFRAG$
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

#ifndef SYS_LOB_FRAG_H_
#define SYS_LOB_FRAG_H_

namespace OpenLogReplicator {
    class SysLobFrag final {
    public:
        typeRowId rowId;
        typeObj fragObj;
        typeObj parentObj;
        typeTs ts;

        SysLobFrag(typeRowId newRowId, typeObj newFragObj, typeObj newParentObj, typeTs newTs) :
                rowId(newRowId),
                fragObj(newFragObj),
                parentObj(newParentObj),
                ts(newTs) {
        }

        explicit SysLobFrag(typeRowId newRowId) :
                rowId(newRowId),
                fragObj(0),
                parentObj(0),
                ts(0) {
        }

        bool operator!=(const SysLobFrag& other) const {
            return (other.rowId != rowId) || (other.fragObj != fragObj) || (other.parentObj != parentObj) || (other.ts != ts);
        }

        static std::string tableName() {
            return "SYS.LOBFRAG$";
        }

        std::string toString() const {
            return "ROWID: " + rowId.toString() + ", FRAGOBJ#: " + std::to_string(fragObj) + ", PARENTOBJ#: " + std::to_string(parentObj) + ", TS#: " +
                   std::to_string(ts);
        }

        static constexpr bool dependentTable() {
            return false;
        }

        static constexpr bool dependentTableLob() {
            return true;
        }

        static constexpr bool dependentTableLobFrag() {
            return true;
        }

        static constexpr bool dependentTablePart() {
            return false;
        }

        typeObj getDependentTableLob() const {
            return parentObj;
        }

        typeObj getDependentTableLobFrag() const {
            return parentObj;
        }
    };

    class SysLobFragKey final {
    public:
        typeObj parentObj;
        typeObj fragObj;

        SysLobFragKey(typeObj newParentObj, typeObj newFragObj) :
                parentObj(newParentObj),
                fragObj(newFragObj) {
        }

        explicit SysLobFragKey(const SysLobFrag* sysLobFrag) :
                parentObj(sysLobFrag->parentObj),
                fragObj(sysLobFrag->fragObj) {
        }

        bool operator<(const SysLobFragKey other) const {
            if (parentObj < other.parentObj)
                return true;
            if (other.parentObj < parentObj)
                return false;
            if (fragObj < other.fragObj)
                return true;
            return false;
        }
    };
}

#endif
