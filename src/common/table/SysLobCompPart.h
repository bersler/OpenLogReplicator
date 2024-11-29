/* Definition of schema SYS.LOBCOMPPART$
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

#ifndef SYS_LOB_COMP_PART_H_
#define SYS_LOB_COMP_PART_H_

namespace OpenLogReplicator {
    class SysLobCompPart final {
    public:
        typeRowId rowId;
        typeObj partObj;
        typeObj lObj;

        SysLobCompPart(typeRowId newRowId, typeObj newPartObj, typeObj newLObj) :
                rowId(newRowId),
                partObj(newPartObj),
                lObj(newLObj) {
        }

        explicit SysLobCompPart(typeRowId newRowId) :
                rowId(newRowId),
                partObj(0),
                lObj(0) {
        }

        bool operator!=(const SysLobCompPart& other) const {
            return (other.rowId != rowId) || (other.partObj != partObj) || (other.lObj != lObj);
        }

        static std::string tableName() {
            return "SYS.LOBCOMPPART$";
        }

        std::string toString() const {
            return "ROWID: " + rowId.toString() + ", PARTOBJ#: " + std::to_string(partObj) + ", LOBJ#: " + std::to_string(lObj);
        }

        static constexpr bool dependentTable() {
            return false;
        }

        static constexpr bool dependentTableLob() {
            return true;
        }

        static constexpr bool dependentTableLobFrag() {
            return false;
        }

        static constexpr bool dependentTablePart() {
            return false;
        }

        typeObj getDependentTableLob() const {
            return lObj;
        }
    };

    class SysLobCompPartKey final {
    public:
        typeObj lObj;
        typeObj partObj;

        SysLobCompPartKey(typeObj newLObj, typeObj newPartObj) :
                lObj(newLObj),
                partObj(newPartObj) {
        }

        explicit SysLobCompPartKey(const SysLobCompPart* sysLobCompPart) :
                lObj(sysLobCompPart->lObj),
                partObj(sysLobCompPart->partObj) {
        }

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

        explicit SysLobCompPartPartObj(typeObj newPartObj) :
                partObj(newPartObj) {
        }

        explicit SysLobCompPartPartObj(const SysLobCompPart* sysLobCompPart) :
                partObj(sysLobCompPart->partObj) {
        }

        bool operator!=(const SysLobCompPartPartObj other) const {
            return (other.partObj != partObj);
        }

        bool operator==(const SysLobCompPartPartObj other) const {
            return (other.partObj == partObj);
        }
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::SysLobCompPartPartObj> {
        size_t operator()(const OpenLogReplicator::SysLobCompPartPartObj sysLobCompPartPartObj) const {
            return hash<typeObj>()(sysLobCompPartPartObj.partObj);
        }
    };
}

#endif
