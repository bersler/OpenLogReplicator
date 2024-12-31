/* Definition of schema XDB.X$PTxxx
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <utility>

#include "../types.h"
#include "../typeRowId.h"

#ifndef XDB_XPT_H_
#define XDB_XPT_H_

namespace OpenLogReplicator {
    class XdbXPt final {
    public:
        static constexpr uint PATH_LENGTH{2000};
        static constexpr uint ID_LENGTH{16};

        typeRowId rowId;
        std::string path;
        std::string id;

        XdbXPt(typeRowId newRowId, const char* newPath, const char* newId) :
                rowId(newRowId),
                path(newPath),
                id(newId) {
        }

        explicit XdbXPt(typeRowId newRowId) :
                rowId(newRowId) {
        }

        bool operator!=(const XdbXPt& other) const {
            return (other.rowId != rowId) || (other.path != path) || (other.id != id);
        }

        [[nodiscard]] static std::string tableName() {
            return "XDB.X$PT";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", PATH: '" + path + "', ID: '" + id + "'";
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
            return false;
        }
    };

    class XdbXPtKey final {
    public:
        std::string id;

        explicit XdbXPtKey(std::string newId) :
                id(std::move(newId)) {
        }

        explicit XdbXPtKey(const XdbXPt* xdbXPt) :
                id(xdbXPt->id) {
        }

        bool operator!=(const XdbXPtKey& other) const {
            return (other.id != id);
        }

        bool operator==(const XdbXPtKey& other) const {
            return (other.id == id);
        }
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::XdbXPtKey> {
        size_t operator()(const OpenLogReplicator::XdbXPtKey& xdbXPtKey) const {
            return hash<std::string>()(xdbXPtKey.id);
        }
    };
}

#endif
