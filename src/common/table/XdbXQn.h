/* Definition of schema XDB.X$QNxxx
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

#ifndef XDB_XQN_H_
#define XDB_XQN_H_

#include <utility>

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class XdbXQn final {
    public:
        static constexpr uint NMSPCID_LENGTH{16};
        static constexpr uint LOCALNAME_LENGTH{2000};
        static constexpr uint FLAGS_LENGTH{8};
        static constexpr uint ID_LENGTH{16};
        static constexpr uint64_t FLAG_ISATTRIBUTE{1};

        RowId rowId;
        std::string nmSpcId;
        std::string localName;
        std::string flags;
        std::string id;

        XdbXQn(RowId newRowId, std::string newNmSpcId, std::string newLocalName, std::string newFlags, std::string newId):
                rowId(newRowId),
                nmSpcId(std::move(newNmSpcId)),
                localName(std::move(newLocalName)),
                flags(std::move(newFlags)),
                id(std::move(newId)) {}

        explicit XdbXQn(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const XdbXQn& other) const {
            return (other.rowId != rowId) || (other.nmSpcId != nmSpcId) || (other.localName != localName) || (other.flags != flags) || (other.id != id);
        }

        [[nodiscard]] static std::string tableName() {
            return "XDB.X$QN";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", NMSPCID: '" + nmSpcId + "', LOCALNAME: '" + localName + "', FLAGS: '" + flags + "', ID: '" + id + "'";
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

    class XdbXQnKey final {
    public:
        std::string id;

        explicit XdbXQnKey(std::string newId):
                id(std::move(newId)) {}

        explicit XdbXQnKey(const XdbXQn* xdbXQn):
                id(xdbXQn->id) {}

        bool operator!=(const XdbXQnKey& other) const {
            return (other.id != id);
        }

        bool operator==(const XdbXQnKey& other) const {
            return (other.id == id);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::XdbXQnKey> {
    size_t operator()(const OpenLogReplicator::XdbXQnKey& xdbXQnKey) const noexcept {
        return hash<std::string>()(xdbXQnKey.id);
    }
};

#endif
