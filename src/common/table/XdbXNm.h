/* Definition of schema XDB.X$NMxxx
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

#ifndef XDB_XNM_H_
#define XDB_XNM_H_

#include <utility>

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class XdbXNm final {
    public:
        static constexpr uint NMSPCURI_LENGTH{2000};
        static constexpr uint ID_LENGTH{16};

        RowId rowId;
        std::string nmSpcUri;
        std::string id;

        XdbXNm(RowId newRowId, std::string newNmSpcUri, std::string newId):
                rowId(newRowId),
                nmSpcUri(std::move(newNmSpcUri)),
                id(std::move(newId)) {}

        explicit XdbXNm(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const XdbXNm& other) const {
            return (other.rowId != rowId) || (other.nmSpcUri != nmSpcUri) || (other.id != id);
        }

        [[nodiscard]] static std::string tableName() {
            return "XDB.X$NM";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", NMSPCURI: '" + nmSpcUri + "', ID: '" + id + "'";
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

    class XdbXNmKey final {
    public:
        std::string id;

        explicit XdbXNmKey(std::string newId):
                id(std::move(newId)) {}

        explicit XdbXNmKey(const XdbXNm* xdbXNm):
                id(xdbXNm->id) {}

        bool operator!=(const XdbXNmKey& other) const {
            return (other.id != id);
        }

        bool operator==(const XdbXNmKey& other) const {
            return (other.id == id);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::XdbXNmKey> {
    size_t operator()(const OpenLogReplicator::XdbXNmKey& xdbXNmKey) const noexcept {
        return hash<std::string>()(xdbXNmKey.id);
    }
};

#endif
