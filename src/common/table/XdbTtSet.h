/* Definition of schema XDB.XDB$TTSET
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

#ifndef XDB_TTSET_H_
#define XDB_TTSET_H_

#include <utility>

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class XdbTtSet final {
    public:
        static constexpr uint GUID_LENGTH{32};
        static constexpr uint TOKSUF_LENGTH{26};

        RowId rowId;
        std::string guid;
        std::string tokSuf;
        uint64_t flags{0};
        typeObj obj{0};

        XdbTtSet(RowId newRowId, std::string newGuid, std::string newTokSuf, uint64_t newFlags, typeObj newObj):
                rowId(newRowId),
                guid(std::move(newGuid)),
                tokSuf(std::move(newTokSuf)),
                flags(newFlags),
                obj(newObj) {}

        explicit XdbTtSet(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const XdbTtSet& other) const {
            return (other.rowId != rowId) || (other.guid != guid) || (other.tokSuf != tokSuf) || (other.flags != flags) || (other.obj != obj);
        }

        [[nodiscard]] static std::string tableName() {
            return "XDB.XDB$TTSET";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", GUID: '" + guid + "', TOKSUF: '" + tokSuf + "', FLAGS: " + std::to_string(flags) + ", OBJ#: " +
                    std::to_string(obj);
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

    class XdbTtSetTokSuf final {
    public:
        std::string tokSuf;

        explicit XdbTtSetTokSuf(std::string newTokSuf):
                tokSuf(std::move(newTokSuf)) {}

        explicit XdbTtSetTokSuf(const XdbTtSet* xdbTtSet):
                tokSuf(xdbTtSet->tokSuf) {}

        bool operator!=(const XdbTtSetTokSuf& other) const {
            return (other.tokSuf != tokSuf);
        }

        bool operator==(const XdbTtSetTokSuf& other) const {
            return (other.tokSuf == tokSuf);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::XdbTtSetTokSuf> {
    size_t operator()(const OpenLogReplicator::XdbTtSetTokSuf& xdbTtSetTokSuf) const noexcept {
        return hash<std::string>()(xdbTtSetTokSuf.tokSuf);
    }
};

#endif
