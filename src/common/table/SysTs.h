/* Definition of schema SYS.TS$
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

#ifndef SYS_TS_H_
#define SYS_TS_H_

#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysTs final {
    public:
        static constexpr uint NAME_LENGTH{30};

        RowId rowId;
        typeTs ts{0};
        std::string name;
        uint32_t blockSize{0};

        SysTs(RowId newRowId, typeTs newTs, std::string newName, uint32_t newBlockSize):
                rowId(newRowId),
                ts(newTs),
                name(std::move(newName)),
                blockSize(newBlockSize) {}

        explicit SysTs(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysTs& other) const {
            return (other.rowId != rowId) || (other.ts != ts) || (other.name != name) || (other.blockSize != blockSize);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.TS$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", TS#: " + std::to_string(ts) + ", NAME: '" + name + "', BLOCKSIZE: " + std::to_string(blockSize);
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

    class SysTsTs final {
    public:
        typeTs ts;

        explicit SysTsTs(typeTs newTs):
                ts(newTs) {}

        explicit SysTsTs(const SysTs* sysTs):
                ts(sysTs->ts) {}

        bool operator!=(const SysTsTs other) const {
            return (other.ts != ts);
        }

        bool operator==(const SysTsTs other) const {
            return (other.ts == ts);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysTsTs> {
    size_t operator()(const OpenLogReplicator::SysTsTs sysTsTs) const noexcept {
        return hash<typeTs>()(sysTsTs.ts);
    }
};

#endif
