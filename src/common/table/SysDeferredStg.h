/* Definition of schema SYS.DEFERRED_STG$
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

#ifndef SYS_DEFERRED_STG_H_
#define SYS_DEFERRED_STG_H_

#include "../types/IntX.h"
#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysDeferredStg final {
    public:
        static constexpr uint64_t FLAGSSTG_COMPRESSED{4};

        RowId rowId;
        typeObj obj{0};
        IntX flagsStg{0, 0}; // NULL

        SysDeferredStg(RowId newRowId, typeObj newObj, uint64_t newFlagsStg1, uint64_t newFlagsStg2):
                rowId(newRowId),
                obj(newObj),
                flagsStg(newFlagsStg1, newFlagsStg2) {}

        explicit SysDeferredStg(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysDeferredStg& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.flagsStg != flagsStg);
        }

        [[nodiscard]] bool isCompressed() const {
            return flagsStg.isSet64(FLAGSSTG_COMPRESSED);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.DEFERRED_STG$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", FLAGS_STG: " + flagsStg.toString();
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

    class SysDeferredStgObj final {
    public:
        typeObj obj;

        explicit SysDeferredStgObj(typeObj newObj):
                obj(newObj) {}

        explicit SysDeferredStgObj(const SysDeferredStg* sysDeferredStg):
                obj(sysDeferredStg->obj) {}

        bool operator!=(const SysDeferredStgObj other) const {
            return (other.obj != obj);
        }

        bool operator==(const SysDeferredStgObj other) const {
            return (other.obj == obj);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysDeferredStgObj> {
    size_t operator()(const OpenLogReplicator::SysDeferredStgObj sysDeferredStgObj) const noexcept {
        return hash<typeObj>()(sysDeferredStgObj.obj);
    }
};

#endif
