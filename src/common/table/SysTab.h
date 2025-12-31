/* Definition of schema SYS.TAB$
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

#ifndef SYS_TAB_H_
#define SYS_TAB_H_

#include "../types/IntX.h"
#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysTab final {
    public:
        enum class PROPERTY : unsigned long long {
            NONE                          = 0ULL,
            BINARY                        = 1ULL << 0,
            ATD_COLUMNS                   = 1ULL << 1,
            NESTED_TABLE_COLUMNS          = 1ULL << 2,
            REF_COLUMNS                   = 1ULL << 3,
            ARRAY_COLUMNS                 = 1ULL << 4,
            PARTITIONED                   = 1ULL << 5,
            IOT_INDEX_ONLY                = 1ULL << 6,
            IOT_ROW_OVERFLOW              = 1ULL << 7,
            IOT_ROW_CLUSTERING            = 1ULL << 8,
            IOT_OVERFLOW_SEGMENT          = 1ULL << 9,
            CLUSTERED                     = 1ULL << 10,
            INTERNAL_LOB_COLUMNS          = 1ULL << 11,
            PRIMARY_KEY_BASED_OID_COLUMN  = 1ULL << 12,
            NESTED                        = 1ULL << 13,
            READ_ONLY                     = 1ULL << 14,
            FILE_COLUMNS                  = 1ULL << 15,
            OID_GENERATED_BY_DEFAULT      = 1ULL << 16,
            USER_DEFINED_LOB_COLUMNS      = 1ULL << 18,
            UNUSED_COLUMNS                = 1ULL << 19,
            ON_COMMIT_MATERIALIZED_VIEW   = 1ULL << 20,
            SYSTEM_GENERATED_COLUMN_NAMES = 1ULL << 21,
            GLOBAL_TEMPORARY_TABLE        = 1ULL << 22,
            READ_ONLY_MATERIALIZED_VIEW   = 1ULL << 25,
            MATERIALIZED_VIEW_TABLE       = 1ULL << 26,
            SUB_TABLE                     = 1ULL << 27,
            EXTERNAL                      = 1ULL << 31,
            CUBE                          = 1ULL << 32,
            RESULT_CACHE_FORCE            = 1ULL << 41,
            RESULT_CACHE_MANUAL           = 1ULL << 42,
            RESULT_CACHE_AUTO             = 1ULL << 43,
            LONG_VARCHAR_COLUMN           = 1ULL << 53,
            CLUSTERING_CLAUSE             = 1ULL << 54,
            ZONEMAPS                      = 1ULL << 55,
            IDENTITY_COLUMN               = 1ULL << 58,
            DIMENTION                     = 1ULL << 60
        };

        enum class FLAGS : unsigned long long {
            ROW_MOVEMENT             = 1ULL << 17,
            DEPENDENCIES             = 1ULL << 23,
            IOT_MAPPING              = 1ULL << 29,
            DELAYED_SEGMENT_CREATION = 1ULL << 34
        };

        RowId rowId;
        typeObj obj{0};
        typeDataObj dataObj{0}; // NULL
        typeTs ts{0};
        typeCol cluCols{0}; // NULL
        IntX flags{0, 0};
        IntX property{0, 0};

        SysTab(RowId newRowId, typeObj newObj, typeDataObj newDataObj, typeTs newTs, typeCol newCluCols, uint64_t newFlags1, uint64_t newFlags2,
                uint64_t newProperty1, uint64_t newProperty2):
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                ts(newTs),
                cluCols(newCluCols),
                flags(newFlags1, newFlags2),
                property(newProperty1, newProperty2) {}

        explicit SysTab(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysTab& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.ts != ts) || (other.cluCols != cluCols) ||
                    (other.flags != flags) || (other.property != property);
        }

        [[nodiscard]] bool isProperty(PROPERTY val) const {
            return property.isSet64(static_cast<uint64_t>(val));
        }

        [[nodiscard]] bool isFlags(FLAGS val) const {
            return flags.isSet64(static_cast<uint64_t>(val));
        }

        [[nodiscard]] bool isBinary() const {
            return isProperty(PROPERTY::BINARY);
        }

        [[nodiscard]] bool isClustered() const {
            return isProperty(PROPERTY::CLUSTERED);
        }

        [[nodiscard]] bool isIot() const {
            return isProperty(PROPERTY::IOT_INDEX_ONLY)
                    || isProperty(PROPERTY::IOT_ROW_OVERFLOW)
                    || isFlags(FLAGS::IOT_MAPPING);
        }

        [[nodiscard]] bool isPartitioned() const {
            return isProperty(PROPERTY::PARTITIONED);
        }

        [[nodiscard]] bool isNested() const {
            return isProperty(PROPERTY::NESTED);
        }

        [[nodiscard]] bool isRowMovement() const {
            return isFlags(FLAGS::ROW_MOVEMENT);
        }

        [[nodiscard]] bool isDependencies() const {
            return isFlags(FLAGS::DEPENDENCIES);
        }

        [[nodiscard]] bool isInitial() const {
            return isFlags(FLAGS::DELAYED_SEGMENT_CREATION);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.TAB$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", DATAOBJ#: " + std::to_string(dataObj) + ", CLUCOLS: " +
                    std::to_string(cluCols) + ", FLAGS: " + flags.toString() + ", PROPERTY: " + property.toString();
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

    class SysTabObj final {
    public:
        typeObj obj;

        explicit SysTabObj(typeObj newObj):
                obj(newObj) {}

        explicit SysTabObj(const SysTab* sysTab):
                obj(sysTab->obj) {}

        bool operator!=(const SysTabObj other) const {
            return (other.obj != obj);
        }

        bool operator==(const SysTabObj other) const {
            return (other.obj == obj);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysTabObj> {
    size_t operator()(const OpenLogReplicator::SysTabObj sysTabObj) const noexcept {
        return hash<typeObj>()(sysTabObj.obj);
    }
};

#endif
