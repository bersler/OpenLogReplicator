/* Definition of schema SYS.TAB$
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
#include "../typeIntX.h"
#include "../typeRowId.h"

#ifndef SYS_TAB_H_
#define SYS_TAB_H_

namespace OpenLogReplicator {
    class SysTab final {
    public:
        enum PROPERTY {
            BINARY = 1UL << 0, ATD_COLUMNS = 1UL << 1, NESTED_TABLE_COLUMNS = 1UL << 2, REF_COLUMNS = 1UL << 3, ARRAY_COLUMNS = 1UL << 4,
            PARTITIONED = 1UL << 5, INDEX_ONLY = 1UL << 6, IOT_ROW_OVERFLOW = 1UL << 7, IOT_ROW_CLUSTERING = 1UL << 8, IOT_OVERFLOW_SEGMENT = 1UL << 9,
            CLUSTERED = 1UL << 10, INTERNAL_LOB_COLUMNS = 1UL << 11, PRIMARY_KEY_BASED_OID_COLUMN = 1UL << 12, NESTED = 1UL << 13, READ_ONLY = 1UL << 14,
            FILE_COLUMNS = 1UL << 15, OID_GENERATED_BY_DEFAULT = 1UL << 16, ROW_MOVEMENT = 1UL << 17, USER_DEFINED_LOB_COLUMNS = 1UL << 18,
            UNUSED_COLUMNS = 1UL << 19, ON_COMMIT_MATERIALIZED_VIEW = 1UL << 20, SYSTEM_GENERATED_COLUMN_NAMES = 1UL << 21, GLOBAL_TEMPORARY_TABLE = 1UL << 22,
            DEPENDENCIES = 1UL << 23, READ_ONLY_MATERIALIZED_VIEW = 1UL << 25, MATERIALIZED_VIEW_TABLE = 1UL << 26, SUB_TABLE = 1UL << 27, IOT2 = 1UL << 29,
            EXTERNAL = 1UL << 31, CUBE = 1UL << 32, DELAYED_SEGMENT_CREATION = 1UL << 34, RESULT_CACHE_FORCE = 1UL << 41, RESULT_CACHE_MANUAL = 1UL << 42,
            RESULT_CACHE_AUTO = 1UL << 43, LONG_VARCHAR_COLUMN = 1UL << 53, CLUSTERING_CLAUSE = 1UL << 54, ZONEMAPS = 1UL << 55, IDENTITY_COLUMN = 1UL << 58,
            DIMENTION = 1UL << 60
        };

        SysTab(typeRowId newRowId, typeObj newObj, typeDataObj newDataObj, typeTs newTs, typeCol newCluCols, uint64_t newFlags1, uint64_t newFlags2,
               uint64_t newProperty1, uint64_t newProperty2) :
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                ts(newTs),
                cluCols(newCluCols),
                flags(newFlags1, newFlags2),
                property(newProperty1, newProperty2) {
        }

        bool operator!=(const SysTab& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.ts != ts) || (other.cluCols != cluCols) ||
                   (other.flags != flags) || (other.property != property);
        }

        [[nodiscard]] bool isBinary() {
            return property.isSet64(PROPERTY::BINARY);
        }

        [[nodiscard]] bool isClustered() {
            return property.isSet64(PROPERTY::CLUSTERED);
        }

        [[nodiscard]] bool isIot() {
            return property.isSet64(PROPERTY::IOT_OVERFLOW_SEGMENT) || flags.isSet64(PROPERTY::IOT2);
        }

        [[nodiscard]] bool isPartitioned() {
            return property.isSet64(PROPERTY::PARTITIONED);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(PROPERTY::NESTED);
        }

        [[nodiscard]] bool isRowMovement() {
            return flags.isSet64(PROPERTY::ROW_MOVEMENT);
        }

        [[nodiscard]] bool isDependencies() {
            return flags.isSet64(PROPERTY::DEPENDENCIES);
        }

        [[nodiscard]] bool isInitial() {
            return flags.isSet64(PROPERTY::DELAYED_SEGMENT_CREATION);
        }

        typeRowId rowId;
        typeObj obj;
        typeDataObj dataObj;        // NULL
        typeTs ts;
        typeCol cluCols;            // NULL
        typeIntX flags;
        typeIntX property;
    };
}

#endif
