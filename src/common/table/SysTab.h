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
        static constexpr uint64_t PROPERTY_BINARY = 1;
        static constexpr uint64_t PROPERTY_ATD_COLUMNS = 2;
        static constexpr uint64_t PROPERTY_NESTED_TABLE_COLUMNS = 4;
        static constexpr uint64_t PROPERTY_REF_COLUMNS = 8;
        static constexpr uint64_t PROPERTY_ARRAY_COLUMNS = 16;
        static constexpr uint64_t PROPERTY_PARTITIONED = 32;
        static constexpr uint64_t PROPERTY_INDEX_ONLY = 64;
        static constexpr uint64_t PROPERTY_IOT_ROW_OVERFLOW = 128;
        static constexpr uint64_t PROPERTY_IOT_ROW_CLUSTERING = 256;
        static constexpr uint64_t PROPERTY_IOT_OVERFLOW_SEGMENT = 512;
        static constexpr uint64_t PROPERTY_CLUSTERED = 1024;
        static constexpr uint64_t PROPERTY_INTERNAL_LOB_COLUMNS = 2048;
        static constexpr uint64_t PROPERTY_PRIMARY_KEY_BASED_OID_COLUMN = 4096;
        static constexpr uint64_t PROPERTY_NESTED = 8192;
        static constexpr uint64_t PROPERTY_READ_ONLY = 16384;
        static constexpr uint64_t PROPERTY_FILE_COLUMNS = 32768;
        static constexpr uint64_t PROPERTY_OID_GENERATED_BY_DEFAULT = 65536;
        static constexpr uint64_t PROPERTY_ROW_MOVEMENT = 131072;
        static constexpr uint64_t PROPERTY_USER_DEFINED_LOB_COLUMNS = 262144;
        static constexpr uint64_t PROPERTY_UNUSED_COLUMNS = 524288;
        static constexpr uint64_t PROPERTY_ON_COMMIT_MATERIALIZED_VIEW = 1048576;
        static constexpr uint64_t PROPERTY_SYSTEM_GENERATED_COLUMN_NAMES = 2097152;
        static constexpr uint64_t PROPERTY_GLOBAL_TEMPORARY_TABLE = 4194304;
        static constexpr uint64_t PROPERTY_DEPENDENCIES = 8388608;
        static constexpr uint64_t PROPERTY_READ_ONLY_MATERIALIZED_VIEW = 33554432;
        static constexpr uint64_t PROPERTY_MATERIALIZED_VIEW_TABLE = 67108864;
        static constexpr uint64_t PROPERTY_SUB_TABLE = 134217728;
        static constexpr uint64_t PROPERTY_IOT2 = 536870912;
        static constexpr uint64_t PROPERTY_EXTERNAL = 2147483648;
        static constexpr uint64_t PROPERTY_CUBE = 4294967296;
        static constexpr uint64_t PROPERTY_DELAYED_SEGMENT_CREATION = 17179869184;
        static constexpr uint64_t PROPERTY_RESULT_CACHE_FORCE = 2199023255552;
        static constexpr uint64_t PROPERTY_RESULT_CACHE_MANUAL = 4398046511104;
        static constexpr uint64_t PROPERTY_RESULT_CACHE_AUTO = 8796093022208;
        static constexpr uint64_t PROPERTY_LONG_VARCHAR_COLUMN = 9007199254740992;
        static constexpr uint64_t PROPERTY_CLUSTERING_CLAUSE = 18014398509481984;
        static constexpr uint64_t PROPERTY_ZONEMAPS = 36028797018963968;
        static constexpr uint64_t PROPERTY_IDENTITY_COLUMN = 288230376151711744;
        static constexpr uint64_t PROPERTY_DIMENTION = 1152921504606846976;

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
            return property.isSet64(PROPERTY_BINARY);
        }

        [[nodiscard]] bool isClustered() {
            return property.isSet64(PROPERTY_CLUSTERED);
        }

        [[nodiscard]] bool isIot() {
            return property.isSet64(PROPERTY_IOT_OVERFLOW_SEGMENT) || flags.isSet64(PROPERTY_IOT2);
        }

        [[nodiscard]] bool isPartitioned() {
            return property.isSet64(PROPERTY_PARTITIONED);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(PROPERTY_NESTED);
        }

        [[nodiscard]] bool isRowMovement() {
            return flags.isSet64(PROPERTY_ROW_MOVEMENT);
        }

        [[nodiscard]] bool isDependencies() {
            return flags.isSet64(PROPERTY_DEPENDENCIES);
        }

        [[nodiscard]] bool isInitial() {
            return flags.isSet64(PROPERTY_DELAYED_SEGMENT_CREATION);
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
