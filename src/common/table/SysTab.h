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

#define SYS_TAB_PROPERTY_BINARY                         1
#define SYS_TAB_PROPERTY_ATD_COLUMNS                    2
#define SYS_TAB_PROPERTY_NESTED_TABLE_COLUMNS           4
#define SYS_TAB_PROPERTY_REF_COLUMNS                    8
#define SYS_TAB_PROPERTY_ARRAY_COLUMNS                  16
#define SYS_TAB_PROPERTY_PARTITIONED                    32
#define SYS_TAB_PROPERTY_INDEX_ONLY                     64
#define SYS_TAB_PROPERTY_IOT_ROW_OVERFLOW               128
#define SYS_TAB_PROPERTY_IOT_ROW_CLUSTERING             256
#define SYS_TAB_PROPERTY_IOT_OVERFLOW_SEGMENT           512
#define SYS_TAB_PROPERTY_CLUSTERED                      1024
#define SYS_TAB_PROPERTY_INTERNAL_LOB_COLUMNS           2048
#define SYS_TAB_PROPERTY_PRIMARY_KEY_BASED_OID_COLUMN   4096
#define SYS_TAB_PROPERTY_NESTED                         8192
#define SYS_TAB_PROPERTY_READ_ONLY                      16384
#define SYS_TAB_PROPERTY_FILE_COLUMNS                   32768
#define SYS_TAB_PROPERTY_OID_GENERATED_BY_DEFAULT       65536
#define SYS_TAB_PROPERTY_ROW_MOVEMENT                   131072
#define SYS_TAB_PROPERTY_USER_DEFINED_LOB_COLUMNS       262144
#define SYS_TAB_PROPERTY_UNUSED_COLUMNS                 524288
#define SYS_TAB_PROPERTY_ON_COMMIT_MATERIALIZED_VIEW    1048576
#define SYS_TAB_PROPERTY_SYSTEM_GENERATED_COLUMN_NAMES  2097152
#define SYS_TAB_PROPERTY_GLOBAL_TEMPORARY_TABLE         4194304
#define SYS_TAB_PROPERTY_DEPENDENCIES                   8388608
#define SYS_TAB_PROPERTY_READ_ONLY_MATERIALIZED_VIEW    33554432
#define SYS_TAB_PROPERTY_MATERIALIZED_VIEW_TABLE        67108864
#define SYS_TAB_PROPERTY_SUB_TABLE                      134217728
#define SYS_TAB_PROPERTY_IOT2                           536870912
#define SYS_TAB_PROPERTY_EXTERNAL                       2147483648
#define SYS_TAB_PROPERTY_CUBE                           4294967296
#define SYS_TAB_PROPERTY_DELAYED_SEGMENT_CREATION       17179869184
#define SYS_TAB_PROPERTY_RESULT_CACHE_FORCE             2199023255552
#define SYS_TAB_PROPERTY_RESULT_CACHE_MANUAL            4398046511104
#define SYS_TAB_PROPERTY_RESULT_CACHE_AUTO              8796093022208
#define SYS_TAB_PROPERTY_LONG_VARCHAR_COLUMN            9007199254740992
#define SYS_TAB_PROPERTY_CLUSTERING_CLAUSE              18014398509481984
#define SYS_TAB_PROPERTY_ZONEMAPS                       36028797018963968
#define SYS_TAB_PROPERTY_IDENTITY_COLUMN                288230376151711744
#define SYS_TAB_PROPERTY_DIMENTION                      1152921504606846976

namespace OpenLogReplicator {
    class SysTab final {
    public:
        SysTab(typeRowId& newRowId, typeObj newObj, typeDataObj newDataObj, typeTs newTs, typeCol newCluCols, uint64_t newFlags1, uint64_t newFlags2,
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
            return property.isSet64(SYS_TAB_PROPERTY_BINARY);
        }

        [[nodiscard]] bool isClustered() {
            return property.isSet64(SYS_TAB_PROPERTY_CLUSTERED);
        }

        [[nodiscard]] bool isIot() {
            return property.isSet64(SYS_TAB_PROPERTY_IOT_OVERFLOW_SEGMENT) || flags.isSet64(SYS_TAB_PROPERTY_IOT2);
        }

        [[nodiscard]] bool isPartitioned() {
            return property.isSet64(SYS_TAB_PROPERTY_PARTITIONED);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(SYS_TAB_PROPERTY_NESTED);
        }

        [[nodiscard]] bool isRowMovement() {
            return flags.isSet64(SYS_TAB_PROPERTY_ROW_MOVEMENT);
        }

        [[nodiscard]] bool isDependencies() {
            return flags.isSet64(SYS_TAB_PROPERTY_DEPENDENCIES);
        }

        [[nodiscard]] bool isInitial() {
            return flags.isSet64(SYS_TAB_PROPERTY_DELAYED_SEGMENT_CREATION);
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
