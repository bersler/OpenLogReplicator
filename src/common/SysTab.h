/* Definition of schema SYS.TAB$
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "types.h"
#include "typeIntX.h"
#include "typeRowId.h"

#ifndef SYS_TAB_H_
#define SYS_TAB_H_

#define SYS_TAB_PROPERTY_BINARY                       1
#define SYS_TAB_PROPERTY_PARTITIONED_TABLE            32
#define SYS_TAB_PROPERTY_IOT_OVERFLOW_SEGMENT         512
#define SYS_TAB_PROPERTY_CLUSTERED_TABLE              1024
#define SYS_TAB_PROPERTY_NESTED_TABLE                 8192
#define SYS_TAB_PROPERTY_ROW_MOVEMENT                 131072
#define SYS_TAB_PROPERTY_GLOBAL_TEMPORARY_TABLE       4194304
#define SYS_TAB_PROPERTY_DEPENDENCIES                 8388608
#define SYS_TAB_PROPERTY_READ_ONLY_MATERIALIZED_VIEW  33554432
#define SYS_TAB_PROPERTY_MATERIALIZED_VIEW_TABLE      67108864
#define SYS_TAB_PROPERTY_IOT2                         536870912
#define SYS_TAB_PROPERTY_INITIAL                      17179869184

namespace OpenLogReplicator {
    class SysTab {
    public:
        SysTab(typeRowId& newRowId, typeObj newObj, typeDataObj newDataObj, typeCol newCluCols, uint64_t newFlags1, uint64_t newFlags2, uint64_t newProperty1,
               uint64_t newProperty2) :
                rowId(newRowId),
                obj(newObj),
                dataObj(newDataObj),
                cluCols(newCluCols),
                flags(newFlags1, newFlags2),
                property(newProperty1, newProperty2) {
        }

        bool operator!=(const SysTab& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.dataObj != dataObj) || (other.cluCols != cluCols) || (other.flags != flags) ||
                   (other.property != property);
        }

        [[nodiscard]] bool isBinary() {
            return property.isSet64(SYS_TAB_PROPERTY_BINARY);
        }

        [[nodiscard]] bool isClustered() {
            return property.isSet64(SYS_TAB_PROPERTY_CLUSTERED_TABLE);
        }

        [[nodiscard]] bool isIot() {
            return property.isSet64(SYS_TAB_PROPERTY_IOT_OVERFLOW_SEGMENT) || flags.isSet64(SYS_TAB_PROPERTY_IOT2);
        }

        [[nodiscard]] bool isPartitioned() {
            return property.isSet64(SYS_TAB_PROPERTY_PARTITIONED_TABLE);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(SYS_TAB_PROPERTY_NESTED_TABLE);
        }

        [[nodiscard]] bool isRowMovement() {
            return flags.isSet64(SYS_TAB_PROPERTY_ROW_MOVEMENT);
        }

        [[nodiscard]] bool isDependencies() {
            return flags.isSet64(SYS_TAB_PROPERTY_DEPENDENCIES);
        }

        [[nodiscard]] bool isInitial() {
            return flags.isSet64(SYS_TAB_PROPERTY_INITIAL);
        }

        typeRowId rowId;
        typeObj obj;
        typeDataObj dataObj;        // NULL
        typeCol cluCols;            // NULL
        typeIntX flags;
        typeIntX property;
    };
}

#endif
