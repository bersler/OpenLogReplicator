/* Header for SysTab class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "typeINTX.h"
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
               uint64_t newProperty2, bool newTouched);

        bool operator!=(const SysTab& other) const;
        [[nodiscard]] bool isBinary();
        [[nodiscard]] bool isClustered();
        [[nodiscard]] bool isIot();
        [[nodiscard]] bool isPartitioned();
        [[nodiscard]] bool isNested();
        [[nodiscard]] bool isRowMovement();
        [[nodiscard]] bool isDependencies();
        [[nodiscard]] bool isInitial();

        typeRowId rowId;
        typeObj obj;
        typeDataObj dataObj;        // NULL
        typeCol cluCols;            // NULL
        typeINTX flags;
        typeINTX property;
        bool touched;
    };
}

#endif
