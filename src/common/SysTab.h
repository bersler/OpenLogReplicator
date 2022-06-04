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

#ifndef SYSTAB_H_
#define SYSTAB_H_

#define SYSTAB_PROPERTY_BINARY              1
#define SYSTAB_PROPERTY_PARTITIONED         32
#define SYSTAB_PROPERTY_IOT1                512
#define SYSTAB_PROPERTY_CLUSTERED           1024
#define SYSTAB_PROPERTY_NESTED              8192
#define SYSTAB_PROPERTY_ROW_MOVEMENT        131072
#define SYSTAB_PROPERTY_DEPENDENCIES        8388608
#define SYSTAB_PROPERTY_IOT2                536870912
#define SYSTAB_PROPERTY_INITIAL             17179869184

namespace OpenLogReplicator {
    class SysTab {
    public:
        SysTab(typeRowId& rowId, typeObj obj, typeDataObj dataObj, typeCol cluCols, uint64_t flags1, uint64_t flags2, uint64_t property1,
               uint64_t property2, bool touched);

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
        typeDataObj dataObj;        //NULL
        typeCol cluCols;            //NULL
        typeINTX flags;
        typeINTX property;
        bool touched;
    };
}

#endif
