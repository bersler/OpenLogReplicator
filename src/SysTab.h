/* Header for SysTab class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "RowId.h"

#ifndef SYSTAB_H_
#define SYSTAB_H_

using namespace std;

namespace OpenLogReplicator {
    class SysTabKey {
    public:
        SysTabKey();
        SysTabKey(uint32_t file, uint32_t block, uint32_t ts);
        bool operator!=(const SysTabKey& other) const;
        bool operator==(const SysTabKey& other) const;

        uint32_t file;
        uint32_t block;
        uint32_t ts;
    };

    class SysTab {
    public:
        SysTab(RowId &rowId, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block, typeCOL cluCols,
                uint64_t flags1, uint64_t flags2, uint64_t property1, uint64_t property2, bool touched);
        bool isClustered(void);
        bool isIot(void);
        bool isPartitioned(void);
        bool isNested(void);
        bool isRowMovement(void);
        bool isDependencies(void);
        bool isInitial(void);

        RowId rowId;
        typeOBJ obj;
        typeDATAOBJ dataObj;        //NULL
        uint32_t ts;
        uint32_t file;
        uint32_t block;
        typeCOL cluCols;            //NULL
        uintX_t flags;
        uintX_t property;
        bool touched;
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::SysTabKey> {
        size_t operator()(const OpenLogReplicator::SysTabKey &sysTabKey) const;
    };
}

#endif
