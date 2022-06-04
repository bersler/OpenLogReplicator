/* Header for SysCol class
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

#include "RowId.h"

#ifndef SYSCOL_H_
#define SYSCOL_H_

#define SYSCOL_ROWID_LENGTH     18
#define SYSCOL_NAME_LENGTH      128

namespace OpenLogReplicator {
    class SysColSeg {
    public:
        SysColSeg();
        SysColSeg(typeOBJ obj, typeCOL segCol);
        bool operator<(const SysColSeg& other) const;

        typeOBJ obj;
        typeCOL segCol;
    };

    class SysColKey {
    public:
        SysColKey();
        SysColKey(typeOBJ obj, typeCOL intCol);
        bool operator<(const SysColKey& other) const;

        typeOBJ obj;
        typeCOL intCol;
    };

    class SysCol {
    public:
        SysCol(RowId& rowId, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char* name, typeTYPE type,
                uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null_,
                uint64_t property1, uint64_t property2, bool touched);
        bool operator!=(const SysCol& other) const;
        bool isInvisible(void);
        bool isStoredAsLob(void);
        bool isConstraint(void);
        bool isNested(void);
        bool isUnused(void);
        bool isAdded(void);
        bool isGuard(void);
        bool lengthInChars(void);

        RowId rowId;
        typeOBJ obj;
        typeCOL col;
        typeCOL segCol;
        typeCOL intCol;
        std::string name;
        typeTYPE type;
        uint64_t length;
        int64_t precision;          //NULL
        int64_t scale;              //NULL
        uint64_t charsetForm;       //NULL
        uint64_t charsetId;         //NULL
        int64_t null_;
        uintX_t property;
        bool touched;
        bool saved;
    };
}

#endif
