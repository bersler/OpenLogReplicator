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

#include "types.h"
#include "typeINTX.h"
#include "typeRowId.h"

#ifndef SYSCOL_H_
#define SYSCOL_H_

#define SYSCOL_NAME_LENGTH                  128
#define SYSCOL_PROPERTY_INVISIBLE           32
#define SYSCOL_PROPERTY_STORED_AS_LOB       128
#define SYSCOL_PROPERTY_CONSTRAINT          256
#define SYSCOL_PROPERTY_NESTED              1024
#define SYSCOL_PROPERTY_UNUSED              32768
#define SYSCOL_PROPERTY_LENGTH_IN_CHARS     8388608
#define SYSCOL_PROPERTY_ADDED               1073741824
#define SYSCOL_PROPERTY_GUARD               549755813888
#define SYSCOL_TYPE_VARCHAR                 1
#define SYSCOL_TYPE_NUMBER                  2
#define SYSCOL_TYPE_LONG                    8
#define SYSCOL_TYPE_DATE                    12
#define SYSCOL_TYPE_RAW                     23
#define SYSCOL_TYPE_LONG_RAW                24
#define SYSCOL_TYPE_ROWID                   69
#define SYSCOL_TYPE_CHAR                    96
#define SYSCOL_TYPE_FLOAT                   100
#define SYSCOL_TYPE_DOUBLE                  101
#define SYSCOL_TYPE_CLOB                    112
#define SYSCOL_TYPE_BLOB                    113
#define SYSCOL_TYPE_TIMESTAMP               180
#define SYSCOL_TYPE_TIMESTAMP_WITH_TZ       181
#define SYSCOL_TYPE_INTERVAL_YEAR_TO_MONTH  182
#define SYSCOL_TYPE_INTERVAL_DAY_TO_SECOND  183
#define SYSCOL_TYPE_URAWID                  208
#define SYSCOL_TYPE_TIMESTAMP_WITH_LOCAL_TZ 231

namespace OpenLogReplicator {
    class SysColSeg {
    public:
        SysColSeg(typeObj obj, typeCol segCol);

        bool operator<(const SysColSeg& other) const;

        typeObj obj;
        typeCol segCol;
    };

    class SysColKey {
    public:
        SysColKey(typeObj obj, typeCol intCol);

        bool operator<(const SysColKey& other) const;

        typeObj obj;
        typeCol intCol;
    };

    class SysCol {
    public:
        SysCol(typeRowId& rowId, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type,
               uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null_,
               uint64_t property1, uint64_t property2, bool touched);

        bool operator!=(const SysCol& other) const;
        [[nodiscard]] bool isInvisible();
        [[nodiscard]] bool isStoredAsLob();
        [[nodiscard]] bool isConstraint();
        [[nodiscard]] bool isNested();
        [[nodiscard]] bool isUnused();
        [[nodiscard]] bool isAdded();
        [[nodiscard]] bool isGuard();
        [[nodiscard]] bool lengthInChars();

        typeRowId rowId;
        typeObj obj;
        typeCol col;
        typeCol segCol;
        typeCol intCol;
        std::string name;
        typeType type;
        uint64_t length;
        int64_t precision;          //NULL
        int64_t scale;              //NULL
        uint64_t charsetForm;       //NULL
        uint64_t charsetId;         //NULL
        int64_t null_;
        typeINTX property;
        bool touched;
    };
}

#endif
