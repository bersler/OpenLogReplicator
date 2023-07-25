/* Definition of schema SYS.COL$
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

#ifndef SYS_COL_H_
#define SYS_COL_H_

#define SYS_COL_NAME_LENGTH                  128
#define SYS_COL_PROPERTY_INVISIBLE           32
#define SYS_COL_PROPERTY_STORED_AS_LOB       128
#define SYS_COL_PROPERTY_CONSTRAINT          256
#define SYS_COL_PROPERTY_NESTED              1024
#define SYS_COL_PROPERTY_UNUSED              32768
#define SYS_COL_PROPERTY_LENGTH_IN_CHARS     8388608
#define SYS_COL_PROPERTY_ADDED               1073741824
#define SYS_COL_PROPERTY_GUARD               549755813888
#define SYS_COL_TYPE_VARCHAR                 1
#define SYS_COL_TYPE_NUMBER                  2
#define SYS_COL_TYPE_LONG                    8
#define SYS_COL_TYPE_DATE                    12
#define SYS_COL_TYPE_RAW                     23
#define SYS_COL_TYPE_LONG_RAW                24
#define SYS_COL_TYPE_CHAR                    96
#define SYS_COL_TYPE_FLOAT                   100
#define SYS_COL_TYPE_DOUBLE                  101
#define SYS_COL_TYPE_CLOB                    112
#define SYS_COL_TYPE_BLOB                    113
#define SYS_COL_TYPE_TIMESTAMP               180
#define SYS_COL_TYPE_TIMESTAMP_WITH_TZ       181
#define SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH  182
#define SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND  183
#define SYS_COL_TYPE_UROWID                  208
#define SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ 231

namespace OpenLogReplicator {
    class SysColSeg {
    public:
        SysColSeg(typeObj newObj, typeCol newSegCol, typeRowId& newRowId) :
                obj(newObj),
                segCol(newSegCol),
                rowId(newRowId) {
        }

        bool operator<(const SysColSeg& other) const {
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (segCol < other.segCol)
                return true;
            if (other.segCol < segCol)
                return false;
            if (rowId < other.rowId)
                return true;
            return false;
        }

        typeObj obj;
        typeCol segCol;
        typeRowId rowId;
    };

    class SysColKey {
    public:
        SysColKey(typeObj newObj, typeCol newIntCol) :
                obj(newObj),
                intCol(newIntCol) {
        }

        bool operator<(const SysColKey& other) const {
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (intCol < other.intCol)
                return true;
            return false;
        }

        typeObj obj;
        typeCol intCol;
    };

    class SysCol {
    public:
        SysCol(typeRowId& newRowId, typeObj newObj, typeCol newCol, typeCol newSegCol, typeCol newIntCol, const char* newName, typeType newType,
               uint64_t newLength, int64_t newPrecision, int64_t newScale, uint64_t newCharsetForm, uint64_t newCharsetId, int64_t newNull,
               uint64_t newProperty1, uint64_t newProperty2) :
                rowId(newRowId),
                obj(newObj),
                col(newCol),
                segCol(newSegCol),
                intCol(newIntCol),
                name(newName),
                type(newType),
                length(newLength),
                precision(newPrecision),
                scale(newScale),
                charsetForm(newCharsetForm),
                charsetId(newCharsetId),
                null_(newNull),
                property(newProperty1, newProperty2) {
        }

        bool operator!=(const SysCol& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.col != col) || (other.segCol != segCol) || (other.intCol != intCol) ||
                   (other.name != name) || (other.type != type) || (other.length != length) || (other.precision != precision) || (other.scale != scale) ||
                   (other.charsetForm != charsetForm) || (other.charsetId != charsetId) || (other.null_ != null_) || (other.property != property);
        }

        [[nodiscard]] bool isInvisible() {
            return property.isSet64(SYS_COL_PROPERTY_INVISIBLE);
        }

        [[nodiscard]] bool isNullable() {
            return (null_ == 0);
        }

        [[nodiscard]] bool isStoredAsLob() {
            return property.isSet64(SYS_COL_PROPERTY_STORED_AS_LOB);
        }

        [[nodiscard]] bool isConstraint() {
            return property.isSet64(SYS_COL_PROPERTY_CONSTRAINT);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(SYS_COL_PROPERTY_NESTED);
        }

        [[nodiscard]] bool isUnused() {
            return property.isSet64(SYS_COL_PROPERTY_UNUSED);
        }

        [[nodiscard]] bool isAdded() {
            return property.isSet64(SYS_COL_PROPERTY_ADDED);
        }

        [[nodiscard]] bool isGuard() {
            return property.isSet64(SYS_COL_PROPERTY_GUARD);
        }

        [[nodiscard]] bool lengthInChars() {
            return ((type == SYS_COL_TYPE_VARCHAR || type == SYS_COL_TYPE_CHAR) && property.isSet64(SYS_COL_PROPERTY_LENGTH_IN_CHARS));
            // Else in bytes
        }

        typeRowId rowId;
        typeObj obj;
        typeCol col;
        typeCol segCol;
        typeCol intCol;
        std::string name;
        typeType type;
        uint64_t length;
        int64_t precision;          // NULL
        int64_t scale;              // NULL
        uint64_t charsetForm;       // NULL
        uint64_t charsetId;         // NULL
        int64_t null_;
        typeIntX property;
    };
}

#endif
