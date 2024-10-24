/* Definition of schema SYS.COL$
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

#ifndef SYS_COL_H_
#define SYS_COL_H_

namespace OpenLogReplicator {
    class SysColSeg final {
    public:
        SysColSeg(typeObj newObj, typeCol newSegCol, typeRowId newRowId) :
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

    class SysColKey final {
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

    class SysCol final {
    public:
        static constexpr uint NAME_LENGTH{128};

        enum PROPERTY {
            ADT = 1UL << 0, OID = 1UL << 1, NESTED1 = 1UL << 2, VIRTUAL1 = 1UL << 3, NESTED_TABLE_SETID = 1UL << 4, HIDDEN = 1UL << 5,
            PRIMARY_KEY_BASED_OID = 1UL << 6, STORED_AS_LOB = 1UL << 7, SYSTEM_GENERATED = 1UL << 8, ROWINFO_TYPED_TABLE_VIEW = 1UL << 9,
            NESTED_TABLES_SETID = 1UL << 10, NOT_INSERTABLE = 1UL << 11, NOT_UPDATABLE = 1UL << 12, NOT_DELETABLE = 1UL << 13, DROPPED = 1UL << 14,
            UNUSED = 1UL << 15, VIRTUAL2 = 1UL << 16, PLACE_DESCEND_OPERATOR_ON_TOP = 1UL << 17, VIRTUAL_IS_NLS_DEPENDENT = 1UL << 18,
            REF_OID_COL = 1UL << 19, HIDDEN_SNAPSHOT_BASE_TABLE = 1UL << 20, ATTRIBUTE_OF_USER_DEFINED_REF = 1UL << 21, HIDDEN_RLS = 1UL << 22,
            LENGTH_IN_CHARS = 1UL << 23, VIRTUAL_EXPRESSION_SPECIFIED = 1UL << 24, TYPEID = 1UL << 25, ENCRYPTED = 1UL << 26,
            ENCRYPTED_WITHOUT_SALT = 1UL << 29, ADDED = 1UL << 30, DEFAULT_WITH_SEQUENCE = 1UL << 35, DEFAULT_ON_NULL = 1UL << 36,
            GENERATED_ALWAYS_IDENTITY = 1UL << 37, GENERATED_BY_DEFAULT_IDENTITY = 1UL << 38, GUARD = 1UL << 39
        };

        enum COLTYPE {
            VARCHAR = 1, NUMBER = 2, LONG = 8, DATE = 12, RAW = 23, LONG_RAW = 24, XMLTYPE = 58, CHAR = 96, FLOAT = 100, DOUBLE = 101, CLOB = 112, BLOB = 113,
            JSON = 119, TIMESTAMP = 180, TIMESTAMP_WITH_TZ = 181, INTERVAL_YEAR_TO_MONTH = 182, INTERVAL_DAY_TO_SECOND = 183, UROWID = 208,
            TIMESTAMP_WITH_LOCAL_TZ = 231, BOOLEAN = 252
        };

        SysCol(typeRowId newRowId, typeObj newObj, typeCol newCol, typeCol newSegCol, typeCol newIntCol, const char* newName, typeType newType,
               uint newLength, int newPrecision, int newScale, uint newCharsetForm, uint newCharsetId, int newNull,
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

        [[nodiscard]] bool isHidden() {
            return property.isSet64(PROPERTY::HIDDEN);
        }

        [[nodiscard]] bool isNullable() {
            return (null_ == 0);
        }

        [[nodiscard]] bool isStoredAsLob() {
            return property.isSet64(PROPERTY::STORED_AS_LOB);
        }

        [[nodiscard]] bool isSystemGenerated() {
            return property.isSet64(PROPERTY::SYSTEM_GENERATED);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(PROPERTY::NESTED_TABLES_SETID);
        }

        [[nodiscard]] bool isUnused() {
            return property.isSet64(PROPERTY::UNUSED);
        }

        [[nodiscard]] bool isAdded() {
            return property.isSet64(PROPERTY::ADDED);
        }

        [[nodiscard]] bool isGuard() {
            return property.isSet64(PROPERTY::GUARD);
        }

        [[nodiscard]] bool lengthInChars() {
            return ((type == COLTYPE::VARCHAR || type == COLTYPE::CHAR) && property.isSet64(PROPERTY::LENGTH_IN_CHARS));
            // Else in bytes
        }

        typeRowId rowId;
        typeObj obj;
        typeCol col;
        typeCol segCol;
        typeCol intCol;
        std::string name;
        typeType type;
        uint length;
        int precision;          // NULL
        int scale;              // NULL
        uint charsetForm;       // NULL
        uint charsetId;         // NULL
        int null_;
        typeIntX property;
    };
}

#endif
