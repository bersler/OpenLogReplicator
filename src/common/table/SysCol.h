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

#define SYS_COL_NAME_LENGTH                             128
#define SYS_COL_PROPERTY_ADT                            1
#define SYS_COL_PROPERTY_OID                            2
#define SYS_COL_PROPERTY_NESTED1                        4
#define SYS_COL_PROPERTY_VIRTUAL1                       8
#define SYS_COL_PROPERTY_NESTED_TABLE_SETID             16
#define SYS_COL_PROPERTY_HIDDEN                         32
#define SYS_COL_PROPERTY_PRIMARY_KEY_BASED_OID          64
#define SYS_COL_PROPERTY_STORED_AS_LOB                  128
#define SYS_COL_PROPERTY_SYSTEM_GENERATED               256
#define SYS_COL_PROPERTY_ROWINFO_TYPED_TABLE_VIEW       512
#define SYS_COL_PROPERTY_NESTED_TABLES_SETID            1024
#define SYS_COL_PROPERTY_NOT_INSERTABLE                 2048
#define SYS_COL_PROPERTY_NOT_UPDATABLE                  4096
#define SYS_COL_PROPERTY_NOT_DELETABLE                  8192
#define SYS_COL_PROPERTY_DROPPED                        16384
#define SYS_COL_PROPERTY_UNUSED                         32768
#define SYS_COL_PROPERTY_VIRTUAL2                       65536
#define SYS_COL_PROPERTY_PLACE_DESCEND_OPERATOR_ON_TOP  131072
#define SYS_COL_PROPERTY_VIRTUAL_IS_NLS_DEPENDENT       262144
#define SYS_COL_PROPERTY_REF_OID_COL                    524288
#define SYS_COL_PROPERTY_HIDDEN_SNAPSHOT_BASE_TABLE     1048576
#define SYS_COL_PROPERTY_ATTRIBUTE_OF_USER_DEFINED_REF  2097152
#define SYS_COL_PROPERTY_HIDDEN_RLS                     4194304
#define SYS_COL_PROPERTY_LENGTH_IN_CHARS                8388608
#define SYS_COL_PROPERTY_VIRTUAL_EXPRESSION_SPECIFIED   16777216
#define SYS_COL_PROPERTY_TYPEID                         33554432
#define SYS_COL_PROPERTY_ENCRYPTED                      67108864
#define SYS_COL_PROPERTY_ENCRYPTED_WITHOUT_SALT         536870912
#define SYS_COL_PROPERTY_ADDED                          1073741824
#define SYS_COL_PROPERTY_DEFAULT_WITH_SEQUENCE          34359738368
#define SYS_COL_PROPERTY_DEFAULT_ON_NULL                68719476736
#define SYS_COL_PROPERTY_GENERATED_ALWAYS_IDENTITY      137438953472
#define SYS_COL_PROPERTY_GENERATED_BY_DEFAULT_IDENTITY  274877906944
#define SYS_COL_PROPERTY_GUARD                          549755813888

#define SYS_COL_TYPE_VARCHAR                 1
#define SYS_COL_TYPE_NUMBER                  2
#define SYS_COL_TYPE_LONG                    8
#define SYS_COL_TYPE_DATE                    12
#define SYS_COL_TYPE_RAW                     23
#define SYS_COL_TYPE_LONG_RAW                24
#define SYS_COL_TYPE_XMLTYPE                 58
#define SYS_COL_TYPE_CHAR                    96
#define SYS_COL_TYPE_FLOAT                   100
#define SYS_COL_TYPE_DOUBLE                  101
#define SYS_COL_TYPE_CLOB                    112
#define SYS_COL_TYPE_BLOB                    113
#define SYS_COL_TYPE_JSON                    119
#define SYS_COL_TYPE_TIMESTAMP               180
#define SYS_COL_TYPE_TIMESTAMP_WITH_TZ       181
#define SYS_COL_TYPE_INTERVAL_YEAR_TO_MONTH  182
#define SYS_COL_TYPE_INTERVAL_DAY_TO_SECOND  183
#define SYS_COL_TYPE_UROWID                  208
#define SYS_COL_TYPE_TIMESTAMP_WITH_LOCAL_TZ 231
#define SYS_COL_TYPE_BOOLEAN                 252

namespace OpenLogReplicator {
    class SysColSeg final {
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

        [[nodiscard]] bool isHidden() {
            return property.isSet64(SYS_COL_PROPERTY_HIDDEN);
        }

        [[nodiscard]] bool isNullable() {
            return (null_ == 0);
        }

        [[nodiscard]] bool isStoredAsLob() {
            return property.isSet64(SYS_COL_PROPERTY_STORED_AS_LOB);
        }

        [[nodiscard]] bool isSystemGenerated() {
            return property.isSet64(SYS_COL_PROPERTY_SYSTEM_GENERATED);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(SYS_COL_PROPERTY_NESTED_TABLES_SETID);
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
