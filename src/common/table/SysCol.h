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
        static constexpr uint64_t NAME_LENGTH = 128;
        static constexpr uint64_t PROPERTY_ADT = 1;
        static constexpr uint64_t PROPERTY_OID = 2;
        static constexpr uint64_t PROPERTY_NESTED1 = 4;
        static constexpr uint64_t PROPERTY_VIRTUAL1 = 8;
        static constexpr uint64_t PROPERTY_NESTED_TABLE_SETID = 16;
        static constexpr uint64_t PROPERTY_HIDDEN = 32;
        static constexpr uint64_t PROPERTY_PRIMARY_KEY_BASED_OID = 64;
        static constexpr uint64_t PROPERTY_STORED_AS_LOB = 128;
        static constexpr uint64_t PROPERTY_SYSTEM_GENERATED = 256;
        static constexpr uint64_t PROPERTY_ROWINFO_TYPED_TABLE_VIEW = 512;
        static constexpr uint64_t PROPERTY_NESTED_TABLES_SETID = 1024;
        static constexpr uint64_t PROPERTY_NOT_INSERTABLE = 2048;
        static constexpr uint64_t PROPERTY_NOT_UPDATABLE = 4096;
        static constexpr uint64_t PROPERTY_NOT_DELETABLE = 8192;
        static constexpr uint64_t PROPERTY_DROPPED = 16384;
        static constexpr uint64_t PROPERTY_UNUSED = 32768;
        static constexpr uint64_t PROPERTY_VIRTUAL2 = 65536;
        static constexpr uint64_t PROPERTY_PLACE_DESCEND_OPERATOR_ON_TOP = 131072;
        static constexpr uint64_t PROPERTY_VIRTUAL_IS_NLS_DEPENDENT = 262144;
        static constexpr uint64_t PROPERTY_REF_OID_COL = 524288;
        static constexpr uint64_t PROPERTY_HIDDEN_SNAPSHOT_BASE_TABLE = 1048576;
        static constexpr uint64_t PROPERTY_ATTRIBUTE_OF_USER_DEFINED_REF = 2097152;
        static constexpr uint64_t PROPERTY_HIDDEN_RLS = 4194304;
        static constexpr uint64_t PROPERTY_LENGTH_IN_CHARS = 8388608;
        static constexpr uint64_t PROPERTY_VIRTUAL_EXPRESSION_SPECIFIED = 16777216;
        static constexpr uint64_t PROPERTY_TYPEID = 33554432;
        static constexpr uint64_t PROPERTY_ENCRYPTED = 67108864;
        static constexpr uint64_t PROPERTY_ENCRYPTED_WITHOUT_SALT = 536870912;
        static constexpr uint64_t PROPERTY_ADDED = 1073741824;
        static constexpr uint64_t PROPERTY_DEFAULT_WITH_SEQUENCE = 34359738368;
        static constexpr uint64_t PROPERTY_DEFAULT_ON_NULL = 68719476736;
        static constexpr uint64_t PROPERTY_GENERATED_ALWAYS_IDENTITY = 137438953472;
        static constexpr uint64_t PROPERTY_GENERATED_BY_DEFAULT_IDENTITY = 274877906944;
        static constexpr uint64_t PROPERTY_GUARD = 549755813888;

        static constexpr uint64_t TYPE_VARCHAR = 1;
        static constexpr uint64_t TYPE_NUMBER = 2;
        static constexpr uint64_t TYPE_LONG = 8;
        static constexpr uint64_t TYPE_DATE = 12;
        static constexpr uint64_t TYPE_RAW = 23;
        static constexpr uint64_t TYPE_LONG_RAW = 24;
        static constexpr uint64_t TYPE_XMLTYPE = 58;
        static constexpr uint64_t TYPE_CHAR = 96;
        static constexpr uint64_t TYPE_FLOAT = 100;
        static constexpr uint64_t TYPE_DOUBLE = 101;
        static constexpr uint64_t TYPE_CLOB = 112;
        static constexpr uint64_t TYPE_BLOB = 113;
        static constexpr uint64_t TYPE_JSON = 119;
        static constexpr uint64_t TYPE_TIMESTAMP = 180;
        static constexpr uint64_t TYPE_TIMESTAMP_WITH_TZ = 181;
        static constexpr uint64_t TYPE_INTERVAL_YEAR_TO_MONTH = 182;
        static constexpr uint64_t TYPE_INTERVAL_DAY_TO_SECOND = 183;
        static constexpr uint64_t TYPE_UROWID = 208;
        static constexpr uint64_t TYPE_TIMESTAMP_WITH_LOCAL_TZ = 231;
        static constexpr uint64_t TYPE_BOOLEAN = 252;

        SysCol(typeRowId newRowId, typeObj newObj, typeCol newCol, typeCol newSegCol, typeCol newIntCol, const char* newName, typeType newType,
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
            return property.isSet64(PROPERTY_HIDDEN);
        }

        [[nodiscard]] bool isNullable() {
            return (null_ == 0);
        }

        [[nodiscard]] bool isStoredAsLob() {
            return property.isSet64(PROPERTY_STORED_AS_LOB);
        }

        [[nodiscard]] bool isSystemGenerated() {
            return property.isSet64(PROPERTY_SYSTEM_GENERATED);
        }

        [[nodiscard]] bool isNested() {
            return property.isSet64(PROPERTY_NESTED_TABLES_SETID);
        }

        [[nodiscard]] bool isUnused() {
            return property.isSet64(PROPERTY_UNUSED);
        }

        [[nodiscard]] bool isAdded() {
            return property.isSet64(PROPERTY_ADDED);
        }

        [[nodiscard]] bool isGuard() {
            return property.isSet64(PROPERTY_GUARD);
        }

        [[nodiscard]] bool lengthInChars() {
            return ((type == TYPE_VARCHAR || type == TYPE_CHAR) && property.isSet64(PROPERTY_LENGTH_IN_CHARS));
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
