/* Definition of schema SYS.COL$
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef SYS_COL_H_
#define SYS_COL_H_

#include "../types/IntX.h"
#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysCol final {
    public:
        static constexpr uint NAME_LENGTH{128};

        enum class PROPERTY : unsigned long long {
            ADT                           = 1ULL << 0,
            OID                           = 1ULL << 1,
            NESTED1                       = 1ULL << 2,
            VIRTUAL1                      = 1ULL << 3,
            NESTED_TABLE_SETID            = 1ULL << 4,
            HIDDEN                        = 1ULL << 5,
            PRIMARY_KEY_BASED_OID         = 1ULL << 6,
            STORED_AS_LOB                 = 1ULL << 7,
            SYSTEM_GENERATED              = 1ULL << 8,
            ROWINFO_TYPED_TABLE_VIEW      = 1ULL << 9,
            NESTED_TABLES_SETID           = 1ULL << 10,
            NOT_INSERTABLE                = 1ULL << 11,
            NOT_UPDATABLE                 = 1ULL << 12,
            NOT_DELETABLE                 = 1ULL << 13,
            DROPPED                       = 1ULL << 14,
            UNUSED                        = 1ULL << 15,
            VIRTUAL2                      = 1ULL << 16,
            PLACE_DESCEND_OPERATOR_ON_TOP = 1ULL << 17,
            VIRTUAL_IS_NLS_DEPENDENT      = 1ULL << 18,
            REF_OID_COL                   = 1ULL << 19,
            HIDDEN_SNAPSHOT_BASE_TABLE    = 1ULL << 20,
            ATTRIBUTE_OF_USER_DEFINED_REF = 1ULL << 21,
            HIDDEN_RLS                    = 1ULL << 22,
            LENGTH_IN_CHARS               = 1ULL << 23,
            VIRTUAL_EXPRESSION_SPECIFIED  = 1ULL << 24,
            TYPEID                        = 1ULL << 25,
            ENCRYPTED                     = 1ULL << 26,
            ENCRYPTED_WITHOUT_SALT        = 1ULL << 29,
            ADDED                         = 1ULL << 30,
            DEFAULT_WITH_SEQUENCE         = 1ULL << 35,
            DEFAULT_ON_NULL               = 1ULL << 36,
            GENERATED_ALWAYS_IDENTITY     = 1ULL << 37,
            GENERATED_BY_DEFAULT_IDENTITY = 1ULL << 38,
            GUARD                         = 1ULL << 39
        };

        enum class COLTYPE : unsigned char {
            NONE                    = 0,
            VARCHAR                 = 1,
            NUMBER                  = 2,
            LONG                    = 8,
            DATE                    = 12,
            RAW                     = 23,
            LONG_RAW                = 24,
            XMLTYPE                 = 58,
            CHAR                    = 96,
            FLOAT                   = 100,
            DOUBLE                  = 101,
            CLOB                    = 112,
            BLOB                    = 113,
            JSON                    = 119,
            TIMESTAMP               = 180,
            TIMESTAMP_WITH_TZ       = 181,
            INTERVAL_YEAR_TO_MONTH  = 182,
            INTERVAL_DAY_TO_SECOND  = 183,
            UROWID                  = 208,
            TIMESTAMP_WITH_LOCAL_TZ = 231,
            BOOLEAN                 = 252
        };

        RowId rowId;
        typeObj obj{0};
        typeCol col{0};
        typeCol segCol{0};
        typeCol intCol{0};
        std::string name;
        COLTYPE type{COLTYPE::NONE};
        uint length{0};
        int precision{-1};   // NULL
        int scale{-1};       // NULL
        uint charsetForm{0}; // NULL
        uint charsetId{0};   // NULL
        int null_{0};
        IntX property{0, 0};

        SysCol(RowId newRowId, typeObj newObj, typeCol newCol, typeCol newSegCol, typeCol newIntCol, std::string newName, COLTYPE newType,
               uint newLength, int newPrecision, int newScale, uint newCharsetForm, uint newCharsetId, int newNull,
               uint64_t newProperty1, uint64_t newProperty2):
                rowId(newRowId),
                obj(newObj),
                col(newCol),
                segCol(newSegCol),
                intCol(newIntCol),
                name(std::move(newName)),
                type(newType),
                length(newLength),
                precision(newPrecision),
                scale(newScale),
                charsetForm(newCharsetForm),
                charsetId(newCharsetId),
                null_(newNull),
                property(newProperty1, newProperty2) {}

        explicit SysCol(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysCol& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.col != col) || (other.segCol != segCol) || (other.intCol != intCol) ||
                    (other.name != name) || (other.type != type) || (other.length != length) || (other.precision != precision) || (other.scale != scale) ||
                    (other.charsetForm != charsetForm) || (other.charsetId != charsetId) || (other.null_ != null_) || (other.property != property);
        }

        [[nodiscard]] bool isProperty(PROPERTY val) const {
            return property.isSet64(static_cast<uint64_t>(val));
        }

        [[nodiscard]] bool isHidden() const {
            return isProperty(PROPERTY::HIDDEN);
        }

        [[nodiscard]] bool isNullable() const {
            return (null_ == 0);
        }

        [[nodiscard]] bool isStoredAsLob() const {
            return isProperty(PROPERTY::STORED_AS_LOB);
        }

        [[nodiscard]] bool isSystemGenerated() const {
            return isProperty(PROPERTY::SYSTEM_GENERATED);
        }

        [[nodiscard]] bool isNested() const {
            return isProperty(PROPERTY::NESTED_TABLES_SETID);
        }

        [[nodiscard]] bool isUnused() const {
            return isProperty(PROPERTY::UNUSED);
        }

        [[nodiscard]] bool isAdded() const {
            return isProperty(PROPERTY::ADDED);
        }

        [[nodiscard]] bool isGuard() const {
            return isProperty(PROPERTY::GUARD);
        }

        [[nodiscard]] bool lengthInChars() const {
            return ((type == COLTYPE::VARCHAR || type == COLTYPE::CHAR) && isProperty(PROPERTY::LENGTH_IN_CHARS));
            // Else in bytes
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.COL$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", COL#: " + std::to_string(col) + ", SEGCOL#: " +
                    std::to_string(segCol) + ", INTCOL#: " + std::to_string(intCol) + ", NAME: '" + name + "', TYPE#: " +
                    std::to_string(static_cast<uint>(type)) + ", SIZE: " + std::to_string(length) + ", PRECISION#: " + std::to_string(precision) + ", SCALE: " +
                    std::to_string(scale) + ", CHARSETFORM: " + std::to_string(charsetForm) + ", CHARSETID: " + std::to_string(charsetId) + ", NULL$: " +
                    std::to_string(null_) + ", PROPERTY: " + property.toString();
        }

        [[nodiscard]] static constexpr bool dependentTable() {
            return true;
        }

        [[nodiscard]] static constexpr bool dependentTableLob() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLobFrag() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTablePart() {
            return false;
        }

        [[nodiscard]] typeObj getDependentTable() const {
            return obj;
        }
    };

    class SysColSeg final {
    public:
        SysColSeg(typeObj newObj, typeCol newSegCol, RowId newRowId):
                obj(newObj),
                segCol(newSegCol),
                rowId(newRowId) {}

        explicit SysColSeg(const SysCol* sysCol):
                obj(sysCol->obj),
                segCol(sysCol->segCol),
                rowId(sysCol->rowId) {}

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
        RowId rowId;
    };

    class SysColKey final {
    public:
        typeObj obj;
        typeCol intCol;

        explicit SysColKey(const SysCol* sysCol):
                obj(sysCol->obj),
                intCol(sysCol->intCol) {}

        SysColKey(typeObj newObj, typeCol newIntCol):
                obj(newObj),
                intCol(newIntCol) {}

        bool operator<(const SysColKey other) const {
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (intCol < other.intCol)
                return true;
            return false;
        }
    };
}

#endif
