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

#include "SysCol.h"

namespace OpenLogReplicator {
    SysColSeg::SysColSeg(typeObj newObj, typeCol newSegCol) :
            obj(newObj),
            segCol(newSegCol) {
    }

    bool SysColSeg::operator<(const SysColSeg& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj < obj)
            return false;
        if (other.segCol > segCol)
            return true;
        return false;
    }

    SysColKey::SysColKey(typeObj newObj, typeCol newIntCol) :
            obj(newObj),
            intCol(newIntCol) {
    }

    bool SysColKey::operator<(const SysColKey& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj < obj)
            return false;
        if (other.intCol > intCol)
            return true;
        return false;
    }

    SysCol::SysCol(typeRowId& newRowId, typeObj newObj, typeCol newCol, typeCol newSegCol, typeCol newIntCol, const char* newName, typeType newType,
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

    bool SysCol::operator!=(const SysCol& other) const {
        return (other.rowId != rowId) || (other.obj != obj) || (other.col != col) || (other.segCol != segCol) || (other.intCol != intCol) ||
                (other.name != name) || (other.type != type) || (other.length != length) || (other.precision != precision) || (other.scale != scale) ||
                (other.charsetForm != charsetForm) || (other.charsetId != charsetId) || (other.null_ != null_) || (other.property != property);
    }

    bool SysCol::isInvisible() {
        return property.isSet64(SYS_COL_PROPERTY_INVISIBLE);
    }

    bool SysCol::isNullable() {
        return (null_ == 0);
    }

    bool SysCol::isStoredAsLob() {
        return property.isSet64(SYS_COL_PROPERTY_STORED_AS_LOB);
    }

    bool SysCol::isConstraint() {
        return property.isSet64(SYS_COL_PROPERTY_CONSTRAINT);
    }

    bool SysCol::isNested() {
        return property.isSet64(SYS_COL_PROPERTY_NESTED);
    }

    bool SysCol::isUnused() {
        return property.isSet64(SYS_COL_PROPERTY_UNUSED);
    }

    bool SysCol::isAdded() {
        return property.isSet64(SYS_COL_PROPERTY_ADDED);
    }

    bool SysCol::isGuard() {
        return property.isSet64(SYS_COL_PROPERTY_GUARD);
    }

    bool SysCol::lengthInChars() {
        return ((type == SYS_COL_TYPE_VARCHAR || type == SYS_COL_TYPE_CHAR) && property.isSet64(SYS_COL_PROPERTY_LENGTH_IN_CHARS));
        // Else in bytes
    }
}
