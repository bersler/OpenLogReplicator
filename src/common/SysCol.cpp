/* Definition of schema SYS.COL$
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

#include "SysCol.h"

namespace OpenLogReplicator {
    SysColSeg::SysColSeg(typeObj obj, typeCol segCol) :
            obj(obj),
            segCol(segCol) {
    }

    bool SysColSeg::operator<(const SysColSeg& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj == obj && other.segCol > segCol)
            return true;
        return false;
    }

    SysColKey::SysColKey(typeObj obj, typeCol intCol) :
            obj(obj),
            intCol(intCol) {
    }

    bool SysColKey::operator<(const SysColKey& other) const {
        if (other.obj > obj)
            return true;
        if (other.obj == obj && other.intCol > intCol)
            return true;
        return false;
    }

    SysCol::SysCol(typeRowId& rowId, typeObj obj, typeCol col, typeCol segCol, typeCol intCol, const char* name, typeType type,
                   uint64_t length, int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null_,
                   uint64_t property1, uint64_t property2, bool touched) :
            rowId(rowId),
            obj(obj),
            col(col),
            segCol(segCol),
            intCol(intCol),
            name(name),
            type(type),
            length(length),
            precision(precision),
            scale(scale),
            charsetForm(charsetForm),
            charsetId(charsetId),
            null_(null_),
            property(property1, property2),
            touched(touched) {
    }

    bool SysCol::operator!=(const SysCol& other) const {
        return other.rowId != rowId || other.obj != obj || other.col != col || other.segCol != segCol || other.intCol != intCol || other.name != name ||
                other.type != type || other.length != length || other.precision != precision || other.scale != scale || other.charsetForm != charsetForm ||
                other.charsetId != charsetId || other.null_ != null_ || other.property != property;
    }

    bool SysCol::isInvisible() {
        return property.isSet64(SYSCOL_PROPERTY_INVISIBLE);
    }

    bool SysCol::isStoredAsLob() {
        return property.isSet64(SYSCOL_PROPERTY_STORED_AS_LOB);
    }

    bool SysCol::isConstraint() {
        return property.isSet64(SYSCOL_PROPERTY_CONSTRAINT);
    }

    bool SysCol::isNested() {
        return property.isSet64(SYSCOL_PROPERTY_NESTED);
    }

    bool SysCol::isUnused() {
        return property.isSet64(SYSCOL_PROPERTY_UNUSED);
    }

    bool SysCol::isAdded() {
        return property.isSet64(SYSCOL_PROPERTY_ADDED);
    }

    bool SysCol::isGuard() {
        return property.isSet64(SYSCOL_PROPERTY_GUARD);
    }

    bool SysCol::lengthInChars() {
        return ((type == SYSCOL_TYPE_VARCHAR || type == SYSCOL_TYPE_CHAR) && property.isSet64(SYSCOL_PROPERTY_LENGTH_IN_CHARS));
        //else in bytes
    }
}
