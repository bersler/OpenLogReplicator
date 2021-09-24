/* Definition of schema SYS.COL$
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

#include "SysCol.h"

namespace OpenLogReplicator {
    SysColSeg::SysColSeg() :
            obj(0),
            segCol(0) {
    }

    SysColSeg::SysColSeg(typeOBJ obj, typeCOL segCol) :
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

    SysColKey::SysColKey() :
            obj(0),
            intCol(0) {
    }

    SysColKey::SysColKey(typeOBJ obj, typeCOL intCol) :
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

    SysCol::SysCol(RowId& rowId, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char* name, typeTYPE type,
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
            touched(touched),
            saved(false) {
        property.set(property1, property2);
    }

    bool SysCol::isInvisible(void) {
        return property.isSet64(32);
    }

    bool SysCol::isStoredAsLob(void) {
        return property.isSet64(128);
    }

    bool SysCol::isConstraint(void) {
        return property.isSet64(256);
    }

    bool SysCol::isNested(void) {
        return property.isSet64(1024);
    }

    bool SysCol::isUnused(void) {
        return property.isSet64(32768);
    }

    bool SysCol::isAdded(void) {
        return property.isSet64(1073741824);
    }

    bool SysCol::isGuard(void) {
        return property.isSet64(549755813888);
    }
}
