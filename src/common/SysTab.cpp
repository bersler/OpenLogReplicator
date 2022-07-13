/* Definition of schema SYS.TAB$
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

#include "SysTab.h"

namespace OpenLogReplicator {
    SysTab::SysTab(typeRowId& newRowId, typeObj newObj, typeDataObj newDataObj, typeCol newCluCols, uint64_t newFlags1, uint64_t newFlags2, uint64_t newProperty1,
                   uint64_t newProperty2, bool newTouched) :
            rowId(newRowId),
            obj(newObj),
            dataObj(newDataObj),
            cluCols(newCluCols),
            flags(newFlags1, newFlags2),
            property(newProperty1, newProperty2),
            touched(newTouched) {
    }

    bool SysTab::operator!=(const SysTab& other) const {
        return other.rowId != rowId || other.obj != obj || other.dataObj != dataObj || other.cluCols != cluCols || other.flags != flags ||
                other.property != property;
    }

    bool SysTab::isBinary() {
        return property.isSet64(SYS_TAB_PROPERTY_BINARY);
    }

    bool SysTab::isClustered() {
        return property.isSet64(SYS_TAB_PROPERTY_CLUSTERED_TABLE);
    }

    bool SysTab::isIot() {
        return property.isSet64(SYS_TAB_PROPERTY_IOT_OVERFLOW_SEGMENT) || flags.isSet64(SYS_TAB_PROPERTY_IOT2);
    }

    bool SysTab::isPartitioned() {
        return property.isSet64(SYS_TAB_PROPERTY_PARTITIONED_TABLE);
    }

    bool SysTab::isNested() {
        return property.isSet64(SYS_TAB_PROPERTY_NESTED_TABLE);
    }

    bool SysTab::isRowMovement() {
        return flags.isSet64(SYS_TAB_PROPERTY_ROW_MOVEMENT);
    }

    bool SysTab::isDependencies() {
        return flags.isSet64(SYS_TAB_PROPERTY_DEPENDENCIES);
    }

    bool SysTab::isInitial() {
        return flags.isSet64(SYS_TAB_PROPERTY_INITIAL);
    }
}
