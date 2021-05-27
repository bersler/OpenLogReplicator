/* Definition of schema SYS.TAB$
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

#include "SysTab.h"

namespace OpenLogReplicator {
    SysTabKey::SysTabKey() :
            file(0),
            block(0),
            ts(0) {
    }

    SysTabKey::SysTabKey(uint32_t file, uint32_t block, uint32_t ts) :
            file(file),
            block(block),
            ts(ts) {
    }

    bool SysTabKey::operator==(const SysTabKey& other) const {
        return (other.file == file) &&
                (other.block == block) &&
                (other.ts == ts);
    }

    bool SysTabKey::operator!=(const SysTabKey& other) const {
        return (other.file != file) ||
                (other.block != block) ||
                (other.ts != ts);
    }

    SysTab::SysTab(RowId &rowId, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block, typeCOL cluCols,
            uint64_t flags1, uint64_t flags2, uint64_t property1, uint64_t property2, bool touched) :
            rowId(rowId),
            obj(obj),
            dataObj(dataObj),
            ts(ts),
            file(file),
            block(block),
            cluCols(cluCols),
            touched(touched),
            saved(false) {
        flags.set(flags1, flags2);
        property.set(property1, property2);
    }

    bool SysTab::isClustered(void) {
        return property.isSet64(1024);
    }

    bool SysTab::isIot(void) {
        return property.isSet64(512) || flags.isSet64(536870912);
    }

    bool SysTab::isPartitioned(void) {
        return property.isSet64(32);
    }

    bool SysTab::isNested(void) {
        return property.isSet64(8192);
    }

    bool SysTab::isRowMovement(void) {
        return flags.isSet64(131072);
    }

    bool SysTab::isDependencies(void) {
        return flags.isSet64(8388608);
    }

    bool SysTab::isInitial(void) {
        return flags.isSet64(17179869184);
    }
}

namespace std {
    size_t std::hash<OpenLogReplicator::SysTabKey>::operator()(const OpenLogReplicator::SysTabKey &sysTabKey) const {
        return hash<typeDATAOBJ>()(sysTabKey.file) ^
                hash<typeDBA>()(sysTabKey.block) ^
                hash<typeSLOT>()(sysTabKey.ts);
    }
}
