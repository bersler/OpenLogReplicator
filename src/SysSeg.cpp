/* Definition of schema SYS.SEG$
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

#include "SysSeg.h"

namespace OpenLogReplicator {
    SysSegKey::SysSegKey() :
            file(0),
            block(0),
            ts(0) {
    }

    SysSegKey::SysSegKey(uint32_t file, uint32_t block, uint32_t ts) :
            file(file),
            block(block),
            ts(ts) {
    }

    bool SysSegKey::operator==(const SysSegKey& other) const {
        return (other.file == file) &&
                (other.block == block) &&
                (other.ts == ts);
    }

    bool SysSegKey::operator!=(const SysSegKey& other) const {
        return (other.file != file) ||
                (other.block != block) ||
                (other.ts != ts);
    }

    SysSeg::SysSeg(RowId &rowId, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare11, uint64_t spare12) :
            rowId(rowId),
            file(file),
            block(block),
            ts(ts) {
        spare1.set(spare11, spare12);
    }

    bool SysSeg::isCompressed(void) {
        return spare1.isSet64(2048);
    }
}

namespace std {
    size_t std::hash<OpenLogReplicator::SysSegKey>::operator()(const OpenLogReplicator::SysSegKey &sysSegKey) const {
        return hash<typeDATAOBJ>()(sysSegKey.file) ^
                hash<typeDBA>()(sysSegKey.block) ^
                hash<typeSLOT>()(sysSegKey.ts);
    }
}
