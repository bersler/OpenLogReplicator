/* Definition of schema SYS.TS$
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
#include "../typeRowId.h"

#ifndef SYS_TS_H_
#define SYS_TS_H_

namespace OpenLogReplicator {
    class SysTs final {
    public:
        static constexpr uint64_t NAME_LENGTH = 30;

        SysTs(typeRowId newRowId, typeTs newTs, const char* newName, uint32_t newBlockSize) :
                rowId(newRowId),
                ts(newTs),
                name(newName),
                blockSize(newBlockSize) {
        }

        bool operator!=(const SysTs& other) const {
            return (other.rowId != rowId) || (other.ts != ts) || (other.name != name) || (other.blockSize != blockSize);
        }

        typeRowId rowId;
        typeTs ts;
        std::string name;
        uint32_t blockSize;
    };
}

#endif
