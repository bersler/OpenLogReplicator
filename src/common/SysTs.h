/* Header for SysTs class
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

#include "types.h"
#include "typeRowId.h"

#ifndef SYS_TS_H_
#define SYS_TS_H_

#define SYS_TS_NAME_LENGTH                 30

namespace OpenLogReplicator {
    class SysTs {
    public:
        SysTs(typeRowId& newRowId, typeTs newTs, const char* newName, uint32_t newBlockSize, bool newTouched);

        bool operator!=(const SysTs& other) const;

        typeRowId rowId;
        typeTs ts;
        std::string name;
        uint32_t blockSize;
        bool touched;
    };
}

#endif
