/* Header for SysSeg class
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

#include "RowId.h"

#ifndef SYSSEG_H_
#define SYSSEG_H_

#define SYSSEG_SPARE1_MASK (2048)

using namespace std;

namespace OpenLogReplicator {
    class SysSegKey {
    public:
        SysSegKey();
        SysSegKey(uint32_t file, uint32_t block, uint32_t ts);
        bool operator!=(const SysSegKey& other) const;
        bool operator==(const SysSegKey& other) const;

        uint32_t file;
        uint32_t block;
        uint32_t ts;
    };

    class SysSeg {
    public:
        SysSeg(RowId &rowId, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare11, uint64_t spare12, bool touched);
        bool isCompressed(void);

        RowId rowId;
        uint32_t file;
        uint32_t block;
        uint32_t ts;
        uintX_t spare1;            //NULL
        bool touched;
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::SysSegKey> {
        size_t operator()(const OpenLogReplicator::SysSegKey &sysSegKey) const;
    };
}

#endif
