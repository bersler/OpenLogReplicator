/* Header for LobData class
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

#ifndef LOB_DATA_H_
#define LOB_DATA_H_

#include <map>

#include "types/Types.h"

namespace OpenLogReplicator {
    class LobDataElement final {
    public:
        LobDataElement(typeDba newDba, uint16_t newPageOffset);

        bool operator<(const LobDataElement& other) const;

        typeDba dba{0};
        uint16_t pageOffset{0};
    };

    class LobData final {
    public:
        LobData();
        ~LobData();

        std::map<LobDataElement, uint8_t*> dataMap;
        std::map<uint32_t, typeDba> indexMap;

        uint32_t pageSize;
        uint32_t sizePages;
        uint16_t sizeRest;
    };
}

#endif
