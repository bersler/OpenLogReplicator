/* Header for LobData class
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

#include <map>

#include "types.h"

#ifndef LOB_DATA_H_
#define LOB_DATA_H_

namespace OpenLogReplicator {
    class LobData {
    public:
        LobData();
        virtual ~LobData();

        std::map<typeDba, uint8_t*> dataMap;
        std::map<uint32_t, typeDba> indexMap;

        uint32_t pageSize;
        uint32_t sizePages;
        uint16_t sizeRest;
    };
}

#endif
