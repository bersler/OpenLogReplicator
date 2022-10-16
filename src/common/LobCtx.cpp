/* Context for LOB data
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

#include "LobCtx.h"
#include "LobData.h"
#include "RedoLogException.h"

namespace OpenLogReplicator {
    void LobCtx::addLob(typeLobId lobId, uint32_t pageSize, typeDba page, uint8_t* data, typeXid xid) {
        LobData* lobData;
        auto iLob = lobs.find(lobId);
        if (iLob != lobs.end()) {
            lobData = iLob->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        auto iLobData = lobData->dataMap.find(page);
        if (iLobData != lobData->dataMap.end())
            throw RedoLogException("duplicate data LOBID: " + lobId.upper() + ", PAGE: " + std::to_string(page) + ", XID: " + xid.toString());

        lobData->pageSize = pageSize;
        lobData->dataMap[page] = data;
    }

    void LobCtx::setLength(typeLobId lobId, uint32_t sizePages, uint16_t sizeRest) {
        LobData* lobData;
        auto iLob = lobs.find(lobId);
        if (iLob != lobs.end()) {
            lobData = iLob->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        lobData->sizePages = sizePages;
        lobData->sizeRest = sizeRest;
    }

    void LobCtx::setPage(typeLobId lobId, typeDba page, uint32_t  pageNo, typeXid xid) {
        LobData* lobData;
        auto iLob = lobs.find(lobId);
        if (iLob != lobs.end()) {
            lobData = iLob->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        auto iLobIndex = lobData->indexMap.find(page);
        if (iLobIndex != lobData->indexMap.end())
            throw RedoLogException("duplicate index LOBID: " + lobId.upper() + ", PAGE: " + std::to_string(page) + ", XID: " + xid.toString());

        lobData->indexMap[pageNo] = page;
    }

    void LobCtx::purge() {
        for (auto iLob: lobs) {
            LobData* lobData = iLob.second;
            delete lobData;
        }
        lobs.clear();
    }
}
