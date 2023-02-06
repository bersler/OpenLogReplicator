/* Context for LOB data
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

#include "LobCtx.h"
#include "LobData.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "../metadata/Schema.h"

namespace OpenLogReplicator {
    void LobCtx::checkOrphanedLobs(Ctx* ctx, typeLobId lobId) {
        LobKey lobKey(lobId, 0);
        for (auto orphanedLobsIt = orphanedLobs->upper_bound(lobKey);
             orphanedLobsIt != orphanedLobs->end() && orphanedLobsIt->first.lobId == lobId; ) {

            addLob(lobId, orphanedLobsIt->first.page, orphanedLobsIt->second);

            TRACE(TRACE2_LOB, "LOB" <<
                    " id: " << lobId <<
                    " page: 0x" << std::setfill('0') << std::setw(8) << std::hex << orphanedLobsIt->first.page)

            orphanedLobsIt = orphanedLobs->erase(orphanedLobsIt);
        }
    }

    void LobCtx::addLob(typeLobId lobId, typeDba page, uint8_t* data) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        auto dataMapIt = lobData->dataMap.find(page);
        if (dataMapIt != lobData->dataMap.end())
            delete[] lobData->dataMap[page];

        lobData->pageSize = *(reinterpret_cast<uint32_t*>(data + sizeof(uint64_t)));
        lobData->dataMap[page] = data;
    }

    void LobCtx::setLength(typeLobId lobId, uint32_t sizePages, uint16_t sizeRest) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        lobData->sizePages = sizePages;
        lobData->sizeRest = sizeRest;
    }

    void LobCtx::setPage(typeLobId lobId, typeDba page, uint32_t  pageNo, typeXid xid) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        auto indexMapIt = lobData->indexMap.find(page);
        if (indexMapIt != lobData->indexMap.end())
            throw RedoLogException("duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) + ", xid: " + xid.toString());

        lobData->indexMap[pageNo] = page;
    }

    void LobCtx::purge() {
        for (auto lobsIt: lobs) {
            LobData* lobData = lobsIt.second;
            delete lobData;
        }
        lobs.clear();
    }
}
