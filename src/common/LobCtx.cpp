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
    void LobCtx::checkOrphanedLobs(Ctx* ctx, typeLobId lobId, typeXid xid) {
        LobKey lobKey(lobId, 0);
        for (auto orphanedLobsIt = orphanedLobs->upper_bound(lobKey);
             orphanedLobsIt != orphanedLobs->end() && orphanedLobsIt->first.lobId == lobId; ) {

            addLob(ctx, lobId, orphanedLobsIt->first.page, orphanedLobsIt->second, xid);

            TRACE(TRACE2_LOB, "LOB" <<
                    " id: " << lobId <<
                    " page: 0x" << std::setfill('0') << std::setw(8) << std::hex << orphanedLobsIt->first.page)

            orphanedLobsIt = orphanedLobs->erase(orphanedLobsIt);
        }
    }

    void LobCtx::addLob(Ctx* ctx, typeLobId lobId, typeDba page, uint8_t* data, typeXid xid) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        auto dataMapIt = lobData->dataMap.find(page);
        if (dataMapIt != lobData->dataMap.end()) {
            TRACE(TRACE2_LOB, "LOB" <<
                          " id: " << lobId <<
                          " page: 0x" << std::setfill('0') << std::setw(8) << std::hex << page << " OVERWRITE")
            delete[] lobData->dataMap[page];
        }

        lobData->dataMap[page] = data;

        RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(data + sizeof(uint64_t));
        if (lobData->pageSize == 0) {
            lobData->pageSize = redoLogRecordLob->lobPageSize;
        } else if (lobData->pageSize != redoLogRecordLob->lobPageSize) {
            throw RedoLogException("inconsistent page size lobid: " + lobId.upper() + ", new: " + std::to_string(redoLogRecordLob->lobPageSize) +
                                   ", already set to: " + std::to_string(lobData->pageSize) + ", xid: " + xid.toString());
        }

        uint32_t pageNo = redoLogRecordLob->lobPageNo;
        if (pageNo != INVALID_LOB_PAGE_NO) {
            auto indexMapIt = lobData->indexMap.find(page);
            if (indexMapIt != lobData->indexMap.end()) {
                if (indexMapIt->second != page)
                    throw RedoLogException("duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) +
                                       ", already set to: " + std::to_string(indexMapIt->second) + ", xid: " + xid.toString());
            } else {
                lobData->indexMap[pageNo] = page;
            }
        }
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

    void LobCtx::setPage(typeLobId lobId, typeDba page, uint32_t pageNo, typeXid xid) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs[lobId] = lobData;
        }

        auto indexMapIt = lobData->indexMap.find(page);
        if (indexMapIt != lobData->indexMap.end()) {
            if (indexMapIt->second != page)
                throw RedoLogException("duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) +
                                       ", already set to: " + std::to_string(indexMapIt->second) + ", xid: " + xid.toString());
            return;
        }

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
