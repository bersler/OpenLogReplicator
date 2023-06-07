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
    LobCtx::~LobCtx() {
    }

    void LobCtx::checkOrphanedLobs(Ctx* ctx, const typeLobId& lobId, typeXid xid, uint64_t offset) {
        LobKey lobKey(lobId, 0);
        for (auto orphanedLobsIt = orphanedLobs->upper_bound(lobKey);
             orphanedLobsIt != orphanedLobs->end() && orphanedLobsIt->first.lobId == lobId; ) {

            addLob(ctx, lobId, orphanedLobsIt->first.page, orphanedLobsIt->second, xid, offset);

            if (ctx->trace & TRACE_LOB)
                ctx->logTrace(TRACE_LOB, "id: " + lobId.lower() + " page: " + std::to_string(orphanedLobsIt->first.page));

            orphanedLobsIt = orphanedLobs->erase(orphanedLobsIt);
        }
    }

    void LobCtx::addLob(Ctx* ctx, const typeLobId& lobId, typeDba page, uint8_t* data, typeXid xid, uint64_t offset) {
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
            if (ctx->trace & TRACE_LOB)
                ctx->logTrace(TRACE_LOB, "id: " + lobId.lower() +  " page: " + std::to_string(page) + " OVERWRITE");
            delete[] lobData->dataMap[page];
        }

        lobData->dataMap[page] = data;

        RedoLogRecord* redoLogRecordLob = reinterpret_cast<RedoLogRecord*>(data + sizeof(uint64_t));
        if (lobData->pageSize == 0) {
            lobData->pageSize = redoLogRecordLob->lobPageSize;
        } else if (lobData->pageSize != redoLogRecordLob->lobPageSize) {
            throw RedoLogException(50003, "inconsistent page size lobid: " + lobId.upper() + ", new: " +
                                   std::to_string(redoLogRecordLob->lobPageSize) + ", already set to: " + std::to_string(lobData->pageSize) +
                                   ", xid: " + xid.toString() + ", offset: " + std::to_string(offset));
        }

        uint32_t pageNo = redoLogRecordLob->lobPageNo;
        if (pageNo != INVALID_LOB_PAGE_NO) {
            auto indexMapIt = lobData->indexMap.find(page);
            if (indexMapIt != lobData->indexMap.end()) {
                if (indexMapIt->second != page)
                    throw RedoLogException(50004, "duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) +
                                           ", already set to: " + std::to_string(indexMapIt->second) + ", xid: " + xid.toString()  + ", offset: " +
                                           std::to_string(offset));
            } else {
                lobData->indexMap[pageNo] = page;
            }
        }
    }

    void LobCtx::orderList(typeDba page, typeDba next) {
        auto listMapIt = listMap.find(page);
        if (listMapIt != listMap.end()) {
            uint8_t* oldData = listMapIt->second;
            typeDba* dba = reinterpret_cast<typeDba*>(oldData);
            *dba = next;
        } else {
            uint8_t* newData = new uint8_t[8];
            typeDba* dba = reinterpret_cast<typeDba*>(newData);
            *(dba++) = next;
            *dba = 0;

            listMap[page] = newData;
        }
    }

    void LobCtx::setList(typeDba page, uint8_t* data, uint16_t length) {
        typeDba nextPage = 0;
        auto listMapIt = listMap.find(page);
        if (listMapIt != listMap.end()) {
            uint8_t* oldData = listMapIt->second;
            typeDba* oldPage = reinterpret_cast<typeDba*>(oldData);
            nextPage = *oldPage;
            delete[] oldData;
        }

        uint8_t* newData = new uint8_t[length];
        typeDba* newPage = reinterpret_cast<typeDba*>(newData);
        *newPage = nextPage;
        memcpy(newData + 4, data + 4, length - 4);

        listMap[page] = newData;
    }

    void LobCtx::appendList(Ctx* ctx, typeDba page, uint8_t* data) {
        uint32_t asiz;
        uint32_t nent = ctx->read32(data + 4);
        uint32_t sidx = ctx->read32(data + 8);
        uint8_t* newData = new uint8_t[8 + (sidx + nent) * 8];

        auto listMapIt = listMap.find(page);
        if (listMapIt != listMap.end()) {
            // Found
            uint8_t* oldData = listMapIt->second;
            asiz = ctx->read32(oldData + 4);

            memcpy(newData, oldData, 4);
            memcpy(newData + 8, oldData + 8, asiz * 8);
            memcpy(newData + 8 + sidx * 8, data + 12, nent * 8);

            asiz = sidx + nent;
            ctx->write32(newData + 4, asiz);
            delete[] oldData;
        } else {
            // Not found
            memset(newData, 0, sidx * 8 + 8);
            memcpy(newData + sidx * 8 + 8, data + 12, nent * 8);

            asiz = sidx + nent;
            ctx->write32(newData + 4, asiz);
        }

        listMap[page] = newData;
    }

    void LobCtx::setLength(const typeLobId& lobId, uint32_t sizePages, uint16_t sizeRest) {
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

    void LobCtx::setPage(const typeLobId& lobId, typeDba page, uint32_t pageNo, typeXid xid, uint64_t offset) {
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
                throw RedoLogException(50004, "duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) +
                                       ", already set to: " + std::to_string(indexMapIt->second) + ", xid: " + xid.toString() + ", offset: " +
                                       std::to_string(offset));
            return;
        }

        lobData->indexMap[pageNo] = page;
    }

    void LobCtx::purge() {
        for (const auto& lobsIt: lobs) {
            LobData* lobData = lobsIt.second;
            delete lobData;
        }
        lobs.clear();

        for (auto listMapIt: listMap) {
            uint8_t* ptr = listMapIt.second;
            delete[] ptr;
        }
        listMap.clear();
    }
}
