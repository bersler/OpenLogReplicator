/* Context for LOB data
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

#include "LobCtx.h"
#include "LobData.h"
#include "RedoLogRecord.h"
#include "exception/RedoLogException.h"

namespace OpenLogReplicator {
    LobCtx::~LobCtx() {
    }

    void LobCtx::checkOrphanedLobs(const Ctx* ctx, const typeLobId& lobId, typeXid xid, uint64_t offset) {
        LobKey lobKey(lobId, 0);
        for (auto orphanedLobsIt = orphanedLobs->upper_bound(lobKey);
             orphanedLobsIt != orphanedLobs->end() && orphanedLobsIt->first.lobId == lobId;) {

            addLob(ctx, lobId, orphanedLobsIt->first.page, 0, orphanedLobsIt->second, xid, offset);

            if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                ctx->logTrace(Ctx::TRACE_LOB, "id: " + lobId.lower() + " page: " + std::to_string(orphanedLobsIt->first.page));

            orphanedLobsIt = orphanedLobs->erase(orphanedLobsIt);
        }
    }

    void LobCtx::addLob(const Ctx* ctx, const typeLobId& lobId, typeDba page, uint64_t pageOffset, uint8_t* data, typeXid xid, uint64_t offset) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs.insert_or_assign(lobId, lobData);
        }

        LobDataElement element(page, pageOffset);
        auto dataMapIt = lobData->dataMap.find(element);
        if (dataMapIt != lobData->dataMap.end()) {
            if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                ctx->logTrace(Ctx::TRACE_LOB, "id: " + lobId.lower() + " page: " + std::to_string(page) + " OVERWRITE");
            delete[] dataMapIt->second;
        }

        lobData->dataMap.insert_or_assign(element, data);

        const RedoLogRecord* redoLogRecordLob = reinterpret_cast<const RedoLogRecord*>(data + sizeof(uint64_t));
        if (redoLogRecordLob->lobPageSize != 0) {
            if (lobData->pageSize == 0) {
                lobData->pageSize = redoLogRecordLob->lobPageSize;
            } else if (unlikely(lobData->pageSize != redoLogRecordLob->lobPageSize)) {
                throw RedoLogException(50003, "inconsistent page size lobid: " + lobId.upper() + ", new: " +
                                              std::to_string(redoLogRecordLob->lobPageSize) + ", already set to: " + std::to_string(lobData->pageSize) +
                                              ", xid: " + xid.toString() + ", offset: " + std::to_string(offset));
            }
        }

        typeDba pageNo = redoLogRecordLob->lobPageNo;
        if (pageNo != RedoLogRecord::INVALID_LOB_PAGE_NO) {
            auto indexMapIt = lobData->indexMap.find(page);
            if (indexMapIt != lobData->indexMap.end()) {
                if (unlikely(indexMapIt->second != page))
                    throw RedoLogException(50004, "duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) +
                                                  ", already set to: " + std::to_string(indexMapIt->second) + ", xid: " + xid.toString() + ", offset: " +
                                                  std::to_string(offset));
            } else {
                lobData->indexMap.insert_or_assign(pageNo, page);
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

            listMap.insert_or_assign(page, newData);
        }
    }

    void LobCtx::setList(typeDba page, const uint8_t* data, uint16_t size) {
        typeDba nextPage = 0;
        auto listMapIt = listMap.find(page);
        if (listMapIt != listMap.end()) {
            const uint8_t* oldData = listMapIt->second;
            const typeDba* oldPage = reinterpret_cast<const typeDba*>(oldData);
            nextPage = *oldPage;
            delete[] oldData;
        }

        uint8_t* newData = new uint8_t[size];
        typeDba* newPage = reinterpret_cast<typeDba*>(newData);
        *newPage = nextPage;
        memcpy(newData + 4, data + 4, size - 4U);

        listMap.insert_or_assign(page, newData);
    }

    void LobCtx::appendList(const Ctx* ctx, typeDba page, const uint8_t* data) {
        uint32_t aSiz;
        const uint32_t nEnt = ctx->read32(data + 4);
        const uint32_t sIdx = ctx->read32(data + 8);
        uint8_t* newData = new uint8_t[8 + (sIdx + nEnt) * 8];

        auto listMapIt = listMap.find(page);
        if (listMapIt != listMap.end()) {
            // Found
            uint8_t* oldData = listMapIt->second;
            aSiz = ctx->read32(oldData + 4);

            memcpy(newData, oldData, 4);
            memcpy(newData + 8, oldData + 8, aSiz * 8);
            memcpy(newData + 8 + sIdx * 8, data + 12, nEnt * 8);

            aSiz = sIdx + nEnt;
            ctx->write32(newData + 4, aSiz);
            delete[] oldData;
        } else {
            // Not found
            memset(newData, 0, sIdx * 8 + 8);
            memcpy(newData + sIdx * 8 + 8, data + 12, nEnt * 8);

            aSiz = sIdx + nEnt;
            ctx->write32(newData + 4, aSiz);
        }

        listMap.insert_or_assign(page, newData);
    }

    void LobCtx::setSize(const typeLobId& lobId, uint32_t sizePages, uint16_t sizeRest) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs.insert_or_assign(lobId, lobData);
        }

        lobData->sizePages = sizePages;
        lobData->sizeRest = sizeRest;
    }

    void LobCtx::setPage(const typeLobId& lobId, typeDba page, typeDba pageNo, typeXid xid, uint64_t offset) {
        LobData* lobData;
        auto lobsIt = lobs.find(lobId);
        if (lobsIt != lobs.end()) {
            lobData = lobsIt->second;
        } else {
            lobData = new LobData();
            lobs.insert_or_assign(lobId, lobData);
        }

        auto indexMapIt = lobData->indexMap.find(page);
        if (indexMapIt != lobData->indexMap.end()) {
            if (unlikely(indexMapIt->second != page))
                throw RedoLogException(50004, "duplicate index lobid: " + lobId.upper() + ", page: " + std::to_string(page) +
                                              ", already set to: " + std::to_string(indexMapIt->second) + ", xid: " + xid.toString() + ", offset: " +
                                              std::to_string(offset));
            return;
        }

        lobData->indexMap.insert_or_assign(pageNo, page);
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
