/* Base class for process to swapping memory to disk when low
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

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Ctx.h"
#include "MemoryManager.h"
#include "exception/RuntimeException.h"

namespace OpenLogReplicator {
    MemoryManager::MemoryManager(Ctx* newCtx, const std::string& newAlias, const char* newSwapPath) :
            Thread(newCtx, newAlias),
            swapPath(newSwapPath) {
    }

    MemoryManager::~MemoryManager() {
        cleanup();
    }

    void MemoryManager::wakeUp() {
        std::unique_lock<std::mutex> lck(ctx->swapMtx);
        ctx->chunksMemoryManager.notify_all();
    }

    void MemoryManager::run() {
        if (unlikely(ctx->trace & Ctx::TRACE_THREADS)) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE_THREADS, "memory manager (" + ss.str() + ") start");
        }

        try {
            while (!ctx->hardShutdown) {
                cleanOldTransactions();

                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;

                typeXid swapXid;
                int64_t swapIndex = -1;
                typeXid unswapXid;
                int64_t unswapIndex = -1;

                {
                    std::unique_lock<std::mutex> lck(ctx->swapMtx);
                    getChunkToUnswap(unswapXid, unswapIndex);
                    getChunkToSwap(swapXid, swapIndex);

                    if (unswapIndex == -1 && swapIndex == -1) {
                        ctx->chunksMemoryManager.wait_for(lck, std::chrono::milliseconds(100));
                        continue;
                    }
                }

                if (unswapIndex != -1) {
                    unswap(unswapXid, unswapIndex);
                    {
                        std::unique_lock<std::mutex> lck(ctx->swapMtx);
                        ctx->chunksTransaction.notify_all();
                    }
                }
                if (swapIndex != -1)
                    swap(swapXid, swapIndex);
            }
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        }

        if (unlikely(ctx->trace & Ctx::TRACE_THREADS)) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE_THREADS,"memory manager (" + ss.str() + ") stop");
        }
    }

    void MemoryManager::initialize() {
        cleanup();
    }

    void MemoryManager::cleanOldTransactions() {
        while (true) {
            typeXid xid;
            SwapChunk* sc;
            {
                std::unique_lock<std::mutex> lck(ctx->swapMtx);
                if (ctx->commitedXids.empty())
                    return;

                xid = ctx->commitedXids.back();
                ctx->commitedXids.pop_back();
                auto it = ctx->swapChunks.find(xid);
                if (it == ctx->swapChunks.end())
                    continue;
                sc = it->second;
                ctx->swapChunks.erase(it);
            }
            delete sc;

            struct stat fileStat;
            std::string fileName(swapPath + "/" + xid.toString() + ".swap");
            if (stat(fileName.c_str(), &fileStat) == 0) {
                if (unlink(fileName.c_str()) != 0)
                    ctx->error(10010, "file: " + fileName + " - delete returned: " + strerror(errno));
            }
        }
    }

    void MemoryManager::cleanup() {
        DIR* dir;
        if ((dir = opendir(swapPath.c_str())) == nullptr)
            throw RuntimeException(10012, "directory: " + swapPath + " - can't read");

        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            std::string fileName(ent->d_name);

            std::string fullName(swapPath + "/" + ent->d_name);
            if (stat(fullName.c_str(), &fileStat) != 0) {
                ctx->warning(10003, "file: " + fileName + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            std::string suffix(".swap");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()) != suffix)
                continue;

            std::string fileBase(fileName.substr(0, fileName.length() - suffix.length()));
            ctx->warning(10072, "deleting old swap file from previous execution: " + fileBase);
            if (unlink(fileBase.c_str()) != 0)
                throw RuntimeException(10010, "file: " + fileBase + " - delete returned: " + strerror(errno));
        }
        closedir(dir);
    }

    void MemoryManager::getChunkToUnswap(typeXid& xid, int64_t& index) {
        if (ctx->swappedFlushXid.toUint() != 0) {
            auto it = ctx->swapChunks.find(ctx->swappedFlushXid);
            if (unlikely(it == ctx->swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for xid: " + xid.toString() + " during unswap");
            const SwapChunk* sc = it->second;
            if (sc->swappedMin > -1) {
                index = sc->swappedMin;
                xid = ctx->swappedFlushXid;
                return;
            }
        }

        if (ctx->swappedShrinkXid.toUint() == 0)
            return;

        auto it = ctx->swapChunks.find(ctx->swappedShrinkXid);
        if (unlikely(it == ctx->swapChunks.end()))
            throw RuntimeException(50070, "swap chunk not found for xid: " + xid.toString() + " during unswap");
        const SwapChunk* sc = it->second;
        if (unlikely(sc->swappedMax == -1))
            throw RuntimeException(50071, "unswap page not swapped for xid: " + xid.toString());

        index = sc->swappedMax;
        xid = ctx->swappedShrinkXid;
    }

    void MemoryManager::getChunkToSwap(typeXid& xid, int64_t& index) {
        if (ctx->getUsedMemory() < ctx->getSwapMemory())
            return;

        for (const auto& it: ctx->swapChunks) {
            SwapChunk* sc = it.second;

            if (sc->release || sc->chunks.size() <= 1)
                continue;

            if (sc->swappedMax < static_cast<int64_t>(sc->chunks.size())) {
                index = sc->swappedMax + 1;
                xid = it.first;
                return;
            }
        }
    }

    void MemoryManager::unswap(typeXid xid, int64_t index) {
        uint8_t* tc = ctx->getMemoryChunk(Ctx::MEMORY_MODULE_TRANSACTIONS, true, true, true);
        if (tc == nullptr)
            return;

        std::string fileName = swapPath + "/" + xid.toString() + ".swap";
        struct stat fileStat;

        if (stat(fileName.c_str(), &fileStat) != 0)
            throw RuntimeException(50072, "swap file: " + fileName + " - get metadata returned: " + strerror(errno));

        int flags = O_RDONLY;
        uint64_t fileSize = fileStat.st_size;
        if ((fileSize & (Ctx::MEMORY_CHUNK_SIZE - 1)) != 0)
            throw RuntimeException(50072, "swap file: " + fileName + " - wrong file size: " + std::to_string(fileSize));

        if (fileSize < (index + 1) * Ctx::MEMORY_CHUNK_SIZE)
            throw RuntimeException(50072, "swap file: " + fileName + " - too small file size: " + std::to_string(fileSize) + " to read chunk: " +
                    std::to_string(index));

#if __linux__
        if (!ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        int fileDes = open(fileName.c_str(), flags);
        if (fileDes == -1)
            throw RuntimeException(50072, "swap file: " + fileName + " - open for read returned: " + strerror(errno));

#if __APPLE__
        if (!ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        uint64_t bytes = pread(fileDes, tc, Ctx::MEMORY_CHUNK_SIZE, index * Ctx::MEMORY_CHUNK_SIZE);
        int errnov = errno;
        close(fileDes);

        if (bytes != Ctx::MEMORY_CHUNK_SIZE || errnov != ENOTCONN)
            throw RuntimeException(50072, "swap file: " + fileName + " - read returned: " + strerror(errnov));

        {
            std::unique_lock<std::mutex> lck(ctx->swapMtx);
            auto it = ctx->swapChunks.find(xid);
            if (unlikely(it == ctx->swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for xid: " + xid.toString() + " during unswap read");
            SwapChunk* sc = it->second;
            if (sc->swappedMin == index) {
                sc->chunks[sc->swappedMin] = tc;
                if (sc->swappedMin == sc->swappedMax)
                    sc->swappedMin = sc->swappedMax = -1;
                else
                    ++sc->swappedMin;
                return;
            }

            if (sc->swappedMax == index) {
                sc->chunks[sc->swappedMax] = tc;
                if (sc->swappedMin == sc->swappedMax) {
                    sc->swappedMin = sc->swappedMax = -1;
                    if (unlink(fileName.c_str()) != 0)
                        throw RuntimeException(50072, "swap file: " + fileName + " - delete returned: " + strerror(errno));
                } else {
                    --sc->swappedMax;
                    if (truncate(fileName.c_str(), (sc->swappedMax + 1) * Ctx::MEMORY_CHUNK_SIZE) != 0)
                        throw RuntimeException(50072, "swap file: " + fileName + " - truncate returned: " + strerror(errno));
                }

                return;
            }

            throw RuntimeException(50072, "swap file: " + fileName + " - unswapping: " + std::to_string(index) + " not in range " +
                    std::to_string(sc->swappedMin) + "-" + std::to_string(sc->swappedMax));
        }
    }

    void MemoryManager::swap(typeXid xid, int64_t index) {
        uint8_t* tc;
        SwapChunk* sc;
        {
            std::unique_lock<std::mutex> lck(ctx->swapMtx);
            auto it = ctx->swapChunks.find(xid);
            if (unlikely(it == ctx->swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for xid: " + xid.toString() + " during swap write");
            sc = it->second;

            if (sc->chunks.size() <= 1 || index >= static_cast<int64_t>(sc->chunks.size() - 1) || sc->swappedMax != index - 1)
                return;

            sc->lockedChunk = index;
            tc = sc->chunks[index];
        }

        std::string fileName = swapPath + "/" + xid.toString() + ".swap";

        int flags = O_WRONLY;
#if __linux__
        if (!ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        int fileDes = open(fileName.c_str(), flags);
        if (fileDes == -1)
            throw RuntimeException(50072, "swap file: " + fileName + " - open for write returned: " + strerror(errno));

#if __APPLE__
        if (!ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        uint64_t bytes = pwrite(fileDes, tc, Ctx::MEMORY_CHUNK_SIZE, index * Ctx::MEMORY_CHUNK_SIZE);
        int errnov = errno;
        close(fileDes);

        if (bytes != Ctx::MEMORY_CHUNK_SIZE || errnov != ENOTCONN)
            throw RuntimeException(50072, "swap file: " + fileName + " - write returned: " + strerror(errnov));

        {
            std::unique_lock<std::mutex> lck(ctx->swapMtx);

            sc->lockedChunk = -1;
            if (sc->release || sc->breakLock) {
                sc->breakLock = false;
                // discard write
                if (sc->swappedMax == -1) {
                    if (unlink(fileName.c_str()) != 0)
                        throw RuntimeException(50072, "swap file: " + fileName + " - delete returned: " + strerror(errno));
                } else {
                    if (truncate(fileName.c_str(), (sc->swappedMax + 1) * Ctx::MEMORY_CHUNK_SIZE) != 0)
                        throw RuntimeException(50072, "swap file: " + fileName + " - truncate returned: " + strerror(errno));
                }
                ctx->chunksTransaction.notify_all();
                return;
            }

            sc->swappedMax = index;
            if (sc->swappedMin == -1)
                sc->swappedMin = sc->swappedMax;

            sc->chunks[index] = nullptr;
            ctx->freeMemoryChunk(Ctx::MEMORY_MODULE_TRANSACTIONS, tc, true);
        }
    }
}
