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
#include "metrics/Metrics.h"

namespace OpenLogReplicator {
    MemoryManager::MemoryManager(Ctx* newCtx, const std::string& newAlias, const char* newSwapPath) :
            Thread(newCtx, newAlias),
            swapPath(newSwapPath) {
    }

    MemoryManager::~MemoryManager() {
        cleanup(true);
    }

    void MemoryManager::wakeUp() {
        std::unique_lock<std::mutex> lck(ctx->swapMtx);
        ctx->chunksMemoryManager.notify_all();
    }

    void MemoryManager::run() {
        if (unlikely(ctx->trace & Ctx::TRACE::THREADS)) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "memory manager (" + ss.str() + ") start");
        }

        try {
            while (!ctx->hardShutdown) {
                uint64_t discard = cleanOldTransactions();
                if (discard > 0)
                    ctx->metrics->emitSwapOperationsMbDiscard(discard);

                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;

                typeXid swapXid;
                int64_t swapIndex = -1;
                typeXid unswapXid;
                int64_t unswapIndex = -1;

                {
                    contextSet(Thread::CONTEXT_MUTEX, Thread::MEMORY_RUN1);
                    std::unique_lock<std::mutex> lck(ctx->swapMtx);
                    getChunkToUnswap(unswapXid, unswapIndex);
                    getChunkToSwap(swapXid, swapIndex);

                    if (swapIndex == -1)
                        ctx->wontSwap(this);

                    if (unswapIndex == -1 && swapIndex == -1) {
                        contextSet(Thread::CONTEXT_WAIT, Thread::MEMORY_NO_WORK);
                        ctx->chunksMemoryManager.wait_for(lck, std::chrono::milliseconds(10000));
                        contextSet(Thread::CONTEXT_CPU);
                        continue;
                    }
                }
                contextSet(Thread::CONTEXT_CPU);

                if (unswapIndex != -1) {
                    if (unswap(unswapXid, unswapIndex) && ctx->metrics)
                        ctx->metrics->emitSwapOperationsMbRead(1);
                    {
                        contextSet(Thread::CONTEXT_MUTEX, Thread::MEMORY_RUN2);
                        std::unique_lock<std::mutex> lck(ctx->swapMtx);
                        ctx->chunksTransaction.notify_all();
                    }
                    contextSet(Thread::CONTEXT_CPU);
                }
                if (swapIndex != -1 && swap(swapXid, swapIndex) && ctx->metrics)
                    ctx->metrics->emitSwapOperationsMbWrite(1);
            }
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        }

        if (unlikely(ctx->trace & Ctx::TRACE::THREADS)) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "memory manager (" + ss.str() + ") stop");
        }
    }

    void MemoryManager::initialize() {
        cleanup();
    }

    uint64_t MemoryManager::cleanOldTransactions() {
        uint64_t discard = 0;
        while (true) {
            typeXid xid;
            SwapChunk* sc;
            {
                contextSet(Thread::CONTEXT_MUTEX, Thread::MEMORY_CLEAN);
                std::unique_lock<std::mutex> lck(ctx->swapMtx);
                if (ctx->commitedXids.empty()) {
                    contextSet(Thread::CONTEXT_CPU);
                    return discard;
                }

                xid = ctx->commitedXids.back();
                ctx->commitedXids.pop_back();
                auto it = ctx->swapChunks.find(xid);
                if (it == ctx->swapChunks.end())
                    continue;
                sc = it->second;
                if (sc->swappedMax >= 0)
                    discard += sc->swappedMax - sc->swappedMin;
                ctx->swapChunks.erase(it);
            }
            contextSet(Thread::CONTEXT_CPU);
            delete sc;

            struct stat fileStat;
            std::string fileName(swapPath + "/" + xid.toString() + ".swap");
            if (stat(fileName.c_str(), &fileStat) == 0) {
                if (unlink(fileName.c_str()) != 0)
                    ctx->error(10010, "file: " + fileName + " - delete returned: " + strerror(errno));
            }
        }
    }

    void MemoryManager::cleanup(bool silent) {
        if (ctx->getSwapMemory(this) == 0)
            return;

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

            //ctx->warning(0, "checking file name: " + fileName);
            std::string suffix(".swap");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()) != suffix)
                continue;

            if (!silent)
                ctx->warning(10067, "deleting old swap file from previous execution: " + fullName);

            if (unlink(fullName.c_str()) != 0)
                throw RuntimeException(10010, "file: " + fullName + " - delete returned: " + strerror(errno));
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
            throw RuntimeException(50070, "swap chunk not found for xid: " + ctx->swappedShrinkXid.toString() + " during unswap");
        const SwapChunk* sc = it->second;
        if (sc->swappedMax == -1)
            return;

        index = sc->swappedMax;
        xid = ctx->swappedShrinkXid;
    }

    void MemoryManager::getChunkToSwap(typeXid& xid, int64_t& index) {
        if (ctx->nothingToSwap(this))
            return;

        for (const auto& it: ctx->swapChunks) {
            SwapChunk* sc = it.second;

            if (ctx->swappedFlushXid == it.first || sc->release || sc->chunks.size() <= 1)
                continue;

            if (sc->swappedMax < static_cast<int64_t>(sc->chunks.size() - 2)) {
                index = sc->swappedMax + 1;
                xid = it.first;
                return;
            }
        }
    }

    bool MemoryManager::unswap(typeXid xid, int64_t index) {
        uint8_t* tc = ctx->getMemoryChunk(this, Ctx::MEMORY::TRANSACTIONS, true);
        if (tc == nullptr)
            return false;

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
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        int fileDes = open(fileName.c_str(), flags);
        if (fileDes == -1)
            throw RuntimeException(50072, "swap file: " + fileName + " - open for read returned: " + strerror(errno));

#if __APPLE__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        uint64_t bytes = pread(fileDes, tc, Ctx::MEMORY_CHUNK_SIZE, index * Ctx::MEMORY_CHUNK_SIZE);
        close(fileDes);

        if (bytes != Ctx::MEMORY_CHUNK_SIZE)
            throw RuntimeException(50072, "swap file: " + fileName + " - read returned: " + strerror(errno));

        {
            contextSet(Thread::CONTEXT_MUTEX, Thread::MEMORY_UNSWAP);
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
                contextSet(Thread::CONTEXT_CPU);
                return true;
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

                contextSet(Thread::CONTEXT_CPU);
                return true;
            }

            throw RuntimeException(50072, "swap file: " + fileName + " - unswapping: " + std::to_string(index) + " not in range " +
                                          std::to_string(sc->swappedMin) + "-" + std::to_string(sc->swappedMax));
        }
    }

    bool MemoryManager::swap(typeXid xid, int64_t index) {
        uint8_t* tc;
        SwapChunk* sc;
        {
            contextSet(CONTEXT_MUTEX, MEMORY_SWAP1);
            std::unique_lock<std::mutex> lck(ctx->swapMtx);
            auto it = ctx->swapChunks.find(xid);
            if (unlikely(it == ctx->swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for xid: " + xid.toString() + " during swap write");
            sc = it->second;

            if (sc->chunks.size() <= 1 || index >= static_cast<int64_t>(sc->chunks.size() - 1) || sc->swappedMax != index - 1) {
                contextSet(CONTEXT_CPU);
                return false;
            }

            tc = sc->chunks[index];

            sc->swappedMax = index;
            if (sc->swappedMin == -1)
                sc->swappedMin = sc->swappedMax;

            sc->chunks[index] = nullptr;
        }
        contextSet(CONTEXT_CPU);

        std::string fileName = swapPath + "/" + xid.toString() + ".swap";

        int flags = O_WRONLY | O_CREAT;
#if __linux__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        int mode = S_IWUSR | S_IRUSR;
        int fileDes = open(fileName.c_str(), flags, mode);
        if (fileDes == -1)
            throw RuntimeException(50072, "swap file: " + fileName + " - open for write returned: " + strerror(errno));

#if __APPLE__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        uint64_t bytes = pwrite(fileDes, tc, Ctx::MEMORY_CHUNK_SIZE, index * Ctx::MEMORY_CHUNK_SIZE);
        if (bytes != Ctx::MEMORY_CHUNK_SIZE) {
            close(fileDes);
            throw RuntimeException(50072, "swap file: " + fileName + " - write returned: " + strerror(errno));
        }
        close(fileDes);
        ++ctx->swappedMB;
        bool remove = false;
        uint64_t truncateSize(0);

        {
            contextSet(CONTEXT_MUTEX, MEMORY_SWAP2);
            std::unique_lock<std::mutex> lck(ctx->swapMtx);

            if (ctx->swappedShrinkXid == xid) {
                sc->chunks[index] = tc;

                if (sc->swappedMax == 0) {
                    sc->swappedMin = sc->swappedMax = -1;
                    remove = true;
                } else {
                    --sc->swappedMax;
                    truncateSize = (sc->swappedMax + 1) * Ctx::MEMORY_CHUNK_SIZE;
                }
                ctx->chunksTransaction.notify_all();
            }
        }
        contextSet(CONTEXT_CPU);

        // discard write
        if (remove) {
            if (unlink(fileName.c_str()) != 0)
                throw RuntimeException(50072, "swap file: " + fileName + " - delete returned: " + strerror(errno));
            return false;
        }
        if (truncateSize > 0) {
            if (truncate(fileName.c_str(), truncateSize) != 0)
                throw RuntimeException(50072, "swap file: " + fileName + " - truncate returned: " + strerror(errno));
            return false;
        }

        ctx->freeMemoryChunk(this, Ctx::MEMORY::TRANSACTIONS, tc);
        return true;
    }
}