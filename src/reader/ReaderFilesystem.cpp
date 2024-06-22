/* Base class for reading redo from file system
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

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "ReaderFilesystem.h"

namespace OpenLogReplicator {
    ReaderFilesystem::ReaderFilesystem(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, int64_t newGroup, bool newConfiguredBlockSum) :
            Reader(newCtx, newAlias, newDatabase, newGroup, newConfiguredBlockSum),
            fileDes(-1),
            flags(0) {
    }

    ReaderFilesystem::~ReaderFilesystem() {
        ReaderFilesystem::redoClose();
    }

    void ReaderFilesystem::redoClose() {
        if (fileDes != -1) {
            close(fileDes);
            fileDes = -1;
        }
    }

    uint64_t ReaderFilesystem::redoOpen() {
        struct stat fileStat;

        if (stat(fileName.c_str(), &fileStat) != 0) {
            ctx->error(10003, "file: " + fileName + " - stat returned: " + strerror(errno));
            return REDO_ERROR;
        }

        flags = O_RDONLY;
        fileSize = fileStat.st_size;

#if __linux__
        if (!ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        fileDes = open(fileName.c_str(), flags);
        if (fileDes == -1) {
            ctx->error(10001, "file: " + fileName + " - open returned: " + strerror(errno));
            return REDO_ERROR;
        }

#if __APPLE__
        if (!ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache returned: " + strerror(errno));
        }
#endif

        return REDO_OK;
    }

    int64_t ReaderFilesystem::redoRead(uint8_t* buf, uint64_t offset, uint64_t size) {
        uint64_t startTime = 0;
        if (unlikely(ctx->trace & Ctx::TRACE_PERFORMANCE))
            startTime = ctx->clock->getTimeUt();
        int64_t bytes = 0;
        uint64_t tries = ctx->archReadTries;

        while (tries > 0) {
            if (ctx->hardShutdown)
                break;
            bytes = pread(fileDes, buf, size, static_cast<int64_t>(offset));
            if (unlikely(ctx->trace & Ctx::TRACE_FILE))
                ctx->logTrace(Ctx::TRACE_FILE, "read " + fileName + ", " + std::to_string(offset) + ", " + std::to_string(size) +
                                               " returns " + std::to_string(bytes));

            if (bytes > 0)
                break;

            // Retry for SSHFS broken connection: Transport endpoint is not isConnected
            if (bytes == -1 && errno != ENOTCONN)
                break;

            ctx->error(10005, "file: " + fileName + " - " + std::to_string(bytes) + " bytes read instead of " + std::to_string(size));

            if (ctx->hardShutdown)
                break;

            ctx->info(0, "sleeping " + std::to_string(ctx->archReadSleepUs) + " us before retrying read");
            usleep(ctx->archReadSleepUs);
            --tries;
        }

        // Maybe direct IO does not work
        if (bytes < 0 && !ctx->flagsSet(Ctx::REDO_FLAGS_DIRECT_DISABLE)) {
            ctx->hint("if problem is related to Direct IO, try to restart with Direct IO mode disabled, set 'flags' to value: " +
                      std::to_string(Ctx::REDO_FLAGS_DIRECT_DISABLE));
        }

        if (unlikely(ctx->trace & Ctx::TRACE_PERFORMANCE)) {
            if (bytes > 0)
                sumRead += bytes;
            sumTime += ctx->clock->getTimeUt() - startTime;
        }

        return bytes;
    }
}
