/* Base class for reading redo from file system
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

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/Timer.h"
#include "ReaderFilesystem.h"

namespace OpenLogReplicator {
    ReaderFilesystem::ReaderFilesystem(Ctx* newCtx, std::string newAlias, std::string& newDatabase, int64_t newGroup, bool newConfiguredBlockSum) :
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

        int fileRet = stat(fileName.c_str(), &fileStat);
        TRACE(TRACE2_FILE, "FILE: stat for file: " << fileName << " - " << strerror(errno))
        if (fileRet != 0) {
            WARNING("reading information for file: " << fileName << " - " << strerror(errno))
            return REDO_ERROR;
        }

        flags = O_RDONLY;
        fileSize = fileStat.st_size;

#if __linux__
        if (!FLAG(REDO_FLAGS_DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        fileDes = open(fileName.c_str(), flags);
        TRACE(TRACE2_FILE, "FILE: open for " << fileName << " returns " << std::dec << fileDes << ", errno = " << errno)

        if (fileDes == -1) {
            ERROR("opening file returned: " << std::dec << fileName << " - " << strerror(errno))
            return REDO_ERROR;
        }

#if __APPLE__
        if (!FLAG(REDO_FLAGS_DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0) {
                ERROR("set no cache for: " << std::dec << fileName << " - " << strerror(errno))
            }
        }
#endif

        return REDO_OK;
    }

    int64_t ReaderFilesystem::redoRead(uint8_t* buf, uint64_t offset, uint64_t size) {
        uint64_t startTime = 0;
        if ((ctx->trace2 & TRACE2_PERFORMANCE) != 0)
            startTime = Timer::getTime();
        int64_t bytes = 0;
        uint64_t tries = ctx->archReadTries;

        while (tries > 0) {
            if (ctx->hardShutdown)
                break;
            bytes = pread(fileDes, buf, size, (int64_t)offset);
            TRACE(TRACE2_FILE, "FILE: read " << fileName << ", " << std::dec << offset << ", " << std::dec << size << " returns " << std::dec << bytes)

            if (bytes > 0)
                break;

            //retry for SSHFS broken connection: Transport endpoint is not isConnected
            if (bytes == -1 && errno != ENOTCONN)
                break;

            ERROR("reading file: " << fileName << " - " << strerror(errno) << " - sleeping " << std::dec << ctx->archReadSleepUs << " us")
            if (ctx->hardShutdown)
                break;
            usleep(ctx->archReadSleepUs);
            --tries;
        }

        //maybe direct IO does not work
        if (bytes < 0 && !FLAG(REDO_FLAGS_DIRECT_DISABLE)) {
            ERROR("HINT: if problem is related to Direct IO, try to restart with Direct IO mode disabled, set 'flags' to value: " << std::dec << REDO_FLAGS_DIRECT_DISABLE)
        }

        if ((ctx->trace2 & TRACE2_PERFORMANCE) != 0) {
            if (bytes > 0)
                sumRead += bytes;
            sumTime += Timer::getTime() - startTime;
        }

        return bytes;
    }
}
