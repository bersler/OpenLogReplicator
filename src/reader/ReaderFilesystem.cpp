/* Base class for reading redo from file system
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

enum {
    _FILE_OFFSET_BITS = 64
};

#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "ReaderFilesystem.h"

namespace OpenLogReplicator {
    ReaderFilesystem::ReaderFilesystem(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum):
        Reader(newCtx, std::move(newAlias), std::move(newDatabase), newGroup, newConfiguredBlockSum) {}

    ReaderFilesystem::~ReaderFilesystem() {
        ReaderFilesystem::redoClose();
    }

    void ReaderFilesystem::redoClose() {
        if (fileDes != -1) {
            contextSet(CONTEXT::OS, REASON::OS);
            close(fileDes);
            contextSet(CONTEXT::CPU);
            fileDes = -1;
        }
    }

    Reader::REDO_CODE ReaderFilesystem::redoOpen() {
        struct stat fileStat{};

        contextSet(CONTEXT::OS, REASON::OS);
        const int statRet = stat(fileName.c_str(), &fileStat);
        contextSet(CONTEXT::CPU);
        if (statRet != 0) {
            ctx->error(10003, "file: " + fileName + " - get metadata returned: " + strerror(errno));
            return REDO_CODE::ERROR;
        }

        flags = O_RDONLY;
        fileSize = fileStat.st_size;
        if ((fileSize & (Ctx::MIN_BLOCK_SIZE - 1)) != 0) {
            fileSize &= ~(Ctx::MIN_BLOCK_SIZE - 1);
            ctx->warning(10071, "file: " + fileName + " size is not a multiplication of " + std::to_string(Ctx::MIN_BLOCK_SIZE) + ", reading only " +
                         std::to_string(fileSize) + " bytes ");
        }

#if __linux__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        contextSet(CONTEXT::OS, REASON::OS);
        fileDes = open(fileName.c_str(), flags);
        contextSet(CONTEXT::CPU);
        if (fileDes == -1) {
            ctx->error(10001, "file: " + fileName + " - open for read returned: " + strerror(errno));
            return REDO_CODE::ERROR;
        }

#if __APPLE__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE)) {
            contextSet(CONTEXT::OS, REASON::OS);
            const int fcntlRet = fcntl(fileDes, F_GLOBAL_NOCACHE, 1);
            contextSet(CONTEXT::CPU);
            if (fcntlRet < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        return REDO_CODE::OK;
    }

    int ReaderFilesystem::redoRead(uint8_t* buf, uint64_t offset, uint size) {
        uint64_t startTime = 0;
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::PERFORMANCE)))
            startTime = ctx->clock->getTimeUt();
        int bytes = 0;
        uint tries = ctx->archReadTries;

        while (tries > 0) {
            if (ctx->hardShutdown)
                break;
            contextSet(CONTEXT::OS, REASON::OS);
            bytes = pread(fileDes, buf, size, static_cast<int64_t>(offset));
            contextSet(CONTEXT::CPU);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
                ctx->logTrace(Ctx::TRACE::FILE, "read " + fileName + ", " + std::to_string(offset) + ", " + std::to_string(size) +
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
            contextSet(CONTEXT::SLEEP);
            ctx->usleepInt(ctx->archReadSleepUs);
            contextSet(CONTEXT::CPU);
            --tries;
        }

        // Maybe direct IO does not work
        if (bytes < 0 && !ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE)) {
            ctx->hint("if problem is related to Direct IO, try to restart with Direct IO mode disabled, set 'flags' to value: " +
                    std::to_string(static_cast<uint>(Ctx::REDO_FLAGS::DIRECT_DISABLE)));
        }

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::PERFORMANCE))) {
            if (bytes > 0)
                sumRead += bytes;
            sumTime += ctx->clock->getTimeUt() - startTime;
        }

        return bytes;
    }

    void ReaderFilesystem::showHint(Thread* t, std::string origPath, std::string mappedPath) const {
        bool first = true;
        uid_t uid = geteuid();
        gid_t gid = getegid();

        if (origPath.empty())
            ctx->hint("check mapping, failed to read: " + mappedPath + " run as uid: " + std::to_string(uid) + " gid: " + std::to_string(gid));
        else
            ctx->hint("check mapping, failed to read: " + origPath + " mapped to: " + mappedPath + " run as uid: " + std::to_string(uid) +
                      " gid: " + std::to_string(gid));

        while (!mappedPath.empty()) {
            std::string partialFileName;
            if (!first) {
                size_t found = mappedPath.find_last_of("/\\");
                partialFileName = mappedPath.substr(found + 1);
                mappedPath.resize(found);
            }
            if (mappedPath.empty())
                break;

            struct stat fileStat{};
            t->contextSet(CONTEXT::OS, REASON::OS);
            const int statRet = stat(mappedPath.c_str(), &fileStat);
            t->contextSet(CONTEXT::CPU);

            // try with stat
            if (statRet != 0) {
                ctx->hint("- path: " + mappedPath + " - get metadata returned: " + strerror(errno));
                first = false;
                continue;
            }

            std::string fileType;
            switch (fileStat.st_mode & S_IFMT) {
                case S_IFBLK:
                    fileType = "block device";
                    break;
                case S_IFCHR:
                    fileType = "character device";
                    break;
                case S_IFDIR:
                    fileType = "directory";
                    break;
                case S_IFIFO:
                    fileType = "FIFO/pipe";
                    break;
                case S_IFLNK:
                    fileType = "symlink";
                    break;
                case S_IFREG:
                    fileType = "regular file";
                    break;
                case S_IFSOCK:
                    fileType = "socket";
                    break;
                default:
                    fileType = "unknown?";
            }

            std::stringstream permissions;
            permissions << std::oct << fileStat.st_mode;

            ctx->hint("- path: " + mappedPath + " - type: " + fileType + " permissions: " + permissions.str() +
                    " uid: " + std::to_string(fileStat.st_uid) + " gid: " + std::to_string(fileStat.st_gid));

            DIR* dir = opendir(mappedPath.c_str());
            if (dir == nullptr) {
                ctx->hint("- path: " + mappedPath + " - get metadata returned: " + strerror(errno));
                first = false;
                continue;
            }

            bool found = false;
            const dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                const std::string dName(ent->d_name);
                if (dName == "." || dName == "..")
                    continue;

                const std::string localFileName(ent->d_name);
                if (partialFileName != localFileName)
                    continue;

                found = true;
                break;
            }
            closedir(dir);

            if (!found)
                ctx->hint("- path: " + mappedPath + " - can be listed but does not contain: " + partialFileName);

            first = false;
        }
    }
}
