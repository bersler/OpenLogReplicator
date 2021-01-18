/* Base class for reading redo from file system
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "OracleAnalyzer.h"
#include "ReaderFilesystem.h"

using namespace std;

namespace OpenLogReplicator {

    ReaderFilesystem::ReaderFilesystem(const char *alias, OracleAnalyzer *oracleAnalyzer, uint64_t group) :
        Reader(alias, oracleAnalyzer, group),
        fileDes(0),
        flags(0) {
    }

    ReaderFilesystem::~ReaderFilesystem() {
        redoClose();
    }

    void ReaderFilesystem::redoClose(void) {
        if (fileDes > 0) {
            close(fileDes);
            fileDes = -1;
        }
    }

    uint64_t ReaderFilesystem::redoOpen(void) {
        struct stat fileStat;

        int ret = stat(pathMapped.c_str(), &fileStat);
        TRACE(TRACE2_FILE, "stat for " << pathMapped << " returns " << dec << ret << ", errno = " << errno);
        if (ret != 0) {
            return REDO_ERROR;
        }

        flags = O_RDONLY | O_LARGEFILE;
        fileSize = fileStat.st_size;

        if ((oracleAnalyzer->flags & REDO_FLAGS_DIRECT) == 0)
            flags |= O_DIRECT;
        if ((oracleAnalyzer->flags & REDO_FLAGS_NOATIME) != 0)
            flags |= O_NOATIME;

        fileDes = open(pathMapped.c_str(), flags);
        TRACE(TRACE2_FILE, "open for " << pathMapped << " returns " << dec << fileDes << ", errno = " << errno);

        if (fileDes == -1) {
            if ((oracleAnalyzer->flags & REDO_FLAGS_DIRECT) != 0)
                return REDO_ERROR;

            flags &= (~O_DIRECT);
            fileDes = open(pathMapped.c_str(), flags);
            if (fileDes == -1)
                return REDO_ERROR;

            FULL("file system does not support direct read for: " << pathMapped);
        }

        return REDO_OK;
    }

    int64_t ReaderFilesystem::redoRead(uint8_t *buf, uint64_t pos, uint64_t size) {
        int64_t bytes = pread(fileDes, buf, size, pos);
        TRACE(TRACE2_FILE, "read " << pathMapped << ", " << dec << pos << ", " << dec << size << " returns " << dec << bytes);

        //O_DIRECT does not work
        if (bytes < 0 && (flags & O_DIRECT) != 0) {
            flags &= ~O_DIRECT;
            fcntl(fileDes, F_SETFL, flags);

            //disable direct read and re-try the read
            bytes = pread(fileDes, buf, size, pos);

            //display warning only if this helped
            if (bytes > 0) {
                FULL("disabling direct read for: " << pathMapped);
            }
        }

        return bytes;
    }
}
