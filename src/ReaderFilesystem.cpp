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

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "OracleAnalyzer.h"
#include "ReaderFilesystem.h"

using namespace std;

namespace OpenLogReplicator {
    ReaderFilesystem::ReaderFilesystem(const char* alias, OracleAnalyzer* oracleAnalyzer, uint64_t group) :
        Reader(alias, oracleAnalyzer, group),
        fileDes(-1),
        flags(0) {
    }

    ReaderFilesystem::~ReaderFilesystem() {
        ReaderFilesystem::redoClose();
    }

    void ReaderFilesystem::redoClose(void) {
        if (fileDes != -1) {
            close(fileDes);
            fileDes = -1;
        }
    }

    uint64_t ReaderFilesystem::redoOpen(void) {
        struct stat fileStat;

        int ret = stat(fileName.c_str(), &fileStat);
        TRACE(TRACE2_FILE, "FILE: stat for file: " << fileName << " - " << strerror(errno));
        if (ret != 0) {
            WARNING("reading information for file: " << fileName << " - " << strerror(errno));
            return REDO_ERROR;
        }

        flags = O_RDONLY | O_LARGEFILE;
        fileSize = fileStat.st_size;

        if ((oracleAnalyzer->flags & REDO_FLAGS_DIRECT) == 0)
            flags |= O_DIRECT;
        if ((oracleAnalyzer->flags & REDO_FLAGS_NOATIME) != 0)
            flags |= O_NOATIME;

        fileDes = open(fileName.c_str(), flags);
        TRACE(TRACE2_FILE, "FILE: open for " << fileName << " returns " << dec << fileDes << ", errno = " << errno);

        if (fileDes == -1) {
            ERROR("opening file returned: " << dec << fileName << " - " << strerror(errno));
            return REDO_ERROR;
        }

        return REDO_OK;
    }

    int64_t ReaderFilesystem::redoRead(uint8_t* buf, uint64_t offset, uint64_t size) {
        uint64_t startTime = 0;
        if ((trace2 & TRACE2_PERFORMANCE) != 0)
            startTime = getTime();
        int64_t bytes = 0;
        uint64_t tries = oracleAnalyzer->archReadTries;

        while (tries > 0 && !shutdown) {
            bytes = pread(fileDes, buf, size, offset);
            TRACE(TRACE2_FILE, "FILE: read " << fileName << ", " << dec << offset << ", " << dec << size << " returns " << dec << bytes);

            if (bytes > 0)
                break;

            //retry for SSHFS broken connection: Transport endpoint is not connected
            if (bytes == -1 && errno != ENOTCONN)
                break;

            ERROR("reading file: " << fileName << " - " << strerror(errno) << " - sleeping " << dec << oracleAnalyzer->archReadSleepUs << " us");
            usleep(oracleAnalyzer->archReadSleepUs);
            --tries;
        }

        //O_DIRECT does not work
        if (bytes < 0 && (flags & O_DIRECT) != 0) {
            ERROR("HINT: if problem is related to Direct IO, try to restart with Direct IO mode disabled, set \"flags\" to value: " << dec << REDO_FLAGS_DIRECT);
        }

        if ((trace2 & TRACE2_PERFORMANCE) != 0) {
            if (bytes > 0)
                sumRead += bytes;
            sumTime += getTime() - startTime;
        }

        return bytes;
    }
}
