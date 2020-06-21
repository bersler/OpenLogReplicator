/* Base class for reading redo from file system
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "OracleAnalyser.h"
#include "Reader.h"
#include "ReaderFilesystem.h"

using namespace std;

namespace OpenLogReplicator {

    ReaderFilesystem::ReaderFilesystem(const string alias, OracleAnalyser *oracleAnalyser, uint64_t group) :
        Reader(alias, oracleAnalyser, group),
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

        int ret = stat(path.c_str(), &fileStat);
        if ((oracleAnalyser->trace2 & TRACE2_FILE) != 0)
            cerr << "FILE: stat for " << path << " returns " << dec << ret << endl;
        if (ret != 0) {
            return REDO_ERROR;
        }

        flags = O_RDONLY | O_LARGEFILE;
        fileSize = fileStat.st_size;

        if ((oracleAnalyser->flags & REDO_FLAGS_DIRECT) == 0)
            flags |= O_DIRECT;
        if ((oracleAnalyser->flags & REDO_FLAGS_NOATIME) != 0)
            flags |= O_NOATIME;

        fileDes = open(path.c_str(), flags);
        if ((oracleAnalyser->trace2 & TRACE2_FILE) != 0)
            cerr << "FILE: open for " << path << " returns " << dec << fileDes << endl;

        if (fileDes == -1) {
            if ((oracleAnalyser->flags & REDO_FLAGS_DIRECT) != 0)
                return REDO_ERROR;

            flags &= (~O_DIRECT);
            fileDes = open(path.c_str(), flags);
            if (fileDes == -1)
                return REDO_ERROR;

            if (oracleAnalyser->trace >= TRACE_WARN)
                cerr << "WARNING: file system does not support direct read for: " << path << endl;
        }

        return REDO_OK;
    }

    int64_t ReaderFilesystem::redoRead(uint8_t *buf, uint64_t pos, uint64_t size) {
        int64_t bytes = pread(fileDes, buf, size, pos);

        //O_DIRECT does not work
        if (bytes < 0 && (flags & O_DIRECT) != 0) {
            flags &= ~O_DIRECT;
            fcntl(fileDes, F_SETFL, flags);

            //disable direc read and re-try the read
            bytes = pread(fileDes, buf, size, pos);

            //display warning only if this helped
            if (oracleAnalyser->trace >= TRACE_INFO && bytes > 0)
                cerr << "INFO: disabling direct read for: " << path << endl;
        }

        return bytes;
    }
}
