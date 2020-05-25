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

#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "Reader.h"
#include "ReaderFilesystem.h"
#include "OracleAnalyser.h"

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

    void ReaderFilesystem::redoClose() {
        if (fileDes > 0) {
            close(fileDes);
            fileDes = -1;
        }
    }

    uint64_t ReaderFilesystem::redoOpen() {
        struct stat fileStat;

        if (stat(path, &fileStat) != 0)
            return REDO_ERROR;

        flags = O_RDONLY | O_LARGEFILE;
        fileSize = fileStat.st_size;

        if ((oracleAnalyser->directRead & 1) == 1)
            flags |= O_DIRECT;
        if ((oracleAnalyser->directRead & 2) == 2)
            flags |= O_SYNC;

        fileDes = open(path, flags);
        if (fileDes == -1) {
            if (oracleAnalyser->directRead ==  0)
                return REDO_ERROR;

            flags &= (~O_DIRECT);
            fileDes = open(path, flags);
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
            if (oracleAnalyser->trace >= TRACE_WARN)
                cerr << "WARNING: disabling direct read for: " << path << endl;

            //disable it and re-try the read
            bytes = pread(fileDes, buf, size, pos);
        }

        return bytes;
    }
}
