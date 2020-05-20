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
        fileDes(0) {
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
            return false;
        fileSize = fileStat.st_size;

        fileDes = open(path, O_RDONLY | O_LARGEFILE | ((oracleAnalyser->directRead > 0) ? O_DIRECT : 0));
        if (fileDes == -1)
            return REDO_ERROR;
        else
            return REDO_OK;
    }

    uint64_t ReaderFilesystem::redoRead(uint8_t *buf, uint64_t pos, uint64_t size) {
        return pread(fileDes, buf, size, pos);
    }
}
