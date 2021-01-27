/* Thread reading Oracle Redo Logs using batch mode
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

#include "OracleAnalyzerBatch.h"
#include "RuntimeException.h"

using namespace std;

extern void stopMain();

namespace OpenLogReplicator {

    OracleAnalyzerBatch::OracleAnalyzerBatch(OutputBuffer *outputBuffer, const char *alias, const char *database, uint64_t trace,
            uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpRawData, uint64_t flags, uint64_t disableChecks, uint64_t redoReadSleep,
            uint64_t archReadSleep, uint64_t redoVerifyDelay, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t readBufferMax,
            const char *logArchiveFormat, const char *redoCopyPath, typeconid conId) :
                    OracleAnalyzer(outputBuffer, alias, database, trace, trace2, dumpRedoLog, dumpRawData, flags, disableChecks,
                    redoReadSleep, archReadSleep, redoVerifyDelay, memoryMinMb, memoryMaxMb, readBufferMax, logArchiveFormat,
                    redoCopyPath) {
        this->conId = conId;
    }

    OracleAnalyzerBatch::~OracleAnalyzerBatch() {
    }

    void OracleAnalyzerBatch::start(void) {
        initializeSchema();
        context = database;
        if (scn == ZERO_SCN)
            scn = 0;
    }

    const char* OracleAnalyzerBatch::getModeName(void) const {
        return "batch";
    }

    bool OracleAnalyzerBatch::continueWithOnline(void) {
        INFO_("finished batch processing, exiting");
        stopMain();
        return false;
    }
}
