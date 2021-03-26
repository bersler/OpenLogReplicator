/* Header for OracleAnalyzerBatch class
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

#include "OracleAnalyzer.h"

#ifndef ORACLEANALYZERBATCH_H_
#define ORACLEANALYZERBATCH_H_

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzerBatch : public OracleAnalyzer {
    protected:
        virtual const char* getModeName(void) const;
        virtual bool continueWithOnline(void);
        virtual void start(void);

    public:
        OracleAnalyzerBatch(OutputBuffer *outputBuffer, uint64_t dumpRedoLog, uint64_t dumpRawData, const char *alias,
                const char *database, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t readBufferMax, uint64_t disableChecks,
                typeconid conId);
        virtual ~OracleAnalyzerBatch();
    };
}

#endif
