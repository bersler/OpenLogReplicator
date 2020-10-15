/* Header for OracleAnalyzerOnlineASM class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OracleAnalyzerOnline.h"

#ifndef ORACLEANALYZERONLINEASM_H_
#define ORACLEANALYZERONLINEASM_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleAnalyzerOnlineASM : public OracleAnalyzerOnline {
    protected:
        string userASM;
        string passwordASM;
        string connectStringASM;

        virtual const char* getModeName(void);
        virtual Reader *readerCreate(int64_t group);
        virtual void checkConnection(void);

    public:
        DatabaseConnection *connASM;

        OracleAnalyzerOnlineASM(OutputBuffer *outputBuffer, const char *alias, const char *database, uint64_t trace,
                uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpData, uint64_t flags, uint64_t disableChecks,
                uint64_t redoReadSleep, uint64_t archReadSleep, uint64_t memoryMinMb, uint64_t memoryMaxMb,
                const char *user, const char *password, const char *connectString, const char *userASM, const char *passwdASM,
                const char *connectStringASM, bool isStandby);
        virtual ~OracleAnalyzerOnlineASM();
    };
}

#endif
