/* Header for OracleAnalyserRedoLog class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "types.h"
#include "RedoLogRecord.h"

#ifndef ORACLEANALYSERREDO_H_
#define ORACLEANALYSERREDO_H_

using namespace std;

#define VECTOR_MAX_LENGTH 512

namespace OpenLogReplicator {

    class OracleAnalyser;
    class OpCode;
    class Reader;

    class OracleAnalyserRedoLog {
    protected:
        OracleAnalyser *oracleAnalyser;
        typescn lastCheckpointScn;
        typescn extScn;
        typescn curScn;
        typescn curScnPrev;
        typesubscn curSubScn;
        uint64_t recordBeginPos;
        typeblk recordBeginBlock;
        typetime recordTimestmap;
        uint64_t recordPos;
        uint64_t recordLength4;
        uint64_t recordLeftToCopy;
        uint64_t blockNumber;
        OpCode *opCodes[VECTOR_MAX_LENGTH];
        RedoLogRecord zero;
        uint64_t vectors;

        void printHeaderInfo(void);
        void analyzeRecord(void);
        void flushTransactions(typescn checkpointScn);
        void appendToTransaction(RedoLogRecord *redoLogRecord);
        void appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void dumpRedoVector(void);

    public:
        int64_t group;
        string path;
        typeseq sequence;
        typescn firstScn;
        typescn nextScn;
        Reader *reader;

        void resetRedo(void);
        void continueRedo(OracleAnalyserRedoLog *prev);
        uint64_t processLog(void);
        OracleAnalyserRedoLog(OracleAnalyser *oracleAnalyser, int64_t group, const string path);
        virtual ~OracleAnalyserRedoLog(void);

        friend ostream& operator<<(ostream& os, const OracleAnalyserRedoLog& ors);
    };
}

#endif
