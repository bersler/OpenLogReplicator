/* Header for RedoLog class
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

#include "types.h"
#include "RedoLogRecord.h"

#ifndef REDOLOG_H_
#define REDOLOG_H_

using namespace std;

#define VECTOR_MAX_LENGTH 512
#define MAX_LWN_CHUNKS (256*2/MEMORY_CHUNK_SIZE_MB)

namespace OpenLogReplicator {

    class OracleAnalyzer;
    class OpCode;
    class Reader;

    struct LwnMember {
        typeSCN scn;
        typeSubSCN subScn;
        typeblk block;
        uint64_t pos;
    };

    class RedoLog {
    protected:
        OracleAnalyzer *oracleAnalyzer;
        OpCode *opCodes[VECTOR_MAX_LENGTH];
        RedoLogRecord zero;
        uint64_t vectors;
        uint64_t lwnConfirmedBlock;
        uint8_t *lwnChunks[MAX_LWN_CHUNKS];
        uint64_t lwnAllocated;
        typetime lwnTimestamp;
        typeSCN lwnScn;
        LwnMember* lwnMembers[MAX_RECORDS_IN_LWN];
        uint64_t lwnRecords;
        uint64_t lwnStartBlock;

        void printHeaderInfo(void) const;
        void analyzeLwn(LwnMember* lwnMember);
        void appendToTransactionDDL(RedoLogRecord *redoLogRecord);
        void appendToTransactionUndo(RedoLogRecord *redoLogRecord);
        void appendToTransactionBegin(RedoLogRecord *redoLogRecord);
        void appendToTransactionCommit(RedoLogRecord *redoLogRecord);
        void appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void dumpRedoVector(uint8_t *data, uint64_t recordLength4) const;

    public:
        int64_t group;
        string path;
        typeSEQ sequence;
        typeSCN firstScn;
        typeSCN nextScn;
        Reader *reader;

        void resetRedo(void);
        void continueRedo(RedoLog *prev);
        uint64_t processLog(void);
        RedoLog(OracleAnalyzer *oracleAnalyzer, int64_t group, const char *path);
        virtual ~RedoLog(void);

        friend ostream& operator<<(ostream& os, const RedoLog& ors);
    };
}

#endif
