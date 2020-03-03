/* Header for OracleReaderRedo class
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

#include "types.h"
#include "RedoLogRecord.h"

#ifndef ORACLEREADERREDO_H_
#define ORACLEREADERREDO_H_

using namespace std;

#define VECTOR_MAX_LENGTH 512
#define REDO_END                0x0008
#define REDO_ASYNC              0x0100
#define REDO_NODATALOSS         0x0200
#define REDO_RESYNC             0x0800
#define REDO_CLOSEDTHREAD       0x1000
#define REDO_MAXPERFORMANCE     0x2000

namespace OpenLogReplicator {

    class OracleReader;
    class OracleEnvironment;
    class OpCode;

    class OracleReaderRedo {
    private:
        OracleEnvironment *oracleEnvironment;
        int group;
        uint32_t blockSize;
        uint32_t blockNumber;
        uint32_t numBlocks;
        uint32_t lastRead;
        bool lastReadSuccessfull;
        bool lastCheckpointInfo;
        int fileDes;
        typescn lastCheckpointScn;
        typescn curScn;
        uint32_t recordBeginPos;
        uint32_t recordBeginBlock;
        typetime recordTimestmap;
        uint32_t recordPos;
        uint32_t recordLeftToCopy;
        uint32_t redoBufferPos;
        uint64_t redoBufferFileStart;
        uint64_t redoBufferFileEnd;

        void initFile();
        int readFileMore();
        int checkBlockHeader(uint8_t *buffer, uint32_t blockNumberExpected);
        int checkRedoHeader(bool first);
        int processBuffer();
        void analyzeRecord();
        void flushTransactions(bool checkpoint);
        void appendToTransaction(RedoLogRecord *redoLogRecord);
        void appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        uint16_t calcChSum(uint8_t *buffer, uint32_t size);

    public:
        string path;
        typescn firstScn;
        typescn nextScn;
        typeseq sequence;

        void reload();
        void clone(OracleReaderRedo *redo);
        int processLog(OracleReader *oracleReader);
        OracleReaderRedo(OracleEnvironment *oracleEnvironment, int group, const char* path);
        virtual ~OracleReaderRedo();

        friend ostream& operator<<(ostream& os, const OracleReaderRedo& ors);
    };
}

#endif
