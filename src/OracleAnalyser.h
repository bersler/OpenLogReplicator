/* Header for OracleAnalyser class
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

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <occi.h>
#include <stdint.h>

#include "types.h"
#include "Thread.h"
#include "TransactionBuffer.h"
#include "TransactionHeap.h"
#include "TransactionMap.h"

#ifndef ORACLEANALYSER_H_
#define ORACLEANALYSER_H_

using namespace std;
using namespace oracle::occi;

namespace OpenLogReplicator {

    class CommandBuffer;
    class OracleObject;
    class OracleAnalyserRedoLog;
    class Reader;
    class RedoLogRecord;
    class Transaction;

    struct OracleAnalyserRedoLogCompare {
        bool operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2);
    };

    struct OracleAnalyserRedoLogCompareReverse {
        bool operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2);
    };

    class OracleAnalyser : public Thread {
    protected:
        static string SQL_GET_ARCHIVE_LOG_LIST;
        static string SQL_GET_DATABASE_INFORMATION;
        static string SQL_GET_CON_ID;
        static string SQL_GET_CURRENT_SEQUENCE;
        static string SQL_GET_LOGFILE_LIST;
        static string SQL_GET_TABLE_LIST;
        static string SQL_GET_COLUMN_LIST;
        static string SQL_GET_SUPPLEMNTAL_LOG_TABLE;

        typeseq databaseSequence;
        Environment *env;
        Connection *conn;
        string user;
        string passwd;
        string connectString;
        Reader *archReader;
        RedoLogRecord *rolledBack1;
        RedoLogRecord *rolledBack2;

        priority_queue<OracleAnalyserRedoLog*, vector<OracleAnalyserRedoLog*>, OracleAnalyserRedoLogCompare> archiveRedoQueue;
        set<OracleAnalyserRedoLog*> onlineRedoSet;
        set<Reader*> readers;

        void writeCheckpoint(bool atShutdown);
        void readCheckpoint();
        void addToDict(OracleObject *object);
        void checkConnection(bool reconnect);
        void archLogGetList();
        void updateOnlineLogs();
        bool readerCheckRedoLog(Reader *reader, string &path);
        void readerDropAll();
        void checkTableForGrants(string tableName);
        Reader *readerCreate(int64_t group);

    public:
        mutex mtx;
        condition_variable readerCond;
        condition_variable sleepingCond;
        condition_variable analyserCond;
        string database;
        string databaseContext;
        typescn databaseScn;
        unordered_map<typeobj, OracleObject*> objectMap;
        unordered_map<typexid, Transaction*> xidTransactionMap;
        TransactionMap lastOpTransactionMap;
        TransactionHeap transactionHeap;
        TransactionBuffer *transactionBuffer;
        uint8_t *recordBuffer;
        CommandBuffer *commandBuffer;
        ofstream dumpStream;
        uint64_t dumpRedoLog;
        uint64_t dumpRawData;
        uint64_t flags;
        uint64_t disableChecks;
        vector<string> pathMapping;
        uint32_t redoReadSleep;
        uint64_t trace;
        uint64_t trace2;
        uint64_t version;           //compatiblity level of redo logs
        typecon conId;
        typeresetlogs resetlogs;
        clock_t previousCheckpoint;
        uint64_t checkpointInterval;

        uint16_t (*read16)(const uint8_t* buf);
        uint32_t (*read32)(const uint8_t* buf);
        uint64_t (*read56)(const uint8_t* buf);
        uint64_t (*read64)(const uint8_t* buf);
        typescn (*readSCN)(const uint8_t* buf);
        typescn (*readSCNr)(const uint8_t* buf);
        void (*write16)(uint8_t* buf, uint16_t val);
        void (*write32)(uint8_t* buf, uint32_t val);
        void (*write56)(uint8_t* buf, uint64_t val);
        void (*write64)(uint8_t* buf, uint64_t val);
        void (*writeSCN)(uint8_t* buf, typescn val);

        static uint16_t read16Little(const uint8_t* buf);
        static uint16_t read16Big(const uint8_t* buf);
        static uint32_t read32Little(const uint8_t* buf);
        static uint32_t read32Big(const uint8_t* buf);
        static uint64_t read56Little(const uint8_t* buf);
        static uint64_t read56Big(const uint8_t* buf);
        static uint64_t read64Little(const uint8_t* buf);
        static uint64_t read64Big(const uint8_t* buf);
        static typescn readSCNLittle(const uint8_t* buf);
        static typescn readSCNBig(const uint8_t* buf);
        static typescn readSCNrLittle(const uint8_t* buf);
        static typescn readSCNrBig(const uint8_t* buf);

        static void write16Little(uint8_t* buf, uint16_t val);
        static void write16Big(uint8_t* buf, uint16_t val);
        static void write32Little(uint8_t* buf, uint32_t val);
        static void write32Big(uint8_t* buf, uint32_t val);
        static void write56Little(uint8_t* buf, uint64_t val);
        static void write56Big(uint8_t* buf, uint64_t val);
        static void write64Little(uint8_t* buf, uint64_t val);
        static void write64Big(uint8_t* buf, uint64_t val);
        static void writeSCNLittle(uint8_t* buf, typescn val);
        static void writeSCNBig(uint8_t* buf, typescn val);

        void initialize();
        void *run();
        void freeRollbackList();
        bool onRollbackList(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void addToRollbackList(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        OracleObject *checkDict(typeobj objn, typeobj objd);
        void dumpTransactions();
        void addTable(string mask, vector<string> &keys, string &keysStr, uint64_t options);
        void checkForCheckpoint();
        bool readerUpdateRedoLog(Reader *reader);
        virtual void stop(void);
        void addPathMapping(const string source, const string target);

        OracleAnalyser(CommandBuffer *commandBuffer, const string alias, const string database, const string user, const string passwd,
                const string connectString, uint64_t trace, uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpData, uint64_t flags,
                uint64_t disableChecks, uint32_t redoReadSleep, uint64_t checkpointInterval, uint64_t redoBuffers, uint64_t redoBufferSize,
                uint64_t maxConcurrentTransactions);
        virtual ~OracleAnalyser();
    };
}

#endif
