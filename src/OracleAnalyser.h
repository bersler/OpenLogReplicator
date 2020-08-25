/* Header for OracleAnalyser class
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

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdint.h>

#include "types.h"
#include "Thread.h"
#include "TransactionBuffer.h"
#include "TransactionHeap.h"
#include "TransactionMap.h"

#ifndef ORACLEANALYSER_H_
#define ORACLEANALYSER_H_

using namespace std;

namespace OpenLogReplicator {

    class DatabaseConnection;
    class DatabaseEnvironment;
    class OracleObject;
    class OracleAnalyserRedoLog;
    class OutputBuffer;
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
        static string SQL_GET_CON_INFO;
        static string SQL_GET_CURRENT_SEQUENCE;
        static string SQL_GET_LOGFILE_LIST;
        static string SQL_GET_TABLE_LIST;
        static string SQL_GET_COLUMN_LIST;
        static string SQL_GET_COLUMN_LIST_INV;
        static string SQL_GET_PARTITION_LIST;
        static string SQL_GET_SUPPLEMNTAL_LOG_TABLE;
        static string SQL_GET_PARAMETER;
        static string SQL_GET_PROPERTY;

        typeseq databaseSequence;
        string user;
        string password;
        string connectString;
        string userASM;
        string passwordASM;
        string connectStringASM;
        string database;
        string nlsCharacterSet;
        string nlsNcharCharacterSet;
        string dbRecoveryFileDest;
        string logArchiveFormat;
        string logArchiveDest;
        Reader *archReader;
        RedoLogRecord *rolledBack1;
        RedoLogRecord *rolledBack2;

        priority_queue<OracleAnalyserRedoLog*, vector<OracleAnalyserRedoLog*>, OracleAnalyserRedoLogCompare> archiveRedoQueue;
        set<OracleAnalyserRedoLog*> onlineRedoSet;
        set<Reader*> readers;
        unordered_map<typeobj, OracleObject*> objectMap;
        unordered_map<typeobj, OracleObject*> partitionMap;
        uint64_t suppLogDbPrimary, suppLogDbAll;
        clock_t previousCheckpoint;
        uint64_t checkpointInterval;
        uint64_t memoryMinMb;
        uint64_t memoryMaxMb;
        uint8_t **memoryChunks;
        uint64_t memoryChunksMin;
        uint64_t memoryChunksAllocated;
        uint64_t memoryChunksFree;
        uint64_t memoryChunksMax;
        uint64_t memoryChunksHWM;
        uint64_t memoryChunksSupplemental;
        OracleObject *object;

        stringstream& writeEscapeValue(stringstream &ss, string &str);
        string getParameterValue(const char *parameter);
        string getPropertyValue(const char *property);
        void writeCheckpoint(bool atShutdown);
        void readCheckpoint(void);
        void addToDict(OracleObject *object);
        void checkConnection(bool reconnect);
        void archLogGetList(void);
        void updateOnlineLogs(void);
        bool readerCheckRedoLog(Reader *reader);
        void readerDropAll(void);
        void checkTableForGrants(string tableName);
        Reader *readerCreate(int64_t group);
        void checkOnlineRedoLogs();
        uint64_t getSequenceFromFileName(const char *file);

    public:
        OracleAnalyser(OutputBuffer *outputBuffer, const string alias, const string database, const string user, const string passwd, const string connectString,
                const string userASM, const string passwdASM, const string connectStringASM, uint64_t trace, uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpData,
                uint64_t flags, uint64_t modeType, uint64_t disableChecks, uint64_t redoReadSleep, uint64_t archReadSleep, uint64_t checkpointInterval,
                uint64_t memoryMinMb, uint64_t memoryMaxMb);
        virtual ~OracleAnalyser();

        DatabaseEnvironment *env;
        DatabaseConnection *conn;
        DatabaseConnection *connASM;
        bool waitingForKafkaWriter;
        mutex mtx;
        condition_variable readerCond;
        condition_variable sleepingCond;
        condition_variable analyserCond;
        condition_variable memoryCond;
        string databaseContext;
        typescn databaseScn;
        unordered_map<typexid, Transaction*> xidTransactionMap;
        TransactionMap *lastOpTransactionMap;
        TransactionHeap *transactionHeap;
        TransactionBuffer *transactionBuffer;
        uint8_t recordBuffer[REDO_RECORD_MAX_SIZE];
        OutputBuffer *outputBuffer;
        ofstream dumpStream;
        uint64_t dumpRedoLog;
        uint64_t dumpRawData;
        uint64_t flags;
        uint64_t modeType;
        uint64_t disableChecks;
        vector<string> pathMapping;
        vector<string> redoLogsBatch;
        uint64_t redoReadSleep;
        uint64_t archReadSleep;
        uint64_t trace;
        uint64_t trace2;
        uint64_t version;                   //compatibility level of redo logs
        typecon conId;
        string conName;
        string lastCheckedDay;
        typeresetlogs resetlogs;
        typeactivation activation;
        uint64_t isBigEndian;
        uint64_t suppLogSize;

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

        void initializeOnlineMode(void);
        bool readSchema(void);
        void writeSchema(void);
        void *run(void);
        void freeRollbackList(void);
        bool onRollbackList(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void addToRollbackList(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        OracleObject *checkDict(typeobj objn, typeobj objd);
        void addTable(string mask, vector<string> &keys, string &keysStr, uint64_t options);
        void checkForCheckpoint(void);
        bool readerUpdateRedoLog(Reader *reader);
        virtual void stop(void);
        void addPathMapping(const string source, const string target);
        void addRedoLogsBatch(const string path);

        void skipEmptyFields(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength);
        void nextField(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength);
        bool nextFieldOpt(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength);
        string applyMapping(string path);
        void printRollbackInfo(RedoLogRecord *redoLogRecord, Transaction *transaction, const char *msg);
        void printRollbackInfo(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, Transaction *transaction, const char *msg);

        uint8_t *getMemoryChunk(const char *module, bool supp);
        void freeMemoryChunk(const char *module, uint8_t *chunk, bool supp);

        friend ostream& operator<<(ostream& os, const OracleAnalyser& oracleAnalyser);
    };
}

#endif
