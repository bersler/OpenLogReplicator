/* Header for OracleAnalyzer class
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

#include <fstream>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "Thread.h"

#ifndef ORACLEANALYZER_H_
#define ORACLEANALYZER_H_

using namespace std;

namespace OpenLogReplicator {
    class RedoLog;
    class OutputBuffer;
    class Reader;
    class RedoLogRecord;
    class Schema;
    class SystemTransaction;
    class Transaction;
    class TransactionBuffer;

    struct redoLogCompare {
        bool operator()(RedoLog* const& p1, RedoLog* const& p2);
    };

    struct redoLogCompareReverse {
        bool operator()(RedoLog* const& p1, RedoLog* const& p2);
    };

    class OracleAnalyzer : public Thread {
    protected:
        typeSEQ sequence;

        priority_queue<RedoLog*, vector<RedoLog*>, redoLogCompare> archiveRedoQueue;
        set<RedoLog*> onlineRedoSet;
        uint64_t suppLogDbPrimary, suppLogDbAll;
        uint64_t memoryMinMb;
        uint64_t memoryMaxMb;
        uint8_t** memoryChunks;
        uint64_t memoryChunksMin;
        uint64_t memoryChunksAllocated;
        uint64_t memoryChunksFree;
        uint64_t memoryChunksMax;
        uint64_t memoryChunksHWM;
        uint64_t memoryChunksSupplemental;
        string nlsCharacterSet;
        string nlsNcharCharacterSet;
        string dbRecoveryFileDest;
        string dbBlockChecksum;
        string logArchiveDest;
        Reader* archReader;
        set<Reader*> readers;
        bool waitingForWriter;
        mutex mtx;
        condition_variable readerCond;
        condition_variable sleepingCond;
        condition_variable analyzerCond;
        condition_variable memoryCond;
        condition_variable writerCond;
        string context;
        typeSCN checkpointScn;
        typeSCN schemaFirstScn;
        typeSCN schemaScn;
        typeSCN startScn;
        typeSEQ startSequence;
        string startTime;
        int64_t startTimeRel;
        uint64_t readStartOffset;
        uint64_t readBufferMax;
        unordered_map<typeXIDMAP, Transaction*> xidTransactionMap;
        uint64_t disableChecks;
        vector<string> pathMapping;
        vector<string> redoLogsBatch;
        set<typeSCN> checkpointScnList;
        typeCONID conId;
        string conName;
        string lastCheckedDay;
        uint64_t bigEndian;
        bool version12;

        void updateOnlineLogs(void);
        bool readerCheckRedoLog(Reader* reader);
        uint64_t readerDropAll(void);
        static uint64_t getSequenceFromFileName(OracleAnalyzer* oracleAnalyzer, const string& file);
        virtual const char* getModeName(void) const;
        virtual void checkConnection(void);
        virtual bool continueWithOnline(void);
        virtual void createSchema(void);

    public:
        OracleAnalyzer(OutputBuffer* outputBuffer, uint64_t dumpRedoLog, uint64_t dumpRawData, const char* alias, const char* database,
                uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t readBufferMax, uint64_t disableChecks);
        virtual ~OracleAnalyzer();

        typeSCN firstScn;
        string database;
        uint64_t checkpointIntervalS;
        uint64_t checkpointIntervalMB;
        uint64_t checkpointFirst;
        bool checkpointAll;
        bool checkpointOutputCheckpoint;
        bool checkpointOutputLogSwitch;
        typeTIME checkpointLastTime;
        uint64_t checkpointLastOffset;
        string logArchiveFormat;
        string redoCopyPath;
        string checkpointPath;
        ofstream dumpStream;
        uint64_t dumpRedoLog;
        uint64_t version;                   //compatibility level of redo logs
        uint64_t suppLogSize;
        uint64_t dumpRawData;
        Schema* schema;
        OutputBuffer* outputBuffer;
        uint64_t flags;
        uint64_t redoReadSleepUS;
        uint64_t archReadSleepUS;
        uint64_t archReadRetry;
        uint64_t redoVerifyDelayUS;
        SystemTransaction* systemTransaction;
        TransactionBuffer* transactionBuffer;
        typeRESETLOGS resetlogs;
        typeACTIVATION activation;
        uint64_t stopLogSwitches;
        uint64_t stopCheckpoints;
        uint64_t stopTransactions;
        uint64_t transactionMax;
        set<typeXID> skipXidList;
        bool stopFlushBuffer;

        void (*archGetLog)(OracleAnalyzer* oracleAnalyzer);
        uint16_t (*read16)(const uint8_t* buf);
        uint32_t (*read32)(const uint8_t* buf);
        uint64_t (*read56)(const uint8_t* buf);
        uint64_t (*read64)(const uint8_t* buf);
        typeSCN (*readSCN)(const uint8_t* buf);
        typeSCN (*readSCNr)(const uint8_t* buf);
        void (*write16)(uint8_t* buf, uint16_t val);
        void (*write32)(uint8_t* buf, uint32_t val);
        void (*write56)(uint8_t* buf, uint64_t val);
        void (*write64)(uint8_t* buf, uint64_t val);
        void (*writeSCN)(uint8_t* buf, typeSCN val);

        static uint16_t read16Little(const uint8_t* buf);
        static uint16_t read16Big(const uint8_t* buf);
        static uint32_t read32Little(const uint8_t* buf);
        static uint32_t read32Big(const uint8_t* buf);
        static uint64_t read56Little(const uint8_t* buf);
        static uint64_t read56Big(const uint8_t* buf);
        static uint64_t read64Little(const uint8_t* buf);
        static uint64_t read64Big(const uint8_t* buf);
        static typeSCN readSCNLittle(const uint8_t* buf);
        static typeSCN readSCNBig(const uint8_t* buf);
        static typeSCN readSCNrLittle(const uint8_t* buf);
        static typeSCN readSCNrBig(const uint8_t* buf);

        static void write16Little(uint8_t* buf, uint16_t val);
        static void write16Big(uint8_t* buf, uint16_t val);
        static void write32Little(uint8_t* buf, uint32_t val);
        static void write32Big(uint8_t* buf, uint32_t val);
        static void write56Little(uint8_t* buf, uint64_t val);
        static void write56Big(uint8_t* buf, uint64_t val);
        static void write64Little(uint8_t* buf, uint64_t val);
        static void write64Big(uint8_t* buf, uint64_t val);
        static void writeSCNLittle(uint8_t* buf, typeSCN val);
        static void writeSCNBig(uint8_t* buf, typeSCN val);

        void setBigEndian(void);
        virtual void positionReader(void);
        virtual void initialize(void);
        void* run(void);
        virtual Reader* readerCreate(int64_t group);
        void checkOnlineRedoLogs();
        bool readerUpdateRedoLog(Reader* reader);
        virtual void doShutdown(void);
        virtual void goStandby(void);
        void addPathMapping(const char* source, const char* target);
        void addRedoLogsBatch(const char* path);
        static void archGetLogPath(OracleAnalyzer* oracleAnalyzer);
        static void archGetLogList(OracleAnalyzer* oracleAnalyzer);
        void applyMapping(string& path);
        bool checkpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool switchRedo);
        void readCheckpoints(void);
        bool readCheckpointFile(string& fileName, typeSCN fileScn);
        void skipEmptyFields(RedoLogRecord* redoLogRecord, uint64_t& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength);
        uint8_t* getMemoryChunk(const char* module, bool supp);
        void freeMemoryChunk(const char* module, uint8_t* chunk, bool supp);

        bool nextFieldOpt(RedoLogRecord* redoLogRecord, uint64_t& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength, uint32_t code) {
            if (fieldNum >= redoLogRecord->fieldCnt)
                return false;

            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + fieldNum * 2);

            if (fieldPos + fieldLength > redoLogRecord->length) {
                REDOLOG_FAIL("field length out of vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                        ", pos: " << dec << fieldPos <<
                        ", length:" << fieldLength <<
                        ", max: " << redoLogRecord->length <<
                        ", code: " << hex << code);
            }
            return true;
        };

        void nextField(RedoLogRecord* redoLogRecord, uint64_t& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength, uint32_t code) {
            ++fieldNum;
            if (fieldNum > redoLogRecord->fieldCnt) {
                REDOLOG_FAIL("field missing in vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                        ", data: " << dec << redoLogRecord->rowData <<
                        ", obj: " << dec << redoLogRecord->obj <<
                        ", dataObj: " << dec << redoLogRecord->dataObj <<
                        ", op: " << hex << redoLogRecord->opCode <<
                        ", cc: " << dec << (uint64_t)redoLogRecord->cc <<
                        ", suppCC: " << dec << redoLogRecord->suppLogCC <<
                        ", fieldLength: " << dec << fieldLength <<
                        ", code: " << hex << code);
            }

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + fieldNum * 2);

            if (fieldPos + fieldLength > redoLogRecord->length) {
                REDOLOG_FAIL("field length out of vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                        ", pos: " << dec << fieldPos <<
                        ", length:" << fieldLength <<
                        ", max: " << redoLogRecord->length <<
                        ", code: " << hex << code);
            }
        };

        friend ostream& operator<<(ostream& os, const OracleAnalyzer& oracleAnalyzer);
        friend class Reader;
        friend class RedoLog;
        friend class Schema;
        friend class SystemTransaction;
        friend class Writer;
    };
}

#endif
