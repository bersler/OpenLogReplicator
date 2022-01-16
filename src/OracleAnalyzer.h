/* Header for OracleAnalyzer class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

namespace OpenLogReplicator {
    class RedoLog;
    class OutputBuffer;
    class OracleIncarnation;
    class Reader;
    class RedoLogRecord;
    class Schema;
    class SystemTransaction;
    class State;
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
        uint64_t offset;
        typeSCN nextScn;

        std::priority_queue<RedoLog*, std::vector<RedoLog*>, redoLogCompare> archiveRedoQueue;
        std::set<RedoLog*> onlineRedoSet;
        uint64_t suppLogDbPrimary;
        uint64_t suppLogDbAll;
        uint64_t memoryMinMb;
        uint64_t memoryMaxMb;
        uint8_t** memoryChunks;
        uint64_t memoryChunksMin;
        uint64_t memoryChunksAllocated;
        uint64_t memoryChunksFree;
        uint64_t memoryChunksMax;
        uint64_t memoryChunksHWM;
        uint64_t memoryChunksSupplemental;
        std::string nlsCharacterSet;
        std::string nlsNcharCharacterSet;
        std::string dbRecoveryFileDest;
        std::string dbBlockChecksum;
        std::string logArchiveDest;
        Reader* archReader;
        std::set<Reader*> readers;
        bool waitingForWriter;
        std::mutex mtx;
        std::condition_variable readerCond;
        std::condition_variable sleepingCond;
        std::condition_variable analyzerCond;
        std::condition_variable memoryCond;
        std::condition_variable writerCond;
        std::string context;
        typeSCN checkpointScn;
        typeSCN schemaFirstScn;
        typeSCN schemaScn;
        typeSCN startScn;
        typeSEQ startSequence;
        std::string startTime;
        int64_t startTimeRel;
        uint64_t readBufferMax;
        std::unordered_map<typeXIDMAP, Transaction*> xidTransactionMap;
        uint64_t disableChecks;
        std::vector<std::string> pathMapping;
        std::vector<std::string> redoLogsBatch;
        std::set<typeSCN> checkpointScnList;
        typeCONID conId;
        std::string conName;
        std::string lastCheckedDay;
        uint64_t bigEndian;
        bool version12;
        bool schemaChanged;
        bool activationChanged;

        void updateOnlineLogs(void);
        bool readerCheckRedoLog(Reader* reader);
        uint64_t readerDropAll(void);
        void updateResetlogs(void);
        static uint64_t getSequenceFromFileName(OracleAnalyzer* oracleAnalyzer, const std::string& file);
        virtual const char* getModeName(void) const;
        virtual bool checkConnection(void);
        virtual bool continueWithOnline(void);
        virtual void createSchema(void);
        virtual void updateOnlineRedoLogData(void);

    public:
        OracleAnalyzer(OutputBuffer* outputBuffer, uint64_t dumpRedoLog, uint64_t dumpRawData, const char* dumpPath, const char* alias,
                const char* database, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t readBufferMax, uint64_t disableChecks);
        virtual ~OracleAnalyzer();

        typeSCN firstScn;
        std::string database;
        uint64_t checkpointIntervalS;
        uint64_t checkpointIntervalMB;
        uint64_t checkpointFirst;
        bool checkpointAll;
        bool checkpointOutputCheckpoint;
        bool checkpointOutputLogSwitch;
        typeTIME checkpointLastTime;
        uint64_t checkpointLastOffset;
        std::string logArchiveFormat;
        std::string redoCopyPath;
        State *state;
        std::ofstream dumpStream;
        uint64_t dumpRedoLog;
        uint64_t dumpRawData;
        std::string dumpPath;
        uint64_t version;                   //compatibility level of redo logs
        uint64_t suppLogSize;
        Schema* schema;
        OutputBuffer* outputBuffer;
        uint64_t flags;
        uint64_t redoReadSleepUs;
        uint64_t archReadSleepUs;
        uint64_t archReadTries;
        uint64_t redoVerifyDelayUs;
        uint64_t refreshIntervalUs;
        SystemTransaction* systemTransaction;
        TransactionBuffer* transactionBuffer;
        typeRESETLOGS resetlogs;
        typeACTIVATION activation;
        uint64_t stopLogSwitches;
        uint64_t stopCheckpoints;
        uint64_t stopTransactions;
        uint64_t transactionMax;
        std::set<typeXID> skipXidList;
        std::set<typeXIDMAP> brokenXidMapList;
        bool stopFlushBuffer;
        std::set<OracleIncarnation*> oiSet;
        OracleIncarnation* oiCurrent;

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

        void initialize(void);
        void setBigEndian(void);
        virtual void positionReader(void);
        virtual void loadDatabaseMetadata(void);
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
        void applyMapping(std::string& path);
        bool checkpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool switchRedo);
        void readCheckpoints(void);
        bool readCheckpoint(std::string& jsonName, typeSCN fileScn);
        void skipEmptyFields(RedoLogRecord* redoLogRecord, typeFIELD& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength);
        uint8_t* getMemoryChunk(const char* module, bool supp);
        void freeMemoryChunk(const char* module, uint8_t* chunk, bool supp);

        bool nextFieldOpt(RedoLogRecord* redoLogRecord, typeFIELD& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength, uint32_t code) {
            if (fieldNum >= redoLogRecord->fieldCnt)
                return false;

            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (((uint64_t)fieldNum) * 2));

            if (fieldPos + fieldLength > redoLogRecord->length) {
                REDOLOG_FAIL("field length out of vector, field: " << std::dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                        ", pos: " << std::dec << fieldPos <<
                        ", length:" << fieldLength <<
                        ", max: " << redoLogRecord->length <<
                        ", code: " << std::hex << code);
            }
            return true;
        };

        void nextField(RedoLogRecord* redoLogRecord, typeFIELD& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength, uint32_t code) {
            ++fieldNum;
            if (fieldNum > redoLogRecord->fieldCnt) {
                REDOLOG_FAIL("field missing in vector, field: " << std::dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                        ", data: " << std::dec << redoLogRecord->rowData <<
                        ", obj: " << std::dec << redoLogRecord->obj <<
                        ", dataObj: " << std::dec << redoLogRecord->dataObj <<
                        ", op: " << std::hex << redoLogRecord->opCode <<
                        ", cc: " << std::dec << (uint64_t)redoLogRecord->cc <<
                        ", suppCC: " << std::dec << redoLogRecord->suppLogCC <<
                        ", fieldLength: " << std::dec << fieldLength <<
                        ", code: " << std::hex << code);
            }

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (((uint64_t)fieldNum) * 2));

            if (fieldPos + fieldLength > redoLogRecord->length) {
                REDOLOG_FAIL("field length out of vector, field: " << std::dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                        ", pos: " << std::dec << fieldPos <<
                        ", length:" << fieldLength <<
                        ", max: " << redoLogRecord->length <<
                        ", code: " << std::hex << code);
            }
        };

        friend std::ostream& operator<<(std::ostream& os, const OracleAnalyzer& oracleAnalyzer);
        friend class Reader;
        friend class RedoLog;
        friend class Schema;
        friend class SystemTransaction;
        friend class Writer;
    };
}

#endif
