/* Header for OracleReader class
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

#include <set>
#include <queue>
#include <unordered_map>
#include <string>
#include <iostream>
#include <fstream>
#include <stdint.h>
#include <occi.h>

#include "CommandBuffer.h"
#include "types.h"
#include "TransactionMap.h"
#include "TransactionHeap.h"
#include "TransactionBuffer.h"
#include "Thread.h"

#ifndef ORACLEREADER_H_
#define ORACLEREADER_H_

using namespace std;
using namespace oracle::occi;

namespace OpenLogReplicator {

    class CommandBuffer;
    class OracleObject;
    class OracleReaderRedo;
    class Transaction;

    struct OracleReaderRedoCompare {
        bool operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2);
    };

    struct OracleReaderRedoCompareReverse {
        bool operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2);
    };

    class OracleReader : public Thread {
    protected:
        OracleReaderRedo* currentRedo;
        typeseq databaseSequence;
        typeseq databaseSequenceArchMax;
        Environment *env;
        Connection *conn;
        string user;
        string passwd;
        string connectString;

        priority_queue<OracleReaderRedo*, vector<OracleReaderRedo*>, OracleReaderRedoCompare> archiveRedoQueue;
        set<OracleReaderRedo*> onlineRedoSet;
        set<OracleReaderRedo*> archiveRedoSet;

        void checkConnection(bool reconnect);
        void archLogGetList();
        void onlineLogGetList();
        void refreshOnlineLogs();

    public:
        string database;
        string databaseContext;
        typescn databaseScn;
        unordered_map<typeobj, OracleObject*> objectMap;
        unordered_map<typexid, Transaction*> xidTransactionMap;
        TransactionMap lastOpTransactionMap;
        TransactionHeap transactionHeap;
        TransactionBuffer *transactionBuffer;
        uint8_t *redoBuffer;
        uint8_t *headerBuffer;
        uint8_t *recordBuffer;
        CommandBuffer *commandBuffer;
        ofstream dumpStream;
        uint64_t dumpRedoLog;
        uint64_t dumpRawData;
        uint64_t directRead;
        uint64_t redoReadSleep;
        uint64_t trace;
        uint64_t trace2;
        uint64_t version;           //compatiblity level of redo logs
        typecon conId;
        typeresetlogs resetlogs;
        clock_t previousCheckpoint;
        uint64_t checkpointInterval;
        bool bigEndian;

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

        OracleObject *checkDict(typeobj objn, typeobj objd);
        void addToDict(OracleObject *object);
        void transactionNew(typexid xid);
        void transactionAppend(typexid xid);
        virtual void *run();
        void addTable(string mask, uint64_t options);
        void readCheckpoint();
        void writeCheckpoint(bool atShutdown);
        void checkForCheckpoint();
        uint64_t initialize();
        void dumpTransactions();

        OracleReader(CommandBuffer *commandBuffer, const string alias, const string database, const string user, const string passwd,
                const string connectString, uint64_t trace, uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpData, uint64_t directRead,
                uint64_t redoReadSleep, uint64_t checkpointInterval, uint64_t redoBuffers, uint64_t redoBufferSize, uint64_t maxConcurrentTransactions);
        virtual ~OracleReader();
    };
}

#endif
