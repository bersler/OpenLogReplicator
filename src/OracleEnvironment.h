/* Header for OracleEnvironment class
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

#include <unordered_map>
#include <string>
#include <iostream>
#include <fstream>

#include "CommandBuffer.h"
#include "types.h"
#include "DatabaseEnvironment.h"
#include "TransactionMap.h"
#include "TransactionHeap.h"
#include "TransactionBuffer.h"

#ifndef ORACLEENVIRONMENT_H_
#define ORACLEENVIRONMENT_H_

#define TRACE_NO 0
#define TRACE_WARN 1
#define TRACE_INFO 2
#define TRACE_DETAIL 3
#define TRACE_FULL 4

#define TRACE2_DISK        0x0000001
#define TRACE2_TRANSACTION 0x0000002
#define TRACE2_DUMP        0x0000004
#define TRACE2_UBA         0x0000008
#define TRACE2_REDO        0x0000010

using namespace std;

namespace OpenLogReplicator {

    class OracleObject;
    class Transaction;

    class OracleEnvironment : public DatabaseEnvironment {
    public:
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
        uint64_t dumpLogFile;
        bool dumpData;
        bool directRead;
        uint64_t trace;
        uint64_t trace2;
        uint32_t version;           //compatiblity level of redo logs
        uint64_t sortCols;          //1 - sort cols for UPDATE operations, 2 - sort cols & remove unchanged values
        typecon conId;
        typeresetlogs resetlogsId;
        uint64_t forceCheckpointScn;

        OracleObject *checkDict(typeobj objn, typeobj objd);
        void addToDict(OracleObject *object);
        void transactionNew(typexid xid);
        void transactionAppend(typexid xid);

        OracleEnvironment(CommandBuffer *commandBuffer, uint64_t trace, uint64_t trace2, uint64_t dumpLogFile, bool dumpData, bool directRead,
                uint64_t sortCols, uint64_t forceCheckpointScn, uint64_t redoBuffers, uint64_t redoBufferSize, uint64_t maxConcurrentTransactions);
        virtual ~OracleEnvironment();
    };
}

#endif
