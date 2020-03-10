/* Class Oracle database environment variables
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

#include <iostream>
#include <sys/stat.h>
#include "OracleEnvironment.h"
#include "OracleObject.h"
#include "Transaction.h"

using namespace std;

namespace OpenLogReplicator {

    OracleEnvironment::OracleEnvironment(CommandBuffer *commandBuffer, uint64_t trace, uint64_t trace2, uint64_t dumpLogFile, bool dumpData, bool directRead,
            uint64_t sortCols, uint64_t forceCheckpointScn, uint64_t redoBuffers, uint64_t redoBufferSize, uint64_t maxConcurrentTransactions) :
        DatabaseEnvironment(),
        lastOpTransactionMap(maxConcurrentTransactions),
        transactionBuffer(new TransactionBuffer(redoBuffers, redoBufferSize)),
        redoBuffer(new uint8_t[DISK_BUFFER_SIZE * 2]),
        headerBuffer(new uint8_t[REDO_PAGE_SIZE_MAX * 2]),
        recordBuffer(new uint8_t[REDO_RECORD_MAX_SIZE]),
        commandBuffer(commandBuffer),
        dumpLogFile(dumpLogFile),
        dumpData(dumpData),
        directRead(directRead),
        trace(trace),
        trace2(trace2),
        version(0),
        sortCols(sortCols),
        conId(0),
        resetlogsId(0),
        forceCheckpointScn(forceCheckpointScn) {
        transactionHeap.initialize(maxConcurrentTransactions);
    }

    OracleEnvironment::~OracleEnvironment() {
        delete transactionBuffer;

        for (auto it : objectMap) {
            OracleObject *object = it.second;
            delete object;
        }
        objectMap.clear();

        for (auto it : xidTransactionMap) {
            Transaction *transaction = it.second;
            delete transaction;
        }
        xidTransactionMap.clear();

        if (redoBuffer != nullptr) {
            delete[] redoBuffer;
            redoBuffer = nullptr;
        }

        if (headerBuffer != nullptr) {
            delete[] headerBuffer;
            headerBuffer = nullptr;
        }

        if (recordBuffer != nullptr) {
            delete[] recordBuffer;
            recordBuffer = nullptr;
        }
    }

    OracleObject *OracleEnvironment::checkDict(typeobj objn, typeobj objd) {
        OracleObject *object = objectMap[objn];
        return object;
    }

    void OracleEnvironment::addToDict(OracleObject *object) {
        if (objectMap[object->objn] == nullptr) {
            objectMap[object->objn] = object;
        }
    }
}
