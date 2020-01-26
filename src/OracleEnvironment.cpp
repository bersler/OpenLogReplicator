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

    OracleEnvironment::OracleEnvironment(CommandBuffer *commandBuffer, uint32_t trace, uint32_t dumpLogFile, bool dumpData, bool directRead) :
        DatabaseEnvironment(),
        redoBuffer(new uint8_t[REDO_LOG_BUFFER_SIZE * 2]),
        headerBuffer(new uint8_t[REDO_PAGE_SIZE_MAX * 2]),
        recordBuffer(new uint8_t[REDO_RECORD_MAX_SIZE]),
        commandBuffer(commandBuffer),
        dumpLogFile(dumpLogFile),
        dumpData(dumpData),
        directRead(directRead),
        trace(trace),
        version(0) {
        transactionHeap.initialize(MAX_CONCURRENT_TRANSACTIONS);
    }

    OracleEnvironment::~OracleEnvironment() {

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

    OracleObject *OracleEnvironment::checkDict(uint32_t objn, uint32_t objd) {
        OracleObject *object = objectMap[objn];
        return object;
    }

    void OracleEnvironment::addToDict(OracleObject *object) {
        if (objectMap[object->objn] == nullptr) {
            objectMap[object->objn] = object;
        }
    }
}
