/* Hash map class for transactions
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

#include <iomanip>
#include <iostream>
#include <string.h>

#include "OracleAnalyser.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Transaction.h"
#include "TransactionMap.h"

using namespace std;

namespace OpenLogReplicator {

    TransactionMap::TransactionMap(OracleAnalyser *oracleAnalyser, uint64_t maps) :
        oracleAnalyser(oracleAnalyser),
        maps(0),
        elements(0) {

        for (uint64_t i = 0; i < maps; ++i) {
            hashMapList[i] = (Transaction **)oracleAnalyser->getMemoryChunk("MAP", false);
            memset(hashMapList[i], 0, MEMORY_CHUNK_SIZE);
            ++this->maps;
        }
    }

    TransactionMap::~TransactionMap() {
        while (maps > 0)
            oracleAnalyser->freeMemoryChunk("MAP", (uint8_t*)hashMapList[--maps], false);
    }

    void TransactionMap::set(Transaction* transaction) {
        if (transaction->lastRedoLogRecord1 == nullptr) {
            RUNTIME_FAIL("trying to set empty last record in transaction map");
        }

        uint64_t hashKey = HASHINGFUNCTION(transaction->lastRedoLogRecord1->uba, transaction->lastRedoLogRecord1->slt,
                transaction->lastRedoLogRecord1->rci);

        Transaction *transactionTmp = MAP_AT(hashKey);
        while (transactionTmp != nullptr) {
            if (transactionTmp == transaction) {
                RUNTIME_FAIL("transaction already present in hash map");
            }
            transactionTmp = transactionTmp->next;
        }

        transaction->next = MAP_AT(hashKey);
        MAP_AT(hashKey) = transaction;
        ++elements;
    }

    void TransactionMap::erase(Transaction * transaction) {
        uint64_t hashKey = HASHINGFUNCTION(transaction->lastRedoLogRecord1->uba, transaction->lastRedoLogRecord1->slt,
                transaction->lastRedoLogRecord1->rci);

        if (MAP_AT(hashKey) == nullptr) {
            RUNTIME_FAIL("transaction does not exist in hash map1, codes: " << dec << transaction->opCodes <<
                    ", UBA: " << PRINTUBA(transaction->lastRedoLogRecord1->uba) <<
                    ", SLT: " << (uint64_t)transaction->lastRedoLogRecord1->slt <<
                    ", RCI: " << transaction->lastRedoLogRecord1->rci);
        }

        Transaction *transactionTmp = MAP_AT(hashKey);
        if (transactionTmp == transaction) {
            MAP_AT(hashKey) = transactionTmp->next;
            transactionTmp->next = nullptr;
            --elements;
            return;
        }
        Transaction *transactionTmpNext = transactionTmp->next;
        while (transactionTmpNext != nullptr) {
            if (transactionTmpNext == transaction) {
                transactionTmp->next = transactionTmpNext->next;
                transactionTmpNext->next = nullptr;
                --elements;
                return;
            }
            transactionTmp = transactionTmpNext;
            transactionTmpNext = transactionTmp->next;
        }

        RUNTIME_FAIL("transaction does not exist in hash map2, codes: " << dec << transaction->opCodes <<
                ", UBA: " << PRINTUBA(transaction->lastRedoLogRecord1->uba) <<
                ", SLT: " << (uint64_t)transaction->lastRedoLogRecord1->slt <<
                ", RCI: " << transaction->lastRedoLogRecord1->rci);
    }

    //typeuba uba, typedba dba, typeslt slt, typerci rci, typescn scn, uint64_t opFlags
    Transaction* TransactionMap::getMatchForRollback(RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2) {
        uint64_t hashKey = HASHINGFUNCTION(rollbackRedoLogRecord1->uba, rollbackRedoLogRecord2->slt, rollbackRedoLogRecord2->rci);
        Transaction *transactionTmp = MAP_AT(hashKey);

        while (transactionTmp != nullptr) {
            if (Transaction::matchesForRollback(transactionTmp->lastRedoLogRecord1, transactionTmp->lastRedoLogRecord2,
                    rollbackRedoLogRecord1, rollbackRedoLogRecord2))
                return transactionTmp;

            transactionTmp = transactionTmp->next;
        }

        return nullptr;
    }
}
