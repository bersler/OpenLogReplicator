/* Hash map class for transactions
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
along with Open Log Replicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <iomanip>
#include <iostream>
#include <string.h>

#include "MemoryException.h"
#include "RedoLogRecord.h"
#include "Transaction.h"
#include "TransactionMap.h"

using namespace std;

namespace OpenLogReplicator {

    void TransactionMap::set(Transaction* transaction) {
        if (transaction->lastRedoLogRecord1 == nullptr) {
            cerr << "ERROR: trying to set empty last record in transaction map" << endl;
            return;
        }

        uint64_t hashKey = HASHINGFUNCTION(transaction->lastRedoLogRecord1->uba, transaction->lastRedoLogRecord1->slt,
                transaction->lastRedoLogRecord1->rci);

        Transaction *transactionTmp = hashMap[hashKey];
        while (transactionTmp != nullptr) {
            if (transactionTmp == transaction) {
                cerr << "ERROR: transaction already present in hash map" << endl;
                return;
            }
            transactionTmp = transactionTmp->next;
        }

        transaction->next = hashMap[hashKey];
        hashMap[hashKey] = transaction;
        ++elements;
    }

    void TransactionMap::erase(Transaction * transaction) {
        uint64_t hashKey = HASHINGFUNCTION(transaction->lastRedoLogRecord1->uba, transaction->lastRedoLogRecord1->slt,
                transaction->lastRedoLogRecord1->rci);

        if (hashMap[hashKey] == nullptr) {
            cerr << "ERROR: transaction does not exists in hash map1" << endl;
            return;
        }

        Transaction *transactionTmp = hashMap[hashKey];
        if (transactionTmp == transaction) {
            hashMap[hashKey] = transactionTmp->next;
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

        cerr << "ERROR: transaction does not exists in hash map2" << endl;
        return;
    }

    //typeuba uba, typedba dba, typeslt slt, typerci rci, typescn scn, uint64_t opFlags
    Transaction* TransactionMap::getMatchForRollback(RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2) {
        uint64_t hashKey = HASHINGFUNCTION(rollbackRedoLogRecord1->uba, rollbackRedoLogRecord2->slt, rollbackRedoLogRecord2->rci);
        Transaction *transactionTmp = hashMap[hashKey];

        while (transactionTmp != nullptr) {
            if (Transaction::matchesForRollback(transactionTmp->lastRedoLogRecord1, transactionTmp->lastRedoLogRecord2,
                    rollbackRedoLogRecord1, rollbackRedoLogRecord2))
                return transactionTmp;

            transactionTmp = transactionTmp->next;
        }

        return nullptr;
    }

    TransactionMap::TransactionMap(uint64_t maxConcurrentTransactions) :
        elements(0),
        maxConcurrentTransactions(maxConcurrentTransactions) {
        hashMap = new Transaction*[maxConcurrentTransactions * 2];
        if (hashMap == nullptr)
            throw MemoryException("TransactionMap::TransactionMap.1", sizeof(Transaction*) * maxConcurrentTransactions * 2);
        memset(hashMap, 0, sizeof(Transaction*) * maxConcurrentTransactions * 2);
    }

    TransactionMap::~TransactionMap() {
        if (hashMap != nullptr) {
            delete[] hashMap;
            hashMap = nullptr;
        }
    }
}
