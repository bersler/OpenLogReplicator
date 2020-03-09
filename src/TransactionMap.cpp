/* Hash map class for transactions
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
#include <iomanip>
#include <string.h>
#include "MemoryException.h"
#include "TransactionMap.h"
#include "Transaction.h"

using namespace std;

namespace OpenLogReplicator {

    void TransactionMap::set(typeuba uba, typedba dba, typeslt slt, typerci rci, Transaction* transaction) {
        if (uba == 0 && dba == 0 && slt == 0 && rci == 0)
            return;
        uint64_t hashKey = HASHINGFUNCTION(uba, slt, rci);

        if (hashMap[hashKey] == nullptr) {
            hashMap[hashKey] = transaction;
        } else {
            Transaction *transactionTemp = hashMap[hashKey];
            while (transactionTemp->next != nullptr)
                transactionTemp = transactionTemp->next;
            transactionTemp->next = transaction;
        }
        ++elements;
    }

    void TransactionMap::erase(typeuba uba, typedba dba, typeslt slt, typerci rci) {
        if (uba == 0 && dba == 0 && slt == 0 && rci == 0)
            return;
        uint64_t hashKey = HASHINGFUNCTION(uba, slt, rci);

        if (hashMap[hashKey] == nullptr) {
            cerr << "ERROR: transaction does not exists in hash map1: UBA: " << PRINTUBA(uba) <<
                    " DBA: 0x" << setfill('0') << setw(8) << hex << dba <<
                    " SLT: " << dec << (uint64_t) slt <<
                    " RCI: " << dec << (uint64_t) rci << endl;
            return;
        }

        Transaction *transactionTemp = hashMap[hashKey];
        if (transactionTemp->lastUba == uba && transactionTemp->lastDba == dba && transactionTemp->lastSlt == slt
                 && transactionTemp->lastRci == rci) {
            hashMap[hashKey] = transactionTemp->next;
            transactionTemp->next = nullptr;
            --elements;
            return;
        }
        Transaction *transactionTempNext = transactionTemp->next;
        while (transactionTempNext != nullptr) {
            if (transactionTempNext->lastUba == uba && transactionTempNext->lastDba == dba
                    && transactionTempNext->lastSlt == slt && transactionTempNext->lastRci == rci) {
                transactionTemp->next = transactionTempNext->next;
                transactionTempNext->next = nullptr;
                --elements;
                return;
            }
            transactionTemp = transactionTempNext;
            transactionTempNext = transactionTemp->next;
        }

        cerr << "ERROR: transaction does not exists in hash map2: UBA: " << PRINTUBA(uba) <<
                " DBA: 0x" << setfill('0') << setw(8) << hex << dba <<
                " SLT: " << dec << (uint64_t) slt <<
                " RCI: " << dec << (uint64_t) rci << endl;
        return;
    }

    Transaction* TransactionMap::getMatch(typeuba uba, typedba dba, typeslt slt, typerci rci) {
        uint64_t hashKey = HASHINGFUNCTION(uba, slt, rci);

        Transaction *transactionTemp = hashMap[hashKey], *transactionTempPartial1, *transactionTempPartial2;
        uint64_t partialMatches1 = 0, partialMatches2 = 0;

        while (transactionTemp != nullptr) {
            if (transactionTemp->lastUba == uba && transactionTemp->lastDba == dba &&
                    transactionTemp->lastSlt == slt && transactionTemp->lastRci == rci)
                return transactionTemp;

            if (transactionTemp->lastUba == uba && (uba != 0 || transactionTemp->lastDba == dba) &&
                    transactionTemp->lastSlt == slt && transactionTemp->lastRci == rci) {
                transactionTempPartial1 = transactionTemp;
                ++partialMatches1;
            }

            if (transactionTemp->lastUba == uba && transactionTemp->lastSlt == slt && transactionTemp->lastRci == rci) {
                transactionTempPartial2 = transactionTemp;
                ++partialMatches2;
            }

            transactionTemp = transactionTemp->next;
        }

        if (partialMatches1 == 1)
            return transactionTempPartial1;

        if (partialMatches2 == 1)
            return transactionTempPartial2;

        return nullptr;
    }

    Transaction* TransactionMap::get(typeuba uba, typedba dba, typeslt slt, typerci rci) {
        uint64_t hashKey = HASHINGFUNCTION(uba, slt, rci);

        Transaction *transactionTemp = hashMap[hashKey];

        while (transactionTemp != nullptr) {
            if (transactionTemp->lastUba == uba && transactionTemp->lastDba == dba &&
                    transactionTemp->lastSlt == slt && transactionTemp->lastRci == rci)
                return transactionTemp;

            transactionTemp = transactionTemp->next;
        }

        return nullptr;
    }

    TransactionMap::TransactionMap(uint64_t maxConcurrentTransactions) :
        elements(0),
        maxConcurrentTransactions(maxConcurrentTransactions) {
        hashMap = new Transaction*[maxConcurrentTransactions * 2];
        if (hashMap == nullptr) {
            cerr << "ERROR: unable allocate memory for Transaction map(" << dec << maxConcurrentTransactions << ")" << endl;
            throw MemoryException("out of memory");
        }
        memset(hashMap, 0, (sizeof(Transaction*)) * (maxConcurrentTransactions * 2));
    }

    TransactionMap::~TransactionMap() {
        if (hashMap != nullptr) {
            delete[] hashMap;
            hashMap = nullptr;
        }
    }

}
