/* Hash map class for transactions
   Copyright (C) 2018 Adam Leszczynski.

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
#include "TransactionMap.h"
#include "Transaction.h"

#define HASINGFUNCTION(uba,dba,slt) ((uba>>32)^(uba&0xFFFFFFFF)^dba^slt)%(MAX_CONCURRENT_TRANSACTIONS*2-1)

using namespace std;

namespace OpenLogReplicatorOracle {

	void TransactionMap::set(typeuba uba, uint32_t dba, uint8_t slt, Transaction* transaction) {
		uint32_t hashKey = HASINGFUNCTION(uba,dba,slt);

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

	void TransactionMap::erase(typeuba uba, uint32_t dba, uint8_t slt) {
		uint32_t hashKey = HASINGFUNCTION(uba,dba,slt);

		if (hashMap[hashKey] == nullptr) {
			cerr << "ERROR: transaction does not exists in hash map: UBA: " << PRINTUBA(uba) <<
					" DBA: " << setfill('0') << setw(8) << hex << dba <<
					" SLT: " << setfill('0') << setw(2) << hex << slt << endl;
			return;
		}

		Transaction *transactionTemp = hashMap[hashKey];
		if (transactionTemp->lastUba == uba && transactionTemp->lastDba == dba && transactionTemp->lastSlt == slt) {
			hashMap[hashKey] = transactionTemp->next;
			transactionTemp->next = nullptr;
			--elements;
			return;
		}
		Transaction *transactionTempNext = transactionTemp->next;
		while (transactionTempNext != nullptr) {
			if (transactionTempNext->lastUba == uba && transactionTempNext->lastDba == dba && transactionTempNext->lastSlt == slt) {
				transactionTemp->next = transactionTempNext->next;
				transactionTempNext->next = nullptr;
				--elements;
				return;
			}
			transactionTemp = transactionTempNext;
			transactionTempNext = transactionTemp->next;
		}

		cerr << "ERROR: transaction does not exists in hash map: UBA: " << PRINTUBA(uba) <<
				" DBA: " << setfill('0') << setw(8) << hex << dba <<
				" SLT: " << setfill('0') << setw(2) << hex << slt << endl;
		return;
	}

	Transaction* TransactionMap::get(typeuba uba, uint32_t dba, uint8_t slt) {
		uint32_t hashKey = HASINGFUNCTION(uba,dba,slt);

		Transaction *transactionTemp = hashMap[hashKey];
		while (transactionTemp != nullptr) {
			if (transactionTemp->lastUba == uba && transactionTemp->lastDba == dba && transactionTemp->lastSlt == slt)
				return transactionTemp;
			transactionTemp = transactionTemp->next;
		}
		return nullptr;
	}

	TransactionMap::TransactionMap() :
		elements(0) {
		hashMap = new Transaction*[MAX_CONCURRENT_TRANSACTIONS * 2];
		memset(hashMap, 0, (sizeof(Transaction*)) * (MAX_CONCURRENT_TRANSACTIONS * 2));
	}

	TransactionMap::~TransactionMap() {
		if (hashMap != nullptr) {
			delete[] hashMap;
			hashMap = nullptr;
		}
	}

}
