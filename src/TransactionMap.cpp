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

#define HASHINGFUNCTION(uba,dba,slt,rci) ((uba>>32)^(uba&0xFFFFFFFF)^((uba>0)?0:dba)^(slt<<9)^(rci<<17))%(MAX_CONCURRENT_TRANSACTIONS*2-1)

using namespace std;

namespace OpenLogReplicatorOracle {

	void TransactionMap::set(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, Transaction* transaction) {
		uint32_t hashKey = HASHINGFUNCTION(uba, dba, slt, rci);

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

	void TransactionMap::erase(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci) {
		uint32_t hashKey = HASHINGFUNCTION(uba, dba, slt, rci);

		if (hashMap[hashKey] == nullptr) {
			cerr << "ERROR: transaction does not exists in hash map1: UBA: " << PRINTUBA(uba) <<
					" DBA: " << setfill('0') << setw(8) << hex << dba <<
					" SLT: " << dec << (uint32_t) slt <<
					" RCI: " << dec << (uint32_t) rci << endl;
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
			if (transactionTempNext->lastUba == uba && (uba != 0 || transactionTempNext->lastDba == dba)
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
				" DBA: " << setfill('0') << setw(8) << hex << dba <<
				" SLT: " << dec << (uint32_t) slt <<
				" RCI: " << dec << (uint32_t) rci << endl;
		return;
	}

	Transaction* TransactionMap::get(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci) {
		uint32_t hashKey = HASHINGFUNCTION(uba, dba, slt, rci);

		Transaction *transactionTemp = hashMap[hashKey], *transactionTempPartial;
		uint32_t partialMatches = 0;

		while (transactionTemp != nullptr) {
			if (transactionTemp->lastUba == uba && transactionTemp->lastDba == dba &&
					transactionTemp->lastSlt == slt && transactionTemp->lastRci == rci)
				return transactionTemp;

			if (transactionTemp->lastUba == uba && (uba != 0 || transactionTemp->lastDba == dba) &&
					transactionTemp->lastSlt == slt && transactionTemp->lastRci == rci) {
				transactionTempPartial = transactionTemp;
				++partialMatches;
			}

			transactionTemp = transactionTemp->next;
		}

		if (uba != 0 && partialMatches == 1)
			return transactionTempPartial;

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
