/* Header for TransactionMap class
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

#include "types.h"

#ifndef TRANSACTIONMAP_H_
#define TRANSACTIONMAP_H_

namespace OpenLogReplicatorOracle {

	class Transaction;

	class TransactionMap {
	protected:
		uint32_t elements;
		Transaction** hashMap;

	public:
		TransactionMap();
		virtual ~TransactionMap();
		void erase(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci);
		void set(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, Transaction * transaction);
		Transaction* get(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci);
		Transaction* getMatch(typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci);
	};
}

#endif
