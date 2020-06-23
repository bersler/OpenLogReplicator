/* Header for TransactionMap class
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
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include "types.h"

#ifndef TRANSACTIONMAP_H_
#define TRANSACTIONMAP_H_

#define HASHINGFUNCTION(uba,slt,rci) (uba^((uint64_t)slt<<9)^((uint64_t)rci<<37))%(maxConcurrentTransactions*2-1)

namespace OpenLogReplicator {

    class Transaction;

    class TransactionMap {
    protected:
        uint64_t elements;
        Transaction** hashMap;
        uint64_t maxConcurrentTransactions;

    public:
        TransactionMap(uint64_t maxConcurrentTransactions);
        virtual ~TransactionMap();
        void erase(Transaction * transaction);
        void set(Transaction * transaction);
        Transaction* getMatch(typeuba uba, typedba dba, typeslt slt, typerci rci, uint64_t opFlags);
    };
}

#endif
