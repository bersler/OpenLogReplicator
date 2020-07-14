/* Header for TransactionHeap class
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

#include "types.h"

#ifndef TRANSACTIONHEAP_H_
#define TRANSACTIONHEAP_H_

#define HEAPS_MAX (MAX_TRANSACTIONS_LIMIT*sizeof(Transaction*)/(MEMORY_CHUNK_SIZE_MB*1024*1024))
#define HEAP_IN_CHUNK (1024*1024/sizeof(Transaction*))
#define HEAP_AT(a) heapsList[(a)/HEAP_IN_CHUNK][(a)%HEAP_IN_CHUNK]

namespace OpenLogReplicator {

    class OracleAnalyser;
    class Transaction;

    class TransactionHeap {
    protected:
        OracleAnalyser *oracleAnalyser;
        uint64_t heaps;
        Transaction **heapsList[HEAPS_MAX];

    public:
        uint64_t size;

        TransactionHeap(OracleAnalyser *oracleAnalyser);
        virtual ~TransactionHeap();

        void pop(void);
        void pop(uint64_t pos);
        Transaction *top(void);
        Transaction *at(uint64_t pos);
        uint64_t add(Transaction *element);
        void update(uint64_t pos);
    };
}

#endif
