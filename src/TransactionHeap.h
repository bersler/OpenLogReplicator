/* Header for TransactionHeap class
   Copyright (C) 2018-2019 Adam Leszczynski.

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

#ifndef TRANSACTIONHEAP_H_
#define TRANSACTIONHEAP_H_

namespace OpenLogReplicatorOracle {

    class Transaction;

    class TransactionHeap {
    public:
        uint32_t heapMaxSize;
        uint32_t heapSize;
        Transaction **heap;

        void initialize(uint32_t heapMaxSize);
        void pop();
        void pop(uint32_t pos);
        Transaction *top();
        int add(Transaction *element);
        void update(uint32_t pos);

        TransactionHeap();
        virtual ~TransactionHeap();
    };
}

#endif
