/* Heap memory structure to hold transactions
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

#include <iostream>
#include <stdint.h>
#include "TransactionHeap.h"
#include "MemoryException.h"
#include "Transaction.h"

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

    void TransactionHeap::initialize(uint32_t heapMaxSize) {
        this->heapMaxSize = heapMaxSize;
        this->heapSize = 0;
        heap = new Transaction*[heapMaxSize];
    }

    void TransactionHeap::pop() {
        pop(1);
    }

    void TransactionHeap::pop(uint32_t pos) {
        if (pos > heapSize)
            throw MemoryException("heap inconsistency: pop of non existent transaction from heap");

        while ((pos << 1) < heapSize) {
            if (*heap[pos << 1] < *heap[heapSize]) {
                if ((pos << 1) + 1 < heapSize && *heap[(pos << 1) + 1] < *heap[pos << 1]) {
                    heap[pos] = heap[(pos << 1) + 1];
                    heap[pos]->pos = pos;
                    pos = (pos << 1) + 1;
                } else {
                    heap[pos] = heap[pos << 1];
                    heap[pos]->pos = pos;
                    pos = pos << 1;
                }
            } else
            if ((pos << 1) + 1 < heapSize && *heap[(pos << 1) + 1] < *heap[heapSize]) {
                heap[pos] = heap[(pos << 1) + 1];
                heap[pos]->pos = pos;
                pos = (pos << 1) + 1;
            } else
                break;
        }

        heap[pos] = heap[heapSize];
        heap[pos]->pos = pos;
        --heapSize;
    }

    Transaction *TransactionHeap::top() {
        if (heapSize > 0)
            return heap[1];
        else
            return nullptr;
    }

    int TransactionHeap::add(Transaction *transaction) {
        if (heapSize == heapMaxSize) {
            cout << "Transaction heap content:" << endl;
            for (uint32_t i = 1; i <= heapSize; ++i) {
                cout << "[" << dec << i << "]: " << *heap[i] << endl;
            }
            throw MemoryException("out of memory: maximum heap size reached");
        }

        uint32_t pos = heapSize + 1;
        ++heapSize;

        while (pos > 1 && *transaction < *heap[pos >> 1]) {
            heap[pos] = heap[pos >> 1];
            heap[pos]->pos = pos;
            pos >>= 1;
        }
        heap[pos] = transaction;
        heap[pos]->pos = pos;
        return pos;
    }

    void TransactionHeap::update(uint32_t pos) {
        if (pos > heapSize)
            throw MemoryException("heap inconsistency: update of non existent transaction from heap");

        Transaction *transaction = heap[pos];
        while (true) {
            if ((pos << 1) < heapSize && *heap[pos << 1] < *transaction) {
                if ((pos << 1) + 1 < heapSize && *heap[(pos << 1) + 1] < *heap[pos << 1]) {
                    heap[pos] = heap[(pos << 1) + 1];
                    heap[pos]->pos = pos;
                    pos = (pos << 1) + 1;
                } else {
                    heap[pos] = heap[pos << 1];
                    heap[pos]->pos = pos;
                    pos <<= 1;
                }
            } else
            if ((pos << 1) + 1 < heapSize && *heap[(pos << 1) + 1] < *transaction) {
                heap[pos] = heap[(pos << 1) + 1];
                heap[pos]->pos = pos;
                pos = (pos << 1) + 1;
            } else
            if (pos > 1 && *transaction < *heap[pos >> 1]) {
                heap[pos] = heap[pos >> 1];
                heap[pos]->pos = pos;
                pos >>= 1;
            } else
                break;
        }

        heap[pos] = transaction;
        heap[pos]->pos = pos;
    }

    TransactionHeap::TransactionHeap() :
        heapMaxSize(0),
        heapSize(0),
        heap(nullptr) {
    }

    TransactionHeap::~TransactionHeap() {
        if (heap != nullptr) {
            delete[] heap;
            heap = nullptr;
        }
    }
}
