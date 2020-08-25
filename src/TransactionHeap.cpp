/* Heap memory structure to hold transactions
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

#include "RuntimeException.h"
#include "OracleAnalyser.h"
#include "Transaction.h"
#include "TransactionHeap.h"

using namespace std;

namespace OpenLogReplicator {

    TransactionHeap::TransactionHeap(OracleAnalyser *oracleAnalyser) :
        oracleAnalyser(oracleAnalyser),
        heaps(0),
        size(0) {

        heapsList[0] = (Transaction **)oracleAnalyser->getMemoryChunk("HEAP", false);
        heaps = 1;
    }

    TransactionHeap::~TransactionHeap() {
        while (heaps > 0)
            oracleAnalyser->freeMemoryChunk("HEAP", (uint8_t*)heapsList[--heaps], false);
    }

    void TransactionHeap::pop(void) {
        pop((uint64_t)1);
    }

    void TransactionHeap::pop(uint64_t pos) {
        if (pos > size) {
            RUNTIME_FAIL("trying to pop element from heap: " << dec << pos << " with size: " << dec << size);
        }

        while ((pos << 1) < size) {
            if (*HEAP_AT(pos << 1) < *HEAP_AT(size)) {
                if ((pos << 1) + 1 < size && *HEAP_AT((pos << 1) + 1) < *HEAP_AT(pos << 1)) {
                    HEAP_AT(pos) = HEAP_AT((pos << 1) + 1);
                    HEAP_AT(pos)->pos = pos;
                    pos = (pos << 1) + 1;
                } else {
                    HEAP_AT(pos) = HEAP_AT(pos << 1);
                    HEAP_AT(pos)->pos = pos;
                    pos = pos << 1;
                }
            } else
            if ((pos << 1) + 1 < size && *HEAP_AT((pos << 1) + 1) < *HEAP_AT(size)) {
                HEAP_AT(pos) = HEAP_AT((pos << 1) + 1);
                HEAP_AT(pos)->pos = pos;
                pos = (pos << 1) + 1;
            } else
                break;
        }

        HEAP_AT(pos) = HEAP_AT(size);
        HEAP_AT(pos)->pos = pos;
        --size;

        if (heaps > 1 && size + HEAP_IN_CHUNK + (HEAP_IN_CHUNK/2) < HEAP_IN_CHUNK * heaps) {
            --heaps;
            oracleAnalyser->freeMemoryChunk("HEAP", (uint8_t*)heapsList[heaps], false);
            heapsList[heaps] = nullptr;
        }
    }

    Transaction *TransactionHeap::top(void) {
        if (size > 0)
            return HEAP_AT(1);
        else
            return nullptr;
    }

    Transaction *TransactionHeap::at(uint64_t pos) {
        if (pos <= size)
            return HEAP_AT(pos);
        else
            return nullptr;
    }

    uint64_t TransactionHeap::add(Transaction *transaction) {
        if (size + 1 == HEAP_IN_CHUNK * heaps) {
            //resize heap
            if (heaps == HEAPS_MAX) {
                RUNTIME_FAIL("reached maximum number of open transactions = " << dec << MAX_TRANSACTIONS_LIMIT);
            }

            heapsList[heaps++] = (Transaction **)oracleAnalyser->getMemoryChunk("HEAP", false);
        }

        uint64_t pos = size + 1;
        ++size;

        while (pos > 1 && *transaction < *HEAP_AT(pos >> 1)) {
            HEAP_AT(pos) = HEAP_AT(pos >> 1);
            HEAP_AT(pos)->pos = pos;
            pos >>= 1;
        }
        HEAP_AT(pos) = transaction;
        HEAP_AT(pos)->pos = pos;
        return pos;
    }

    void TransactionHeap::update(uint64_t pos) {
        if (pos > size) {
            RUNTIME_FAIL("trying to update element from heap: " << dec << pos << " with size: " << dec << size);
        }

        Transaction *transaction = HEAP_AT(pos);
        while (true) {
            if ((pos << 1) < size && *HEAP_AT(pos << 1) < *transaction) {
                if ((pos << 1) + 1 < size && *HEAP_AT((pos << 1) + 1) < *HEAP_AT(pos << 1)) {
                    HEAP_AT(pos) = HEAP_AT((pos << 1) + 1);
                    HEAP_AT(pos)->pos = pos;
                    pos = (pos << 1) + 1;
                } else {
                    HEAP_AT(pos) = HEAP_AT(pos << 1);
                    HEAP_AT(pos)->pos = pos;
                    pos <<= 1;
                }
            } else
            if ((pos << 1) + 1 < size && *HEAP_AT((pos << 1) + 1) < *transaction) {
                HEAP_AT(pos) = HEAP_AT((pos << 1) + 1);
                HEAP_AT(pos)->pos = pos;
                pos = (pos << 1) + 1;
            } else
            if (pos > 1 && *transaction < *HEAP_AT(pos >> 1)) {
                HEAP_AT(pos) = HEAP_AT(pos >> 1);
                HEAP_AT(pos)->pos = pos;
                pos >>= 1;
            } else
                break;
        }

        HEAP_AT(pos) = transaction;
        HEAP_AT(pos)->pos = pos;
    }
}
