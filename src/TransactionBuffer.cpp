/* Buffer to handle transactions
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    TransactionBuffer::TransactionBuffer(OracleAnalyzer* oracleAnalyzer) :
        oracleAnalyzer(oracleAnalyzer) {
    }

    TransactionBuffer::~TransactionBuffer() {
        if (partiallyFullChunks.size() > 0) {
            WARNING("non free blocks in transaction buffer: " << std::dec << partiallyFullChunks.size());
        }
    }

    TransactionChunk* TransactionBuffer::newTransactionChunk(Transaction* transaction) {
        uint8_t* chunk;
        TransactionChunk* tc;
        uint64_t pos;
        uint64_t freeMap;
        if (partiallyFullChunks.size() > 0) {
            chunk = partiallyFullChunks.begin()->first;
            freeMap = partiallyFullChunks.begin()->second;
            pos = ffs(freeMap) - 1;
            freeMap &= ~(1 << pos);
            if (freeMap == 0)
                partiallyFullChunks.erase(chunk);
            else
                partiallyFullChunks[chunk] = freeMap;
        } else {
            chunk = oracleAnalyzer->getMemoryChunk(transaction->name.c_str(), false);
            pos = 0;
            freeMap = BUFFERS_FREE_MASK & (~1);
            partiallyFullChunks[chunk] = freeMap;
        }

        tc = (TransactionChunk*) (chunk + FULL_BUFFER_SIZE * pos);
        memset(tc, 0, HEADER_BUFFER_SIZE);
        tc->header = chunk;
        tc->pos = pos;
        return tc;
    }

    void TransactionBuffer::deleteTransactionChunk(TransactionChunk* tc) {
        uint8_t* chunk = tc->header;
        uint64_t pos = tc->pos;
        uint64_t freeMap = partiallyFullChunks[chunk];

        freeMap |= (1 << pos);

        if (freeMap == BUFFERS_FREE_MASK) {
            oracleAnalyzer->freeMemoryChunk("transaction chunk", chunk, false);
            partiallyFullChunks.erase(chunk);
        } else
            partiallyFullChunks[chunk] = freeMap;
    }

    void TransactionBuffer::deleteTransactionChunks(TransactionChunk* tc) {
        TransactionChunk* nextTc;
        while (tc != nullptr) {
            nextTc = tc->next;
            deleteTransactionChunk(tc);
            tc = nextTc;
        }
    }

    void TransactionBuffer::addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord) {
        uint64_t length = redoLogRecord->length + ROW_HEADER_TOTAL;

        if (length > DATA_BUFFER_SIZE) {
            RUNTIME_FAIL(*oracleAnalyzer <<  "block size (" << std::dec << length
                    << ") exceeding max block size (" << FULL_BUFFER_SIZE << "), try increasing the FULL_BUFFER_SIZE parameter");
        }

        //empty list
        if (transaction->lastTc == nullptr) {
            transaction->lastTc = newTransactionChunk(transaction);
            transaction->firstTc = transaction->lastTc;
        }

        //new block needed
        if (transaction->lastTc->size + length > DATA_BUFFER_SIZE) {
            TransactionChunk* tcNew = newTransactionChunk(transaction);
            tcNew->prev = transaction->lastTc;
            transaction->lastTc->next = tcNew;
            transaction->lastTc = tcNew;
        }

        //append to the chunk at the end
        TransactionChunk* tc = transaction->lastTc;
        *((typeOP2*) (tc->buffer + tc->size + ROW_HEADER_OP)) = (redoLogRecord->opCode << 16);
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO1, redoLogRecord, sizeof(struct RedoLogRecord));
        memset(tc->buffer + tc->size + ROW_HEADER_REDO2, 0, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA, redoLogRecord->data, redoLogRecord->length);

        *((uint64_t*) (tc->buffer + tc->size + ROW_HEADER_SIZE + redoLogRecord->length)) = length;

        tc->size += length;
        ++tc->elements;
        transaction->size += length;
    }

    void TransactionBuffer::addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        uint64_t length = redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;

        if (length > DATA_BUFFER_SIZE) {
            RUNTIME_FAIL(*oracleAnalyzer <<  "block size (" << std::dec << length
                    << ") exceeding max block size (" << FULL_BUFFER_SIZE << "), try increasing the FULL_BUFFER_SIZE parameter");
        }

        //empty list
        if (transaction->lastTc == nullptr) {
            transaction->lastTc = newTransactionChunk(transaction);
            transaction->firstTc = transaction->lastTc;
        }

        //new block needed
        if (transaction->lastTc->size + length > DATA_BUFFER_SIZE) {
            TransactionChunk* tcNew = newTransactionChunk(transaction);
            tcNew->prev = transaction->lastTc;
            transaction->lastTc->next = tcNew;
            transaction->lastTc = tcNew;
        }

        //append to the chunk at the end
        TransactionChunk* tc = transaction->lastTc;
        *((typeOP2*) (tc->buffer + tc->size + ROW_HEADER_OP)) = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO1, redoLogRecord1, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO2, redoLogRecord2, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA, redoLogRecord1->data, redoLogRecord1->length);
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA + redoLogRecord1->length, redoLogRecord2->data, redoLogRecord2->length);

        *((uint64_t*) (tc->buffer + tc->size + ROW_HEADER_SIZE + redoLogRecord1->length + redoLogRecord2->length)) = length;

        tc->size += length;
        ++tc->elements;
        transaction->size += length;
    }

    void TransactionBuffer::rollbackTransactionChunk(Transaction* transaction) {
        if (transaction->lastTc == nullptr)
            return;

        if (transaction->lastTc->size < ROW_HEADER_TOTAL || transaction->lastTc->elements == 0) {
            RUNTIME_FAIL(*oracleAnalyzer << "trying to remove from empty buffer size2: " << std::dec << transaction->lastTc->size << " elements: " <<
                    std::dec << transaction->lastTc->elements);
        }

        uint64_t length = *((uint64_t*) (transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        transaction->lastTc->size -= length;
        --transaction->lastTc->elements;
        transaction->size -= length;

        if (transaction->lastTc->elements == 0) {
            TransactionChunk* tc = transaction->lastTc;
            transaction->lastTc = tc->prev;

            if (transaction->lastTc != nullptr) {
                transaction->lastTc->next = nullptr;
            } else {
                transaction->firstTc = nullptr;
            }
            deleteTransactionChunk(tc);
        }
    }
}
