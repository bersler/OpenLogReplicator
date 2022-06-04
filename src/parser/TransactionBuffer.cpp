/* Buffer to handle transactions
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OpCode0501.h"
#include "OpCode050B.h"
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

        if (redoLogRecord->flg & (FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID) == 0) {
            RUNTIME_FAIL("split undo error flag: " << std::dec << redoLogRecord->flg << " offset: " << std::dec << redoLogRecord->dataOffset);
        }

        if (transaction->lastSplit) {
            if ((redoLogRecord->opCode) != 0x0501) {
                RUNTIME_FAIL("split undo no 5.1 offset: " << std::dec << redoLogRecord->dataOffset);
            }

            uint64_t lengthLast = *((uint64_t*) (transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
            RedoLogRecord* last501 = (RedoLogRecord*) (transaction->lastTc->buffer + transaction->lastTc->size - lengthLast + ROW_HEADER_REDO1);
            last501->data = transaction->lastTc->buffer + transaction->lastTc->size - lengthLast + ROW_HEADER_DATA;

            uint64_t size = last501->length + redoLogRecord->length;
            uint8_t* merge = new uint8_t[size];
            if (merge == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << size << " bytes memory (for: merge split undo #1) offset: " << std::dec << redoLogRecord->dataOffset);
            }
            transaction->merges.push_back(merge);
            mergeBlocks(merge, redoLogRecord, last501);
            rollbackTransactionChunk(transaction);
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

        if (transaction->lastSplit) {
            for (uint8_t* buf : transaction->merges)
                delete[] buf;
            transaction->merges.clear();
        } else
            transaction->lastSplit = true;
    }

    void TransactionBuffer::addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        uint64_t length = redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;

        if (length > DATA_BUFFER_SIZE) {
            RUNTIME_FAIL(*oracleAnalyzer <<  "block size (" << std::dec << length
                    << ") exceeding max block size (" << FULL_BUFFER_SIZE << "), try increasing the FULL_BUFFER_SIZE parameter");
        }

        if (transaction->lastSplit) {
            if ((redoLogRecord1->opCode) != 0x0501) {
                RUNTIME_FAIL("split undo HEAD no 5.1 offset: " << std::dec << redoLogRecord1->dataOffset);
            }

            if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOHEAD) != 0) {
                uint64_t lengthLast = *((uint64_t*) (transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
                RedoLogRecord* last501 = (RedoLogRecord*) (transaction->lastTc->buffer + transaction->lastTc->size - lengthLast + ROW_HEADER_REDO1);
                last501->data = transaction->lastTc->buffer + transaction->lastTc->size - lengthLast + ROW_HEADER_DATA;

                uint64_t size = last501->length + redoLogRecord1->length;
                uint8_t* merge = new uint8_t[size];
                if (merge == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << size << " bytes memory (for: merge split undo #1) offset: " << std::dec << redoLogRecord1->dataOffset);
                }
                transaction->merges.push_back(merge);
                mergeBlocks(merge, redoLogRecord1, last501);

                uint16_t fieldPos = redoLogRecord1->fieldPos;
                uint16_t fieldLength = oracleAnalyzer->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 1 * 2);
                fieldPos += (fieldLength + 3) & 0xFFFC;

                uint16_t flg = oracleAnalyzer->read16(redoLogRecord1->data + fieldPos + 20);
                flg &= ~(FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL | FLG_LASTBUFFERSPLIT);
                oracleAnalyzer->write16(redoLogRecord1->data + fieldPos + 20, flg);
                OpCode0501::process(oracleAnalyzer, redoLogRecord1);
                length = redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
            }

            rollbackTransactionChunk(transaction);
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

        if (transaction->lastSplit) {
            transaction->lastSplit = false;
            for (uint8_t* buf : transaction->merges)
                delete[] buf;
            transaction->merges.clear();
        }
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

    void TransactionBuffer::mergeBlocks(uint8_t* buffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        memcpy(buffer, redoLogRecord1->data, redoLogRecord1->fieldLengthsDelta);
        uint64_t pos = redoLogRecord1->fieldLengthsDelta;
        uint16_t fieldCnt;
        uint16_t fieldPos1;
        uint16_t fieldPos2;

        if ((redoLogRecord1->flg & FLG_LASTBUFFERSPLIT) != 0) {
            uint16_t length1 = oracleAnalyzer->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + redoLogRecord1->fieldCnt * 2);
            uint16_t length2 = oracleAnalyzer->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6);
            oracleAnalyzer->write16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6, length1 + length2);
            --redoLogRecord1->fieldCnt;
        }

        //field list
        fieldCnt = redoLogRecord1->fieldCnt + redoLogRecord2->fieldCnt - 2;
        oracleAnalyzer->write16(buffer + pos, fieldCnt);
        memcpy(buffer + pos + 2, redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 2, redoLogRecord1->fieldCnt * 2);
        memcpy(buffer + pos + 2 + redoLogRecord1->fieldCnt * 2, redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6, redoLogRecord2->fieldCnt * 2 - 4);
        pos += (((fieldCnt + 1) * 2) + 2) & (0xFFFC);
        fieldPos1 = pos;

        //data
        memcpy(buffer + pos, redoLogRecord1->data + redoLogRecord1->fieldPos, redoLogRecord1->length - redoLogRecord1->fieldPos);
        pos += (redoLogRecord1->length - redoLogRecord1->fieldPos + 3) & (0xFFFC);
        fieldPos2 = redoLogRecord2->fieldPos +
                ((oracleAnalyzer->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 2) + 3) & 0xFFFC) +
                ((oracleAnalyzer->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 4) + 3) & 0xFFFC);

        memcpy(buffer + pos, redoLogRecord2->data + fieldPos2, redoLogRecord2->length - fieldPos2);
        pos += (redoLogRecord2->length - fieldPos2 + 3) & (0xFFFC);

        redoLogRecord1->length = pos;
        redoLogRecord1->fieldCnt = fieldCnt;
        redoLogRecord1->fieldPos = fieldPos1;
        redoLogRecord1->data = buffer;
    }
}
