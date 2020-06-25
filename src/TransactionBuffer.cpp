/* Buffer to handle transactions
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

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "MemoryException.h"
#include "OracleAnalyser.h"
#include "RedoLogRecord.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "TransactionChunk.h"

using namespace std;

namespace OpenLogReplicator {

    TransactionBuffer::TransactionBuffer(uint64_t redoBuffers, uint64_t redoBufferSize) :
        redoBufferSize(redoBufferSize),
        freeBuffers(redoBuffers),
        redoBuffers(redoBuffers) {
        TransactionChunk *tc, *prevTc;

        prevTc = new TransactionChunk(nullptr, redoBufferSize);
        if (prevTc == nullptr)
            throw MemoryException("TransactionBuffer::TransactionBuffer.1", sizeof(TransactionChunk));

        copyTc = new TransactionChunk(nullptr, redoBufferSize);
        if (copyTc == nullptr)
            throw MemoryException("TransactionBuffer::TransactionBuffer.2", sizeof(TransactionChunk));
        unusedTc = prevTc;

        for (uint64_t a = 1; a < this->redoBuffers; ++a) {
            tc = new TransactionChunk(prevTc, redoBufferSize);
            if (tc == nullptr)
                throw MemoryException("TransactionBuffer::TransactionBuffer.3", sizeof(TransactionChunk));

            prevTc = tc;
        }
    }

    TransactionChunk *TransactionBuffer::newTransactionChunk(OracleAnalyser *oracleAnalyser) {
        if (unusedTc == nullptr) {
            oracleAnalyser->dumpTransactions();
            throw MemoryException("TransactionBuffer::newTransactionChunk.1", 0);
        }

        TransactionChunk *tc = unusedTc;
        unusedTc = unusedTc->next;

        if (unusedTc == nullptr) {
            cerr << "ERROR: out of transaction buffer, you can increase the redo-buffer-mb parameter" << endl;
            oracleAnalyser->dumpTransactions();
            throw MemoryException("TransactionBuffer::newTransactionChunk.2", 0);
        }

        unusedTc->prev = nullptr;
        tc->next = nullptr;
        tc->size = 0;
        tc->elements = 0;

        --freeBuffers;

        return tc;
    }

    void TransactionBuffer::deleteTransactionChunk(TransactionChunk* tc) {
        ++freeBuffers;

        tc->prev = nullptr;
        tc->next = unusedTc;
        unusedTc->prev = tc;
        unusedTc = tc;
    }

    void TransactionBuffer::addTransactionChunk(OracleAnalyser *oracleAnalyser, Transaction *transaction, RedoLogRecord *redoLogRecord1,
            RedoLogRecord *redoLogRecord2) {

        if (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            cerr << "ERROR: block size (" << dec << (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL)
                    << ") exceeding redo buffer size (" << redoBufferSize << "), try increasing the redo-buffer-size parameter" << endl;
            oracleAnalyser->dumpTransactions();
            throw MemoryException("TransactionBuffer::addTransactionChunk.1", 0);
        }

        //empty list
        if (transaction->lastTc == nullptr) {
            transaction->lastTc = oracleAnalyser->transactionBuffer->newTransactionChunk(oracleAnalyser);
            transaction->firstTc = transaction->lastTc;
        } else
        if (transaction->lastTc->elements > 0) {
            uint64_t prevSize = *((uint64_t *)(transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
            typescn prevScn = *((typescn *)(transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
            typesubscn prevSubScn = *((typescn *)(transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));

            //last scn/subScn is higher
            if ((prevScn > redoLogRecord1->scn ||
                    ((prevScn == redoLogRecord1->scn && prevSubScn > redoLogRecord1->subScn)))) {
                //locate correct position
                TransactionChunk* tc = transaction->lastTc;
                uint64_t elementsSkipped = 0;
                uint64_t pos = tc->size;

                while (true) {
                    if (pos < prevSize) {
                        cerr << "ERROR: trying move pos " << dec << pos << " back " << prevSize << endl;
                        oracleAnalyser->dumpTransactions();
                        return;
                    }
                    pos -= prevSize;
                    ++elementsSkipped;

                    if (pos == 0) {
                        if (tc->prev == nullptr)
                            break;
                        tc = tc->prev;
                        pos = tc->size;
                        elementsSkipped = 0;
                    }
                    if (elementsSkipped > tc->elements || pos < ROW_HEADER_TOTAL) {
                        cerr << "ERROR: bad data during finding SCN out of order" << endl;
                        oracleAnalyser->dumpTransactions();
                        return;
                    }

                    prevSize = *((uint64_t *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
                    prevScn = *((typescn *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
                    prevSubScn = *((typesubscn *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));

                    if ((prevScn < redoLogRecord1->scn ||
                            ((prevScn == redoLogRecord1->scn && prevSubScn <= redoLogRecord1->subScn))))
                        break;
                }

                if (pos < tc->size) {
                    //does the block need to be divided
                    if (tc->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {

                        TransactionChunk *tmpTc = newTransactionChunk(oracleAnalyser);
                        if (tmpTc == nullptr)
                            throw MemoryException("TransactionBuffer::addTransactionChunk.1", sizeof(TransactionChunk));

                        tmpTc->elements = elementsSkipped;
                        tmpTc->size = tc->size - pos;
                        tmpTc->prev = tc;
                        tmpTc->next = tc->next;
                        memcpy(tmpTc->buffer, tc->buffer + pos, tmpTc->size);

                        if (tc->next != nullptr)
                            tc->next->prev = tmpTc;
                        tc->next = tmpTc;

                        tc->elements -= elementsSkipped;
                        tc->size = pos;

                        if (tc == transaction->lastTc) {
                            transaction->lastTc = tmpTc;
                            transaction->updateLastRecord();
                        }
                    } else {
                        uint64_t oldSize = tc->size - pos;
                        memcpy(copyTc->buffer, tc->buffer + pos, oldSize);
                        tc->size = pos;
                        appendTransactionChunk(tc, redoLogRecord1, redoLogRecord2);
                        memcpy(tc->buffer + tc->size, copyTc->buffer, oldSize);
                        tc->size += oldSize;
                        if (tc == transaction->lastTc)
                            transaction->updateLastRecord();
                        return;
                    }
                }

                //new block needed
                if (tc->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
                    TransactionChunk *tcNew = newTransactionChunk(oracleAnalyser);
                    if (tcNew == nullptr)
                        throw MemoryException("TransactionBuffer::addTransactionChunk.2", sizeof(TransactionChunk));

                    tcNew->prev = tc;
                    tcNew->next = tc->next;
                    tc->next->prev = tcNew;
                    tc->next = tcNew;
                    tc = tcNew;
                }
                appendTransactionChunk(tc, redoLogRecord1, redoLogRecord2);
                if (tc == transaction->lastTc)
                    transaction->updateLastRecord();
                return;
            }
        }

        //new block needed
        if (transaction->lastTc->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            TransactionChunk *tcNew = newTransactionChunk(oracleAnalyser);
            if (tcNew == nullptr)
                throw MemoryException("TransactionBuffer::addTransactionChunk.3", sizeof(TransactionChunk));

            tcNew->prev = transaction->lastTc;
            tcNew->elements = 0;
            tcNew->size = 0;
            transaction->lastTc->next = tcNew;
            transaction->lastTc = tcNew;
        }
        appendTransactionChunk(transaction->lastTc, redoLogRecord1, redoLogRecord2);
        transaction->updateLastRecord();
    }

    void TransactionBuffer::appendTransactionChunk(TransactionChunk* tc, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        //append to the chunk at the end
        *((typeop2 *)(tc->buffer + tc->size + ROW_HEADER_OP)) = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO1, redoLogRecord1, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO2, redoLogRecord2, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA, redoLogRecord1->data, redoLogRecord1->length);
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA + redoLogRecord1->length, redoLogRecord2->data, redoLogRecord2->length);

        *((uint64_t *)(tc->buffer + tc->size + ROW_HEADER_SIZE + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
        *((typesubscn *)(tc->buffer + tc->size + ROW_HEADER_SUBSCN + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->subScn;
        *((typescn *)(tc->buffer + tc->size + ROW_HEADER_SCN + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->scn;

        tc->size += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
        ++tc->elements;
    }

    bool TransactionBuffer::deleteTransactionPart(OracleAnalyser *oracleAnalyser, Transaction *transaction,
            RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2) {
        TransactionChunk *tc = transaction->lastTc;
        if (tc == nullptr || tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer size1: " << dec << transaction->lastTc->size << " elements: " << dec << transaction->lastTc->elements << endl;
            oracleAnalyser->dumpTransactions();
            return false;
        }

        while (tc != nullptr) {
            uint64_t pos = tc->size;
            int64_t left = tc->elements;

            while (pos > 0) {
                if (pos < ROW_HEADER_TOTAL || left <= 0) {
                    cerr << "ERROR: error while deleting transaction part" << endl;
                    oracleAnalyser->dumpTransactions();
                    return false;
                }

                uint64_t lastSize = *((uint64_t *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
                RedoLogRecord *prevRedoLogRecord1 = (RedoLogRecord*)(tc->buffer + pos - lastSize + ROW_HEADER_REDO1);
                RedoLogRecord *prevRedoLogRecord2 = (RedoLogRecord*)(tc->buffer + pos - lastSize + ROW_HEADER_REDO2);
                //found match
                if (Transaction::matchesForRollback(prevRedoLogRecord1, prevRedoLogRecord2, rollbackRedoLogRecord1, rollbackRedoLogRecord2)) {
                    if (pos < tc->size) {
                        memcpy(copyTc->buffer, tc->buffer + pos, tc->size - pos);
                        memcpy(tc->buffer + pos - lastSize, copyTc->buffer, tc->size - pos);
                    }
                    tc->size -= lastSize;

                    --tc->elements;
                    if (tc->elements == 0 && tc->next != nullptr) {
                        tc->next->prev = tc->prev;
                        if (tc->prev != nullptr)
                            tc->prev->next = tc->next;
                        else
                            transaction->firstTc = tc->next;
                        deleteTransactionChunk(tc);
                    }

                    return true;
                }

                pos -= lastSize;
                --left;
            }

            tc = tc->prev;
        }

        return false;
    }

    void TransactionBuffer::rollbackTransactionChunk(OracleAnalyser *oracleAnalyser, Transaction *transaction) {
        if (transaction->lastTc == nullptr)
            return;

        if (transaction->lastTc->size < ROW_HEADER_TOTAL || transaction->lastTc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer size2: " << dec << transaction->lastTc->size << " elements: " <<
                    dec << transaction->lastTc->elements << endl;
            oracleAnalyser->dumpTransactions();
            return;
        }

        uint64_t lastSize = *((uint64_t *)(transaction->lastTc->buffer + transaction->lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        transaction->lastTc->size -= lastSize;
        --transaction->lastTc->elements;

        if (transaction->lastTc->elements == 0) {
            TransactionChunk *tc = transaction->lastTc;
            transaction->lastTc = tc->prev;

            if (transaction->lastTc != nullptr) {
                transaction->lastTc->next = nullptr;
                transaction->updateLastRecord();
            } else {
                transaction->firstTc = nullptr;
                transaction->lastRedoLogRecord1 = nullptr;
                transaction->lastRedoLogRecord2 = nullptr;
            }
            deleteTransactionChunk(tc);
        } else
            transaction->updateLastRecord();
    }

    void TransactionBuffer::deleteTransactionChunks(TransactionChunk* startTc, TransactionChunk* endTc) {
        TransactionChunk* tc = startTc;
        ++freeBuffers;
        while (tc->next != nullptr) {
            ++freeBuffers;
            tc = tc->next;
        }

        endTc->next = unusedTc;
        unusedTc->prev = endTc;
        unusedTc = startTc;
    }

    TransactionBuffer::~TransactionBuffer() {
        if (copyTc != nullptr) {
            delete copyTc;
            copyTc = nullptr;
        }

        while (unusedTc != nullptr) {
            TransactionChunk *tc = unusedTc->next;
            delete unusedTc;
            unusedTc = tc;
        }
    }
}
