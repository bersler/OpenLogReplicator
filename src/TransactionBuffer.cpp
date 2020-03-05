/* Buffer to handle transactions
   Copyright (C) 2018-2020 Adam Leszczynski.

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
#include <cstdlib>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "TransactionBuffer.h"
#include "MemoryException.h"
#include "TransactionChunk.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    TransactionBuffer::TransactionBuffer(uint32_t redoBuffers, uint32_t redoBufferSize) :
        usedBuffers(1),
        redoBuffers(redoBuffers),
        redoBufferSize(redoBufferSize) {
        TransactionChunk *tc, *prevTc;

        prevTc = new TransactionChunk(nullptr, redoBufferSize);
        unused = prevTc;

        for (uint32_t a = 1; a < this->redoBuffers; ++a) {
            tc = new TransactionChunk(prevTc, redoBufferSize);
            prevTc = tc;
            ++usedBuffers;
        }
    }

    TransactionChunk *TransactionBuffer::newTransactionChunk() {
        if (unused == nullptr) {
            cerr << "ERROR: out of transaction buffer, size1: " << dec << usedBuffers << endl;
            throw MemoryException("out of memory");
        }

        TransactionChunk *tc = unused;
        unused = unused->next;

        if (unused == nullptr) {
            cerr << "ERROR: out of transaction buffer, size2: " << dec << usedBuffers << endl;
            throw MemoryException("out of memory");
        }

        unused->prev = nullptr;
        tc->next = nullptr;
        tc->size = 0;
        tc->elements = 0;

        --usedBuffers;

        return tc;
    }

    void TransactionBuffer::deleteTransactionChunk(TransactionChunk* tc) {
        ++usedBuffers;

        tc->next = unused;
        if (unused != nullptr)
            unused->prev = tc;
        unused = tc;
    }

    TransactionChunk* TransactionBuffer::addTransactionChunk(TransactionChunk* tcLast, uint32_t objn, uint32_t objd,
            typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {

        if (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            cerr << "ERROR: block size (" << dec << (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL)
                    << ") exceeding redo buffer size (" << redoBufferSize << ")" << endl;
            throw MemoryException("too big chunk size");
        }

        if (tcLast->size >= ROW_HEADER_TOTAL) {
            //last scn/subScn is higher
            uint32_t prevSize;
            typescn prevScn = *((typescn *)(tcLast->buffer + tcLast->size - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
            uint16_t prevSubScn = *((typescn *)(tcLast->buffer + tcLast->size - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));

            if ((prevScn > redoLogRecord1->scn || ((prevScn == redoLogRecord1->scn && prevSubScn > redoLogRecord1->subScn)))) {
                //locate correct position
                TransactionChunk* tcTemp = tcLast;
                uint32_t elementsSkipped = 0;
                uint32_t pos = tcTemp->size;

                while (true) {
                    if (pos == 0) {
                        if (tcTemp->prev == nullptr)
                            break;
                        tcTemp = tcTemp->prev;
                        pos = tcTemp->size;
                        elementsSkipped = 0;
                    }
                    if (elementsSkipped > tcTemp->elements || pos < ROW_HEADER_TOTAL) {
                        cerr << "ERROR: bad data during finding scn out of order" << endl;
                        return tcLast;
                    }
                    prevScn = *((typescn *)(tcTemp->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
                    prevSubScn = *((typescn *)(tcTemp->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));
                    prevSize = *((uint32_t *)(tcTemp->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));

                    if ((prevScn < redoLogRecord1->scn || ((prevScn == redoLogRecord1->scn && prevSubScn <= redoLogRecord1->subScn))))
                        break;
                    if (pos < prevSize) {
                        cerr << "ERROR: trying move pos " << dec << pos << " back " << prevSize << endl;
                        return tcLast;
                    }
                    pos -= prevSize;
                    ++elementsSkipped;
                }

                //does the block need to be divided
                if (pos < tcTemp->size) {
                    TransactionChunk *tcNew = newTransactionChunk();
                    tcNew->elements = elementsSkipped;
                    tcNew->size = tcTemp->size - pos;
                    tcNew->prev = tcTemp;
                    memcpy(tcNew->buffer, tcTemp->buffer + pos, tcNew->size);

                    if (tcTemp->next != nullptr) {
                        tcNew->next = tcTemp->next;
                        tcTemp->next->prev = tcNew;
                    }
                    tcTemp->next = tcNew;

                    tcTemp->elements -= elementsSkipped;
                    tcTemp->size = pos;

                    if (tcTemp == tcLast)
                        tcLast = tcNew;
                }

                //new block needed
                if (tcTemp->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
                    TransactionChunk *tcNew = newTransactionChunk();
                    tcNew->prev = tcTemp;
                    tcNew->next = tcTemp->next;
                    tcTemp->next->prev = tcNew;
                    tcTemp->next = tcNew;
                    tcTemp = tcNew;
                }
                appendTransactionChunk(tcTemp, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
                return tcLast;
            }
        }

        //new block needed
        if (tcLast->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            TransactionChunk *tcNew = newTransactionChunk();
            tcNew->prev = tcLast;
            tcNew->elements = 0;
            tcNew->size = 0;
            tcLast->next = tcNew;
            tcLast = tcNew;
        }
        appendTransactionChunk(tcLast, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);

        return tcLast;
    }

    void TransactionBuffer::appendTransactionChunk(TransactionChunk* tc, uint32_t objn, uint32_t objd, typeuba uba,
            uint32_t dba, uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        //append to the chunk at the end
        *((uint32_t *)(tc->buffer + tc->size + ROW_HEADER_OP)) = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO1, redoLogRecord1, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO2, redoLogRecord2, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA, redoLogRecord1->data, redoLogRecord1->length);
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA + redoLogRecord1->length, redoLogRecord2->data, redoLogRecord2->length);

        *((uint32_t *)(tc->buffer + tc->size + ROW_HEADER_OBJN + redoLogRecord1->length + redoLogRecord2->length)) = objn;
        *((uint32_t *)(tc->buffer + tc->size + ROW_HEADER_OBJD + redoLogRecord1->length + redoLogRecord2->length)) = objd;
        *((uint32_t *)(tc->buffer + tc->size + ROW_HEADER_SIZE + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
        *((uint8_t *)(tc->buffer + tc->size + ROW_HEADER_SLT + redoLogRecord1->length + redoLogRecord2->length)) = slt;
        *((uint8_t *)(tc->buffer + tc->size + ROW_HEADER_RCI + redoLogRecord1->length + redoLogRecord2->length)) = rci;
        *((uint16_t *)(tc->buffer + tc->size + ROW_HEADER_SUBSCN + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->subScn;
        *((uint32_t *)(tc->buffer + tc->size + ROW_HEADER_DBA + redoLogRecord1->length + redoLogRecord2->length)) = dba;
        *((typeuba *)(tc->buffer + tc->size + ROW_HEADER_UBA + redoLogRecord1->length + redoLogRecord2->length)) = uba;
        *((typescn *)(tc->buffer + tc->size + ROW_HEADER_SCN + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->scn;

        tc->size += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
        ++tc->elements;
    }

    bool TransactionBuffer::deleteTransactionPart(TransactionChunk* tc, typeuba &uba, uint32_t &dba, uint8_t &slt, uint8_t &rci) {
        cerr << "ERROR: part transaction delete: not yet implemented" << endl;
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer" << endl;
            return false;
        }

        //while (tc != nullptr) {
        //    uint32_t lastSize = *((uint32_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
            //...
        //    tc = tc->prev;
        //}

        return false;
    }

    bool TransactionBuffer::getLastRecord(TransactionChunk* tc, uint32_t &opCode, RedoLogRecord* &redoLogRecord1, RedoLogRecord* &redoLogRecord2) {
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            return false;
        }
        uint32_t lastSize = *((uint32_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        uint8_t *buffer = tc->buffer + tc->size - lastSize;

        opCode = *((uint32_t *)(buffer));
        redoLogRecord1 = (RedoLogRecord*)(buffer + ROW_HEADER_REDO1);
        redoLogRecord1->data = buffer + ROW_HEADER_DATA;
        redoLogRecord2 = (RedoLogRecord*)(buffer + ROW_HEADER_REDO2);
        redoLogRecord2->data = buffer + ROW_HEADER_DATA + redoLogRecord1->length;

        return true;
    }

    TransactionChunk* TransactionBuffer::rollbackTransactionChunk(TransactionChunk* tc, typeuba &lastUba, uint32_t &lastDba,
            uint8_t &lastSlt, uint8_t &lastRci) {
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer" << endl;
            return tc;
        }
        uint32_t lastSize = *((uint32_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        tc->size -= lastSize;
        --tc->elements;

        if (tc->elements == 0 && tc->prev != nullptr) {
            TransactionChunk *prevTc = tc->prev;
            prevTc->next = nullptr;
            deleteTransactionChunk(tc);
            tc = prevTc;
        }

        if (tc->elements == 0) {
            lastUba = 0;
            lastDba = 0;
            lastSlt = 0;
            lastRci = 0;
            return tc;
        }

        if (tc->size < ROW_HEADER_TOTAL) {
            cerr << "ERROR: can't set last UBA size: " << dec << tc->size << ", elements: " << tc->elements << endl;
            return tc;
        }
        lastUba = *((typeuba *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_UBA));
        lastDba = *((uint32_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_DBA));
        lastSlt = *((uint8_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SLT));
        lastRci = *((uint8_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_RCI));

        return tc;
    }

    void TransactionBuffer::deleteTransactionChunks(TransactionChunk* tc, TransactionChunk* tcLast) {
        uint32_t num = 1;
        TransactionChunk* tcTemp = tc;
        while (tcTemp->next != nullptr) {
            ++num;
            tcTemp = tcTemp->next;
        }
        usedBuffers += num;

        tcLast->next = unused;
        if (unused != nullptr)
            unused->prev = tcLast;
        unused = tc;
    }

    TransactionBuffer::~TransactionBuffer() {
        while (unused != nullptr) {
            TransactionChunk *nextTc = unused->next;
            delete unused;
            unused = nextTc;
        }
    }
}
