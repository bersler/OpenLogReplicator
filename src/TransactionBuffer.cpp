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

    bool TransactionBuffer::addTransactionChunk(OracleAnalyser *oracleAnalyser, TransactionChunk* &lastTc, typeobj objn, typeobj objd,
            typeuba uba, typedba dba, typeslt slt, typerci rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {

        if (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            cerr << "ERROR: block size (" << dec << (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL)
                    << ") exceeding redo buffer size (" << redoBufferSize << "), try increasing the redo-buffer-size parameter" << endl;
            oracleAnalyser->dumpTransactions();
            throw MemoryException("TransactionBuffer::addTransactionChunk.1", 0);
        }

        if (lastTc->elements > 0) {
            //last scn/subScn is higher
            uint64_t prevSize = *((uint64_t *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
            typescn prevScn = *((typescn *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
            typesubscn prevSubScn = *((typescn *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));

            if ((prevScn > redoLogRecord1->scn ||
                    ((prevScn == redoLogRecord1->scn && prevSubScn > redoLogRecord1->subScn)))) {
                //locate correct position
                TransactionChunk* tc = lastTc;
                uint64_t elementsSkipped = 0;
                uint64_t pos = tc->size;

                while (true) {
                    if (pos < prevSize) {
                        cerr << "ERROR: trying move pos " << dec << pos << " back " << prevSize << endl;
                        oracleAnalyser->dumpTransactions();
                        return false;
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
                        return false;
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

                        if (tc == lastTc) {
                            lastTc = tmpTc;
                        }
                    } else {
                        uint64_t oldSize = tc->size - pos;
                        memcpy(copyTc->buffer, tc->buffer + pos, oldSize);
                        tc->size = pos;
                        appendTransactionChunk(tc, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
                        memcpy(tc->buffer + tc->size, copyTc->buffer, oldSize);
                        tc->size += oldSize;
                        return false;
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
                appendTransactionChunk(tc, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);

                return false;
            }
        }

        //new block needed
        if (lastTc->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            TransactionChunk *tcNew = newTransactionChunk(oracleAnalyser);
            if (tcNew == nullptr)
                throw MemoryException("TransactionBuffer::addTransactionChunk.3", sizeof(TransactionChunk));

            tcNew->prev = lastTc;
            tcNew->elements = 0;
            tcNew->size = 0;
            lastTc->next = tcNew;
            lastTc = tcNew;
        }
        appendTransactionChunk(lastTc, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
        return true;
    }

    void TransactionBuffer::appendTransactionChunk(TransactionChunk* tc, typeobj objn, typeobj objd, typeuba uba,
            typedba dba, typeslt slt, typerci rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        //append to the chunk at the end
        *((typeop2 *)(tc->buffer + tc->size + ROW_HEADER_OP)) = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO1, redoLogRecord1, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_REDO2, redoLogRecord2, sizeof(struct RedoLogRecord));
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA, redoLogRecord1->data, redoLogRecord1->length);
        memcpy(tc->buffer + tc->size + ROW_HEADER_DATA + redoLogRecord1->length, redoLogRecord2->data, redoLogRecord2->length);

        *((typeobj *)(tc->buffer + tc->size + ROW_HEADER_OBJN + redoLogRecord1->length + redoLogRecord2->length)) = objn;
        *((typeobj *)(tc->buffer + tc->size + ROW_HEADER_OBJD + redoLogRecord1->length + redoLogRecord2->length)) = objd;
        *((uint64_t *)(tc->buffer + tc->size + ROW_HEADER_SIZE + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
        *((typeslt *)(tc->buffer + tc->size + ROW_HEADER_SLT + redoLogRecord1->length + redoLogRecord2->length)) = slt;
        *((typerci *)(tc->buffer + tc->size + ROW_HEADER_RCI + redoLogRecord1->length + redoLogRecord2->length)) = rci;
        *((typesubscn *)(tc->buffer + tc->size + ROW_HEADER_SUBSCN + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->subScn;
        *((typedba *)(tc->buffer + tc->size + ROW_HEADER_DBA + redoLogRecord1->length + redoLogRecord2->length)) = dba;
        *((typeuba *)(tc->buffer + tc->size + ROW_HEADER_UBA + redoLogRecord1->length + redoLogRecord2->length)) = uba;
        *((typescn *)(tc->buffer + tc->size + ROW_HEADER_SCN + redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->scn;

        tc->size += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;
        ++tc->elements;
    }

    bool TransactionBuffer::deleteTransactionPart(OracleAnalyser *oracleAnalyser, TransactionChunk* &firstTc, TransactionChunk* &lastTc, typeuba &uba, typedba &dba, typeslt &slt, typerci &rci,
            uint64_t opFlags) {
        TransactionChunk *tc = lastTc;
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer size1: " << dec << lastTc->size << " elements: " << dec << lastTc->elements << endl;
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
                typeuba prevUba = *((typeuba *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_UBA));
                typedba prevDba = *((typedba *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_DBA));
                typeslt prevSlt = *((typeslt *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SLT));
                typerci prevRci = *((typerci *)(tc->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_RCI));

                //found match
                if (prevSlt == slt && prevRci == rci && prevUba == uba &&
                        ((opFlags & OPFLAG_BEGIN_TRANS) != 0 || prevDba == dba)) {

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
                            firstTc = tc->next;
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

    bool TransactionBuffer::getLastRecord(TransactionChunk* lastTc, typeop2 &opCode, RedoLogRecord* &redoLogRecord1, RedoLogRecord* &redoLogRecord2) {
        if (lastTc->size < ROW_HEADER_TOTAL || lastTc->elements == 0) {
            return false;
        }
        uint64_t lastSize = *((uint64_t *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        uint8_t *buffer = lastTc->buffer + lastTc->size - lastSize;

        opCode = *((typeop2 *)(buffer + ROW_HEADER_OP));
        redoLogRecord1 = (RedoLogRecord*)(buffer + ROW_HEADER_REDO1);
        redoLogRecord1->data = buffer + ROW_HEADER_DATA;
        redoLogRecord2 = (RedoLogRecord*)(buffer + ROW_HEADER_REDO2);
        redoLogRecord2->data = buffer + ROW_HEADER_DATA + redoLogRecord1->length;

        return true;
    }

    void TransactionBuffer::rollbackTransactionChunk(OracleAnalyser *oracleAnalyser, TransactionChunk* &lastTc, typeuba &lastUba, typedba &lastDba,
            typeslt &lastSlt, typerci &lastRci) {
        if (lastTc->size < ROW_HEADER_TOTAL || lastTc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer size2: " << dec << lastTc->size << " elements: " << dec << lastTc->elements << endl;
            oracleAnalyser->dumpTransactions();
            return;
        }

        uint64_t lastSize = *((uint64_t *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        lastTc->size -= lastSize;
        --lastTc->elements;

        if (lastTc->elements == 0 && lastTc->prev != nullptr) {
            TransactionChunk *tc = lastTc;
            lastTc = lastTc->prev;
            lastTc->next = nullptr;
            deleteTransactionChunk(tc);
        }

        if (lastTc->elements == 0) {
            lastUba = 0;
            lastDba = 0;
            lastSlt = 0;
            lastRci = 0;
            return;
        }

        if (lastTc->size < ROW_HEADER_TOTAL) {
            cerr << "ERROR: can't set last UBA size: " << dec << lastTc->size << ", elements: " << lastTc->elements << endl;
            oracleAnalyser->dumpTransactions();
            return;
        }
        lastUba = *((typeuba *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_UBA));
        lastDba = *((typedba *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_DBA));
        lastSlt = *((typeslt *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SLT));
        lastRci = *((typerci *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_RCI));
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
