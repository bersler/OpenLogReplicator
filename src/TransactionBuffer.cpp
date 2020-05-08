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
#include "OracleReader.h"

using namespace std;

namespace OpenLogReplicator {

    TransactionBuffer::TransactionBuffer(uint64_t redoBuffers, uint64_t redoBufferSize) :
        redoBufferSize(redoBufferSize),
        freeBuffers(redoBuffers),
        redoBuffers(redoBuffers) {
        TransactionChunk *tc, *prevTc;

        prevTc = new TransactionChunk(nullptr, redoBufferSize);
        if (prevTc == nullptr) {
            cerr << "ERROR: out of memory for transaction buffer, for size: " << dec << this->redoBuffers << endl;
            throw MemoryException("out of memory");
        }
        unused = prevTc;

        for (uint64_t a = 1; a < this->redoBuffers; ++a) {
            tc = new TransactionChunk(prevTc, redoBufferSize);
            if (tc == nullptr) {
                cerr << "ERROR: out of memory for transaction buffer, for size: " << dec << this->redoBuffers << endl;
                throw MemoryException("out of memory");
            }
            prevTc = tc;
        }
    }

    TransactionChunk *TransactionBuffer::newTransactionChunk(OracleReader *oracleReader) {
        if (unused == nullptr) {
            cerr << "ERROR: out of transaction buffer, size1: " << dec << freeBuffers << endl;
            oracleReader->dumpTransactions();
            throw MemoryException("out of memory");
        }

        TransactionChunk *tc = unused;
        unused = unused->next;

        if (unused == nullptr) {
            cerr << "ERROR: out of transaction buffer, you can increase the redo-buffer-mb parameter" << endl;
            oracleReader->dumpTransactions();
            throw MemoryException("out of memory");
        }

        unused->prev = nullptr;
        tc->next = nullptr;
        tc->size = 0;
        tc->elements = 0;

        --freeBuffers;

        return tc;
    }

    void TransactionBuffer::deleteTransactionChunk(TransactionChunk* tc) {
        ++freeBuffers;

        tc->prev = nullptr;
        tc->next = unused;
        unused->prev = tc;
        unused = tc;
    }

    TransactionChunk* TransactionBuffer::addTransactionChunk(OracleReader *oracleReader, TransactionChunk* tcLast, typeobj objn, typeobj objd,
            typeuba uba, typedba dba, typeslt slt, typerci rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {

        if (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL > redoBufferSize) {
            cerr << "ERROR: block size (" << dec << (redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL)
                    << ") exceeding redo buffer size (" << redoBufferSize << "), try increasing the redo-buffer-size parameter" << endl;
            oracleReader->dumpTransactions();
            throw MemoryException("too big chunk size");
        }

        if (tcLast->size >= ROW_HEADER_TOTAL) {
            //last scn/subScn is higher
            uint64_t prevSize;
            typescn prevScn = *((typescn *)(tcLast->buffer + tcLast->size - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
            typesubscn prevSubScn = *((typescn *)(tcLast->buffer + tcLast->size - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));

            if ((prevScn > redoLogRecord1->scn || ((prevScn == redoLogRecord1->scn && prevSubScn > redoLogRecord1->subScn)))) {
                //locate correct position
                TransactionChunk* tcTemp = tcLast;
                uint64_t elementsSkipped = 0;
                uint64_t pos = tcTemp->size;

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
                        oracleReader->dumpTransactions();
                        return tcLast;
                    }
                    prevScn = *((typescn *)(tcTemp->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SCN));
                    prevSubScn = *((typesubscn *)(tcTemp->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SUBSCN));
                    prevSize = *((uint64_t *)(tcTemp->buffer + pos - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));

                    if ((prevScn < redoLogRecord1->scn || ((prevScn == redoLogRecord1->scn && prevSubScn <= redoLogRecord1->subScn))))
                        break;
                    if (pos < prevSize) {
                        cerr << "ERROR: trying move pos " << dec << pos << " back " << prevSize << endl;
                        oracleReader->dumpTransactions();
                        return tcLast;
                    }
                    pos -= prevSize;
                    ++elementsSkipped;
                }

                //does the block need to be divided
                if (pos < tcTemp->size) {
                    TransactionChunk *tcNew = newTransactionChunk(oracleReader);
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
                    TransactionChunk *tcNew = newTransactionChunk(oracleReader);
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
            TransactionChunk *tcNew = newTransactionChunk(oracleReader);
            tcNew->prev = tcLast;
            tcNew->elements = 0;
            tcNew->size = 0;
            tcLast->next = tcNew;
            tcLast = tcNew;
        }
        appendTransactionChunk(tcLast, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
        return tcLast;
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

    bool TransactionBuffer::deleteTransactionPart(OracleReader *oracleReader, TransactionChunk* tc, typeuba &uba, typedba &dba, typeslt &slt, typerci &rci) {
        cerr << "ERROR: part transaction delete: not yet implemented" << endl;
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer" << endl;
            oracleReader->dumpTransactions();
            return false;
        }

        return false;
    }

    bool TransactionBuffer::getLastRecord(TransactionChunk* tc, typeop2 &opCode, RedoLogRecord* &redoLogRecord1, RedoLogRecord* &redoLogRecord2) {
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            return false;
        }
        uint64_t lastSize = *((uint64_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        uint8_t *buffer = tc->buffer + tc->size - lastSize;

        opCode = *((typeop2 *)(buffer + ROW_HEADER_OP));
        redoLogRecord1 = (RedoLogRecord*)(buffer + ROW_HEADER_REDO1);
        redoLogRecord1->data = buffer + ROW_HEADER_DATA;
        redoLogRecord2 = (RedoLogRecord*)(buffer + ROW_HEADER_REDO2);
        redoLogRecord2->data = buffer + ROW_HEADER_DATA + redoLogRecord1->length;

        return true;
    }

    TransactionChunk* TransactionBuffer::rollbackTransactionChunk(OracleReader *oracleReader, TransactionChunk* tc, typeuba &lastUba, typedba &lastDba,
            typeslt &lastSlt, typerci &lastRci) {
        if (tc->size < ROW_HEADER_TOTAL || tc->elements == 0) {
            cerr << "ERROR: trying to remove from empty buffer" << endl;
            oracleReader->dumpTransactions();
            return tc;
        }
        uint64_t lastSize = *((uint64_t *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        tc->size -= lastSize;
        --tc->elements;

        if (tc->elements == 0 && tc->prev != nullptr) {
            TransactionChunk *tcTmp = tc;
            tc = tc->prev;
            tc->next = nullptr;
            deleteTransactionChunk(tcTmp);
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
            oracleReader->dumpTransactions();
            return tc;
        }
        lastUba = *((typeuba *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_UBA));
        lastDba = *((typedba *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_DBA));
        lastSlt = *((typeslt *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_SLT));
        lastRci = *((typerci *)(tc->buffer + tc->size - ROW_HEADER_TOTAL + ROW_HEADER_RCI));

        return tc;
    }

    void TransactionBuffer::deleteTransactionChunks(TransactionChunk* tc, TransactionChunk* tcLast) {
        TransactionChunk* tcTemp = tc;
        ++freeBuffers;
        while (tcTemp->next != nullptr) {
            ++freeBuffers;
            tcTemp = tcTemp->next;
        }

        tcLast->next = unused;
        unused->prev = tcLast;
        unused = tc;
    }

    TransactionBuffer::~TransactionBuffer() {
        while (unused != nullptr) {
            TransactionChunk *tcTemp = unused->next;
            delete unused;
            unused = tcTemp;
        }
    }
}
