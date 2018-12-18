/* Buffer to handle transactions
   Copyright (C) 2018 Adam Leszczynski.

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
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

	TransactionBuffer::TransactionBuffer() :
		size(1) {
		TransactionChunk *tc, *prevTc;
		buffer = (uint8_t*) malloc(TRANSACTION_BUFFER_CHUNK_SIZE * TRANSACTION_BUFFER_CHUNK_NUM);

		prevTc = new TransactionChunk(nullptr, buffer);
		unused = prevTc;

		for (uint32_t a = 1; a < TRANSACTION_BUFFER_CHUNK_NUM; ++a) {
			tc = new TransactionChunk(prevTc, buffer + TRANSACTION_BUFFER_CHUNK_SIZE * a);
			prevTc = tc;
			++size;
		}
	}

	TransactionChunk *TransactionBuffer::newTransactionChunk() {
		if (unused == nullptr)
			throw MemoryException("out of memory: out of transaction buffer size");

		TransactionChunk *tc = unused;
		unused = unused->next;

		if (unused == nullptr)
			throw MemoryException("out of memory: out of transaction buffer size");

		unused->prev = nullptr;
		tc->next = nullptr;
		tc->size = 0;
		tc->elements = 0;

		--size;

		return tc;
	}

	void TransactionBuffer::deleteTransactionChunk(TransactionChunk* tc) {
		++size;

		tc->next = unused;
		if (unused != nullptr)
			unused->prev = tc;
		unused = tc;
	}

	TransactionChunk* TransactionBuffer::addTransactionChunk(TransactionChunk* tcLast, uint32_t objdId, typeuba uba, uint32_t dba,
			uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {

		//0:objId
		//4:op
		//X1:struct RedoLogRecord1
		//X2:struct RedoLogRecord2
		//d1:data1
		//d2:data2
		//4:size  -28
		//1:slt   -24
		//1:rci   -23
		//2:...
		//4:dba   -20
		//8:uba   -16
		//8:scn   -8

		//last scn is higher
		if (tcLast->size >= ROW_HEADER_MEMORY && *((typescn *)(tcLast->buffer + tcLast->size - 8)) > redoLogRecord1->scn) {
			//cerr << "WARN: scn out of order" << endl;
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
				if (elementsSkipped > tcTemp->elements || pos < ROW_HEADER_MEMORY) {
					cerr << "ERROR: bad data during finding scn out of order" << endl;
					return tcLast;
				}
				if (*((typescn *)(tcTemp->buffer + pos - 8)) <= redoLogRecord1->scn)
					break;
				pos -= *((uint32_t *)(tcTemp->buffer + pos - 28));
				++elementsSkipped;
			}

			//does the block need to be divided
			if (pos < tcTemp->size) {
				TransactionChunk *tcNew = newTransactionChunk();
				tcNew->elements = elementsSkipped;
				tcNew->size = tcTemp->size - pos;
				tcNew->prev = tcTemp;
				if (tcTemp->next != nullptr) {
					tcNew->next = tcTemp->next;
					tcTemp->next->prev = tcNew;
				}
				tcTemp->next = tcNew;

				memcpy(tcNew->buffer, tcTemp->buffer + pos, tcTemp->size - pos);
				tcTemp->elements -= elementsSkipped;
				tcTemp->size = pos;

				if (tcTemp == tcLast)
					tcLast = tcNew;
			}

			//new block needed
			if (tcTemp->size + redoLogRecord1->length + redoLogRecord2->length + sizeof(RedoLogRecord) +
					sizeof(struct RedoLogRecord) + 32 > TRANSACTION_BUFFER_CHUNK_SIZE) {
				TransactionChunk *tcNew = newTransactionChunk();
				tcNew->prev = tcTemp;
				tcNew->next = tcTemp->next;
				tcTemp->next->prev = tcNew;
				tcTemp->next = tcNew;
				tcTemp = tcNew;
			}
			appendTransactionChunk(tcTemp, objdId, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
		} else {
			//new block needed
			if (tcLast->size + redoLogRecord1->length + redoLogRecord2->length + sizeof(RedoLogRecord) +
					sizeof(struct RedoLogRecord) + 32 > TRANSACTION_BUFFER_CHUNK_SIZE) {
				TransactionChunk *tcNew = newTransactionChunk();
				tcNew->prev = tcLast;
				tcNew->elements = 0;
				tcNew->size = 0;
				tcLast->next = tcNew;
				tcLast = tcNew;
			}
			appendTransactionChunk(tcLast, objdId, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
		}

		return tcLast;
	}

	void TransactionBuffer::appendTransactionChunk(TransactionChunk* tc, uint32_t objdId, typeuba uba, uint32_t dba,
			uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
		//append to the chunk at the end
		*((uint32_t *)(tc->buffer + tc->size)) = objdId;
		*((uint32_t *)(tc->buffer + tc->size + 4)) = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
		memcpy(tc->buffer + tc->size + 8,
				redoLogRecord1, sizeof(struct RedoLogRecord));
		memcpy(tc->buffer + tc->size + 8 + sizeof(struct RedoLogRecord),
				redoLogRecord2, sizeof(struct RedoLogRecord));

		memcpy(tc->buffer + tc->size + 8 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord),
				redoLogRecord1->data, redoLogRecord1->length);
		memcpy(tc->buffer + tc->size + 8 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) + redoLogRecord1->length,
				redoLogRecord2->data, redoLogRecord2->length);

		*((uint32_t *)(tc->buffer + tc->size + 8 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
				redoLogRecord1->length + redoLogRecord2->length)) =
				redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_MEMORY;
		*((uint8_t *)(tc->buffer + tc->size + 12 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
				redoLogRecord1->length + redoLogRecord2->length)) = slt;
		*((uint8_t *)(tc->buffer + tc->size + 13 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
				redoLogRecord1->length + redoLogRecord2->length)) = rci;
		*((uint32_t *)(tc->buffer + tc->size + 16 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
				redoLogRecord1->length + redoLogRecord2->length)) = dba;
		*((typeuba *)(tc->buffer + tc->size + 20 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
				redoLogRecord1->length + redoLogRecord2->length)) = uba;
		*((typescn *)(tc->buffer + tc->size + 28 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
				redoLogRecord1->length + redoLogRecord2->length)) = redoLogRecord1->scn;

		tc->size += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_MEMORY;
		++tc->elements;
	}


	TransactionChunk* TransactionBuffer::rollbackTransactionChunk(TransactionChunk* tc, typeuba &lastUba, uint32_t &lastDba,
			uint8_t &lastSlt, uint8_t &lastRci) {
		if (tc->size < ROW_HEADER_MEMORY || tc->elements == 0) {
			cerr << "ERROR: trying to remove from empty buffer" << endl;
			return tc;
		}
		uint32_t lastSize = *((uint32_t *)(tc->buffer + tc->size - 28));
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

		if (tc->size < ROW_HEADER_MEMORY) {
			cerr << "ERROR: can't set last UBA size: " << dec << tc->size << ", elements: " << tc->elements << endl;
			return tc;
		}
		lastUba = *((typeuba *)(tc->buffer + tc->size - 16));
		lastDba = *((uint32_t *)(tc->buffer + tc->size - 20));
		lastSlt = *((uint8_t *)(tc->buffer + tc->size - 24));
		lastRci = *((uint8_t *)(tc->buffer + tc->size - 23));

		return tc;
	}

	void TransactionBuffer::deleteTransactionChunks(TransactionChunk* tc, TransactionChunk* tcLast) {
		uint32_t num = 1;
		TransactionChunk* tcTemp = tc;
		while (tcTemp->next != nullptr) {
			++num;
			tcTemp = tcTemp->next;
		}
		size += num;

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

		if (buffer != nullptr) {
			free(buffer);
			buffer = nullptr;
		}
	}
}
