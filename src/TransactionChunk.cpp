/* Buffer to handle transactions peaces
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

#include <stdlib.h>
#include "MemoryException.h"
#include "TransactionChunk.h"

namespace OpenLogReplicator {

    TransactionChunk::TransactionChunk(TransactionChunk *prev, uint32_t redoBufferSize) :
            elements(0),
            size(0),
            prev(prev),
            next(nullptr) {
        if (prev != nullptr)
            prev->next = this;
        buffer = (uint8_t*)malloc(redoBufferSize);
        if (buffer == nullptr) {
            cerr << "ERROR: malloc buffer alloc error: " << dec << redoBufferSize << " bytes" << endl;
            throw MemoryException("error allocating memory");
        }
    }

    TransactionChunk::~TransactionChunk() {
        free(buffer);
        if (prev != nullptr)
            prev->next = next;
        if (next != nullptr)
            next->prev = prev;
    }
}
