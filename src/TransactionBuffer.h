/* Header for TransactionBuffer class
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

#include "types.h"

#ifndef TRANSACTIONBUFFER_H_
#define TRANSACTIONBUFFER_H_

namespace OpenLogReplicatorOracle {

    class TransactionChunk;
    class RedoLogRecord;

    class TransactionBuffer {
    protected:
        TransactionChunk *unused;
        uint8_t *buffer;
        uint32_t size;

        void appendTransactionChunk(TransactionChunk* tc, uint32_t objdId, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci,
                RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
    public:
        TransactionChunk *newTransactionChunk();
        TransactionChunk* addTransactionChunk(TransactionChunk* tc, uint32_t objdId, typeuba uba, uint32_t dba, uint8_t slt,
                uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        TransactionChunk* rollbackTransactionChunk(TransactionChunk* tc, typeuba &lastUba, uint32_t &lastDba,
                uint8_t &lastSlt, uint8_t &lastRci);
        bool deleteTransactionPart(TransactionChunk* tc, typeuba &uba, uint32_t &dba, uint8_t &slt, uint8_t &rci);
        void deleteTransactionChunk(TransactionChunk* tc);
        void deleteTransactionChunks(TransactionChunk* tc, TransactionChunk* lastTc);

        TransactionBuffer();
        virtual ~TransactionBuffer();
    };
}

#endif
