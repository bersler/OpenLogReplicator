/* Header for TransactionBuffer class
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

#include "types.h"

#ifndef TRANSACTIONBUFFER_H_
#define TRANSACTIONBUFFER_H_

#define ROW_HEADER_OP       (0)
#define ROW_HEADER_REDO1    (sizeof(typeop2))
#define ROW_HEADER_REDO2    (sizeof(typeop2)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_DATA     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_SIZE     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_SUBSCN   (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(uint64_t))
#define ROW_HEADER_SCN      (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(uint64_t)+sizeof(uint32_t))
#define ROW_HEADER_TOTAL    (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(uint64_t)+sizeof(uint32_t)+sizeof(typescn))

namespace OpenLogReplicator {

    class OracleAnalyser;
    class RedoLogRecord;
    class Transaction;
    class TransactionChunk;

    class TransactionBuffer {
    protected:
        TransactionChunk *unusedTc;
        TransactionChunk *copyTc;
        uint64_t redoBufferSize;

        void appendTransactionChunk(TransactionChunk* tc, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
    public:
        uint64_t freeBuffers;
        uint64_t redoBuffers;

        TransactionChunk* newTransactionChunk(OracleAnalyser *oracleAnalyser);
        void addTransactionChunk(OracleAnalyser *oracleAnalyser, Transaction *transaction, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void rollbackTransactionChunk(OracleAnalyser *oracleAnalyser, Transaction *transaction);
        bool deleteTransactionPart(OracleAnalyser *oracleAnalyser, Transaction *transaction,
                RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2);
        void deleteTransactionChunk(TransactionChunk* tc);
        void deleteTransactionChunks(TransactionChunk* startTc, TransactionChunk* endTc);

        TransactionBuffer(uint64_t redoBuffers, uint64_t redoBufferSize);
        virtual ~TransactionBuffer();
    };
}

#endif
