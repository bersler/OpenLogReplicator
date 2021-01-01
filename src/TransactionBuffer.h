/* Header for TransactionBuffer class
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

#include <unordered_map>

#include "types.h"

#ifndef TRANSACTIONBUFFER_H_
#define TRANSACTIONBUFFER_H_

#define ROW_HEADER_OP       (0)
#define ROW_HEADER_REDO1    (sizeof(typeop2))
#define ROW_HEADER_REDO2    (sizeof(typeop2)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_DATA     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_SIZE     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_TOTAL    (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(uint64_t))

#define FULL_BUFFER_SIZE    65536
#define HEADER_BUFFER_SIZE  (sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint8_t*)+sizeof(TransactionChunk*)+sizeof(TransactionChunk*))
#define DATA_BUFFER_SIZE    (FULL_BUFFER_SIZE-HEADER_BUFFER_SIZE)
#define BUFFERS_FREE_MASK   0xFFFF

namespace OpenLogReplicator {

    class OracleAnalyzer;
    class RedoLogRecord;
    class Transaction;
    class TransactionChunk;

    struct TransactionChunk {
        uint64_t elements;
        uint64_t size;
        uint64_t pos;
        uint8_t *header;
        TransactionChunk *prev;
        TransactionChunk *next;
        uint8_t buffer[DATA_BUFFER_SIZE];
    };

    class TransactionBuffer {
    protected:
        OracleAnalyzer *oracleAnalyzer;
        uint8_t buffer[DATA_BUFFER_SIZE];

    public:
        unordered_map<uint8_t*,uint64_t> partiallyFullChunks;

        TransactionBuffer(OracleAnalyzer *oracleAnalyzer);
        virtual ~TransactionBuffer();

        TransactionChunk* newTransactionChunk(void);
        void addTransactionChunk(Transaction *transaction, RedoLogRecord *redoLogRecord1);
        void addTransactionChunk(Transaction *transaction, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void rollbackTransactionChunk(Transaction *transaction);
        void deleteTransactionChunk(TransactionChunk* tc);
        void deleteTransactionChunks(TransactionChunk* tc);
    };
}

#endif
