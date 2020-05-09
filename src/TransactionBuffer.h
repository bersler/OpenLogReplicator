/* Header for TransactionBuffer class
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

#include "types.h"

#ifndef TRANSACTIONBUFFER_H_
#define TRANSACTIONBUFFER_H_

#define ROW_HEADER_OP       (0)
#define ROW_HEADER_REDO1    (sizeof(typeop2))
#define ROW_HEADER_REDO2    (sizeof(typeop2)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_DATA     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_OBJN     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord))
#define ROW_HEADER_OBJD     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj))
#define ROW_HEADER_SIZE     (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj))
#define ROW_HEADER_SLT      (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t))
#define ROW_HEADER_RCI      (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t)+1)
#define ROW_HEADER_SUBSCN   (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t)+2)
#define ROW_HEADER_DBA      (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t)+sizeof(uint32_t))
#define ROW_HEADER_UBA      (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t)+sizeof(uint32_t)+sizeof(typedba))
#define ROW_HEADER_SCN      (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t)+sizeof(uint32_t)+sizeof(typedba)+sizeof(typeuba))
#define ROW_HEADER_TOTAL    (sizeof(typeop2)+sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeobj)+sizeof(typeobj)+sizeof(uint64_t)+sizeof(uint32_t)+sizeof(typedba)+sizeof(typeuba)+sizeof(typescn))

namespace OpenLogReplicator {

    class OracleReader;
    class TransactionChunk;
    class RedoLogRecord;

    class TransactionBuffer {
    protected:
        TransactionChunk *unusedTc;
        TransactionChunk *tmpTc;
        uint64_t redoBufferSize;

        void appendTransactionChunk(TransactionChunk* tc, typeobj objn, typeobj objd, typeuba uba, typedba dba,
                uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
    public:
        uint64_t freeBuffers;
        uint64_t redoBuffers;

        TransactionChunk* newTransactionChunk(OracleReader *oracleReader);
        void addTransactionChunk(OracleReader *oracleReader, TransactionChunk* &tcLast, typeobj objn, typeobj objd, typeuba uba, typedba dba,
                uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void rollbackTransactionChunk(OracleReader *oracleReader, TransactionChunk* &tcLast, typeuba &lastUba, typedba &lastDba,
                uint8_t &lastSlt, uint8_t &lastRci);
        bool getLastRecord(TransactionChunk* tc, typeop2 &opCode, RedoLogRecord* &redoLogRecord1, RedoLogRecord* &redoLogRecord2);
        bool deleteTransactionPart(OracleReader *oracleReader, TransactionChunk* &tcLast, typeuba &uba, typedba &dba, uint8_t &slt, uint8_t &rci);
        void deleteTransactionChunk(TransactionChunk* tc);
        void deleteTransactionChunks(TransactionChunk* tc, TransactionChunk* lastTc);

        TransactionBuffer(uint64_t redoBuffers, uint64_t redoBufferSize);
        virtual ~TransactionBuffer();
    };
}

#endif
