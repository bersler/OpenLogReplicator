/* Header for Transaction class
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

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#define SPLIT_BLOCK_SIZE       (0)
#define SPLIT_BLOCK_NEXT       (sizeof(uint8_t*))
#define SPLIT_BLOCK_OP1        (sizeof(uint8_t*)+sizeof(uint8_t*))
#define SPLIT_BLOCK_OP2        (sizeof(uint8_t*)+sizeof(uint8_t*)+sizeof(typeop1))

#define SPLIT_BLOCK_RECORD1    (sizeof(uint8_t*)+sizeof(uint8_t*)+sizeof(typeop1)+sizeof(typeop1))
#define SPLIT_BLOCK_DATA1      (sizeof(uint8_t*)+sizeof(uint8_t*)+sizeof(typeop1)+sizeof(typeop1)+sizeof(RedoLogRecord))

#define SPLIT_BLOCK_RECORD2    (sizeof(uint8_t*)+sizeof(uint8_t*)+sizeof(typeop1)+sizeof(typeop1)+sizeof(RedoLogRecord))
#define SPLIT_BLOCK_DATA2      (sizeof(uint8_t*)+sizeof(uint8_t*)+sizeof(typeop1)+sizeof(typeop1)+sizeof(RedoLogRecord)+sizeof(RedoLogRecord))


namespace OpenLogReplicator {

    class TransactionChunk;
    class TransactionBuffer;
    class OpCode;
    class OpCode0502;
    class OpCode0504;
    class RedoLogRecord;
    class OracleAnalyser;

    class Transaction {
    protected:
        uint8_t *splitBlockList;

        void mergeSplitBlocksToBuffer(OracleAnalyser *oracleAnalyser, uint8_t *buffer, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void mergeSplitBlocks(OracleAnalyser *oracleAnalyser, RedoLogRecord *headRedoLogRecord1, RedoLogRecord *midRedoLogRecord1, RedoLogRecord *tailRedoLogRecord1,
                RedoLogRecord *redoLogRecord2);

    public:
        typexid xid;
        typeseq firstSequence;
        typescn firstScn;
        typescn lastScn;
        TransactionChunk *firstTc;
        TransactionChunk *lastTc;
        uint64_t opCodes;
        uint64_t pos;
        typeuba lastUba;
        typedba lastDba;
        typeslt lastSlt;
        typerci lastRci;
        typetime commitTime;
        bool isBegin;
        bool isCommit;
        bool isRollback;
        bool shutdown;
        Transaction *next;

        Transaction(OracleAnalyser *oracleAnalyser, typexid xid, TransactionBuffer *transactionBuffer);
        virtual ~Transaction();

        void touch(typescn scn, typeseq sequence);
        void addSplitBlock(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord);
        void addSplitBlock(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void add(OracleAnalyser *oracleAnalyser, typeobj objn, typeobj objd, typeuba uba, typedba dba, typeslt slt, typerci rci,
                RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, TransactionBuffer *transactionBuffer, typeseq sequence);
        void rollbackLastOp(OracleAnalyser *oracleAnalyser, typescn scn, TransactionBuffer *transactionBuffer);
        bool rollbackPartOp(OracleAnalyser *oracleAnalyser, typescn scn, TransactionBuffer *transactionBuffer, typeuba uba,
                typedba dba, typeslt slt, typerci rci, uint64_t opFlags);
        void flushSplitBlocks(OracleAnalyser *oracleAnalyser);
        void flush(OracleAnalyser *oracleAnalyser);

        bool operator< (Transaction &p);
        friend ostream& operator<<(ostream& os, const Transaction& tran);
    };
}

#endif
