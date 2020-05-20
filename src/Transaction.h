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

namespace OpenLogReplicator {

    class TransactionChunk;
    class TransactionBuffer;
    class OpCode;
    class OpCode0502;
    class OpCode0504;
    class RedoLogRecord;
    class OracleAnalyser;

    class Transaction {
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

        bool operator< (Transaction &p);
        void touch(typescn scn, typeseq sequence);
        void add(OracleAnalyser *oracleAnalyser, typeobj objn, typeobj objd, typeuba uba, typedba dba, typeslt slt, typerci rci,
                RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, TransactionBuffer *transactionBuffer, typeseq sequence);
        void rollbackLastOp(OracleAnalyser *oracleAnalyser, typescn scn, TransactionBuffer *transactionBuffer);
        bool rollbackPartOp(OracleAnalyser *oracleAnalyser, typescn scn, TransactionBuffer *transactionBuffer, typeuba uba,
                typedba dba, typeslt slt, typerci rci, uint64_t opFlags);

        void flush(OracleAnalyser *oracleAnalyser);

        Transaction(OracleAnalyser *oracleAnalyser, typexid xid, TransactionBuffer *transactionBuffer);
        virtual ~Transaction();

        friend ostream& operator<<(ostream& os, const Transaction& tran);
    };
}

#endif
