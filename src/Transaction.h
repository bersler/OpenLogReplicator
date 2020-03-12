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
    class OracleReader;

    class Transaction {
    public:
        typexid xid;
        typeseq firstSequence;
        typescn firstScn;
        typescn lastScn;
        TransactionChunk *tc;
        TransactionChunk *tcLast;
        uint64_t opCodes;
        uint64_t pos;
        typeuba lastUba;
        typedba lastDba;
        typeslt lastSlt;
        typerci lastRci;
        bool isBegin;
        bool isCommit;
        bool isRollback;
        Transaction *next;

        bool operator< (Transaction &p);
        void touch(typescn scn, typeseq sequence);
        void add(OracleReader *oracleReader, typeobj objn, typeobj objd, typeuba uba, typedba dba, typeslt slt, typerci rci,
                RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, TransactionBuffer *transactionBuffer, typeseq sequence);
        void rollbackLastOp(OracleReader *oracleReader, typescn scn, TransactionBuffer *transactionBuffer);
        bool rollbackPreviousOp(OracleReader *oracleReader, typescn scn, TransactionBuffer *transactionBuffer, typeuba uba,
                typedba dba, typeslt slt, typerci rci);

        void flush(OracleReader *oracleReader);

        Transaction(typexid xid, TransactionBuffer *transactionBuffer);
        virtual ~Transaction();

        friend ostream& operator<<(ostream& os, const Transaction& tran);
    };
}

#endif

