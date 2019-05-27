/* Header for Transaction class
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

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

namespace OpenLogReplicatorOracle {

    class TransactionChunk;
    class TransactionBuffer;
    class OpCode;
    class OpCode0502;
    class OpCode0504;
    class RedoLogRecord;
    class OracleEnvironment;

    class Transaction {
    public:
        typexid xid;
        typescn firstScn;
        typescn lastScn;
        TransactionChunk *tc;
        TransactionChunk *tcLast;
        uint32_t opCodes;
        uint32_t pos;
        typeuba lastUba;
        uint32_t lastDba;
        uint8_t lastSlt;
        uint8_t lastRci;
        bool isBegin;
        bool isCommit;
        bool isRollback;
        Transaction *next;

        bool operator< (Transaction &p);
        void touch(typescn scn);
        void add(uint32_t objd, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2,
                TransactionBuffer *transactionBuffer);
        void rollbackLastOp(typescn scn, TransactionBuffer *transactionBuffer);
        bool rollbackPreviousOp(typescn scn, TransactionBuffer *transactionBuffer, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci);

        void flush(OracleEnvironment *oracleEnvironment);

        Transaction(typexid xid, TransactionBuffer *transactionBuffer);
        virtual ~Transaction();

        friend ostream& operator<<(ostream& os, const Transaction& tran);
    };
}

#endif

