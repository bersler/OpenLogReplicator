/* Header for Transaction class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <vector>

#include "types.h"

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

namespace OpenLogReplicator {

    class TransactionChunk;
    class OpCode;
    class OpCode0502;
    class OpCode0504;
    class RedoLogRecord;
    class OracleAnalyzer;

    class Transaction {
    protected:
        OracleAnalyzer *oracleAnalyzer;
        vector<uint8_t*> merges;
        TransactionChunk *deallocTc;
        void mergeBlocks(uint8_t *buffer, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);

    public:
        typeXID xid;
        typeSEQ firstSequence;
        uint64_t firstPos;
        typeSCN commitScn;
        TransactionChunk *firstTc;
        TransactionChunk *lastTc;
        uint64_t opCodes;
        uint64_t pos;
        typetime commitTimestamp;
        bool isBegin;
        bool isRollback;
        bool shutdown;

        Transaction(OracleAnalyzer *oracleAnalyzer, typeXID xid);
        virtual ~Transaction();

        void add(RedoLogRecord *redoLogRecord);
        void add(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void rollbackLastOp(typeSCN scn);
        void flush(void);
        friend ostream& operator<<(ostream& os, const Transaction& tran);
    };
}

#endif
