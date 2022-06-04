/* Header for Transaction class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/types.h"
#include "../common/typeTime.h"
#include "../common/typeXid.h"

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

namespace OpenLogReplicator {
    class Builder;
    class RedoLogRecord;
    class Metadata;
    class TransactionBuffer;
    struct TransactionChunk;

    class Transaction {
    protected:
        TransactionChunk* deallocTc;
        uint64_t opCodes;

    public:
        std::vector<uint8_t*> merges;
        typeXid xid;
        typeSeq firstSequence;
        uint64_t firstOffset;
        typeSeq commitSequence;
        typeScn commitScn;
        TransactionChunk* firstTc;
        TransactionChunk* lastTc;
        typeTime commitTimestamp;
        bool begin;
        bool rollback;
        bool system;
        bool shutdown;
        bool lastSplit;
        uint64_t size;

        explicit Transaction(typeXid xid);

        void add(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord);
        void add(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord);
        void flush(Metadata* metadata, TransactionBuffer* transactionBuffer, Builder* builder);
        void purge(TransactionBuffer* transactionBuffer);

        friend std::ostream& operator<<(std::ostream& os, const Transaction& tran);
    };
}

#endif
