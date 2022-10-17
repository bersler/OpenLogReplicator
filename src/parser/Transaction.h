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

#include <unordered_map>
#include <vector>

#include "../common/LobKey.h"
#include "../common/LobCtx.h"
#include "../common/RedoLogRecord.h"
#include "../common/types.h"
#include "../common/typeTime.h"
#include "../common/typeXid.h"

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

namespace OpenLogReplicator {
    class Builder;
    class Metadata;
    class TransactionBuffer;
    struct TransactionChunk;

    class Transaction {
    protected:
        TransactionChunk* deallocTc;
        uint64_t opCodes;

    public:
        uint8_t* mergeBuffer;
        LobCtx lobCtx;
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
        bool dump;
        uint64_t size;

        explicit Transaction(typeXid newXid);

        void add(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1);
        void add(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1);
        void flush(Metadata* metadata, TransactionBuffer* transactionBuffer, Builder* builder);
        void purge(TransactionBuffer* transactionBuffer);

        void log(Ctx* ctx, const char* msg, RedoLogRecord* redoLogRecord1) {
            if (!dump || (ctx->trace2 & TRACE2_DUMP) != 0)
                return;

            INFO(msg << " xid: " << xid <<
                     " OP: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord1->opCode <<
                     " opc: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord1->opc <<
                     " obj: " << std::dec << redoLogRecord1->obj <<
                     " bdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord1->bdba <<
                     " slot: " << std::dec << redoLogRecord1->slot <<
                     " fb: " << std::hex << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord1->fb <<
                     " cc: " << std::dec << (uint64_t)redoLogRecord1->cc <<
                     " suppbdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord1->suppLogBdba <<
                     " suppslot: " << std::dec << redoLogRecord1->suppLogSlot <<
                     " suppfb: " << std::hex << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord1->suppLogFb <<
                     " suppcc: " << std::dec << (uint64_t)redoLogRecord1->suppLogCC <<
                     " dba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord1->dba <<
                     " slt: " << std::dec << redoLogRecord1->slt <<
                     " rci: " << std::dec << (uint64_t)redoLogRecord1->rci <<
                     " seq: " << std::dec << (uint64_t)redoLogRecord1->seq <<
                     " flg: " << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord1->flg <<
                     " split: " << std::dec << lastSplit <<
                     " offset: " << std::dec << redoLogRecord1->dataOffset)
        }

        friend std::ostream& operator<<(std::ostream& os, const Transaction& tran);
    };
}

#endif
