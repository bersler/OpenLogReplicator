/* Header for Transaction class
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <map>
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
    class XmlCtx;

    class Transaction final {
    protected:
        TransactionChunk* deallocTc;
        uint64_t opCodes;

    public:
        uint8_t* mergeBuffer;
        LobCtx lobCtx;
        XmlCtx* xmlCtx;
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
        bool schema;
        bool shutdown;
        bool lastSplit;
        bool dump;
        uint64_t size;

        // Attributes
        std::unordered_map<std::string, std::string> attributes;

        explicit Transaction(typeXid newXid, std::map<LobKey, uint8_t*>* newOrphanedLobs, XmlCtx* newXmlCtx);

        void add(const Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1);
        void add(const Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(const Metadata* metadata, TransactionBuffer* transactionBuffer, const RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(const Metadata* metadata, TransactionBuffer* transactionBuffer, const RedoLogRecord* redoLogRecord1);
        void flush(Metadata* metadata, TransactionBuffer* transactionBuffer, Builder* builder, typeScn lwnScn);
        void purge(TransactionBuffer* transactionBuffer);

        inline void log(const Ctx* ctx, const char* msg, const RedoLogRecord* redoLogRecord1) const {
            if (likely(!dump && (ctx->trace & Ctx::TRACE_DUMP) == 0))
                return;

            ctx->info(0, std::string(msg) + " xid: " + xid.toString() +
                         " OP: " + std::to_string(redoLogRecord1->opCode) +
                         " opc: " + std::to_string(redoLogRecord1->opc) +
                         " obj: " + std::to_string(redoLogRecord1->obj) +
                         " dataobj: " + std::to_string(redoLogRecord1->dataObj) +
                         " bdba: " + std::to_string(redoLogRecord1->bdba) +
                         " slot: " + std::to_string(redoLogRecord1->slot) +
                         " fb: " + std::to_string(static_cast<uint64_t>(redoLogRecord1->fb)) +
                         " cc: " + std::to_string(static_cast<uint64_t>(redoLogRecord1->cc)) +
                         " suppbdba: " + std::to_string(redoLogRecord1->suppLogBdba) +
                         " suppslot: " + std::to_string(redoLogRecord1->suppLogSlot) +
                         " suppfb: " + std::to_string(static_cast<uint64_t>(redoLogRecord1->suppLogFb)) +
                         " suppcc: " + std::to_string(static_cast<uint64_t>(redoLogRecord1->suppLogCC)) +
                         " dba: " + std::to_string(redoLogRecord1->dba) +
                         " slt: " + std::to_string(redoLogRecord1->slt) +
                         " seq: " + std::to_string(static_cast<uint64_t>(redoLogRecord1->seq)) +
                         " flg: " + std::to_string(redoLogRecord1->flg) +
                         " split: " + std::to_string(lastSplit) +
                         " offset: " + std::to_string(redoLogRecord1->dataOffset));
        }

        std::string toString() const;
    };
}

#endif
