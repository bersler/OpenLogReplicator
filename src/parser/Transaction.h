/* Header for Transaction class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include <map>
#include <unordered_map>
#include <vector>

#include "../common/Attribute.h"
#include "../common/LobKey.h"
#include "../common/LobCtx.h"
#include "../common/RedoLogRecord.h"
#include "../common/types/FileOffset.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"
#include "../common/types/Xid.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    class Builder;
    class Metadata;
    class TransactionBuffer;
    struct TransactionChunk;
    class XmlCtx;

    class Transaction final {
    protected:
        std::vector<uint64_t> deallocChunks;
        uint64_t opCodes{0};

    public:
        uint8_t* mergeBuffer{nullptr};
        LobCtx lobCtx;
        XmlCtx* xmlCtx;
        Xid xid;
        Seq beginSequence{Seq::none()};
        Scn beginScn{Scn::none()};
        Time beginTimestamp{0};
        FileOffset beginFileOffset{0};
        Seq commitSequence{Seq::none()};
        Scn commitScn{Scn::none()};
        TransactionChunk* lastTc{nullptr};
        Time commitTimestamp{0};
        bool begin{false};
        bool rollback{false};
        bool system{false};
        bool schema{false};
        bool shutdown{false};
        bool lastSplit{false};
        bool dump{false};
        typeTransactionSize size{0};
        uint16_t thread;

        AttributeMap attributes;

        explicit Transaction(Xid newXid, std::map<LobKey, uint8_t*>* newOrphanedLobs, XmlCtx* newXmlCtx, uint16_t thread);

        void add(const Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1);
        void add(const Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(const Metadata* metadata, TransactionBuffer* transactionBuffer, const RedoLogRecord* redoLogRecord1,
                            const RedoLogRecord* redoLogRecord2);
        void rollbackLastOp(const Metadata* metadata, TransactionBuffer* transactionBuffer, const RedoLogRecord* redoLogRecord1);
        void flush(Metadata* metadata, Builder* builder);
        void purge(Ctx* ctx);

        void log(const Ctx* ctx, const char* msg, const RedoLogRecord* redoLogRecord1) const {
            if (likely(!dump && !ctx->isTraceSet(Ctx::TRACE::DUMP)))
                return;

            ctx->info(0, std::string(msg) + " xid: " + xid.toString() +
                      " OP: " + std::to_string(static_cast<uint>(redoLogRecord1->opCode >> 8)) +
                      "." + std::to_string(static_cast<uint>(redoLogRecord1->opCode & 0xFF)) +
                      " scn: " + redoLogRecord1->scn.toString() +
                      " opc: " + std::to_string(redoLogRecord1->opc) +
                      " obj: " + std::to_string(redoLogRecord1->obj) +
                      " dataobj: " + std::to_string(redoLogRecord1->dataObj) +
                      " bdba: " + std::to_string(redoLogRecord1->bdba) +
                      " slot: " + std::to_string(redoLogRecord1->slot) +
                      " fb: " + std::to_string(static_cast<uint>(redoLogRecord1->fb)) +
                      " cc: " + std::to_string(static_cast<uint>(redoLogRecord1->cc)) +
                      " suppbdba: " + std::to_string(redoLogRecord1->suppLogBdba) +
                      " suppslot: " + std::to_string(redoLogRecord1->suppLogSlot) +
                      " suppfb: " + std::to_string(static_cast<uint>(redoLogRecord1->suppLogFb)) +
                      " suppcc: " + std::to_string(static_cast<uint>(redoLogRecord1->suppLogCC)) +
                      " dba: " + std::to_string(redoLogRecord1->dba) +
                      " slt: " + std::to_string(redoLogRecord1->slt) +
                      " seq: " + std::to_string(static_cast<uint>(redoLogRecord1->seq)) +
                      " flg: " + std::to_string(redoLogRecord1->flg) +
                      " split: " + std::to_string(lastSplit ? 1 : 0) +
                      " offset: " + redoLogRecord1->fileOffset.toString());
        }

        [[nodiscard]] std::string toString(const Ctx* ctx) const;
    };
}

#endif
