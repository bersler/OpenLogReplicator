/* Header for TransactionBuffer class
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
#include <mutex>
#include <set>
#include <unordered_map>

#include "../common/Ctx.h"
#include "../common/LobKey.h"
#include "../common/RedoLogRecord.h"
#include "../common/types.h"
#include "../common/typeXid.h"

#ifndef TRANSACTION_BUFFER_H_
#define TRANSACTION_BUFFER_H_

namespace OpenLogReplicator {
    class Transaction;
    class XmlCtx;

    struct TransactionChunk {
        static constexpr uint64_t FULL_BUFFER_SIZE = 65536;
        static constexpr uint64_t HEADER_BUFFER_SIZE = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint8_t*) +
                                                       sizeof(TransactionChunk*) + sizeof(TransactionChunk*);
        static constexpr uint64_t DATA_BUFFER_SIZE = FULL_BUFFER_SIZE - HEADER_BUFFER_SIZE;

        uint64_t elements;
        uint64_t size;
        uint64_t pos;
        uint8_t* header;
        TransactionChunk* prev;
        TransactionChunk* next;
        uint8_t buffer[DATA_BUFFER_SIZE];
    };

    class TransactionBuffer {
    public:
        static constexpr uint64_t ROW_HEADER_OP = 0;
        static constexpr uint64_t ROW_HEADER_REDO1 = sizeof(typeOp2);
        static constexpr uint64_t ROW_HEADER_REDO2 = sizeof(typeOp2) + sizeof(RedoLogRecord);
        static constexpr uint64_t ROW_HEADER_DATA = sizeof(typeOp2) + sizeof(RedoLogRecord) + sizeof(RedoLogRecord);
        static constexpr uint64_t ROW_HEADER_SIZE = sizeof(typeOp2) + sizeof(RedoLogRecord) + sizeof(RedoLogRecord);
        static constexpr uint64_t ROW_HEADER_TOTAL = sizeof(typeOp2) + sizeof(RedoLogRecord) + sizeof(RedoLogRecord) + sizeof(uint64_t);

        static constexpr uint64_t BUFFERS_FREE_MASK = 0xFFFF;

    protected:
        Ctx* ctx;
        uint8_t buffer[TransactionChunk::DATA_BUFFER_SIZE];
        std::unordered_map<uint8_t*, uint64_t> partiallyFullChunks;

        std::mutex mtx;
        std::unordered_map<typeXidMap, Transaction*> xidTransactionMap;
        std::map<LobKey, uint8_t*> orphanedLobs;

    public:
        std::set<typeXid> skipXidList;
        std::set<typeXid> dumpXidList;
        std::set<typeXidMap> brokenXidMapList;
        std::string dumpPath;

        explicit TransactionBuffer(Ctx* newCtx);
        virtual ~TransactionBuffer();

        void purge();
        [[nodiscard]] Transaction* findTransaction(XmlCtx* xmlCtx, typeXid xid, typeConId conId, bool old, bool add, bool rollback);
        void dropTransaction(typeXid xid, typeConId conId);
        void addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord);
        void addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void rollbackTransactionChunk(Transaction* transaction);
        [[nodiscard]] TransactionChunk* newTransactionChunk();
        void deleteTransactionChunk(TransactionChunk* tc);
        void deleteTransactionChunks(TransactionChunk* tc);
        void mergeBlocks(uint8_t* mergeBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void checkpoint(typeSeq& minSequence, uint64_t& minOffset, typeXid& minXid);
        void addOrphanedLob(RedoLogRecord* redoLogRecord1);
        uint8_t* allocateLob(const RedoLogRecord* redoLogRecord1) const;
    };
}

#endif
