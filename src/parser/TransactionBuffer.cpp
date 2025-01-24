/* Buffer to handle transactions
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <cstddef>
#include <cstring>

#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/exception/RedoLogException.h"
#include "../common/types/Seq.h"
#include "OpCode0501.h"
#include "OpCode050B.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    TransactionBuffer::TransactionBuffer(Ctx* newCtx) :
            ctx(newCtx) {
        buffer[0] = 0;
    }

    TransactionBuffer::~TransactionBuffer() {
        skipXidList.clear();
        dumpXidList.clear();
        brokenXidMapList.clear();

        for (const auto& [_, data]: orphanedLobs)
            delete[] data;
        orphanedLobs.clear();
    }

    void TransactionBuffer::purge() {
        for (const auto& [_, transaction]: xidTransactionMap) {
            transaction->purge(ctx);
            delete transaction;
        }
        xidTransactionMap.clear();
    }

    Transaction* TransactionBuffer::findTransaction(XmlCtx* xmlCtx, Xid xid, typeConId conId, bool old, bool add, bool rollback) {
        const XidMap xidMap = (xid.getData() >> 32) | ((static_cast<uint64_t>(conId)) << 32);
        Transaction* transaction;

        auto xidTransactionMapIt = xidTransactionMap.find(xidMap);
        if (xidTransactionMapIt != xidTransactionMap.end()) {
            transaction = xidTransactionMapIt->second;
            if (unlikely(!rollback && (!old || transaction->xid != xid)))
                throw RedoLogException(50039, "transaction " + xid.toString() + " conflicts with " + transaction->xid.toString());
        } else {
            if (!add)
                return nullptr;

            transaction = new Transaction(xid, &orphanedLobs, xmlCtx);
            {
                ctx->parserThread->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::TRANSACTION_FIND);
                std::unique_lock<std::mutex> const lck(mtx);
                xidTransactionMap.insert_or_assign(xidMap, transaction);
            }
            ctx->parserThread->contextSet(Thread::CONTEXT::CPU);
            ctx->swappedMemoryInit(ctx->parserThread, xid);

            if (dumpXidList.find(xid) != dumpXidList.end())
                transaction->dump = true;
        }

        return transaction;
    }

    void TransactionBuffer::dropTransaction(Xid xid, typeConId conId) {
        const XidMap xidMap = (xid.getData() >> 32) | (static_cast<uint64_t>(conId) << 32);
        {
            ctx->parserThread->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::TRANSACTION_DROP);
            std::unique_lock<std::mutex> const lck(mtx);
            xidTransactionMap.erase(xidMap);
        }
        ctx->parserThread->contextSet(Thread::CONTEXT::CPU);
    }

    void TransactionBuffer::addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord) {
        const typeChunkSize chunkSize = redoLogRecord->size + ROW_HEADER_TOTAL;

        if (unlikely(chunkSize > TransactionChunk::DATA_BUFFER_SIZE))
            throw RedoLogException(50040, "block size (" + std::to_string(chunkSize) + ") exceeding max block size (" +
                                          std::to_string(TransactionChunk::DATA_BUFFER_SIZE) + "), please report this issue");

        if (unlikely(transaction->lastSplit)) {
            if (unlikely((redoLogRecord->flg & OpCode::FLG_MULTIBLOCKUNDOMID) == 0))
                throw RedoLogException(50041, "bad split offset: " + redoLogRecord->fileOffset.toString() + " xid: " + transaction->xid.toString());

            auto* const lastTc = transaction->lastTc;
            auto lastSize = *reinterpret_cast<typeChunkSize*>(lastTc->buffer + lastTc->size - sizeof(typeChunkSize));
            const auto* const last501 = reinterpret_cast<const RedoLogRecord*>(lastTc->buffer + lastTc->size - lastSize + ROW_HEADER_DATA0);

            const uint32_t mergeSize = last501->size + redoLogRecord->size;
            transaction->mergeBuffer = new uint8_t[mergeSize];
            mergeBlocks(transaction->mergeBuffer, redoLogRecord, last501);
            rollbackTransactionChunk(transaction);
        }
        transaction->lastSplit = (redoLogRecord->flg & (OpCode::FLG_MULTIBLOCKUNDOTAIL | OpCode::FLG_MULTIBLOCKUNDOMID)) != 0;

        // New block
        if (transaction->lastTc == nullptr || transaction->lastTc->size + chunkSize > TransactionChunk::DATA_BUFFER_SIZE)
            transaction->lastTc = reinterpret_cast<TransactionChunk*>(ctx->swappedMemoryGrow(ctx->parserThread, transaction->xid));

        // Append to the chunk at the end
        auto* lastTc = transaction->lastTc;
        *reinterpret_cast<typeOp2*>(lastTc->buffer + lastTc->size + ROW_HEADER_OP) = redoLogRecord->opCode << 16;
        auto* redoLogRecordTarget1 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA0);
        memcpy(reinterpret_cast<void*>(redoLogRecordTarget1),
               reinterpret_cast<const void*>(redoLogRecord), sizeof(RedoLogRecord));
        redoLogRecordTarget1->dataExt = nullptr;
        memcpy(reinterpret_cast<void*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA1),
               reinterpret_cast<const void*>(redoLogRecord->data()), redoLogRecord->size);
        memset(reinterpret_cast<void*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA1 + redoLogRecord->size), 0, sizeof(RedoLogRecord));

        *reinterpret_cast<typeChunkSize*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA2 + redoLogRecord->size) = chunkSize;

        lastTc->size += chunkSize;
        ++lastTc->elements;
        transaction->size += chunkSize;

        if (transaction->mergeBuffer != nullptr) {
            delete[] transaction->mergeBuffer;
            transaction->mergeBuffer = nullptr;
        }
    }

    void TransactionBuffer::addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2) {
        typeChunkSize chunkSize = redoLogRecord1->size + redoLogRecord2->size + ROW_HEADER_TOTAL;

        if (unlikely(chunkSize > TransactionChunk::DATA_BUFFER_SIZE))
            throw RedoLogException(50040, "block size (" + std::to_string(chunkSize) + ") exceeding max block size (" +
                                          std::to_string(TransactionChunk::DATA_BUFFER_SIZE) + "), please report this issue");

        if (unlikely(transaction->lastSplit)) {
            if (unlikely((redoLogRecord1->opCode) != 0x0501))
                throw RedoLogException(50042, "split undo HEAD on 5.1 offset: " + redoLogRecord1->fileOffset.toString());

            if (unlikely((redoLogRecord1->flg & OpCode::FLG_MULTIBLOCKUNDOHEAD) == 0))
                throw RedoLogException(50043, "bad split offset: " + redoLogRecord1->fileOffset.toString() + " xid: " + transaction->xid.toString() +
                                              " second position");

            auto* const lastTc = transaction->lastTc;
            const auto lastSize = *reinterpret_cast<typeChunkSize*>(lastTc->buffer + lastTc->size - sizeof(typeChunkSize));
            const auto* last501 = reinterpret_cast<const RedoLogRecord*>(lastTc->buffer + lastTc->size - lastSize + ROW_HEADER_DATA0);

            const uint32_t mergeSize = last501->size + redoLogRecord1->size;
            transaction->mergeBuffer = new uint8_t[mergeSize];
            mergeBlocks(transaction->mergeBuffer, redoLogRecord1, last501);

            typePos fieldPos = redoLogRecord1->fieldPos;
            typeSize const fieldSize = ctx->read16(redoLogRecord1->data(redoLogRecord1->fieldSizesDelta + (1 * 2)));
            fieldPos += (fieldSize + 3) & 0xFFFC;

            ctx->write16(const_cast<uint8_t*>(redoLogRecord1->data(fieldPos + 20)), redoLogRecord1->flg);
            OpCode0501::process0501(ctx, redoLogRecord1);
            chunkSize = redoLogRecord1->size + redoLogRecord2->size + ROW_HEADER_TOTAL;

            rollbackTransactionChunk(transaction);
            transaction->lastSplit = false;
        }

        // New block
        if (transaction->lastTc == nullptr || transaction->lastTc->size + chunkSize > TransactionChunk::DATA_BUFFER_SIZE)
            transaction->lastTc = reinterpret_cast<TransactionChunk*>(ctx->swappedMemoryGrow(ctx->parserThread, transaction->xid));

        // Append to the chunk at the end
        auto* lastTc = transaction->lastTc;
        *reinterpret_cast<typeOp2*>(lastTc->buffer + lastTc->size + ROW_HEADER_OP) = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;

        auto* redoLogRecordTarget1 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA0);
        memcpy(reinterpret_cast<void*>(redoLogRecordTarget1),
               reinterpret_cast<const void*>(redoLogRecord1), sizeof(RedoLogRecord));
        redoLogRecordTarget1->dataExt = nullptr;
        memcpy(reinterpret_cast<void*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA1),
               reinterpret_cast<const void*>(redoLogRecord1->data()), redoLogRecord1->size);

        auto* redoLogRecordTarget2 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA1 + redoLogRecord1->size);
        memcpy(reinterpret_cast<void*>(redoLogRecordTarget2),
               reinterpret_cast<const void*>(redoLogRecord2), sizeof(RedoLogRecord));
        redoLogRecordTarget2->dataExt = nullptr;
        memcpy(reinterpret_cast<void*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA2 + redoLogRecord1->size),
               reinterpret_cast<const void*>(redoLogRecord2->data()), redoLogRecord2->size);
        *reinterpret_cast<typeChunkSize*>(lastTc->buffer + lastTc->size + ROW_HEADER_DATA2 + redoLogRecord1->size + redoLogRecord2->size) = chunkSize;

        lastTc->size += chunkSize;
        ++lastTc->elements;
        transaction->size += chunkSize;

        if (transaction->mergeBuffer != nullptr) {
            delete[] transaction->mergeBuffer;
            transaction->mergeBuffer = nullptr;
        }
    }

    void TransactionBuffer::rollbackTransactionChunk(Transaction* transaction) {
        auto* lastTc = transaction->lastTc;
        if (unlikely(lastTc == nullptr))
            throw RedoLogException(50044, "trying to remove from empty buffer size: <null> elements: <null>");
        if (unlikely(lastTc->size < ROW_HEADER_TOTAL || lastTc->elements == 0))
            throw RedoLogException(50044, "trying to remove from empty buffer size: " + std::to_string(lastTc->size) +
                                          " elements: " + std::to_string(lastTc->elements));

        typeChunkSize const chunkSize = *reinterpret_cast<typeChunkSize*>(lastTc->buffer + lastTc->size - sizeof(typeChunkSize));
        lastTc->size -= chunkSize;
        --lastTc->elements;
        transaction->size -= chunkSize;

        if (likely(lastTc->elements > 0))
            return;

        transaction->lastTc = reinterpret_cast<TransactionChunk*>(ctx->swappedMemoryShrink(ctx->parserThread, transaction->xid));
    }

    void TransactionBuffer::mergeBlocks(uint8_t* mergeBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2) {
        memcpy(reinterpret_cast<void*>(mergeBuffer),
               reinterpret_cast<const void*>(redoLogRecord1->data()), redoLogRecord1->fieldSizesDelta);
        typePos pos = redoLogRecord1->fieldSizesDelta;
        typeField fieldCnt;
        typePos fieldPos1;
        typePos fieldPos2;

        if ((redoLogRecord1->flg & OpCode::FLG_LASTBUFFERSPLIT) != 0) {
            redoLogRecord1->flg &= ~OpCode::FLG_LASTBUFFERSPLIT;
            const typeSize size1 = ctx->read16(redoLogRecord1->data(redoLogRecord1->fieldSizesDelta + (redoLogRecord1->fieldCnt * 2)));
            const typeSize size2 = ctx->read16(redoLogRecord2->data(redoLogRecord2->fieldSizesDelta + 6));
            ctx->write16(const_cast<uint8_t*>(redoLogRecord2->data(redoLogRecord2->fieldSizesDelta + 6)), size1 + size2);
            --redoLogRecord1->fieldCnt;
        }

        // Field list
        fieldCnt = redoLogRecord1->fieldCnt + redoLogRecord2->fieldCnt - 2;
        ctx->write16(mergeBuffer + pos, fieldCnt);
        memcpy(reinterpret_cast<void*>(mergeBuffer + pos + 2),
               reinterpret_cast<const void*>(redoLogRecord1->data(redoLogRecord1->fieldSizesDelta + 2)), static_cast<size_t>(redoLogRecord1->fieldCnt * 2));
        memcpy(reinterpret_cast<void*>(mergeBuffer + pos + 2 + static_cast<ptrdiff_t>(redoLogRecord1->fieldCnt * 2)),
               reinterpret_cast<const void*>(redoLogRecord2->data(redoLogRecord2->fieldSizesDelta + 6)), (redoLogRecord2->fieldCnt * 2) - 4);
        pos += (((fieldCnt + 1) * 2) + 2) & (0xFFFC);
        fieldPos1 = pos;

        memcpy(reinterpret_cast<void*>(mergeBuffer + pos),
               reinterpret_cast<const void*>(redoLogRecord1->data(redoLogRecord1->fieldPos)), redoLogRecord1->size - redoLogRecord1->fieldPos);
        pos += (redoLogRecord1->size - redoLogRecord1->fieldPos + 3) & (0xFFFC);
        fieldPos2 = redoLogRecord2->fieldPos +
                    ((ctx->read16(redoLogRecord2->data(redoLogRecord2->fieldSizesDelta + 2)) + 3) & 0xFFFC) +
                    ((ctx->read16(redoLogRecord2->data(redoLogRecord2->fieldSizesDelta + 4)) + 3) & 0xFFFC);

        memcpy(reinterpret_cast<void*>(mergeBuffer + pos),
               reinterpret_cast<const void*>(redoLogRecord2->data(fieldPos2)), redoLogRecord2->size - fieldPos2);
        pos += (redoLogRecord2->size - fieldPos2 + 3) & (0xFFFC);

        redoLogRecord1->size = pos;
        redoLogRecord1->fieldCnt = fieldCnt;
        redoLogRecord1->fieldPos = fieldPos1;
        redoLogRecord1->dataExt = mergeBuffer;
        redoLogRecord1->flg |= redoLogRecord2->flg;
        if ((redoLogRecord1->flg & OpCode::FLG_MULTIBLOCKUNDOTAIL) != 0)
            redoLogRecord1->flg &= ~(OpCode::FLG_MULTIBLOCKUNDOHEAD | OpCode::FLG_MULTIBLOCKUNDOMID | OpCode::FLG_MULTIBLOCKUNDOTAIL);
    }

    void TransactionBuffer::checkpoint(Seq& minSequence, FileOffset& minFileOffset, Xid& minXid) {
        for (const auto& [_, transaction] : xidTransactionMap) {
            if (transaction->firstSequence < minSequence) {
                minSequence = transaction->firstSequence;
                minFileOffset = transaction->firstFileOffset;
                minXid = transaction->xid;
            } else if (transaction->firstSequence == minSequence && transaction->firstFileOffset < minFileOffset) {
                minFileOffset = transaction->firstFileOffset;
                minXid = transaction->xid;
            }
        }
    }

    void TransactionBuffer::addOrphanedLob(RedoLogRecord* redoLogRecord1) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::LOB)))
            ctx->logTrace(Ctx::TRACE::LOB, "id: " + redoLogRecord1->lobId.upper() + " page: " + std::to_string(redoLogRecord1->dba) +
                                           " can't match, offset: " + redoLogRecord1->fileOffset.toString());

        const LobKey lobKey(redoLogRecord1->lobId, redoLogRecord1->dba);

        if (orphanedLobs.find(lobKey) != orphanedLobs.end()) {
            ctx->warning(60009, "duplicate orphaned lob: " + redoLogRecord1->lobId.lower() + ", page: " +
                                std::to_string(redoLogRecord1->dba));
            return;
        }

        orphanedLobs.insert_or_assign(lobKey, allocateLob(redoLogRecord1));
    }

    uint8_t* TransactionBuffer::allocateLob(const RedoLogRecord* redoLogRecord1) {
        const typeTransactionSize lobSize = redoLogRecord1->size + sizeof(RedoLogRecord) + sizeof(typeTransactionSize);
        auto* data = new uint8_t[lobSize];
        *reinterpret_cast<typeTransactionSize*>(data) = lobSize;
        memcpy(reinterpret_cast<void*>(data + sizeof(typeTransactionSize)),
               reinterpret_cast<const void*>(redoLogRecord1), sizeof(RedoLogRecord));
        memcpy(reinterpret_cast<void*>(data + sizeof(typeTransactionSize) + sizeof(RedoLogRecord)),
               reinterpret_cast<const void*>(redoLogRecord1->data()), redoLogRecord1->size);

        auto* redoLogRecord1new = reinterpret_cast<RedoLogRecord*>(data + sizeof(typeTransactionSize));
        redoLogRecord1new->dataExt = data + sizeof(typeTransactionSize) + sizeof(RedoLogRecord);
        return data;
    }
}
