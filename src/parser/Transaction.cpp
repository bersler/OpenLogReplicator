/* Transaction from Oracle database
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

#include "../builder/Builder.h"
#include "../builder/SystemTransaction.h"
#include "../common/LobCtx.h"
#include "../common/OracleLob.h"
#include "../common/OracleTable.h"
#include "../common/RedoLogRecord.h"
#include "../common/XmlCtx.h"
#include "../common/exception/RedoLogException.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "OpCode0501.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    Transaction::Transaction(typeXid newXid, std::map<LobKey, uint8_t*>* newOrphanedLobs, XmlCtx* newXmlCtx) :
            deallocTc(nullptr),
            opCodes(0),
            mergeBuffer(nullptr),
            xmlCtx(newXmlCtx),
            xid(newXid),
            firstSequence(0),
            firstOffset(0),
            commitSequence(0),
            commitScn(0),
            firstTc(nullptr),
            lastTc(nullptr),
            commitTimestamp(0),
            begin(false),
            rollback(false),
            system(false),
            schema(false),
            shutdown(false),
            lastSplit(false),
            dump(false),
            size(0) {
        lobCtx.orphanedLobs = newOrphanedLobs;
    }

    void Transaction::add(const Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1) {
        log(metadata->ctx, "add ", redoLogRecord1);
        transactionBuffer->addTransactionChunk(this, redoLogRecord1);
        ++opCodes;
    }

    void Transaction::add(const Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2) {
        log(metadata->ctx, "add1", redoLogRecord1);
        log(metadata->ctx, "add2", redoLogRecord2);
        transactionBuffer->addTransactionChunk(this, redoLogRecord1, redoLogRecord2);
        ++opCodes;
    }

    void Transaction::rollbackLastOp(const Metadata* metadata, TransactionBuffer* transactionBuffer, const RedoLogRecord* redoLogRecord1,
                                     const RedoLogRecord* redoLogRecord2) {
        const Ctx* ctx = metadata->ctx;
        log(ctx, "rlb1", redoLogRecord1);
        log(ctx, "rlb2", redoLogRecord2);

        while (lastTc != nullptr && lastTc->size > 0 && opCodes > 0) {
            const uint64_t sizeLast = *(reinterpret_cast<uint64_t*>(lastTc->buffer + lastTc->size - TransactionBuffer::ROW_HEADER_TOTAL +
                    TransactionBuffer::ROW_HEADER_SIZE));
            // auto lastRedoLogRecord1 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size - sizeLast + ROW_HEADER_REDO1);
            const auto lastRedoLogRecord2 = reinterpret_cast<const RedoLogRecord*>(lastTc->buffer + lastTc->size - sizeLast +
                    TransactionBuffer::ROW_HEADER_REDO2);

            bool ok = false;
            switch (lastRedoLogRecord2->opCode) {
                case 0x0A02:
                case 0x0A08:
                case 0x0A12:
                case 0x1A02:
                    transactionBuffer->rollbackTransactionChunk(this);
                    --opCodes;
                    continue;

                case 0x0B05:
                    if (redoLogRecord1->opCode == 0x0B05)
                        ok = true;
                    break;
                case 0x0B02:
                    if (redoLogRecord1->opCode == 0x0B03)
                        ok = true;
                    break;
                case 0x0B03:
                    if (redoLogRecord1->opCode == 0x0B02)
                        ok = true;
                    break;
                case 0x0B06:
                    if (redoLogRecord1->opCode == 0x0B06)
                        ok = true;
                    break;
                case 0x0B08:
                    if (redoLogRecord1->opCode == 0x0B08)
                        ok = true;
                    break;
                case 0x0B0B:
                    if (redoLogRecord1->opCode == 0x0B0C)
                        ok = true;
                    break;
                case 0x0B0C:
                    if (redoLogRecord1->opCode == 0x0B0B)
                        ok = true;
                    break;
                case 0x0B16:
                    if (redoLogRecord1->opCode == 0x0B16)
                        ok = true;
                    break;
            }

            if (lastRedoLogRecord2->obj != redoLogRecord1->obj)
                ok = false;

            if (!ok) {
                ctx->warning(70003, "trying to rollback: " + std::to_string(lastRedoLogRecord2->opCode) + " with: " +
                                    std::to_string(redoLogRecord1->opCode) + ", offset: " + std::to_string(redoLogRecord1->dataOffset) + ", xid: " +
                                    xid.toString() + ", pos: 2");
                return;
            }

            transactionBuffer->rollbackTransactionChunk(this);
            --opCodes;
            return;
        }

        ctx->warning(70004, "rollback failed for " + std::to_string(redoLogRecord1->opCode) +
                            " empty buffer, offset: " + std::to_string(redoLogRecord1->dataOffset) + ", xid: " + xid.toString() + ", pos: 2");
    }

    void Transaction::rollbackLastOp(const Metadata* metadata, TransactionBuffer* transactionBuffer, const RedoLogRecord* redoLogRecord1) {
        log(metadata->ctx, "rlb ", redoLogRecord1);

        while (lastTc != nullptr && lastTc->size > 0 && opCodes > 0) {
            const uint64_t sizeLast = *(reinterpret_cast<const uint64_t*>(lastTc->buffer + lastTc->size - TransactionBuffer::ROW_HEADER_TOTAL +
                    TransactionBuffer::ROW_HEADER_SIZE));
            const auto lastRedoLogRecord1 = reinterpret_cast<const RedoLogRecord*>(lastTc->buffer + lastTc->size - sizeLast +
                    TransactionBuffer::ROW_HEADER_REDO1);
            const auto lastRedoLogRecord2 = reinterpret_cast<const RedoLogRecord*>(lastTc->buffer + lastTc->size - sizeLast +
                    TransactionBuffer::ROW_HEADER_REDO2);

            bool ok = false;
            switch (lastRedoLogRecord2->opCode) {
                case 0x0A02:
                case 0x0A08:
                case 0x0A12:
                case 0x1A02:
                    transactionBuffer->rollbackTransactionChunk(this);
                    --opCodes;
                    continue;

                case 0x0000:
                case 0x0B10:
                case 0x0513:
                case 0x0514:
                    ok = true;
                    break;
            }

            if (lastRedoLogRecord1->obj != redoLogRecord1->obj)
                ok = false;

            if (!ok) {
                metadata->ctx->warning(70003, "trying to rollback: " + std::to_string(lastRedoLogRecord2->opCode) + " with: " +
                                              std::to_string(redoLogRecord1->opCode) + ", offset: " + std::to_string(redoLogRecord1->dataOffset) +
                                              ", xid: " + xid.toString() + ", pos: 1");
                return;
            }

            transactionBuffer->rollbackTransactionChunk(this);
            --opCodes;
            return;
        }

        metadata->ctx->warning(70004, "rollback failed for " + std::to_string(redoLogRecord1->opCode) +
                                      " empty buffer, offset: " + std::to_string(redoLogRecord1->dataOffset) + ", xid: " + xid.toString() + ", pos: 1");
    }

    void Transaction::flush(Metadata* metadata, TransactionBuffer* transactionBuffer, Builder* builder, typeScn lwnScn) {
        bool opFlush;
        deallocTc = nullptr;
        uint64_t maxMessageMb = builder->getMaxMessageMb();
        std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
        std::unique_lock<std::mutex> lckSchema(metadata->mtxSchema, std::defer_lock);

        if (opCodes == 0 || rollback)
            return;
        if (unlikely(metadata->ctx->trace & Ctx::TRACE_TRANSACTION))
            metadata->ctx->logTrace(Ctx::TRACE_TRANSACTION, toString());

        if (system) {
            lckSchema.lock();

            if (unlikely(builder->systemTransaction != nullptr))
                throw RedoLogException(50056, "system transaction already active");
            builder->systemTransaction = new SystemTransaction(builder, metadata);
            metadata->schema->scn = commitScn;
        }
        builder->processBegin(xid, commitScn, lwnScn, &attributes);

        uint64_t type = 0;
        std::deque<const RedoLogRecord*> redo1;
        std::deque<const RedoLogRecord*> redo2;

        TransactionChunk* tc = firstTc;
        while (tc != nullptr) {
            uint64_t pos = 0;
            for (uint64_t i = 0; i < tc->elements; ++i) {
                typeOp2 op = *(reinterpret_cast<typeOp2*>(tc->buffer + pos));

                RedoLogRecord* redoLogRecord1 = reinterpret_cast<RedoLogRecord*>(tc->buffer + pos + TransactionBuffer::ROW_HEADER_REDO1);
                RedoLogRecord* redoLogRecord2 = reinterpret_cast<RedoLogRecord*>(tc->buffer + pos + TransactionBuffer::ROW_HEADER_REDO2);
                log(metadata->ctx, "flu1", redoLogRecord1);
                log(metadata->ctx, "flu2", redoLogRecord2);

                redoLogRecord1->dataExt = tc->buffer + pos + TransactionBuffer::ROW_HEADER_DATA;
                redoLogRecord2->dataExt = tc->buffer + pos + TransactionBuffer::ROW_HEADER_DATA + redoLogRecord1->size;
                pos += redoLogRecord1->size + redoLogRecord2->size + TransactionBuffer::ROW_HEADER_TOTAL;

                if (unlikely(metadata->ctx->trace & Ctx::TRACE_TRANSACTION))
                    metadata->ctx->logTrace(Ctx::TRACE_TRANSACTION, std::to_string(redoLogRecord1->size) + ":" +
                                                                    std::to_string(redoLogRecord2->size) + " fb: " +
                                                                    std::to_string(static_cast<uint64_t>(redoLogRecord1->fb)) + ":" +
                                                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->fb)) + " op: " + std::to_string(op) +
                                                                    " scn: " + std::to_string(redoLogRecord1->scn) + " subscn: " +
                                                                    std::to_string(redoLogRecord1->subScn) + " scnrecord: " +
                                                                    std::to_string(redoLogRecord1->scnRecord) + " obj: " +
                                                                    std::to_string(redoLogRecord1->obj) + " dataobj: " + std::to_string(redoLogRecord1->dataObj) +
                                                                    " flg1: " + std::to_string(redoLogRecord1->flg) + " flg2: " +
                                                                    std::to_string(redoLogRecord2->flg) +
                                                                    // " uba1: " + PRINTUBA(redoLogRecord1->uba) +
                                                                    // " uba2: " + PRINTUBA(redoLogRecord2->uba) <<
                                                                    " bdba1: " + std::to_string(redoLogRecord1->bdba) + "." +
                                                                    std::to_string(static_cast<uint64_t>(redoLogRecord1->slot)) + " bdba2: " +
                                                                    std::to_string(redoLogRecord2->bdba) + "." +
                                                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->slot)) +
                                                                    " supp: (" + std::to_string(static_cast<uint64_t>(redoLogRecord1->suppLogFb)) + ", " +
                                                                    std::to_string(redoLogRecord1->suppLogCC) + ", " +
                                                                    std::to_string(redoLogRecord1->suppLogBefore) + ", " +
                                                                    std::to_string(redoLogRecord1->suppLogAfter) + ", " +
                                                                    std::to_string(redoLogRecord1->suppLogBdba) + "." +
                                                                    std::to_string(redoLogRecord1->suppLogSlot) + ")");

                // Cluster key
                if ((redoLogRecord1->fb & RedoLogRecord::FB_K) != 0 || (redoLogRecord2->fb & RedoLogRecord::FB_K) != 0)
                    continue;

                // Partition move
                if ((redoLogRecord1->suppLogFb & RedoLogRecord::FB_K) != 0 || (redoLogRecord2->suppLogFb & RedoLogRecord::FB_K) != 0)
                    continue;

                opFlush = false;
                switch (op) {
                    case 0x05010000:
                        // Single undo - ignore
                    case 0x05010513:
                    case 0x05010514:
                        // Session information
                        break;

                    case 0x1A020000: {
                        // LOB idx
                        const OracleLob* lob = metadata->schema->checkLobDict(redoLogRecord1->obj);
                        if (lob != nullptr) {
                            if (unlikely(metadata->ctx->trace & Ctx::TRACE_LOB))
                                metadata->ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord1->lobId.lower() + " xid: " + xid.toString() +
                                                                        " obj: " + std::to_string(lob->obj) + " op: " + std::to_string(op) + " dba: " +
                                                                        std::to_string(redoLogRecord1->dba) + " page: " + std::to_string(redoLogRecord1->lobPageNo) +
                                                                        " col: " + std::to_string(lob->intCol) + " table: " + lob->table->owner + "." +
                                                                        lob->table->name + " lobj: " + std::to_string(lob->lObj) + " IDX");
                        }
                    }
                        break;

                    case 0x13010000:
                    case 0x1A060000: {
                        // LOB data
                        const OracleLob* lob = metadata->schema->checkLobDict(redoLogRecord1->obj);
                        if (lob != nullptr) {
                            if (unlikely(metadata->ctx->trace & Ctx::TRACE_LOB))
                                metadata->ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord1->lobId.lower() + " xid: " + xid.toString() + " obj: " +
                                                                        std::to_string(lob->obj) + " op: " + std::to_string(op) + " dba: " +
                                                                        std::to_string(redoLogRecord1->dba) + " page: " + std::to_string(redoLogRecord1->lobPageNo) +
                                                                        " col: " + std::to_string(lob->intCol) + " table: " + lob->table->owner + "." +
                                                                        lob->table->name +
                                                                        " lobj: " + std::to_string(lob->lObj));
                        }
                    }
                        break;

                    case 0x05011A02:
                        // LOB index 12+ and LOB redo
                        if (unlikely(metadata->ctx->trace & Ctx::TRACE_LOB_DATA)) {
                            std::ostringstream ss;
                            for (typeSize j = 0; j < redoLogRecord2->indKeyDataSize; ++j) {
                                ss << " " << std::setfill('0') << std::setw(2) << std::hex <<
                                   static_cast<uint64_t>(redoLogRecord2->data()[redoLogRecord2->indKeyData + j]);
                            }

                            metadata->ctx->logTrace(Ctx::TRACE_LOB_DATA, "index: " + ss.str() + " code: " +
                                                                         std::to_string(static_cast<uint64_t>(redoLogRecord2->indKeyDataCode)));
                        }

                        if (redoLogRecord2->dba0 != 0) {
                            lobCtx.orderList(redoLogRecord2->dba, redoLogRecord2->dba0);

                            if (redoLogRecord2->dba1 != 0) {
                                lobCtx.orderList(redoLogRecord2->dba0, redoLogRecord2->dba1);

                                if (redoLogRecord2->dba2 != 0) {
                                    lobCtx.orderList(redoLogRecord2->dba1, redoLogRecord2->dba2);

                                    if (redoLogRecord2->dba3 != 0)
                                        lobCtx.orderList(redoLogRecord2->dba2, redoLogRecord2->dba3);
                                }
                            }
                        }

                        switch (redoLogRecord2->indKeyDataCode) {
                            case OpCode::KDLI_CODE_LMAP:
                            case OpCode::KDLI_CODE_LOAD_ITREE:
                                lobCtx.setList(redoLogRecord2->dba, redoLogRecord2->data() + redoLogRecord2->indKeyData,
                                               redoLogRecord2->indKeyDataSize);
                                break;

                            case OpCode::KDLI_CODE_IMAP:
                            case OpCode::KDLI_CODE_ALMAP:
                                lobCtx.appendList(metadata->ctx, redoLogRecord2->dba, redoLogRecord2->data() + redoLogRecord2->indKeyData);
                                break;

                            case OpCode::KDLI_CODE_FILL:
                                if (unlikely(metadata->ctx->trace & Ctx::TRACE_LOB))
                                    metadata->ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + xid.toString() +
                                                                            " obj: " + std::to_string(redoLogRecord2->dataObj) + " op: " +
                                                                            std::to_string(redoLogRecord2->opCode) + "     dba: " +
                                                                            std::to_string(redoLogRecord2->dba) + " page: " +
                                                                            std::to_string(redoLogRecord2->lobPageNo) + " pg: " +
                                                                            std::to_string(redoLogRecord2->lobPageSize));

                                lobCtx.addLob(metadata->ctx, redoLogRecord2->lobId, redoLogRecord2->dba, redoLogRecord2->lobOffset,
                                              transactionBuffer->allocateLob(redoLogRecord2), xid, redoLogRecord2->lobData);
                                break;
                        }
                        break;

                    case 0x05010A02:
                        // Insert leaf row
                    case 0x05010A08:
                        // Init header
                    case 0x05010A12: {
                        // Update key data in row
                        const OracleLob* lob = metadata->schema->checkLobIndexDict(redoLogRecord2->dataObj);
                        if (lob == nullptr) {
                            metadata->ctx->warning(60016, "LOB is null for (obj: " + std::to_string(redoLogRecord2->obj) +
                                                          ", dataobj: " + std::to_string(redoLogRecord2->dataObj) + ", offset: " +
                                                          std::to_string(redoLogRecord1->dataOffset) + ", xid: " + xid.toString());
                            break;
                        }

                        std::ostringstream pages;
                        typeSize start = 16;
                        typeDba pageNo = redoLogRecord2->lobPageNo;
                        if (pageNo > 0)
                            start = 0;

                        for (typeSize j = start; j < redoLogRecord2->indKeyDataSize; j += 4) {
                            typeDba page = metadata->ctx->read32Big(redoLogRecord2->data() + redoLogRecord2->indKeyData + j);
                            if (page > 0) {
                                lobCtx.setPage(redoLogRecord2->lobId, page, pageNo, xid, redoLogRecord1->dataOffset);
                                pages << " [0x" << std::setfill('0') << std::setw(8) << std::hex << page << "]";
                            }
                            ++pageNo;
                        }

                        if (op == 0x05010A12 && redoLogRecord2->lobPageNo == 0) {
                            lobCtx.setSize(redoLogRecord2->lobId, redoLogRecord2->lobSizePages, redoLogRecord2->lobSizeRest);
                        }

                        if (unlikely(metadata->ctx->trace & Ctx::TRACE_LOB))
                            metadata->ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + xid.toString() + " obj: " +
                                                                    std::to_string(redoLogRecord1->obj) + " op: " + std::to_string(op) + " dba: " +
                                                                    std::to_string(redoLogRecord2->dba) + " page: " + std::to_string(redoLogRecord2->lobPageNo) +
                                                                    " col: " + std::to_string(lob->intCol) + " table: " + lob->table->owner + "." +
                                                                    lob->table->name + " lobj: " + std::to_string(lob->lObj) + " - INDEX: " + pages.str() +
                                                                    " PAGES: " + std::to_string(redoLogRecord2->lobSizePages) + " REST: " +
                                                                    std::to_string(redoLogRecord2->lobSizeRest));
                        break;
                    }

                    case 0x05010B02:
                        // Insert leaf row
                    case 0x05010B03:
                        // Delete row piece
                    case 0x05010B05:
                        // Update row piece
                    case 0x05010B06:
                        // Overwrite row piece
                    case 0x05010B08:
                        // Change row forwarding address
                    case 0x05010B10:
                        // Supp log for update
                    case 0x05010B16:
                        // Logminer support - KDOCMP
                        redoLogRecord2->suppLogAfter = redoLogRecord1->suppLogAfter;

                        if (type == 0) {
                            if (op == 0x05010B02)
                                type = Builder::TRANSACTION_INSERT;
                            else if (op == 0x05010B03)
                                type = Builder::TRANSACTION_DELETE;
                            else
                                type = Builder::TRANSACTION_UPDATE;
                        } else if (type == Builder::TRANSACTION_INSERT) {
                            if (op == 0x05010B03 || op == 0x05010B05 || op == 0x05010B06 || op == 0x05010B08)
                                type = Builder::TRANSACTION_UPDATE;
                        } else if (type == Builder::TRANSACTION_DELETE) {
                            if (op == 0x05010B02 || op == 0x05010B05 || op == 0x05010B06 || op == 0x05010B08)
                                type = Builder::TRANSACTION_UPDATE;
                        }

                        if (redo1.empty()) {
                            if (redoLogRecord1->suppLogBdba == 0 && op == 0x05010B16 && (redoLogRecord1->suppLogFb & RedoLogRecord::FB_L) == 0) {
                                log(metadata->ctx, "nul1", redoLogRecord1);
                                log(metadata->ctx, "nul2", redoLogRecord2);
                                // Ignore
                            } else {
                                redo1.push_back(redoLogRecord1);
                                redo2.push_back(redoLogRecord2);
                            }
                        } else {
                            if (redo1.back()->suppLogBdba == redoLogRecord1->suppLogBdba && redo1.back()->suppLogSlot == redoLogRecord1->suppLogSlot &&
                                    redo1.front()->obj == redoLogRecord1->obj && redo2.front()->obj == redoLogRecord2->obj) {
                                if (type == Builder::TRANSACTION_INSERT) {
                                    redo1.push_front(redoLogRecord1);
                                    redo2.push_front(redoLogRecord2);
                                } else {
                                    if (op == 0x05010B06 && redo2.back()->opCode == 0x0B02) {
                                        const RedoLogRecord* prev = redo1.back();
                                        redo1.pop_back();
                                        redo1.push_back(redoLogRecord1);
                                        redo1.push_back(prev);
                                        prev = redo2.back();
                                        redo2.pop_back();
                                        redo2.push_back(redoLogRecord2);
                                        redo2.push_back(prev);
                                    } else {
                                        redo1.push_back(redoLogRecord1);
                                        redo2.push_back(redoLogRecord2);
                                    }
                                }
                            } else {
                                metadata->ctx->warning(60017, "minimal supplemental log missing or redo log inconsistency for transaction " +
                                                              xid.toString());
                            }
                        }

                        if ((redoLogRecord1->suppLogFb & RedoLogRecord::FB_L) != 0) {
                            builder->processDml(redo2.front()->scnRecord, commitSequence, commitTimestamp.toEpoch(metadata->ctx->hostTimezone),
                                                &lobCtx, xmlCtx, redo1, redo2, type, system, schema, dump);
                            opFlush = true;
                        }
                        break;

                    case 0x05010B0B:
                        // Insert multiple rows
                        builder->processInsertMultiple(redoLogRecord2->scnRecord, commitSequence,
                                                       commitTimestamp.toEpoch(metadata->ctx->hostTimezone), &lobCtx, xmlCtx, redoLogRecord1,
                                                       redoLogRecord2, system, schema, dump);
                        opFlush = true;
                        break;

                    case 0x05010B0C:
                        // Delete multiple rows
                        builder->processDeleteMultiple(redoLogRecord2->scnRecord, commitSequence,
                                                       commitTimestamp.toEpoch(metadata->ctx->hostTimezone), &lobCtx, xmlCtx, redoLogRecord1,
                                                       redoLogRecord2, system, schema, dump);
                        opFlush = true;
                        break;

                    case 0x18010000:
                        // DDL operation
                        builder->processDdlHeader(commitScn, commitSequence, commitTimestamp.toEpoch(metadata->ctx->hostTimezone),
                                                  redoLogRecord1);
                        opFlush = true;
                        break;

                    default:
                        // Should not happen
                        throw RedoLogException(50057, "unknown op code " + std::to_string(op) + ", offset: " +
                                                      std::to_string(redoLogRecord1->dataOffset));
                }

                // Split very big transactions
                if (maxMessageMb > 0 && builder->builderSize() + TransactionChunk::DATA_BUFFER_SIZE > maxMessageMb * 1024 * 1024) {
                    metadata->ctx->warning(60015, "big transaction divided (forced commit after " + std::to_string(builder->builderSize()) +
                                                  " bytes), xid: " + xid.toString());

                    if (system) {
                        if (unlikely(metadata->ctx->trace & Ctx::TRACE_SYSTEM))
                            metadata->ctx->logTrace(Ctx::TRACE_SYSTEM, "commit");
                        builder->systemTransaction->commit(commitScn);
                        delete builder->systemTransaction;
                        builder->systemTransaction = nullptr;

                        if (unlikely(metadata->ctx->trace & Ctx::TRACE_SYSTEM))
                            metadata->ctx->logTrace(Ctx::TRACE_SYSTEM, "begin");
                        builder->systemTransaction = new SystemTransaction(builder, metadata);
                    }

                    builder->processCommit(commitScn, commitSequence, commitTimestamp.toEpoch(metadata->ctx->hostTimezone));
                    builder->processBegin(xid, commitScn, lwnScn, &attributes);
                }

                if (opFlush) {
                    redo1.clear();
                    redo2.clear();
                    type = 0;

                    while (deallocTc != nullptr) {
                        TransactionChunk* nextTc = deallocTc->next;
                        transactionBuffer->deleteTransactionChunk(deallocTc);
                        deallocTc = nextTc;
                    }
                }
            }

            TransactionChunk* nextTc = tc->next;
            tc->next = deallocTc;
            deallocTc = tc;
            tc = nextTc;
            firstTc = tc;
        }

        while (deallocTc != nullptr) {
            TransactionChunk* nextTc = deallocTc->next;
            transactionBuffer->deleteTransactionChunk(deallocTc);
            deallocTc = nextTc;
        }

        firstTc = nullptr;
        lastTc = nullptr;
        opCodes = 0;

        if (system) {
            builder->systemTransaction->commit(commitScn);
            delete builder->systemTransaction;
            builder->systemTransaction = nullptr;
            metadata->schema->scn = commitScn;

            // Unlock schema
            lckSchema.unlock();
        }
        builder->processCommit(commitScn, commitSequence, commitTimestamp.toEpoch(metadata->ctx->hostTimezone));
    }

    void Transaction::purge(TransactionBuffer* transactionBuffer) {
        if (firstTc != nullptr) {
            transactionBuffer->deleteTransactionChunks(firstTc);
            firstTc = nullptr;
            lastTc = nullptr;
        }

        while (deallocTc != nullptr) {
            TransactionChunk* nextTc = deallocTc->next;
            transactionBuffer->deleteTransactionChunk(deallocTc);
            deallocTc = nextTc;
        }
        deallocTc = nullptr;

        if (mergeBuffer != nullptr) {
            delete[] mergeBuffer;
            mergeBuffer = nullptr;
        }

        lobCtx.purge();

        size = 0;
        opCodes = 0;
    }

    std::string Transaction::toString() const {
        uint64_t tcCount = 0;
        TransactionChunk* tc = firstTc;
        while (tc != nullptr) {
            ++tcCount;
            tc = tc->next;
        }

        std::ostringstream ss;
        ss << "scn: " << std::dec << commitScn <<
           " seq: " << std::dec << firstSequence <<
           " offset: " << std::dec << firstOffset <<
           " xid: " << xid.toString() <<
           " flags: " << std::dec << begin << "/" << rollback << "/" << system <<
           " op: " << std::dec << opCodes <<
           " chunks: " << std::dec << tcCount <<
           " sz: " << std::dec << size;
        return ss.str();
    }
}
