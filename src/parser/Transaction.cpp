/* Transaction from Oracle database
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/LobCtx.h"
#include "../common/OracleLob.h"
#include "../common/OracleTable.h"
#include "../common/RedoLogException.h"
#include "../common/RedoLogRecord.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "OpCode0501.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    Transaction::Transaction(typeXid newXid, std::map<LobKey, uint8_t*>* newOrphanedLobs) :
        deallocTc(nullptr),
        opCodes(0),
        mergeBuffer(nullptr),
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

    void Transaction::add(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1) {
        log(metadata->ctx, "add ", redoLogRecord1);
        transactionBuffer->addTransactionChunk(this, redoLogRecord1);
        ++opCodes;
    }

    void Transaction::add(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        log(metadata->ctx, "add1", redoLogRecord1);
        log(metadata->ctx, "add2", redoLogRecord2);
        transactionBuffer->addTransactionChunk(this, redoLogRecord1, redoLogRecord2);
        ++opCodes;
    }

    void Transaction::rollbackLastOp(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        Ctx* ctx = metadata->ctx;
        log(ctx, "rlb1", redoLogRecord1);
        log(ctx, "rlb2", redoLogRecord2);

        while (lastTc != nullptr && lastTc->size > 0 && opCodes > 0) {
            uint64_t lengthLast = *(reinterpret_cast<uint64_t*>(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
            // auto lastRedoLogRecord1 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO1);
            auto lastRedoLogRecord2 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO2);

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
                ctx->warning(70003, "trying to rollback: " + std::to_string(lastRedoLogRecord2->opCode) +  " with: " +
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

    void Transaction::rollbackLastOp(Metadata* metadata, TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1) {
        log(metadata->ctx, "rlb ", redoLogRecord1);

        while (lastTc != nullptr && lastTc->size > 0 && opCodes > 0) {
            uint64_t lengthLast = *(reinterpret_cast<uint64_t*>(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
            auto lastRedoLogRecord1 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO1);
            auto lastRedoLogRecord2 = reinterpret_cast<RedoLogRecord*>(lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO2);

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
                metadata->ctx->warning(70003, "trying to rollback: " + std::to_string(lastRedoLogRecord2->opCode) +  " with: " +
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
        bool opFlush = false;
        deallocTc = nullptr;
        uint64_t maxMessageMb = builder->getMaxMessageMb();
        std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
        std::unique_lock<std::mutex> lckSchema(metadata->mtxSchema, std::defer_lock);

        if (opCodes == 0 || rollback)
            return;
        if (metadata->ctx->trace & TRACE_TRANSACTION)
            metadata->ctx->logTrace(TRACE_TRANSACTION, toString());

        if (system) {
            lckSchema.lock();

            if (builder->systemTransaction != nullptr)
                throw RedoLogException(50056, "system transaction already active");
            builder->systemTransaction = new SystemTransaction(builder, metadata);
            metadata->schema->scn = commitScn;
        }
        builder->processBegin(xid, commitScn, lwnScn, &attributes);

        uint64_t pos;
        uint64_t type = 0;
        RedoLogRecord* first1 = nullptr;
        RedoLogRecord* first2 = nullptr;
        RedoLogRecord* last1 = nullptr;
        RedoLogRecord* last2 = nullptr;

        TransactionChunk* tc = firstTc;
        while (tc != nullptr) {
            pos = 0;
            for (uint64_t i = 0; i < tc->elements; ++i) {
                typeOp2 op = *(reinterpret_cast<typeOp2*>(tc->buffer + pos));

                RedoLogRecord* redoLogRecord1 = reinterpret_cast<RedoLogRecord*>(tc->buffer + pos + ROW_HEADER_REDO1);
                RedoLogRecord* redoLogRecord2 = reinterpret_cast<RedoLogRecord*>(tc->buffer + pos + ROW_HEADER_REDO2);
                log(metadata->ctx, "flu1", redoLogRecord1);
                log(metadata->ctx, "flu2", redoLogRecord2);

                redoLogRecord1->data = tc->buffer + pos + ROW_HEADER_DATA;
                redoLogRecord2->data = tc->buffer + pos + ROW_HEADER_DATA + redoLogRecord1->length;
                pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;

                if (metadata->ctx->trace & TRACE_TRANSACTION)
                    metadata->ctx->logTrace(TRACE_TRANSACTION, std::to_string(redoLogRecord1->length) + ":" +
                                            std::to_string(redoLogRecord2->length) + " fb: " +
                                            std::to_string(static_cast<uint64_t>(redoLogRecord1->fb)) + ":" +
                                            std::to_string(static_cast<uint64_t>(redoLogRecord2->fb)) + " op: " + std::to_string(op) + " scn: " +
                                            std::to_string(redoLogRecord1->scn) + " subscn: " + std::to_string(redoLogRecord1->subScn) +
                                            " scnrecord: " + std::to_string(redoLogRecord1->scnRecord) + " obj: " +
                                            std::to_string(redoLogRecord1->obj) + " dataobj: " + std::to_string(redoLogRecord1->dataObj) +
                                            " flg1: " + std::to_string(redoLogRecord1->flg) + " flg2: " + std::to_string(redoLogRecord2->flg) +
                                            // " uba1: " + PRINTUBA(redoLogRecord1->uba) +
                                            // " uba2: " + PRINTUBA(redoLogRecord2->uba) <<
                                            " bdba1: " + std::to_string(redoLogRecord1->bdba) + "." +
                                            std::to_string(static_cast<uint64_t>(redoLogRecord1->slot)) + " bdba2: " +
                                            std::to_string(redoLogRecord2->bdba) + "." + std::to_string(static_cast<uint64_t>(redoLogRecord2->slot)) +
                                            " supp: (" + std::to_string(static_cast<uint64_t>(redoLogRecord1->suppLogFb)) + ", " +
                                            std::to_string(static_cast<uint64_t>(redoLogRecord1->suppLogType)) + ", " +
                                            std::to_string(redoLogRecord1->suppLogCC) + ", " + std::to_string(redoLogRecord1->suppLogBefore) + ", " +
                                            std::to_string(redoLogRecord1->suppLogAfter) + ", " + std::to_string(redoLogRecord1->suppLogBdba) + "." +
                                            std::to_string(redoLogRecord1->suppLogSlot) + ")");

                // Cluster key
                if ((redoLogRecord1->fb & FB_K) != 0 || (redoLogRecord2->fb & FB_K) != 0)
                    continue;

                // Partition move
                if ((redoLogRecord1->suppLogFb & FB_K) != 0 || (redoLogRecord2->suppLogFb & FB_K) != 0)
                    continue;

                opFlush = false;
                switch (op) {
                    // Single undo - ignore
                    case 0x05010000:
                    // Session information
                    case 0x05010513:
                    case 0x05010514:
                        break;

                    // LOB idx
                    case 0x1A020000:
                    {
                        OracleLob* lob = metadata->schema->checkLobDict(redoLogRecord1->obj);
                        if (lob != nullptr) {
                            if (metadata->ctx->trace & TRACE_LOB)
                                metadata->ctx->logTrace(TRACE_LOB, "id: " + redoLogRecord1->lobId.lower() +  " xid: " + xid.toString() +
                                                        " obj: " + std::to_string(lob->obj) + " op: " + std::to_string(op) + " dba: " +
                                                        std::to_string(redoLogRecord1->dba) + " page: " + std::to_string(redoLogRecord1->lobPageNo) +
                                                        " col: " + std::to_string(lob->intCol) + " table: " + lob->table->owner + "." + lob->table->name +
                                                        " lobj: " + std::to_string(lob->lObj) + " IDX");
                        }
                    }
                    break;

                    // LOB data
                    case 0x13010000:
                    case 0x1A060000:
                    {
                        OracleLob* lob = metadata->schema->checkLobDict(redoLogRecord1->obj);
                        if (lob != nullptr) {
                            if (metadata->ctx->trace & TRACE_LOB)
                                metadata->ctx->logTrace(TRACE_LOB, "id: " + redoLogRecord1->lobId.lower() + " xid: " + xid.toString() + " obj: " +
                                                        std::to_string(lob->obj) + " op: " + std::to_string(op) + " dba: " +
                                                        std::to_string(redoLogRecord1->dba) + " page: " + std::to_string(redoLogRecord1->lobPageNo) +
                                                        " col: " + std::to_string(lob->intCol) + " table: " + lob->table->owner + "." + lob->table->name +
                                                        " lobj: " + std::to_string(lob->lObj));
                        }
                    }
                    break;

                    // LOB index 12+ and LOB redo
                    case 0x05011A02:
                        if (metadata->ctx->trace & TRACE_LOB_DATA) {
                            std::ostringstream ss;
                            for (uint64_t j = 0; j < redoLogRecord2->indKeyDataLength; ++j) {
                                ss << " " << std::setfill('0') << std::setw(2) << std::hex <<
                                   static_cast<uint64_t>(redoLogRecord2->data[redoLogRecord2->indKeyData + j]);
                            }

                            metadata->ctx->logTrace(TRACE_LOB_DATA, "index: " + ss.str() + " code: " +
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
                            case KDLI_CODE_LMAP:
                            case KDLI_CODE_LOAD_ITREE:
                                lobCtx.setList(redoLogRecord2->dba, redoLogRecord2->data + redoLogRecord2->indKeyData,
                                               redoLogRecord2->indKeyDataLength);
                                break;

                            case KDLI_CODE_IMAP:
                            case KDLI_CODE_ALMAP:
                                lobCtx.appendList(metadata->ctx, redoLogRecord2->dba, redoLogRecord2->data + redoLogRecord2->indKeyData);
                                break;

                            case KDLI_CODE_FILL:
                                if (metadata->ctx->trace & TRACE_LOB)
                                    metadata->ctx->logTrace(TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + xid.toString() +
                                                            " obj: " + std::to_string(redoLogRecord2->dataObj) +  " op: " +
                                                            std::to_string(redoLogRecord2->opCode) + "     dba: " +
                                                            std::to_string(redoLogRecord2->dba) + " page: " +
                                                            std::to_string(redoLogRecord2->lobPageNo) + " pg: " +
                                                            std::to_string(redoLogRecord2->lobPageSize));

                                lobCtx.addLob(metadata->ctx, redoLogRecord2->lobId, redoLogRecord2->dba, redoLogRecord2->lobOffset,
                                              transactionBuffer->allocateLob(redoLogRecord2), xid, redoLogRecord2->lobData);
                                break;
                        }
                        break;

                    // Insert leaf row
                    case 0x05010A02:
                    // Init header
                    case 0x05010A08:
                    // Update key data in row
                    case 0x05010A12:
                    {
                        OracleLob* lob = metadata->schema->checkLobIndexDict(redoLogRecord2->dataObj);
                        if (lob == nullptr) {
                            metadata->ctx->warning(60016, "LOB is null for (obj: " + std::to_string(redoLogRecord2->obj) +
                                                   ", dataobj: " + std::to_string(redoLogRecord2->dataObj) + ", offset: " +
                                                   std::to_string(redoLogRecord1->dataOffset) + ", xid: " + xid.toString());
                            break;
                        }

                        std::ostringstream pages;
                        uint64_t start = 16;
                        uint32_t pageNo = redoLogRecord2->lobPageNo;
                        if (pageNo > 0)
                            start = 0;

                        for (uint64_t j = start; j < redoLogRecord2->indKeyDataLength; j += 4) {
                            typeDba page = metadata->ctx->read32Big(redoLogRecord2->data + redoLogRecord2->indKeyData + j);
                            if (page > 0) {
                                lobCtx.setPage(redoLogRecord2->lobId, page, pageNo, xid, redoLogRecord1->dataOffset);
                                pages << " [0x" << std::setfill('0') << std::setw(8) << std::hex << page << "]";
                            }
                            ++pageNo;
                        }

                        if (op == 0x05010A12 && redoLogRecord2->lobPageNo == 0) {
                            lobCtx.setLength(redoLogRecord2->lobId, redoLogRecord2->lobLengthPages, redoLogRecord2->lobLengthRest);
                        }

                        if (metadata->ctx->trace & TRACE_LOB)
                            metadata->ctx->logTrace(TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + xid.toString() + " obj: " +
                                                    std::to_string(redoLogRecord1->obj) + " op: " + std::to_string(op) + " dba: " +
                                                    std::to_string(redoLogRecord2->dba) + " page: " + std::to_string(redoLogRecord2->lobPageNo) +
                                                    " col: " + std::to_string(lob->intCol) + " table: " + lob->table->owner + "." + lob->table->name +
                                                    " lobj: " + std::to_string(lob->lObj) + " - INDEX: " + pages.str() + " PAGES: " +
                                                    std::to_string(redoLogRecord2->lobLengthPages) + " REST: " +
                                                    std::to_string(redoLogRecord2->lobLengthRest));
                        break;
                    }

                    case 0x05010B02:
                    // Delete row piece
                    case 0x05010B03:
                    // Update row piece
                    case 0x05010B05:
                    // Overwrite row piece
                    case 0x05010B06:
                    // Change row forwarding address
                    case 0x05010B08:
                    // Supp log for update
                    case 0x05010B10:
                    // Logminer support - KDOCMP
                    case 0x05010B16:
                        redoLogRecord2->suppLogAfter = redoLogRecord1->suppLogAfter;

                        if (type == 0) {
                            if (op == 0x05010B02)
                                type = TRANSACTION_INSERT;
                            else if (op == 0x05010B03)
                                type = TRANSACTION_DELETE;
                            else
                                type = TRANSACTION_UPDATE;
                        } else if (type == TRANSACTION_INSERT) {
                            if (op == 0x05010B03 || op == 0x05010B05 || op == 0x05010B06 || op == 0x05010B08)
                                type = TRANSACTION_UPDATE;
                        } else if (type == TRANSACTION_DELETE) {
                            if (op == 0x05010B02 || op == 0x05010B05 || op == 0x05010B06 || op == 0x05010B08)
                                type = TRANSACTION_UPDATE;
                        }

                        if (first1 == nullptr) {
                            if (redoLogRecord1->suppLogBdba == 0 && op == 0x05010B16 && (redoLogRecord1->suppLogFb & FB_L) == 0) {
                                log(metadata->ctx, "nul1", redoLogRecord1);
                                log(metadata->ctx, "nul2", redoLogRecord2);
                                // Ignore
                            } else {
                                first1 = redoLogRecord1;
                                first2 = redoLogRecord2;
                                last1 = redoLogRecord1;
                                last2 = redoLogRecord2;
                            }
                        } else {
                            if (last1->suppLogBdba == redoLogRecord1->suppLogBdba && last1->suppLogSlot == redoLogRecord1->suppLogSlot &&
                                    first1->obj == redoLogRecord1->obj && first2->obj == redoLogRecord2->obj) {
                                if (type == TRANSACTION_INSERT) {
                                    redoLogRecord1->next = first1;
                                    redoLogRecord2->next = first2;
                                    first1->prev = redoLogRecord1;
                                    first2->prev = redoLogRecord2;
                                    first1 = redoLogRecord1;
                                    first2 = redoLogRecord2;
                                } else {
                                    if (op == 0x05010B06 && last2->opCode == 0x0B02) {
                                        if (last1->prev == nullptr) {
                                            first1 = redoLogRecord1;
                                            first2 = redoLogRecord2;
                                            first1->next = last1;
                                            first2->next = last2;
                                            last1->prev = first1;
                                            last2->prev = first2;
                                        } else {
                                            redoLogRecord1->prev = last1->prev;
                                            redoLogRecord2->prev = last2->prev;
                                            redoLogRecord1->next = last1;
                                            redoLogRecord2->next = last2;
                                            last1->prev->next = redoLogRecord1;
                                            last2->prev->next = redoLogRecord2;
                                            last1->prev = redoLogRecord1;
                                            last2->prev = redoLogRecord2;
                                        }
                                    } else {
                                        last1->next = redoLogRecord1;
                                        last2->next = redoLogRecord2;
                                        redoLogRecord1->prev = last1;
                                        redoLogRecord2->prev = last2;
                                        last1 = redoLogRecord1;
                                        last2 = redoLogRecord2;
                                    }
                                }
                            } else {
                                metadata->ctx->warning(60017, "minimal supplemental log missing or redo log inconsistency for transaction " +
                                                       xid.toString());
                            }
                        }

                        if ((redoLogRecord1->suppLogFb & FB_L) != 0) {
                            builder->processDml(first2->scnRecord, commitSequence, commitTimestamp, &lobCtx, first1,
                                                first2, type, system, schema, dump);
                            opFlush = true;
                        }
                        break;

                    // Insert multiple rows
                    case 0x05010B0B:
                        builder->processInsertMultiple(redoLogRecord2->scnRecord, commitSequence, commitTimestamp, &lobCtx, redoLogRecord1,
                                                       redoLogRecord2, system, schema, dump);
                        opFlush = true;
                        break;

                    // Delete multiple rows
                    case 0x05010B0C:
                        builder->processDeleteMultiple(redoLogRecord2->scnRecord, commitSequence, commitTimestamp, &lobCtx, redoLogRecord1,
                                                       redoLogRecord2, system, schema, dump);
                        opFlush = true;
                        break;

                    // DDL operation
                    case 0x18010000:
                        builder->processDdlHeader( commitScn, commitSequence, commitTimestamp, redoLogRecord1);
                        opFlush = true;
                        break;

                    // Should not happen
                    default:
                        throw RedoLogException(50057, "unknown op code " + std::to_string(op) + ", offset: " +
                                               std::to_string(redoLogRecord1->dataOffset));
                }

                // Split very big transactions
                if (maxMessageMb > 0 && builder->builderSize() + DATA_BUFFER_SIZE > maxMessageMb * 1024 * 1024) {
                    metadata->ctx->warning(60015, "big transaction divided (forced commit after " + std::to_string(builder->builderSize()) +
                                           " bytes), xid: " + xid.toString());

                    if (system) {
                        if (metadata->ctx->trace & TRACE_SYSTEM)
                            metadata->ctx->logTrace(TRACE_SYSTEM, "commit");
                        builder->systemTransaction->commit(commitScn);
                        delete builder->systemTransaction;
                        builder->systemTransaction = nullptr;

                        if (metadata->ctx->trace & TRACE_SYSTEM)
                            metadata->ctx->logTrace(TRACE_SYSTEM, "begin");
                        builder->systemTransaction = new SystemTransaction(builder, metadata);
                    }

                    builder->processCommit(commitScn, commitSequence, commitTimestamp);
                    builder->processBegin(xid, commitScn, lwnScn, &attributes);
                }

                if (opFlush) {
                    first1 = nullptr;
                    last1 = nullptr;
                    first2 = nullptr;
                    last2 = nullptr;
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
        builder->processCommit(commitScn, commitSequence, commitTimestamp);
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
