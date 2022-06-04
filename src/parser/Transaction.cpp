/* Transaction from Oracle database
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

#include "../builder/Builder.h"
#include "../builder/SystemTransaction.h"
#include "../common/RedoLogRecord.h"
#include "../common/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "OpCode0501.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    Transaction::Transaction(typeXid xid) :
        deallocTc(nullptr),
        merges(),
        xid(xid),
        firstSequence(0),
        firstOffset(0),
        commitSequence(0),
        commitScn(0),
        firstTc(nullptr),
        lastTc(nullptr),
        opCodes(0),
        commitTimestamp(0),
        begin(false),
        rollback(false),
        system(false),
        shutdown(false),
        lastSplit(false),
        size(0) {
    }

    void Transaction::add(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord) {
        transactionBuffer->addTransactionChunk(this, redoLogRecord);
        ++opCodes;
    }

    void Transaction::add(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        transactionBuffer->addTransactionChunk(this, redoLogRecord1, redoLogRecord2);
        ++opCodes;
    }

    void Transaction::rollbackLastOp(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2 __attribute__((unused))) {
        uint64_t lengthLast = *((uint64_t *) (lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        //RedoLogRecord* lastRedoLogRecord1 = (RedoLogRecord*) (lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO1);
        auto lastRedoLogRecord2 = (RedoLogRecord *) (lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO2);

        bool ok = false;
        if (lastRedoLogRecord2 != nullptr) {
            if ((lastRedoLogRecord2->opCode == 0x0B05 && redoLogRecord1->opCode == 0x0B05) ||
                (lastRedoLogRecord2->opCode == 0x0B03 && redoLogRecord1->opCode == 0x0B02) ||
                (lastRedoLogRecord2->opCode == 0x0B02 && redoLogRecord1->opCode == 0x0B03) ||
                (lastRedoLogRecord2->opCode == 0x0B06 && redoLogRecord1->opCode == 0x0B06) ||
                (lastRedoLogRecord2->opCode == 0x0B08 && redoLogRecord1->opCode == 0x0B08) ||
                (lastRedoLogRecord2->opCode == 0x0B0B && redoLogRecord1->opCode == 0x0B0C))
                ok = true;
        }

        if (!ok)
            return;

        transactionBuffer->rollbackTransactionChunk(this);
        if (opCodes > 0)
            --opCodes;
    }

    void Transaction::rollbackLastOp(TransactionBuffer* transactionBuffer, RedoLogRecord* redoLogRecord  __attribute__((unused))) {
        if (lastTc == nullptr || lastTc->size == 0)
            return;

        uint64_t lengthLast = *((uint64_t*) (lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        //RedoLogRecord* lastRedoLogRecord1 = (RedoLogRecord*) (lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO1);
        auto lastRedoLogRecord2 = (RedoLogRecord*) (lastTc->buffer + lastTc->size - lengthLast + ROW_HEADER_REDO2);

        bool ok = false;
        if (lastRedoLogRecord2 != nullptr) {
            if (lastRedoLogRecord2->opCode == 0x0B10)
                ok = true;
        }

        if (!ok)
            return;

        transactionBuffer->rollbackTransactionChunk(this);
        if (opCodes > 0)
            --opCodes;
    }

    void Transaction::flush(Metadata* metadata, TransactionBuffer* transactionBuffer, Builder* builder) {
        Ctx* ctx = metadata->ctx;
        bool opFlush = false;
        deallocTc = nullptr;
        uint64_t maxMessageMb = builder->getMaxMessageMb();
        std::unique_lock<std::mutex> lck(metadata->mtx, std::defer_lock);

        if (opCodes == 0 || rollback)
            return;
        TRACE(TRACE2_TRANSACTION, "TRANSACTION: " << *this)

        if (system) {
            lck.lock();

            if (builder->systemTransaction != nullptr)
                throw RedoLogException("system transaction already active:1");
            builder->systemTransaction = new SystemTransaction(builder, metadata);
        }
        builder->processBegin(commitScn, commitTimestamp, commitSequence, xid, system);

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
                typeOp2 op = *((typeOp2*) (tc->buffer + pos));

                RedoLogRecord* redoLogRecord1 = ((RedoLogRecord*) (tc->buffer + pos + ROW_HEADER_REDO1));
                RedoLogRecord* redoLogRecord2 = ((RedoLogRecord*) (tc->buffer + pos + ROW_HEADER_REDO2));

                redoLogRecord1->data = tc->buffer + pos + ROW_HEADER_DATA;
                redoLogRecord2->data = tc->buffer + pos + ROW_HEADER_DATA + redoLogRecord1->length;
                pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;

                TRACE(TRACE2_TRANSACTION, "TRANSACTION: " << std::setfill(' ') << std::setw(4) << std::dec << redoLogRecord1->length <<
                                    ":" << std::setfill(' ') << std::setw(4) << std::dec << redoLogRecord2->length <<
                                " fb: " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord1->fb <<
                                    ":" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord2->fb << " " <<
                                " op: " << std::setfill('0') << std::setw(8) << std::hex << op <<
                                " scn: " << std::dec << redoLogRecord1->scn <<
                                " subScn: " << std::dec << redoLogRecord1->subScn <<
                                " scnRecord: " << std::dec << redoLogRecord1->scnRecord <<
                                " obj: " << std::dec << redoLogRecord1->obj <<
                                " dataObj: " << std::dec << redoLogRecord1->dataObj <<
                                " flg1: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord1->flg <<
                                " flg2: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord2->flg <<
                                " uba1: " << PRINTUBA(redoLogRecord1->uba) <<
                                " uba2: " << PRINTUBA(redoLogRecord2->uba) <<
                                " bdba1: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord1->bdba << "." << std::hex << (uint64_t)redoLogRecord1->slot <<
                                " nrid1: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord1->nridBdba << "." << std::hex << redoLogRecord1->nridSlot <<
                                " bdba2: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord2->bdba << "." << std::hex << (uint64_t)redoLogRecord2->slot <<
                                " nrid2: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord2->nridBdba << "." << std::hex << redoLogRecord2->nridSlot <<
                                " supp: (0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord1->suppLogFb <<
                                    ", " << std::setfill(' ') << std::setw(3) << std::dec << (uint64_t)redoLogRecord1->suppLogType <<
                                    ", " << std::setfill(' ') << std::setw(3) << std::dec << redoLogRecord1->suppLogCC <<
                                    ", " << std::setfill(' ') << std::setw(3) << std::dec << redoLogRecord1->suppLogBefore <<
                                    ", " << std::setfill(' ') << std::setw(3) << std::dec << redoLogRecord1->suppLogAfter <<
                                    ", 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord1->suppLogBdba << "." << std::hex << redoLogRecord1->suppLogSlot << ")")

                opFlush = false;
                switch (op) {
                //single undo - ignore
                case 0x05010000:
                    break;
                //insert row piece
                case 0x05010B02:
                //delete row piece
                case 0x05010B03:
                //update row piece
                case 0x05010B05:
                //overwrite row piece
                case 0x05010B06:
                //change row forwarding address
                case 0x05010B08:
                //supp log for update
                case 0x05010B10:
                //Logminer support - KDOCMP
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
                        first1 = redoLogRecord1;
                        first2 = redoLogRecord2;
                        last1 = redoLogRecord1;
                        last2 = redoLogRecord2;
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
                            //throw RuntimeException("minimal supplemental log missing or redo log inconsistency for transaction " + xid.toString());
                        }
                    }

                    if ((redoLogRecord1->suppLogFb & FB_L) != 0) {
                        builder->processDml(first1, first2, type, system);
                        opFlush = true;
                    }
                    break;

                //insert multiple rows
                case 0x05010B0B:
                    builder->processInsertMultiple(redoLogRecord1, redoLogRecord2, system);
                    opFlush = true;
                    break;

                //delete multiple rows
                case 0x05010B0C:
                    builder->processDeleteMultiple(redoLogRecord1, redoLogRecord2, system);
                    opFlush = true;
                    break;

                //truncate table
                case 0x18010000:
                    builder->processDdlHeader(redoLogRecord1);
                    opFlush = true;
                    break;

                //should not happen
                default:
                    throw RedoLogException("Unknown OpCode " + std::to_string(op) + " offset: " + std::to_string(redoLogRecord1->dataOffset));
                }

                //split very big transactions
                if (maxMessageMb > 0 && builder->builderSize() + DATA_BUFFER_SIZE > maxMessageMb * 1024 * 1024) {
                    WARNING("big transaction divided (forced commit after " << builder->builderSize() << " bytes)")

                    if (system) {
                        TRACE(TRACE2_SYSTEM, "SYSTEM: commit");
                        builder->systemTransaction->commit(commitScn);
                        delete builder->systemTransaction;
                        builder->systemTransaction = nullptr;

                        TRACE(TRACE2_SYSTEM, "SYSTEM: begin")
                        builder->systemTransaction = new SystemTransaction(builder, metadata);
                    }

                    builder->processCommit(system);
                    builder->processBegin(commitScn, commitTimestamp, commitSequence, xid, system);
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

                    for (uint8_t* buf : merges)
                        delete[] buf;
                    merges.clear();
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

            //unlock schema
            lck.unlock();
        }
        builder->processCommit(system);
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

        for (uint8_t* buf : merges)
            delete[] buf;
        merges.clear();

        size = 0;
        opCodes = 0;
    }

    std::ostream& operator<<(std::ostream& os, const Transaction& tran) {
        uint64_t tcCount = 0;
        TransactionChunk* tc = tran.firstTc;
        while (tc != nullptr) {
            ++tcCount;
            tc = tc->next;
        }

        os << "scn: " << std::dec << tran.commitScn <<
                " seq: " << std::dec << tran.firstSequence <<
                " offset: " << std::dec << tran.firstOffset <<
                " xid: " << tran.xid <<
                " flags: " << std::dec << tran.begin << "/" << tran.rollback << "/" << tran.system <<
                " op: " << std::dec << tran.opCodes <<
                " chunks: " << std::dec << tcCount <<
                " sz: " << std::dec << tran.size;
        return os;
    }
}
