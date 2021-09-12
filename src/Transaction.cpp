/* Transaction from Oracle database
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OpCode0501.h"
#include "OracleAnalyzer.h"
#include "OutputBuffer.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "SystemTransaction.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "Writer.h"

using namespace std;

namespace OpenLogReplicator {
    Transaction::Transaction(OracleAnalyzer* oracleAnalyzer, typeXID xid) :
        oracleAnalyzer(oracleAnalyzer),
        deallocTc(nullptr),
        opCode0501(nullptr),
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
        size(0) {

        stringstream ss;
        ss << "transaction " << PRINTXID(xid);
        name = ss.str();
    }

    Transaction::~Transaction() {
        if (opCode0501 != nullptr) {
            delete opCode0501;
            opCode0501 = nullptr;
        }

        purge();
    }

    void Transaction::mergeBlocks(uint8_t* buffer, RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        memcpy(buffer, redoLogRecord1->data, redoLogRecord1->fieldLengthsDelta);
        uint64_t pos = redoLogRecord1->fieldLengthsDelta;
        uint16_t fieldCnt, fieldPos1, fieldPos2;

        if ((redoLogRecord1->flg & FLG_LASTBUFFERSPLIT) != 0) {
            uint16_t length1 = oracleAnalyzer->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + redoLogRecord1->fieldCnt * 2);
            uint16_t length2 = oracleAnalyzer->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6);
            oracleAnalyzer->write16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6, length1 + length2);
            --redoLogRecord1->fieldCnt;
        }

        //field list
        fieldCnt = redoLogRecord1->fieldCnt + redoLogRecord2->fieldCnt - 2;
        oracleAnalyzer->write16(buffer + pos, fieldCnt);
        memcpy(buffer + pos + 2, redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 2, redoLogRecord1->fieldCnt * 2);
        memcpy(buffer + pos + 2 + redoLogRecord1->fieldCnt * 2, redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6, redoLogRecord2->fieldCnt * 2 - 4);
        pos += (((fieldCnt + 1) * 2) + 2) & (0xFFFC);
        fieldPos1 = pos;

        //data
        memcpy(buffer + pos, redoLogRecord1->data + redoLogRecord1->fieldPos, redoLogRecord1->length - redoLogRecord1->fieldPos);
        pos += (redoLogRecord1->length - redoLogRecord1->fieldPos + 3) & (0xFFFC);
        fieldPos2 = redoLogRecord2->fieldPos +
                ((oracleAnalyzer->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 2) + 3) & 0xFFFC) +
                ((oracleAnalyzer->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 4) + 3) & 0xFFFC);

        memcpy(buffer + pos, redoLogRecord2->data + fieldPos2, redoLogRecord2->length - fieldPos2);
        pos += (redoLogRecord2->length - fieldPos2 + 3) & (0xFFFC);

        redoLogRecord1->length = pos;
        redoLogRecord1->fieldCnt = fieldCnt;
        redoLogRecord1->fieldPos = fieldPos1;
        redoLogRecord1->data = buffer;

        uint16_t myFieldLength = oracleAnalyzer->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 1 * 2);
    }

    void Transaction::add(RedoLogRecord* redoLogRecord) {
        oracleAnalyzer->transactionBuffer->addTransactionChunk(this, redoLogRecord);
        ++opCodes;
    }

    void Transaction::add(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        oracleAnalyzer->transactionBuffer->addTransactionChunk(this, redoLogRecord1, redoLogRecord2);
        ++opCodes;
    }

    void Transaction::rollbackLastOp(typeSCN scn) {
        oracleAnalyzer->transactionBuffer->rollbackTransactionChunk(this);
        if (opCodes > 0)
            --opCodes;
    }

    void Transaction::flush(void) {
        bool opFlush = false;
        deallocTc = nullptr;

        if (opCodes > 0 && !rollback) {
            TRACE(TRACE2_TRANSACTION, "TRANSACTION: " << *this);

            if (system) {
                if (oracleAnalyzer->systemTransaction != nullptr) {
                    RUNTIME_FAIL("system transaction already active:1");
                }
                oracleAnalyzer->systemTransaction = new SystemTransaction(oracleAnalyzer, oracleAnalyzer->outputBuffer, oracleAnalyzer->schema);
                if (oracleAnalyzer->systemTransaction == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(SystemTransaction) << " bytes memory (for: system transaction)");
                }

                if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) != 0)
                    oracleAnalyzer->outputBuffer->processBegin(commitScn, commitTimestamp, commitSequence, xid);
            } else {
                oracleAnalyzer->outputBuffer->processBegin(commitScn, commitTimestamp, commitSequence, xid);
            }
            uint64_t pos, type = 0;
            RedoLogRecord* first1 = nullptr;
            RedoLogRecord* first2 = nullptr;
            RedoLogRecord* last1 = nullptr;
            RedoLogRecord* last2 = nullptr;
            RedoLogRecord* last501 = nullptr;

            TransactionChunk* tc = firstTc;
            while (tc != nullptr) {
                pos = 0;
                for (uint64_t i = 0; i < tc->elements; ++i) {
                    typeOP2 op = *((typeOP2*) (tc->buffer + pos));

                    RedoLogRecord* redoLogRecord1 = ((RedoLogRecord*) (tc->buffer + pos + ROW_HEADER_REDO1));
                    RedoLogRecord* redoLogRecord2 = ((RedoLogRecord*) (tc->buffer + pos + ROW_HEADER_REDO2));

                    redoLogRecord1->data = tc->buffer + pos + ROW_HEADER_DATA;
                    redoLogRecord2->data = tc->buffer + pos + ROW_HEADER_DATA + redoLogRecord1->length;
                    pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;

                    TRACE(TRACE2_TRANSACTION, "TRANSACTION: " << setfill(' ') << setw(4) << dec << redoLogRecord1->length <<
                                        ":" << setfill(' ') << setw(4) << dec << redoLogRecord2->length <<
                                    " fb: " << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord1->fb <<
                                        ":" << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord2->fb << " " <<
                                    " op: " << setfill('0') << setw(8) << hex << op <<
                                    " scn: " << dec << redoLogRecord1->scn <<
                                    " subScn: " << dec << redoLogRecord1->subScn <<
                                    " scnRecord: " << dec << redoLogRecord1->scnRecord <<
                                    " obj: " << dec << redoLogRecord1->obj <<
                                    " dataObj: " << dec << redoLogRecord1->dataObj <<
                                    " flg1: 0x" << setfill('0') << setw(4) << hex << redoLogRecord1->flg <<
                                    " flg2: 0x" << setfill('0') << setw(4) << hex << redoLogRecord2->flg <<
                                    " uba1: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " uba2: " << PRINTUBA(redoLogRecord2->uba) <<
                                    " bdba1: 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->bdba << "." << hex << (uint64_t)redoLogRecord1->slot <<
                                    " nrid1: 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->nridBdba << "." << hex << redoLogRecord1->nridSlot <<
                                    " bdba2: 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->bdba << "." << hex << (uint64_t)redoLogRecord2->slot <<
                                    " nrid2: 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->nridBdba << "." << hex << redoLogRecord2->nridSlot <<
                                    " supp: (0x" << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord1->suppLogFb <<
                                        ", " << setfill(' ') << setw(3) << dec << (uint64_t)redoLogRecord1->suppLogType <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogCC <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogBefore <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogAfter <<
                                        ", 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->suppLogBdba << "." << hex << redoLogRecord1->suppLogSlot << ")");

                    //undo split
                    if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOTAIL) != 0) {
                        if (last501 != nullptr || op != 0x05010000) {
                            RUNTIME_FAIL("split undo TAIL error");
                        }
                        last501 = redoLogRecord1;
                        continue;
                    } else

                    if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOMID) != 0) {
                        if (last501 == nullptr || op != 0x05010000) {
                            RUNTIME_FAIL("split undo MID error");
                        }

                        uint64_t size = last501->length + redoLogRecord1->length;
                        uint8_t* merge = new uint8_t[size];
                        if (merge == nullptr) {
                            RUNTIME_FAIL("couldn't allocate " << dec << size << " bytes memory (for: merge split undo #1)");
                        }
                        merges.push_back(merge);
                        mergeBlocks(merge, redoLogRecord1, last501);
                        last501 = redoLogRecord1;
                        continue;
                    } else

                    if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOHEAD) != 0) {
                        if (last501 == nullptr || op == 0x05010000) {
                            RUNTIME_FAIL("split undo HEAD error");
                        }

                        uint64_t size = last501->length + redoLogRecord1->length;
                        uint8_t* merge = new uint8_t[size];
                        if (merge == nullptr) {
                            RUNTIME_FAIL("couldn't allocate " << dec << size << " bytes memory (for: merge split undo #1)");
                        }
                        merges.push_back(merge);
                        mergeBlocks(merge, redoLogRecord1, last501);

                        uint16_t fieldPos = redoLogRecord1->fieldPos;
                        uint16_t fieldLength = oracleAnalyzer->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 1 * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;

                        uint16_t flg = oracleAnalyzer->read16(redoLogRecord1->data + fieldPos + 20);
                        flg &= ~(FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL | FLG_LASTBUFFERSPLIT);
                        oracleAnalyzer->write16(redoLogRecord1->data + fieldPos + 20, flg);

                        opCode0501 = new OpCode0501(oracleAnalyzer, redoLogRecord1);
                        if (opCode0501 == nullptr) {
                            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0501) << " bytes memory (for: merge split blocks #3)");
                        }

                        opCode0501->process();
                        if (opCode0501 != nullptr) {
                            delete opCode0501;
                            opCode0501 = nullptr;
                        }
                        last501 = nullptr;
                    } else if (last501 != nullptr) {
                        RUNTIME_FAIL("split undo is broken");
                    }

                    opFlush = false;
                    switch (op) {
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

                        redoLogRecord2->suppLogAfter = redoLogRecord1->suppLogAfter;

                        if (type == 0) {
                            if (op == 0x05010B02)
                                type = TRANSACTION_INSERT;
                            else if (op == 0x05010B03)
                                type = TRANSACTION_DELETE;
                            else
                                type = TRANSACTION_UPDATE;
                        } else
                        if (type == TRANSACTION_INSERT) {
                            if (op == 0x05010B03 || op == 0x05010B05 || op == 0x05010B06 || op == 0x05010B08)
                                type = TRANSACTION_UPDATE;
                        } else
                        if (type == TRANSACTION_DELETE) {
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
                                //RUNTIME_FAIL("minimal supplemental log missing or redo log inconsistency for transaction " << PRINTXID(xid));
                            }
                        }

                        if ((redoLogRecord1->suppLogFb & FB_L) != 0) {
                            oracleAnalyzer->outputBuffer->processDML(first1, first2, type, system);
                            opFlush = true;
                        }
                        break;

                    //insert multiple rows
                    case 0x05010B0B:
                        oracleAnalyzer->outputBuffer->processInsertMultiple(redoLogRecord1, redoLogRecord2, system);
                        opFlush = true;
                        break;

                    //delete multiple rows
                    case 0x05010B0C:
                        oracleAnalyzer->outputBuffer->processDeleteMultiple(redoLogRecord1, redoLogRecord2, system);
                        opFlush = true;
                        break;

                    //truncate table
                    case 0x18010000:
                        oracleAnalyzer->outputBuffer->processDDLheader(redoLogRecord1);
                        opFlush = true;
                        break;

                    //should not happen
                    default:
                        RUNTIME_FAIL("Unknown OpCode " << hex << op);
                    }

                    //split very big transactions
                    if (oracleAnalyzer->outputBuffer->writer->maxMessageMb > 0 &&
                            oracleAnalyzer->outputBuffer->outputBufferSize() + DATA_BUFFER_SIZE > oracleAnalyzer->outputBuffer->writer->maxMessageMb * 1024 * 1024) {
                        WARNING("big transaction divided (forced commit after " << oracleAnalyzer->outputBuffer->outputBufferSize() << " bytes)");

                        if (system) {
                            TRACE(TRACE2_SYSTEM, "SYSTEM: commit");
                            oracleAnalyzer->systemTransaction->commit(commitScn);
                            delete oracleAnalyzer->systemTransaction;

                            TRACE(TRACE2_SYSTEM, "SYSTEM: begin");
                            if (oracleAnalyzer->systemTransaction != nullptr) {
                                RUNTIME_FAIL("system transaction already active:2");
                            }
                            oracleAnalyzer->systemTransaction = new SystemTransaction(oracleAnalyzer, oracleAnalyzer->outputBuffer, oracleAnalyzer->schema);
                            if (oracleAnalyzer->systemTransaction == nullptr) {
                                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(SystemTransaction) << " bytes memory (for: system transaction merge)");
                            }

                            if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) != 0) {
                                oracleAnalyzer->outputBuffer->processCommit();
                                oracleAnalyzer->outputBuffer->processBegin(commitScn, commitTimestamp, commitSequence, xid);
                            }
                        } else {
                            oracleAnalyzer->outputBuffer->processCommit();
                            oracleAnalyzer->outputBuffer->processBegin(commitScn, commitTimestamp, commitSequence, xid);
                        }
                    }

                    if (opFlush) {
                        first1 = nullptr;
                        last1 = nullptr;
                        first2 = nullptr;
                        last2 = nullptr;
                        type = 0;

                        while (deallocTc != nullptr) {
                            TransactionChunk* nextTc = deallocTc->next;
                            oracleAnalyzer->transactionBuffer->deleteTransactionChunk(deallocTc);
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
                oracleAnalyzer->transactionBuffer->deleteTransactionChunk(deallocTc);
                deallocTc = nextTc;
            }

            firstTc = nullptr;
            lastTc = nullptr;
            opCodes = 0;

            if (system) {
                oracleAnalyzer->systemTransaction->commit(commitScn);
                delete oracleAnalyzer->systemTransaction;
                oracleAnalyzer->systemTransaction = nullptr;

                if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS) != 0)
                    oracleAnalyzer->outputBuffer->processCommit();
            } else {
                oracleAnalyzer->outputBuffer->processCommit();
            }
        }
    }

    void Transaction::purge(void) {
        if (firstTc != nullptr) {
            oracleAnalyzer->transactionBuffer->deleteTransactionChunks(firstTc);
            firstTc = nullptr;
            lastTc = nullptr;
        }

        while (deallocTc != nullptr) {
            TransactionChunk* nextTc = deallocTc->next;
            oracleAnalyzer->transactionBuffer->deleteTransactionChunk(deallocTc);
            deallocTc = nextTc;
        }
        deallocTc = nullptr;

        for (uint8_t* buf : merges)
            delete[] buf;
        merges.clear();

        size = 0;
        opCodes = 0;
    }

    ostream& operator<<(ostream& os, const Transaction& tran) {
        uint64_t tcCount = 0;
        TransactionChunk* tc = tran.firstTc;
        while (tc != nullptr) {
            ++tcCount;
            tc = tc->next;
        }

        os << "scn: " << dec << tran.commitScn <<
                " seq: " << dec << tran.firstSequence <<
                " offset: " << dec << tran.firstOffset <<
                " xid: " << PRINTXID(tran.xid) <<
                " flags: " << dec << tran.begin << "/" << tran.rollback << "/" << tran.system <<
                " op: " << dec << tran.opCodes <<
                " chunks: " << dec << tcCount <<
                " sz: " << dec << tran.size;
        return os;
    }
}
