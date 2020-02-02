/* Transaction from Oracle database
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <iostream>
#include <iomanip>
#include <string>
#include "types.h"
#include "CommandBuffer.h"
#include "OracleEnvironment.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "TransactionChunk.h"
#include "RedoLogRecord.h"
#include "Writer.h"
#include "OpCode.h"
#include "OpCode0501.h"
#include "OpCode0502.h"
#include "OpCode0504.h"
#include "OpCode0B02.h"
#include "OpCode0B03.h"
#include "OpCode0B05.h"
#include "OpCode0B06.h"
#include "OpCode0B08.h"
#include "OpCode0B0B.h"
#include "OpCode0B0C.h"
#include "OpCode1801.h"

using namespace std;

namespace OpenLogReplicator {

    bool Transaction::operator< (Transaction &p) {
        if (isCommit && !p.isCommit)
            return true;
        if (!isCommit && p.isCommit)
            return false;

        bool ret = lastScn < p.lastScn;
        if (ret != false)
            return ret;
        return lastScn == p.lastScn && xid < p.xid;
    }

    void Transaction::touch(typescn scn) {
        if (firstScn == ZERO_SCN || firstScn > scn)
            firstScn = scn;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::add(uint32_t objn, uint32_t objd, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2,
            TransactionBuffer *transactionBuffer) {
        tcLast = transactionBuffer->addTransactionChunk(tcLast, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
        ++opCodes;
        touch(redoLogRecord1->scn);
    }

    bool Transaction::rollbackPreviousOp(typescn scn, TransactionBuffer *transactionBuffer, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci) {
        if (transactionBuffer->deleteTransactionPart(tcLast, uba, dba, slt, rci)) {
            --opCodes;
            if (lastScn == ZERO_SCN || lastScn < scn)
                lastScn = scn;
            return true;
        } else
            return false;
    }

    void Transaction::rollbackLastOp(typescn scn, TransactionBuffer *transactionBuffer) {
        tcLast = transactionBuffer->rollbackTransactionChunk(tcLast, lastUba, lastDba, lastSlt, lastRci);

        --opCodes;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::flush(OracleEnvironment *oracleEnvironment) {
        TransactionChunk *tcTemp = tc;
        bool hasPrev = false;

        //transaction that has some DML's

        if (opCodes > 0 && !isRollback) {
            if (oracleEnvironment->trace >= TRACE_DETAIL) {
                cerr << endl << "Transaction xid:  " << PRINTXID(xid) <<
                        " SCN: " << PRINTSCN64(firstScn) <<
                        " - " << PRINTSCN64(lastScn) <<
                        " opCodes: " << dec << opCodes << endl;
            }

            if (oracleEnvironment->commandBuffer->posEnd >= INTRA_THREAD_BUFFER_SIZE - MAX_TRANSACTION_SIZE)
                oracleEnvironment->commandBuffer->rewind();

            oracleEnvironment->commandBuffer->writer->beginTran(lastScn, xid);

            while (tcTemp != nullptr) {
                uint32_t pos = 0;
                RedoLogRecord *first1 = nullptr, *first2 = nullptr, *last1 = nullptr, *last2 = nullptr, *prev1 = nullptr, *prev2 = nullptr,
                        *multiPrev = nullptr, *insert1 = nullptr, *insert2 = nullptr;
                typescn prevScn = 0;

                for (uint32_t i = 0; i < tcTemp->elements; ++i) {
                    uint32_t op = *((uint32_t*)(tcTemp->buffer + pos + 8));

                    RedoLogRecord *redoLogRecord1 = ((RedoLogRecord *)(tcTemp->buffer + pos + 12)),
                                  *redoLogRecord2 = ((RedoLogRecord *)(tcTemp->buffer + pos + 12 + sizeof(struct RedoLogRecord)));
                    typescn scn = *((typescn *)(tcTemp->buffer + pos + 32 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
                            redoLogRecord1->length + redoLogRecord2->length));
                    redoLogRecord1->data = tcTemp->buffer + pos + 12 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord);
                    redoLogRecord2->data = tcTemp->buffer + pos + 12 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) + redoLogRecord1->length;

                    if (oracleEnvironment->trace >= TRACE_WARN) {
                        if (oracleEnvironment->trace >= TRACE_DETAIL) {
                            uint32_t objn = *((uint32_t*)(tcTemp->buffer + pos));
                            uint32_t objd = *((uint32_t*)(tcTemp->buffer + pos + 4));
                            cerr << "Row: " << setfill(' ') << setw(4) << dec << redoLogRecord1->length <<
                                        ":" << setfill(' ') << setw(4) << dec << redoLogRecord2->length <<\
                                    " fb: " << setfill('0') << setw(2) << hex << (uint32_t)redoLogRecord1->fb <<
                                        ":" << setfill('0') << setw(2) << hex << (uint32_t)redoLogRecord2->fb << " " <<
                                    " op: " << setfill('0') << setw(8) << hex << op <<
                                    " objn: " << dec << objn <<
                                    " objd: " << dec << objd <<
                                    " flg1: 0x" << setfill('0') << setw(4) << hex << redoLogRecord1->flg <<
                                    " flg2: 0x" << setfill('0') << setw(4) << hex << redoLogRecord2->flg <<
                                    " uba1: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " uba2: " << PRINTUBA(redoLogRecord2->uba) <<
                                    " bdba1: 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->bdba << "." << hex << (uint32_t)redoLogRecord1->slot <<
                                    " nrid1: 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->nridBdba << "." << hex << redoLogRecord1->nridSlot <<
                                    " bdba2: 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->bdba << "." << hex << (uint32_t)redoLogRecord2->slot <<
                                    " nrid2: 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->nridBdba << "." << hex << redoLogRecord2->nridSlot <<
                                    " supp1: (0x" << setfill('0') << setw(2) << hex << (uint32_t)redoLogRecord1->suppLogFb <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogCC <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogBefore <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogAfter <<
                                        ", 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->suppLogBdba << "." << hex << redoLogRecord1->suppLogSlot << ") " <<
                                    " supp2: (0x" << setfill('0') << setw(2) << hex << (uint32_t)redoLogRecord2->suppLogFb <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord2->suppLogCC <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord2->suppLogBefore <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord2->suppLogAfter <<
                                        ", 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->suppLogBdba << "." << hex << redoLogRecord2->suppLogSlot << ") " <<
                                    " scn: " << PRINTSCN64(scn) << endl;
                        }
                        if (prevScn != 0 && prevScn > scn)
                            cerr << "ERROR: SCN swap" << endl;
                    }
                    pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_MEMORY;

                    //combine multi-block UNDO
                    if ((op & 0xFFFF0000) == 0x05010000) {
                        if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOHEAD) != 0) {
                            redoLogRecord1->multiNext = multiPrev;
                            multiPrev = nullptr;
                        }
                        if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOTAIL) != 0) {
                            multiPrev = redoLogRecord1;
                        }
                        if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOMID) != 0) {
                            redoLogRecord1->multiNext = multiPrev;
                            multiPrev = redoLogRecord1;
                        }
                    }

                    switch (op) {
                    //undo part
                    case 0x05010000:
                        break;

                    //insert row piece
                    case 0x05010B02:
                        if ((redoLogRecord2->fb & FB_L) != 0)
                            last2 = redoLogRecord2;
                        else {
                            if (prev2 != nullptr && prev2->bdba == redoLogRecord2->nridBdba && prev2->slot == redoLogRecord2->nridSlot) {
                                redoLogRecord2->next = prev2;
                            } else {
                                if (oracleEnvironment->trace >= TRACE_WARN)
                                    cerr << "WARNING: next BDBA/SLOT does not match (I)" << endl;
                            }
                        }
                        if ((redoLogRecord2->fb & FB_F) != 0) {
                            first1 = redoLogRecord1;
                            first2 = redoLogRecord2;
                        }

                        if (first2 != nullptr && last2 != nullptr) {
                            if (first1->suppLogBdba != 0) {
                                insert1 = first1;
                                insert2 = first2;
                            } else {
                                if (hasPrev)
                                    oracleEnvironment->commandBuffer->writer->next();
                                oracleEnvironment->commandBuffer->writer->parseInsert(first1, first2, oracleEnvironment);
                                hasPrev = true;
                            }
                            first1 = nullptr;
                            last1 = nullptr;
                            first2 = nullptr;
                            last2 = nullptr;
                        }
                        break;

                    //delete row piece
                    case 0x05010B03:
                        if ((redoLogRecord1->fb & FB_F) != 0) {
                            first1 = redoLogRecord1;
                            first2 = redoLogRecord2;
                        } else {
                            if (prev1 != nullptr && prev1->nridBdba == redoLogRecord1->bdba && prev1->nridSlot == redoLogRecord1->slot) {
                                prev1->next = redoLogRecord1;
                            } else {
                                if (oracleEnvironment->trace >= TRACE_WARN)
                                    cerr << "WARNING: next BDBA/SLOT does not match (D)" << endl;
                            }
                        }
                        if ((redoLogRecord1->fb & FB_L) != 0)
                            last1 = redoLogRecord1;

                        if (first1 != nullptr && last1 != nullptr) {
                            if (hasPrev)
                                oracleEnvironment->commandBuffer->writer->next();
                            oracleEnvironment->commandBuffer->writer->parseDelete(first1, first2, oracleEnvironment);
                            hasPrev = true;
                            first1 = nullptr;
                            last1 = nullptr;
                            first2 = nullptr;
                            last2 = nullptr;
                        }
                        break;

                    //update row piece
                    case 0x05010B05:
                    //overwrite row piece
                    case 0x05010B06:
                        redoLogRecord2->suppLogAfter = redoLogRecord1->suppLogAfter;

                        if ((redoLogRecord1->suppLogFb & FB_L) == 0 && (redoLogRecord1->flg & FLG_MULTIBLOCKUNDOHEAD) == 0) {
                            if (first1 != nullptr) {
                                if (prev1 != nullptr && prev1->suppLogBdba == redoLogRecord1->suppLogBdba && prev1->suppLogSlot == redoLogRecord1->suppLogSlot && prev2 != nullptr) {
                                    prev1->next = redoLogRecord1;
                                    prev2->next = redoLogRecord2;
                                } else {
                                    if (oracleEnvironment->trace >= TRACE_WARN)
                                        cerr << "WARNING: next BDBA/SLOT does not match (U)" << endl;
                                }
                            } else {
                                first1 = redoLogRecord1;
                                first2 = redoLogRecord2;
                            }
                        } else {
                            if (first1 == nullptr) {
                                first1 = redoLogRecord1;
                                first2 = redoLogRecord2;
                            }

                            if (hasPrev)
                                oracleEnvironment->commandBuffer->writer->next();
                            if (insert1 != nullptr && insert2 != nullptr && first1->suppLogBdba == insert1->suppLogBdba) {
                                oracleEnvironment->commandBuffer->writer->parseUpdate(first1, insert2, oracleEnvironment);
                                insert1 = nullptr;
                                insert2 = nullptr;
                            } else
                                oracleEnvironment->commandBuffer->writer->parseUpdate(first1, first2, oracleEnvironment);
                            hasPrev = true;
                            first1 = nullptr;
                            first2 = nullptr;
                        }
                        break;

                    //insert multiple rows
                    case 0x05010B0B:
                        if (hasPrev)
                            oracleEnvironment->commandBuffer->writer->next();
                        oracleEnvironment->commandBuffer->writer->parseInsertMultiple(redoLogRecord1, redoLogRecord2, oracleEnvironment);
                        hasPrev = true;
                        break;

                    //delete multiple rows
                    case 0x05010B0C:
                        if (hasPrev)
                            oracleEnvironment->commandBuffer->writer->next();
                        oracleEnvironment->commandBuffer->writer->parseDeleteMultiple(redoLogRecord1, redoLogRecord2, oracleEnvironment);
                        hasPrev = true;
                        break;

                    //truncate table
                    case 0x18010000:
                        if (hasPrev)
                            oracleEnvironment->commandBuffer->writer->next();
                        oracleEnvironment->commandBuffer->writer->parseDDL(redoLogRecord1, oracleEnvironment);
                        hasPrev = true;
                        break;

                    default:
                        cerr << "ERROR: Unknown OpCode " << hex << op << endl;
                    }

                    //split very big transactions
                    if (oracleEnvironment->commandBuffer->currentTranSize() >= MAX_TRANSACTION_SIZE) {
                        cerr << "WARNING: Big transaction divided (" << oracleEnvironment->commandBuffer->currentTranSize() << ")" << endl;
                        oracleEnvironment->commandBuffer->writer->commitTran();
                        if (oracleEnvironment->commandBuffer->posEnd >= INTRA_THREAD_BUFFER_SIZE - MAX_TRANSACTION_SIZE)
                            oracleEnvironment->commandBuffer->rewind();
                        oracleEnvironment->commandBuffer->writer->beginTran(lastScn, xid);
                    }

                    prev1 = redoLogRecord1;
                    prev2 = redoLogRecord2;
                    prevScn = scn;
                }
                tcTemp = tcTemp->next;
            }

            oracleEnvironment->commandBuffer->writer->commitTran();
        }

        oracleEnvironment->transactionBuffer.deleteTransactionChunks(tc, tcLast);
    }

    Transaction::Transaction(typexid xid, TransactionBuffer *transactionBuffer) :
            xid(xid),
            firstScn(ZERO_SCN),
            lastScn(ZERO_SCN),
            opCodes(0),
            pos(0),
            lastUba(0),
            lastDba(0),
            lastSlt(0),
            lastRci(0),
            isBegin(false),
            isCommit(false),
            isRollback(false),
            next(nullptr) {
        tc = transactionBuffer->newTransactionChunk();
        tcLast = tc;
    }

    Transaction::~Transaction() {
    }

    //void Transaction::free(TransactionBuffer *transactionBuffer) {
    //    transactionBuffer->deleteTransactionChunks(tc, tcLast);
    //}


    ostream& operator<<(ostream& os, const Transaction& tran) {
        os << "xid: " << PRINTXID(tran.xid) <<
                " scn: " << PRINTSCN64(tran.firstScn) << " - " << PRINTSCN64(tran.lastScn) <<
                " begin: " << tran.isBegin <<
                " commit: " << tran.isCommit <<
                " rollback: " << tran.isRollback;
        return os;
    }
}

