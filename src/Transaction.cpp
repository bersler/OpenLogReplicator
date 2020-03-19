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
#include <string.h>
#include "types.h"
#include "CommandBuffer.h"
#include "OracleReader.h"
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

    void Transaction::touch(typescn scn, typeseq sequence) {
        if (firstSequence == 0 || firstSequence > sequence)
            firstSequence = sequence;
        if (firstScn == ZERO_SCN || firstScn > scn)
            firstScn = scn;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::add(OracleReader *oracleReader, typeobj objn, typeobj objd, typeuba uba, typedba dba, typeslt slt, typerci rci,
            RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, TransactionBuffer *transactionBuffer, typeseq sequence) {

        uint8_t buffer[REDO_RECORD_MAX_SIZE];
        if ((oracleReader->trace2 & TRACE2_DUMP) != 0)
            cerr << "DUMP: add: " << setfill('0') << setw(4) << hex << redoLogRecord1->opCode << ":" <<
                    setfill('0') << setw(4) << hex << redoLogRecord2->opCode << " XID: " << PRINTXID(redoLogRecord1->xid) << endl;

        //check if previous op was a partial operation
        if (redoLogRecord1->opCode == 0x0501 && (redoLogRecord1->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID)) != 0) {
            typeop2 opCode;
            RedoLogRecord *lastRedoLogRecord1, *lastRedoLogRecord2;

            if (transactionBuffer->getLastRecord(tcLast, opCode, lastRedoLogRecord1, lastRedoLogRecord2) && opCode == 0x05010000 && (lastRedoLogRecord1->flg & FLG_MULTIBLOCKUNDOTAIL) != 0) {
                uint64_t pos = 0, newFieldCnt;
                uint16_t fieldPos, fieldPos2;
                memcpy(buffer, redoLogRecord1->data, redoLogRecord1->fieldLengthsDelta);
                pos = redoLogRecord1->fieldLengthsDelta;

                if ((redoLogRecord1->flg & FLG_LASTBUFFERSPLIT) != 0) {
                    uint16_t length1 = oracleReader->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + redoLogRecord1->fieldCnt * 2);
                    uint16_t length2 = oracleReader->read16(lastRedoLogRecord1->data + lastRedoLogRecord1->fieldLengthsDelta + 6);
                    oracleReader->write16(lastRedoLogRecord1->data + lastRedoLogRecord1->fieldLengthsDelta + 6, length1 + length2);
                    --redoLogRecord1->fieldCnt;
                }

                newFieldCnt = redoLogRecord1->fieldCnt + lastRedoLogRecord1->fieldCnt - 2;
                oracleReader->write16(buffer + pos, newFieldCnt);
                memcpy(buffer + pos + 2, redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 2, redoLogRecord1->fieldCnt * 2);
                memcpy(buffer + pos + 2 + redoLogRecord1->fieldCnt * 2, lastRedoLogRecord1->data + lastRedoLogRecord1->fieldLengthsDelta + 6, lastRedoLogRecord1->fieldCnt * 2 - 4);
                pos += (((newFieldCnt + 1) * 2) + 2) & (0xFFFC);
                fieldPos = pos;

                memcpy(buffer + pos, redoLogRecord1->data + redoLogRecord1->fieldPos, redoLogRecord1->length - redoLogRecord1->fieldPos);
                pos += (redoLogRecord1->length - redoLogRecord1->fieldPos + 3) & (0xFFFC);
                fieldPos2 = lastRedoLogRecord1->fieldPos +
                        ((oracleReader->read16(lastRedoLogRecord1->data + lastRedoLogRecord1->fieldLengthsDelta + 2) + 3) & 0xFFFC) +
                        ((oracleReader->read16(lastRedoLogRecord1->data + lastRedoLogRecord1->fieldLengthsDelta + 4) + 3) & 0xFFFC);

                memcpy(buffer + pos, lastRedoLogRecord1->data + fieldPos2, lastRedoLogRecord1->length - fieldPos2);
                pos += (lastRedoLogRecord1->length - fieldPos2 + 3) & (0xFFFC);

                redoLogRecord1->length = pos;
                redoLogRecord1->fieldCnt = newFieldCnt;
                redoLogRecord1->fieldPos = fieldPos;
                redoLogRecord1->data = buffer;

                uint16_t myFieldLength = oracleReader->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 1 * 2);
                fieldPos += (myFieldLength + 3) & 0xFFFC;
                uint16_t flg = oracleReader->read16(redoLogRecord1->data + fieldPos + 20);
                flg &= ~(FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL | FLG_LASTBUFFERSPLIT);

                if ((redoLogRecord1->flg & FLG_MULTIBLOCKUNDOHEAD) != 0 && (lastRedoLogRecord1->flg & FLG_MULTIBLOCKUNDOTAIL) != 0) {
                    oracleReader->write16(redoLogRecord1->data + fieldPos + 20, flg);

                    OpCode0501 *opCode0501 = new OpCode0501(oracleReader, redoLogRecord1);
                    opCode0501->process();
                    delete opCode0501;
                } else {
                    flg |= FLG_MULTIBLOCKUNDOTAIL;
                    oracleReader->write16(redoLogRecord1->data + fieldPos + 20, flg);
                }

                rollbackLastOp(oracleReader, redoLogRecord1->scn, transactionBuffer);
            } else
                cerr << "ERROR: next multi buffer without previous" << endl;
        }

        if ((oracleReader->trace2 & TRACE2_UBA) != 0)
            cerr << "UBA: add: " << PRINTUBA(uba) << ", dba: 0x" << hex << dba << ", slt: " << dec << (uint64_t)slt << ", rci: " << dec << (uint64_t)rci << endl;

        tcLast = transactionBuffer->addTransactionChunk(tcLast, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
        ++opCodes;
        touch(redoLogRecord1->scn, sequence);
    }

    bool Transaction::rollbackPreviousOp(OracleReader *oracleReader, typescn scn, TransactionBuffer *transactionBuffer, typeuba uba, typedba dba, typeslt slt, typerci rci) {
        if ((oracleReader->trace2 & TRACE2_UBA) != 0)
            cerr << "UBA: rollback previous: " << PRINTUBA(uba) << ", dba: 0x" << hex << dba << ", slt: " << dec << (uint64_t)slt << ", rci: " << dec << (uint64_t)rci << endl;

        if (transactionBuffer->deleteTransactionPart(tcLast, uba, dba, slt, rci)) {
            --opCodes;
            if (lastScn == ZERO_SCN || lastScn < scn)
                lastScn = scn;
            return true;
        } else
            return false;
    }

    void Transaction::rollbackLastOp(OracleReader *oracleReader, typescn scn, TransactionBuffer *transactionBuffer) {
        if ((oracleReader->trace2 & TRACE2_UBA) != 0)
            cerr << "UBA: rollback last: " << PRINTUBA(lastUba) << ", dba: 0x" << hex << lastDba << ", slt: " << dec << (uint64_t)lastSlt << ", rci: " << dec << (uint64_t)lastRci << endl;
        tcLast = transactionBuffer->rollbackTransactionChunk(tcLast, lastUba, lastDba, lastSlt, lastRci);

        --opCodes;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::flush(OracleReader *oracleReader) {
        TransactionChunk *tcTemp = tc;
        bool hasPrev = false, opFlush = false;

        //transaction that has some DML's

        if (opCodes > 0 && !isRollback) {
            if ((oracleReader->trace2 & TRACE2_TRANSACTION) != 0) {
                cerr << endl << "TRANSACTION: " << *this << endl;
            }

            if (oracleReader->commandBuffer->posEnd >= oracleReader->commandBuffer->outputBufferSize - (oracleReader->commandBuffer->outputBufferSize/4))
                oracleReader->commandBuffer->rewind();

            oracleReader->commandBuffer->writer->beginTran(lastScn, xid);
            uint64_t pos, type = 0;
            RedoLogRecord *first1 = nullptr, *first2 = nullptr, *last1 = nullptr, *last2 = nullptr;
            typescn prevScn = 0;

            while (tcTemp != nullptr) {
                pos = 0;
                for (uint64_t i = 0; i < tcTemp->elements; ++i) {
                    typeop2 op = *((typeop2*)(tcTemp->buffer + pos));

                    RedoLogRecord *redoLogRecord1 = ((RedoLogRecord *)(tcTemp->buffer + pos + ROW_HEADER_REDO1)),
                                  *redoLogRecord2 = ((RedoLogRecord *)(tcTemp->buffer + pos + ROW_HEADER_REDO2));
                    redoLogRecord1->data = tcTemp->buffer + pos + ROW_HEADER_DATA;
                    redoLogRecord2->data = tcTemp->buffer + pos + ROW_HEADER_DATA + redoLogRecord1->length;
                    typescn scn = *((typescn *)(tcTemp->buffer + pos + ROW_HEADER_SCN + redoLogRecord1->length + redoLogRecord2->length));

                    if (oracleReader->trace >= TRACE_WARN) {
                        if ((oracleReader->trace2 & TRACE2_TRANSACTION) != 0) {
                            typeobj objn = *((typeobj*)(tcTemp->buffer + pos + ROW_HEADER_OBJN + redoLogRecord1->length + redoLogRecord2->length));
                            typeobj objd = *((typeobj*)(tcTemp->buffer + pos + ROW_HEADER_OBJD + redoLogRecord1->length + redoLogRecord2->length));
                            cerr << "TRANSACTION Row: " << setfill(' ') << setw(4) << dec << redoLogRecord1->length <<
                                        ":" << setfill(' ') << setw(4) << dec << redoLogRecord2->length <<
                                    " fb: " << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord1->fb <<
                                        ":" << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord2->fb << " " <<
                                    " op: " << setfill('0') << setw(8) << hex << op <<
                                    " objn: " << dec << objn <<
                                    " objd: " << dec << objd <<
                                    " flg1: 0x" << setfill('0') << setw(4) << hex << redoLogRecord1->flg <<
                                    " flg2: 0x" << setfill('0') << setw(4) << hex << redoLogRecord2->flg <<
                                    " uba1: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " uba2: " << PRINTUBA(redoLogRecord2->uba) <<
                                    " bdba1: 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->bdba << "." << hex << (uint64_t)redoLogRecord1->slot <<
                                    " nrid1: 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->nridBdba << "." << hex << redoLogRecord1->nridSlot <<
                                    " bdba2: 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->bdba << "." << hex << (uint64_t)redoLogRecord2->slot <<
                                    " nrid2: 0x" << setfill('0') << setw(8) << hex << redoLogRecord2->nridBdba << "." << hex << redoLogRecord2->nridSlot <<
                                    " supp: (0x" << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord1->suppLogFb <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogCC <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogBefore <<
                                        ", " << setfill(' ') << setw(3) << dec << redoLogRecord1->suppLogAfter <<
                                        ", 0x" << setfill('0') << setw(8) << hex << redoLogRecord1->suppLogBdba << "." << hex << redoLogRecord1->suppLogSlot << ") " <<
                                    " scn: " << PRINTSCN64(scn) << endl;
                        }
                        if (prevScn != 0 && prevScn > scn)
                            cerr << "ERROR: SCN swap" << endl;
                    }
                    pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL;

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
                    //change row forwading address
                    case 0x05010B08:

                        redoLogRecord2->suppLogAfter = redoLogRecord1->suppLogAfter;
                        if (type == 0) {
                            if ((redoLogRecord1->suppLogFb & FB_F) != 0 && op == 0x05010B02 &&
                                    ((redoLogRecord1->suppLogBdba == redoLogRecord2->bdba && redoLogRecord1->suppLogSlot == redoLogRecord2->slot) || redoLogRecord1->suppLogBdba == 0))
                                type = TRANSACTION_INSERT;
                            else if ((redoLogRecord1->suppLogFb & FB_F) != 0 && op == 0x05010B03)
                                type = TRANSACTION_DELETE;
                            else
                                type = TRANSACTION_UPDATE;
                        }

                        if (first1 == nullptr) {
                            first1 = redoLogRecord1;
                            first2 = redoLogRecord2;
                            last1 = redoLogRecord1;
                            last2 = redoLogRecord2;
                        } else {
                            if (last1->suppLogBdba == redoLogRecord1->suppLogBdba && last1->suppLogSlot == redoLogRecord1->suppLogSlot) {
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
                                if (oracleReader->trace >= TRACE_WARN)
                                    cerr << "ERROR: next BDBA/SLOT does not match, probably the value of force-checkpoint-scn is set too low and part of transaction got lost" << endl;
                            }
                        }

                        if ((redoLogRecord1->suppLogFb & FB_L) != 0) {
                            if (hasPrev)
                                oracleReader->commandBuffer->writer->next();
                            oracleReader->commandBuffer->writer->parseDML(first1, first2, type, oracleReader);
                            opFlush = true;
                        }
                        break;

                    //insert multiple rows
                    case 0x05010B0B:
                        if (hasPrev)
                            oracleReader->commandBuffer->writer->next();
                        oracleReader->commandBuffer->writer->parseInsertMultiple(redoLogRecord1, redoLogRecord2, oracleReader);
                        opFlush = true;
                        break;

                    //delete multiple rows
                    case 0x05010B0C:
                        if (hasPrev)
                            oracleReader->commandBuffer->writer->next();
                        oracleReader->commandBuffer->writer->parseDeleteMultiple(redoLogRecord1, redoLogRecord2, oracleReader);
                        opFlush = true;
                        break;

                    //truncate table
                    case 0x18010000:
                        if (hasPrev)
                            oracleReader->commandBuffer->writer->next();
                        oracleReader->commandBuffer->writer->parseDDL(redoLogRecord1, oracleReader);
                        opFlush = true;
                        break;

                    default:
                        cerr << "ERROR: Unknown OpCode " << hex << op << endl;
                    }

                    //split very big transactions
                    if (oracleReader->commandBuffer->currentTranSize() >= oracleReader->commandBuffer->outputBufferSize/4) {
                        cerr << "WARNING: Big transaction divided (" << oracleReader->commandBuffer->currentTranSize() << ")" << endl;
                        oracleReader->commandBuffer->writer->commitTran();
                        if (oracleReader->commandBuffer->posEnd >= oracleReader->commandBuffer->outputBufferSize - (oracleReader->commandBuffer->outputBufferSize/4))
                            oracleReader->commandBuffer->rewind();
                        oracleReader->commandBuffer->writer->beginTran(lastScn, xid);
                        hasPrev = false;
                    }

                    if (opFlush) {
                        first1 = nullptr;
                        last1 = nullptr;
                        first2 = nullptr;
                        last2 = nullptr;
                        hasPrev = true;
                        type = 0;
                    }
                    prevScn = scn;
                }
                tcTemp = tcTemp->next;
            }

            oracleReader->commandBuffer->writer->commitTran();
        }

        oracleReader->transactionBuffer->deleteTransactionChunks(tc, tcLast);
    }

    Transaction::Transaction(typexid xid, TransactionBuffer *transactionBuffer) :
            xid(xid),
            firstSequence(0),
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
            isShutdown(false),
            next(nullptr) {
        tc = transactionBuffer->newTransactionChunk();
        tcLast = tc;
    }

    Transaction::~Transaction() {
    }

    ostream& operator<<(ostream& os, const Transaction& tran) {
        os << "xid: " << PRINTXID(tran.xid) <<
                " scn: " << PRINTSCN64(tran.firstScn) << "-" << PRINTSCN64(tran.lastScn) <<
                " begin: " << dec << tran.isBegin <<
                " commit: " << dec << tran.isCommit <<
                " rollback: " << dec << tran.isRollback <<
                " opCodes: " << dec << tran.opCodes;
        return os;
    }
}

