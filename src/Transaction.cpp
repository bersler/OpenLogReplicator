/* Transaction from Oracle database
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
along with Open Log Replicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <iomanip>
#include <iostream>
#include <string>
#include <string.h>

#include "CommandBuffer.h"
#include "MemoryException.h"
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
#include "OpCode0B10.h"
#include "OpCode1801.h"
#include "OracleAnalyser.h"
#include "RedoLogRecord.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "TransactionChunk.h"
#include "Writer.h"

using namespace std;

namespace OpenLogReplicator {

    Transaction::Transaction(OracleAnalyser *oracleAnalyser, typexid xid) :
            splitBlockList(nullptr),
            xid(xid),
            firstSequence(0),
            firstScn(ZERO_SCN),
            lastScn(ZERO_SCN),
            firstTc(nullptr),
            lastTc(nullptr),
            opCodes(0),
            pos(0),
            lastRedoLogRecord1(nullptr),
            lastRedoLogRecord2(nullptr),
            commitTime(0),
            isBegin(false),
            isCommit(false),
            isRollback(false),
            shutdown(false),
            next(nullptr) {
    }

    Transaction::~Transaction() {
        while (splitBlockList != nullptr) {
            uint8_t *nextSplitBlockList = *((uint8_t**)(splitBlockList + SPLIT_BLOCK_NEXT));
            delete[] splitBlockList;
            splitBlockList = nextSplitBlockList;
        }
    }

    void Transaction::mergeSplitBlocksToBuffer(OracleAnalyser *oracleAnalyser, uint8_t *buffer, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        memcpy(buffer, redoLogRecord1->data, redoLogRecord1->fieldLengthsDelta);
        uint64_t pos = redoLogRecord1->fieldLengthsDelta;
        uint16_t fieldCnt, fieldPos1, fieldPos2;

        if ((redoLogRecord1->flg & FLG_LASTBUFFERSPLIT) != 0) {
            uint16_t length1 = oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + redoLogRecord1->fieldCnt * 2);
            uint16_t length2 = oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6);
            oracleAnalyser->write16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6, length1 + length2);
            --redoLogRecord1->fieldCnt;
        }

        //field list
        fieldCnt = redoLogRecord1->fieldCnt + redoLogRecord2->fieldCnt - 2;
        oracleAnalyser->write16(buffer + pos, fieldCnt);
        memcpy(buffer + pos + 2, redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 2, redoLogRecord1->fieldCnt * 2);
        memcpy(buffer + pos + 2 + redoLogRecord1->fieldCnt * 2, redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 6, redoLogRecord2->fieldCnt * 2 - 4);
        pos += (((fieldCnt + 1) * 2) + 2) & (0xFFFC);
        fieldPos1 = pos;

        //data
        memcpy(buffer + pos, redoLogRecord1->data + redoLogRecord1->fieldPos, redoLogRecord1->length - redoLogRecord1->fieldPos);
        pos += (redoLogRecord1->length - redoLogRecord1->fieldPos + 3) & (0xFFFC);
        fieldPos2 = redoLogRecord2->fieldPos +
                ((oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 2) + 3) & 0xFFFC) +
                ((oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + 4) + 3) & 0xFFFC);

        memcpy(buffer + pos, redoLogRecord2->data + fieldPos2, redoLogRecord2->length - fieldPos2);
        pos += (redoLogRecord2->length - fieldPos2 + 3) & (0xFFFC);

        redoLogRecord1->length = pos;
        redoLogRecord1->fieldCnt = fieldCnt;
        redoLogRecord1->fieldPos = fieldPos1;
        redoLogRecord1->data = buffer;

        uint16_t myFieldLength = oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + 1 * 2);
    }

    void Transaction::touch(typescn scn, typeseq sequence) {
        if (firstSequence == 0 || firstSequence > sequence)
            firstSequence = sequence;
        if (firstScn == ZERO_SCN || firstScn > scn)
            firstScn = scn;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::mergeSplitBlocks(OracleAnalyser *oracleAnalyser, RedoLogRecord *headRedoLogRecord1, RedoLogRecord *midRedoLogRecord1,
            RedoLogRecord *tailRedoLogRecord1, RedoLogRecord *redoLogRecord2) {
        if (headRedoLogRecord1 == nullptr || tailRedoLogRecord1 == nullptr || redoLogRecord2 == nullptr) {
            cerr << "ERROR: merging of incomplete split UNDO block" << endl;
            cerr << "head: ";
            if (headRedoLogRecord1 == nullptr)
                cerr << "- null";
            else
                headRedoLogRecord1->dump(oracleAnalyser);
            cerr << endl;
            cerr << "mid: ";
            if (midRedoLogRecord1 == nullptr)
                cerr << "- null";
            else
                midRedoLogRecord1->dump(oracleAnalyser);
            cerr << endl;
            cerr << "tail: ";
            if (tailRedoLogRecord1 == nullptr)
                cerr << "- null";
            else
                tailRedoLogRecord1->dump(oracleAnalyser);
            cerr << endl;

            return;
        }

        uint8_t *buffer1 = nullptr, *buffer2 = nullptr;

        //mid
        if (midRedoLogRecord1 != nullptr) {
            uint64_t size1 = headRedoLogRecord1->length + midRedoLogRecord1->length;
            buffer1 = new uint8_t[size1];
            if (buffer1 == nullptr)
                throw MemoryException("Transaction::mergeSplitBlocks.1", size1);
            mergeSplitBlocksToBuffer(oracleAnalyser, buffer1, headRedoLogRecord1, midRedoLogRecord1);
        }

        //tail
        uint64_t size2 = headRedoLogRecord1->length + tailRedoLogRecord1->length;
        buffer2 = new uint8_t[size2];
        if (buffer2 == nullptr)
            throw MemoryException("Transaction::mergeSplitBlocks.2", size2);
        mergeSplitBlocksToBuffer(oracleAnalyser, buffer2, headRedoLogRecord1, tailRedoLogRecord1);

        uint16_t fieldPos = headRedoLogRecord1->fieldPos;
        uint16_t fieldLength = oracleAnalyser->read16(headRedoLogRecord1->data + headRedoLogRecord1->fieldLengthsDelta + 1 * 2);
        fieldPos += (fieldLength + 3) & 0xFFFC;

        uint16_t flg = oracleAnalyser->read16(headRedoLogRecord1->data + fieldPos + 20);
        flg &= ~(FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL | FLG_LASTBUFFERSPLIT);
        oracleAnalyser->write16(headRedoLogRecord1->data + fieldPos + 20, flg);

        OpCode0501 *opCode0501 = new OpCode0501(oracleAnalyser, headRedoLogRecord1);
        if (opCode0501 == nullptr)
            throw MemoryException("Transaction::mergeSplitBlocks.3", sizeof(OpCode0501));

        opCode0501->process();


        if (oracleAnalyser->onRollbackList(headRedoLogRecord1, redoLogRecord2)) {
            oracleAnalyser->printRollbackInfo(headRedoLogRecord1, redoLogRecord2, this, "merged, rolled back");
        } else {
            if (opCodes > 0)
                oracleAnalyser->lastOpTransactionMap.erase(this);

            oracleAnalyser->printRollbackInfo(headRedoLogRecord1, redoLogRecord2, this, "merged");
            add(oracleAnalyser, headRedoLogRecord1, redoLogRecord2, firstSequence, headRedoLogRecord1->scn);
            oracleAnalyser->transactionHeap.update(pos);
            oracleAnalyser->lastOpTransactionMap.set(this);
        }

        delete opCode0501;
        opCode0501 = nullptr;

        if (buffer1 != nullptr) {
            delete[] buffer1;
            buffer1 = nullptr;
        }

        delete[] buffer2;
        buffer2 = nullptr;
    }

    void Transaction::addSplitBlock(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord) {
        if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0) {
            cerr << "SPLIT: ";
            redoLogRecord->dump(oracleAnalyser);
            cerr << endl;
        };

        uint64_t size = SPLIT_BLOCK_DATA1 + redoLogRecord->length;
        uint8_t *splitBlock = new uint8_t[size], *prevSplitBlock = nullptr, *tmpSplitBlockList = nullptr;

        if (splitBlock == nullptr)
            throw MemoryException("Transaction::addSplitBlock.1", size);
        *((uint64_t*) (splitBlock + SPLIT_BLOCK_SIZE)) = size;
        *((typeop1*) (splitBlock + SPLIT_BLOCK_OP1)) = redoLogRecord->opCode;
        *((typeop1*) (splitBlock + SPLIT_BLOCK_OP2)) = 0;

        memcpy(splitBlock + SPLIT_BLOCK_RECORD1, redoLogRecord, sizeof(RedoLogRecord));
        memcpy(splitBlock + SPLIT_BLOCK_DATA1, redoLogRecord->data, redoLogRecord->length);
        ((RedoLogRecord*)(splitBlock + SPLIT_BLOCK_RECORD1))->data = splitBlock + SPLIT_BLOCK_DATA1;

        tmpSplitBlockList = splitBlockList;
        while (tmpSplitBlockList != nullptr) {
            RedoLogRecord *prevRedoLogRecord = (RedoLogRecord*)(tmpSplitBlockList + SPLIT_BLOCK_RECORD1);
            if ((prevRedoLogRecord->scn < redoLogRecord->scn ||
                    ((prevRedoLogRecord->scn == redoLogRecord->scn && prevRedoLogRecord->subScn <= redoLogRecord->subScn))))
                break;
            prevSplitBlock = tmpSplitBlockList;
            tmpSplitBlockList = *((uint8_t**) (tmpSplitBlockList + SPLIT_BLOCK_NEXT));
        }

        if (prevSplitBlock != nullptr)
            *((uint8_t**) (prevSplitBlock + SPLIT_BLOCK_NEXT)) = splitBlock;
        else
            splitBlockList = splitBlock;
        *((uint8_t**) (splitBlock + SPLIT_BLOCK_NEXT)) = tmpSplitBlockList;
    }

    void Transaction::addSplitBlock(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0) {
            cerr << "SPLIT: ";
            redoLogRecord1->dump(oracleAnalyser);
            cerr << endl << "SPLIT: ";
            redoLogRecord2->dump(oracleAnalyser);
            cerr << endl;
        };

        uint64_t size = SPLIT_BLOCK_DATA2 + redoLogRecord1->length + redoLogRecord2->length;
        uint8_t *splitBlock = new uint8_t[size], *prevSplitBlock = nullptr, *tmpSplitBlockList = nullptr;

        if (splitBlock == nullptr)
            throw MemoryException("Transaction::addSplitBlock.2", size);
        *((uint64_t*) (splitBlock + SPLIT_BLOCK_SIZE)) = size;
        *((typeop1*) (splitBlock + SPLIT_BLOCK_OP1)) = redoLogRecord1->opCode;
        *((typeop1*) (splitBlock + SPLIT_BLOCK_OP2)) = redoLogRecord2->opCode;

        memcpy(splitBlock + SPLIT_BLOCK_RECORD1, redoLogRecord1, sizeof(RedoLogRecord));
        memcpy(splitBlock + SPLIT_BLOCK_DATA1, redoLogRecord1->data, redoLogRecord1->length);
        ((RedoLogRecord*)(splitBlock + SPLIT_BLOCK_RECORD1))->data = splitBlock + SPLIT_BLOCK_DATA1;

        memcpy(splitBlock + SPLIT_BLOCK_RECORD2 + redoLogRecord1->length, redoLogRecord2, sizeof(RedoLogRecord));
        memcpy(splitBlock + SPLIT_BLOCK_DATA2 + redoLogRecord1->length, redoLogRecord2->data, redoLogRecord2->length);
        ((RedoLogRecord*)(splitBlock + SPLIT_BLOCK_RECORD2 + redoLogRecord1->length))->data = splitBlock + SPLIT_BLOCK_DATA2 + redoLogRecord1->length;

        tmpSplitBlockList = splitBlockList;
        while (tmpSplitBlockList != nullptr) {
            RedoLogRecord *prevRedoLogRecord = (RedoLogRecord*)(tmpSplitBlockList + SPLIT_BLOCK_RECORD1);
            if ((prevRedoLogRecord->scn < redoLogRecord1->scn ||
                    ((prevRedoLogRecord->scn == redoLogRecord1->scn && prevRedoLogRecord->subScn <= redoLogRecord1->subScn))))
                break;
            prevSplitBlock = tmpSplitBlockList;
            tmpSplitBlockList = *((uint8_t**) (tmpSplitBlockList + SPLIT_BLOCK_NEXT));
        }

        if (prevSplitBlock != nullptr)
            *((uint8_t**) (prevSplitBlock + SPLIT_BLOCK_NEXT)) = splitBlock;
        else
            splitBlockList = splitBlock;
        *((uint8_t**) (splitBlock + SPLIT_BLOCK_NEXT)) = tmpSplitBlockList;
    }

    void Transaction::add(OracleAnalyser *oracleAnalyser, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, typeseq sequence,
            typescn scn) {

        oracleAnalyser->transactionBuffer->addTransactionChunk(oracleAnalyser, this, redoLogRecord1, redoLogRecord2);
        ++opCodes;
        touch(scn, sequence);
    }

    bool Transaction::rollbackPartOp(OracleAnalyser *oracleAnalyser, RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2,
            typescn scn) {

        if (oracleAnalyser->transactionBuffer->deleteTransactionPart(oracleAnalyser, this, rollbackRedoLogRecord1, rollbackRedoLogRecord2)) {
            --opCodes;
            if (lastScn == ZERO_SCN || lastScn < scn)
                lastScn = scn;
            return true;
        } else
            return false;
    }

    void Transaction::rollbackLastOp(OracleAnalyser *oracleAnalyser, typescn scn) {
        oracleAnalyser->transactionBuffer->rollbackTransactionChunk(oracleAnalyser, this);
        --opCodes;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::flushSplitBlocks(OracleAnalyser *oracleAnalyser) {
        if (splitBlockList == nullptr)
            return;
        if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0) {
            cerr << "SPLIT: merge" << endl;
        }

        uint8_t *headBlock = nullptr, *midBlock = nullptr, *tailBlock = nullptr;
        RedoLogRecord *headRedoLogRecord1 = nullptr, *midRedoLogRecord1 = nullptr, *tailRedoLogRecord1 = nullptr, *redoLogRecord2 = nullptr,
                *curRedoLogRecord = nullptr, *newRedoLogRecord = nullptr;

        while (splitBlockList != nullptr) {
            newRedoLogRecord = (RedoLogRecord*)(splitBlockList + SPLIT_BLOCK_RECORD1);

            if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0) {
                cerr << "SPLIT: next is: ";
                newRedoLogRecord->dump(oracleAnalyser);
                cerr << endl;
            }

            if (curRedoLogRecord == nullptr) {
                curRedoLogRecord = newRedoLogRecord;
            } else {
                if (curRedoLogRecord->slt != newRedoLogRecord->slt ||
                        curRedoLogRecord->rci != newRedoLogRecord->rci ||
                        ((newRedoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0 && headRedoLogRecord1 != nullptr) ||
                        ((newRedoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0 && midRedoLogRecord1 != nullptr) ||
                        ((newRedoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0 && tailRedoLogRecord1 != nullptr)) {
                    if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0)
                        cerr << "SPLIT: flush" << endl;

                    mergeSplitBlocks(oracleAnalyser, headRedoLogRecord1, midRedoLogRecord1, tailRedoLogRecord1, redoLogRecord2);
                    headRedoLogRecord1 = nullptr;
                    midRedoLogRecord1 = nullptr;
                    tailRedoLogRecord1 = nullptr;
                    redoLogRecord2 = nullptr;

                    if (headBlock != nullptr) {
                        delete[] headBlock;
                        headBlock = nullptr;
                    }
                    if (midBlock != nullptr) {
                        delete[] midBlock;
                        midBlock = nullptr;
                    }
                    if (tailBlock != nullptr) {
                        delete[] tailBlock;
                        tailBlock = nullptr;
                    }

                    curRedoLogRecord = newRedoLogRecord;
                }
            }

            if ((newRedoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0) {
                headBlock = splitBlockList;
                headRedoLogRecord1 = newRedoLogRecord;
                redoLogRecord2 = (RedoLogRecord*)(splitBlockList + SPLIT_BLOCK_RECORD2 + headRedoLogRecord1->length);
            } else if ((newRedoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0) {
                tailBlock = splitBlockList;
                tailRedoLogRecord1 = newRedoLogRecord;
            } else {
                midBlock = splitBlockList;
                midRedoLogRecord1 = newRedoLogRecord;
            }

            splitBlockList = *((uint8_t**)(splitBlockList + SPLIT_BLOCK_NEXT));
        }

        if (curRedoLogRecord != nullptr) {
            if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0)
                cerr << "SPLIT: flush last" << endl;
            mergeSplitBlocks(oracleAnalyser, headRedoLogRecord1, midRedoLogRecord1, tailRedoLogRecord1, redoLogRecord2);
            headRedoLogRecord1 = nullptr;
            midRedoLogRecord1 = nullptr;
            tailRedoLogRecord1 = nullptr;
            redoLogRecord2 = nullptr;

            if (headBlock != nullptr) {
                delete[] headBlock;
                headBlock = nullptr;
            }
            if (midBlock != nullptr) {
                delete[] midBlock;
                midBlock = nullptr;
            }
            if (tailBlock != nullptr) {
                delete[] tailBlock;
                tailBlock = nullptr;
            }

            curRedoLogRecord = nullptr;
        }
        if ((oracleAnalyser->trace2 & TRACE2_SPLIT) != 0) {
            cerr << "SPLIT: merge end" << endl;
        }
    }

    void Transaction::flush(OracleAnalyser *oracleAnalyser) {
        TransactionChunk *tc = firstTc;
        bool hasPrev = false, opFlush = false;

        flushSplitBlocks(oracleAnalyser);

        if (opCodes > 0 && !isRollback) {
            if ((oracleAnalyser->trace2 & TRACE2_TRANSACTION) != 0) {
                cerr << endl << "TRANSACTION: " << *this << endl;
            }

            if (oracleAnalyser->commandBuffer->posEnd >= oracleAnalyser->commandBuffer->outputBufferSize - (oracleAnalyser->commandBuffer->outputBufferSize/4))
                oracleAnalyser->commandBuffer->rewind();

            oracleAnalyser->commandBuffer->writer->beginTran(lastScn, commitTime, xid);
            uint64_t pos, type = 0;
            RedoLogRecord *first1 = nullptr, *first2 = nullptr, *last1 = nullptr, *last2 = nullptr;
            typescn prevScn = 0;

            while (tc != nullptr) {
                pos = 0;
                for (uint64_t i = 0; i < tc->elements; ++i) {
                    typeop2 op = *((typeop2*)(tc->buffer + pos));

                    RedoLogRecord *redoLogRecord1 = ((RedoLogRecord *)(tc->buffer + pos + ROW_HEADER_REDO1)),
                                  *redoLogRecord2 = ((RedoLogRecord *)(tc->buffer + pos + ROW_HEADER_REDO2));
                    redoLogRecord1->data = tc->buffer + pos + ROW_HEADER_DATA;
                    redoLogRecord2->data = tc->buffer + pos + ROW_HEADER_DATA + redoLogRecord1->length;
                    typescn scn = *((typescn *)(tc->buffer + pos + ROW_HEADER_SCN + redoLogRecord1->length + redoLogRecord2->length));

                    if (oracleAnalyser->trace >= TRACE_WARN) {
                        if ((oracleAnalyser->trace2 & TRACE2_TRANSACTION) != 0) {
                            cerr << "TRANSACTION Row: " << setfill(' ') << setw(4) << dec << redoLogRecord1->length <<
                                        ":" << setfill(' ') << setw(4) << dec << redoLogRecord2->length <<
                                    " fb: " << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord1->fb <<
                                        ":" << setfill('0') << setw(2) << hex << (uint64_t)redoLogRecord2->fb << " " <<
                                    " op: " << setfill('0') << setw(8) << hex << op <<
                                    " objn: " << dec << redoLogRecord1->objn <<
                                    " objd: " << dec << redoLogRecord1->objd <<
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
                        if (prevScn != 0 && prevScn > scn) {
                            if (oracleAnalyser->trace >= TRACE_WARN)
                                cerr << "WARNING: SCN swap" << endl;
                        }
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
                    //change row forwarding address
                    case 0x05010B08:
                    //supp log for update
                    case 0x05010B10:

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
                            if (last1->suppLogBdba == redoLogRecord1->suppLogBdba && last1->suppLogSlot == redoLogRecord1->suppLogSlot &&
                                    first1->object == redoLogRecord1->object && first2->object == redoLogRecord2->object) {
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
                                cerr << "ERROR: next BDBA/SLOT does not match" << endl;
                            }
                        }

                        if ((redoLogRecord1->suppLogFb & FB_L) != 0) {
                            if (hasPrev)
                                oracleAnalyser->commandBuffer->writer->next();
                            oracleAnalyser->commandBuffer->writer->parseDML(first1, first2, type);
                            opFlush = true;
                        }
                        break;

                    //insert multiple rows
                    case 0x05010B0B:
                        if (hasPrev)
                            oracleAnalyser->commandBuffer->writer->next();
                        oracleAnalyser->commandBuffer->writer->parseInsertMultiple(redoLogRecord1, redoLogRecord2);
                        opFlush = true;
                        break;

                    //delete multiple rows
                    case 0x05010B0C:
                        if (hasPrev)
                            oracleAnalyser->commandBuffer->writer->next();
                        oracleAnalyser->commandBuffer->writer->parseDeleteMultiple(redoLogRecord1, redoLogRecord2);
                        opFlush = true;
                        break;

                    //truncate table
                    case 0x18010000:
                        if (hasPrev)
                            oracleAnalyser->commandBuffer->writer->next();
                        oracleAnalyser->commandBuffer->writer->parseDDL(redoLogRecord1);
                        opFlush = true;
                        break;

                    default:
                        cerr << "ERROR: Unknown OpCode " << hex << op << endl;
                    }

                    //split very big transactions
                    if (oracleAnalyser->commandBuffer->currentTranSize() >= oracleAnalyser->commandBuffer->outputBufferSize/4) {
                        cerr << "WARNING: Big transaction divided (" << oracleAnalyser->commandBuffer->currentTranSize() << ")" << endl;
                        oracleAnalyser->commandBuffer->writer->commitTran();
                        if (oracleAnalyser->commandBuffer->posEnd >= oracleAnalyser->commandBuffer->outputBufferSize - (oracleAnalyser->commandBuffer->outputBufferSize/4))
                            oracleAnalyser->commandBuffer->rewind();
                        oracleAnalyser->commandBuffer->writer->beginTran(lastScn, commitTime, xid);
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
                tc = tc->next;
            }

            oracleAnalyser->commandBuffer->writer->commitTran();
        }
    }

    void Transaction::updateLastRecord(void) {
        if (lastTc == nullptr || lastTc->elements == 0) {
            cerr << "ERROR: updating last element of empty transaction" << endl;

            lastRedoLogRecord1 = nullptr;
            lastRedoLogRecord2 = nullptr;
            return;
        }

        uint64_t lastSize = *((uint64_t *)(lastTc->buffer + lastTc->size - ROW_HEADER_TOTAL + ROW_HEADER_SIZE));
        lastRedoLogRecord1 = (RedoLogRecord*)(lastTc->buffer + lastTc->size - lastSize + ROW_HEADER_REDO1);
        lastRedoLogRecord2 = (RedoLogRecord*)(lastTc->buffer + lastTc->size - lastSize + ROW_HEADER_REDO2);
    }

    bool Transaction::matchesForRollback(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2,
            RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2) {

/*
        cerr << "ROLLBACK:    undo:" <<
                " OP: " << hex << redoLogRecord2->opCode <<
                " SLT: " << dec << (uint64_t)redoLogRecord1->slt <<
                " RCI: " << dec << (uint64_t)redoLogRecord1->rci <<
                " SCN: " << PRINTSCN64(redoLogRecord1->scn) <<
                " UBA: " << PRINTUBA(redoLogRecord1->uba) << endl;

        cerr << "rollback:" <<
                " OP: " << hex << rollbackRedoLogRecord1->opCode <<
                " SLT: " << dec << (uint64_t)rollbackRedoLogRecord2->slt <<
                " RCI: " << dec << (uint64_t)rollbackRedoLogRecord2->rci <<
                " SCN: " << PRINTSCN64(rollbackRedoLogRecord2->scn) <<
                " UBA: " << PRINTUBA(rollbackRedoLogRecord1->uba) << endl;
*/

        bool val =
                (redoLogRecord1->slt == rollbackRedoLogRecord2->slt &&
                redoLogRecord1->rci == rollbackRedoLogRecord2->rci &&
                redoLogRecord1->uba == rollbackRedoLogRecord1->uba &&
                redoLogRecord1->scn <= rollbackRedoLogRecord2->scn &&
          ((rollbackRedoLogRecord2->opFlags & OPFLAG_BEGIN_TRANS) != 0 || (redoLogRecord2->dba == rollbackRedoLogRecord1->dba && redoLogRecord2->slot == rollbackRedoLogRecord1->slot)));
        return val;
    }

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

    ostream& operator<<(ostream& os, const Transaction& tran) {
        uint64_t tcCount = 0, tcSumSize = 0;
        TransactionChunk *tc = tran.firstTc;
        while (tc != nullptr) {
            tcSumSize += tc->size;
            ++tcCount;
            tc = tc->next;
        }

        os << "scn: " << dec << tran.firstScn << "-" << tran.lastScn <<
                " xid: " << PRINTXID(tran.xid) <<
                " flags: " << dec << tran.isBegin << "/" << tran.isCommit << "/" << tran.isRollback <<
                " op: " << dec << tran.opCodes <<
                " chunks: " << dec << tcCount <<
                " sz: " << tcSumSize;
        return os;
    }
}
