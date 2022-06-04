/* Class with main redo log parser
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
#include "../common/OracleObject.h"
#include "../common/RedoLogException.h"
#include "../common/Timer.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "../reader/Reader.h"
#include "OpCode0501.h"
#include "OpCode0502.h"
#include "OpCode0504.h"
#include "OpCode0506.h"
#include "OpCode050B.h"
#include "OpCode0513.h"
#include "OpCode0514.h"
#include "OpCode0B02.h"
#include "OpCode0B03.h"
#include "OpCode0B04.h"
#include "OpCode0B05.h"
#include "OpCode0B06.h"
#include "OpCode0B08.h"
#include "OpCode0B0B.h"
#include "OpCode0B0C.h"
#include "OpCode0B10.h"
#include "OpCode0B16.h"
#include "OpCode1801.h"
#include "Parser.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    Parser::Parser(Ctx* ctx, Builder* builder, Metadata* metadata, TransactionBuffer* transactionBuffer, int64_t group, std::string& path) :
            ctx(ctx),
            builder(builder),
            metadata(metadata),
            transactionBuffer(transactionBuffer),
            lwnAllocated(0),
            lwnAllocatedMax(0),
            lwnTimestamp(0),
            lwnScn(0),
            lwnCheckpointBlock(0),
            group(group),
            path(path),
            sequence(0),
            firstScn(ZERO_SCN),
            nextScn(ZERO_SCN),
            reader(nullptr) {

        memset(&zero, 0, sizeof(RedoLogRecord));

        lwnChunks[0] = ctx->getMemoryChunk("parser", false);
        auto length = (uint64_t*)lwnChunks[0];
        *length = sizeof(uint64_t);
        lwnAllocated = 1;
        lwnAllocatedMax = 1;
    }

    Parser::~Parser() {
        while (lwnAllocated > 0) {
            ctx->freeMemoryChunk("parser", lwnChunks[--lwnAllocated], false);
        }
    }

    void Parser::freeLwn() {
        while (lwnAllocated > 1) {
            ctx->freeMemoryChunk("parser", lwnChunks[--lwnAllocated], false);
        }

        auto length = (uint64_t*)lwnChunks[0];
        *length = sizeof(uint64_t);
    }

    void Parser::analyzeLwn(LwnMember* lwnMember) {
        TRACE(TRACE2_LWN, "LWN: analyze blk: " << std::dec << lwnMember->block << " offset: " << lwnMember->offset <<
                                               " scn: " << lwnMember->scn << " subscn: " << lwnMember->subScn)

        uint8_t *data = ((uint8_t *) lwnMember) + sizeof(struct LwnMember);
        RedoLogRecord redoLogRecord[2];
        int64_t vectorCur = -1;
        int64_t vectorPrev = -1;
        TRACE(TRACE2_LWN, "LWN: analyze length: " << std::dec << lwnMember->length << " scn: " << lwnMember->scn << " subScn: " << lwnMember->subScn)

        uint32_t recordLength = ctx->read32(data);
        uint8_t vld = data[4];
        uint64_t headerLength;

        if (recordLength != lwnMember->length)
            throw RedoLogException( "block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                   ": too small log record, buffer length: " + std::to_string(lwnMember->length) + ", field length: " +
                                   std::to_string(recordLength));

        if ((vld & 0x04) != 0)
            headerLength = 68;
        else
            headerLength = 24;

        if (ctx->dumpRedoLog >= 1) {
            uint16_t thread = 1; //TODO: verify field length/position
            ctx->dumpStream << " " << std::endl;

            if (ctx->version < REDO_VERSION_12_1)
                ctx->dumpStream << "REDO RECORD - Thread:" << thread <<
                                " RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence << "." <<
                                std::setfill('0') << std::setw(8) << std::hex << lwnMember->block << "." <<
                                std::setfill('0') << std::setw(4) << std::hex << lwnMember->offset <<
                                " LEN: 0x" << std::setfill('0') << std::setw(4) << std::hex << recordLength <<
                                " VLD: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t) vld
                                << std::endl;
            else {
                uint32_t conUid = ctx->read32(data + 16);
                ctx->dumpStream << "REDO RECORD - Thread:" << thread <<
                                " RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence << "." <<
                                std::setfill('0') << std::setw(8) << std::hex << lwnMember->block << "." <<
                                std::setfill('0') << std::setw(4) << std::hex << lwnMember->offset <<
                                " LEN: 0x" << std::setfill('0') << std::setw(4) << std::hex << recordLength <<
                                " VLD: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t) vld <<
                                " CON_UID: " << std::dec << conUid << std::endl;
            }

            if (ctx->dumpRawData > 0) {
                ctx->dumpStream << "##: " << std::dec << headerLength;
                for (uint64_t j = 0; j < headerLength; ++j) {
                    if ((j & 0x0F) == 0)
                        ctx->dumpStream << std::endl << "##  " << std::setfill(' ') << std::setw(2) << std::hex << j
                                        << ": ";
                    if ((j & 0x07) == 0)
                        ctx->dumpStream << " ";
                    ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << (uint64_t) data[j] << " ";
                }
                ctx->dumpStream << std::endl;
            }

            if (headerLength == 68) {
                if (ctx->version < REDO_VERSION_12_2)
                    ctx->dumpStream << "SCN: " << PRINTSCN48(lwnMember->scn) << " SUBSCN:" << std::setfill(' ')
                                    << std::setw(3) << std::dec << lwnMember->subScn << " " << lwnTimestamp
                                    << std::endl;
                else
                    ctx->dumpStream << "SCN: " << PRINTSCN64(lwnMember->scn) << " SUBSCN:" << std::setfill(' ')
                                    << std::setw(3) << std::dec << lwnMember->subScn << " " << lwnTimestamp
                                    << std::endl;
                uint16_t lwnNst = ctx->read16(data + 26);
                uint32_t lwnLen = ctx->read32(data + 32);

                if (ctx->version < REDO_VERSION_12_2)
                    ctx->dumpStream << "(LWN RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence
                                    << "." <<
                                    std::setfill('0') << std::setw(8) << std::hex << lwnMember->block << "." <<
                                    std::setfill('0') << std::setw(4) << std::hex << lwnMember->offset <<
                                    " LEN: " << std::setfill('0') << std::setw(4) << std::dec << lwnLen <<
                                    " NST: " << std::setfill('0') << std::setw(4) << std::dec << lwnNst <<
                                    " SCN: " << PRINTSCN48(lwnScn) << ")" << std::endl;
                else
                    ctx->dumpStream << "(LWN RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence
                                    << "." <<
                                    std::setfill('0') << std::setw(8) << std::hex << lwnMember->block << "." <<
                                    std::setfill('0') << std::setw(4) << std::hex << lwnMember->offset <<
                                    " LEN: 0x" << std::setfill('0') << std::setw(8) << std::hex << lwnLen <<
                                    " NST: 0x" << std::setfill('0') << std::setw(4) << std::hex << lwnNst <<
                                    " SCN: " << PRINTSCN64(lwnScn) << ")" << std::endl;
            } else {
                if (ctx->version < REDO_VERSION_12_2)
                    ctx->dumpStream << "SCN: " << PRINTSCN48(lwnMember->scn) << " SUBSCN:" << std::setfill(' ')
                                    << std::setw(3) << std::dec << lwnMember->subScn << " " << lwnTimestamp
                                    << std::endl;
                else
                    ctx->dumpStream << "SCN: " << PRINTSCN64(lwnMember->scn) << " SUBSCN:" << std::setfill(' ')
                                    << std::setw(3) << std::dec << lwnMember->subScn << " " << lwnTimestamp
                                    << std::endl;
            }
        }

        if (headerLength > recordLength) {
            dumpRedoVector(data, recordLength);
            throw RedoLogException("block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                   ": too small log record, header length: " + std::to_string(headerLength) + ", field length: " +
                                   std::to_string(recordLength));
        }

        uint64_t offset = headerLength;
        uint64_t vectors = 0;
        while (offset < recordLength) {
            vectorPrev = vectorCur;
            if (vectorPrev == -1)
                vectorCur = 0;
            else
                vectorCur = 1 - vectorPrev;

            memset(&redoLogRecord[vectorCur], 0, sizeof(RedoLogRecord));
            redoLogRecord[vectorCur].vectorNo = (++vectors);
            redoLogRecord[vectorCur].cls = ctx->read16(data + offset + 2);
            redoLogRecord[vectorCur].afn = ctx->read32(data + offset + 4) & 0xFFFF;
            redoLogRecord[vectorCur].dba = ctx->read32(data + offset + 8);
            redoLogRecord[vectorCur].scnRecord = ctx->readScn(data + offset + 12);
            redoLogRecord[vectorCur].rbl = 0; //TODO: verify field length/position
            redoLogRecord[vectorCur].seq = data[offset + 20];
            redoLogRecord[vectorCur].typ = data[offset + 21];
            typeUsn usn = (redoLogRecord[vectorCur].cls >= 15) ? (redoLogRecord[vectorCur].cls - 15) / 2 : -1;

            uint64_t fieldOffset;
            if (ctx->version >= REDO_VERSION_12_1) {
                fieldOffset = 32;
                redoLogRecord[vectorCur].flgRecord = ctx->read16(data + offset + 28);
                redoLogRecord[vectorCur].conId = (typeConId)ctx->read16(data + offset + 24);
            } else {
                fieldOffset = 24;
                redoLogRecord[vectorCur].flgRecord = 0;
                redoLogRecord[vectorCur].conId = 0;
            }

            if (offset + fieldOffset + 1 >= recordLength) {
                dumpRedoVector(data, recordLength);
                throw RedoLogException("block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                       ": position of field list (" + std::to_string(offset + fieldOffset + 1) + ") outside of record, length: " +
                                       std::to_string(recordLength));
            }

            uint8_t* fieldList = data + offset + fieldOffset;

            redoLogRecord[vectorCur].opCode = (((typeOp1)data[offset + 0]) << 8) | data[offset + 1];
            redoLogRecord[vectorCur].length = fieldOffset + ((ctx->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectorCur].sequence = sequence;
            redoLogRecord[vectorCur].scn = lwnMember->scn;
            redoLogRecord[vectorCur].subScn = lwnMember->subScn;
            redoLogRecord[vectorCur].usn = usn;
            redoLogRecord[vectorCur].data = data + offset;
            redoLogRecord[vectorCur].dataOffset = lwnMember->block * reader->getBlockSize() + lwnMember->offset + offset;
            redoLogRecord[vectorCur].fieldLengthsDelta = fieldOffset;
            if (redoLogRecord[vectorCur].fieldLengthsDelta + 1 >= recordLength) {
                dumpRedoVector(data, recordLength);
                throw RedoLogException("block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                       ": field length list (" + std::to_string(redoLogRecord[vectorCur].fieldLengthsDelta) +
                                       ") outside of record, length: " + std::to_string(recordLength));
            }
            redoLogRecord[vectorCur].fieldCnt = (ctx->read16(redoLogRecord[vectorCur].data + redoLogRecord[vectorCur].fieldLengthsDelta) - 2) / 2;
            redoLogRecord[vectorCur].fieldPos = fieldOffset + ((ctx->read16(redoLogRecord[vectorCur].data + redoLogRecord[vectorCur].fieldLengthsDelta) + 2) & 0xFFFC);
            if (redoLogRecord[vectorCur].fieldPos >= recordLength) {
                dumpRedoVector(data, recordLength);
                throw RedoLogException("block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                       ": fields (" + std::to_string(redoLogRecord[vectorCur].fieldPos) + ") outside of record, length: " +
                                       std::to_string(recordLength));
            }

            uint64_t fieldPos = redoLogRecord[vectorCur].fieldPos;
            for (uint64_t i = 1; i <= redoLogRecord[vectorCur].fieldCnt; ++i) {
                redoLogRecord[vectorCur].length += (ctx->read16(fieldList + i * 2) + 3) & 0xFFFC;
                fieldPos += (ctx->read16(redoLogRecord[vectorCur].data + redoLogRecord[vectorCur].fieldLengthsDelta + i * 2) + 3) & 0xFFFC;

                if (offset + redoLogRecord[vectorCur].length > recordLength) {
                    dumpRedoVector(data, recordLength);
                    throw RedoLogException("block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                           ": position of field list outside of record (" +
                                           "i: " + std::to_string(i) +
                                           " c: " + std::to_string(redoLogRecord[vectorCur].fieldCnt) + " " +
                                           " o: " + std::to_string(fieldOffset) +
                                           " p: " + std::to_string(offset) +
                                           " l: " + std::to_string(redoLogRecord[vectorCur].length) +
                                           " r: " + std::to_string(recordLength) + ")");
                }
            }

            if (redoLogRecord[vectorCur].fieldPos > redoLogRecord[vectorCur].length) {
                dumpRedoVector(data, recordLength);
                throw RedoLogException("block: " + std::to_string(lwnMember->block) + ", offset: " + std::to_string(lwnMember->offset) +
                                       ": incomplete record, offset: " + std::to_string(redoLogRecord[vectorCur].fieldPos) + ", length: " +
                                       std::to_string(redoLogRecord[vectorCur].length));
            }

            redoLogRecord[vectorCur].recordObj = 0xFFFFFFFF;
            redoLogRecord[vectorCur].recordDataObj = 0xFFFFFFFF;

            offset += redoLogRecord[vectorCur].length;

            switch (redoLogRecord[vectorCur].opCode) {
            case 0x0501: //Undo
                OpCode0501::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0502: //Begin transaction
                OpCode0502::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0504: //Commit/rollback transaction
                OpCode0504::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0506: //Partial rollback
                OpCode0506::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x050B:
                OpCode050B::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0513: //Session information
                OpCode0513::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0514: //Session information
                OpCode0514::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B02: //REDO: Insert row piece
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B02::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B03: //REDO: Delete row piece
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B03::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B04: //REDO: Lock row piece
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B04::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B05: //REDO: Update row piece
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B05::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B06: //REDO: Overwrite row piece
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B06::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B08: //REDO: Change forwarding address
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B08::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B0B: //REDO: Insert multiple rows
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B0B::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B0C: //REDO: Delete multiple rows
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B0C::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B10: //REDO: Supplemental log for update
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B10::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x0B16: //REDO: Logminer support - KDOCMP
                if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                    redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                    redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                }
                OpCode0B16::process(ctx, &redoLogRecord[vectorCur]);
                break;

            case 0x1801: //DDL
                OpCode1801::process(ctx, &redoLogRecord[vectorCur]);
                break;

            default:
                OpCode::process(ctx, &redoLogRecord[vectorCur]);
                break;
            }

            TRACE(TRACE2_DUMP, "DUMP: op: " << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord[vectorCur].opCode <<
                  " obj: " << std::dec << redoLogRecord[vectorCur].recordObj <<
                  " flg: " << std::hex << redoLogRecord[vectorCur].flg)

            if (vectorPrev != -1) {
                //UNDO - data
                if (redoLogRecord[vectorPrev].opCode == 0x0501 && (redoLogRecord[vectorCur].opCode & 0xFF00) == 0x0B00) {
                    appendToTransaction(&redoLogRecord[vectorPrev], &redoLogRecord[vectorCur]);
                    vectorCur = -1;
                    continue;
                }

                //UNDO - index, ignore
                if (redoLogRecord[vectorPrev].opCode == 0x0501 && (redoLogRecord[vectorCur].opCode & 0xFF00) == 0x0A00) {
                    vectorCur = -1;
                    continue;
                }

                //ROLLBACK - data
                if ((redoLogRecord[vectorPrev].opCode & 0xFF00) == 0x0B00 && (redoLogRecord[vectorCur].opCode == 0x0506 || redoLogRecord[vectorCur].opCode == 0x050B)) {
                    appendToTransaction(&redoLogRecord[vectorPrev], &redoLogRecord[vectorCur]);
                    vectorCur = -1;
                    continue;
                }

                //ROLLBACK - index, ignore
                if ((redoLogRecord[vectorPrev].opCode & 0xFF00) == 0x0A00 && (redoLogRecord[vectorCur].opCode == 0x0506 || redoLogRecord[vectorCur].opCode == 0x050B)) {
                    vectorCur = -1;
                    continue;
                }
            }

            //UNDO - data
            if (redoLogRecord[vectorCur].opCode == 0x0501 && (redoLogRecord[vectorCur].flg & (FLG_MULTIBLOCKUNDOTAIL | FLG_MULTIBLOCKUNDOMID)) != 0) {
                appendToTransactionUndo(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            //ROLLBACK - data
            if (redoLogRecord[vectorCur].opCode == 0x0506 || redoLogRecord[vectorCur].opCode == 0x050B) {
                appendToTransactionUndo(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            //BEGIN
            if (redoLogRecord[vectorCur].opCode == 0x0502) {
                appendToTransactionBegin(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            //COMMIT
            if (redoLogRecord[vectorCur].opCode == 0x0504) {
                appendToTransactionCommit(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            //DDL
            if (redoLogRecord[vectorCur].opCode == 0x1801) {
                appendToTransactionDdl(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }
        }
    }

    void Parser::appendToTransactionDdl(RedoLogRecord* redoLogRecord1) {
        bool system = false;
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord1)

        //track DDL
        if (!FLAG(REDO_FLAGS_TRACK_DDL))
            return;

        //skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        OracleObject* object = metadata->schema->checkDict(redoLogRecord1->obj, redoLogRecord1->dataObj);
        if (!FLAG(REDO_FLAGS_SCHEMALESS) && object == nullptr)
            return;

        if (object != nullptr && (object->options & OPTIONS_SYSTEM_TABLE) != 0)
            system = true;

        Transaction* transaction = transactionBuffer->findTransaction(redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, FLAG(REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;

        if (system)
            transaction->system = true;

        //transaction size limit
        if (ctx->transactionSizeMax > 0 &&
            transaction->size + redoLogRecord1->length + ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
            transactionBuffer->skipXidList.insert(transaction->xid);
            transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
            transaction->purge(transactionBuffer);
            delete transaction;
            return;
        }

        transaction->add(transactionBuffer, redoLogRecord1, &zero);
    }

    void Parser::appendToTransactionUndo(RedoLogRecord* redoLogRecord1) {
        bool system = false;
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord1)

        if (redoLogRecord1->opCode == 0x0501) {
            if ((redoLogRecord1->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0)
                return;

            //skip list
            if (redoLogRecord1->xid.getVal() != 0 && transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
                return;

            OracleObject* object = metadata->schema->checkDict(redoLogRecord1->obj, redoLogRecord1->dataObj);
            if (!FLAG(REDO_FLAGS_SCHEMALESS) && object == nullptr)
                return;

            if (object != nullptr && (object->options & OPTIONS_SYSTEM_TABLE) != 0)
                system = true;

            Transaction* transaction = transactionBuffer->findTransaction(redoLogRecord1->xid, redoLogRecord1->conId,
                                                                                   true, FLAG(REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
            if (transaction == nullptr)
                return;

            if (system)
                transaction->system = true;

            //cluster key
            if ((redoLogRecord1->fb & FB_K) != 0)
                return;

            //partition move
            if ((redoLogRecord1->suppLogFb & FB_K) != 0)
                return;

            //transaction size limit
            if (ctx->transactionSizeMax > 0 &&
                transaction->size + redoLogRecord1->length + ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
                transactionBuffer->skipXidList.insert(transaction->xid);
                transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
                transaction->purge(transactionBuffer);
                delete transaction;
                return;
            }

            transaction->add(transactionBuffer, redoLogRecord1);
        } if ((redoLogRecord1->opCode == 0x0506 || redoLogRecord1->opCode == 0x050B) && (redoLogRecord1->opc == 0x0A16 || redoLogRecord1->opc == 0x0B01)) {
            typeXid xid(redoLogRecord1->usn, redoLogRecord1->slt,  0);
            Transaction* transaction = transactionBuffer->findTransaction(xid, redoLogRecord1->conId, true, false, true);
            if (transaction != nullptr) {
                transaction->rollbackLastOp(transactionBuffer, redoLogRecord1);
            } else {
                typeXidMap xidMap = (redoLogRecord1->xid.getVal() >> 32) | (((uint64_t)redoLogRecord1->conId) << 32);
                auto iter = transactionBuffer->brokenXidMapList.find(xidMap);
                if (iter == transactionBuffer->brokenXidMapList.end()) {
                    WARNING("no match found for transaction rollback, skipping, SLT: " << std::dec << (uint64_t)redoLogRecord1->slt <<
                                                                                       " USN: " << (uint64_t)redoLogRecord1->usn)
                    transactionBuffer->brokenXidMapList.insert(xidMap);
                }
            }
        }
    }

    void Parser::appendToTransactionBegin(RedoLogRecord* redoLogRecord1) {
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord1)

        //skip SQN cleanup
        if (redoLogRecord1->xid.sqn() == 0)
            return;

        Transaction* transaction = transactionBuffer->findTransaction(redoLogRecord1->xid, redoLogRecord1->conId, false, true, false);
        transaction->begin = true;
        transaction->firstSequence = sequence;
        transaction->firstOffset = lwnCheckpointBlock * reader->getBlockSize();
    }

    void Parser::appendToTransactionCommit(RedoLogRecord* redoLogRecord1) {
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord1)

        //skip list
        auto it = transactionBuffer->skipXidList.find(redoLogRecord1->xid);
        if (it != transactionBuffer->skipXidList.end()) {
            transactionBuffer->skipXidList.erase(it);
            return;
        }

        //broken transaction
        typeXidMap xidMap = (redoLogRecord1->xid.getVal() >> 32) | (((uint64_t)redoLogRecord1->conId) << 32);
        auto iter = transactionBuffer->brokenXidMapList.find(xidMap);
        if (iter != transactionBuffer->brokenXidMapList.end())
            transactionBuffer->brokenXidMapList.erase(xidMap);

        Transaction* transaction = transactionBuffer->findTransaction(redoLogRecord1->xid, redoLogRecord1->conId,
                                                                               true, FLAG(REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;

        transaction->commitTimestamp = lwnTimestamp;
        transaction->commitScn = redoLogRecord1->scnRecord;
        transaction->commitSequence = redoLogRecord1->sequence;
        if ((redoLogRecord1->flg & FLG_ROLLBACK_OP0504) != 0)
            transaction->rollback = true;

        if ((transaction->commitScn > metadata->firstDataScn && !transaction->system) ||
            (transaction->commitScn > metadata->firstSchemaScn && transaction->system)) {

            if (transaction->begin) {
                transaction->flush(metadata, transactionBuffer, builder);

                if (ctx->stopTransactions > 0) {
                    --ctx->stopTransactions;
                    if (ctx->stopTransactions == 0) {
                        INFO("shutdown started - exhausted number of transactions")
                        ctx->stopSoft();
                    }
                }

                if (transaction->shutdown) {
                    INFO("shutdown started - initiated by debug transaction " << transaction->xid << " at scn " << std::dec << transaction->commitScn)
                    ctx->stopSoft();
                }
            } else {
                WARNING("skipping transaction with no begin: " << *transaction)
            }
        } else {
            DEBUG("skipping transaction already committed: " << *transaction)
        }

        transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
        transaction->purge(transactionBuffer);
        delete transaction;
    }

    void Parser::appendToTransaction(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        bool system = false;
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord1)
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord2)

        //skip other PDB vectors
        if (metadata->conId > 0 && redoLogRecord2->conId != metadata->conId && redoLogRecord1->opCode == 0x0501)
            return;

        if (metadata->conId > 0 && redoLogRecord1->conId != metadata->conId &&
            (redoLogRecord2->opCode == 0x0506 || redoLogRecord2->opCode == 0x050B))
            return;

        //skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        typeObj obj;
        typeDataObj dataObj;

        if (redoLogRecord1->dataObj != 0) {
            obj = redoLogRecord1->obj;
            dataObj = redoLogRecord1->dataObj;
            redoLogRecord2->obj = redoLogRecord1->obj;
            redoLogRecord2->dataObj = redoLogRecord1->dataObj;
        } else {
            obj = redoLogRecord2->obj;
            dataObj = redoLogRecord2->dataObj;
            redoLogRecord1->obj = redoLogRecord2->obj;
            redoLogRecord1->dataObj = redoLogRecord2->dataObj;
        }

        if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0)
            throw RedoLogException("BDBA does not match (" + std::to_string(redoLogRecord1->bdba) + ", " +
                                   std::to_string(redoLogRecord2->bdba) + ")");

        OracleObject *object = metadata->schema->checkDict(obj, dataObj);
        if (!FLAG(REDO_FLAGS_SCHEMALESS) && object == nullptr)
            return;

        if (object != nullptr && (object->options & OPTIONS_SYSTEM_TABLE) != 0)
            system = true;

        //cluster key
        if ((redoLogRecord1->fb & FB_K) != 0 || (redoLogRecord2->fb & FB_K) != 0)
            return;

        //partition move
        if ((redoLogRecord1->suppLogFb & FB_K) != 0 || (redoLogRecord2->suppLogFb & FB_K) != 0)
            return;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        switch (opCodeLong) {
        //insert row piece
        case 0x05010B02:
        //delete row piece
        case 0x05010B03:
        //update row piece
        case 0x05010B05:
        //overwrite row piece
        case 0x05010B06:
        //change forwarding address
        case 0x05010B08:
        //insert multiple rows
        case 0x05010B0B:
        //delete multiple rows
        case 0x05010B0C:
        //supp log for update
        case 0x05010B10:
        //logminer support - KDOCMP
        case 0x05010B16:
            {
                Transaction* transaction = transactionBuffer->findTransaction(redoLogRecord1->xid, redoLogRecord1->conId,
                        true, FLAG(REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
                if (transaction == nullptr)
                    break;

                if (system)
                    transaction->system = true;

                //transaction size limit
                if (ctx->transactionSizeMax > 0 &&
                        transaction->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
                    transactionBuffer->skipXidList.insert(transaction->xid);
                    transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
                    transaction->purge(transactionBuffer);
                    delete transaction;
                    return;
                }

                transaction->add(transactionBuffer, redoLogRecord1, redoLogRecord2);

                if (object != nullptr && (object->options & OPTIONS_DEBUG_TABLE) != 0 && opCodeLong == 0x05010B02 && !ctx->softShutdown)
                    transaction->shutdown = true;
            }
            break;

        //rollback: delete row piece
        case 0x0B030506:
        case 0x0B03050B:
        //rollback: delete multiple rows
        case 0x0B0C0506:
        case 0x0B0C050B:
        //rollback: insert row piece
        case 0x0B020506:
        case 0x0B02050B:
        //rollback: insert multiple row
        case 0x0B0B0506:
        case 0x0B0B050B:
        //rollback: update row piece
        case 0x0B050506:
        case 0x0B05050B:
        //rollback: overwrite row piece
        case 0x0B060506:
        case 0x0B06050B:
        //change forwarding address
        case 0x0B080506:
        case 0x0B08050B:
        //rollback: supp log for update
        case 0x0B100506:
        case 0x0B10050B:
        //rollback: logminer support - KDOCMP
        case 0x0B160506:
        case 0x0B16050B:
            {
                typeXid xid(redoLogRecord2->usn, redoLogRecord2->slt, 0);
                Transaction* transaction = transactionBuffer->findTransaction(xid, redoLogRecord2->conId, true, false, true);
                if (transaction != nullptr) {
                    transaction->rollbackLastOp(transactionBuffer, redoLogRecord1, redoLogRecord2);
                } else {
                    typeXidMap xidMap = (redoLogRecord2->xid.getVal() >> 32) | (((uint64_t)redoLogRecord2->conId) << 32);
                    auto iter = transactionBuffer->brokenXidMapList.find(xidMap);
                    if (iter == transactionBuffer->brokenXidMapList.end()) {
                        WARNING("no match found for transaction rollback, skipping, SLT: " << std::dec << (uint64_t)redoLogRecord2->slt <<
                                " USN: " << (uint64_t)redoLogRecord2->usn)
                        transactionBuffer->brokenXidMapList.insert(xidMap);
                    }
                }
                if (system)
                    transaction->system = true;
            }

            break;
        }
    }

    void Parser::dumpRedoVector(uint8_t* data, uint64_t recordLength) const {
        if (ctx->trace >= TRACE_WARNING) {
            std::stringstream ss;
            ss << "dumping redo vector" << std::endl;
            ss << "##: " << std::dec << recordLength;
            for (uint64_t j = 0; j < recordLength; ++j) {
                if ((j & 0x0F) == 0)
                    ss << std::endl << "##  " << std::setfill(' ') << std::setw(2) << std::hex << j << ": ";
                if ((j & 0x07) == 0)
                    ss << " ";
                ss << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)data[j] << " ";
            }
            WARNING(ss.str())
        }
    }

    uint64_t Parser::parse() {
        uint64_t lwnConfirmedBlock = 2;
        uint64_t lwnRecords = 0;

        if (firstScn == ZERO_SCN && nextScn == ZERO_SCN && reader->getFirstScn() != 0) {
            firstScn = reader->getFirstScn();
            nextScn = reader->getNextScn();
        }
        ctx->suppLogSize = 0;

        if (reader->getBufferStart() == reader->getBlockSize() * 2) {
            if (ctx->dumpRedoLog >= 1) {
                std::string fileName = ctx->dumpPath + "/" + std::to_string(sequence) + ".olr";
                ctx->dumpStream.open(fileName);
                if (!ctx->dumpStream.is_open()) {
                    WARNING("can't open " << fileName << " for write. Aborting log dump.")
                    ctx->dumpRedoLog = 0;
                }
                std::stringstream ss;
                reader->printHeaderInfo(ss, path);
                ctx->dumpStream << ss.str();
            }
        }

        //continue started offset
        if (metadata->offset > 0) {
            if ((metadata->offset % reader->getBlockSize()) != 0)
                throw RedoLogException("incorrect offset start: " + std::to_string(metadata->offset) +
                                       " - not a multiplication of block size: " +
                                       std::to_string(reader->getBlockSize()));

            lwnConfirmedBlock = metadata->offset / reader->getBlockSize();
            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: setting reader start position to " << std::dec << metadata->offset <<
                                                                                     " (block " << std::dec
                                                                                     << lwnConfirmedBlock << ")")
            metadata->offset = 0;
        }
        reader->setBufferStartEnd(lwnConfirmedBlock * reader->getBlockSize(),
                                  lwnConfirmedBlock * reader->getBlockSize());

        INFO("processing redo log: " << *this << " offset: " << std::dec << reader->getBufferStart())
        if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA) && !metadata->schema->loaded && ctx->versionStr.length() > 0) {
            metadata->loadAdaptiveSchema();
            metadata->schema->loaded = true;
        }

        if (metadata->resetlogs == 0)
            metadata->setResetlogs(reader->getResetlogs());

        if (metadata->resetlogs != reader->getResetlogs())
            throw RedoLogException("invalid resetlogs value (found: " + std::to_string(reader->getResetlogs()) + ", expected: " +
                                   std::to_string(metadata->resetlogs) + "): " + reader->fileName);

        if (metadata->activation == 0 || metadata->activation != reader->getActivation()) {
            INFO("new activation detected: " << std::dec << reader->getActivation())
            metadata->setActivation(reader->getActivation());
        }

        time_t cStart = Timer::getTime();
        reader->setStatusRead();
        LwnMember* lwnMember;
        uint64_t currentBlock = lwnConfirmedBlock;
        uint64_t blockOffset = 16;
        uint64_t startBlock = lwnConfirmedBlock;
        uint64_t confirmedBufferStart = reader->getBufferStart();
        uint64_t recordLength4 = 0;
        uint64_t recordPos = 0;
        uint64_t recordLeftToCopy = 0;
        uint64_t lwnEndBlock = lwnConfirmedBlock;
        uint64_t lwnStartBlock = lwnConfirmedBlock;
        uint16_t lwnNum = 0;
        uint16_t lwnNumMax = 0;
        uint16_t lwnNumCur = 0;
        uint16_t lwnNumCnt = 0;
        lwnCheckpointBlock = lwnConfirmedBlock;
        bool switchRedo = false;

        while (!ctx->softShutdown) {
            //there is some work to do
            while (confirmedBufferStart < reader->getBufferEnd()) {
                uint64_t redoBufferPos = (currentBlock * reader->getBlockSize()) % MEMORY_CHUNK_SIZE;
                uint64_t redoBufferNum = ((currentBlock * reader->getBlockSize()) / MEMORY_CHUNK_SIZE) % ctx->readBufferMax;
                uint8_t* redoBlock = reader->redoBufferList[redoBufferNum] + redoBufferPos;

                blockOffset = 16;
                //new LWN block
                if (currentBlock == lwnEndBlock) {
                    uint8_t vld = redoBlock[blockOffset + 4];

                    if ((vld & 0x04) != 0) {
                        lwnNum = ctx->read16(redoBlock + blockOffset + 24);
                        uint32_t lwnLength = ctx->read32(redoBlock + blockOffset + 28);
                        lwnStartBlock = currentBlock;
                        lwnEndBlock = currentBlock + lwnLength;
                        lwnScn = ctx->readScn(redoBlock + blockOffset + 40);
                        lwnTimestamp = ctx->read32(redoBlock + blockOffset + 64);

                        if (lwnNumCnt == 0) {
                            lwnCheckpointBlock = currentBlock;
                            lwnNumMax = ctx->read16(redoBlock + blockOffset + 26);
                            //verify LWN header start
                            if (lwnScn < reader->getFirstScn() || (lwnScn > reader->getNextScn() && reader->getNextScn() != ZERO_SCN))
                                throw RedoLogException("invalid LWN SCN: " + std::to_string(lwnScn));
                        } else {
                            lwnNumCur = ctx->read16(redoBlock + blockOffset + 26);
                            if (lwnNumCur != lwnNumMax)
                                throw RedoLogException("invalid LWN MAX: " + std::to_string(lwnNum) + "/" + std::to_string(lwnNumCur) + "/" +
                                                       std::to_string(lwnNumMax));
                        }
                        ++lwnNumCnt;

                        TRACE(TRACE2_LWN, "LWN: at: " << std::dec << lwnStartBlock << " length: " << lwnLength << " chk: " << std::dec << lwnNum << " max: " << lwnNumMax)

                    } else
                        throw RedoLogException("did not find LWN at offset: " + std::to_string(confirmedBufferStart));
                }

                while (blockOffset < reader->getBlockSize()) {
                    //next record
                    if (recordLeftToCopy == 0) {
                        if (blockOffset + 20 >= reader->getBlockSize())
                            break;

                        recordLength4 = (((uint64_t)ctx->read32(redoBlock + blockOffset)) + 3) & 0xFFFFFFFC;
                        if (recordLength4 > 0) {
                            uint64_t* length = (uint64_t*) (lwnChunks[lwnAllocated - 1]);

                            if (((*length + sizeof(struct LwnMember) + recordLength4 + 7) & 0xFFFFFFF8) > MEMORY_CHUNK_SIZE_MB * 1024 * 1024) {
                                if (lwnAllocated == MAX_LWN_CHUNKS)
                                    throw RedoLogException("all " + std::to_string(MAX_LWN_CHUNKS) + " LWN buffers allocated");

                                lwnChunks[lwnAllocated++] = ctx->getMemoryChunk("parser", false);
                                if (lwnAllocated > lwnAllocatedMax)
                                    lwnAllocatedMax = lwnAllocated;
                                length = (uint64_t*) (lwnChunks[lwnAllocated - 1]);
                                *length = sizeof(uint64_t);
                            }

                            if (((*length + sizeof(struct LwnMember) + recordLength4 + 7) & 0xFFFFFFF8) > MEMORY_CHUNK_SIZE_MB * 1024 * 1024)
                                throw RedoLogException("Too big redo log record, length: " + std::to_string(recordLength4));

                            lwnMember = (struct LwnMember*) (lwnChunks[lwnAllocated - 1] + *length);
                            *length += (sizeof(struct LwnMember) + recordLength4 + 7) & 0xFFFFFFF8;
                            lwnMember->scn = ctx->read32(redoBlock + blockOffset + 8) |
                                             ((uint64_t)(ctx->read16(redoBlock + blockOffset + 6)) << 32);
                            lwnMember->subScn = ctx->read16(redoBlock + blockOffset + 12);
                            lwnMember->block = currentBlock;
                            lwnMember->offset = blockOffset;
                            lwnMember->length = recordLength4;
                            TRACE(TRACE2_LWN, "LWN: length: " << std::dec << recordLength4 << " scn: " << lwnMember->scn << " subScn: " << lwnMember->subScn)

                            uint64_t lwnPos = lwnRecords++;
                            if (lwnPos >= MAX_RECORDS_IN_LWN)
                                throw RedoLogException("all " + std::to_string(lwnPos) + " records in LWN were used");

                            while (lwnPos > 0 &&
                                    (lwnMembers[lwnPos - 1]->scn > lwnMember->scn ||
                                        (lwnMembers[lwnPos - 1]->scn == lwnMember->scn && lwnMembers[lwnPos - 1]->subScn > lwnMember->subScn))) {
                                lwnMembers[lwnPos] = lwnMembers[lwnPos - 1];
                                --lwnPos;
                            }
                            lwnMembers[lwnPos] = lwnMember;
                        }

                        recordLeftToCopy = recordLength4;
                        recordPos = 0;
                    }

                    //nothing more
                    if (recordLeftToCopy == 0)
                        break;

                    uint64_t toCopy;
                    if (blockOffset + recordLeftToCopy > reader->getBlockSize())
                        toCopy = reader->getBlockSize() - blockOffset;
                    else
                        toCopy = recordLeftToCopy;

                    memcpy(((uint8_t*)lwnMember) + sizeof(struct LwnMember) + recordPos, redoBlock + blockOffset, toCopy);
                    recordLeftToCopy -= toCopy;
                    blockOffset += toCopy;
                    recordPos += toCopy;
                }

                ++currentBlock;
                confirmedBufferStart += reader->getBlockSize();
                redoBufferPos += reader->getBlockSize();

                //checkpoint
                TRACE(TRACE2_LWN, "LWN: checkpoint at " << std::dec << currentBlock << "/" << lwnEndBlock << " num: " << lwnNumCnt << "/" << lwnNumMax)
                if (currentBlock == lwnEndBlock && lwnNumCnt == lwnNumMax) {
                    TRACE(TRACE2_LWN, "LWN: analyze")
                    for (uint64_t i = 0; i < lwnRecords; ++i) {
                        try {
                            analyzeLwn(lwnMembers[i]);
                        } catch (RedoLogException &ex) {
                            if (FLAG(REDO_FLAGS_IGNORE_DATA_ERRORS)) {
                                WARNING("forced to continue working in spite of error: " << ex.msg)
                            } else
                                throw RedoLogException("runtime error, aborting further redo log processing: " + ex.msg);
                        }
                    }

                    if (lwnScn > metadata->firstDataScn) {
                        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << lwnScn)
                        builder->processCheckpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->getBlockSize(), switchRedo);

                        typeSeq minSequence = ZERO_SEQ;
                        uint64_t minOffset = -1;
                        typeXid minXid;
                        transactionBuffer->checkpoint(minSequence, minOffset, minXid);
                        metadata->checkpoint(lwnScn, lwnTimestamp, currentBlock * reader->getBlockSize(),
                                             (currentBlock - lwnConfirmedBlock) * reader->getBlockSize(), minSequence, minOffset, minXid);

                        if (ctx->stopCheckpoints > 0) {
                            --ctx->stopCheckpoints;
                            if (ctx->stopCheckpoints == 0) {
                                INFO("shutdown started - exhausted number of checkpoints")
                                ctx->stopSoft();
                            }
                        }
                    }

                    lwnNumCnt = 0;
                    freeLwn();
                    lwnRecords = 0;
                    lwnConfirmedBlock = currentBlock;
                } else if (lwnNumCnt > lwnNumMax)
                    throw RedoLogException("LWN overflow: " + std::to_string(lwnNumCnt) + "/" + std::to_string(lwnNumMax));

                //free memory
                if (redoBufferPos == MEMORY_CHUNK_SIZE) {
                    redoBufferPos = 0;
                    reader->bufferFree(redoBufferNum);
                    if (++redoBufferNum == ctx->readBufferMax)
                        redoBufferNum = 0;
                    reader->confirmReadData(confirmedBufferStart);
                }
            }

            //processing finished
            if (!switchRedo && lwnScn > 0 && lwnScn > metadata->firstDataScn &&
                    confirmedBufferStart == reader->getBufferEnd() && reader->getRet() == REDO_FINISHED) {
                switchRedo = true;
                TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << lwnScn << " with switch")
                builder->processCheckpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->getBlockSize(), switchRedo);
            } else if (ctx->softShutdown) {
                TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << lwnScn << " at exit")
                builder->processCheckpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->getBlockSize(), false);
            }

            if (ctx->softShutdown) {
                reader->setRet(REDO_SHUTDOWN);
            } else {
                if (reader->checkFinished(confirmedBufferStart)) {
                    if (reader->getRet() == REDO_FINISHED && nextScn == ZERO_SCN && reader->getNextScn() != ZERO_SCN)
                        nextScn = reader->getNextScn();
                    if (reader->getRet() == REDO_STOPPED || reader->getRet() == REDO_OVERWRITTEN)
                        metadata->offset = lwnConfirmedBlock * reader->getBlockSize();
                    break;
                }
            }
        }

        //print performance information
        if ((ctx->trace2 & TRACE2_PERFORMANCE) != 0) {
            double suppLogPercent = 0.0;
            if (currentBlock != startBlock)
                suppLogPercent = 100.0 * ctx->suppLogSize / ((currentBlock - startBlock) * reader->getBlockSize());

            if (group == 0) {
                time_t cEnd = Timer::getTime();
                double mySpeed = 0;
                double myTime = (double)(cEnd - cStart) / 1000.0;
                if (myTime > 0)
                    mySpeed = (double)(currentBlock - startBlock) * reader->getBlockSize() * 1000.0 / 1024 / 1024 / myTime;

                double myReadSpeed = 0;
                if (reader->getSumTime() > 0)
                    myReadSpeed = ((double)reader->getSumRead() * 1000000.0 / 1024 / 1024 / (double)reader->getSumTime());

                TRACE(TRACE2_PERFORMANCE, "PERFORMANCE: " << myTime << " ms, " <<
                        "Speed: " << std::fixed << std::setprecision(2) << mySpeed << " MB/s, " <<
                        "Redo log size: " << std::dec << ((currentBlock - startBlock) * reader->getBlockSize() / 1024 / 1024) << " MB, " <<
                        "Read size: " << (reader->getSumRead() / 1024 / 1024) << " MB, " <<
                        "Read speed: " << myReadSpeed << " MB/s, " <<
                        "Max LWN size: " << std::dec << lwnAllocatedMax << ", " <<
                        "Supplemental redo log size: " << std::dec << ctx->suppLogSize << " bytes " <<
                        "(" << std::fixed << std::setprecision(2) << suppLogPercent << " %)")
            } else {
                TRACE(TRACE2_PERFORMANCE, "PERFORMANCE: " <<
                        "Redo log size: " << std::dec << ((currentBlock - startBlock) * reader->getBlockSize() / 1024 / 1024) << " MB, " <<
                        "Max LWN size: " << std::dec << lwnAllocatedMax << ", " <<
                        "Supplemental redo log size: " << std::dec << ctx->suppLogSize << " bytes " <<
                        "(" << std::fixed << std::setprecision(2) << suppLogPercent << " %)")
            }
        }

        if (ctx->dumpRedoLog >= 1 && ctx->dumpStream.is_open()) {
            ctx->dumpStream << "END OF REDO DUMP" << std::endl;
            ctx->dumpStream.close();
        }

        freeLwn();
        return reader->getRet();
    }

    std::ostream& operator<<(std::ostream& os, const Parser& parser) {
        os << "group: " << std::dec << parser.group << " scn: " << parser.firstScn << " to " <<
                ((parser.nextScn != ZERO_SCN) ? parser.nextScn : 0) << " seq: " << parser.sequence << " path: " << parser.path;
        return os;
    }
}
