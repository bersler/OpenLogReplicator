/* Class with main redo log parser
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
#include "../common/Clock.h"
#include "../common/LobCtx.h"
#include "../common/OracleLob.h"
#include "../common/OracleTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/RedoLogException.h"
#include "../common/metrics/Metrics.h"
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
#include "OpCode0A02.h"
#include "OpCode0A08.h"
#include "OpCode0A12.h"
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
#include "OpCode1301.h"
#include "OpCode1801.h"
#include "OpCode1A02.h"
#include "OpCode1A06.h"
#include "Parser.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    Parser::Parser(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer, int64_t newGroup,
                   const std::string& newPath) :
            ctx(newCtx),
            builder(newBuilder),
            metadata(newMetadata),
            transactionBuffer(newTransactionBuffer),
            lastTransaction(nullptr),
            lwnAllocated(0),
            lwnAllocatedMax(0),
            lwnTimestamp(0),
            lwnScn(0),
            lwnCheckpointBlock(0),
            group(newGroup),
            path(newPath),
            sequence(0),
            firstScn(Ctx::ZERO_SCN),
            nextScn(Ctx::ZERO_SCN),
            reader(nullptr) {

        memset(reinterpret_cast<void*>(&zero), 0, sizeof(RedoLogRecord));

        lwnChunks[0] = ctx->getMemoryChunk(Ctx::MEMORY_MODULE_PARSER, false);
        auto size = reinterpret_cast<uint64_t*>(lwnChunks[0]);
        *size = sizeof(uint64_t);
        lwnAllocated = 1;
        lwnAllocatedMax = 1;
        lwnMembers[0] = 0;
    }

    Parser::~Parser() {
        while (lwnAllocated > 0) {
            ctx->freeMemoryChunk(Ctx::MEMORY_MODULE_PARSER, lwnChunks[--lwnAllocated], false);
        }
    }

    void Parser::freeLwn() {
        while (lwnAllocated > 1) {
            ctx->freeMemoryChunk(Ctx::MEMORY_MODULE_PARSER, lwnChunks[--lwnAllocated], false);
        }

        auto size = reinterpret_cast<uint64_t*>(lwnChunks[0]);
        *size = sizeof(uint64_t);
    }

    void Parser::analyzeLwn(LwnMember* lwnMember) {
        if (unlikely(ctx->trace & Ctx::TRACE_LWN))
            ctx->logTrace(Ctx::TRACE_LWN, "analyze blk: " + std::to_string(lwnMember->block) + " offset: " +
                                          std::to_string(lwnMember->offset) + " scn: " + std::to_string(lwnMember->scn) + " subscn: " +
                                          std::to_string(lwnMember->subScn));

        uint8_t* data = reinterpret_cast<uint8_t*>(lwnMember) + sizeof(struct LwnMember);
        RedoLogRecord redoLogRecord[2];
        int64_t vectorCur = -1;
        if (unlikely(ctx->trace & Ctx::TRACE_LWN))
            ctx->logTrace(Ctx::TRACE_LWN, "analyze size: " + std::to_string(lwnMember->size) + " scn: " + std::to_string(lwnMember->scn) +
                                          " subscn: " + std::to_string(lwnMember->subScn));

        uint32_t recordSize = ctx->read32(data);
        uint8_t vld = data[4];
        uint32_t headerSize;

        if (unlikely(recordSize != lwnMember->size))
            throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                          std::to_string(lwnMember->offset) + ": too small log record, buffer size: " + std::to_string(lwnMember->size) +
                                          ", field size: " + std::to_string(recordSize));

        if ((vld & 0x04) != 0)
            headerSize = 68;
        else
            headerSize = 24;

        if (unlikely(ctx->dumpRedoLog >= 1)) {
            uint16_t thread = 1; // TODO: verify field size/position
            *ctx->dumpStream << " \n";

            if (ctx->version < RedoLogRecord::REDO_VERSION_12_1)
                *ctx->dumpStream << "REDO RECORD - Thread:" << thread << " RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence << "." <<
                                std::setfill('0') << std::setw(8) << std::hex << lwnMember->block << "." << std::setfill('0') << std::setw(4) <<
                                std::hex << lwnMember->offset << " LEN: 0x" << std::setfill('0') << std::setw(4) << std::hex << recordSize << " VLD: 0x" <<
                                std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(vld) << '\n';
            else {
                uint32_t conUid = ctx->read32(data + 16);
                *ctx->dumpStream << "REDO RECORD - Thread:" << thread << " RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence <<
                                "." << std::setfill('0') << std::setw(8) << std::hex << lwnMember->block << "." << std::setfill('0') << std::setw(4) <<
                                std::hex << lwnMember->offset << " LEN: 0x" << std::setfill('0') << std::setw(4) << std::hex << recordSize << " VLD: 0x" <<
                                std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(vld) << " CON_UID: " << std::dec << conUid << '\n';
            }

            if (ctx->dumpRawData > 0) {
                std::string header = "## H: [" + std::to_string(static_cast<uint64_t>(lwnMember->block) * reader->getBlockSize() + lwnMember->offset) + "] " +
                                     std::to_string(headerSize);
                *ctx->dumpStream << header;
                if (header.length() < 36)
                    *ctx->dumpStream << std::string(36 - header.length(), ' ');

                for (uint32_t j = 0; j < headerSize; ++j)
                    *ctx->dumpStream << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]) << " ";
                *ctx->dumpStream << '\n';
            }

            if (headerSize == 68) {
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "SCN: " << PRINTSCN48(lwnMember->scn) << " SUBSCN:" << std::setfill(' ') << std::setw(3) << std::dec <<
                                    lwnMember->subScn << " " << lwnTimestamp << '\n';
                else
                    *ctx->dumpStream << "SCN: " << PRINTSCN64(lwnMember->scn) << " SUBSCN:" << std::setfill(' ') << std::setw(3) << std::dec <<
                                    lwnMember->subScn << " " << lwnTimestamp << '\n';
                uint16_t lwnNst = ctx->read16(data + 26);
                uint32_t lwnLen = ctx->read32(data + 32);

                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "(LWN RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence << "." << std::setfill('0') <<
                                    std::setw(8) << std::hex << lwnMember->block << "." << std::setfill('0') << std::setw(4) << std::hex <<
                                    lwnMember->offset << " LEN: " << std::setfill('0') << std::setw(4) << std::dec << lwnLen << " NST: " <<
                                    std::setfill('0') << std::setw(4) << std::dec << lwnNst << " SCN: " << PRINTSCN48(lwnScn) << ")" << '\n';
                else
                    *ctx->dumpStream << "(LWN RBA: 0x" << std::setfill('0') << std::setw(6) << std::hex << sequence << "." << std::setfill('0') <<
                                    std::setw(8) << std::hex << lwnMember->block << "." << std::setfill('0') << std::setw(4) << std::hex <<
                                    lwnMember->offset << " LEN: 0x" << std::setfill('0') << std::setw(8) << std::hex << lwnLen << " NST: 0x" <<
                                    std::setfill('0') << std::setw(4) << std::hex << lwnNst << " SCN: " << PRINTSCN64(lwnScn) << ")" << '\n';
            } else {
                if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
                    *ctx->dumpStream << "SCN: " << PRINTSCN48(lwnMember->scn) << " SUBSCN:" << std::setfill(' ') << std::setw(3) << std::dec <<
                                    lwnMember->subScn << " " << lwnTimestamp << '\n';
                else
                    *ctx->dumpStream << "SCN: " << PRINTSCN64(lwnMember->scn) << " SUBSCN:" << std::setfill(' ') << std::setw(3) << std::dec <<
                                    lwnMember->subScn << " " << lwnTimestamp << '\n';
            }
        }

        if (unlikely(headerSize > recordSize)) {
            dumpRedoVector(data, recordSize);
            throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                          std::to_string(lwnMember->offset) + ": too small log record, header size: " + std::to_string(headerSize) +
                                          ", field size: " + std::to_string(recordSize));
        }

        uint64_t offset = headerSize;
        uint64_t vectors = 0;

        while (offset < recordSize) {
            int64_t vectorPrev = vectorCur;
            if (vectorPrev == -1)
                vectorCur = 0;
            else
                vectorCur = 1 - vectorPrev;

            memset(reinterpret_cast<void*>(&redoLogRecord[vectorCur]), 0, sizeof(RedoLogRecord));
            redoLogRecord[vectorCur].vectorNo = (++vectors);
            redoLogRecord[vectorCur].cls = ctx->read16(data + offset + 2);
            redoLogRecord[vectorCur].afn = static_cast<typeAfn>(ctx->read32(data + offset + 4) & 0xFFFF);
            redoLogRecord[vectorCur].dba = ctx->read32(data + offset + 8);
            redoLogRecord[vectorCur].scnRecord = ctx->readScn(data + offset + 12);
            redoLogRecord[vectorCur].rbl = 0; // TODO: verify field size/position
            redoLogRecord[vectorCur].seq = data[offset + 20];
            redoLogRecord[vectorCur].typ = data[offset + 21];
            typeUsn usn = (redoLogRecord[vectorCur].cls >= 15) ? (redoLogRecord[vectorCur].cls - 15) / 2 : -1;

            uint32_t fieldOffset;
            if (ctx->version >= RedoLogRecord::REDO_VERSION_12_1) {
                fieldOffset = 32;
                redoLogRecord[vectorCur].flgRecord = ctx->read16(data + offset + 28);
                redoLogRecord[vectorCur].conId = static_cast<typeConId>(ctx->read16(data + offset + 24));
            } else {
                fieldOffset = 24;
                redoLogRecord[vectorCur].flgRecord = 0;
                redoLogRecord[vectorCur].conId = 0;
            }

            if (unlikely(offset + fieldOffset + 1U >= recordSize)) {
                dumpRedoVector(data, recordSize);
                throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                              std::to_string(lwnMember->offset) + ": position of field list (" + std::to_string(offset + fieldOffset + 1) +
                                              ") outside of record, size: " + std::to_string(recordSize));
            }

            const uint8_t* fieldList = data + offset + fieldOffset;

            redoLogRecord[vectorCur].opCode = (static_cast<typeOp1>(data[offset + 0]) << 8) | data[offset + 1];
            redoLogRecord[vectorCur].size = fieldOffset + ((ctx->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectorCur].scn = lwnMember->scn;
            redoLogRecord[vectorCur].subScn = lwnMember->subScn;
            redoLogRecord[vectorCur].usn = usn;
            redoLogRecord[vectorCur].dataExt = data + offset;
            redoLogRecord[vectorCur].dataOffset = static_cast<uint64_t>(lwnMember->block) * reader->getBlockSize() + lwnMember->offset + offset;
            redoLogRecord[vectorCur].fieldSizesDelta = fieldOffset;
            if (unlikely(redoLogRecord[vectorCur].fieldSizesDelta + 1U >= recordSize)) {
                dumpRedoVector(data, recordSize);
                throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                              std::to_string(lwnMember->offset) + ": field size list (" +
                                              std::to_string(redoLogRecord[vectorCur].fieldSizesDelta) +
                                              ") outside of record, size: " + std::to_string(recordSize));
            }
            redoLogRecord[vectorCur].fieldCnt = (ctx->read16(redoLogRecord[vectorCur].data() + redoLogRecord[vectorCur].fieldSizesDelta) - 2) / 2;
            redoLogRecord[vectorCur].fieldPos = fieldOffset +
                                                ((ctx->read16(redoLogRecord[vectorCur].data() + redoLogRecord[vectorCur].fieldSizesDelta) + 2) & 0xFFFC);
            if (unlikely(redoLogRecord[vectorCur].fieldPos >= recordSize)) {
                dumpRedoVector(data, recordSize);
                throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                              std::to_string(lwnMember->offset) + ": fields (" + std::to_string(redoLogRecord[vectorCur].fieldPos) +
                                              ") outside of record, size: " + std::to_string(recordSize));
            }

            // typePos fieldPos = redoLogRecord[vectorCur].fieldPos;
            for (typeField i = 1; i <= redoLogRecord[vectorCur].fieldCnt; ++i) {
                redoLogRecord[vectorCur].size += (ctx->read16(fieldList + i * 2) + 3) & 0xFFFC;

                if (unlikely(offset + redoLogRecord[vectorCur].size > recordSize)) {
                    dumpRedoVector(data, recordSize);
                    throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                                  std::to_string(lwnMember->offset) + ": position of field list outside of record (" + "i: " +
                                                  std::to_string(i) + " c: " + std::to_string(redoLogRecord[vectorCur].fieldCnt) + " " + " o: " +
                                                  std::to_string(fieldOffset) + " p: " + std::to_string(offset) + " l: " +
                                                  std::to_string(redoLogRecord[vectorCur].size) + " r: " + std::to_string(recordSize) + ")");
                }
            }

            if (unlikely(redoLogRecord[vectorCur].fieldPos > redoLogRecord[vectorCur].size)) {
                dumpRedoVector(data, recordSize);
                throw RedoLogException(50046, "block: " + std::to_string(lwnMember->block) + ", offset: " +
                                              std::to_string(lwnMember->offset) + ": incomplete record, offset: " +
                                              std::to_string(redoLogRecord[vectorCur].fieldPos) + ", size: " +
                                              std::to_string(redoLogRecord[vectorCur].size));
            }

            redoLogRecord[vectorCur].recordObj = 0xFFFFFFFF;
            redoLogRecord[vectorCur].recordDataObj = 0xFFFFFFFF;
            offset += redoLogRecord[vectorCur].size;

            switch (redoLogRecord[vectorCur].opCode) {
                case 0x0501:
                    // Undo
                    OpCode0501::process0501(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0502:
                    // Begin transaction
                    OpCode0502::process0502(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0504:
                    // Commit/rollback transaction
                    OpCode0504::process0504(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0506:
                    // Partial rollback
                    OpCode0506::process0506(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x050B:
                    OpCode050B::process050B(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0513:
                    // Session information
                    OpCode0513::process0513(ctx, &redoLogRecord[vectorCur], lastTransaction);
                    break;

                    // Session information
                case 0x0514:
                    OpCode0514::process0514(ctx, &redoLogRecord[vectorCur], lastTransaction);
                    break;

                case 0x0A02:
                    // REDO: Insert leaf row
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0A02::process0A02(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0A08:
                    // REDO: Init header
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0A08::process0A08(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0A12:
                    // REDO: Update key data in row
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0A12::process0A12(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B02:
                    // REDO: Insert row piece
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B02::process0B02(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B03:
                    // REDO: Delete row piece
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B03::process0B03(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B04:
                    // REDO: Lock row piece
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B04::process0B04(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B05:
                    // REDO: Update row piece
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B05::process0B05(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B06:
                    // REDO: Overwrite row piece
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B06::process0B06(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B08:
                    // REDO: Change forwarding address
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B08::process0B08(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B0B:
                    // REDO: Insert multiple rows
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B0B::process0B0B(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B0C:
                    // REDO: Delete multiple rows
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B0C::process0B0C(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B10:
                    // REDO: Supplemental log for update
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B10::process0B10(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x0B16:
                    // REDO: Logminer support - KDOCMP
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode0B16::process0B16(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x1301:
                    // LOB
                    OpCode1301::process1301(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x1A02:
                    // LOB index 12+ and LOB redo
                    if (vectorPrev != -1 && redoLogRecord[vectorPrev].opCode == 0x0501) {
                        redoLogRecord[vectorCur].recordDataObj = redoLogRecord[vectorPrev].dataObj;
                        redoLogRecord[vectorCur].recordObj = redoLogRecord[vectorPrev].obj;
                    }
                    OpCode1A02::process1A02(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x1A06:
                    OpCode1A06::process1A06(ctx, &redoLogRecord[vectorCur]);
                    break;

                case 0x1801:
                    // DDL
                    OpCode1801::process1801(ctx, &redoLogRecord[vectorCur]);
                    break;

                default:
                    OpCode::process(ctx, &redoLogRecord[vectorCur]);
                    break;
            }

            if (vectorPrev != -1) {
                if (redoLogRecord[vectorPrev].opCode == 0x0501) {
                    if ((redoLogRecord[vectorCur].opCode & 0xFF00) == 0x0A00 || redoLogRecord[vectorCur].opCode == 0x1A02) {
                        // UNDO - index
                        appendToTransactionIndex(&redoLogRecord[vectorPrev], &redoLogRecord[vectorCur]);
                    } else if ((redoLogRecord[vectorCur].opCode & 0xFF00) == 0x0B00 || redoLogRecord[vectorCur].opCode == 0x0513 ||
                               redoLogRecord[vectorCur].opCode == 0x0514) {
                        // UNDO - data
                        appendToTransaction(&redoLogRecord[vectorPrev], &redoLogRecord[vectorCur]);
                    } else if (redoLogRecord[vectorCur].opCode == 0x0501) {
                        // Single 5.1
                        appendToTransaction(&redoLogRecord[vectorPrev]);
                        continue;
                    } else if (redoLogRecord[vectorPrev].opc == 0x0B01)
                        ctx->warning(70010, "unknown undo OP: " + std::to_string(redoLogRecord[vectorCur].opCode) + ", opc: " +
                                            std::to_string(redoLogRecord[vectorPrev].opc));

                    vectorCur = -1;
                    continue;
                }

                if ((redoLogRecord[vectorCur].opCode == 0x0506 || redoLogRecord[vectorCur].opCode == 0x050B)) {
                    if ((redoLogRecord[vectorPrev].opCode & 0xFF00) == 0x0B00)
                        appendToTransactionRollback(&redoLogRecord[vectorPrev], &redoLogRecord[vectorCur]);
                    else if (redoLogRecord[vectorCur].opc == 0x0B01)
                        ctx->warning(70011, "unknown rollback OP: " + std::to_string(redoLogRecord[vectorPrev].opCode) + ", opc: " +
                                            std::to_string(redoLogRecord[vectorCur].opc));

                    vectorCur = -1;
                    continue;
                }
            }

            // UNDO - data
            if (redoLogRecord[vectorCur].opCode == 0x0501 &&
                    (redoLogRecord[vectorCur].flg & (OpCode::FLG_MULTIBLOCKUNDOTAIL | OpCode::FLG_MULTIBLOCKUNDOMID)) != 0) {
                appendToTransaction(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            // ROLLBACK - data
            if (redoLogRecord[vectorCur].opCode == 0x0506 || redoLogRecord[vectorCur].opCode == 0x050B) {
                appendToTransactionRollback(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            // BEGIN
            if (redoLogRecord[vectorCur].opCode == 0x0502) {
                appendToTransactionBegin(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            // COMMIT
            if (redoLogRecord[vectorCur].opCode == 0x0504) {
                appendToTransactionCommit(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            // LOB
            if (redoLogRecord[vectorCur].opCode == 0x1301 || redoLogRecord[vectorCur].opCode == 0x1A06) {
                appendToTransactionLob(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }

            // DDL
            if (redoLogRecord[vectorCur].opCode == 0x1801) {
                appendToTransactionDdl(&redoLogRecord[vectorCur]);
                vectorCur = -1;
                continue;
            }
        }

        // UNDO - data
        if (vectorCur != -1 && redoLogRecord[vectorCur].opCode == 0x0501) {
            appendToTransaction(&redoLogRecord[vectorCur]);
        }
    }

    void Parser::appendToTransactionDdl(RedoLogRecord* redoLogRecord1) {
        // Skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;
        lastTransaction = transaction;

        const OracleTable* table;
        {
            std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
            table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        }

        if (table == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_SCHEMALESS) && !ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_DDL)) {
                transaction->log(ctx, "tbl ", redoLogRecord1);
                return;
            }
        } else {
            if ((table->options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)
                transaction->system = true;
            if ((table->options & OracleTable::OPTIONS_SCHEMA_TABLE) != 0)
                transaction->schema = true;
        }

        // Transaction size limit
        if (ctx->transactionSizeMax > 0 &&
            transaction->size + redoLogRecord1->size + TransactionBuffer::ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
            transactionBuffer->skipXidList.insert(transaction->xid);
            transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
            transaction->purge(transactionBuffer);
            if (transaction == lastTransaction)
                lastTransaction = nullptr;
            delete transaction;
            return;
        }

        transaction->add(metadata, transactionBuffer, redoLogRecord1, &zero);
    }

    void Parser::appendToTransactionLob(RedoLogRecord* redoLogRecord1) {
        OracleLob* lob;
        {
            std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
            lob = metadata->schema->checkLobDict(redoLogRecord1->dataObj);
        }

        if (lob == nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                ctx->logTrace(Ctx::TRACE_LOB, "skip dataobj: " + std::to_string(redoLogRecord1->dataObj) + " xid: " +
                                              redoLogRecord1->xid.toString());
            return;
        }

        redoLogRecord1->lobPageSize = lob->checkLobPageSize(redoLogRecord1->dataObj);

        if (redoLogRecord1->xid.isEmpty()) {
            auto lobIdToXidMapIt = ctx->lobIdToXidMap.find(redoLogRecord1->lobId);
            if (lobIdToXidMapIt == ctx->lobIdToXidMap.end()) {
                transactionBuffer->addOrphanedLob(redoLogRecord1);
                return;
            } else
                redoLogRecord1->xid = lobIdToXidMapIt->second;
        }

        // Skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;
        lastTransaction = transaction;

        if (lob->table != nullptr && (lob->table->options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)
            transaction->system = true;
        if (lob->table != nullptr && (lob->table->options & OracleTable::OPTIONS_SCHEMA_TABLE) != 0)
            transaction->schema = true;

        if (unlikely(ctx->trace & Ctx::TRACE_LOB))
            ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord1->lobId.lower() + " xid: " + transaction->xid.toString() + " obj: " +
                                          std::to_string(redoLogRecord1->dataObj) + " op: " + std::to_string(redoLogRecord1->opCode) + "     dba: " +
                                          std::to_string(redoLogRecord1->dba) + " page: " + std::to_string(redoLogRecord1->lobPageNo) + " pg: " +
                                          std::to_string(redoLogRecord1->lobPageSize));

        transaction->lobCtx.addLob(ctx, redoLogRecord1->lobId, redoLogRecord1->dba, 0, transactionBuffer->allocateLob(redoLogRecord1),
                                   transaction->xid, redoLogRecord1->dataOffset);
    }

    void Parser::appendToTransaction(RedoLogRecord* redoLogRecord1) {
        // Skip list
        if (redoLogRecord1->xid.getData() != 0 && transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;
        lastTransaction = transaction;

        if (redoLogRecord1->opc != 0x0501 && redoLogRecord1->opc != 0x0A16 && redoLogRecord1->opc != 0x0B01) {
            transaction->log(ctx, "opc ", redoLogRecord1);
            return;
        }

        const OracleTable* table;
        {
            std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
            table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        }

        if (table == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_SCHEMALESS)) {
                transaction->log(ctx, "tbl ", redoLogRecord1);
                return;
            }
        } else {
            if ((table->options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)
                transaction->system = true;
            if ((table->options & OracleTable::OPTIONS_SCHEMA_TABLE) != 0)
                transaction->schema = true;
        }

        // Transaction size limit
        if (ctx->transactionSizeMax > 0 && transaction->size + redoLogRecord1->size + TransactionBuffer::ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
            transaction->log(ctx, "siz ", redoLogRecord1);
            transactionBuffer->skipXidList.insert(transaction->xid);
            transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
            transaction->purge(transactionBuffer);
            if (transaction == lastTransaction)
                lastTransaction = nullptr;
            delete transaction;
            return;
        }

        transaction->add(metadata, transactionBuffer, redoLogRecord1);
    }

    void Parser::appendToTransactionRollback(RedoLogRecord* redoLogRecord1) {
        if ((redoLogRecord1->opc != 0x0A16 && redoLogRecord1->opc != 0x0B01))
            return;

        if ((redoLogRecord1->flg & OpCode::FLG_USERUNDODDONE) == 0)
            return;

        typeXid xid(redoLogRecord1->usn, redoLogRecord1->slt, 0);
        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, xid, redoLogRecord1->conId, true, false,
                                                                      true);
        if (transaction == nullptr) {
            typeXidMap xidMap = (xid.getData() >> 32) | (static_cast<uint64_t>(redoLogRecord1->conId) << 32);
            auto brokenXidMapListIt = transactionBuffer->brokenXidMapList.find(xidMap);
            if (brokenXidMapListIt == transactionBuffer->brokenXidMapList.end()) {
                ctx->warning(60010, "no match found for transaction rollback, skipping, SLT: " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord1->slt)) + ", USN: " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord1->usn)));
                transactionBuffer->brokenXidMapList.insert(xidMap);
            }
            return;
        }
        lastTransaction = transaction;

        const OracleTable* table;
        {
            std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
            table = metadata->schema->checkTableDict(redoLogRecord1->obj);
        }

        if (table == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_SCHEMALESS)) {
                transaction->log(ctx, "rls ", redoLogRecord1);
                return;
            }
        }

        transaction->rollbackLastOp(metadata, transactionBuffer, redoLogRecord1);
    }

    void Parser::appendToTransactionBegin(RedoLogRecord* redoLogRecord1) {
        // Skip SQN cleanup
        if (redoLogRecord1->xid.sqn() == 0)
            return;

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      false, true, false);
        transaction->begin = true;
        transaction->firstSequence = sequence;
        transaction->firstOffset = static_cast<uint64_t>(lwnCheckpointBlock) * reader->getBlockSize();
        transaction->log(ctx, "B   ", redoLogRecord1);
        lastTransaction = transaction;
    }

    void Parser::appendToTransactionCommit(RedoLogRecord* redoLogRecord1) {
        // Clean LOBs if used
        for (auto lobIdToXidMapIt = ctx->lobIdToXidMap.cbegin(); lobIdToXidMapIt != ctx->lobIdToXidMap.cend();) {
            if (lobIdToXidMapIt->second == redoLogRecord1->xid) {
                lobIdToXidMapIt = ctx->lobIdToXidMap.erase(lobIdToXidMapIt);
            } else
                ++lobIdToXidMapIt;
        }

        // Skip list
        auto skipXidListIt = transactionBuffer->skipXidList.find(redoLogRecord1->xid);
        if (skipXidListIt != transactionBuffer->skipXidList.end()) {
            transactionBuffer->skipXidList.erase(skipXidListIt);
            return;
        }

        // Broken transaction
        typeXidMap xidMap = (redoLogRecord1->xid.getData() >> 32) | (static_cast<uint64_t>(redoLogRecord1->conId) << 32);
        auto brokenXidMapListIt = transactionBuffer->brokenXidMapList.find(xidMap);
        if (brokenXidMapListIt != transactionBuffer->brokenXidMapList.end())
            transactionBuffer->brokenXidMapList.erase(xidMap);

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;

        transaction->log(ctx, "C   ", redoLogRecord1);
        transaction->commitTimestamp = lwnTimestamp;
        transaction->commitScn = redoLogRecord1->scnRecord;
        transaction->commitSequence = sequence;
        if ((redoLogRecord1->flg & OpCode::FLG_ROLLBACK_OP0504) != 0)
            transaction->rollback = true;

        if ((transaction->commitScn > metadata->firstDataScn && !transaction->system) ||
            (transaction->commitScn > metadata->firstSchemaScn && transaction->system)) {

            if (transaction->begin) {
                transaction->flush(metadata, transactionBuffer, builder, lwnScn);
                if (ctx->metrics != nullptr) {
                    if (transaction->rollback)
                        ctx->metrics->emitTransactionsRollbackOut(1);
                    else
                        ctx->metrics->emitTransactionsCommitOut(1);
                }

                if (ctx->stopTransactions > 0 && metadata->isNewData(lwnScn, builder->lwnIdx)) {
                    --ctx->stopTransactions;
                    if (ctx->stopTransactions == 0) {
                        ctx->info(0, "shutdown started - exhausted number of transactions");
                        ctx->stopSoft();
                    }
                }

                if (transaction->shutdown && metadata->isNewData(lwnScn, builder->lwnIdx)) {
                    ctx->info(0, "shutdown started - initiated by debug transaction " + transaction->xid.toString() +
                                 " at scn " + std::to_string(transaction->commitScn));
                    ctx->stopSoft();
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (transaction->rollback)
                        ctx->metrics->emitTransactionsRollbackPartial(1);
                    else
                        ctx->metrics->emitTransactionsCommitPartial(1);
                }
                ctx->warning(60011, "skipping transaction with no beginning: " + transaction->toString());
            }
        } else {
            if (ctx->metrics != nullptr) {
                if (transaction->rollback)
                    ctx->metrics->emitTransactionsRollbackSkip(1);
                else
                    ctx->metrics->emitTransactionsCommitSkip(1);
            }
            if (unlikely(ctx->trace & Ctx::TRACE_TRANSACTION))
                ctx->logTrace(Ctx::TRACE_TRANSACTION, "skipping transaction already committed: " + transaction->toString());
        }

        transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
        transaction->purge(transactionBuffer);
        lastTransaction = nullptr;
        delete transaction;
    }

    void Parser::appendToTransaction(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        // Skip other PDB vectors
        if (metadata->conId > 0 && redoLogRecord2->conId != metadata->conId)
            return;

        // Skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;
        lastTransaction = transaction;

        typeObj obj;
        if (redoLogRecord1->dataObj != 0) {
            obj = redoLogRecord1->obj;
            redoLogRecord2->obj = redoLogRecord1->obj;
            redoLogRecord2->dataObj = redoLogRecord1->dataObj;
        } else {
            obj = redoLogRecord2->obj;
            redoLogRecord1->obj = redoLogRecord2->obj;
            redoLogRecord1->dataObj = redoLogRecord2->dataObj;
        }
        if (unlikely(redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0))
            throw RedoLogException(50045, "bdba does not match (" + std::to_string(redoLogRecord1->bdba) + ", " +
                                          std::to_string(redoLogRecord2->bdba) + "), offset: " + std::to_string(redoLogRecord1->dataOffset));

        switch (redoLogRecord2->opCode) {
            case 0x0513:
            case 0x0514:
                // Session information
                break;

            case 0x0B02:
                // Insert row piece
            case 0x0B03:
                // Delete row piece
            case 0x0B05:
                // Update row piece
            case 0x0B06:
                // Overwrite row piece
            case 0x0B08:
                // Change forwarding address
            case 0x0B0B:
                // Insert multiple rows
            case 0x0B0C:
                // Delete multiple rows
            case 0x0B10:
                // Supp log for update
            case 0x0B16: {
                // Logminer support - KDOCMP
                const OracleTable* table;
                {
                    std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
                    table = metadata->schema->checkTableDict(obj);
                }

                if (table == nullptr) {
                    if (!ctx->flagsSet(Ctx::REDO_FLAGS_SCHEMALESS)) {
                        transaction->log(ctx, "tbl1", redoLogRecord1);
                        transaction->log(ctx, "tbl2", redoLogRecord2);
                        return;
                    }
                } else {
                    if ((table->options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)
                        transaction->system = true;
                    if ((table->options & OracleTable::OPTIONS_SCHEMA_TABLE) != 0)
                        transaction->schema = true;
                    if ((table->options & OracleTable::OPTIONS_DEBUG_TABLE) != 0 && redoLogRecord2->opCode == 0x0B02 && !ctx->softShutdown)
                        transaction->shutdown = true;
                }
            }
                break;

            default:
                transaction->log(ctx, "skp1", redoLogRecord1);
                transaction->log(ctx, "skp2", redoLogRecord2);
                return;
        }

        // Transaction size limit
        if (ctx->transactionSizeMax > 0 && transaction->size + redoLogRecord1->size + redoLogRecord2->size +
                TransactionBuffer::ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
            transaction->log(ctx, "siz1", redoLogRecord1);
            transaction->log(ctx, "siz2", redoLogRecord2);
            transactionBuffer->skipXidList.insert(transaction->xid);
            transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
            transaction->purge(transactionBuffer);
            if (transaction == lastTransaction)
                lastTransaction = nullptr;
            delete transaction;
            return;
        }

        transaction->add(metadata, transactionBuffer, redoLogRecord1, redoLogRecord2);
    }

    void Parser::appendToTransactionRollback(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        // Skip other PDB vectors
        if (metadata->conId > 0 && redoLogRecord1->conId != metadata->conId)
            return;

        typeXid xid(redoLogRecord2->usn, redoLogRecord2->slt, 0);
        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, xid, redoLogRecord2->conId, true, false,
                                                                      true);
        if (transaction == nullptr) {
            typeXidMap xidMap = (xid.getData() >> 32) | (static_cast<uint64_t>(redoLogRecord2->conId) << 32);
            auto brokenXidMapListIt = transactionBuffer->brokenXidMapList.find(xidMap);
            if (brokenXidMapListIt == transactionBuffer->brokenXidMapList.end()) {
                ctx->warning(60010, "no match found for transaction rollback, skipping, SLT: " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->slt)) + ", USN: " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->usn)));
                transactionBuffer->brokenXidMapList.insert(xidMap);
            }
            return;
        }
        lastTransaction = transaction;
        redoLogRecord1->xid = transaction->xid;

        // Skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        typeObj obj;
        if (redoLogRecord1->dataObj != 0) {
            obj = redoLogRecord1->obj;
            redoLogRecord2->obj = redoLogRecord1->obj;
            redoLogRecord2->dataObj = redoLogRecord1->dataObj;
        } else {
            obj = redoLogRecord2->obj;
            redoLogRecord1->obj = redoLogRecord2->obj;
            redoLogRecord1->dataObj = redoLogRecord2->dataObj;
        }
        if (unlikely(redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0))
            throw RedoLogException(50045, "bdba does not match (" + std::to_string(redoLogRecord1->bdba) + ", " +
                                          std::to_string(redoLogRecord2->bdba) + "), offset: " + std::to_string(redoLogRecord1->dataOffset));

        const OracleTable* table;
        {
            std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
            table = metadata->schema->checkTableDict(obj);
        }

        if (table == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_SCHEMALESS)) {
                transaction->log(ctx, "rls1", redoLogRecord1);
                transaction->log(ctx, "rls2", redoLogRecord2);
                return;
            }
        } else {
            if ((table->options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)
                transaction->system = true;
            if ((table->options & OracleTable::OPTIONS_SCHEMA_TABLE) != 0)
                transaction->schema = true;
        }

        switch (redoLogRecord1->opCode) {
            case 0x0B02:
                // Insert row piece
            case 0x0B03:
                // Delete row piece
            case 0x0B05:
                // Update row piece
            case 0x0B06:
                // Overwrite row piece
            case 0x0B08:
                // Change forwarding address
            case 0x0B0B:
                // Insert multiple rows
            case 0x0B0C:
                // Delete multiple rows
            case 0x0B10:
                // Supp log for update
            case 0x0B16:
                // Logminer support - KDOCMP
                break;

            default:
                transaction->log(ctx, "skp1", redoLogRecord1);
                transaction->log(ctx, "skp2", redoLogRecord2);
                return;
        }

        transaction->rollbackLastOp(metadata, transactionBuffer, redoLogRecord1, redoLogRecord2);
    }

    void Parser::appendToTransactionIndex(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        // Skip other PDB vectors
        if (metadata->conId > 0 && redoLogRecord2->conId != metadata->conId)
            return;

        // Skip list
        if (transactionBuffer->skipXidList.find(redoLogRecord1->xid) != transactionBuffer->skipXidList.end())
            return;

        Transaction* transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                      true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
        if (transaction == nullptr)
            return;
        lastTransaction = transaction;

        typeDataObj dataObj;
        if (redoLogRecord1->dataObj != 0) {
            dataObj = redoLogRecord1->dataObj;
            redoLogRecord2->obj = redoLogRecord1->obj;
            redoLogRecord2->dataObj = redoLogRecord1->dataObj;
        } else {
            dataObj = redoLogRecord2->dataObj;
            redoLogRecord1->obj = redoLogRecord2->obj;
            redoLogRecord1->dataObj = redoLogRecord2->dataObj;
        }
        if (unlikely(redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0))
            throw RedoLogException(50045, "bdba does not match (" + std::to_string(redoLogRecord1->bdba) + ", " +
                                          std::to_string(redoLogRecord2->bdba) + "), offset: " + std::to_string(redoLogRecord1->dataOffset));

        const OracleLob* lob;
        {
            std::unique_lock<std::mutex> lckTransaction(metadata->mtxTransaction);
            lob = metadata->schema->checkLobIndexDict(dataObj);
        }

        if (lob == nullptr && redoLogRecord2->opCode != 0x1A02) {
            if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                ctx->logTrace(Ctx::TRACE_LOB, "skip index dataobj: " + std::to_string(dataObj) + " (" +
                                              std::to_string(redoLogRecord1->dataObj) + ", " + std::to_string(redoLogRecord2->dataObj) + ")" + " xid: " +
                                              redoLogRecord1->xid.toString());

            transaction->log(ctx, "idx1", redoLogRecord1);
            transaction->log(ctx, "idx2", redoLogRecord2);
            return;
        }

        if (redoLogRecord2->opCode == 0x0A02) {
            if (redoLogRecord2->indKeySize == 16 && redoLogRecord2->data()[redoLogRecord2->indKey] == 10 &&
                redoLogRecord2->data()[redoLogRecord2->indKey + 11] == 4) {
                redoLogRecord2->lobId.set(redoLogRecord2->data() + redoLogRecord2->indKey + 1);
                redoLogRecord2->lobPageNo = ctx->read32Big(redoLogRecord2->data() + redoLogRecord2->indKey + 12);
            } else
                return;
        } else if (redoLogRecord2->opCode == 0x0A08) {
            if (redoLogRecord2->indKey == 0)
                return;

            if (redoLogRecord2->indKeySize == 50 && redoLogRecord2->data()[redoLogRecord2->indKey] == 0x01 &&
                redoLogRecord2->data()[redoLogRecord2->indKey + 1] == 0x01 &&
                redoLogRecord2->data()[redoLogRecord2->indKey + 34] == 10 && redoLogRecord2->data()[redoLogRecord2->indKey + 45] == 4) {
                redoLogRecord2->lobId.set(redoLogRecord2->data() + redoLogRecord2->indKey + 35);
                redoLogRecord2->lobPageNo = ctx->read32Big(redoLogRecord2->data() + redoLogRecord2->indKey + 46);
                redoLogRecord2->indKeyData = redoLogRecord2->indKey + 2;
                redoLogRecord2->indKeyDataSize = 32;
            } else {
                ctx->warning(60014, "verify redo log file for OP:10.8, len: " + std::to_string(redoLogRecord2->indKeySize) +
                                    ", data = [" + std::to_string(static_cast<uint64_t>(redoLogRecord2->data()[redoLogRecord2->indKey])) + ", " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->data()[redoLogRecord2->indKey + 1])) + ", " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->data()[redoLogRecord2->indKey + 34])) + ", " +
                                    std::to_string(static_cast<uint64_t>(redoLogRecord2->data()[redoLogRecord2->indKey + 45])) + "]");
                return;
            }

            auto lobIdToXidMapIt = ctx->lobIdToXidMap.find(redoLogRecord2->lobId);
            if (lobIdToXidMapIt != ctx->lobIdToXidMap.end()) {
                typeXid parentXid = lobIdToXidMapIt->second;

                if (parentXid != redoLogRecord1->xid) {
                    if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                        ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + parentXid.toString() +
                                                      " sub-xid: " + redoLogRecord1->xid.toString());
                    redoLogRecord1->xid = parentXid;
                    redoLogRecord2->xid = parentXid;

                    transaction = transactionBuffer->findTransaction(metadata->schema->xmlCtxDefault, redoLogRecord1->xid, redoLogRecord1->conId,
                                                                     true, ctx->flagsSet(Ctx::REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS), false);
                    if (transaction == nullptr) {
                        if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                            ctx->logTrace(Ctx::TRACE_LOB, "parent transaction not found");
                        return;
                    }
                    lastTransaction = transaction;
                }
            }
        } else if (redoLogRecord2->opCode == 0x0A12) {
            if (redoLogRecord1->indKeySize == 16 && redoLogRecord1->data()[redoLogRecord1->indKey] == 10 &&
                redoLogRecord1->data()[redoLogRecord1->indKey + 11] == 4) {
                redoLogRecord2->lobId.set(redoLogRecord1->data() + redoLogRecord1->indKey + 1);
                redoLogRecord2->lobPageNo = ctx->read32Big(redoLogRecord1->data() + redoLogRecord1->indKey + 12);
                redoLogRecord2->lobSizePages = ctx->read32Big(redoLogRecord2->data() + redoLogRecord2->indKeyData + 4);
                redoLogRecord2->lobSizeRest = ctx->read16Big(redoLogRecord2->data() + redoLogRecord2->indKeyData + 8);
            } else
                return;
        }

        switch (redoLogRecord2->opCode) {
            case 0x0A02:
                // Insert leaf row
            case 0x0A08:
                // Init header
            case 0x0A12:
                // Update key data in row
            case 0x1A02:
                // LOB index 12+ and LOB redo
                break;

            default:
                transaction->log(ctx, "skp1", redoLogRecord1);
                transaction->log(ctx, "skp2", redoLogRecord2);
                return;
        }

        if (redoLogRecord2->lobId.data[0] != 0 || redoLogRecord2->lobId.data[1] != 0 ||
            redoLogRecord2->lobId.data[2] != 0 || redoLogRecord2->lobId.data[3] != 1)
            return;

        if (unlikely(ctx->trace & Ctx::TRACE_LOB)) {
            std::ostringstream ss;
            if (redoLogRecord1->indKeySize > 0)
                ss << "0x";
            for (typeSize i = 0; i < redoLogRecord1->indKeySize; ++i)
                ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord1->data()[redoLogRecord1->indKey + i]);
            if (redoLogRecord2->indKeySize > 0)
                ss << " 0x";
            for (typeSize i = 0; i < redoLogRecord2->indKeySize; ++i)
                ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(redoLogRecord2->data()[redoLogRecord2->indKey + i]);

            ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + redoLogRecord1->xid.toString() + " obj: " +
                                          std::to_string(redoLogRecord2->dataObj) + " op: " + std::to_string(redoLogRecord1->opCode) + ":" +
                                          std::to_string(redoLogRecord2->opCode) + " dba: " + std::to_string(redoLogRecord2->dba) + " page: " +
                                          std::to_string(redoLogRecord2->lobPageNo) + " ind key: " + ss.str());
        }

        auto lobIdToXidMapIt = ctx->lobIdToXidMap.find(redoLogRecord2->lobId);
        if (lobIdToXidMapIt == ctx->lobIdToXidMap.end()) {
            if (unlikely(ctx->trace & Ctx::TRACE_LOB))
                ctx->logTrace(Ctx::TRACE_LOB, "id: " + redoLogRecord2->lobId.lower() + " xid: " + redoLogRecord1->xid.toString() + " MAP");
            ctx->lobIdToXidMap.insert_or_assign(redoLogRecord2->lobId, redoLogRecord1->xid);
            transaction->lobCtx.checkOrphanedLobs(ctx, redoLogRecord2->lobId, redoLogRecord1->xid, redoLogRecord1->dataOffset);
        }

        if (lob != nullptr) {
            if (lob->table != nullptr && (lob->table->options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)
                transaction->system = true;
            if (lob->table != nullptr && (lob->table->options & OracleTable::OPTIONS_SCHEMA_TABLE) != 0)
                transaction->schema = true;
        }

        // Transaction size limit
        if (ctx->transactionSizeMax > 0 &&
            transaction->size + redoLogRecord1->size + redoLogRecord2->size + TransactionBuffer::ROW_HEADER_TOTAL >= ctx->transactionSizeMax) {
            transactionBuffer->skipXidList.insert(transaction->xid);
            transactionBuffer->dropTransaction(redoLogRecord1->xid, redoLogRecord1->conId);
            transaction->purge(transactionBuffer);
            if (transaction == lastTransaction)
                lastTransaction = nullptr;
            delete transaction;
            return;
        }

        transaction->add(metadata, transactionBuffer, redoLogRecord1, redoLogRecord2);
    }

    void Parser::dumpRedoVector(const uint8_t* data, typeSize recordSize) const {
        if (ctx->logLevel >= Ctx::LOG_LEVEL_WARNING) {
            std::ostringstream ss;
            ss << "dumping redo vector\n";
            ss << "##: " << std::dec << recordSize;
            for (typeSize j = 0; j < recordSize; ++j) {
                if ((j & 0x0F) == 0)
                    ss << "\n##  " << std::setfill(' ') << std::setw(2) << std::hex << j << ": ";
                if ((j & 0x07) == 0)
                    ss << " ";
                ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(data[j]) << " ";
            }
            ctx->warning(70002, ss.str());
        }
    }

    uint64_t Parser::parse() {
        typeBlk lwnConfirmedBlock = 2;
        uint64_t lwnRecords = 0;

        if (firstScn == Ctx::ZERO_SCN && nextScn == Ctx::ZERO_SCN && reader->getFirstScn() != 0) {
            firstScn = reader->getFirstScn();
            nextScn = reader->getNextScn();
        }
        ctx->suppLogSize = 0;

        if (reader->getBufferStart() == reader->getBlockSize() * 2) {
            if (unlikely(ctx->dumpRedoLog >= 1)) {
                std::string fileName = ctx->dumpPath + "/" + std::to_string(sequence) + ".olr";
                ctx->dumpStream->open(fileName);
                if (!ctx->dumpStream->is_open()) {
                    ctx->error(10006, "file: " + fileName + " - open for write returned: " + strerror(errno));
                    ctx->warning(60012, "aborting log dump");
                    ctx->dumpRedoLog = 0;
                }
                std::ostringstream ss;
                reader->printHeaderInfo(ss, path);
                *ctx->dumpStream << ss.str();
            }
        }

        // Continue started offset
        if (metadata->offset > 0) {
            if (unlikely((metadata->offset % reader->getBlockSize()) != 0))
                throw RedoLogException(50047, "incorrect offset start: " + std::to_string(metadata->offset) +
                                              " - not a multiplication of block size: " + std::to_string(reader->getBlockSize()));

            lwnConfirmedBlock = metadata->offset / reader->getBlockSize();
            if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
                ctx->logTrace(Ctx::TRACE_CHECKPOINT, "setting reader start position to " + std::to_string(metadata->offset) + " (block " +
                                                     std::to_string(lwnConfirmedBlock) + ")");
            metadata->offset = 0;
        }
        reader->setBufferStartEnd(static_cast<uint64_t>(lwnConfirmedBlock) * reader->getBlockSize(),
                                  static_cast<uint64_t>(lwnConfirmedBlock) * reader->getBlockSize());

        ctx->info(0, "processing redo log: " + toString() + " offset: " + std::to_string(reader->getBufferStart()));
        if (ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA) && !metadata->schema->loaded && ctx->versionStr.length() > 0) {
            metadata->loadAdaptiveSchema();
            metadata->schema->loaded = true;
        }

        if (metadata->resetlogs == 0)
            metadata->setResetlogs(reader->getResetlogs());

        if (unlikely(metadata->resetlogs != reader->getResetlogs()))
            throw RedoLogException(50048, "invalid resetlogs value (found: " + std::to_string(reader->getResetlogs()) + ", expected: " +
                                          std::to_string(metadata->resetlogs) + "): " + reader->fileName);

        if (reader->getActivation() != 0 && (metadata->activation == 0 || metadata->activation != reader->getActivation())) {
            ctx->info(0, "new activation detected: " + std::to_string(reader->getActivation()));
            metadata->setActivation(reader->getActivation());
        }

        time_ut cStart = ctx->clock->getTimeUt();
        reader->setStatusRead();
        LwnMember* lwnMember;
        uint64_t blockOffset;
        uint64_t confirmedBufferStart = reader->getBufferStart();
        uint64_t recordSize4;
        uint64_t recordPos = 0;
        uint64_t recordLeftToCopy = 0;
        typeBlk startBlock = lwnConfirmedBlock;
        typeBlk currentBlock = lwnConfirmedBlock;
        typeBlk lwnEndBlock = lwnConfirmedBlock;
        uint16_t lwnNumMax = 0;
        uint16_t lwnNumCnt = 0;
        lwnCheckpointBlock = lwnConfirmedBlock;
        bool switchRedo = false;

        while (!ctx->softShutdown) {
            // There is some work to do
            while (confirmedBufferStart < reader->getBufferEnd()) {
                uint64_t redoBufferPos = (static_cast<uint64_t>(currentBlock) * reader->getBlockSize()) % Ctx::MEMORY_CHUNK_SIZE;
                uint64_t redoBufferNum = ((static_cast<uint64_t>(currentBlock) * reader->getBlockSize()) / Ctx::MEMORY_CHUNK_SIZE) % ctx->readBufferMax;
                const uint8_t* redoBlock = reader->redoBufferList[redoBufferNum] + redoBufferPos;

                blockOffset = 16;
                // New LWN block
                if (currentBlock == lwnEndBlock) {
                    uint8_t vld = redoBlock[blockOffset + 4];

                    if (likely((vld & 0x04) != 0)) {
                        uint16_t lwnNum = ctx->read16(redoBlock + blockOffset + 24);
                        uint32_t lwnSize = ctx->read32(redoBlock + blockOffset + 28);
                        lwnEndBlock = currentBlock + lwnSize;
                        lwnScn = ctx->readScn(redoBlock + blockOffset + 40);
                        lwnTimestamp = ctx->read32(redoBlock + blockOffset + 64);

                        if (ctx->metrics) {
                            int64_t diff = ctx->clock->getTimeT() - lwnTimestamp.toEpoch(ctx->hostTimezone);
                            ctx->metrics->emitCheckpointLag(diff);
                        }

                        if (lwnNumCnt == 0) {
                            lwnCheckpointBlock = currentBlock;
                            lwnNumMax = ctx->read16(redoBlock + blockOffset + 26);
                            // Verify LWN header start
                            if (unlikely(lwnScn < reader->getFirstScn() || (lwnScn > reader->getNextScn() && reader->getNextScn() != Ctx::ZERO_SCN)))
                                throw RedoLogException(50049, "invalid lwn scn: " + std::to_string(lwnScn));
                        } else {
                            uint16_t lwnNumCur = ctx->read16(redoBlock + blockOffset + 26);
                            if (unlikely(lwnNumCur != lwnNumMax))
                                throw RedoLogException(50050, "invalid lwn max: " + std::to_string(lwnNum) + "/" +
                                                              std::to_string(lwnNumCur) + "/" + std::to_string(lwnNumMax));
                        }
                        ++lwnNumCnt;

                        if (unlikely(ctx->trace & Ctx::TRACE_LWN)) {
                            typeBlk lwnStartBlock = currentBlock;
                            ctx->logTrace(Ctx::TRACE_LWN, "at: " + std::to_string(lwnStartBlock) + " size: " + std::to_string(lwnSize) +
                                                          " chk: " + std::to_string(lwnNum) + " max: " + std::to_string(lwnNumMax));
                        }
                    } else
                        throw RedoLogException(50051, "did not find lwn at offset: " + std::to_string(confirmedBufferStart));
                }

                while (blockOffset < reader->getBlockSize()) {
                    // Next record
                    if (recordLeftToCopy == 0) {
                        if (blockOffset + 20 >= reader->getBlockSize())
                            break;

                        recordSize4 = (static_cast<uint64_t>(ctx->read32(redoBlock + blockOffset)) + 3) & 0xFFFFFFFC;
                        if (recordSize4 > 0) {
                            uint64_t* recordSize = reinterpret_cast<uint64_t*>(lwnChunks[lwnAllocated - 1]);

                            if (((*recordSize + sizeof(struct LwnMember) + recordSize4 + 7) & 0xFFFFFFF8) > Ctx::MEMORY_CHUNK_SIZE_MB * 1024 * 1024) {
                                if (unlikely(lwnAllocated == MAX_LWN_CHUNKS))
                                    throw RedoLogException(50052, "all " + std::to_string(MAX_LWN_CHUNKS) + " lwn buffers allocated");

                                lwnChunks[lwnAllocated++] = ctx->getMemoryChunk(Ctx::MEMORY_MODULE_PARSER, false);
                                if (lwnAllocated > lwnAllocatedMax)
                                    lwnAllocatedMax = lwnAllocated;
                                recordSize = reinterpret_cast<uint64_t*>(lwnChunks[lwnAllocated - 1]);
                                *recordSize = sizeof(uint64_t);
                            }

                            if (unlikely(((*recordSize + sizeof(struct LwnMember) + recordSize4 + 7) & 0xFFFFFFF8) > Ctx::MEMORY_CHUNK_SIZE_MB * 1024 * 1024))
                                throw RedoLogException(50053, "too big redo log record, size: " + std::to_string(recordSize4));

                            lwnMember = reinterpret_cast<struct LwnMember*>(lwnChunks[lwnAllocated - 1] + *recordSize);
                            *recordSize += (sizeof(struct LwnMember) + recordSize4 + 7) & 0xFFFFFFF8;
                            lwnMember->scn = ctx->read32(redoBlock + blockOffset + 8) |
                                             (static_cast<uint64_t>(ctx->read16(redoBlock + blockOffset + 6)) << 32);
                            lwnMember->subScn = ctx->read16(redoBlock + blockOffset + 12);
                            lwnMember->block = currentBlock;
                            lwnMember->offset = blockOffset;
                            lwnMember->size = recordSize4;
                            if (unlikely(ctx->trace & Ctx::TRACE_LWN))
                                ctx->logTrace(Ctx::TRACE_LWN, "size: " + std::to_string(recordSize4) + " scn: " +
                                                              std::to_string(lwnMember->scn) + " subscn: " + std::to_string(lwnMember->subScn));

                            uint64_t lwnPos = ++lwnRecords;
                            if (unlikely(lwnPos >= MAX_RECORDS_IN_LWN))
                                throw RedoLogException(50054, "all " + std::to_string(lwnPos) + " records in lwn were used");

                            while (lwnPos > 1 && *lwnMember < *lwnMembers[lwnPos / 2]) {
                                lwnMembers[lwnPos] = lwnMembers[lwnPos / 2];
                                lwnPos = lwnPos / 2;
                            }
                            lwnMembers[lwnPos] = lwnMember;
                        }

                        recordLeftToCopy = recordSize4;
                        recordPos = 0;
                    }

                    // Nothing more
                    if (recordLeftToCopy == 0)
                        break;

                    uint64_t toCopy;
                    if (blockOffset + recordLeftToCopy > reader->getBlockSize())
                        toCopy = reader->getBlockSize() - blockOffset;
                    else
                        toCopy = recordLeftToCopy;

                    memcpy(reinterpret_cast<void*>(reinterpret_cast<uint8_t*>(lwnMember) + sizeof(struct LwnMember) + recordPos),
                           reinterpret_cast<const void*>(redoBlock + blockOffset), toCopy);
                    recordLeftToCopy -= toCopy;
                    blockOffset += toCopy;
                    recordPos += toCopy;
                }

                ++currentBlock;
                confirmedBufferStart += reader->getBlockSize();
                redoBufferPos += reader->getBlockSize();

                // Checkpoint
                if (unlikely(ctx->trace & Ctx::TRACE_LWN))
                    ctx->logTrace(Ctx::TRACE_LWN, "checkpoint at " + std::to_string(currentBlock) + "/" + std::to_string(lwnEndBlock) +
                                                  " num: " + std::to_string(lwnNumCnt) + "/" + std::to_string(lwnNumMax));
                if (currentBlock == lwnEndBlock && lwnNumCnt == lwnNumMax) {
                    lastTransaction = nullptr;

                    if (unlikely(ctx->trace & Ctx::TRACE_LWN))
                        ctx->logTrace(Ctx::TRACE_LWN, "* analyze: " + std::to_string(lwnScn));

                    while (lwnRecords > 0) {
                        try {
                            analyzeLwn(lwnMembers[1]);
                        } catch (DataException& ex) {
                            if (ctx->flagsSet(Ctx::REDO_FLAGS_IGNORE_DATA_ERRORS)) {
                                ctx->error(ex.code, ex.msg);
                                ctx->warning(60013, "forced to continue working in spite of error");
                            } else
                                throw DataException(ex.code, "runtime error, aborting further redo log processing: " + ex.msg);
                        } catch (RedoLogException& ex) {
                            if (ctx->flagsSet(Ctx::REDO_FLAGS_IGNORE_DATA_ERRORS)) {
                                ctx->error(ex.code, ex.msg);
                                ctx->warning(60013, "forced to continue working in spite of error");
                            } else
                                throw RedoLogException(ex.code, "runtime error, aborting further redo log processing: " + ex.msg);
                        }

                        if (lwnRecords == 1) {
                            lwnRecords = 0;
                            break;
                        }

                        uint64_t lwnPos = 1;
                        while (true) {
                            if (lwnPos * 2 < lwnRecords && *lwnMembers[lwnPos * 2] < *lwnMembers[lwnRecords]) {
                                if (lwnPos * 2U + 1U < lwnRecords && *lwnMembers[lwnPos * 2 + 1] < *lwnMembers[lwnPos * 2]) {
                                    lwnMembers[lwnPos] = lwnMembers[lwnPos * 2 + 1];
                                    lwnPos *= 2;
                                    ++lwnPos;
                                } else {
                                    lwnMembers[lwnPos] = lwnMembers[lwnPos * 2];
                                    lwnPos *= 2;
                                }
                            } else if (lwnPos * 2U + 1U < lwnRecords && *lwnMembers[lwnPos * 2 + 1] < *lwnMembers[lwnRecords]) {
                                lwnMembers[lwnPos] = lwnMembers[lwnPos * 2 + 1];
                                lwnPos *= 2;
                                ++lwnPos;
                            } else
                                break;
                        }

                        lwnMembers[lwnPos] = lwnMembers[lwnRecords];
                        --lwnRecords;
                    }

                    if (lwnScn > metadata->firstDataScn) {
                        if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
                            ctx->logTrace(Ctx::TRACE_CHECKPOINT, "on: " + std::to_string(lwnScn));
                        builder->processCheckpoint(lwnScn, sequence, lwnTimestamp.toEpoch(ctx->hostTimezone),
                                                   static_cast<uint64_t>(currentBlock) * reader->getBlockSize(), switchRedo);

                        typeSeq minSequence = Ctx::ZERO_SEQ;
                        uint64_t minOffset = -1;
                        typeXid minXid;
                        transactionBuffer->checkpoint(minSequence, minOffset, minXid);
                        if (unlikely(ctx->trace & Ctx::TRACE_LWN))
                            ctx->logTrace(Ctx::TRACE_LWN, "* checkpoint: " + std::to_string(lwnScn));
                        metadata->checkpoint(lwnScn, lwnTimestamp, sequence,
                                             static_cast<uint64_t>(currentBlock) * reader->getBlockSize(),
                                             static_cast<uint64_t>(currentBlock - lwnConfirmedBlock) * reader->getBlockSize(), minSequence,
                                             minOffset, minXid);

                        if (ctx->stopCheckpoints > 0 && metadata->isNewData(lwnScn, builder->lwnIdx)) {
                            --ctx->stopCheckpoints;
                            if (ctx->stopCheckpoints == 0) {
                                ctx->info(0, "shutdown started - exhausted number of checkpoints");
                                ctx->stopSoft();
                            }
                        }
                        if (ctx->metrics)
                            ctx->metrics->emitCheckpointsOut(1);
                    } else {
                        if (ctx->metrics)
                            ctx->metrics->emitCheckpointsSkip(1);
                    }

                    lwnNumCnt = 0;
                    freeLwn();

                    if (ctx->metrics)
                        ctx->metrics->emitBytesParsed((currentBlock - lwnConfirmedBlock) * reader->getBlockSize());
                    lwnConfirmedBlock = currentBlock;
                } else if (unlikely(lwnNumCnt > lwnNumMax))
                    throw RedoLogException(50055, "lwn overflow: " + std::to_string(lwnNumCnt) + "/" + std::to_string(lwnNumMax));

                // Free memory
                if (redoBufferPos == Ctx::MEMORY_CHUNK_SIZE) {
                    reader->bufferFree(redoBufferNum);
                    reader->confirmReadData(confirmedBufferStart);
                }
            }

            // Processing finished
            if (!switchRedo && lwnScn > 0 && confirmedBufferStart == reader->getBufferEnd() && reader->getRet() == Reader::REDO_FINISHED) {
                if (lwnScn > metadata->firstDataScn) {
                    switchRedo = true;
                    if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
                        ctx->logTrace(Ctx::TRACE_CHECKPOINT, "on: " + std::to_string(lwnScn) + " with switch");
                    builder->processCheckpoint(lwnScn, sequence, lwnTimestamp.toEpoch(ctx->hostTimezone),
                                               static_cast<uint64_t>(currentBlock) * reader->getBlockSize(), switchRedo);
                    if (ctx->metrics)
                        ctx->metrics->emitCheckpointsOut(1);
                } else {
                    if (ctx->metrics)
                        ctx->metrics->emitCheckpointsSkip(1);
                }
            }

            if (ctx->softShutdown) {
                if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
                    ctx->logTrace(Ctx::TRACE_CHECKPOINT, "on: " + std::to_string(lwnScn) + " at exit");
                builder->processCheckpoint(lwnScn, sequence, lwnTimestamp.toEpoch(ctx->hostTimezone),
                                           static_cast<uint64_t>(currentBlock) * reader->getBlockSize(), false);
                if (ctx->metrics)
                    ctx->metrics->emitCheckpointsOut(1);

                reader->setRet(Reader::REDO_SHUTDOWN);
            } else {
                if (reader->checkFinished(confirmedBufferStart)) {
                    if (reader->getRet() == Reader::REDO_FINISHED && nextScn == Ctx::ZERO_SCN && reader->getNextScn() != Ctx::ZERO_SCN)
                        nextScn = reader->getNextScn();
                    if (reader->getRet() == Reader::REDO_STOPPED || reader->getRet() == Reader::REDO_OVERWRITTEN)
                        metadata->offset = static_cast<uint64_t>(lwnConfirmedBlock) * reader->getBlockSize();
                    break;
                }
            }
        }

        if (ctx->metrics && reader->getNextScn() != Ctx::ZERO_SCN) {
            int64_t diff = ctx->clock->getTimeT() - reader->getNextTime().toEpoch(ctx->hostTimezone);

            if (group == 0) {
                ctx->metrics->emitLogSwitchesArchived(1);
                ctx->metrics->emitLogSwitchesLagArchived(diff);
            } else {
                ctx->metrics->emitLogSwitchesOnline(1);
                ctx->metrics->emitLogSwitchesLagOnline(diff);
            }
        }

        // Print performance information
        if ((ctx->trace & Ctx::TRACE_PERFORMANCE) != 0) {
            time_ut cEnd = ctx->clock->getTimeUt();
            double suppLogPercent = 0.0;
            if (currentBlock != startBlock)
                suppLogPercent = 100.0 * ctx->suppLogSize / (static_cast<double>(currentBlock - startBlock) * reader->getBlockSize());

            if (group == 0) {
                double mySpeed = 0;
                double myTime = static_cast<double>(cEnd - cStart) / 1000.0;
                if (myTime > 0)
                    mySpeed = static_cast<double>(currentBlock - startBlock) * reader->getBlockSize() * 1000.0 / 1024 / 1024 / myTime;

                double myReadSpeed = 0;
                if (reader->getSumTime() > 0)
                    myReadSpeed = (static_cast<double>(reader->getSumRead()) * 1000000.0 / 1024 / 1024 / static_cast<double>(reader->getSumTime()));

                ctx->logTrace(Ctx::TRACE_PERFORMANCE, std::to_string(myTime) + " ms, " +
                                                      "Speed: " + std::to_string(mySpeed) + " MB/s, " +
                                                      "Redo log size: " + std::to_string(static_cast<uint64_t>(currentBlock - startBlock) *
                                                      reader->getBlockSize() / 1024 / 1024) +
                                                      " MB, Read size: " + std::to_string(reader->getSumRead() / 1024 / 1024) + " MB, " +
                                                      "Read speed: " + std::to_string(myReadSpeed) + " MB/s, " +
                                                      "Max LWN size: " + std::to_string(lwnAllocatedMax) + ", " +
                                                      "Supplemental redo log size: " + std::to_string(ctx->suppLogSize) + " bytes " +
                                                      "(" + std::to_string(suppLogPercent) + " %)");
            } else {
                ctx->logTrace(Ctx::TRACE_PERFORMANCE, "Redo log size: " + std::to_string(static_cast<uint64_t>(currentBlock - startBlock) *
                                                      reader->getBlockSize() / 1024 / 1024) + " MB, " +
                                                      "Max LWN size: " + std::to_string(lwnAllocatedMax) + ", " +
                                                      "Supplemental redo log size: " + std::to_string(ctx->suppLogSize) + " bytes " +
                                                      "(" + std::to_string(suppLogPercent) + " %)");
            }
        }

        if (ctx->dumpRedoLog >= 1 && ctx->dumpStream->is_open()) {
            *ctx->dumpStream << "END OF REDO DUMP\n";
            ctx->dumpStream->close();
        }

        freeLwn();
        return reader->getRet();
    }

    std::string Parser::toString() const {
        return "group: " + std::to_string(group) + " scn: " + std::to_string(firstScn) + " to " +
               std::to_string(nextScn != Ctx::ZERO_SCN ? nextScn : 0) + " seq: " + std::to_string(sequence) + " path: " + path;
    }
}
