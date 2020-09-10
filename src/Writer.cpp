/* Base class for thread to write output
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <thread>

#include "ConfigurationException.h"
#include "OutputBuffer.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Writer.h"

using namespace std;

void stopMain();

namespace OpenLogReplicator {

    Writer::Writer(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t maxMessageMb) :
        Thread(alias),
        outputBuffer(oracleAnalyser->outputBuffer),
        oracleAnalyser(oracleAnalyser),
        maxMessageMb(maxMessageMb) {

        msgBuffer = oracleAnalyser->getMemoryChunk("WRITER", false);
    }

    Writer::~Writer() {
        if (msgBuffer != nullptr) {
            oracleAnalyser->freeMemoryChunk("WRITER", msgBuffer, false);
            msgBuffer = nullptr;
        }
    }

    void *Writer::run(void) {
        TRACE(TRACE2_THREADS, "WRITER (" << hex << this_thread::get_id() << ") START");

        INFO("Writer is starting: " << getName());

        try {
            for (;;) {
                uint64_t length = 0, bufferEnd;

                //get new block to read
                {
                    unique_lock<mutex> lck(outputBuffer->mtx);
                    bufferEnd = *((uint64_t*)(outputBuffer->firstBuffer + OUTPUT_BUFFER_END));
                    length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                    while ((outputBuffer->firstBufferPos == bufferEnd || length == 0) && !shutdown) {
                        oracleAnalyser->waitingForWriter = false;
                        oracleAnalyser->memoryCond.notify_all();
                        outputBuffer->writersCond.wait(lck);
                        bufferEnd = *((uint64_t*)(outputBuffer->firstBuffer + OUTPUT_BUFFER_END));
                        length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                        if (!shutdown)
                            oracleAnalyser->waitingForWriter = true;
                    }
                }

                //all data sent & shutdown command
                if (outputBuffer->firstBufferPos == bufferEnd && shutdown)
                    break;

                while (outputBuffer->firstBufferPos < bufferEnd) {
                    length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                    if (length == 0)
                        break;

                    outputBuffer->firstBufferPos += OUTPUT_BUFFER_LENGTH_SIZE;
                    uint64_t leftLength = (length + 7) & 0xFFFFFFFFFFFFFFF8;

                    //message in one part - send directly from buffer
                    if (outputBuffer->firstBufferPos + leftLength < MEMORY_CHUNK_SIZE) {
                        sendMessage(outputBuffer->firstBuffer + outputBuffer->firstBufferPos, length, false);
                        outputBuffer->firstBufferPos += leftLength;

                    //message in many parts - copy
                    } else {
                        uint8_t *buffer;
                        bool dealloc = false;
                       if (leftLength <= MEMORY_CHUNK_SIZE) {
                            buffer = msgBuffer;
                        } else {
                            buffer = (uint8_t*)malloc(leftLength);
                            if (buffer == nullptr) {
                                RUNTIME_FAIL("could not allocate temporary buffer for JSON message for " << dec << leftLength << " bytes");
                            }
                            dealloc = true;
                        }

                        uint64_t targetPos = 0;

                        while (leftLength > 0) {
                            if (outputBuffer->firstBufferPos + leftLength >= MEMORY_CHUNK_SIZE) {
                                uint64_t tmpLength = (MEMORY_CHUNK_SIZE - outputBuffer->firstBufferPos);
                                memcpy(buffer + targetPos, outputBuffer->firstBuffer + outputBuffer->firstBufferPos, tmpLength);
                                leftLength -= tmpLength;
                                targetPos += tmpLength;

                                //switch to next
                                uint8_t* nextBuffer = *((uint8_t**)(outputBuffer->firstBuffer + OUTPUT_BUFFER_NEXT));
                                oracleAnalyser->freeMemoryChunk("KAFKA", outputBuffer->firstBuffer, true);
                                outputBuffer->firstBufferPos = OUTPUT_BUFFER_DATA;

                                {
                                    unique_lock<mutex> lck(outputBuffer->mtx);
                                    --outputBuffer->buffersAllocated;
                                    outputBuffer->firstBuffer = nextBuffer;
                                    oracleAnalyser->memoryCond.notify_all();
                                }
                            } else {
                                memcpy(buffer + targetPos, outputBuffer->firstBuffer + outputBuffer->firstBufferPos, leftLength);
                                outputBuffer->firstBufferPos += leftLength;
                                leftLength = 0;
                            }
                        }

                        sendMessage(buffer, length, dealloc);
                        break;
                    }
                }
            }

        } catch(ConfigurationException &ex) {
            stopMain();
        } catch(RuntimeException &ex) {
            stopMain();
        }

        INFO("Writer is stopping: " << getName());

        TRACE(TRACE2_THREADS, "WRITER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }

    //0x05010B0B
    void Writer::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = 0, fieldNum = 0, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength = 0, colLength = 0;
        OracleObject *object = redoLogRecord2->object;

        for (uint64_t i = fieldNum; i < redoLogRecord2->rowData; ++i)
            oracleAnalyser->nextField(redoLogRecord2, fieldNum, fieldPos, fieldLength);

        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord2->nrow; ++r) {
            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord2->data[fieldPos + pos + 2];
            pos = 3;

            memset(outputBuffer->colIsSupp, 0, object->maxSegCol * sizeof(uint8_t));
            memset(outputBuffer->afterPos, 0, object->maxSegCol * sizeof(uint64_t));
            memset(outputBuffer->beforePos, 0, object->maxSegCol * sizeof(uint64_t));

            if ((redoLogRecord2->op & OP_ROWDEPENDENCIES) != 0) {
                if (oracleAnalyser->version < 0x12200)
                    pos += 6;
                else
                    pos += 8;
            }

            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (i >= jcc) {
                    colLength = 0;
                } else {
                    colLength = redoLogRecord2->data[fieldPos + pos];
                    ++pos;
                    if (colLength == 0xFF) {
                        colLength = 0;
                    } else
                    if (colLength == 0xFE) {
                        colLength = oracleAnalyser->read16(redoLogRecord2->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (object->columns[i] != nullptr) {
                    outputBuffer->afterPos[i] = fieldPos + pos;
                    outputBuffer->afterLen[i] = colLength;
                    outputBuffer->afterRecord[i] = redoLogRecord2;
                }
                pos += colLength;
            }

            outputBuffer->processInsert(object, redoLogRecord2->bdba,
                    oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2), redoLogRecord1->xid);

            fieldPosStart += oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta + r * 2);
        }
    }

    //0x05010B0C
    void Writer::parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = 0, fieldNum = 0, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength = 0, colLength = 0;
        OracleObject *object = redoLogRecord1->object;

        for (uint64_t i = fieldNum; i < redoLogRecord1->rowData; ++i)
            oracleAnalyser->nextField(redoLogRecord1, fieldNum, fieldPos, fieldLength);

        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord1->nrow; ++r) {
            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord1->data[fieldPos + pos + 2];
            pos = 3;

            memset(outputBuffer->colIsSupp, 0, object->maxSegCol * sizeof(uint8_t));
            memset(outputBuffer->afterPos, 0, object->maxSegCol * sizeof(uint64_t));
            memset(outputBuffer->beforePos, 0, object->maxSegCol * sizeof(uint64_t));

            if ((redoLogRecord1->op & OP_ROWDEPENDENCIES) != 0) {
                if (oracleAnalyser->version < 0x12200)
                    pos += 6;
                else
                    pos += 8;
            }

            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (i >= jcc)
                    colLength = 0;
                else {
                    colLength = redoLogRecord1->data[fieldPos + pos];
                    ++pos;
                    if (colLength == 0xFF) {
                        colLength = 0;
                    } else
                    if (colLength == 0xFE) {
                        colLength = oracleAnalyser->read16(redoLogRecord1->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (object->columns[i] != nullptr) {
                    outputBuffer->beforePos[i] = fieldPos + pos;
                    outputBuffer->beforeLen[i] = colLength;
                    outputBuffer->beforeRecord[i] = redoLogRecord1;
                }

                pos += colLength;
            }

            outputBuffer->processDelete(object, redoLogRecord2->bdba,
                    oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2), redoLogRecord1->xid);

            fieldPosStart += oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->rowLenghsDelta + r * 2);
        }
    }

    void Writer::parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type) {
        typedba bdba;
        typeslot slot;
        RedoLogRecord *redoLogRecord1p, *redoLogRecord2p = nullptr;
        OracleObject *object = redoLogRecord1->object;

        if (type == TRANSACTION_INSERT) {
            redoLogRecord2p = redoLogRecord2;
            while (redoLogRecord2p != nullptr) {
                if ((redoLogRecord2p->fb & FB_F) != 0)
                    break;
                redoLogRecord2p = redoLogRecord2p->next;
            }

            if (redoLogRecord2p == nullptr) {
                WARNING("could not find correct rowid for INSERT");
                bdba = 0;
                slot = 0;
            } else {
                bdba = redoLogRecord2p->bdba;
                slot = redoLogRecord2p->slot;
            }
        } else if (type == TRANSACTION_DELETE) {
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        } else {
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        }

        uint64_t fieldPos, fieldNum, colNum, colShift, rowDeps;
        uint16_t fieldLength, colLength;
        uint8_t *nulls, bits, *colNums;

        memset(outputBuffer->colIsSupp, 0, object->maxSegCol * sizeof(uint8_t));
        memset(outputBuffer->afterPos, 0, object->maxSegCol * sizeof(uint64_t));
        memset(outputBuffer->beforePos, 0, object->maxSegCol * sizeof(uint64_t));

        //data in UNDO
        redoLogRecord1p = redoLogRecord1;
        redoLogRecord2p = redoLogRecord2;
        colNums = nullptr;

        while (redoLogRecord1p != nullptr) {
            fieldPos = 0;
            fieldNum = 0;
            fieldLength = 0;

            //UNDO
            if (redoLogRecord1p->rowData > 0) {
                nulls = redoLogRecord1p->data + redoLogRecord1p->nullsDelta;
                bits = 1;

                if (redoLogRecord1p->suppLogBefore > 0)
                    colShift = redoLogRecord1p->suppLogBefore - 1;
                else
                    colShift = 0;

                if (redoLogRecord1p->colNumsDelta > 0) {
                    colNums = redoLogRecord1p->data + redoLogRecord1p->colNumsDelta;
                    colShift -= oracleAnalyser->read16(colNums);
                } else {
                    colNums = nullptr;
                }

                for (uint64_t i = fieldNum; i < redoLogRecord1p->rowData - 1; ++i)
                    oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);

                for (uint64_t i = 0; i < redoLogRecord1p->cc; ++i) {
                    if (fieldNum + 1 > redoLogRecord1p->fieldCnt) {
                        WARNING("table: " << object->owner << "." << object->name << ": out of columns (Undo): " << dec << colNum << "/" << (uint64_t)redoLogRecord1p->cc);
                        break;
                    }
                    if (colNums != nullptr) {
                        colNum = oracleAnalyser->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (colNum >= object->maxSegCol) {
                        WARNING("table: " << object->owner << "." << object->name << ": referring to unknown column id(" <<
                                dec << colNum << "), probably table was altered, ignoring extra column");
                        break;
                    }

                    if ((*nulls & bits) != 0)
                        colLength = 0;
                    else {
                        oracleAnalyser->skipEmptyFields(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                        oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                        colLength = fieldLength;
                    }

                    if (object->columns[colNum] != nullptr) {
                        outputBuffer->beforePos[colNum] = fieldPos;
                        outputBuffer->beforeLen[colNum] = colLength;
                        outputBuffer->beforeRecord[colNum] = redoLogRecord1p;
                    }

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            //supplemental columns
            if (redoLogRecord1p->suppLogRowData > 0) {
                for (uint64_t i = fieldNum; i < redoLogRecord1p->suppLogRowData - 1; ++i)
                    oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);

                colNums = redoLogRecord1p->data + redoLogRecord1p->suppLogNumsDelta;
                uint8_t* colSizes = redoLogRecord1p->data + redoLogRecord1p->suppLogLenDelta;

                for (uint64_t i = 0; i < redoLogRecord1p->suppLogCC; ++i) {
                    if (fieldNum + 1 > redoLogRecord1p->fieldCnt) {
                        RUNTIME_FAIL("table: " << object->owner << "." << object->name << ": out of columns (Supp): " << dec << colNum << "/" << (uint64_t)redoLogRecord1p->suppLogCC);
                    }

                    oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                    colNum = oracleAnalyser->read16(colNums) - 1;

                    if (colNum >= object->maxSegCol) {
                        WARNING("table: " << object->owner << "." << object->name << ": referring to unknown column id(" <<
                                dec << colNum << "), probably table was altered, ignoring extra column");
                        break;
                    }

                    colNums += 2;
                    colLength = oracleAnalyser->read16(colSizes);

                    if (object->columns[colNum] != nullptr) {
                        outputBuffer->colIsSupp[colNum] = 1;
                        if (colLength == 0xFFFF)
                            colLength = 0;

                        //insert, lock, update
                        if (outputBuffer->afterPos[colNum] == 0 && (redoLogRecord2p->opCode == 0x0B02 || redoLogRecord2p->opCode == 0x0B04 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B10)) {
                            outputBuffer->afterPos[colNum] = fieldPos;
                            outputBuffer->afterLen[colNum] = colLength;
                            outputBuffer->afterRecord[colNum] = redoLogRecord1p;
                        }
                        //delete, update, overwrite
                        if (outputBuffer->beforePos[colNum] == 0 && (redoLogRecord2p->opCode == 0x0B03 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B06 || redoLogRecord2p->opCode == 0x0B10)) {
                            outputBuffer->beforePos[colNum] = fieldPos;
                            outputBuffer->beforeLen[colNum] = colLength;
                            outputBuffer->beforeRecord[colNum] = redoLogRecord1p;
                        }
                    }

                    colSizes += 2;
                }
            }

            //REDO
            if (redoLogRecord2p->rowData > 0) {
                fieldPos = 0;
                fieldNum = 0;
                fieldLength = 0;
                nulls = redoLogRecord2p->data + redoLogRecord2p->nullsDelta;
                bits = 1;

                if (redoLogRecord2p->colNumsDelta > 0) {
                    colNums = redoLogRecord2p->data + redoLogRecord2p->colNumsDelta;
                    colShift = redoLogRecord2p->suppLogAfter - 1 - oracleAnalyser->read16(colNums);
                } else {
                    colNums = nullptr;
                    colShift = redoLogRecord2p->suppLogAfter - 1;
                }

                for (uint64_t i = fieldNum; i < redoLogRecord2p->rowData - 1; ++i)
                    oracleAnalyser->nextField(redoLogRecord2p, fieldNum, fieldPos, fieldLength);

                for (uint64_t i = 0; i < redoLogRecord2p->cc; ++i) {
                    if (fieldNum + 1 > redoLogRecord2p->fieldCnt) {
                        WARNING("table: " << object->owner << "." << object->name << ": out of columns (Redo): " << dec << colNum << "/" << (uint64_t)redoLogRecord2p->cc);
                        break;
                    }

                    oracleAnalyser->nextField(redoLogRecord2p, fieldNum, fieldPos, fieldLength);

                    if (colNums != nullptr) {
                        colNum = oracleAnalyser->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (colNum >= object->maxSegCol) {
                        WARNING("table: " << object->owner << "." << object->name << ": referring to unknown column id(" <<
                                dec << colNum << "), probably table was altered, ignoring extra column");
                        break;
                    }

                    if (object->columns[colNum] != nullptr) {
                        if ((*nulls & bits) != 0)
                            colLength = 0;
                        else
                            colLength = fieldLength;

                        outputBuffer->afterPos[colNum] = fieldPos;
                        outputBuffer->afterLen[colNum] = colLength;
                        outputBuffer->afterRecord[colNum] = redoLogRecord2p;
                    } else {
                        //present null value for
                        if (redoLogRecord2p->data[fieldPos] == 1 && fieldLength == 1 && colNum + 1 < object->maxSegCol && object->columns[colNum + 1] != nullptr) {
                            outputBuffer->afterPos[colNum + 1] = fieldPos;
                            outputBuffer->afterLen[colNum + 1] = 0;
                            outputBuffer->afterRecord[colNum + 1] = redoLogRecord2p;
                        }
                    }

                    bits <<= 1;
                    if (bits == 0) {
                        bits = 1;
                        ++nulls;
                    }
                }
            }

            redoLogRecord1p = redoLogRecord1p->next;
            redoLogRecord2p = redoLogRecord2p->next;
        }

        if ((oracleAnalyser->trace2 & TRACE2_DML) != 0) {
            TRACE(TRACE2_DML, "tab: " << object->owner << "." << object->name << " type: " << type);
            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                TRACE(TRACE2_DML, dec << i << ": " <<
                        " B(" << outputBuffer->beforePos[i] << ", " << dec << outputBuffer->beforeLen[i] << ")" <<
                        " A(" << outputBuffer->afterPos[i] << ", " << dec << outputBuffer->afterLen[i] << ")" <<
                        " pk: " << dec << object->columns[i]->numPk <<
                        " supp: " << dec << (uint64_t)outputBuffer->colIsSupp[i]);
            }
        }

        if (type == TRANSACTION_UPDATE)
            outputBuffer->processUpdate(object, bdba, slot, redoLogRecord1->xid); else
        if (type == TRANSACTION_INSERT)
            outputBuffer->processInsert(object, bdba, slot, redoLogRecord1->xid); else
        if (type == TRANSACTION_DELETE)
            outputBuffer->processDelete(object, bdba, slot, redoLogRecord1->xid);
    }

    //0x18010000
    void Writer::parseDDL(RedoLogRecord *redoLogRecord1) {
        uint64_t fieldPos = 0, fieldNum = 0, sqlLength;
        uint16_t seq = 0, cnt = 0, type = 0, fieldLength = 0;
        OracleObject *object = redoLogRecord1->object;
        uint8_t *sqlText = nullptr;

        oracleAnalyser->nextField(redoLogRecord1, fieldNum, fieldPos, fieldLength);
        //field: 1
        type = oracleAnalyser->read16(redoLogRecord1->data + fieldPos + 12);
        seq = oracleAnalyser->read16(redoLogRecord1->data + fieldPos + 18);
        cnt = oracleAnalyser->read16(redoLogRecord1->data + fieldPos + 20);

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 2

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 3

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 4

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 5

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 6

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 7

        if (!oracleAnalyser->nextFieldOpt(redoLogRecord1, fieldNum, fieldPos, fieldLength))
            return;
        //field: 8
        sqlLength = fieldLength;
        sqlText = redoLogRecord1->data + fieldPos;

        if (type == 85)
            outputBuffer->processDDL(object, type, seq, "truncate", sqlText, sqlLength - 1);
        else if (type == 12)
            outputBuffer->processDDL(object, type, seq, "drop", sqlText, sqlLength - 1);
        else if (type == 15)
            outputBuffer->processDDL(object, type, seq, "alter", sqlText, sqlLength - 1);
        else
            outputBuffer->processDDL(object, type, seq, "?", sqlText, sqlLength - 1);
    }
}
