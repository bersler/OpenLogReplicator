/* Thread writing to Kafka stream
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
#include <librdkafka/rdkafkacpp.h>

#include "OutputBuffer.h"
#include "ConfigurationException.h"
#include "KafkaWriter.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"

using namespace std;
using namespace RdKafka;

void stopMain();

namespace OpenLogReplicator {

    KafkaWriter::KafkaWriter(string alias, string brokers, string topic, OracleAnalyser *oracleAnalyser, uint64_t maxMessageMb, uint64_t stream,
            uint64_t singleDml, uint64_t showColumns, uint64_t test, uint64_t timestampFormat, uint64_t charFormat) :
        Writer(alias, oracleAnalyser, stream, singleDml, showColumns, test, timestampFormat, charFormat, maxMessageMb),
        msgBuffer(nullptr),
        conf(nullptr),
        tconf(nullptr),
        brokers(brokers),
        topic(topic),
        producer(nullptr),
        ktopic(nullptr),
        lastScn(0) {

        msgBuffer = oracleAnalyser->getMemoryChunk("KAFKATMP", false);
        conf = Conf::create(Conf::CONF_GLOBAL);
        tconf = Conf::create(Conf::CONF_TOPIC);
    }

    KafkaWriter::~KafkaWriter() {
        if (msgBuffer != nullptr) {
            oracleAnalyser->freeMemoryChunk("KAFKATMP", msgBuffer, false);
            msgBuffer = nullptr;
        }

        if (ktopic != nullptr) {
            delete ktopic;
            ktopic = nullptr;
        }
        if (producer != nullptr) {
            delete producer;
            producer = nullptr;
        }
        if (tconf != nullptr) {
            delete tconf;
            tconf = nullptr;
        }
        if (conf != nullptr) {
            delete conf;
            conf = nullptr;
        }
    }

    void KafkaWriter::sendMessage(uint8_t *buffer, uint64_t length, bool dealloc) {
        if (test >= 1) {
            cout.write((const char*)buffer, length);
            cout << endl;
            if (dealloc)
                free(buffer);
        } else {
            int msgflags = Producer::RK_MSG_COPY;
            if (dealloc)
                msgflags = Producer::RK_MSG_FREE;

            if (producer->produce(ktopic, Topic::PARTITION_UA, msgflags, buffer, length, nullptr, nullptr)) {
                //on error, memory is not released by librdkafka
                if (dealloc)
                    free(buffer);
                RUNTIME_FAIL("writing to topic, bytes sent: " << dec << length);
            }
        }
    }

    void *KafkaWriter::run(void) {
        TRACE(TRACE2_THREADS, "WRITER (" << hex << this_thread::get_id() << ") START");

        if (test >= 1) {
            INFO("Kafka Writer with stdout output mode (test: " << dec << test << ") is starting");
        } else {
            INFO("Kafka Writer for: " << brokers << " topic: " << topic << " is starting");
        }

        try {
            for (;;) {
                uint64_t length = 0, bufferEnd;

                //get new block to read
                {
                    unique_lock<mutex> lck(outputBuffer->mtx);
                    bufferEnd = *((uint64_t*)(outputBuffer->firstBuffer + KAFKA_BUFFER_END));
                    length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                    while ((outputBuffer->firstBufferPos == bufferEnd || length == 0) && !shutdown) {
                        oracleAnalyser->waitingForKafkaWriter = false;
                        oracleAnalyser->memoryCond.notify_all();
                        outputBuffer->writersCond.wait(lck);
                        bufferEnd = *((uint64_t*)(outputBuffer->firstBuffer + KAFKA_BUFFER_END));
                        length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                        if (!shutdown)
                            oracleAnalyser->waitingForKafkaWriter = true;
                    }
                }

                //all data sent & shutdown command
                if (outputBuffer->firstBufferPos == bufferEnd && shutdown)
                    break;

                while (outputBuffer->firstBufferPos < bufferEnd) {
                    length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                    if (length == 0)
                        break;

                    outputBuffer->firstBufferPos += KAFKA_BUFFER_LENGTH_SIZE;
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
                                RUNTIME_FAIL("could not allocate temporary buffer for Kafka message for " << dec << leftLength << " bytes");
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
                                uint8_t* nextBuffer = *((uint8_t**)(outputBuffer->firstBuffer + KAFKA_BUFFER_NEXT));
                                oracleAnalyser->freeMemoryChunk("KAFKA", outputBuffer->firstBuffer, true);
                                outputBuffer->firstBufferPos = KAFKA_BUFFER_DATA;

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

        if (test >= 1) {
            INFO("Kafka Writer with stdout output mode (test: " << dec << test << ") is shutting down");
        } else {
            INFO("Kafka Writer for: " << brokers << " topic: " << topic << " is shutting down");
        }
        TRACE(TRACE2_THREADS, "WRITER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }

    void KafkaWriter::initialize(void) {
        string errstr;
        conf->set("metadata.broker.list", brokers, errstr);
        conf->set("client.id", "OpenLogReplicator", errstr);
        string maxMessageMbStr = to_string(maxMessageMb * 1024 * 1024);
        conf->set("message.max.bytes", maxMessageMbStr.c_str(), errstr);

        if (test == 0) {
            producer = Producer::create(conf, errstr);
            if (producer == nullptr) {
                CONFIG_FAIL("Kafka message: " << errstr);
            }

            ktopic = Topic::create(producer, topic, tconf, errstr);
            if (ktopic == nullptr) {
                CONFIG_FAIL("Kafka message: " << errstr);
            }
        }
    }

    void KafkaWriter::beginTran(typescn scn, typetime time, typexid xid) {
        if (stream == STREAM_JSON) {
            outputBuffer
                    ->beginMessage()
                    ->append('{')
                    ->appendScn(scn)
                    ->append(',')
                    ->appendMs("timestamp", time.toTime() * 1000)
                    ->append(',')
                    ->appendXid(xid)
                    ->appendChr(",\"dml\":[");
        }

        lastTime = time;
        lastScn = scn;
    }

    void KafkaWriter::next(void) {
        if (stream == STREAM_JSON) {
            if (test <= 1)
                outputBuffer->append(',');
        }
    }

    void KafkaWriter::commitTran(void) {
        if (stream == STREAM_JSON) {
            if (test <= 1)
                outputBuffer->appendChr("]}");
            outputBuffer->commitMessage();
        }
    }

    //0x05010B0B
    void KafkaWriter::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = 0, fieldNum = 0, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength = 0, colLength = 0;
        OracleObject *object = redoLogRecord2->object;

        for (uint64_t i = fieldNum; i < redoLogRecord2->rowData; ++i)
            oracleAnalyser->nextField(redoLogRecord2, fieldNum, fieldPos, fieldLength);

        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord2->nrow; ++r) {
            if (r > 0) {
                next();
            }

            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord2->data[fieldPos + pos + 2];
            pos = 3;

            if ((redoLogRecord2->op & OP_ROWDEPENDENCIES) != 0) {
                if (oracleAnalyser->version < 0x12200)
                    pos += 6;
                else
                    pos += 8;
            }

            if (stream == STREAM_JSON) {
                if (test >= 2)
                    outputBuffer->append('\n');
                outputBuffer->append('{');
                if (test >= 2)
                    outputBuffer
                            ->appendScn(lastScn)
                            ->append(',');
                outputBuffer
                        ->appendOperation("insert")
                        ->append(',')
                        ->appendTable(object->owner, object->objectName)
                        ->append(',')
                        ->appendRowid(redoLogRecord2->objn, redoLogRecord2->objd, redoLogRecord2->bdba,
                                oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2))
                        ->appendChr(",\"after\":{");
            } else if (stream == STREAM_DBZ_JSON) {
                outputBuffer
                        ->beginMessage()
                        ->appendDbzHead(object)
                        ->appendChr("\"before\":null,\"after\":{");
            }


            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                bool isNull = false;

                if (i >= jcc)
                    isNull = true;
                else {
                    colLength = redoLogRecord2->data[fieldPos + pos];
                    ++pos;
                    if (colLength == 0xFF) {
                        colLength = 0;
                        isNull = true;
                    } else
                    if (colLength == 0xFE) {
                        colLength = oracleAnalyser->read16(redoLogRecord2->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (object->columns[i] != nullptr) {
                    if (isNull) {
                        if (showColumns >= 1 || object->columns[i]->numPk > 0) {
                            if (prevValue)
                                outputBuffer->append(',');
                            else
                                prevValue = true;

                            outputBuffer->appendNull(object->columns[i]->columnName);
                        }
                    } else {
                        if (prevValue)
                            outputBuffer->append(',');
                        else
                            prevValue = true;

                        outputBuffer->appendValue(object->columns[i]->columnName, redoLogRecord2, object->columns[i]->typeNo,
                                object->columns[i]->charsetId, fieldPos + pos, colLength);
                    }
                }
                pos += colLength;
            }

            if (stream == STREAM_JSON) {
                outputBuffer->appendChr("}}");
            } else
            if (stream == STREAM_DBZ_JSON) {

                outputBuffer
                        ->append('}')
                        ->appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'c', redoLogRecord1->xid)
                        ->commitMessage();
            }

            fieldPosStart += oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta + r * 2);
        }
    }

    //0x05010B0C
    void KafkaWriter::parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = 0, fieldNum = 0, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength = 0, colLength = 0;
        OracleObject *object = redoLogRecord1->object;

        for (uint64_t i = fieldNum; i < redoLogRecord1->rowData; ++i)
            oracleAnalyser->nextField(redoLogRecord1, fieldNum, fieldPos, fieldLength);

        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord1->nrow; ++r) {
            if (r > 0) {
                next();
            }

            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord1->data[fieldPos + pos + 2];
            pos = 3;

            if ((redoLogRecord1->op & OP_ROWDEPENDENCIES) != 0) {
                if (oracleAnalyser->version < 0x12200)
                    pos += 6;
                else
                    pos += 8;
            }

            if (stream == STREAM_JSON) {
                if (test >= 2)
                    outputBuffer->append('\n');
                outputBuffer->append('{');
                if (test >= 2)
                    outputBuffer
                            ->appendScn(lastScn)
                            ->append(',');
                outputBuffer
                        ->appendOperation("delete")
                        ->append(',')
                        ->appendTable(object->owner, object->objectName)
                        ->append(',')
                        ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->bdba,
                                oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2))
                        ->appendChr(",\"before\":{");
            } else
            if (stream == STREAM_DBZ_JSON) {
                outputBuffer
                        ->beginMessage()
                        ->appendDbzHead(object)
                        ->appendChr("\"before\":{");
            }

            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                bool isNull = false;

                if (i >= jcc)
                    isNull = true;
                else {
                    colLength = redoLogRecord1->data[fieldPos + pos];
                    ++pos;
                    if (colLength == 0xFF) {
                        colLength = 0;
                        isNull = true;
                    } else
                    if (colLength == 0xFE) {
                        colLength = oracleAnalyser->read16(redoLogRecord1->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (object->columns[i] != nullptr) {
                    if (isNull) {
                        if (showColumns >= 1 || object->columns[i]->numPk > 0) {
                            if (prevValue)
                                outputBuffer->append(',');
                            else
                                prevValue = true;

                            outputBuffer->appendNull(object->columns[i]->columnName);
                        }
                    } else {
                        if (prevValue)
                            outputBuffer->append(',');
                        else
                            prevValue = true;

                        outputBuffer->appendValue(object->columns[i]->columnName, redoLogRecord1, object->columns[i]->typeNo,
                                object->columns[i]->charsetId, fieldPos + pos, colLength);
                    }
                }

                pos += colLength;
            }

            if (stream == STREAM_JSON) {
                outputBuffer->appendChr("}}");
            } else
            if (stream == STREAM_DBZ_JSON) {
                outputBuffer
                    ->appendChr("},\"after\":null,")
                    ->appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'd', redoLogRecord1->xid)
                    ->commitMessage();
            }

            fieldPosStart += oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->rowLenghsDelta + r * 2);
        }
    }

    void KafkaWriter::parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type) {
        typedba bdba;
        typeslot slot;
        RedoLogRecord *redoLogRecord1p, *redoLogRecord2p = nullptr;
        OracleObject *object = nullptr;
        typeobj objn, objd;

        if (stream == STREAM_JSON) {
            if (test >= 2)
                outputBuffer->append('\n');
            outputBuffer
                    ->append('{');
            if (test >= 2)
                outputBuffer
                    ->appendScn(lastScn)
                    ->append(',');
        }

        objn = redoLogRecord1->objn;
        objd = redoLogRecord1->objd;
        object = redoLogRecord1->object;

        if (type == TRANSACTION_INSERT) {
            if (stream == STREAM_JSON) {
                outputBuffer->appendOperation("insert");
            }

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
            if (stream == STREAM_JSON) {
                outputBuffer->appendOperation("delete");
            }

            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        } else {
            if (stream == STREAM_JSON) {
                outputBuffer->appendOperation("update");
            }

            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        }

        if (stream == STREAM_JSON) {
            outputBuffer
                    ->append(',')
                    ->appendTable(object->owner, object->objectName)
                    ->append(',')
                    ->appendRowid(objn, objd, bdba, slot);
        }

        if (stream == STREAM_DBZ_JSON) {
            outputBuffer
                    ->beginMessage()
                    ->appendDbzHead(object)
                    ->appendChr("\"before\":");
        }

        uint64_t fieldPos, fieldNum, colNum, colShift, rowDeps;
        uint16_t fieldLength, colLength;
        uint8_t *nulls, bits, *colNums;
        bool prevValue = false;

        memset(colIsSupp, 0, object->maxSegCol * sizeof(uint8_t));
        memset(afterPos, 0, object->maxSegCol * sizeof(uint64_t));
        memset(beforePos, 0, object->maxSegCol * sizeof(uint64_t));

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
                        WARNING("table: " << object->owner << "." << object->objectName << ": out of columns (Undo): " << dec << colNum << "/" << (uint64_t)redoLogRecord1p->cc);
                        break;
                    }
                    if (colNums != nullptr) {
                        colNum = oracleAnalyser->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (colNum >= object->maxSegCol) {
                        WARNING("table: " << object->owner << "." << object->objectName << ": referring to unknown column id(" <<
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
                        beforePos[colNum] = fieldPos;
                        beforeLen[colNum] = colLength;
                        beforeRecord[colNum] = redoLogRecord1p;
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
                        RUNTIME_FAIL("table: " << object->owner << "." << object->objectName << ": out of columns (Supp): " << dec << colNum << "/" << (uint64_t)redoLogRecord1p->suppLogCC);
                    }

                    oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                    colNum = oracleAnalyser->read16(colNums) - 1;

                    if (colNum >= object->maxSegCol) {
                        WARNING("table: " << object->owner << "." << object->objectName << ": referring to unknown column id(" <<
                                dec << colNum << "), probably table was altered, ignoring extra column");
                        break;
                    }

                    colNums += 2;
                    colLength = oracleAnalyser->read16(colSizes);

                    if (object->columns[colNum] != nullptr) {
                        colIsSupp[colNum] = 1;
                        if (colLength == 0xFFFF)
                            colLength = 0;

                        //insert, lock, update
                        if (afterPos[colNum] == 0 && (redoLogRecord2p->opCode == 0x0B02 || redoLogRecord2p->opCode == 0x0B04 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B10)) {
                            afterRecord[colNum] = redoLogRecord1p;
                            afterPos[colNum] = fieldPos;
                            afterLen[colNum] = colLength;
                        }
                        //delete, update, overwrite
                        if (beforePos[colNum] == 0 && (redoLogRecord2p->opCode == 0x0B03 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B06 || redoLogRecord2p->opCode == 0x0B10)) {
                            beforeRecord[colNum] = redoLogRecord1p;
                            beforePos[colNum] = fieldPos;
                            beforeLen[colNum] = colLength;
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
                        WARNING("table: " << object->owner << "." << object->objectName << ": out of columns (Redo): " << dec << colNum << "/" << (uint64_t)redoLogRecord2p->cc);
                        break;
                    }

                    oracleAnalyser->nextField(redoLogRecord2p, fieldNum, fieldPos, fieldLength);

                    if (colNums != nullptr) {
                        colNum = oracleAnalyser->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (colNum >= object->maxSegCol) {
                        WARNING("table: " << object->owner << "." << object->objectName << ": referring to unknown column id(" <<
                                dec << colNum << "), probably table was altered, ignoring extra column");
                        break;
                    }

                    if (object->columns[colNum] != nullptr) {
                        if ((*nulls & bits) != 0)
                            colLength = 0;
                        else
                            colLength = fieldLength;

                        afterPos[colNum] = fieldPos;
                        afterLen[colNum] = colLength;
                        afterRecord[colNum] = redoLogRecord2p;
                    } else {
                        //present null value for
                        if (redoLogRecord2p->data[fieldPos] == 1 && fieldLength == 1 && colNum + 1 < object->maxSegCol && object->columns[colNum + 1] != nullptr) {
                            afterPos[colNum + 1] = fieldPos;
                            afterLen[colNum + 1] = 0;
                            afterRecord[colNum + 1] = redoLogRecord2p;
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
            TRACE(TRACE2_DML, "tab: " << object->owner << "." << object->objectName << " type: " << type);
            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                TRACE(TRACE2_DML, dec << i << ": " <<
                        " B(" << beforePos[i] << ", " << dec << beforeLen[i] << ")" <<
                        " A(" << afterPos[i] << ", " << dec << afterLen[i] << ")" <<
                        " pk: " << dec << object->columns[i]->numPk <<
                        " supp: " << dec << (uint64_t)colIsSupp[i]);
            }
        }

        if (type == TRANSACTION_UPDATE && showColumns <= 1) {
            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                //remove unchanged column values - only for tables with defined primary key
                if (object->columns[i]->numPk == 0 && beforePos[i] > 0 && afterPos[i] > 0 && beforeLen[i] == afterLen[i]) {
                    if (beforeLen[i] == 0 || memcmp(beforeRecord[i]->data + beforePos[i], afterRecord[i]->data + afterPos[i], beforeLen[i]) == 0) {
                        beforePos[i] = 0;
                        afterPos[i] = 0;
                        beforeLen[i] = 0;
                        afterLen[i] = 0;
                    }
                }

                //remove columns additionally present, but not modified
                if (beforePos[i] > 0 && beforeLen[i] == 0 && afterPos[i] == 0) {
                    if (object->columns[i]->numPk == 0) {
                        beforePos[i] = 0;
                    } else {
                        afterPos[i] = beforePos[i];
                        afterLen[i] = beforeLen[i];
                        afterRecord[i] = beforeRecord[i];
                    }
                }
                if (afterPos[i] > 0 && afterLen[i] == 0 && beforePos[i] == 0) {
                    if (object->columns[i]->numPk == 0) {
                        afterPos[i] = 0;
                    } else {
                        beforePos[i] = afterPos[i];
                        beforeLen[i] = afterLen[i];
                        beforeRecord[i] = afterRecord[i];
                    }
                }
            }
        }

        if (type == TRANSACTION_DELETE || type == TRANSACTION_UPDATE) {
            if (stream == STREAM_JSON) {
                outputBuffer->appendChr(",\"before\":{");
            }

            if (stream == STREAM_DBZ_JSON) {
                outputBuffer->appendChr("{");
            }

            prevValue = false;

            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                //value present before
                if (beforePos[i] > 0 && beforeLen[i] > 0) {
                    if (prevValue)
                        outputBuffer->append(',');
                    else
                        prevValue = true;

                    outputBuffer->appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                            object->columns[i]->charsetId, beforePos[i], beforeLen[i]);

                } else
                if ((type == TRANSACTION_DELETE && (showColumns >= 1 || object->columns[i]->numPk > 0)) ||
                    (type == TRANSACTION_UPDATE && (afterPos[i] > 0 || beforePos[i] > 0))) {
                    if (prevValue)
                        outputBuffer->append(',');
                    else
                        prevValue = true;

                    outputBuffer->appendNull(object->columns[i]->columnName);
                }
            }

            if (stream == STREAM_JSON) {
                outputBuffer->append('}');
            }

            if (stream == STREAM_DBZ_JSON) {
                outputBuffer->append('}');
            }
        }

        if (type == TRANSACTION_INSERT || type == TRANSACTION_UPDATE) {
            if (stream == STREAM_JSON) {
                outputBuffer->appendChr(",\"after\":{");
            }

            if (stream == STREAM_DBZ_JSON) {
                outputBuffer->appendChr(",\"after\":{");
            }

            prevValue = false;

            for (uint64_t i = 0; i < object->maxSegCol; ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                if (afterPos[i] > 0 && afterLen[i] > 0) {
                    if (prevValue)
                        outputBuffer->append(',');
                    else
                        prevValue = true;

                    outputBuffer->appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo,
                            object->columns[i]->charsetId, afterPos[i], afterLen[i]);
                } else
                if ((type == TRANSACTION_INSERT && (showColumns >= 1 || object->columns[i]->numPk > 0)) ||
                    (type == TRANSACTION_UPDATE && (afterPos[i] > 0 || beforePos[i] > 0))) {
                    if (prevValue)
                        outputBuffer->append(',');
                    else
                        prevValue = true;

                    outputBuffer->appendNull(object->columns[i]->columnName);
                }
            }

            if (stream == STREAM_JSON) {
                outputBuffer->append('}');
            }
            if (stream == STREAM_DBZ_JSON) {
                outputBuffer->append('}');
            }
        }

        if (stream == STREAM_JSON) {
            outputBuffer->append('}');
        }

        if (stream == STREAM_DBZ_JSON) {
            char op = 'u';
            if (type == TRANSACTION_INSERT) op = 'c';
            else if (type == TRANSACTION_DELETE) op = 'd';

            outputBuffer
                    ->appendDbzTail(object, lastTime.toTime() * 1000, lastScn, op, redoLogRecord1->xid)
                    ->commitMessage();
        }
    }

    //0x18010000
    void KafkaWriter::parseDDL(RedoLogRecord *redoLogRecord1) {
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

        if (stream == STREAM_JSON) {
            if (test >= 2)
                outputBuffer->append('\n');
            outputBuffer->append('{');
            if (test >= 2)
                outputBuffer
                        ->appendScn(lastScn)
                        ->append(',');
            outputBuffer
                    ->appendTable(object->owner, object->objectName)
                    ->appendChr(",\"type\":")
                    ->appendDec(type)
                    ->appendChr(",\"seq\":")
                    ->appendDec(seq)
                    ->append(',');

            if (type == 85)
                outputBuffer->appendOperation("truncate");
            else if (type == 12)
                outputBuffer->appendOperation("drop");
            else if (type == 15)
                outputBuffer->appendOperation("alter");
            else
                outputBuffer->appendOperation("?");

            outputBuffer
                    ->appendChr(",\"sql\":\"")
                    ->appendEscape(sqlText, sqlLength - 1)
                    ->appendChr("\"}");
        }
    }
}
