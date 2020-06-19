/* Thread writing to Kafka stream
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

#include <cstdio>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <unistd.h>
#include <string.h>
#include <librdkafka/rdkafkacpp.h>
#include <sys/stat.h>

#include "CommandBuffer.h"
#include "ConfigurationException.h"
#include "KafkaWriter.h"
#include "MemoryException.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"

using namespace std;
using namespace RdKafka;

void stopMain();

namespace OpenLogReplicator {

    KafkaWriter::KafkaWriter(string alias, string brokers, string topic, OracleAnalyser *oracleAnalyser, uint64_t trace,
            uint64_t trace2, uint64_t stream, uint64_t metadata, uint64_t singleDml, uint64_t showColumns, uint64_t test,
            uint64_t timestampFormat) :
        Writer(alias, oracleAnalyser, stream, metadata, singleDml, showColumns, test, timestampFormat),
        conf(nullptr),
        tconf(nullptr),
        brokers(brokers),
        topic(topic),
        producer(nullptr),
        ktopic(nullptr),
        trace(trace),
        trace2(trace2),
        lastScn(0),
        afterPos(nullptr),
        beforePos(nullptr),
        afterLen(nullptr),
        beforeLen(nullptr),
        colIsSupp(nullptr),
        beforeRecord(nullptr),
        afterRecord(nullptr) {
    }

    KafkaWriter::~KafkaWriter() {
        if (afterRecord != nullptr) {
            delete[] afterRecord;
            afterRecord = nullptr;
        }
        if (beforeRecord != nullptr) {
            delete[] beforeRecord;
            beforeRecord = nullptr;
        }
        if (colIsSupp != nullptr) {
            delete[] colIsSupp;
            colIsSupp = nullptr;
        }
        if (afterLen != nullptr) {
            delete[] afterLen;
            afterLen = nullptr;
        }
        if (beforeLen != nullptr) {
            delete[] beforeLen;
            beforeLen = nullptr;
        }
        if (afterPos != nullptr) {
            delete[] afterPos;
            afterPos = nullptr;
        }
        if (beforePos != nullptr) {
            delete[] beforePos;
            beforePos = nullptr;
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

    void *KafkaWriter::run() {
        cout << "Starting thread: Kafka writer for : " << brokers << " topic: " << topic << endl;
        uint64_t length = 0;

        length = 0;
        try {
            while (!shutdown) {
                if (length == 0) {
                    unique_lock<mutex> lck(commandBuffer->mtx);
                    while (commandBuffer->posStart == commandBuffer->posEnd) {
                        if (shutdown)
                            break;
                        commandBuffer->analysersCond.wait(lck);
                    }

                    if (commandBuffer->posStart == commandBuffer->posSize && commandBuffer->posSize > 0) {
                        commandBuffer->posStart = 0;
                        commandBuffer->posSize = 0;
                    }
                    if (commandBuffer->posStart == commandBuffer->posEnd)
                        length = 0;
                    else
                        length = *((uint64_t*)(commandBuffer->intraThreadBuffer + commandBuffer->posStart));
                }
                if ((trace2 & TRACE2_OUTPUT_BUFFER) != 0)
                    cerr << "Kafka writer buffer: " << dec << commandBuffer->posStart << " - " << commandBuffer->posEnd << " (" << length << ")" << endl;

                if (length > 0) {
                    if (test >= 1) {
                        for (uint64_t i = 0; i < length - 8; ++i)
                            cout << commandBuffer->intraThreadBuffer[commandBuffer->posStart + 8 + i];
                        cout << endl;
                    } else {
                        if (producer->produce(
                                ktopic, Topic::PARTITION_UA, Producer::RK_MSG_COPY, commandBuffer->intraThreadBuffer + commandBuffer->posStart + 8,
                                length - 8, nullptr, nullptr)) {
                            cerr << "ERROR: writing to topic " << endl;
                        }
                    }

                    {
                        unique_lock<mutex> lck(commandBuffer->mtx);
                        commandBuffer->posStart += (length + 7) & 0xFFFFFFFFFFFFFFF8;

                        if (commandBuffer->posStart == commandBuffer->posSize && commandBuffer->posSize > 0) {
                            commandBuffer->posStart = 0;
                            commandBuffer->posSize = 0;
                        }

                        commandBuffer->writerCond.notify_all();
                    }
                    length = 0;
                } else
                    if (shutdown)
                        break;
            }
        } catch(ConfigurationException &ex) {
            cerr << "ERROR: configuration error: " << ex.msg << endl;
            stopMain();
        } catch(RuntimeException &ex) {
            cerr << "ERROR: runtime: " << ex.msg << endl;
            stopMain();
        } catch (MemoryException &e) {
            cerr << "ERROR: memory allocation error for " << e.msg << " for " << e.bytes << " bytes" << endl;
            stopMain();
        }


        if ((trace2 & TRACE2_OUTPUT_BUFFER) != 0)
            cerr << "Kafka writer buffer at shutdown: " << dec << commandBuffer->posStart << " - " << commandBuffer->posEnd << " (" << length << ")" << endl;
        return 0;
    }

    void KafkaWriter::initialize() {
        string errstr;
        Conf *conf = Conf::create(Conf::CONF_GLOBAL);
        Conf *tconf = Conf::create(Conf::CONF_TOPIC);
        conf->set("metadata.broker.list", brokers, errstr);
        conf->set("client.id", "OpenLogReplicator", errstr);

        if (test == 0) {
            producer = Producer::create(conf, errstr);
            if (producer == nullptr) {
                std::cerr << "ERROR: Kafka message: " << errstr << endl;
                throw ConfigurationException("error creating topic");
            }

            ktopic = Topic::create(producer, topic, tconf, errstr);
            if (ktopic == nullptr) {
                std::cerr << "ERROR: Kafka message: " << errstr << endl;
                throw ConfigurationException("error creating topic");
            }
        }
    }

    void KafkaWriter::beginTran(typescn scn, typetime time, typexid xid) {
        if (stream == STREAM_JSON) {
            commandBuffer
                    ->beginTran()
                    ->append('{')
                    ->appendScn(scn)
                    ->append(',')
                    ->appendMs("timestamp", time.toTime() * 1000)
                    ->append(',')
                    ->appendXid(xid)
                    ->appendChr(",dml:[");
        }

        lastTime = time;
        lastScn = scn;
    }

    void KafkaWriter::next() {
        if (stream == STREAM_JSON) {
            if (test <= 1)
                commandBuffer->append(',');
        }
    }

    void KafkaWriter::commitTran() {
        if (stream == STREAM_JSON) {
            if (test <= 1)
                commandBuffer->appendChr("]}");
            commandBuffer->commitTran();
        }
    }

    //0x05010B0B
    void KafkaWriter::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = 0, fieldNum = 0, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength = 0;
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
                    commandBuffer->append('\n');
                commandBuffer
                        ->append('{')
                        ->appendScn(lastScn)
                        ->append(',')
                        ->appendOperation("insert")
                        ->append(',')
                        ->appendTable(object->owner, object->objectName)
                        ->append(',')
                        ->appendRowid(object->objn, object->objd, redoLogRecord2->bdba,
                                oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2))
                        ->appendChr(",\"after\":{");
            }

            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                bool isNull = false;

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer
                            ->beginTran()
                            ->appendDbzHead(object)
                            ->appendChr("\"before\":null,\"after\":{");
                }

                if (i >= jcc)
                    isNull = true;
                else {
                    fieldLength = redoLogRecord2->data[fieldPos + pos];
                    ++pos;
                    if (fieldLength == 0xFF) {
                        isNull = true;
                    } else
                    if (fieldLength == 0xFE) {
                        fieldLength = oracleAnalyser->read16(redoLogRecord2->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (isNull) {
                    if (showColumns >= 1 || object->columns[i]->numPk > 0) {
                        if (prevValue)
                            commandBuffer->append(',');
                        else
                            prevValue = true;

                        commandBuffer->appendNull(object->columns[i]->columnName);
                    }
                } else {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendValue(object->columns[i]->columnName,
                            redoLogRecord2, object->columns[i]->typeNo, fieldPos + pos, fieldLength);

                    pos += fieldLength;
                }

                if (stream == STREAM_DBZ_JSON) {

                    commandBuffer
                            ->append('}')
                            ->appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'c', redoLogRecord1->xid)
                            ->commitTran();
                }
            }

            if (stream == STREAM_JSON) {
                commandBuffer->appendChr("}}");
            }

            fieldPosStart += oracleAnalyser->read16(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta + r * 2);
        }
    }

    //0x05010B0C
    void KafkaWriter::parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = 0, fieldNum = 0, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength = 0;
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
                    commandBuffer->append('\n');
                commandBuffer
                        ->append('{')
                        ->appendScn(lastScn)
                        ->append(',')
                        ->appendOperation("delete")
                        ->append(',')
                        ->appendTable(object->owner, object->objectName)
                        ->append(',')
                        ->appendRowid(object->objn, object->objd, redoLogRecord2->bdba,
                                oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2))
                        ->appendChr(",\"before\":{");
            }

            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                bool isNull = false;

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer
                            ->beginTran()
                            ->appendDbzHead(object)
                            ->appendChr("\"before\":{");
                }

                if (i >= jcc)
                    isNull = true;
                else {
                    fieldLength = redoLogRecord1->data[fieldPos + pos];
                    ++pos;
                    if (fieldLength == 0xFF) {
                        isNull = true;
                    } else
                    if (fieldLength == 0xFE) {
                        fieldLength = oracleAnalyser->read16(redoLogRecord1->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (isNull) {
                    if (showColumns >= 1 || object->columns[i]->numPk > 0) {
                        if (prevValue)
                            commandBuffer->append(',');
                        else
                            prevValue = true;

                        commandBuffer->appendNull(object->columns[i]->columnName);
                    }
                } else {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendValue(object->columns[i]->columnName,
                            redoLogRecord1, object->columns[i]->typeNo, fieldPos + pos, fieldLength);

                    pos += fieldLength;
                }

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer
                        ->appendChr("},\"after\":null,")
                        ->appendDbzTail(object, lastTime.toTime() * 1000, lastScn, 'd', redoLogRecord1->xid)
                        ->commitTran();
                }
            }

            if (stream == STREAM_JSON) {
                commandBuffer->appendChr("}}");
            }

            fieldPosStart += oracleAnalyser->read16(redoLogRecord1->data + redoLogRecord1->rowLenghsDelta + r * 2);
        }
    }

    void KafkaWriter::parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type) {
        typedba bdba;
        typeslot slot;
        RedoLogRecord *redoLogRecord1p, *redoLogRecord2p = nullptr;
        OracleObject *object = nullptr;

        if (stream == STREAM_JSON) {
            if (test >= 2)
                commandBuffer->append('\n');
            commandBuffer
                    ->append('{')
                    ->appendScn(lastScn)
                    ->append(',');
        }

        if (type == TRANSACTION_INSERT) {
            if (stream == STREAM_JSON) {
                commandBuffer->appendOperation("insert");
            }

            object = redoLogRecord2->object;
            redoLogRecord2p = redoLogRecord2;
            while (redoLogRecord2p != nullptr) {
                if ((redoLogRecord2p->fb & FB_F) != 0)
                    break;
                redoLogRecord2p = redoLogRecord2p->next;
            }

            if (redoLogRecord2p == nullptr) {
                if (oracleAnalyser->trace >= TRACE_WARN)
                    cerr << "WARNING: could not find correct rowid for INSERT" << endl;
                bdba = 0;
                slot = 0;
            } else {
                bdba = redoLogRecord2p->bdba;
                slot = redoLogRecord2p->slot;
            }

        } else if (type == TRANSACTION_DELETE) {
            if (stream == STREAM_JSON) {
                commandBuffer->appendOperation("delete");
            }

            object = redoLogRecord1->object;
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        } else {
            if (stream == STREAM_JSON) {
                commandBuffer->appendOperation("update");
            }

            object = redoLogRecord1->object;
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        }

        if (stream == STREAM_JSON) {
            commandBuffer
                    ->append(',')
                    ->appendTable(object->owner, object->objectName)
                    ->append(',')
                    ->appendRowid(object->objn, object->objd, bdba, slot);
        }

        if (stream == STREAM_DBZ_JSON) {
            commandBuffer
                    ->beginTran()
                    ->appendDbzHead(object)
                    ->appendChr("\"before\":");
        }

        uint64_t fieldPos, fieldNum, colNum, colShift, rowDeps;
        uint16_t fieldLength, colLength;
        uint8_t *nulls, bits, *colNums;
        bool prevValue = false;

        if (afterRecord != nullptr) {
            delete[] afterRecord;
            afterRecord = nullptr;
        }
        afterRecord = new RedoLogRecord*[object->totalCols];
        if (afterRecord == nullptr)
            throw MemoryException("KafkaWriter::parseDML.7", sizeof(RedoLogRecord*) * object->totalCols);

        if (beforeRecord != nullptr) {
            delete[] beforeRecord;
            beforeRecord = nullptr;
        }
        beforeRecord = new RedoLogRecord*[object->totalCols];
        if (beforeRecord == nullptr)
            throw MemoryException("KafkaWriter::parseDML.6", sizeof(RedoLogRecord*) * object->totalCols);

        if (colIsSupp != nullptr) {
            delete[] colIsSupp;
            colIsSupp = nullptr;
        }
        colIsSupp = new uint8_t[object->totalCols];
        if (colIsSupp == nullptr)
            throw MemoryException("KafkaWriter::parseDML.5", sizeof(uint64_t) * object->totalCols);
        memset(colIsSupp, 0, object->totalCols * sizeof(uint8_t));

        if (afterLen != nullptr) {
            delete[] afterLen;
            afterLen = nullptr;
        }
        afterLen = new uint16_t[object->totalCols];
        if (afterLen == nullptr)
            throw MemoryException("KafkaWriter::parseDML.3", sizeof(uint16_t) * object->totalCols);

        if (beforeLen != nullptr) {
            delete[] beforeLen;
            beforeLen = nullptr;
        }
        beforeLen = new uint16_t[object->totalCols];
        if (beforeLen == nullptr)
            throw MemoryException("KafkaWriter::parseDML.4", sizeof(uint16_t) * object->totalCols);

        if (afterPos != nullptr) {
            delete[] afterPos;
            afterPos = nullptr;
        }
        afterPos = new uint64_t[object->totalCols];
        if (afterPos == nullptr)
            throw MemoryException("KafkaWriter::parseDML.1", sizeof(uint64_t) * object->totalCols);
        memset(afterPos, 0, object->totalCols * sizeof(uint64_t));

        if (beforePos != nullptr) {
            delete[] beforePos;
            beforePos = nullptr;
        }
        beforePos = new uint64_t[object->totalCols];
        if (beforePos == nullptr)
            throw MemoryException("KafkaWriter::parseDML.2", sizeof(uint64_t) * object->totalCols);
        memset(beforePos, 0, object->totalCols * sizeof(uint64_t));

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
                        cerr << "ERROR: table: " << object->owner << "." << object->objectName << ": out of columns (Undo): " << dec << colNum << "/" << (uint64_t)redoLogRecord1p->cc << endl;
                        break;
                    }
                    if (colNums != nullptr) {
                        colNum = oracleAnalyser->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (colNum >= object->columns.size()) {
                        cerr << "WARNING: table: " << object->owner << "." << object->objectName << ": referring to unknown column id(" <<
                                dec << colNum << "), probably table was altered, ignoring extra column" << endl;
                        break;
                    }

                    if ((*nulls & bits) != 0)
                        colLength = 0;
                    else {
                        oracleAnalyser->skipEmptyFields(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                        oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                        colLength = fieldLength;
                    }

                    beforePos[colNum] = fieldPos;
                    beforeLen[colNum] = colLength;
                    beforeRecord[colNum] = redoLogRecord1p;

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
                        cerr << "ERROR: table: " << object->owner << "." << object->objectName << ": out of columns (Supp): " << dec << colNum << "/" << (uint64_t)redoLogRecord1p->suppLogCC << endl;
                        break;
                    }

                    oracleAnalyser->nextField(redoLogRecord1p, fieldNum, fieldPos, fieldLength);
                    colNum = oracleAnalyser->read16(colNums) - 1;

                    if (colNum >= object->columns.size()) {
                        cerr << "WARNING: table: " << object->owner << "." << object->objectName << ": referring to unknown column id, probably table was altered: " << dec << colNum << endl;
                        break;
                    }

                    colNums += 2;
                    colLength = oracleAnalyser->read16(colSizes);

                    colIsSupp[colNum] = 1;
                    if (colLength == 0xFFFF)
                        colLength = 0;

                    //insert, lock, update
                    if (redoLogRecord2p->opCode == 0x0B02 || redoLogRecord2p->opCode == 0x0B04 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B10) {
                        afterRecord[colNum] = redoLogRecord1p;
                        afterPos[colNum] = fieldPos;
                        afterLen[colNum] = colLength;
                    }
                    //delete, update, overwrite
                    if (redoLogRecord2p->opCode == 0x0B03 || redoLogRecord2p->opCode == 0x0B05 || redoLogRecord2p->opCode == 0x0B06 || redoLogRecord2p->opCode == 0x0B10) {
                        beforeRecord[colNum] = redoLogRecord1p;
                        beforePos[colNum] = fieldPos;
                        beforeLen[colNum] = colLength;
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
                        cerr << "ERROR: table: " << object->owner << "." << object->objectName << ": out of columns (Redo): " << dec << colNum << "/" << (uint64_t)redoLogRecord2p->cc << endl;
                        break;
                    }

                    oracleAnalyser->nextField(redoLogRecord2p, fieldNum, fieldPos, fieldLength);

                    if (colNums != nullptr) {
                        colNum = oracleAnalyser->read16(colNums) + colShift;
                        colNums += 2;
                    } else
                        colNum = i + colShift;

                    if (colNum >= object->columns.size()) {
                        cerr << "WARNING: table: " << object->owner << "." << object->objectName << ": referring to unknown column id, probably table was altered: " << dec << colNum << endl;
                        break;
                    }

                    if ((*nulls & bits) != 0)
                        colLength = 0;
                    else
                        colLength = fieldLength;

                    afterPos[colNum] = fieldPos;
                    afterLen[colNum] = colLength;
                    afterRecord[colNum] = redoLogRecord2p;

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
            cerr << "DML: tab: " << object->owner << "." << object->objectName << " type: " << type << endl;
            for (uint64_t i = 0; i < object->totalCols; ++i) {
                cerr << "DML: " << dec << i << ": ";
                if (beforePos[i] > 0)
                    cerr << " B(" << beforePos[i] << ", " << dec << beforeLen[i] << ")";
                if (afterPos[i] > 0)
                    cerr << " A(" << afterPos[i] << ", " << dec << afterLen[i] << ")";
                cerr << " pk: " << dec << object->columns[i]->numPk;
                cerr << " supp: " << dec << (uint64_t)colIsSupp[i];
                cerr << endl;
            }
        }


        if (type == TRANSACTION_UPDATE && showColumns <= 1) {
            for (uint64_t i = 0; i < object->totalCols; ++i) {

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
                commandBuffer->appendChr(",\"before\":{");
            }

            if (stream == STREAM_DBZ_JSON) {
                commandBuffer->appendChr("{");
            }

            prevValue = false;

            for (uint64_t i = 0; i < object->totalCols; ++i) {
                //value present before
                if (beforePos[i] > 0 && beforeLen[i] > 0) {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo, beforePos[i], beforeLen[i]);

                } else
                if ((type == TRANSACTION_DELETE && (showColumns >= 1 || object->columns[i]->numPk > 0)) ||
                    (type == TRANSACTION_UPDATE && (afterPos[i] > 0 || beforePos[i] > 0))) {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendNull(object->columns[i]->columnName);
                }
            }

            if (stream == STREAM_JSON) {
                commandBuffer->append('}');
            }

            if (stream == STREAM_DBZ_JSON) {
                commandBuffer->append('}');
            }
        }

        if (type == TRANSACTION_INSERT || type == TRANSACTION_UPDATE) {
            if (stream == STREAM_JSON) {
                commandBuffer->appendChr(",\"after\":{");
            }

            if (stream == STREAM_DBZ_JSON) {
                commandBuffer->appendChr(",\"after\":{");
            }

            prevValue = false;

            for (uint64_t i = 0; i < object->totalCols; ++i) {
                if (afterPos[i] > 0 && afterLen[i] > 0) {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo, afterPos[i], afterLen[i]);
                } else
                if ((type == TRANSACTION_INSERT && (showColumns >= 1 || object->columns[i]->numPk > 0)) ||
                    (type == TRANSACTION_UPDATE && (afterPos[i] > 0 || beforePos[i] > 0))) {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendNull(object->columns[i]->columnName);
                }
            }

            if (stream == STREAM_JSON) {
                commandBuffer->append('}');
            }
            if (stream == STREAM_DBZ_JSON) {
                commandBuffer->append('}');
            }
        }

        if (stream == STREAM_JSON) {
            commandBuffer->append('}');
        }

        if (stream == STREAM_DBZ_JSON) {
            char op = 'u';
            if (type == TRANSACTION_INSERT) op = 'c';
            else if (type == TRANSACTION_DELETE) op = 'd';

            commandBuffer
                    ->appendDbzTail(object, lastTime.toTime() * 1000, lastScn, op, redoLogRecord1->xid)
                    ->commitTran();
        }

        delete[] beforePos; beforePos = nullptr;
        delete[] afterPos; afterPos = nullptr;
        delete[] beforeLen; beforeLen = nullptr;
        delete[] afterLen; afterLen = nullptr;
        delete[] colIsSupp; colIsSupp = nullptr;
        delete[] beforeRecord; beforeRecord = nullptr;
        delete[] afterRecord; afterRecord = nullptr;
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
                commandBuffer->append('\n');

            commandBuffer
                    ->append('{')
                    ->appendScn(lastScn)
                    ->append(',')
                    ->appendTable(object->owner, object->objectName)
                    ->appendChr(",\"type\":")
                    ->appendDec(type)
                    ->appendChr(",\"seq\":")
                    ->appendDec(seq)
                    ->append(',');

            if (type == 85)
                commandBuffer->appendOperation("truncate");
            else if (type == 12)
                commandBuffer->appendOperation("drop");
            else if (type == 15)
                commandBuffer->appendOperation("alter");
            else
                commandBuffer->appendOperation("?");

            commandBuffer
                    ->appendChr(",\"sql\":\"")
                    ->appendEscape(sqlText, sqlLength - 1)
                    ->appendChr("\"}");
        }
    }
}
