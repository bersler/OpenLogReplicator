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

#include <sys/stat.h>
#include <string>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <mutex>
#include <unistd.h>
#include <string.h>
#include <librdkafka/rdkafkacpp.h>
#include "types.h"
#include "KafkaWriter.h"
#include "OracleReader.h"
#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleReader.h"
#include "RedoLogRecord.h"

using namespace std;
using namespace RdKafka;

namespace OpenLogReplicator {

    KafkaWriter::KafkaWriter(const string alias, const string brokers, const string topic, OracleReader *oracleReader, uint64_t trace,
            uint64_t trace2, uint64_t stream, uint64_t sortColumns, uint64_t metadata, uint64_t singleDml, uint64_t nullColumns, uint64_t test,
            uint64_t timestampFormat) :
        Writer(alias, oracleReader, stream, sortColumns, metadata, singleDml, nullColumns, test, timestampFormat),
        conf(nullptr),
        tconf(nullptr),
        brokers(brokers),
        topic(topic),
        producer(nullptr),
        ktopic(nullptr),
        trace(trace),
        trace2(trace2),
        lastScn(0) {
    }

    KafkaWriter::~KafkaWriter() {
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
        cout << "- Kafka Writer for: " << brokers << " topic: " << topic << endl;
        uint64_t length = 0;

        length = 0;
        while (true) {
            if (length == 0) {
                unique_lock<mutex> lck(commandBuffer->mtx);
                while (commandBuffer->posStart == commandBuffer->posEnd) {
                    if (this->shutdown)
                        break;
                    commandBuffer->readersCond.wait(lck);
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

        if ((trace2 & TRACE2_OUTPUT_BUFFER) != 0)
            cerr << "Kafka writer buffer at shutdown: " << dec << commandBuffer->posStart << " - " << commandBuffer->posEnd << " (" << length << ")" << endl;
        return 0;
    }

    uint64_t KafkaWriter::initialize() {
        string errstr;
        Conf *conf = Conf::create(Conf::CONF_GLOBAL);
        Conf *tconf = Conf::create(Conf::CONF_TOPIC);
        conf->set("metadata.broker.list", brokers, errstr);
        conf->set("client.id", "OpenLogReplicator", errstr);

        if (test == 0) {
            producer = Producer::create(conf, errstr);
            if (producer == nullptr) {
                std::cerr << "ERROR: creating Kafka producer: " << errstr << endl;
                return 0;
            }

            ktopic = Topic::create(producer, topic, tconf, errstr);
            if (ktopic == nullptr) {
                std::cerr << "ERROR: Failed to create Kafka topic: " << errstr << endl;
                return 0;
            }
        }

        return 1;
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
                    ->append(",dml:[");
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
                commandBuffer->append("]}");
            commandBuffer->commitTran();
        }
    }

    //0x05010B0B
    void KafkaWriter::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0,  fieldPos = redoLogRecord2->fieldPos, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength;

        for (uint64_t i = 1; i < 4; ++i) {
            fieldLength = oracleReader->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + i * 2);
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
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
                if (oracleReader->version < 0x12200)
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
                        ->appendTable(redoLogRecord2->object->owner, redoLogRecord2->object->objectName)
                        ->append(',')
                        ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->bdba,
                                oracleReader->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2))
                        ->append(",\"after\":{");
            }

            for (uint64_t i = 0; i < redoLogRecord2->object->columns.size(); ++i) {
                bool isNull = false;

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer
                            ->beginTran()
                            ->appendDbzHead(redoLogRecord2->object)
                            ->append("\"before\":null,\"after\":{");
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
                        fieldLength = oracleReader->read16(redoLogRecord2->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (isNull) {
                    if (nullColumns >= 1) {
                        if (prevValue)
                            commandBuffer->append(',');
                        else
                            prevValue = true;

                        commandBuffer->appendNull(redoLogRecord2->object->columns[i]->columnName);
                    }
                } else {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendValue(redoLogRecord2->object->columns[i]->columnName,
                            redoLogRecord2, redoLogRecord2->object->columns[i]->typeNo, fieldPos + pos, fieldLength);

                    pos += fieldLength;
                }

                if (stream == STREAM_DBZ_JSON) {

                    commandBuffer
                            ->append('}')
                            ->appendDbzTail(redoLogRecord2->object, lastTime.toTime() * 1000, lastScn, 'c', redoLogRecord1->xid)
                            ->commitTran();
                }
            }

            if (stream == STREAM_JSON) {
                commandBuffer->append("}}");
            }

            fieldPosStart += oracleReader->read16(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta + r * 2);
        }
    }

    //0x05010B0C
    void KafkaWriter::parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint64_t pos = 0, fieldPos = redoLogRecord1->fieldPos, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength;

        for (uint64_t i = 1; i < 6; ++i) {
            fieldLength = oracleReader->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + i * 2);
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
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
                if (oracleReader->version < 0x12200)
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
                        ->appendTable(redoLogRecord1->object->owner, redoLogRecord1->object->objectName)
                        ->append(',')
                        ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->bdba,
                                oracleReader->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2))
                        ->append(",\"before\":{");
            }

            for (uint64_t i = 0; i < redoLogRecord1->object->columns.size(); ++i) {
                bool isNull = false;

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer
                            ->beginTran()
                            ->appendDbzHead(redoLogRecord2->object)
                            ->append("\"before\":{");
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
                        fieldLength = oracleReader->read16(redoLogRecord1->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                if (isNull) {
                    if (nullColumns >= 1) {
                        if (prevValue)
                            commandBuffer->append(',');
                        else
                            prevValue = true;

                        commandBuffer->appendNull(redoLogRecord1->object->columns[i]->columnName);
                    }
                } else {
                    if (prevValue)
                        commandBuffer->append(',');
                    else
                        prevValue = true;

                    commandBuffer->appendValue(redoLogRecord1->object->columns[i]->columnName,
                            redoLogRecord1, redoLogRecord1->object->columns[i]->typeNo, fieldPos + pos, fieldLength);

                    pos += fieldLength;
                }

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer
                        ->append("},\"after\":null,")
                        ->appendDbzTail(redoLogRecord2->object, lastTime.toTime() * 1000, lastScn, 'd', redoLogRecord1->xid)
                        ->commitTran();
                }
            }

            if (stream == STREAM_JSON) {
                commandBuffer->append("}}");
            }

            fieldPosStart += oracleReader->read16(redoLogRecord1->data + redoLogRecord1->rowLenghsDelta + r * 2);
        }
    }

    void KafkaWriter::parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type) {
        typedba bdba;
        typeslot slot;
        RedoLogRecord *redoLogRecord;

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

            redoLogRecord = redoLogRecord2;
            while (redoLogRecord != nullptr) {
                if ((redoLogRecord->fb & FB_F) != 0)
                    break;
                redoLogRecord = redoLogRecord->next;
            }

            if (redoLogRecord == nullptr) {
                if (oracleReader->trace >= TRACE_WARN)
                    cerr << "WARNING: could not find correct rowid for INSERT" << endl;
                bdba = 0;
                slot = 0;
            } else {
                bdba = redoLogRecord->bdba;
                slot = redoLogRecord->slot;
            }

        } else if (type == TRANSACTION_DELETE) {
            if (stream == STREAM_JSON) {
                commandBuffer->appendOperation("delete");
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
                commandBuffer->appendOperation("update");
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
            commandBuffer
                    ->append(',')
                    ->appendTable(redoLogRecord2->object->owner, redoLogRecord2->object->objectName)
                    ->append(',')
                    ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, bdba, slot);
        }

        if (stream == STREAM_DBZ_JSON) {
            commandBuffer
                    ->beginTran()
                    ->appendDbzHead(redoLogRecord2->object)
                    ->append("\"before\":");
        }

        uint64_t fieldPos, colNum, colShift, cc, headerSize;
        uint16_t fieldLength;
        uint8_t *nulls, bits, *colNums;
        bool prevValue = false;
        uint64_t *afterPos = nullptr, *beforePos = nullptr;
        uint16_t *afterLen = nullptr, *beforeLen = nullptr;
        uint8_t *colSupp = nullptr;
        RedoLogRecord **beforeRecord = nullptr, **afterRecord = nullptr;
        if (type == TRANSACTION_UPDATE && sortColumns > 0) {
            afterPos = new uint64_t[redoLogRecord1->object->totalCols * sizeof(uint64_t)];
            memset(afterPos, 0, redoLogRecord1->object->totalCols * sizeof(uint64_t));
            beforePos = new uint64_t[redoLogRecord1->object->totalCols * sizeof(uint64_t)];
            memset(beforePos, 0, redoLogRecord1->object->totalCols * sizeof(uint64_t));
            afterLen = new uint16_t[redoLogRecord1->object->totalCols * sizeof(uint16_t)];
            beforeLen = new uint16_t[redoLogRecord1->object->totalCols * sizeof(uint16_t)];
            colSupp = new uint8_t[redoLogRecord1->object->totalCols * sizeof(uint8_t)];
            memset(colSupp, 0, redoLogRecord1->object->totalCols * sizeof(uint8_t));
            beforeRecord = new RedoLogRecord*[redoLogRecord1->object->totalCols * sizeof(RedoLogRecord *)];
            afterRecord = new RedoLogRecord*[redoLogRecord1->object->totalCols * sizeof(RedoLogRecord *)];
        }

        //data in UNDO
        if (type == TRANSACTION_DELETE || type == TRANSACTION_UPDATE) {
            if (stream == STREAM_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append(",\"before\":{");
            }

            if (stream == STREAM_DBZ_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append('{');
            }

            redoLogRecord = redoLogRecord1;
            prevValue = false;
            colNums = nullptr;

            while (redoLogRecord != nullptr) {
                if (redoLogRecord->opCode == 0x0501) {
                    fieldPos = redoLogRecord->fieldPos;
                    nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
                    bits = 1;
                    cc = redoLogRecord->cc;
                    if (redoLogRecord->colNumsDelta > 0) {
                        colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
                        headerSize = 5;
                        colShift = redoLogRecord->suppLogBefore - 1 - oracleReader->read16(colNums);
                    } else {
                        colNums = nullptr;
                        headerSize = 4;
                        colShift = redoLogRecord->suppLogBefore - 1;
                    }

                    for (uint64_t i = 1; i <= headerSize; ++i) {
                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                    for (uint64_t i = 0; i < cc; ++i) {
                        if (i + headerSize + 1 > redoLogRecord->fieldCnt) {
                            cerr << "ERROR: reached out of columns" << endl;
                            break;
                        }
                        if (colNums != nullptr) {
                            colNum = oracleReader->read16(colNums) + colShift;
                            colNums += 2;
                        } else
                            colNum = i + colShift;

                        if (colNum > redoLogRecord->object->columns.size()) {
                            cerr << "ERROR: too big column id: " << dec << colNum << endl;
                            break;
                        }

                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (i + headerSize + 1) * 2);
                        if (((*nulls & bits) != 0 || fieldLength == 0) && type == TRANSACTION_DELETE) {
                            //null
                        } else {
                            if (type == TRANSACTION_UPDATE && sortColumns > 0) {
                                if (fieldLength != 0) {
                                    beforePos[colNum] = fieldPos;
                                    beforeLen[colNum] = fieldLength;
                                    beforeRecord[colNum] = redoLogRecord;
                                }
                            } else {
                                if ((*nulls & bits) == 0 && fieldLength > 0) {
                                    if (prevValue) {
                                        commandBuffer->append(',');
                                    } else
                                        prevValue = true;

                                    commandBuffer->appendValue(redoLogRecord->object->columns[colNum]->columnName,
                                            redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, fieldLength);
                                } else {
                                    if (nullColumns >= 1) {
                                        if (prevValue)
                                            commandBuffer->append(',');
                                        else
                                            prevValue = true;

                                        commandBuffer->appendNull(redoLogRecord->object->columns[colNum]->columnName);
                                    }
                                }
                            }
                        }

                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                    if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 1) * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                        ++headerSize;
                    }

                    //supplemental columns
                    if (cc + headerSize + 1 <= redoLogRecord->fieldCnt) {
                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 1) * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;

                        if (redoLogRecord->suppLogCC > 0 && redoLogRecord->cc + headerSize + 4 <= redoLogRecord->fieldCnt) {
                            fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 2) * 2);
                            colNums = redoLogRecord->data + fieldPos;
                            fieldPos += (fieldLength + 3) & 0xFFFC;

                            fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 3) * 2);
                            uint8_t* colSizes = redoLogRecord->data + fieldPos;
                            fieldPos += (fieldLength + 3) & 0xFFFC;

                            for (uint64_t i = 0; i < redoLogRecord->suppLogCC; ++i) {
                                fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 4 + i) * 2);
                                colNum = oracleReader->read16(colNums) + colShift - 1;
                                colNums += 2;
                                uint16_t colLength = oracleReader->read16(colSizes);

                                if (type == TRANSACTION_UPDATE && sortColumns > 0) {
                                    colSupp[colNum] = 1;
                                    beforePos[colNum] = fieldPos;
                                    afterPos[colNum] = fieldPos;
                                    beforeRecord[colNum] = redoLogRecord;
                                    afterRecord[colNum] = redoLogRecord;
                                    if (colLength != 0xFFFF) {
                                        beforeLen[colNum] = colLength;
                                        afterLen[colNum] = colLength;
                                    } else {
                                        beforeLen[colNum] = 0;
                                        afterLen[colNum] = 0;
                                    }
                                } else {
                                    if (colLength == 0xFFFF) {
                                        if (nullColumns >= 1) {
                                            if (prevValue)
                                                commandBuffer->append(',');
                                            else
                                                prevValue = true;

                                            commandBuffer->appendNull(redoLogRecord->object->columns[colNum]->columnName);
                                        }
                                    } else {
                                        if (prevValue) {
                                            commandBuffer->append(',');
                                        } else
                                            prevValue = true;

                                        commandBuffer->appendValue(redoLogRecord->object->columns[colNum]->columnName,
                                                redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, colLength);
                                    }
                                }

                                colSizes += 2;
                                fieldPos += (fieldLength + 3) & 0xFFFC;
                            }
                        }
                    }
                }

                redoLogRecord = redoLogRecord->next;
            }

            if (stream == STREAM_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append('}');
            }

            if (stream == STREAM_DBZ_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append('}');
            }
        } else {
            if (stream == STREAM_DBZ_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append("null");
            }
        }

        if (stream == STREAM_DBZ_JSON) {
            if (type != TRANSACTION_UPDATE || sortColumns == 0)
                commandBuffer->append(",\"after\":");
        }

        //data in REDO
        if (type == TRANSACTION_INSERT || type == TRANSACTION_UPDATE) {
            if (stream == STREAM_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append(",\"after\":{");
            }

            if (stream == STREAM_DBZ_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append('{');
            }

            redoLogRecord = redoLogRecord2;
            prevValue = false;

            while (redoLogRecord != nullptr) {
                if (redoLogRecord->opCode == 0x0B02) {
                    fieldPos = redoLogRecord->fieldPos;
                    nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
                    bits = 1;
                    cc = redoLogRecord->cc;
                    colNum = redoLogRecord->suppLogAfter - 1;

                    for (uint64_t i = 1; i <= 2; ++i) {
                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                    for (uint64_t i = 0; i < cc; ++i) {
                        if (i + 3 > redoLogRecord->fieldCnt) {
                            cerr << "ERROR: reached out of columns" << endl;
                            break;
                        }
                        if (colNum > redoLogRecord->object->columns.size()) {
                            cerr << "ERROR: too big column id: " << dec << colNum << endl;
                            break;
                        }

                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (i + 3) * 2);
                        if ((*nulls & bits) != 0 || fieldLength == 0) {
                            if (nullColumns >= 1) {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendNull(redoLogRecord->object->columns[colNum]->columnName);
                            }
                        } else {
                            if (type == TRANSACTION_UPDATE && sortColumns > 0) {
                                afterPos[colNum] = fieldPos;
                                afterLen[colNum] = fieldLength;
                                afterRecord[colNum] = redoLogRecord;
                            } else {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendValue(redoLogRecord->object->columns[colNum]->columnName,
                                        redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, fieldLength);
                            }
                        }

                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                        ++colNum;
                    }

                } else if (redoLogRecord->opCode == 0x0B05 || redoLogRecord->opCode == 0x0B06) {
                    fieldPos = redoLogRecord->fieldPos;
                    nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
                    if (redoLogRecord->colNumsDelta > 0) {
                        colNums = redoLogRecord->data + redoLogRecord->colNumsDelta;
                        colShift = redoLogRecord->suppLogAfter - 1 - oracleReader->read16(colNums);
                        headerSize = 3;
                    } else {
                        colNums = nullptr;
                        colShift = redoLogRecord->suppLogAfter - 1;
                        headerSize = 2;
                    }
                    bits = 1;

                    for (uint64_t i = 1; i <= headerSize; ++i) {
                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                    for (uint64_t i = 0; i < redoLogRecord->cc && i + headerSize + 1 <= redoLogRecord->fieldCnt; ++i) {
                        fieldLength = oracleReader->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (i + headerSize + 1) * 2);
                        if (colNums != nullptr) {
                            colNum = oracleReader->read16(colNums) + colShift;
                            colNums += 2;
                        } else
                            colNum = i + colShift;

                        if (type == TRANSACTION_UPDATE && sortColumns > 0) {
                            if (fieldLength != 0) {
                                afterPos[colNum] = fieldPos;
                                afterLen[colNum] = fieldLength;
                                afterRecord[colNum] = redoLogRecord;
                            }
                        } else {
                            if ((*nulls & bits) != 0 || fieldLength == 0) {
                                if (nullColumns >= 1) {
                                    if (prevValue)
                                        commandBuffer->append(',');
                                    else
                                        prevValue = true;

                                    commandBuffer->appendNull(redoLogRecord->object->columns[colNum]->columnName);
                                }
                            } else {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendValue(redoLogRecord->object->columns[colNum]->columnName,
                                        redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, fieldLength);
                            }
                        }

                        bits <<= 1;
                        if (bits == 0) {
                            bits = 1;
                            ++nulls;
                        }
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                }

                redoLogRecord = redoLogRecord->next;
            }

            if (stream == STREAM_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append('}');
            }

            if (stream == STREAM_DBZ_JSON) {
                if (type != TRANSACTION_UPDATE || sortColumns == 0)
                    commandBuffer->append('}');
            }


            if (type == TRANSACTION_UPDATE && sortColumns > 0) {
                if (sortColumns >= 2) {
                    for (uint64_t i = 0; i < redoLogRecord1->object->totalCols; ++i) {
                        if (redoLogRecord1->object->columns[i]->numPk == 0 && colSupp[i] == 0) {
                            if (beforePos[i] > 0 && afterPos[i] > 0 && beforeLen[i] == afterLen[i]) {
                                if (beforeLen[i] == 0 || memcmp(beforeRecord[i]->data + beforePos[i], afterRecord[i]->data + afterPos[i], beforeLen[i]) == 0) {
                                    beforePos[i] = 0;
                                    afterPos[i] = 0;
                                    beforeLen[i] = 0;
                                    afterLen[i] = 0;
                                }
                            }
                        }
                    }
                }

                if (stream == STREAM_JSON) {
                    commandBuffer->append(",\"before\":{");
                }

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer->append("{");
                }

                for (uint64_t i = 0; i < redoLogRecord1->object->totalCols; ++i) {
                    if (beforePos[i] > 0 || afterPos[i] > 0) {
                        if (beforePos[i] == 0 || beforeLen[i] == 0) {
                            if (nullColumns >= 1 || colSupp[i] > 0 || afterPos[i] > 0) {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendNull(redoLogRecord1->object->columns[i]->columnName);
                            }
                        } else {
                            if (prevValue)
                                commandBuffer->append(',');
                            else
                                prevValue = true;

                            commandBuffer->appendValue(redoLogRecord1->object->columns[i]->columnName,
                                    beforeRecord[i], redoLogRecord1->object->columns[i]->typeNo, beforePos[i], beforeLen[i]);
                        }
                    }
                }

                if (stream == STREAM_JSON) {
                    commandBuffer->append("},\"after\":{");
                }

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer->append("},\"after\":{");
                }

                prevValue = false;

                for (uint64_t i = 0; i < redoLogRecord1->object->totalCols; ++i) {
                    if (afterPos[i] > 0 || beforePos[i] > 0) {
                        if (afterPos[i] == 0 && (redoLogRecord1->object->columns[i]->numPk > 0 || colSupp[i] > 0)) {
                            if (beforePos[i] == 0 || beforeLen[i] == 0) {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendNull(redoLogRecord1->object->columns[i]->columnName);
                            } else {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendValue(redoLogRecord1->object->columns[i]->columnName,
                                        beforeRecord[i], redoLogRecord1->object->columns[i]->typeNo, beforePos[i], beforeLen[i]);
                            }
                        } else {
                            if (afterPos[i] == 0 || afterLen[i] == 0) {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendNull(redoLogRecord1->object->columns[i]->columnName);
                            } else {
                                if (prevValue)
                                    commandBuffer->append(',');
                                else
                                    prevValue = true;

                                commandBuffer->appendValue(redoLogRecord1->object->columns[i]->columnName,
                                        afterRecord[i], redoLogRecord1->object->columns[i]->typeNo, afterPos[i], afterLen[i]);
                            }
                        }
                    }
                }
                if (stream == STREAM_JSON) {
                    commandBuffer->append('}');
                }

                if (stream == STREAM_DBZ_JSON) {
                    commandBuffer->append('}');
                }

                delete[] afterRecord;
                delete[] beforeRecord;
                delete[] colSupp;
                delete[] afterLen;
                delete[] beforeLen;
                delete[] afterPos;
                delete[] beforePos;
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
                    ->appendDbzTail(redoLogRecord2->object, lastTime.toTime() * 1000, lastScn, op, redoLogRecord1->xid)
                    ->commitTran();
        }
    }

    //0x18010000
    void KafkaWriter::parseDDL(RedoLogRecord *redoLogRecord1) {
        uint64_t fieldPos = redoLogRecord1->fieldPos;
        uint16_t seq = 0, cnt = 0, type = 0;

        if (oracleReader->trace >= TRACE_DETAIL)
            cerr << "INFO: DDL";

        uint16_t fieldLength;
        for (uint64_t i = 1; i <= redoLogRecord1->fieldCnt; ++i) {
            fieldLength = oracleReader->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + i * 2);
            if (i == 1) {
                type = oracleReader->read16(redoLogRecord1->data + fieldPos + 12);
                seq = oracleReader->read16(redoLogRecord1->data + fieldPos + 18);
                cnt = oracleReader->read16(redoLogRecord1->data + fieldPos + 20);
                if (oracleReader->trace >= TRACE_DETAIL) {
                    cerr << " SEQ: " << dec << seq << "/" << dec << cnt;
                    cerr << " TYPE: " << dec << type;
                }
            } else if (i == 8) {
                //DDL text
                if (oracleReader->trace >= TRACE_DETAIL) {
                    cerr << " DDL[" << dec << fieldLength << "]: '";
                    for (uint64_t j = 0; j < (uint64_t)(fieldLength - 1); ++j) {
                        cerr << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cerr << "'";
                }
            } else if (i == 9) {
                //owner
                if (oracleReader->trace >= TRACE_DETAIL) {
                    cerr << " OWNER[" << dec << fieldLength << "]: '";
                    for (uint64_t j = 0; j < fieldLength; ++j) {
                        cerr << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cerr << "'";
                }
            } else if (i == 10) {
                //table
                if (oracleReader->trace >= TRACE_DETAIL) {
                    cerr << " TABLE[" << fieldLength << "]: '";
                    for (uint64_t j = 0; j < fieldLength; ++j) {
                        cerr << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cerr << "'";
                }
            } else if (i == 12) {
                redoLogRecord1->objn = oracleReader->read32(redoLogRecord1->data + fieldPos + 0);
                if (oracleReader->trace >= TRACE_DETAIL) {
                    cerr << " OBJN: " << dec << redoLogRecord1->objn;
                }
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
        if (oracleReader->trace >= TRACE_DETAIL)
            cerr << endl;

        if (type == 85) {
            if (stream == STREAM_JSON) {
                if (test >= 2)
                    commandBuffer->append('\n');
                commandBuffer
                        ->append('{')
                        ->appendScn(lastScn)
                        ->append(',')
                        ->appendOperation("truncate")
                        ->append(',')
                        ->appendTable(redoLogRecord1->object->owner, redoLogRecord1->object->objectName)
                        ->append('}');
            }
        } else if (type == 12) {
            if (stream == STREAM_JSON) {
                if (test >= 2)
                    commandBuffer->append('\n');
                commandBuffer
                        ->append('{')
                        ->appendScn(lastScn)
                        ->append(',')
                        ->appendOperation("drop")
                        ->append(',')
                        ->appendTable(redoLogRecord1->object->owner, redoLogRecord1->object->objectName)
                        ->append('}');
            }
        } else if (type == 15) {
            if (stream == STREAM_JSON) {
                if (test >= 2)
                    commandBuffer->append('\n');
                commandBuffer
                        ->append('{')
                        ->appendScn(lastScn)
                        ->append(',')
                        ->appendOperation("alter")
                        ->append(',')
                        ->appendTable(redoLogRecord1->object->owner, redoLogRecord1->object->objectName)
                        ->append('}');
            }
       }
    }
}
