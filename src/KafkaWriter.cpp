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
#include "OracleEnvironment.h"
#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;
using namespace RdKafka;

namespace OpenLogReplicator {

    KafkaWriter::KafkaWriter(const string alias, const string brokers, const string topic, CommandBuffer *commandBuffer, uint64_t trace) :
        Writer(alias, commandBuffer),
        conf(nullptr),
        tconf(nullptr),
        brokers(brokers.c_str()),
        topic(topic.c_str()),
        producer(nullptr),
        ktopic(nullptr),
        trace(trace){
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
            if (trace >= 3)
                cerr << "Kafka writer buffer: " << dec << commandBuffer->posStart << " - " << commandBuffer->posEnd << " (" << length << ")" << endl;

            if (length > 0) {
                if (trace <= 0) {
                    if (producer->produce(
                            ktopic, Topic::PARTITION_UA, Producer::RK_MSG_COPY, commandBuffer->intraThreadBuffer + commandBuffer->posStart + 8,
                            length - 8, nullptr, nullptr)) {
                        cerr << "ERROR: writing to topic " << endl;
                    }
                } else {
                    cout << "KAFKA: ";
                    for (uint64_t i = 0; i < length - 8; ++i)
                        cout << commandBuffer->intraThreadBuffer[commandBuffer->posStart + 8 + i];
                    cout << endl;
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

        if (trace >= 3)
            cerr << "Kafka writer buffer at shutdown: " << dec << commandBuffer->posStart << " - " << commandBuffer->posEnd << " (" << length << ")" << endl;
        return 0;
    }

    int KafkaWriter::initialize() {
        string errstr;
        Conf *conf = Conf::create(Conf::CONF_GLOBAL);
        Conf *tconf = Conf::create(Conf::CONF_TOPIC);
        conf->set("metadata.broker.list", brokers, errstr);

        if (trace <= 0) {
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

    void KafkaWriter::beginTran(typescn scn, typexid xid) {
        commandBuffer
                ->beginTran()
                ->append("{\"scn\": \"")
                ->append(to_string(scn))
                ->append("\", \"xid\": \"0x")
                ->appendHex(USN(xid), 4)
                ->append('.')
                ->appendHex(SLT(xid), 3)
                ->append('.')
                ->appendHex(SQN(xid), 8)
                ->append("\", dml: [");
    }

    void KafkaWriter::next() {
        commandBuffer->append(", ");
    }

    void KafkaWriter::commitTran() {
        commandBuffer
                ->append("]}")
                ->commitTran();
    }

    //0x05010B0B
    void KafkaWriter::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        uint64_t pos = 0,  fieldPos = redoLogRecord2->fieldPos, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength;

        for (uint64_t i = 1; i < 4; ++i) {
            fieldLength = oracleEnvironment->read16(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta + i * 2);
            fieldPos += (fieldLength + 3) & 0xFFFC;
        }
        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord2->nrow; ++r) {
            if (r > 0)
                commandBuffer->append(", ");

            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord2->data[fieldPos + pos + 2];
            pos = 3;

            if ((redoLogRecord2->op & OP_ROWDEPENDENCIES) != 0) {
                if (oracleEnvironment->version < 12200)
                    pos += 6;
                else
                    pos += 8;
            }

            commandBuffer
                    ->append("{\"operation\": \"insert\", \"table\": \"")
                    ->append(redoLogRecord2->object->owner)
                    ->append('.')
                    ->append(redoLogRecord2->object->objectName)
                    ->append("\", \"rowid\": \"")
                    ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->bdba,
                            oracleEnvironment->read16(redoLogRecord2->data + redoLogRecord2->slotsDelta + r * 2))
                    ->append("\", \"after\": {");

            for (uint64_t i = 0; i < redoLogRecord2->object->columns.size(); ++i) {
                bool isNull = false;

                if (i >= jcc)
                    isNull = true;
                else {
                    fieldLength = redoLogRecord2->data[fieldPos + pos];
                    ++pos;
                    if (fieldLength == 0xFF) {
                        isNull = true;
                    } else
                    if (fieldLength == 0xFE) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord2->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                //NULL values
                if (!isNull) {
                    if (prevValue)
                        commandBuffer->append(", ");
                    else
                        prevValue = true;

                    commandBuffer
                            ->append('"')
                            ->append(redoLogRecord2->object->columns[i]->columnName)
                            ->append("\": \"");

                    appendValue(redoLogRecord2, redoLogRecord2->object->columns[i]->typeNo, fieldPos + pos, fieldLength);
                    commandBuffer->append('"');

                    pos += fieldLength;
                }
            }

            commandBuffer->append("}}");

            fieldPosStart += oracleEnvironment->read16(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta + r * 2);
        }
    }

    //0x05010B0C
    void KafkaWriter::parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        uint64_t pos = 0, fieldPos = redoLogRecord1->fieldPos, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength;
        //uint16_t rowLengths;

        for (uint64_t i = 1; i < 6; ++i) {
            fieldLength = oracleEnvironment->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + i * 2);
            fieldPos += (fieldLength + 3) & 0xFFFC;
            //if (i == 5)
            //    rowLengths = fieldPos;
        }
        fieldPosStart = fieldPos;

        for (uint64_t r = 0; r < redoLogRecord1->nrow; ++r) {
            if (r > 0)
                commandBuffer->append(", ");

            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord1->data[fieldPos + pos + 2];
            pos = 3;

            if ((redoLogRecord1->op & OP_ROWDEPENDENCIES) != 0) {
                if (oracleEnvironment->version < 12200)
                    pos += 6;
                else
                    pos += 8;
            }

            commandBuffer
                    ->append("{\"operation\": \"delete\", \"table\": \"")
                    ->append(redoLogRecord1->object->owner)
                    ->append('.')
                    ->append(redoLogRecord1->object->objectName)
                    ->append("\", \"rowid\": \"")
                    ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->bdba,
                            oracleEnvironment->read16(redoLogRecord1->data + redoLogRecord1->slotsDelta + r * 2))
                    ->append("\", \"before\": {");

            for (uint64_t i = 0; i < redoLogRecord1->object->columns.size(); ++i) {
                bool isNull = false;

                if (i >= jcc)
                    isNull = true;
                else {
                    fieldLength = redoLogRecord1->data[fieldPos + pos];
                    ++pos;
                    if (fieldLength == 0xFF) {
                        isNull = true;
                    } else
                    if (fieldLength == 0xFE) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + pos);
                        pos += 2;
                    }
                }

                //NULL values
                if (!isNull) {
                    if (prevValue)
                        commandBuffer->append(", ");
                    else
                        prevValue = true;

                    commandBuffer
                            ->append('"')
                            ->append(redoLogRecord1->object->columns[i]->columnName)
                            ->append("\": \"");

                    appendValue(redoLogRecord1, redoLogRecord1->object->columns[i]->typeNo, fieldPos + pos, fieldLength);
                    commandBuffer->append('"');

                    pos += fieldLength;
                }
            }

            commandBuffer->append("}}");

            fieldPosStart += oracleEnvironment->read16(redoLogRecord1->data + redoLogRecord1->rowLenghsDelta + r * 2);
        }
    }

    void KafkaWriter::parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type, OracleEnvironment *oracleEnvironment) {
        typedba bdba;
        uint16_t slot;
        RedoLogRecord *redoLogRecord;

        if (type == TRANSACTION_INSERT) {
            commandBuffer->append("{\"operation\": \"insert\", \"table\": \"");

            redoLogRecord = redoLogRecord2;
            while (redoLogRecord != nullptr) {
                if ((redoLogRecord->fb & FB_F) != 0)
                    break;
                redoLogRecord = redoLogRecord->next;
            }

            if (redoLogRecord == nullptr) {
                if (oracleEnvironment->trace >= TRACE_WARN)
                    cerr << "WARNING: could not find correct rowid for INSERT" << endl;
                bdba = 0;
                slot = 0;
            } else {
                bdba = redoLogRecord->bdba;
                slot = redoLogRecord->slot;
            }

        } else if (type == TRANSACTION_DELETE) {
            commandBuffer->append("{\"operation\": \"delete\", \"table\": \"");
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        } else {
            commandBuffer->append("{\"operation\": \"update\", \"table\": \"");
            if (redoLogRecord1->suppLogBdba > 0 || redoLogRecord1->suppLogSlot > 0) {
                bdba = redoLogRecord1->suppLogBdba;
                slot = redoLogRecord1->suppLogSlot;
            } else {
                bdba = redoLogRecord2->bdba;
                slot = redoLogRecord2->slot;
            }
        }

        commandBuffer
                ->append(redoLogRecord2->object->owner)
                ->append('.')
                ->append(redoLogRecord2->object->objectName)
                ->append("\", \"rowid\": \"")
                ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, bdba, slot)
                ->append("\"");

        uint64_t fieldPos, colNum, colShift, cc, headerSize;
        uint16_t fieldLength;
        uint8_t *nulls, bits, *colNums;
        bool prevValue;
        uint64_t *afterPos = nullptr, *beforePos = nullptr;
        uint16_t *afterLen = nullptr, *beforeLen = nullptr;
        RedoLogRecord **beforeRecord, **afterRecord;
        if (type == TRANSACTION_UPDATE && oracleEnvironment->sortCols > 0) {
            afterPos = new uint64_t[redoLogRecord1->object->totalCols * sizeof(uint64_t)];
            memset(afterPos, 0, redoLogRecord1->object->totalCols * sizeof(uint64_t));
            beforePos = new uint64_t[redoLogRecord1->object->totalCols * sizeof(uint64_t)];
            memset(beforePos, 0, redoLogRecord1->object->totalCols * sizeof(uint64_t));
            afterLen = new uint16_t[redoLogRecord1->object->totalCols * sizeof(uint16_t)];
            beforeLen = new uint16_t[redoLogRecord1->object->totalCols * sizeof(uint16_t)];
            beforeRecord = new RedoLogRecord*[redoLogRecord1->object->totalCols * sizeof(RedoLogRecord *)];
            afterRecord = new RedoLogRecord*[redoLogRecord1->object->totalCols * sizeof(RedoLogRecord *)];

        }

        if (type == TRANSACTION_DELETE || type == TRANSACTION_UPDATE) {
            if (type != TRANSACTION_UPDATE || oracleEnvironment->sortCols == 0)
                commandBuffer->append(", \"before\": {");
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
                        colShift = redoLogRecord->suppLogBefore - 1 - oracleEnvironment->read16(colNums);
                    } else {
                        colNums = nullptr;
                        headerSize = 4;
                        colShift = redoLogRecord->suppLogBefore - 1;
                    }

                    for (uint64_t i = 1; i <= headerSize; ++i) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                    for (uint64_t i = 0; i < cc; ++i) {
                        if (i + headerSize + 1 > redoLogRecord->fieldCnt) {
                            cerr << "ERROR: reached out of columns" << endl;
                            break;
                        }
                        if (colNums != nullptr) {
                            colNum = oracleEnvironment->read16(colNums) + colShift;
                            colNums += 2;
                        } else
                            colNum = i + colShift;

                        if (colNum > redoLogRecord->object->columns.size()) {
                            cerr << "ERROR: too big column id: " << dec << colNum << endl;
                            break;
                        }

                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (i + headerSize + 1) * 2);
                        if (((*nulls & bits) != 0 || fieldLength == 0) && type == TRANSACTION_DELETE) {
                            //null
                        } else {
                            if (type == TRANSACTION_UPDATE && oracleEnvironment->sortCols > 0) {
                                if (fieldLength != 0) {
                                    beforePos[colNum] = fieldPos;
                                    beforeLen[colNum] = fieldLength;
                                    beforeRecord[colNum] = redoLogRecord;
                                }
                            } else {
                                if (prevValue) {
                                    commandBuffer->append(", ");
                                } else
                                    prevValue = true;

                                commandBuffer
                                        ->append('"')
                                        ->append(redoLogRecord->object->columns[colNum]->columnName)
                                        ->append("\": \"");

                                if ((*nulls & bits) == 0 && fieldLength > 0)
                                    appendValue(redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, fieldLength);

                                commandBuffer->append('"');
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
                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 1) * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                        ++headerSize;
                    }

                    //supplemental columns
                    if (cc + headerSize + 1 <= redoLogRecord->fieldCnt) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 1) * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;

                        if (redoLogRecord->suppLogCC > 0 && redoLogRecord->cc + headerSize + 4 <= redoLogRecord->fieldCnt) {
                            fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 2) * 2);
                            colNums = redoLogRecord->data + fieldPos;
                            fieldPos += (fieldLength + 3) & 0xFFFC;

                            fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 3) * 2);
                            uint8_t* colSizes = redoLogRecord->data + fieldPos;
                            fieldPos += (fieldLength + 3) & 0xFFFC;

                            for (uint64_t i = 0; i < redoLogRecord->suppLogCC; ++i) {
                                fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (redoLogRecord->cc + headerSize + 4 + i) * 2);
                                colNum = oracleEnvironment->read16(colNums) + colShift - 1;
                                colNums += 2;
                                uint16_t colLength = oracleEnvironment->read16(colSizes);

                                if (type == TRANSACTION_UPDATE && oracleEnvironment->sortCols > 0) {

                                    beforePos[colNum] = fieldPos;
                                    beforeRecord[colNum] = redoLogRecord;
                                    if (colLength != 0xFFFF) {
                                        beforeLen[colNum] = colLength;
                                    } else
                                        beforeLen[colNum] = 0;
                                } else {
                                    if (prevValue) {
                                        commandBuffer->append(", ");
                                    } else
                                        prevValue = true;

                                    commandBuffer
                                            ->append('"')
                                            ->append(redoLogRecord->object->columns[colNum]->columnName)
                                            ->append("\": \"");

                                    if (colLength == 0xFFFF) {
                                        //null
                                    } else {
                                        appendValue(redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, colLength);
                                    }

                                    commandBuffer
                                            ->append('"');
                                }

                                colSizes += 2;
                                fieldPos += (fieldLength + 3) & 0xFFFC;
                            }
                        }
                    }
                }

                redoLogRecord = redoLogRecord->next;
            }
            if (type != TRANSACTION_UPDATE || oracleEnvironment->sortCols == 0)
                commandBuffer->append("}");
        }

        if (type == TRANSACTION_INSERT || type == TRANSACTION_UPDATE) {
            if (type != TRANSACTION_UPDATE || oracleEnvironment->sortCols == 0)
                commandBuffer->append(", \"after\": {");
            redoLogRecord = redoLogRecord2;
            prevValue = false;

            while (redoLogRecord != nullptr) {
                if (oracleEnvironment->trace >= TRACE_FULL) {
                    cerr << "AFTER OP:" << setw(4) << setfill('0') << hex << redoLogRecord->opCode << endl;
                    redoLogRecord->dumpHex(cerr, oracleEnvironment);
                }

                if (redoLogRecord->opCode == 0x0B02) {
                    fieldPos = redoLogRecord->fieldPos;
                    nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
                    bits = 1;
                    cc = redoLogRecord->cc;
                    colNum = redoLogRecord->suppLogAfter - 1;

                    for (uint64_t i = 1; i <= 2; ++i) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
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

                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (i + 3) * 2);
                        if ((*nulls & bits) != 0 || fieldLength == 0) {
                            //null
                        } else {
                            if (type == TRANSACTION_UPDATE && oracleEnvironment->sortCols > 0) {
                                afterPos[colNum] = fieldPos;
                                afterLen[colNum] = fieldLength;
                                afterRecord[colNum] = redoLogRecord;
                            } else {
                                if (prevValue)
                                    commandBuffer->append(", ");
                                else
                                    prevValue = true;

                                commandBuffer
                                        ->append('"')
                                        ->append(redoLogRecord->object->columns[colNum]->columnName)
                                        ->append("\": \"");

                                appendValue(redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, fieldLength);

                                commandBuffer->append('"');
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
                        colShift = redoLogRecord->suppLogAfter - 1 - oracleEnvironment->read16(colNums);
                        headerSize = 3;
                    } else {
                        colNums = nullptr;
                        colShift = redoLogRecord->suppLogAfter - 1;
                        headerSize = 2;
                    }
                    bits = 1;

                    for (uint64_t i = 1; i <= headerSize; ++i) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + i * 2);
                        fieldPos += (fieldLength + 3) & 0xFFFC;
                    }

                    for (uint64_t i = 0; i < redoLogRecord->cc && i + headerSize + 1 <= redoLogRecord->fieldCnt; ++i) {
                        fieldLength = oracleEnvironment->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (i + headerSize + 1) * 2);
                        if (colNums != nullptr) {
                            colNum = oracleEnvironment->read16(colNums) + colShift;
                            colNums += 2;
                        } else
                            colNum = i + colShift;

                        if (type == TRANSACTION_UPDATE && oracleEnvironment->sortCols > 0) {
                            if (fieldLength != 0) {
                                afterPos[colNum] = fieldPos;
                                afterLen[colNum] = fieldLength;
                                afterRecord[colNum] = redoLogRecord;
                            }
                        } else {
                            if (prevValue)
                                commandBuffer->append(", ");
                            else
                                prevValue = true;

                            commandBuffer
                                    ->append('"')
                                    ->append(redoLogRecord->object->columns[colNum]->columnName)
                                    ->append("\": \"");

                            if ((*nulls & bits) != 0 || fieldLength == 0) {
                                //nulls
                            } else {
                                appendValue(redoLogRecord, redoLogRecord->object->columns[colNum]->typeNo, fieldPos, fieldLength);

                            }

                            commandBuffer->append('"');
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
            if (type != TRANSACTION_UPDATE || oracleEnvironment->sortCols == 0)
                commandBuffer->append("}");
        }

        if (type == TRANSACTION_UPDATE && oracleEnvironment->sortCols > 0) {
            if (oracleEnvironment->sortCols >= 2) {
                for (uint64_t i = 0; i < redoLogRecord1->object->totalCols; ++i) {
                    if (redoLogRecord1->object->columns[i]->numPk == 0) {
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

            commandBuffer->append(", \"before\": {");
            for (uint64_t i = 0; i < redoLogRecord1->object->totalCols; ++i) {
                if (beforePos[i] > 0 || afterPos[i] > 0) {
                    if (prevValue)
                        commandBuffer->append(", ");
                    else
                        prevValue = true;

                    commandBuffer
                            ->append('"')
                            ->append(redoLogRecord1->object->columns[i]->columnName)
                            ->append("\": \"");

                    if (beforePos[i] == 0 || beforeLen[i] == 0) {
                        //nulls
                    } else {
                        if (oracleEnvironment->trace >= TRACE_FULL) {
                            cerr << "BEFORE[" << dec << i << "] type: " << dec << redoLogRecord1->object->columns[i]->typeNo <<
                                    " pos: " << dec << beforePos[i] << " length: " << dec << beforeLen[i] << " value:";
                            for (uint64_t j = 0; j < beforeLen[i]; ++j)
                                cerr << " " << hex << setfill('0') << setw(2) << hex << (uint64_t)beforeRecord[i]->data[beforePos[i] + j];
                            cerr << endl;
                        }
                        appendValue(beforeRecord[i], redoLogRecord1->object->columns[i]->typeNo, beforePos[i], beforeLen[i]);
                    }

                    commandBuffer->append('"');
                }
            }
            commandBuffer->append("}");
            prevValue = false;
            commandBuffer->append(", \"after\": {");
            for (uint64_t i = 0; i < redoLogRecord1->object->totalCols; ++i) {
                if (afterPos[i] > 0 || (redoLogRecord1->object->columns[i]->numPk > 0 && beforePos[i] > 0)) {
                    if (prevValue)
                        commandBuffer->append(", ");
                    else
                        prevValue = true;

                    commandBuffer
                            ->append('"')
                            ->append(redoLogRecord1->object->columns[i]->columnName)
                            ->append("\": \"");

                    //for PK value is only present before
                    if (afterPos[i] == 0 && redoLogRecord1->object->columns[i]->numPk > 0) {
                        if (beforeLen[i] == 0) {
                            //nulls
                        } else {
                            if (oracleEnvironment->trace >= TRACE_FULL) {
                                cerr << "AFTER[" << dec << i << "] type: " << dec << redoLogRecord1->object->columns[i]->typeNo <<
                                        " pos: " << dec << beforePos[i] << " length: " << dec << beforeLen[i] << " value:";
                                for (uint64_t j = 0; j < beforeLen[i]; ++j)
                                    cerr << " " << hex << setfill('0') << setw(2) << hex << (uint64_t)beforeRecord[i]->data[beforePos[i] + j];
                                cerr << endl;
                            }
                            appendValue(beforeRecord[i], redoLogRecord1->object->columns[i]->typeNo, beforePos[i], beforeLen[i]);
                        }
                    } else {
                        if (afterLen[i] == 0) {
                            //nulls
                        } else {
                            if (oracleEnvironment->trace >= TRACE_FULL) {
                                cerr << "AFTER[" << dec << i << "] type: " << dec << redoLogRecord1->object->columns[i]->typeNo <<
                                        " pos: " << dec << afterPos[i] << " length: " << dec << afterLen[i] << " value:";
                                for (uint64_t j = 0; j < afterLen[i]; ++j)
                                    cerr << " " << hex << setfill('0') << setw(2) << hex << (uint64_t)afterRecord[i]->data[afterPos[i] + j];
                                cerr << endl;
                            }
                            appendValue(afterRecord[i], redoLogRecord1->object->columns[i]->typeNo, afterPos[i], afterLen[i]);
                        }
                    }

                    commandBuffer->append('"');
                }
            }
            commandBuffer->append("}");

            delete[] afterRecord;
            delete[] beforeRecord;
            delete[] afterLen;
            delete[] beforeLen;
            delete[] afterPos;
            delete[] beforePos;
        }

        commandBuffer->append("}");
    }

    //0x18010000
    void KafkaWriter::parseDDL(RedoLogRecord *redoLogRecord1, OracleEnvironment *oracleEnvironment) {
        uint64_t fieldPos = redoLogRecord1->fieldPos;
        uint16_t seq = 0, cnt = 0, type;

        uint16_t fieldLength;
        for (uint64_t i = 1; i <= redoLogRecord1->fieldCnt; ++i) {
            fieldLength = oracleEnvironment->read16(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta + i * 2);
            if (i == 1) {
                type = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + 12);
                seq = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + 18);
                cnt = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + 20);
                if (oracleEnvironment->trace >= TRACE_DETAIL) {
                    cerr << "SEQ: " << dec << seq << "/" << dec << cnt << endl;
                    cerr << "TYPE: " << dec << type << endl;
                }
            } else if (i == 8) {
                //DDL text
                if (oracleEnvironment->trace >= TRACE_DETAIL) {
                    cerr << "DDL[" << dec << fieldLength << "]: ";
                    for (uint64_t j = 0; j < (uint64_t)(fieldLength - 1); ++j) {
                        cerr << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cerr << endl;
                }
            } else if (i == 9) {
                //owner
                if (oracleEnvironment->trace >= TRACE_DETAIL) {
                    cerr << "OWNER[" << dec << fieldLength << "]: ";
                    for (uint64_t j = 0; j < fieldLength; ++j) {
                        cerr << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cerr << endl;
                }
            } else if (i == 10) {
                //table
                if (oracleEnvironment->trace >= TRACE_DETAIL) {
                    cerr << "TABLE[" << fieldLength << "]: ";
                    for (uint64_t j = 0; j < fieldLength; ++j) {
                        cerr << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cerr << endl;
                }
            } else if (i == 12) {
                redoLogRecord1->objn = oracleEnvironment->read32(redoLogRecord1->data + fieldPos + 0);
                if (oracleEnvironment->trace >= TRACE_DETAIL) {
                    cerr << "OBJN: " << dec << redoLogRecord1->objn << endl;
                }
            }

            fieldPos += (fieldLength + 3) & 0xFFFC;
        }

        if (type == 85) {
            commandBuffer
                    ->append("{\"operation\": \"truncate\", \"table\": \"")
                    ->append(redoLogRecord1->object->owner)
                    ->append('.')
                    ->append(redoLogRecord1->object->objectName)
                    ->append("\"}");
        } else if (type == 12) {
           commandBuffer
                   ->append("{\"operation\": \"drop\", \"table\": \"")
                   ->append(redoLogRecord1->object->owner)
                   ->append('.')
                   ->append(redoLogRecord1->object->objectName)
                   ->append("\"}");
        } else if (type == 15) {
          commandBuffer
                  ->append("{\"operation\": \"alter\", \"table\": \"")
                  ->append(redoLogRecord1->object->owner)
                  ->append('.')
                  ->append(redoLogRecord1->object->objectName)
                  ->append("\"}");
       }
    }
}
