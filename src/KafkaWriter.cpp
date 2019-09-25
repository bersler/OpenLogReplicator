/* Thread writing to Kafka stream
   Copyright (C) 2018-2019 Adam Leszczynski.

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

    KafkaWriter::KafkaWriter(const string alias, const string brokers, const string topic, CommandBuffer *commandBuffer, uint32_t trace) :
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

        while (!this->shutdown) {
            uint32_t length;
            {
                unique_lock<mutex> lck(commandBuffer->mtx);
                while (commandBuffer->posStart == commandBuffer->posEnd) {
                    commandBuffer->readersCond.wait(lck);

                    if (this->shutdown)
                        break;
                }
                if (this->shutdown)
                    break;

                if (commandBuffer->posStart == commandBuffer->posSize && commandBuffer->posSize > 0) {
                    commandBuffer->posStart = 0;
                    commandBuffer->posSize = 0;
                }
                length = *((uint32_t*)(commandBuffer->intraThreadBuffer + commandBuffer->posStart));
            }
            //cout << "Buffer: " << commandBuffer->posStart << " - " << commandBuffer->posEnd << endl;

            if (trace <= 0) {
                if (producer->produce(
                        ktopic, Topic::PARTITION_UA, Producer::RK_MSG_COPY, commandBuffer->intraThreadBuffer + commandBuffer->posStart + 4,
                        length - 4, nullptr, nullptr)) {
                    cerr << "ERROR: writing to topic " << endl;
                }
            } else {
                cout << "KAFKA: ";
                for (uint32_t i = 0; i < length - 4; ++i)
                    cout << commandBuffer->intraThreadBuffer[commandBuffer->posStart + 4 + i];
                cout << endl;
            }

            {
                unique_lock<mutex> lck(commandBuffer->mtx);
                commandBuffer->posStart += (length + 3) & 0xFFFFFFFC;

                if (commandBuffer->posStart == commandBuffer->posSize && commandBuffer->posSize > 0) {
                    commandBuffer->posStart = 0;
                    commandBuffer->posSize = 0;
                }

                commandBuffer->writerCond.notify_all();
            }
        }

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

    void KafkaWriter::parseInsert(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint32_t fieldPos = redoLogRecord2->fieldPos;
        uint8_t *nulls = redoLogRecord2->data + redoLogRecord2->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 2; ++i)
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i] + 3) & 0xFFFC;

        commandBuffer
                ->append("{\"operation\":\"insert\", \"table\": \"")
                ->append(redoLogRecord2->object->owner)
                ->append('.')
                ->append(redoLogRecord2->object->objectName)
                ->append("\", \"rowid\": \"")
                ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->afn, redoLogRecord2->bdba & 0xFFFF, redoLogRecord2->slot)
                ->append("\", \"after\": {");

        for (uint32_t i = 0; i < redoLogRecord2->object->columns.size(); ++i) {
            if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3] == 0
                    || i >= redoLogRecord2->cc) {
                //null
            } else {
                if (prevValue)
                    commandBuffer->append(", ");
                else
                    prevValue = true;

                commandBuffer
                        ->append('"')
                        ->append(redoLogRecord2->object->columns[i]->columnName)
                        ->append("\": \"");

                appendValue(redoLogRecord2, redoLogRecord2->object->columns[i]->typeNo, fieldPos,
                        ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3]);

                commandBuffer->append('"');
            }

            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3] + 3) & 0xFFFC;
        }

        commandBuffer->append("}}");
    }

    void KafkaWriter::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        uint32_t pos = 0;
        uint32_t fieldPos = redoLogRecord2->fieldPos, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength;

        for (uint32_t i = 1; i < 4; ++i)
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        fieldPosStart = fieldPos;

        for (uint32_t r = 0; r < redoLogRecord2->nrow; ++r) {
            if (r > 0)
                commandBuffer->append(", ");

            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord2->data[fieldPos + pos + 2];
            pos = 3;

            commandBuffer
                    ->append("{\"operation\":\"insert\", \"table\": \"")
                    ->append(redoLogRecord2->object->owner)
                    ->append('.')
                    ->append(redoLogRecord2->object->objectName)
                    ->append("\", \"rowid\": \"")
                    ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->afn, redoLogRecord2->bdba & 0xFFFF,
                            ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->slotsDelta))[r])
                    ->append("\", \"after\": {");

            for (uint32_t i = 0; i < redoLogRecord2->object->columns.size(); ++i) {
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

            fieldPosStart += ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta))[r];
        }
    }

    void KafkaWriter::parseUpdate(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        uint16_t *colnums = nullptr;
        uint32_t fieldPos = redoLogRecord1->fieldPos;
        uint8_t *nulls = redoLogRecord1->data + redoLogRecord1->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 5; ++i) {
            if (i == 5)
                colnums = (uint16_t*)(redoLogRecord1->data + fieldPos);
            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }
        commandBuffer
                ->append("{\"operation\":\"update\", \"table\": \"")
                ->append(redoLogRecord1->object->owner)
                ->append('.')
                ->append(redoLogRecord1->object->objectName)
                ->append("\", \"rowid\": \"")
                ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->afn, redoLogRecord1->bdba & 0xFFFF, redoLogRecord1->slot)
                ->append("\", \"before\": {");

        if ((redoLogRecord1->xtype & 0x80) != 0) {
            uint32_t pos = 0;
            for (uint32_t i = 0; i < redoLogRecord1->cc; ++i) {
                if (prevValue) {
                    commandBuffer->append(", ");
                } else
                    prevValue = true;

                commandBuffer
                        ->append('"')
                        ->append(redoLogRecord1->object->columns[*colnums]->columnName)
                        ->append("\": \"");

                uint16_t fieldLength = redoLogRecord1->data[pos];
                ++pos;
                uint8_t isNull = (fieldLength == 0xFF);

                if (fieldLength == 0xFE) {
                    fieldLength = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + pos);
                    pos += 2;
                }

                if (isNull) {
                    //null
                } else {
                    appendValue(redoLogRecord1, redoLogRecord1->object->columns[*colnums]->typeNo, fieldPos + pos, fieldLength);
                    pos += fieldLength;
                }

                commandBuffer
                        ->append('"');

                ++colnums;
            }
        } else {
            for (uint32_t i = 0; i < redoLogRecord1->cc; ++i) {
                if (prevValue) {
                    commandBuffer->append(", ");
                } else
                    prevValue = true;

                commandBuffer
                        ->append('"')
                        ->append(redoLogRecord1->object->columns[*colnums]->columnName)
                        ->append("\": \"");

                if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 6] == 0) {
                    //null
                } else {
                    appendValue(redoLogRecord1, redoLogRecord1->object->columns[*colnums]->typeNo, fieldPos,
                            ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 6]);
                }

                commandBuffer
                        ->append('"');

                ++colnums;
                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
                fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 6] + 3) & 0xFFFC;
            }
        }

        fieldPos = redoLogRecord2->fieldPos;
        nulls = redoLogRecord2->data + redoLogRecord2->nullsDelta;
        bits = 1;
        prevValue = false;

        for (uint32_t i = 1; i <= 3; ++i) {
            if (i == 3)
                colnums = (uint16_t*)(redoLogRecord2->data + fieldPos);
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }

        commandBuffer->append("}, \"after\": {");

        for (uint32_t i = 0; i < redoLogRecord2->cc; ++i) {
            if (prevValue)
                commandBuffer->append(", ");
            else
                prevValue = true;

            commandBuffer
                    ->append('"')
                    ->append(redoLogRecord2->object->columns[*colnums]->columnName)
                    ->append("\": \"");

            if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 4] == 0) {
                //nulls
            } else {
                appendValue(redoLogRecord2, redoLogRecord2->object->columns[*colnums]->typeNo, fieldPos,
                        ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 4]);

            }

            commandBuffer->append('"');

            ++colnums;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 4] + 3) & 0xFFFC;
        }

        commandBuffer->append("}}");
    }

    void KafkaWriter::parseDelete(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint32_t fieldPos = redoLogRecord1->fieldPos;
        uint8_t *nulls = redoLogRecord1->data + redoLogRecord1->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 4; ++i)
            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i] + 3) & 0xFFFC;

        commandBuffer
                ->append("{\"operation\":\"delete\", \"table\": \"")
                ->append(redoLogRecord1->object->owner)
                ->append('.')
                ->append(redoLogRecord1->object->objectName)
                ->append("\", \"rowid\": \"")
                ->appendRowid(redoLogRecord1->objn, redoLogRecord1->objd, redoLogRecord2->afn, redoLogRecord1->bdba & 0xFFFF, redoLogRecord1->slot)
                ->append("\", \"before\": {");

        for (uint32_t i = 0; i < redoLogRecord1->object->columns.size(); ++i) {
            if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5] == 0
                    || i >= redoLogRecord1->cc) {
                //null
            } else {
                if (prevValue) {
                    commandBuffer->append(", ");
                } else
                    prevValue = true;

                commandBuffer
                        ->append('"')
                        ->append(redoLogRecord1->object->columns[i]->columnName)
                        ->append("\": \"");

                appendValue(redoLogRecord1, redoLogRecord1->object->columns[i]->typeNo, fieldPos,
                        ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5]);

                commandBuffer
                        ->append('"');
            }

            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5] + 3) & 0xFFFC;
        }

        commandBuffer->append("}}");
    }

    void KafkaWriter::parseDDL(RedoLogRecord *redoLogRecord1, OracleEnvironment *oracleEnvironment) {
        uint32_t fieldPos = redoLogRecord1->fieldPos, len;
        uint16_t seq = 0, cnt = 0, type;

        for (uint32_t i = 1; i <= redoLogRecord1->fieldNum; ++i) {
            if (i == 1) {
                type = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + 12);
                seq = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + 18);
                cnt = oracleEnvironment->read16(redoLogRecord1->data + fieldPos + 20);
                if (oracleEnvironment->trace >= 1) {
                    cout << "SEQ: " << dec << seq << "/" << dec << cnt << endl;
                }
            } else if (i == 8) {
                //DDL text
                if (oracleEnvironment->trace >= 1) {
                    len = ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i];
                    cout << "DDL[" << dec << len << "]: ";
                    for (uint32_t j = 0; j < len; ++j) {
                        cout << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cout << endl;
                }
            } else if (i == 9) {
                //owner
                if (oracleEnvironment->trace >= 1) {
                    len = ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i];
                    cout << "OWNER[" << dec << len << "]: ";
                    for (uint32_t j = 0; j < len; ++j) {
                        cout << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cout << endl;
                }
            } else if (i == 10) {
                //table
                if (oracleEnvironment->trace >= 1) {
                    len = ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i];
                    cout << "TABLE[" << len << "]: ";
                    for (uint32_t j = 0; j < len; ++j) {
                        cout << *(redoLogRecord1->data + fieldPos + j);
                    }
                    cout << endl;
                }
            } else if (i == 12) {
                redoLogRecord1->objn = oracleEnvironment->read32(redoLogRecord1->data + fieldPos + 0);
                if (oracleEnvironment->trace >= 1) {
                    cout << "OBJN: " << dec << redoLogRecord1->objn << endl;
                }
            }

            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        }

        if (type == 85) {
            commandBuffer
                    ->append("{\"operation\":\"truncate\", \"table\": \"")
                    ->append(redoLogRecord1->object->owner)
                    ->append('.')
                    ->append(redoLogRecord1->object->objectName)
                    ->append("\"}");
       }
    }
}
