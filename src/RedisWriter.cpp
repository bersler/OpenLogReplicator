/* Thread writing to Redis stream
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
#include <hiredis.h>
#include "types.h"
#include "RedisWriter.h"
#include "OracleEnvironment.h"
#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    RedisWriter::RedisWriter(const string alias, const string host, uint32_t port, CommandBuffer *commandBuffer) :
        Writer(alias, commandBuffer),
        host(host),
        port(port),
        c(nullptr) {
    }

    RedisWriter::~RedisWriter() {
        redisFree(c);
    }

    void *RedisWriter::run() {
        cout << "- Redis Writer for " << host << ":" << port << endl;

        while (!this->shutdown) {
            uint32_t length, lengthProcessed;
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

            redisReply *reply;
            reply = (redisReply *)redisCommand(c, "MULTI");
            freeReplyObject(reply);

            lengthProcessed = 4;
            while (lengthProcessed < length) {
                //FIXME: waste of time to run strlen
                const char *key = (const char *)commandBuffer->intraThreadBuffer + commandBuffer->posStart + lengthProcessed;
                uint32_t keylen = strlen(key);
                const char *cmd = (const char *)commandBuffer->intraThreadBuffer + commandBuffer->posStart + lengthProcessed + keylen + 1;
                uint32_t cmdlen = strlen(cmd);

                cout << "SET [" << key << "] [" << cmd << "]" << endl;
                reply = (redisReply *)redisCommand(c, "SET %s %s", key, cmd);
                //cout << "RET [" << reply->str << "]" << endl;
                freeReplyObject(reply);

                lengthProcessed += keylen + 1 + cmdlen + 1;
            }

            reply = (redisReply *)redisCommand(c, "EXEC");
            freeReplyObject(reply);

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

    int RedisWriter::initialize() {
        c = redisConnect(host.c_str(), port);

        redisReply *reply = (redisReply *)redisCommand(c, "PING");
        if (reply != nullptr)
            printf("PING: %s\n", reply->str);
        freeReplyObject(reply);

        if (c->err) {
            cerr << "ERROR: Redis: " << c->errstr << endl;
            return 0;
        }

        return 1;
    }

    void RedisWriter::beginTran(typescn scn, typexid xid) {
        commandBuffer->beginTran();
    }

    void RedisWriter::next() {
    }

    void RedisWriter::commitTran() {
        commandBuffer->commitTran();
    }

    void RedisWriter::parseInsert(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint32_t fieldPos = redoLogRecord2->fieldPos, fieldPosStart;
        uint8_t *nulls = redoLogRecord2->data + redoLogRecord2->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 2; ++i)
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        fieldPosStart = fieldPos;

        commandBuffer
                ->append(redoLogRecord2->object->owner)
                ->append('.')
                ->append(redoLogRecord2->object->objectName)
                ->append('.');

        for (uint32_t i = 0; i < redoLogRecord2->object->columns.size(); ++i) {
            //is PK or table has no PK
            if (redoLogRecord2->object->columns[i]->numPk > 0 || redoLogRecord2->object->totalPk == 0) {
                if (prevValue)
                    commandBuffer->append('.');
                else
                    prevValue = true;

                if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3] == 0
                        || i >= redoLogRecord2->cc) {
                    commandBuffer->append("NULL");
                } else {
                    commandBuffer->append('"');

                    appendValue(redoLogRecord2, redoLogRecord2->object->columns[i]->typeNo, fieldPos,
                            ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3]);

                    commandBuffer->append('"');
                }
            }

            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3] + 3) & 0xFFFC;
        }
        fieldPos = fieldPosStart;
        nulls = redoLogRecord2->data + redoLogRecord2->nullsDelta;

        commandBuffer->append(0);
        prevValue = false;

        for (uint32_t i = 0; i < redoLogRecord2->object->columns.size(); ++i) {
            if (prevValue)
                commandBuffer->append(',');
            else
                prevValue = true;

            if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i + 3] == 0
                    || i >= redoLogRecord2->cc) {
                commandBuffer->append("NULL");
            } else {
                commandBuffer->append('"');

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

        commandBuffer->append(0);
    }

    void RedisWriter::parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        uint32_t pos = 0;
        uint32_t fieldPos = redoLogRecord2->fieldPos, fieldPosStart;
        bool prevValue;
        uint16_t fieldLength;

        for (uint32_t i = 1; i < 4; ++i)
            fieldPos += (((uint16_t*)(redoLogRecord2->data + redoLogRecord2->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        fieldPosStart = fieldPos;

        for (uint32_t r = 0; r < redoLogRecord2->nrow; ++r) {

            pos = 0;
            prevValue = false;
            fieldPos = fieldPosStart;
            uint8_t jcc = redoLogRecord2->data[fieldPos + pos + 2];
            pos = 3;

            commandBuffer
                    ->append(redoLogRecord2->object->owner)
                    ->append('.')
                    ->append(redoLogRecord2->object->objectName)
                    ->append('.');

            for (uint32_t i = 0; i < jcc; ++i) {
                bool isNull = false;
                fieldLength = redoLogRecord2->data[fieldPos + pos];
                ++pos;
                if (fieldLength == 0xFF) {
                    isNull = true;
                } else
                if (fieldLength == 0xFE) {
                    fieldLength = oracleEnvironment->read16(redoLogRecord2->data + fieldPos + pos);
                    pos += 2;
                }

                //is PK or table has no PK
                if (redoLogRecord2->object->columns[i]->numPk > 0 || redoLogRecord2->object->totalPk == 0) {
                    if (prevValue)
                        commandBuffer->append('.');
                    else
                        prevValue = true;

                    //NULL values
                    if (isNull) {
                        commandBuffer->append("NULL");
                    } else {
                        commandBuffer->append('"');
                        appendValue(redoLogRecord2, redoLogRecord2->object->columns[i]->typeNo, fieldPos + pos, fieldLength);
                        commandBuffer->append('"');
                    }
                }
                pos += fieldLength;
            }
            for (uint32_t i = jcc; i < redoLogRecord2->object->columns.size(); ++i)
                commandBuffer->append(".NULL");

            pos = 0;
            commandBuffer->append(0);

            pos = 3;
            prevValue = false;
            fieldPos = fieldPosStart;

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

                if (prevValue)
                    commandBuffer->append(',');
                else
                    prevValue = true;

                //NULL values
                if (isNull) {
                    commandBuffer->append("NULL");
                } else {
                    commandBuffer->append('"');
                    appendValue(redoLogRecord2, redoLogRecord2->object->columns[i]->typeNo, fieldPos + pos, fieldLength);
                    commandBuffer->append('"');
                }

                pos += fieldLength;
            }

            commandBuffer->append(0);

            fieldPosStart += ((uint16_t*)(redoLogRecord2->data + redoLogRecord2->rowLenghsDelta))[r];
        }
    }

    void RedisWriter::parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        //TODO
    }

    void RedisWriter::parseUpdate(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) {
        //TODO
    }

    void RedisWriter::parseDelete(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        uint32_t fieldPos = redoLogRecord1->fieldPos, fieldPosStart;
        uint8_t *nulls = redoLogRecord1->data + redoLogRecord1->nullsDelta, bits = 1;
        bool prevValue = false;

        for (uint32_t i = 1; i <= 4; ++i)
            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i] + 3) & 0xFFFC;
        fieldPosStart = fieldPos;

        commandBuffer
                ->append(redoLogRecord1->object->owner)
                ->append('.')
                ->append(redoLogRecord1->object->objectName)
                ->append('.');

        for (uint32_t i = 0; i < redoLogRecord1->object->columns.size(); ++i) {
            //is PK or table has no PK
            if (redoLogRecord1->object->columns[i]->numPk > 0 || redoLogRecord1->object->totalPk == 0) {
                if (prevValue) {
                    commandBuffer
                            ->append('.');
                } else
                    prevValue = true;

                if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5] == 0 ||
                        i >= redoLogRecord1->cc) {
                    commandBuffer->append("NULL");
                } else {
                    commandBuffer->append('"');

                    appendValue(redoLogRecord1, redoLogRecord1->object->columns[i]->typeNo, fieldPos,
                            ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5]);

                    commandBuffer->append('"');
                }
            }

            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5] + 3) & 0xFFFC;
        }
        fieldPos = fieldPosStart;
        nulls = redoLogRecord1->data + redoLogRecord1->nullsDelta;

        commandBuffer->append(0);

        prevValue = false;

        for (uint32_t i = 0; i < redoLogRecord1->object->columns.size(); ++i) {
            if ((*nulls & bits) != 0 || ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5] == 0 ||
                    i >= redoLogRecord1->cc) {
                if (prevValue) {
                    commandBuffer->append(',');
                } else
                    prevValue = true;

                commandBuffer->append("NULL");
            } else {
                if (prevValue) {
                    commandBuffer->append(',');
                } else
                    prevValue = true;

                commandBuffer->append('"');

                appendValue(redoLogRecord1, redoLogRecord1->object->columns[i]->typeNo, fieldPos,
                        ((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5]);

                commandBuffer->append('"');
            }

            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
            fieldPos += (((uint16_t*)(redoLogRecord1->data + redoLogRecord1->fieldLengthsDelta))[i + 5] + 3) & 0xFFFC;
        }

        commandBuffer->append(0);
    }

    void RedisWriter::parseDDL(RedoLogRecord *redoLogRecord1, OracleEnvironment *oracleEnvironment) {
        //TODO
    }
}
