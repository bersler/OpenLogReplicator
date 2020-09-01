/* Header for WriterKafka class
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

#include <queue>
#include <set>
#include <stdint.h>
#include <librdkafka/rdkafkacpp.h>

#include "types.h"
#include "Writer.h"

#ifndef WRITERKAFKA_H_
#define WRITERKAFKA_H_

#define MAX_KAFKA_MESSAGE_MB        953
#define MAX_KAFKA_MAX_MESSAGES      10000000

using namespace std;
using namespace RdKafka;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class OracleAnalyser;

    class WriterKafka : public Writer {
    protected:
        Conf *conf;
        Conf *tconf;
        string brokers;
        string topic;
        Producer *producer;
        Topic *ktopic;
        uint64_t maxMessages;

        virtual void sendMessage(uint8_t *buffer, uint64_t length, bool dealloc);
        virtual string getName();

    public:
        WriterKafka(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t shortMessage,
                    const char *brokers, const char *topic, uint64_t maxMessageMb, uint64_t maxMessages);
        virtual ~WriterKafka();
    };
}

#endif
