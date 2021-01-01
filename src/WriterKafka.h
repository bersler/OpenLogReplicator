/* Header for WriterKafka class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <librdkafka/rdkafka.h>

#include "Writer.h"

#ifndef WRITERKAFKA_H_
#define WRITERKAFKA_H_

#define MAX_KAFKA_MESSAGE_MB        953
#define MAX_KAFKA_MAX_MESSAGES      10000000

using namespace std;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class OracleAnalyzer;

    class WriterKafka : public Writer {
    protected:
        string brokers;
        string topic;
        uint64_t maxMessages;
        uint64_t enableIdempotence;
        char errstr[512];
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_conf_t *conf;
        static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
        static void error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque);
        static void logger_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf);

        virtual void sendMessage(OutputBufferMsg *msg);
        virtual string getName() const;
        virtual void pollQueue(void);

    public:
        WriterKafka(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *brokers, const char *topic, uint64_t maxMessageMb,
                uint64_t maxMessages, uint64_t pollInterval, uint64_t checkpointInterval, uint64_t queueSize, typeSCN startScn,
                typeSEQ startSeq, const char* startTime, uint64_t startTimeRel, uint64_t enableIdempotence);
        virtual ~WriterKafka();
    };
}

#endif
