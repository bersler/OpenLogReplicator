/* Thread writing directly to Kafka stream
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
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "WriterKafka.h"

using namespace std;
#ifdef LINK_LIBRARY_LIBRDKAFKA
using namespace RdKafka;
#endif /* LINK_LIBRARY_LIBRDKAFKA */

namespace OpenLogReplicator {

    WriterKafka::WriterKafka(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t shortMessage,
            const char *brokers, const char *topic, uint64_t maxMessageMb, uint64_t maxMessages) :
        Writer(alias, oracleAnalyser, shortMessage, maxMessageMb),
        brokers(brokers),
        topic(topic),
        maxMessages(maxMessages)
#ifdef LINK_LIBRARY_LIBRDKAFKA
    	,conf(nullptr),
        tconf(nullptr),
        producer(nullptr),
        ktopic(nullptr)
#endif /* LINK_LIBRARY_LIBRDKAFKA */
    {

#ifdef LINK_LIBRARY_LIBRDKAFKA
        conf = Conf::create(Conf::CONF_GLOBAL);
        tconf = Conf::create(Conf::CONF_TOPIC);

        string errstr;
        conf->set("metadata.broker.list", brokers, errstr);
        conf->set("client.id", "OpenLogReplicator", errstr);
        string maxMessageMbStr = to_string(maxMessageMb * 1024 * 1024);
        conf->set("message.max.bytes", maxMessageMbStr.c_str(), errstr);
        string maxMessagesStr = to_string(maxMessages);
        conf->set("queue.buffering.max.messages", maxMessagesStr.c_str(), errstr);

        producer = Producer::create(conf, errstr);
        if (producer == nullptr) {
            CONFIG_FAIL("Kafka message: " << errstr);
        }

        ktopic = Topic::create(producer, topic, tconf, errstr);
        if (ktopic == nullptr) {
            CONFIG_FAIL("Kafka message: " << errstr);
        }
#else
        RUNTIME_FAIL("Kafka writer is not compiled, exiting");
#endif /* LINK_LIBRARY_LIBRDKAFKA */
    }

    WriterKafka::~WriterKafka() {
#ifdef LINK_LIBRARY_LIBRDKAFKA
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
#endif /* LINK_LIBRARY_LIBRDKAFKA */
    }

    void WriterKafka::sendMessage(uint8_t *buffer, uint64_t length, bool dealloc) {
#ifdef LINK_LIBRARY_LIBRDKAFKA
        int msgflags = Producer::RK_MSG_COPY;
        if (dealloc)
            msgflags = Producer::RK_MSG_FREE;

        ErrorCode error = producer->produce(ktopic, Topic::PARTITION_UA, msgflags, buffer, length, nullptr, nullptr);
        if (error != ERR_NO_ERROR) {
            //on error, memory is not released by librdkafka
            if (dealloc)
                free(buffer);
            if (error == ERR__QUEUE_FULL) {
                RUNTIME_FAIL("writing to topic, bytes sent: " << dec << length << ", maximum number of outstanding messages has been reached (" <<
                        dec << maxMessages << "), increase \"max-messages\" parameter value");
            } else if (error == ERR_MSG_SIZE_TOO_LARGE) {
                RUNTIME_FAIL("writing to topic, bytes sent: " << dec << length << ", message is larger than configured max size (" <<
                        dec << maxMessageMb << " MB), increase \"max-message-mb\" parameter value");
            } else if (error == ERR__UNKNOWN_PARTITION) {
                RUNTIME_FAIL("writing to topic, bytes sent: " << dec << length << ", requested partition is unknown in the Kafka cluster");
            } else if (error == ERR__UNKNOWN_TOPIC) {
                RUNTIME_FAIL("writing to topic, bytes sent: " << dec << length << ", topic is unknown in the Kafka cluster");
            }
        }
#endif /* LINK_LIBRARY_LIBRDKAFKA */
    }

    string WriterKafka::getName() {
        return "Kafka:" + topic;
    }
}
