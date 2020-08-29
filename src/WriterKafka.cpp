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
using namespace RdKafka;

namespace OpenLogReplicator {

    WriterKafka::WriterKafka(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t shortMessage,
            const char *brokers, const char *topic, uint64_t maxMessageKb) :
        Writer(alias, oracleAnalyser, shortMessage, maxMessageMb),
        conf(nullptr),
        tconf(nullptr),
        brokers(brokers),
        topic(topic),
        producer(nullptr),
        ktopic(nullptr) {

        conf = Conf::create(Conf::CONF_GLOBAL);
        tconf = Conf::create(Conf::CONF_TOPIC);

        string errstr;
        conf->set("metadata.broker.list", brokers, errstr);
        conf->set("client.id", "OpenLogReplicator", errstr);
        string maxMessageMbStr = to_string(maxMessageMb * 1024 * 1024);
        conf->set("message.max.bytes", maxMessageMbStr.c_str(), errstr);

        producer = Producer::create(conf, errstr);
        if (producer == nullptr) {
            CONFIG_FAIL("Kafka message: " << errstr);
        }

        ktopic = Topic::create(producer, topic, tconf, errstr);
        if (ktopic == nullptr) {
            CONFIG_FAIL("Kafka message: " << errstr);
        }
    }

    WriterKafka::~WriterKafka() {
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

    void WriterKafka::sendMessage(uint8_t *buffer, uint64_t length, bool dealloc) {
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

    string WriterKafka::getName() {
        return "Kafka:" + topic;
    }
}
