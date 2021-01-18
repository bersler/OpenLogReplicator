/* Thread writing directly to Kafka stream
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

#include "OutputBuffer.h"
#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "RuntimeException.h"
#include "WriterKafka.h"

using namespace std;

extern uint64_t trace2;

namespace OpenLogReplicator {

    WriterKafka::WriterKafka(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *brokers, const char *topic,
            uint64_t maxMessageMb, uint64_t maxMessages, uint64_t pollInterval, uint64_t checkpointInterval, uint64_t queueSize,
            typeSCN startScn, typeSEQ startSeq, const char* startTime, uint64_t startTimeRel, uint64_t enableIdempotence) :
        Writer(alias, oracleAnalyzer, maxMessageMb, pollInterval, checkpointInterval, queueSize, startScn, startSeq, startTime,
                startTimeRel),
        brokers(brokers),
        topic(topic),
        maxMessages(maxMessages),
        enableIdempotence(enableIdempotence),
        rk(nullptr),
        rkt(nullptr),
        conf(nullptr) {

        conf = rd_kafka_conf_new();
        if (conf == nullptr) {
            CONFIG_FAIL("Kafka failed to create configuration, message: " << errstr);
        }

        string maxMessageMbStr = to_string(maxMessageMb * 1024 * 1024);
        string maxMessagesStr = to_string(maxMessages);
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            (enableIdempotence && rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) ||
            rd_kafka_conf_set(conf, "client.id", "OpenLogReplicator", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf, "group.id", "OpenLogReplicator", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf, "message.max.bytes", maxMessageMbStr.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf, "queue.buffering.max.messages", maxMessagesStr.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            CONFIG_FAIL("Kafka message: " << errstr);
        }

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_error_cb(conf, error_cb);
        rd_kafka_conf_set_log_cb(conf, logger_cb);
        rd_kafka_conf_set_opaque(conf, oracleAnalyzer);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (rk == nullptr) {
            CONFIG_FAIL("Kafka failed to create producer, message: " << errstr);
        }
        conf = nullptr;

        rkt = rd_kafka_topic_new(rk, topic, nullptr);
    }

    WriterKafka::~WriterKafka() {
        if (conf != nullptr)
            rd_kafka_conf_destroy(conf);

        if (rkt != nullptr)
            rd_kafka_topic_destroy(rkt);

        rd_kafka_resp_err_t err = rd_kafka_fatal_error(rk, NULL, 0);
        if (rk != nullptr)
            rd_kafka_destroy(rk);

        INFO("Kafka producer exit code: " << dec << err);
    }

    void WriterKafka::dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        OutputBufferMsg *msg = (OutputBufferMsg *)rkmessage->_private;
        OracleAnalyzer *oracleAnalyzer = msg->oracleAnalyzer;
        if (rkmessage->err) {
            WARNING("Kafka: " << msg->id << " delivery failed: " << rd_kafka_err2str(rkmessage->err));
        } else {
            oracleAnalyzer->outputBuffer->writer->confirmMessage(msg);
        }
    }

    void WriterKafka::error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
        OracleAnalyzer *oracleAnalyzer = (OracleAnalyzer*)opaque;

        WARNING("Kafka: " << rd_kafka_err2name((rd_kafka_resp_err_t)err) << ", reason: " << reason);

        if (err != RD_KAFKA_RESP_ERR__FATAL)
            return;

        char errstr[512];
        rd_kafka_resp_err_t orig_err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        RUNTIME_FAIL("Kafka: fatal error: " << rd_kafka_err2name(orig_err) << ", reason: " << errstr);
    }

    void WriterKafka::logger_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
        TRACE_(TRACE2_KAFKA, "level: " << dec << level << ", rk: " << (rk ? rd_kafka_name(rk) : NULL) << ", fac: " << fac << ", err: " << buf);
    }

    void WriterKafka::sendMessage(OutputBufferMsg *msg) {
        for(;;) {
            rd_kafka_resp_err_t err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic.c_str()), RD_KAFKA_V_VALUE(msg->data, msg->length),
                    RD_KAFKA_V_OPAQUE(msg), RD_KAFKA_V_END);
            //rd_kafka_resp_err_t err = (rd_kafka_resp_err_t)rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, 0, msg->data, msg->length, nullptr, 0, msg);

            if (err) {
                WARNING("Failed to produce to topic " << topic.c_str() << ", message: " << rd_kafka_err2str(err));

                if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    WARNING("Queue, full, sleeping " << (pollInterval / 1000) << "ms, then retrying");
                    rd_kafka_poll(rk, pollInterval / 1000);
                    continue;
                } else {
                    WARNING("OTHER ERROR?");
                    break;
                }
            } else
                break;
        }

        rd_kafka_poll(rk, 0);
    }

    string WriterKafka::getName() const {
        return "Kafka:" + topic;
    }

    void WriterKafka::pollQueue(void) {
        if (tmpQueueSize > 0)
            rd_kafka_poll(rk, 0);
    }
}
