/* Thread writing directly to Kafka stream
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../builder/Builder.h"
#include "../common/exception/ConfigurationException.h"
#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "WriterKafka.h"

namespace OpenLogReplicator {
    WriterKafka::WriterKafka(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, std::string newTopic):
            Writer(newCtx, std::move(newAlias), std::move(newDatabase), newBuilder, newMetadata),
            topic(std::move(newTopic)) {
        errStr[0] = 0;
    }

    WriterKafka::~WriterKafka() {
        if (conf != nullptr)
            rd_kafka_conf_destroy(conf);

        if (rkt != nullptr)
            rd_kafka_topic_destroy(rkt);

        const rd_kafka_resp_err_t err = rd_kafka_fatal_error(rk, nullptr, 0);
        if (rk != nullptr)
            rd_kafka_destroy(rk);

        ctx->info(0, "Kafka producer exit code: " + std::to_string(err));
    }

    void WriterKafka::addProperty(std::string key, std::string value) {
        if (properties.find(key) != properties.end())
            throw ConfigurationException(30009, "Kafka property '" + key + "' is defined multiple times");
        properties.insert_or_assign(std::move(key), std::move(value));
    }

    void WriterKafka::initialize() {
        Writer::initialize();

        if (properties.find("message.max.bytes") != properties.end())
            throw ConfigurationException(30010, "Kafka property 'message.max.bytes' is defined, but it is not allowed to be set by user");

        conf = rd_kafka_conf_new();
        if (conf == nullptr)
            throw RuntimeException(10058, "Kafka failed to create configuration");

        const std::string maxMessageMbStr(std::to_string(builder->getMaxMessageMb() * 1024 * 1024));
        properties.insert_or_assign("message.max.bytes", maxMessageMbStr);

        if (properties.find("client.id") != properties.end())
            properties.insert_or_assign("client.id", "OpenLogReplicator");

        if (properties.find("group.id") != properties.end())
            properties.insert_or_assign("group.id", "OpenLogReplicator");

        for (const auto& [name, value]: properties)
            if (rd_kafka_conf_set(conf, name.c_str(), value.c_str(), errStr, sizeof(errStr)) != RD_KAFKA_CONF_OK)
                throw RuntimeException(10059, "Kafka message: " + std::string(errStr));

        rd_kafka_conf_set_opaque(conf, this);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_error_cb(conf, error_cb);
        rd_kafka_conf_set_log_cb(conf, logger_cb);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errStr, sizeof(errStr));
        if (rk == nullptr)
            throw RuntimeException(10060, "Kafka failed to create producer, message: " + std::string(errStr));
        conf = nullptr;

        rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
        streaming = true;
    }

    void WriterKafka::dr_msg_cb(rd_kafka_t * rkCb __attribute__((unused)), const rd_kafka_message_t * rkMessage, void*opaque __attribute__((unused))) {
        auto* msg = static_cast<BuilderMsg*>(rkMessage->_private);
        auto* writer = static_cast<Writer*>(opaque);
        if (rkMessage->err != 0) {
            writer->ctx->warning(70008, "Kafka: " + std::to_string(msg->id) + " delivery failed: " + rd_kafka_err2str(rkMessage->err));
        } else {
            writer->confirmMessage(msg);
        }
    }

    void WriterKafka::error_cb(rd_kafka_t* rkCb, int err, const char* reason, void* opaque) {
        const auto* writer = static_cast<Writer*>(opaque);

        writer->ctx->warning(70009, "Kafka: " + std::string(rd_kafka_err2name(static_cast<rd_kafka_resp_err_t>(err))) +
                             ", reason: " + reason);

        if (err != RD_KAFKA_RESP_ERR__FATAL)
            return;

        std::array<char, 512> errStrCb {};
        const rd_kafka_resp_err_t orig_err = rd_kafka_fatal_error(rkCb, errStrCb.data(), errStrCb.size());
        writer->ctx->error(10057, "Kafka: fatal error: " + std::string(rd_kafka_err2name(orig_err)) + ", reason: " + errStrCb.data());

        writer->ctx->stopHard();
    }

    void WriterKafka::logger_cb(const rd_kafka_t* rkCb, int level, const char* fac, const char* buf) {
        auto* writer = static_cast<WriterKafka*>(rd_kafka_opaque(rkCb));
        if (unlikely(writer->ctx->isTraceSet(Ctx::TRACE::WRITER)))
            writer->ctx->logTrace(Ctx::TRACE::WRITER, std::to_string(level) + ", rk: " + ((rkCb != nullptr) ? rd_kafka_name(rkCb) : nullptr) +
                                  ", fac: " + fac + ", err: " + buf);
    }

    void WriterKafka::sendMessage(BuilderMsg* msg) {
        msg->ptr = reinterpret_cast<void*>(this);
        for (;;) {
            rd_kafka_resp_err_t err;
            if (msg->tagSize > 0)
                err = rd_kafka_producev(rk,
                                        RD_KAFKA_VTYPE_TOPIC, topic.c_str(),
                                        RD_KAFKA_VTYPE_KEY, reinterpret_cast<void*>(msg->data), static_cast<size_t>(msg->tagSize),
                                        RD_KAFKA_VTYPE_VALUE, reinterpret_cast<void*>(msg->data + msg->tagSize), static_cast<size_t>(msg->size - msg->tagSize),
                                        RD_KAFKA_VTYPE_OPAQUE, reinterpret_cast<void*>(msg),
                                        RD_KAFKA_VTYPE_END);
            else
                err = rd_kafka_producev(rk,
                                        RD_KAFKA_VTYPE_TOPIC, topic.c_str(),
                                        RD_KAFKA_VTYPE_VALUE, reinterpret_cast<void*>(msg->data), static_cast<size_t>(msg->size),
                                        RD_KAFKA_VTYPE_OPAQUE, reinterpret_cast<void*>(msg),
                                        RD_KAFKA_VTYPE_END);

            if (err != 0) {
                ctx->warning(60031, "failed to produce to topic " + topic + ", message: " + rd_kafka_err2str(err));

                if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    ctx->warning(60031, "queue, full, sleeping " + std::to_string(ctx->pollIntervalUs / 1000) + " ms, then retrying");
                    rd_kafka_poll(rk, static_cast<int>((ctx->pollIntervalUs / 1000)));
                    continue;
                }
            }
            break;
        }

        rd_kafka_poll(rk, 0);
    }

    std::string WriterKafka::getType() const {
        return "Kafka:" + topic;
    }

    void WriterKafka::pollQueue() {
        if (metadata->status == Metadata::STATUS::READY)
            metadata->setStatusStarting(this);

        if (currentQueueSize > 0)
            rd_kafka_poll(rk, 0);
    }
}
