/* Thread writing directly to RocketMQ stream
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
#include "WriterRocketMQ.h"

namespace OpenLogReplicator {
    WriterRocketMQ* staticWriter;

    WriterRocketMQ::WriterRocketMQ(const char* alias, OracleAnalyzer* oracleAnalyzer, uint64_t pollIntervalUs, uint64_t checkpointIntervalS,
            uint64_t queueSize, typeSCN startScn, typeSEQ startSequence, const char* startTime, uint64_t startTimeRel,
            const char* groupId, const char *address, const char *domain, const char *topic, const char *tags, const char *keys) :
        Writer(alias, oracleAnalyzer, 1048576, pollIntervalUs, checkpointIntervalS, queueSize, startScn, startSequence, startTime,
                startTimeRel),
        producer(nullptr),
        message(nullptr),
        groupId(groupId),
        address(address),
        domain(domain),
        topic(topic),
        tags(tags),
        keys(keys) {
    }

    WriterRocketMQ::~WriterRocketMQ() {
        if (message != nullptr) {
            DestroyMessage(message);
            message = nullptr;
        }

        if (producer != nullptr) {
            int err = ShutdownProducer(producer);
            DestroyProducer(producer);
            producer = nullptr;

            INFO("RocketMQ producer exit code: " << std::dec << err);
        }
    }

    void WriterRocketMQ::initialize(void) {
        Writer::initialize();

        staticWriter = this;
        producer = CreateProducer(this->groupId.c_str());
        if (producer == nullptr) {
            CONFIG_FAIL("RocketMQ producer create failed with group-id: " << this->groupId);
        }

        message = CreateMessage(this->topic.c_str());
        if (message == nullptr) {
            CONFIG_FAIL("RocketMQ message create failed with with topic: " << this->topic);
        }

        if (this->tags.length() > 0)
            SetMessageTags(message, this->tags.c_str());
        if (this->keys.length() > 0)
            SetMessageKeys(message, this->keys.c_str());

        if (this->address.length() > 0)
            SetProducerNameServerAddress(producer, this->address.c_str());
        else
            SetProducerNameServerDomain(producer, this->domain.c_str());

        //? SetProducerSendMsgTimeout(producer, 3);
        StartProducer(producer);
    }

    void WriterRocketMQ::success_cb(CSendResult result) {
        TRACE(TRACE2_WRITER, "WRITER: async send success, msgid: " << result.msgId);
        INFO("MSG confirmed: " << result.msgId);
        staticWriter->confirmMessage(nullptr);
    }

    void WriterRocketMQ::exception_cb(CMQException e) {
        WARNING("RocketMQ exception: (error: " << e.error << " msg: " << e.msg << " file: " << e.file << " line: " << e.line);
    }

    void WriterRocketMQ::sendMessage(OutputBufferMsg* msg) {
        int ret = 0;
        SetByteMessageBody(message, (const char*)msg->data, msg->length);
        ret = SendMessageAsync(producer, message, success_cb, exception_cb);
        if (ret != 0) {
            WARNING("RocketMQ send message returned: " << std::dec << ret);
        } else {
            TRACE(TRACE2_WRITER, "WRITER: async send message return code: " << std::dec << ret);
        }
        INFO("MSG sent: " << std::dec << msg->length << " bytes");
    }

    std::string WriterRocketMQ::getName() const {
        return "RocketMQ:" + topic;
    }

    void WriterRocketMQ::pollQueue(void) {
    }
}
