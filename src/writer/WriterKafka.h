/* Header for WriterKafka class
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <map>
#include "Writer.h"

#ifndef WRITER_KAFKA_H_
#define WRITER_KAFKA_H_

namespace OpenLogReplicator {
    class WriterKafka final : public Writer {
    protected:
        std::string topic;
        char errStr[512];
        std::map<std::string, std::string> properties;
        rd_kafka_t* rk;
        rd_kafka_topic_t* rkt;
        rd_kafka_conf_t* conf;
        static void dr_msg_cb(rd_kafka_t* rkCb, const rd_kafka_message_t* rkMessage, void* opaque);
        static void error_cb(rd_kafka_t* rkCb, int err, const char* reason, void* opaque);
        static void logger_cb(const rd_kafka_t* rkCb, int level, const char* fac, const char* buf);

        void sendMessage(BuilderMsg* msg) override;
        std::string getName() const override;
        void pollQueue() override;

    public:
        static constexpr uint64_t MAX_KAFKA_MESSAGE_MB = 953;

        WriterKafka(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata,
                    const char* newTopic);
        ~WriterKafka() override;

        void addProperty(const std::string& key, const std::string& value);
        void initialize() override;
    };
}

#endif
