/* Header for WriterRocketMQ class
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

#include <CCommon.h>
#include <CMessage.h>
#include <CProducer.h>
#include <CSendResult.h>

#include "Writer.h"

#ifndef WRITERROCKETMQ_H_
#define WRITERROCKETMQ_H_

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzer;

    class WriterRocketMQ : public Writer {
    protected:
        CProducer* producer;
        CMessage* message;
        string groupId;
        string address;
        string domain;
        string topic;
        string tags;
        string keys;

        static void success_cb(CSendResult result);
        static void exception_cb(CMQException e);
        virtual void sendMessage(OutputBufferMsg* msg);
        virtual string getName() const;
        virtual void pollQueue(void);

    public:
        WriterRocketMQ(const char* alias, OracleAnalyzer* oracleAnalyzer, uint64_t pollIntervalUs, uint64_t checkpointIntervalS,
                uint64_t queueSize, typeSCN startScn, typeSEQ startSequence, const char* startTime, uint64_t startTimeRel,
                const char *groupId, const char *address, const char *domain, const char *topic, const char *tags, const char *keys);
        virtual ~WriterRocketMQ();

        virtual void initialize(void);
    };
}

#endif
