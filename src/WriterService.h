/* Header for WriterService class
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

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include "types.h"
#include "OraProtoBuf.pb.h"
#include "OraProtoBuf.grpc.pb.h"
#include "Writer.h"

#define SERVICE_CONNECT             0
#define SERVICE_READ                1
#define SERVICE_WRITE               2
#define SERVICE_DISCONNECT          3

#ifndef WRITERSERVICE_H_
#define WRITERSERVICE_H_

using namespace std;
using namespace grpc;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class OracleAnalyzer;

    class WriterService : public Writer {
    protected:
        string uri;
        ServerBuilder builder;
        unique_ptr<ServerCompletionQueue> cq;
        unique_ptr<pb::RedoStreamService::AsyncService> service;
        unique_ptr<Server> server;
        unique_ptr<ServerContext> context;
        unique_ptr<ServerAsyncReaderWriter<pb::Response, pb::Request>> stream;
        pb::Request request;
        pb::Response response;
        bool started;

        virtual void sendMessage(OutputBufferMsg *msg);
        virtual string getName();
        virtual void pollQueue(void);
        void processInitialize(void);
        void processConfirm(typescn scn);
        virtual void readCheckpoint(void);

    public:
        WriterService(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
                uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
                uint64_t startTimeRel);
        virtual ~WriterService();
    };
}

#endif
