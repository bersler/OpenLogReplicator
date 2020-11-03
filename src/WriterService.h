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

#define STATE_WAITING_FOR_CLIENT    0
#define STATE_CLIENT_CONNECTED      1
#define STATE_WAITING_FOR_RPC       2
#define STATE_STREAMING             3

#define SERVICE_DISCONNECT          0
#define SERVICE_REDO                1
#define SERVICE_REDO_READ           2
#define SERVICE_REDO_WRITE          3

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
        pb::OpenLogReplicator::AsyncService service;
        unique_ptr<Server> server;
        unique_ptr<ServerContext> context;
        unique_ptr<ServerAsyncReaderWriter<pb::RedoResponse, pb::RedoRequest>> stream;
        pb::RedoRequest request;
        pb::RedoResponse response;

        virtual void sendMessage(OutputBufferMsg *msg);
        virtual string getName();
        virtual void pollQueue(void);
        void processConfirm(typescn scn);
        virtual void readCheckpoint(void);
        void info(void);
        void start(void);
        //void processRedo(void);
        bool getEvent(bool &ok, void *&tag);
        void writeResponse(void);

    public:
        WriterService(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
                uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
                uint64_t startTimeRel);
        virtual ~WriterService();
    };
}

#endif
