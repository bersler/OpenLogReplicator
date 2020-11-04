/* Header for WriterGRPC class
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

#define STATE_STARTED               0
#define STATE_LISTENING             1
#define STATE_READING               2
#define STATE_WRITING               3
#define STATE_CONFIRMING            4

#define SERVICE_DISCONNECT          0
#define SERVICE_REDO                1
#define SERVICE_REDO_READ           2
#define SERVICE_REDO_WRITE          3

#ifndef WRITERGRPC_H_
#define WRITERGRPC_H_

using namespace std;
using namespace grpc;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class OracleAnalyzer;

    class WriterGRPC : public Writer {
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
        uint64_t state;
        OutputBufferMsg *msgToSend;
        uint64_t written;
        bool queuedRead;
        bool queuedWrite;
        int64_t msgRetry;

        virtual void sendMessage(OutputBufferMsg *msg);
        virtual string getName();
        virtual void pollQueue(void);
        virtual void readCheckpoint(void);
        void info(void);
        void start(void);
        void send(void);
        void invalid(void);
        void confirm(void);
        bool getEvent(bool &ok, void *&tag);
        void getEventLoop(bool &ok, void *&tag);

    public:
        WriterGRPC(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
                uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
                uint64_t startTimeRel);
        virtual ~WriterGRPC();
    };
}

#endif
