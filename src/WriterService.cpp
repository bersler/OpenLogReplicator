/* Thread writing to GRPC
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

#include <chrono>

#include "OracleAnalyzer.h"
#include "WriterService.h"

using namespace std;

extern void stopMain();

namespace OpenLogReplicator {

    WriterService::WriterService(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
            uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
            uint64_t startTimeRel) :
        Writer(alias, oracleAnalyzer, 0, pollInterval, checkpointInterval, queueSize, startScn, startSeq, startTime, startTimeRel),
        uri(uri),
        started(false) {
        alwaysPoll = true;

        builder.AddListeningPort(uri, InsecureServerCredentials());
        service.reset(new pb::RedoStreamService::AsyncService);
        builder.RegisterService(service.get());
        cq = builder.AddCompletionQueue();
        server = builder.BuildAndStart();

        context.reset(new ServerContext);
        stream.reset(new ServerAsyncReaderWriter<pb::Response, pb::Request>(context.get()));
        service->RequestRedoStream(context.get(), stream.get(), cq.get(), cq.get(), SERVICE_CONNECT);
        context->AsyncNotifyWhenDone((void*)SERVICE_DISCONNECT);
    }

    WriterService::~WriterService() {
        context.release();
        stream.release();
        service.release();
        server->Shutdown();
        cq->Shutdown();

        //purge queue
        void* ignored_tag;
        bool ignored_ok;
        while (cq->Next(&ignored_tag, &ignored_ok)) {}
    }

    void WriterService::sendMessage(OutputBufferMsg *msg) {
        //response.Clear();
        //response.set_response_code(pb::ResponseCode::PAYLOAD);
        //stream->Write(response, (void*)SERVICE_WRITE);
    }

    string WriterService::getName() {
        return "Service:" + uri;
    }

    void WriterService::pollQueue(void) {
        void* tag;
        bool ok;
        switch (cq->AsyncNext(&tag, &ok, chrono::system_clock::now() + chrono::nanoseconds(pollInterval))) {
        case CompletionQueue::SHUTDOWN:
            stopMain();
            break;

        case CompletionQueue::GOT_EVENT:
            if (ok)
                switch ((uint64_t)tag) {
                    case SERVICE_CONNECT:
                        stream->Read(&request, (void*)SERVICE_READ);
                        break;

                    case SERVICE_DISCONNECT:
                        context.reset(new ServerContext);
                        stream.reset(new ServerAsyncReaderWriter<pb::Response, pb::Request>(context.get()));
                        service->RequestRedoStream(context.get(), stream.get(), cq.get(), cq.get(), (void*)SERVICE_CONNECT);
                        context->AsyncNotifyWhenDone((void*)SERVICE_DISCONNECT);
                        break;

                    case SERVICE_READ:
                        response.Clear();
                        switch (request.request_code()) {
                            case pb::RequestCode::INITIALIZE:
                                processInitialize();
                                break;

                            case pb::RequestCode::START:
                                if (request.database_name().compare(oracleAnalyzer->database) != 0) {
                                    response.set_response_code(pb::ResponseCode::INVALID_DATABASE);
                                } else {
                                    switch (request.tm_val_case()) {
                                        case pb::Request::TmValCase::kScn:
                                            startScn = request.scn();
                                            started = true;
                                            response.set_response_code(pb::ResponseCode::STARTED);
                                            break;

                                        case pb::Request::TmValCase::kSeq:
                                            startSequence = request.seq();
                                            started = true;
                                            response.set_response_code(pb::ResponseCode::STARTED);
                                            break;

                                        case pb::Request::TmValCase::kTms:
                                            startTime = request.tms();
                                            started = true;
                                            response.set_response_code(pb::ResponseCode::STARTED);
                                            break;

                                        case pb::Request::TmValCase::kTmRel:
                                            startTimeRel = request.tm_rel();
                                            started = true;
                                            response.set_response_code(pb::ResponseCode::STARTED);
                                            break;

                                        default:
                                            response.set_response_code(pb::ResponseCode::INVALID_COMMAND);
                                            break;
                                    }
                                }
                                break;

                            case pb::RequestCode::CONFIRM:
                                processConfirm(request.scn());
                                break;

                            default:
                                response.set_response_code(pb::ResponseCode::INVALID_COMMAND);
                                break;
                        }
                        stream->Write(response, (void*)SERVICE_WRITE);
                        break;

                    case SERVICE_WRITE:
                        stream->Read(&request, (void*)SERVICE_READ);
                        break;
                }
            break;

        case CompletionQueue::TIMEOUT:
            break;
        }
    }

    void WriterService::processInitialize(void) {
        if (started) {
            response.set_response_code(pb::ResponseCode::STARTED);
            response.set_scn(oracleAnalyzer->startScn);
        } else {
            response.set_response_code(pb::ResponseCode::READY);
        }
    }

    void WriterService::processConfirm(typescn scn) {
        cerr << "confirm" << endl;
    }

    void WriterService::readCheckpoint(void) {
        while (!shutdown && !started)
            pollQueue();
    }
}
