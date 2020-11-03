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
#include <unistd.h>

#include "OracleAnalyzer.h"
#include "OutputBuffer.h"
#include "RuntimeException.h"
#include "WriterService.h"

using namespace std;

namespace OpenLogReplicator {

    WriterService::WriterService(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
            uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
            uint64_t startTimeRel) :
        Writer(alias, oracleAnalyzer, 0, pollInterval, checkpointInterval, queueSize, startScn, startSeq, startTime, startTimeRel),
        uri(uri) {

        builder.AddListeningPort(uri, InsecureServerCredentials());
        //service.reset(new pb::OpenLogReplicator::AsyncService);
        builder.RegisterService(&service);
        cq = builder.AddCompletionQueue();
        server = builder.BuildAndStart();
    }

    WriterService::~WriterService() {
        server->Shutdown(std::chrono::system_clock::now() + std::chrono::nanoseconds(pollInterval));
        cq->Shutdown();

        void* ignored_tag;
        bool ignored_ok;
        while (cq->Next(&ignored_tag, &ignored_ok))
            ;

        stream.release();
        context.release();
        server.release();
        cq.release();
    }

    void WriterService::sendMessage(OutputBufferMsg *msg) {
        cerr << "send response: " << msg->length << endl;
        response.Clear();
        response.set_code(pb::ResponseCode::PAYLOAD);
        response.set_scn(msg->scn);
        response.add_payload();
        pb::Payload *payload = response.mutable_payload(response.payload_size() - 1);
        payload->ParseFromArray(msg->data, msg->length);
        writeResponse();
    }

    string WriterService::getName() {
        return "Service:" + uri;
    }

    void WriterService::pollQueue(void) {
    }

    void WriterService::processConfirm(typescn scn) {
        cerr << "confirm" << endl;
    }

    void WriterService::readCheckpoint(void) {
        bool ok;
        void *tag;

        while (oracleAnalyzer->scn == ZERO_SCN) {
            context.reset(new ServerContext());
            stream.reset(new ServerAsyncReaderWriter<pb::RedoResponse, pb::RedoRequest>(context.get()));
            service.RequestRedo(context.get(), stream.get(), cq.get(), cq.get(), (void*)SERVICE_REDO);
            context->AsyncNotifyWhenDone((void*)SERVICE_DISCONNECT);

            if (!getEvent(ok, tag) && shutdown)
                return;

            if ((uint64_t)tag != SERVICE_REDO) {
                RUNTIME_FAIL("GRPC service error")
            }

            cerr << "event: tag: " << (uint64_t) tag << " ok: " << ok << endl;
            while (!shutdown) {
                stream->Read(&request, (void*)SERVICE_REDO_READ);

                if (!getEvent(ok, tag) && shutdown)
                    return;

                if ((uint64_t)tag == SERVICE_DISCONNECT) {
                    if (!getEvent(ok, tag) && shutdown)
                        return;

                    if ((uint64_t)tag != SERVICE_REDO_READ) {
                        RUNTIME_FAIL("GRPC read error")
                    }

                    break;
                } else
                if ((uint64_t)tag != SERVICE_REDO_READ) {
                    RUNTIME_FAIL("GRPC read error")
                }

                if (ok) {
                    response.Clear();
                    if (request.code() == pb::RequestCode::INFO)
                        info();
                    else
                    if (request.code() == pb::RequestCode::START)
                        start();
                    else
                        response.set_code(pb::ResponseCode::INVALID_COMMAND);
                    writeResponse();
                }

                if (shutdown)
                    return;
            }
        }

        if (oracleAnalyzer->scn != ZERO_SCN)
            INFO("checkpoint - client requested scn: " << dec << startScn);
    }

    void WriterService::info(void) {
        if (request.database_name().compare(oracleAnalyzer->database) != 0) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (oracleAnalyzer->scn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::STARTED);
            response.set_scn(oracleAnalyzer->scn);
        } else {
            response.set_code(pb::ResponseCode::READY);
        }
    }

    void WriterService::start(void) {
        if (request.database_name().compare(oracleAnalyzer->database) != 0) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (oracleAnalyzer->scn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::ALREADY_STARTED);
            response.set_scn(oracleAnalyzer->scn);
        } else {
            startScn = 0;
            startSequence = 0;
            startTime.clear();
            startTimeRel = 0;

            switch (request.tm_val_case()) {
                case pb::RedoRequest::TmValCase::kScn:
                    startScn = request.scn();
                    startReader();
                    break;

                case pb::RedoRequest::TmValCase::kSeq:
                    startSequence = request.seq();
                    startReader();
                    break;

                case pb::RedoRequest::TmValCase::kTms:
                    startTime = request.tms();
                    startReader();
                    break;

                case pb::RedoRequest::TmValCase::kTmRel:
                    startTimeRel = request.tm_rel();
                    startReader();
                    break;

                default:
                    response.set_code(pb::ResponseCode::INVALID_COMMAND);
                    break;
            }

            if (oracleAnalyzer->scn != ZERO_SCN) {
                response.set_code(pb::ResponseCode::STARTED);
                response.set_scn(oracleAnalyzer->scn);
            } else {
                response.set_code(pb::ResponseCode::FAILED_START);
            }
        }
    }

    void WriterService::writeResponse(void) {
        bool ok;
        void *tag;
        stream->Write(response, (void*)SERVICE_REDO_WRITE);

        if (!getEvent(ok, tag) && shutdown)
            return;

        if ((uint64_t)tag == SERVICE_DISCONNECT) {
            if (!getEvent(ok, tag) && shutdown)
                return;

            if ((uint64_t)tag != SERVICE_REDO_WRITE) {
                RUNTIME_FAIL("GRPC write error")
            }

            return;
        } else
        if ((uint64_t)tag != SERVICE_REDO_WRITE) {
            RUNTIME_FAIL("GRPC write error")
        }
    }

    bool WriterService::getEvent(bool &ok, void *&tag) {
        while (!shutdown) {
            switch (cq->AsyncNext(&tag, &ok, chrono::system_clock::now() + chrono::nanoseconds(pollInterval))) {
                case CompletionQueue::SHUTDOWN:
                    {
                        RUNTIME_FAIL("GRPC shut down")
                    }
                    break;

                case CompletionQueue::GOT_EVENT:
                    return true;

                case CompletionQueue::TIMEOUT:
                    continue;
            }
        }
        return false;
    }
}
