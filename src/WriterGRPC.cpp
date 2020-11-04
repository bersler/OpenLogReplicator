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
#include "WriterGRPC.h"

using namespace std;

namespace OpenLogReplicator {

    WriterGRPC::WriterGRPC(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
            uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
            uint64_t startTimeRel) :
        Writer(alias, oracleAnalyzer, 0, pollInterval, checkpointInterval, queueSize, startScn, startSeq, startTime, startTimeRel),
        uri(uri),
        state(STATE_STARTED),
        msgToSend(nullptr),
        written(0),
        queuedRead(false),
        queuedWrite(false),
        msgRetry(-1) {

        builder.AddListeningPort(uri, InsecureServerCredentials());
        builder.RegisterService(&service);
        cq = builder.AddCompletionQueue();
        server = builder.BuildAndStart();
    }

    WriterGRPC::~WriterGRPC() {
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

    void WriterGRPC::sendMessage(OutputBufferMsg *msg) {
        msgToSend = msg;
        while (msgToSend != nullptr)
            pollQueue();
    }

    string WriterGRPC::getName() {
        return "GRPC:" + uri;
    }

    void WriterGRPC::pollQueue(void) {
        bool ok;
        void *tag;

        for (;;) {
            switch (state) {
                //reset
                case STATE_STARTED:
                    context.reset(new ServerContext());
                    stream.reset(new ServerAsyncReaderWriter<pb::RedoResponse, pb::RedoRequest>(context.get()));
                    service.RequestRedo(context.get(), stream.get(), cq.get(), cq.get(), (void*)SERVICE_REDO);
                    context->AsyncNotifyWhenDone((void*)SERVICE_DISCONNECT);
                    state = STATE_LISTENING;
                    sortQueue();
                    msgRetry = 0;
                    msgToSend = nullptr;
                    break;

                //waiting for client
                case STATE_LISTENING:
                    getEventLoop(ok, tag);
                    if (!ok)
                        break;

                    switch ((uint64_t)tag) {
                        case SERVICE_DISCONNECT:
                            state = STATE_STARTED;
                            while (queuedRead || queuedWrite)
                                getEventLoop(ok, tag);
                            break;

                        case SERVICE_REDO:
                            state = STATE_READING;
                            stream->Read(&request, (void*)SERVICE_REDO_READ);
                            queuedRead = true;
                            break;

                        default:
                            {
                                RUNTIME_FAIL("GRPC service unexpected message tag1: " << (uint64_t)tag);
                            }
                            break;
                    }
                    break;

                //waiting for command
                case STATE_READING:
                    getEventLoop(ok, tag);

                    switch ((uint64_t)tag) {
                        case SERVICE_DISCONNECT:
                            state = STATE_STARTED;
                            while (queuedRead || queuedWrite)
                                getEventLoop(ok, tag);
                            break;

                        case SERVICE_REDO_WRITE:
                            break;

                        case SERVICE_REDO_READ:
                            switch (request.code()) {
                                case pb::RequestCode::INFO:
                                    info();
                                    break;

                                case pb::RequestCode::START:
                                    start();
                                    break;

                                case pb::RequestCode::REDO:
                                    state = STATE_WRITING;
                                    break;

                                default:
                                    invalid();
                                    break;
                            }
                            stream->Read(&request, (void*)SERVICE_REDO_READ);
                            queuedRead = true;
                            break;
                    }

                    break;

                //streaming
                case STATE_WRITING:
                    //re-send after connection lost
                    if (msgRetry != -1) {
                        if (msgRetry == curQueueSize)
                            msgRetry = -1;
                        else
                            msgToSend = queue[msgRetry++];
                    }

                    //message sent
                    if (msgToSend == nullptr) {
                        if (getEvent(ok, tag) && ok) {
                            switch ((uint64_t)tag) {
                                case SERVICE_DISCONNECT:
                                    state = STATE_STARTED;
                                    while (queuedRead || queuedWrite)
                                        getEventLoop(ok, tag);
                                    break;

                                case SERVICE_REDO_READ:
                                    switch (request.code()) {
                                        case pb::RequestCode::CONFIRM:
                                            confirm();
                                            break;

                                        default:
                                            {
                                                WARNING("received code during streaming: " << dec << request.code() << ", ignoring");
                                            }
                                            break;
                                    }
                                    stream->Read(&request, (void*)SERVICE_REDO_READ);
                                    queuedRead = true;
                                    break;

                                default:
                                    {
                                        RUNTIME_FAIL("GRPC service unexpected message tag2: " << (uint64_t)tag);
                                    }
                                    break;
                            }
                            break;
                        }
                        return;
                    }
                    send();
                    state = STATE_CONFIRMING;
                    break;

                //send confirmation
                case STATE_CONFIRMING:
                    getEventLoop(ok, tag);

                    switch ((uint64_t)tag) {
                        case SERVICE_DISCONNECT:
                            state = STATE_STARTED;
                            while (queuedRead || queuedWrite)
                                getEventLoop(ok, tag);
                            break;

                        case SERVICE_REDO_WRITE:
                            ++written;
                            state = STATE_WRITING;
                            break;
                    }
                    break;
            }
        }
    }

    void WriterGRPC::readCheckpoint(void) {
        while (oracleAnalyzer->scn == ZERO_SCN) {
            pollQueue();
            if (shutdown)
                break;
        }

        if (oracleAnalyzer->scn != ZERO_SCN)
            FULL("client requested scn: " << dec << startScn);
    }

    void WriterGRPC::info(void) {
        response.Clear();
        if (request.database_name().compare(oracleAnalyzer->database) != 0) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (oracleAnalyzer->scn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::STARTED);
            response.set_scn(oracleAnalyzer->scn);
        } else {
            response.set_code(pb::ResponseCode::READY);
        }
        stream->Write(response, (void*)SERVICE_REDO_WRITE);
        queuedWrite = true;
    }

    void WriterGRPC::start(void) {
        response.Clear();
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
        stream->Write(response, (void*)SERVICE_REDO_WRITE);
        queuedWrite = true;
    }

    void WriterGRPC::invalid(void) {
        response.Clear();
        response.set_code(pb::ResponseCode::INVALID_COMMAND);
        stream->Write(response, (void*)SERVICE_REDO_WRITE);
        queuedWrite = true;
    }

    void WriterGRPC::confirm(void) {
        if (request.database_name().compare(oracleAnalyzer->database) == 0) {
            uint64_t oldSize = curQueueSize;
            while (curQueueSize > 0 && queue[0]->scn <= request.scn())
                confirmMessage(queue[0]);
        }
    }

    void WriterGRPC::send(void) {
        response.Clear();
        response.set_code(pb::ResponseCode::PAYLOAD);
        response.set_scn(msgToSend->scn);
        response.add_payload();
        pb::Payload *payload = response.mutable_payload(response.payload_size() - 1);
        payload->ParseFromArray(msgToSend->data, msgToSend->length);
        msgToSend = nullptr;
        stream->Write(response, (void*)SERVICE_REDO_WRITE);
        queuedWrite = true;
    }

    bool WriterGRPC::getEvent(bool &ok, void *&tag) {
        switch (cq->AsyncNext(&tag, &ok, chrono::system_clock::now())) {
            case CompletionQueue::SHUTDOWN:
                {
                    RUNTIME_FAIL("GRPC shut down");
                }
                break;

            case CompletionQueue::GOT_EVENT:
                if ((uint64_t)tag == SERVICE_REDO_READ)
                    queuedRead = false;
                else
                if ((uint64_t)tag == SERVICE_REDO_WRITE)
                    queuedWrite = false;
                return true;

            case CompletionQueue::TIMEOUT:
                break;
        }
        return false;
    }

    void WriterGRPC::getEventLoop(bool &ok, void *&tag) {
        while (!shutdown) {
            switch (cq->AsyncNext(&tag, &ok, chrono::system_clock::now() + chrono::nanoseconds(pollInterval))) {
                case CompletionQueue::SHUTDOWN:
                    {
                        RUNTIME_FAIL("GRPC shut down");
                    }
                    break;

                case CompletionQueue::GOT_EVENT:
                    if ((uint64_t)tag == SERVICE_REDO_READ)
                        queuedRead = false;
                    else
                    if ((uint64_t)tag == SERVICE_REDO_WRITE)
                        queuedWrite = false;
                    return;

                case CompletionQueue::TIMEOUT:
                    continue;
            }
        }
        RUNTIME_FAIL("stopping writer");
    }
}
