/* Thread writing to Network
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/NetworkException.h"
#include "../common/OraProtoBuf.pb.h"
#include "../common/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../stream/Stream.h"
#include "WriterStream.h"

namespace OpenLogReplicator {
    WriterStream::WriterStream(Ctx* newCtx, std::string newAlias, std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata, Stream* newStream) :
        Writer(newCtx, newAlias, newDatabase, newBuilder, newMetadata),
        stream(newStream) {
    }

    WriterStream::~WriterStream() {
        if (stream != nullptr) {
            delete stream;
            stream = nullptr;
        }
    }

    void WriterStream::initialize() {
        Writer::initialize();

        stream->initializeServer();
    }

    std::string WriterStream::getName() const {
        return stream->getName();
    }

    void WriterStream::readCheckpoint() {
        while (!streaming && !ctx->softShutdown) {
            try {
                usleep(ctx->pollIntervalUs);
                if (stream->isConnected()) {
                    pollQueue();
                }
            } catch (NetworkException& ex) {
                // Client got disconnected
                streaming = false;
            }
        }

        if (metadata->firstDataScn != ZERO_SCN) {
            DEBUG("client requested scn: " << std::dec << metadata->firstDataScn)
        }
    }

    void WriterStream::processInfo() {
        response.Clear();
        if (request.database_name() != database) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (metadata->firstDataScn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::STARTED);
            response.set_scn(metadata->firstDataScn);
        } else {
            response.set_code(pb::ResponseCode::READY);
        }
    }

    void WriterStream::processStart() {
        response.Clear();
        if (request.database_name() != database) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (metadata->firstDataScn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::ALREADY_STARTED);
            response.set_scn(metadata->firstDataScn);
        } else {
            metadata->startScn = 0;
            if (request.has_seq())
                metadata->startSequence = request.seq();
            else
                metadata->startSequence = ZERO_SEQ;
            metadata->startTime.clear();
            metadata->startTimeRel = 0;

            switch (request.tm_val_case()) {
                case pb::RedoRequest::TmValCase::kScn:
                    metadata->startScn = request.scn();
                    metadata->setStatusReplicate();
                    break;

                case pb::RedoRequest::TmValCase::kTms:
                    metadata->startTime = request.tms();
                    metadata->setStatusReplicate();
                    break;

                case pb::RedoRequest::TmValCase::kTmRel:
                    metadata->startTimeRel = request.tm_rel();
                    metadata->setStatusReplicate();
                    break;

                default:
                    response.set_code(pb::ResponseCode::INVALID_COMMAND);
                    break;
            }

            if (metadata->firstDataScn != ZERO_SCN) {
                response.set_code(pb::ResponseCode::STARTED);
                response.set_scn(metadata->firstDataScn);
            } else {
                response.set_code(pb::ResponseCode::FAILED_START);
            }
        }
    }

    void WriterStream::processRedo() {
        response.Clear();
        if (request.database_name() == database) {
            response.set_code(pb::ResponseCode::STREAMING);
            INFO("streaming to client")
            streaming = true;
        } else {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        }
    }

    void WriterStream::processConfirm() {
        if (request.database_name() == database) {
            while (tmpQueueSize > 0 && queue[0]->scn <= request.scn())
                confirmMessage(queue[0]);
        }
    }

    void WriterStream::pollQueue() {
        uint8_t msgR[READ_NETWORK_BUFFER];
        std::string msgS;

        uint64_t length = stream->receiveMessageNB(msgR, READ_NETWORK_BUFFER);

        if (length > 0) {
            request.Clear();
            if (request.ParseFromArray(msgR, (int)length)) {
                if (streaming) {
                    switch (request.code()) {
                        case pb::RequestCode::INFO:
                            processInfo();
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            streaming = false;
                            break;

                        case pb::RequestCode::CONFIRM:
                            processConfirm();
                            break;

                        default:
                            WARNING("unknown request code: " << request.code())
                            response.Clear();
                            response.set_code(pb::ResponseCode::INVALID_COMMAND);
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;
                    }
                } else {
                    switch (request.code()) {

                        case pb::RequestCode::INFO:
                            processInfo();
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;

                        case pb::RequestCode::START:
                            processStart();
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;

                        case pb::RequestCode::REDO:
                            processRedo();
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;

                        default:
                            WARNING("unknown request code: " << request.code())
                            response.Clear();
                            response.set_code(pb::ResponseCode::INVALID_COMMAND);
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;
                    }
                }
            } else {
                std::stringstream ss;
                ss << "request decoder[" << std::dec << length << "]: ";
                for (uint64_t i = 0; i < (uint64_t)length; ++i)
                    ss << std::hex  << std::setw(2) << std::setfill('0') << (uint64_t)msgR[i] << " ";
                WARNING(ss.str())
            }

        } else if (length == 0) {
            // No request
        } else if (errno != EAGAIN)
            throw RuntimeException("socket error: (ret: "+ std::to_string(length) + " errno: " + std::to_string(errno) + ")");
    }

    void WriterStream::sendMessage(BuilderMsg* msg) {
        stream->sendMessage(msg->data, msg->length);
    }
}
