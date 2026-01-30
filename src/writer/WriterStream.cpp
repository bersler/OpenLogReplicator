/* Thread writing to Network
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../builder/Builder.h"
#include "../common/OraProtoBuf.pb.h"
#include "../common/exception/NetworkException.h"
#include "../metadata/Metadata.h"
#include "../stream/Stream.h"
#include "WriterStream.h"

namespace OpenLogReplicator {
    WriterStream::WriterStream(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, Stream* newStream):
            Writer(newCtx, std::move(newAlias), std::move(newDatabase), newBuilder, newMetadata),
            stream(newStream) {
        metadata->bootFailsafe = true;
        ctx->parserThread = this;
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

    std::string WriterStream::getType() const {
        return stream->getName();
    }

    void WriterStream::processInfo() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::STREAM)))
            ctx->logTrace(Ctx::TRACE::STREAM, "request: INFO: " + request.database_name());

        response.Clear();
        if (request.database_name() != database) {
            ctx->warning(60035, "unknown database requested, got: " + request.database_name() + ", expected: " + database);
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
            return;
        }

        if (metadata->status == Metadata::STATUS::READY) {
            ctx->logTrace(Ctx::TRACE::WRITER, "info, ready");
            response.set_code(pb::ResponseCode::READY);
            return;
        }

        if (metadata->status == Metadata::STATUS::STARTING) {
            ctx->logTrace(Ctx::TRACE::WRITER, "info, start");
            response.set_code(pb::ResponseCode::STARTING);
        }

        ctx->logTrace(Ctx::TRACE::WRITER, "info, first scn: " + metadata->firstDataScn.toString());
        response.set_code(pb::ResponseCode::REPLICATE);
        response.set_scn(metadata->firstDataScn.getData());
        response.set_c_scn(confirmedScn.getData());
        response.set_c_idx(confirmedIdx);
    }

    void WriterStream::processStart() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::STREAM)))
            ctx->logTrace(Ctx::TRACE::STREAM, "request: START: " + request.database_name() +
                ", tm_val_case: " + std::to_string(request.tm_val_case()) +
                (request.has_scn() ? ", scn: " + std::to_string(request.scn()) : "") +
                (request.has_tms() ? ", tms: " + request.tms() : "") +
                (request.has_tm_rel() ? ", tm_rel: " + std::to_string(request.tm_rel()) : ""));

        response.Clear();
        if (request.database_name() != database) {
            ctx->warning(60035, "unknown database requested, got: " + request.database_name() + ", expected: " + database);
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
            return;
        }

        if (metadata->status == Metadata::STATUS::REPLICATING) {
            ctx->logTrace(Ctx::TRACE::WRITER, "client requested start when already started");
            response.set_code(pb::ResponseCode::ALREADY_STARTED);
            response.set_scn(metadata->firstDataScn.getData());
            response.set_c_scn(confirmedScn.getData());
            response.set_c_idx(confirmedIdx);
            return;
        }

        if (metadata->status == Metadata::STATUS::STARTING) {
            ctx->logTrace(Ctx::TRACE::WRITER, "client requested start when already starting");
            response.set_code(pb::ResponseCode::STARTING);
            return;
        }

        std::string paramSeq;
        if (request.has_seq()) {
            metadata->startSequence = request.seq();
            paramSeq = ", seq: " + std::to_string(request.seq());
        } else
            metadata->startSequence = Seq::none();

        metadata->startScn = Scn::none();
        metadata->startTime = "";
        metadata->startTimeRel = 0;

        switch (request.tm_val_case()) {
            case pb::RedoRequest::TmValCase::kScn:
                metadata->startScn = request.scn();
                if (metadata->startScn == Scn::none())
                    ctx->info(0, "client requested to start from NOW" + paramSeq);
                else
                    ctx->info(0, "client requested to start from scn: " + metadata->startScn.toString() + paramSeq);
                break;

            case pb::RedoRequest::TmValCase::kTms:
                metadata->startTime = request.tms();
                ctx->info(0, "client requested to start from time: " + metadata->startTime + paramSeq);
                break;

            case pb::RedoRequest::TmValCase::kTmRel:
                metadata->startTimeRel = request.tm_rel();
                ctx->info(0, "client requested to start from relative time: " + std::to_string(metadata->startTimeRel) + paramSeq);
                break;

            default:
                ctx->logTrace(Ctx::TRACE::WRITER, "client requested an invalid starting point");
                response.set_code(pb::ResponseCode::INVALID_COMMAND);
                return;
        }
        metadata->setStatusStarting(this);

        contextSet(CONTEXT::SLEEP);
        metadata->waitForReplicator(this);

        if (metadata->status == Metadata::STATUS::REPLICATING) {
            response.set_code(pb::ResponseCode::REPLICATE);
            response.set_scn(metadata->firstDataScn.getData());
            response.set_c_scn(confirmedScn.getData());
            response.set_c_idx(confirmedIdx);

            ctx->info(0, "streaming to client");
            streaming = true;
        } else {
            ctx->logTrace(Ctx::TRACE::WRITER, "starting failed");
            response.set_code(pb::ResponseCode::FAILED_START);
        }
    }

    void WriterStream::processContinue() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::STREAM)))
            ctx->logTrace(Ctx::TRACE::STREAM, "request: CONTINUE database: " + request.database_name() +
                (request.has_c_scn() ? ", c_scn: " + std::to_string(request.c_scn()) : "") +
                (request.has_c_idx() ? ", c_idx: " + std::to_string(request.c_idx()) : ""));

        response.Clear();
        if (request.database_name() != database) {
            ctx->warning(60035, "unknown database requested, got: " + std::string(request.database_name()) + " instead of " + database);
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
            return;
        }

        // default values
        metadata->clientScn = confirmedScn;
        metadata->clientIdx = confirmedIdx;
        std::string paramIdx;

        // 0 means continue with last value
        if (request.has_c_scn() && request.c_scn() != 0) {
            metadata->clientScn = request.c_scn();

            if (request.has_c_idx())
                metadata->clientIdx = request.c_idx();
            paramIdx = ", idx: " + std::to_string(metadata->clientIdx);
        }
        ctx->info(0, "client requested scn: " + metadata->clientScn.toString() + paramIdx);

        resetMessageQueue();
        response.set_code(pb::ResponseCode::REPLICATE);
        ctx->info(0, "streaming to client");
        streaming = true;
    }

    void WriterStream::processConfirm() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::STREAM)))
            ctx->logTrace(Ctx::TRACE::STREAM, "request: CONFIRM: " + request.database_name() +
                (request.has_c_scn() ? ", c_scn: " + std::to_string(request.c_scn()) : "") +
                (request.has_c_idx() ? ", c_idx: " + std::to_string(request.c_idx()) : ""));

        if (request.database_name() != database) {
            ctx->warning(60035, "unknown database confirmed, got: " + request.database_name() + ", expected: " + database);
            return;
        }

        if (!request.has_c_scn()) {
            ctx->warning(60035, "missing scn confirmed");
            return;
        }

        while (currentQueueSize > 0 && (queue[0]->lwnScn < Scn(request.c_scn()) ||
                (queue[0]->lwnScn == Scn(request.c_scn()) && queue[0]->lwnIdx <= request.c_idx())))
            confirmMessage(queue[0]);
    }

    void WriterStream::pollQueue() {
        // No client connected
        if (!stream->isConnected())
            return;

        uint8_t msgR[Stream::READ_NETWORK_BUFFER];
        std::string msgS;

        const uint64_t size = stream->receiveMessageNB(msgR, Stream::READ_NETWORK_BUFFER);

        if (size > 0) {
            request.Clear();
            if (request.ParseFromArray(msgR, static_cast<int>(size))) {
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
                            ctx->warning(60032, "unknown request code: " + std::to_string(request.code()));
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

                        case pb::RequestCode::CONTINUE:
                            processContinue();
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;

                        default:
                            ctx->warning(60032, "unknown request code: " + std::to_string(request.code()));
                            response.Clear();
                            response.set_code(pb::ResponseCode::INVALID_COMMAND);
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;
                    }
                }
            } else {
                std::ostringstream ss;
                ss << "request decoder[" << std::dec << size << "]: ";
                for (uint64_t i = 0; i < size; ++i)
                    ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<uint>(msgR[i]) << " ";
                ctx->warning(60033, ss.str());
            }
        } else if (errno != EAGAIN)
            throw NetworkException(10061, "network error, errno: " + std::to_string(errno) + ", message: " + strerror(errno));
    }

    void WriterStream::sendMessage(BuilderMsg* msg) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::STREAM)))
            ctx->logTrace(Ctx::TRACE::STREAM, "data[" + std::to_string(msg->size) + "]: [" +
                std::string(reinterpret_cast<const char*>(msg->data + msg->tagSize), msg->size) + "]");
        stream->sendMessage(msg->data + msg->tagSize, msg->size - msg->tagSize);
    }
}
