/* Thread writing to Network
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

#include <unistd.h>

#include "NetworkException.h"
#include "OracleAnalyzer.h"
#include "OraProtoBuf.pb.h"
#include "OutputBuffer.h"
#include "RuntimeException.h"
#include "Stream.h"
#include "WriterStream.h"

namespace OpenLogReplicator {
    WriterStream::WriterStream(const char* alias, OracleAnalyzer* oracleAnalyzer, uint64_t pollIntervalUs, uint64_t checkpointIntervalS,
            uint64_t queueSize, typeSCN startScn, typeSEQ startSequence, const char* startTime, uint64_t startTimeRel, Stream* stream) :
        Writer(alias, oracleAnalyzer, 0, pollIntervalUs, checkpointIntervalS, queueSize, startScn, startSequence, startTime, startTimeRel),
        stream(stream) {
    }

    WriterStream::~WriterStream() {
        if (stream != nullptr) {
            delete stream;
            stream = nullptr;
        }
    }

    void WriterStream::initialize(void) {
        Writer::initialize();

        stream->initializeServer(&shutdown);
    }

    std::string WriterStream::getName(void) const {
        return stream->getName();
    }

    void WriterStream::readCheckpoint(void) {
        while (!streaming && !shutdown && !stop) {
            try {
                usleep(pollIntervalUs);
                if (stream->connected()) {
                    pollQueue();
                }
            } catch (NetworkException& ex) {
                //client got disconnected
                streaming = false;
            }
        }

        if (oracleAnalyzer->firstScn != ZERO_SCN)
            DEBUG("client requested scn: " << std::dec << startScn);
    }

    void WriterStream::processInfo(void) {
        response.Clear();
        if (request.database_name().compare(oracleAnalyzer->database) != 0) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (oracleAnalyzer->firstScn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::STARTED);
            response.set_scn(oracleAnalyzer->firstScn);
        } else {
            response.set_code(pb::ResponseCode::READY);
        }
    }

    void WriterStream::processStart(void) {
        response.Clear();
        if (request.database_name().compare(oracleAnalyzer->database) != 0) {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        } else if (oracleAnalyzer->firstScn != ZERO_SCN) {
            response.set_code(pb::ResponseCode::ALREADY_STARTED);
            response.set_scn(oracleAnalyzer->firstScn);
        } else {
            startScn = 0;
            if (request.has_seq())
                startSequence = request.seq();
            else
                startSequence = ZERO_SEQ;
            startTime.clear();
            startTimeRel = 0;

            switch (request.tm_val_case()) {
                case pb::RedoRequest::TmValCase::kScn:
                    startScn = request.scn();
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

            if (oracleAnalyzer->firstScn != ZERO_SCN) {
                response.set_code(pb::ResponseCode::STARTED);
                response.set_scn(oracleAnalyzer->firstScn);
            } else {
                response.set_code(pb::ResponseCode::FAILED_START);
            }
        }
    }

    void WriterStream::processRedo(void) {
        response.Clear();
        if (request.database_name().compare(oracleAnalyzer->database) == 0) {
            response.set_code(pb::ResponseCode::STREAMING);
            INFO("streaming to client");
            streaming = true;
        } else {
            response.set_code(pb::ResponseCode::INVALID_DATABASE);
        }
    }

    void WriterStream::processConfirm(void) {
        uint64_t confirmed = 0;
        if (request.database_name().compare(oracleAnalyzer->database) == 0) {
            uint64_t oldSize = tmpQueueSize;
            while (tmpQueueSize > 0 && queue[0]->scn <= request.scn())
                confirmMessage(queue[0]);
        }
    }

    void WriterStream::pollQueue(void) {
        uint8_t msgR[READ_NETWORK_BUFFER];
        std::string msgS;

        int64_t length = stream->receiveMessageNB(msgR, READ_NETWORK_BUFFER);

        if (length > 0) {
            request.Clear();
            if (request.ParseFromArray(msgR, length)) {
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
                            {WARNING("unknown request code: " << request.code());}
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
                            {WARNING("unknown request code: " << request.code());}
                            response.Clear();
                            response.set_code(pb::ResponseCode::INVALID_COMMAND);
                            response.SerializeToString(&msgS);
                            stream->sendMessage(msgS.c_str(), msgS.length());
                            break;
                    }
                }
            } else {
                std::stringstream ss;
                ss << "request data[" << std::dec << length << "]: ";
                for (uint64_t i = 0; i < length; ++i)
                    ss << std::hex  << std::setw(2) << std::setfill('0') << (uint64_t)msgR[i] << " ";
                WARNING(ss.str());
            }

        } else if (length == 0) {
            //no request
        } else if (errno != EAGAIN) {
            RUNTIME_FAIL("socket error: (ret: " << std::dec << length << " errno: " << errno << ")");
        }
    }

    void WriterStream::sendMessage(OutputBufferMsg* msg) {
        stream->sendMessage(msg->data, msg->length);
    }
}
