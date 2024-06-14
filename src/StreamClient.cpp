/* Test client for Zero MQ
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <atomic>

#include "common/ClockHW.h"
#include "common/Ctx.h"
#include "common/OraProtoBuf.pb.h"
#include "common/types.h"
#include "common/exception/ConfigurationException.h"
#include "common/exception/DataException.h"
#include "common/exception/NetworkException.h"
#include "common/exception/RuntimeException.h"
#include "stream/StreamNetwork.h"

#ifdef LINK_LIBRARY_ZEROMQ

#include "stream/StreamZeroMQ.h"

#endif /* LINK_LIBRARY_ZEROMQ */

#define MAX_CLIENT_MESSAGE_SIZE (2*1024*1024*1024ul - 1)

void send(OpenLogReplicator::pb::RedoRequest& request, OpenLogReplicator::Stream* stream, OpenLogReplicator::Ctx* ctx __attribute__((unused))) {
    std::string buffer;
    bool ret = request.SerializeToString(&buffer);
    if (!ret)
        throw OpenLogReplicator::RuntimeException(0, "message serialization");

    stream->sendMessage(buffer.c_str(), buffer.length());
}

uint64_t receive(OpenLogReplicator::pb::RedoResponse& response, OpenLogReplicator::Stream* stream, OpenLogReplicator::Ctx* ctx, uint8_t* buffer, bool decode) {
    uint64_t length = stream->receiveMessage(buffer, MAX_CLIENT_MESSAGE_SIZE);

    response.Clear();
    if (decode && !response.ParseFromArray(buffer, length)) {
        ctx->error(0, "response parse");
        exit(0);
    }

    return length;
}

int main(int argc, char** argv) {
    std::string olrLocales;
    const char* olrLocalesStr = getenv("OLR_LOCALES");
    if (olrLocalesStr != nullptr)
        olrLocales = olrLocalesStr;
    if (olrLocales == "MOCK")
        OLR_LOCALES = OpenLogReplicator::Ctx::OLR_LOCALES_MOCK;

    OpenLogReplicator::Ctx ctx;
    const char* logTimezone = std::getenv("OLR_LOG_TIMEZONE");
    if (logTimezone != nullptr)
        if (!ctx.parseTimezone(logTimezone, ctx.logTimezone))
            ctx.error(10070, "invalid environment variable OLR_LOG_TIMEZONE value: " + std::string(logTimezone));

    ctx.welcome("OpenLogReplicator v." + std::to_string(OpenLogReplicator_VERSION_MAJOR) + "." +
                std::to_string(OpenLogReplicator_VERSION_MINOR) + "." + std::to_string(OpenLogReplicator_VERSION_PATCH) +
                " StreamClient (C) 2018-2024 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");

    // Run arguments:
    // 1. network|zeromq - type of communication protocol
    // 2. uri - network: host:port, zeromq: tcp://host:port
    // 3. database - database name
    // 4. format - protobuf|json
    // 5. The fifth parameter defines mode to start. If OpenLogReplicator is started for the first time, it would expect to define the position to start
    //    replication from. If it is running, it would expect the client to provide c:<scn>,<idx> - position of last confirmed message.
    //    Possible values are:
    //      now - start from NOW
    //      now - start from NOW but start parsing redo log from sequence <seq>
    //      scn:<scn> - start from given SCN
    //      scn:<scn>,<seq> - start from given SCN but start parsing redo log from sequence <seq>
    //      tm_rel:<time> - start from given time (relative to current time)
    //      tm_rel:<time> - start from given time (relative to current time) but start parsing redo log from sequence <seq>
    //      time:<time> - start from given time (absolute)
    //      time:<time> - start from given time (absolute) but start parsing redo log from sequence <seq>
    //      c:<scn>,<idx> - continue from given SCN and IDX
    //      next - continue with next message, from the position
    if (argc != 6) {
        ctx.info(0, "use: ClientNetwork [network|zeromq] <uri> <database> <format> [now{,<seq>}|scn:<scn>{,<seq>}|tm_rel:<time>{,<seq>}|"
                    "tms:<time>{,<seq>}|c:<scn>,<idx>|next]");
        return 0;
    }

    bool formatProtobuf;
    OpenLogReplicator::pb::RedoRequest request;
    OpenLogReplicator::pb::RedoResponse response;
    OpenLogReplicator::Stream* stream = nullptr;
    uint8_t* buffer = new uint8_t[MAX_CLIENT_MESSAGE_SIZE];

    try {
        if (strcmp(argv[1], "network") == 0) {
            stream = new OpenLogReplicator::StreamNetwork(&ctx, argv[2]);
        } else if (strcmp(argv[1], "zeromq") == 0) {
#ifdef LINK_LIBRARY_ZEROMQ
            stream = new OpenLogReplicator::StreamZeroMQ(&ctx, argv[2]);
#else
            throw OpenLogReplicator::RuntimeException(1, "ZeroMQ is not compiled");
#endif /* LINK_LIBRARY_ZEROMQ */
        } else {
            throw OpenLogReplicator::RuntimeException(1, "incorrect transport, expected: [network|zeromq]");
        }
        stream->initialize();
        stream->initializeClient();

        if (strcmp(argv[4], "protobuf") == 0)
            formatProtobuf = true;
        else if (strcmp(argv[4], "json") == 0)
            formatProtobuf = false;
        else
            throw OpenLogReplicator::RuntimeException(1, "incorrect format, expected: [protobuf|json]");

        request.set_code(OpenLogReplicator::pb::RequestCode::INFO);
        request.set_database_name(argv[3]);
        ctx.info(0, "database: " + request.database_name());
        send(request, stream, &ctx);
        receive(response, stream, &ctx, buffer, true);
        ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())) + ", scn: " + std::to_string(response.scn()) +
                    ", confirmed: " + std::to_string(response.c_scn()) + "," + std::to_string(response.c_idx()));

        request.Clear();
        request.set_database_name(argv[3]);

        if (response.code() == OpenLogReplicator::pb::ResponseCode::REPLICATE) {
            request.set_code(OpenLogReplicator::pb::RequestCode::CONTINUE);
            if (strncmp(argv[5], "next", 4) == 0) {
                request.set_c_scn(OpenLogReplicator::Ctx::ZERO_SCN);
                request.set_c_idx(0);
            } else {
                char* idxPtr;
                if (strncmp(argv[5], "c:", 2) != 0 || strlen(argv[5]) <= 4 || (idxPtr = strchr(argv[5] + 2, ',')) == nullptr)
                    throw OpenLogReplicator::RuntimeException(1, "server already stared, expected: [c:<scn>,<idx>]");
                *idxPtr = 0;
                typeScn confirmedScn = atoi(argv[5] + 2);
                typeIdx confirmedIdx = atoi(idxPtr + 1);

                request.set_c_scn(confirmedScn);
                request.set_c_idx(confirmedIdx);
            }
        } else if (response.code() == OpenLogReplicator::pb::ResponseCode::READY) {
            request.set_code(OpenLogReplicator::pb::RequestCode::START);
            std::string paramSeq;
            char* idxPtr = strchr(argv[5], ',');
            if (idxPtr != nullptr) {
                *idxPtr = 0;
                request.set_seq(atoi(idxPtr + 1));
                paramSeq = ", seq: " + std::to_string(request.seq());
            }

            if (strncmp(argv[5], "now", 3) == 0) {
                request.set_scn(OpenLogReplicator::Ctx::ZERO_SCN);
                ctx.info(0, "START NOW" + paramSeq);
            } else if (strncmp(argv[5], "scn:", 4) == 0) {
                request.set_scn(atoi(argv[5] + 4));
                ctx.info(0, "START scn: " + std::to_string(request.scn()) + paramSeq);
            } else if (strncmp(argv[5], "tms:", 4) == 0) {
                std::string tms(argv[5] + 4);
                request.set_tms(tms);
                ctx.info(0, "START tms: " + request.tms() + paramSeq);
            } else if (strncmp(argv[5], "tm_rel:", 7) == 0) {
                request.set_tm_rel(atoi(argv[5] + 7));
                ctx.info(0, "START tm_rel: " + std::to_string(request.tm_rel()) + paramSeq);
            } else
                throw OpenLogReplicator::RuntimeException(1, "server is waiting to define position to start, expected: [now{,<seq>}|"
                                                             "scn:<scn>{,<seq>}|tm_rel:<time>{,<seq>}|tms:<time>{,<seq>}");
        } else
            throw OpenLogReplicator::RuntimeException(1, "server returned code: " + std::to_string(response.code()) +
                                                         " for request code: " + std::to_string(request.code()));

        // Index to count messages, to confirm after 1000th
        uint64_t num = 0;
        uint64_t last = ctx.clock->getTimeUt();

        send(request, stream, &ctx);
        receive(response, stream, &ctx, buffer, true);
        ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())));

        // Either after start or after continue, the server is expected to start streaming
        if (response.code() != OpenLogReplicator::pb::ResponseCode::REPLICATE)
            throw OpenLogReplicator::RuntimeException(1, "server returned code: " + std::to_string(response.code()) +
                                                         " for request code: " + std::to_string(request.code()));

        for (;;) {
            uint64_t length = receive(response, stream, &ctx, buffer, formatProtobuf);

            typeScn cScn;
            uint64_t cIdx;
            if (formatProtobuf) {
                if (response.payload_size() == 1) {
                    const char* msg;
                    switch (response.payload(0).op()) {
                        case OpenLogReplicator::pb::BEGIN:
                            msg = "BEGIN";
                            break;

                        case OpenLogReplicator::pb::COMMIT:
                            msg = "COMMIT";
                            break;

                        case OpenLogReplicator::pb::INSERT:
                            msg = "- INSERT";
                            break;

                        case OpenLogReplicator::pb::UPDATE:
                            msg = "- UPDATE";
                            break;

                        case OpenLogReplicator::pb::DELETE:
                            msg = "- DELETE";
                            break;

                        case OpenLogReplicator::pb::DDL:
                            msg = " DDL";
                            break;

                        case OpenLogReplicator::pb::CHKPT:
                            msg = "*** CHECKPOINT ***";
                            break;

                        default:
                            msg = "??? UNKNOWN ???";
                    }
                    ctx.info(0, "- scn: " + std::to_string(response.scn()) + ", idx: " + std::to_string(response.scn()) + ", code: " +
                                std::to_string(static_cast<uint64_t>(response.code())) + ", length: " + std::to_string(length) + ", op: " + msg);
                } else {
                    ctx.info(0, "- scn: " + std::to_string(response.scn()) + ", code: " +
                                std::to_string(static_cast<uint64_t>(response.code())) + ", length: " + std::to_string(length) +
                                ", payload size: " + std::to_string(response.payload_size()));
                }

                cScn = response.c_scn();
                cIdx = response.c_idx();
            } else {
                buffer[length] = 0;
                ctx.info(0, std::string("message: ") + reinterpret_cast<const char*>(buffer));

                rapidjson::Document document;
                if (document.Parse(reinterpret_cast<const char*>(buffer)).HasParseError())
                    throw OpenLogReplicator::RuntimeException(20001, "offset: " + std::to_string(document.GetErrorOffset()) +
                                                                     " - parse error: " + GetParseError_En(document.GetParseError()));

                cScn = OpenLogReplicator::Ctx::getJsonFieldU64("network", document, "c_scn");
                cIdx = OpenLogReplicator::Ctx::getJsonFieldU64("network", document, "c_idx");
            }

            ++num;
            time_ut now = ctx.clock->getTimeUt();
            double timeDelta = static_cast<double>(now - last) / 1000000.0;

            // Confirm every 1000 messages or every 10 seconds
            if (num > 1000 || timeDelta > 10) {
                request.Clear();
                request.set_code(OpenLogReplicator::pb::RequestCode::CONFIRM);
                request.set_c_scn(cScn);
                request.set_c_idx(cIdx);
                request.set_database_name(argv[3]);
                ctx.info(0, "CONFIRM scn: " + std::to_string(request.c_scn()) + ", idx: " + std::to_string(request.c_idx()) +
                            ", database: " + request.database_name());
                send(request, stream, &ctx);
                num = 0;
                last = now;
            }
        }

    } catch (OpenLogReplicator::DataException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (OpenLogReplicator::RuntimeException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (OpenLogReplicator::NetworkException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (OpenLogReplicator::ConfigurationException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (std::bad_alloc& ex) {
        ctx.error(0, "memory allocation failed: " + std::string(ex.what()));
    }

    delete[] buffer;

    if (stream != nullptr)
        delete stream;

    return 0;
}
