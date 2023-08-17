/* Test client for Zero MQ
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <stdint.h>

#include "common/Ctx.h"
#include "common/ConfigurationException.h"
#include "common/OraProtoBuf.pb.h"
#include "common/NetworkException.h"
#include "common/RuntimeException.h"
#include "common/types.h"
#include "stream/StreamNetwork.h"

#ifdef LINK_LIBRARY_ZEROMQ
#include "stream/StreamZeroMQ.h"
#endif /* LINK_LIBRARY_ZEROMQ */

#define MAX_CLIENT_MESSAGE_SIZE (2*1024*1024*1024ul - 1)

void send(OpenLogReplicator::pb::RedoRequest& request, OpenLogReplicator::Stream* stream, OpenLogReplicator::Ctx* ctx) {
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
        OLR_LOCALES = OLR_LOCALES_MOCK;

    OpenLogReplicator::Ctx ctx;
    ctx.welcome("OpenLogReplicator v." + std::to_string(OpenLogReplicator_VERSION_MAJOR) + "." +
                std::to_string(OpenLogReplicator_VERSION_MINOR) + "." + std::to_string(OpenLogReplicator_VERSION_PATCH) +
                " StreamClient (C) 2018-2023 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");

    if (argc < 5) {
        ctx.info(0, "use: ClientNetwork [network|zeromq] <uri> <database> {format} {<scn>}");
        return 0;
    }

    bool formatProtobuf = false;
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
            throw OpenLogReplicator::RuntimeException(1, "incorrect transport, expected: {network|zeromq}");
        }
        stream->initialize();
        stream->initializeClient();

        if (strcmp(argv[4], "protobuf") == 0)
            formatProtobuf = true;
        else
        if (strcmp(argv[4], "json") == 0)
            formatProtobuf = false;
        else {
            throw OpenLogReplicator::RuntimeException(1, "incorrect format, expected: {protobuf|json}");
        }

        request.set_code(OpenLogReplicator::pb::RequestCode::INFO);
        request.set_database_name(argv[3]);
        ctx.info(0, "database: " + request.database_name());
        send(request, stream, &ctx);
        receive(response, stream, &ctx, buffer, true);
        ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())) + ", scn: " + std::to_string(response.scn()));

        uint64_t scn = 0;
        if (response.code() == OpenLogReplicator::pb::ResponseCode::STARTED) {
            if (argc > 5) {
                scn = atoi(argv[5]);
            } else {
                scn = response.scn();
            }
        } else if (response.code() == OpenLogReplicator::pb::ResponseCode::READY) {
            request.Clear();
            request.set_code(OpenLogReplicator::pb::RequestCode::START);
            request.set_database_name(argv[3]);
            if (argc > 5) {
                request.set_scn(atoi(argv[5]));
                ctx.info(0, "START scn: " + std::to_string(request.scn()) + ", database: " + request.database_name());
            } else {
                // Start from NOW, when SCN is not given
                request.set_scn(ZERO_SCN);
                ctx.info(0, "START NOW, database: " + request.database_name());
            }
            send(request, stream, &ctx);
            receive(response, stream, &ctx, buffer, true);
            ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())) + ", scn: " +
                     std::to_string(response.scn()));

            if (response.code() == OpenLogReplicator::pb::ResponseCode::STARTED || response.code() == OpenLogReplicator::pb::ResponseCode::ALREADY_STARTED) {
                scn = response.scn();
            } else {
                ctx.error(0, "returned code: " + std::to_string(response.code()));

                if (buffer != nullptr) {
                    delete[] buffer;
                    buffer = nullptr;
                }

                if (stream != nullptr) {
                    delete stream;
                    stream = nullptr;
                }
                return 1;
            }
        } else {
            ctx.error(0, "returned code: " + std::to_string(response.code()));

            if (buffer != nullptr) {
                delete[] buffer;
                buffer = nullptr;
            }

            if (stream != nullptr) {
                delete stream;
                stream = nullptr;
            }
            return 1;
        }

        uint64_t lastScn;
        uint64_t prevScn = 0;
        uint64_t num = 0;

        request.Clear();
        request.set_code(OpenLogReplicator::pb::RequestCode::REDO);
        if (scn != 0)
            request.set_scn(scn);
        request.set_database_name(argv[3]);
        ctx.info(0, "REDO database: " + request.database_name() + " scn: " + std::to_string(scn));
        send(request, stream, &ctx);
        receive(response, stream, &ctx, buffer, true);
        ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())));

        if (response.code() != OpenLogReplicator::pb::ResponseCode::STREAMING)
            return 1;

        for (;;) {
            uint64_t length = receive(response, stream, &ctx, buffer, formatProtobuf);

            if (formatProtobuf) {
                // Display checkpoint messages very seldom
                if (response.payload(0).op() != OpenLogReplicator::pb::CHKPT || (num > 1000 && prevScn < lastScn)) {
                    if (response.payload_size() == 1) {
                        const char *msg = "UNKNOWN";
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
                        }
                        ctx.info(0, "- scn: " + std::to_string(response.scn()) + ", code: " +
                                    std::to_string(static_cast<uint64_t>(response.code())) + ", length: " + std::to_string(length) + ", op: " + msg);
                    } else {
                        ctx.info(0, "- scn: " + std::to_string(response.scn()) + ", code: " +
                                    std::to_string(static_cast<uint64_t>(response.code())) + ", length: " + std::to_string(length) +
                                    ", payload size: " + std::to_string(response.payload_size()));
                    }
                }
                lastScn = response.scn();
            } else {
                buffer[length] = 0;
                ctx.info(0, std::string("message: ") + ((const char*)buffer));

                rapidjson::Document document;
                if (document.Parse((const char*)buffer).HasParseError())
                    throw OpenLogReplicator::RuntimeException(20001, "offset: " +
                                               std::to_string(document.GetErrorOffset()) + " - parse error: " + GetParseError_En(document.GetParseError()));

                lastScn = OpenLogReplicator::Ctx::getJsonFieldU64("network", document, "scn");
            }

            ++num;

            // Confirm every 1000 messages
            if (num > 1000 && prevScn < lastScn) {
                request.Clear();
                request.set_code(OpenLogReplicator::pb::RequestCode::CONFIRM);
                request.set_scn(prevScn);
                request.set_database_name(argv[3]);
                ctx.info(0, "CONFIRM scn: " + std::to_string(request.scn()) + ", database: " + request.database_name());
                send(request, stream, &ctx);
                num = 0;
            }
            prevScn = lastScn;
        }

    } catch (OpenLogReplicator::RuntimeException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (OpenLogReplicator::NetworkException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (OpenLogReplicator::ConfigurationException& ex) {
        ctx.error(ex.code, "error: " + ex.msg);
    } catch (std::bad_alloc& ex) {
        ctx.error(0, "memory allocation failed: " + std::string(ex.what()));
    }

    if (buffer != nullptr) {
        delete[] buffer;
        buffer = nullptr;
    }

    if (stream != nullptr) {
        delete stream;
        stream = nullptr;
    }

    return 0;
}
