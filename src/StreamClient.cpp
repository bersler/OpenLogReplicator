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
#include "common/OraProtoBuf.pb.h"
#include "common/NetworkException.h"
#include "common/RuntimeException.h"
#include "common/types.h"
#include "stream/StreamNetwork.h"

#ifdef LINK_LIBRARY_ZEROMQ
#include "stream/StreamZeroMQ.h"
#endif /* LINK_LIBRARY_ZEROMQ */

void send(OpenLogReplicator::pb::RedoRequest& request, OpenLogReplicator::Stream* stream, OpenLogReplicator::Ctx* ctx) {
    std::string buffer;
    bool ret = request.SerializeToString(&buffer);
    if (!ret) {
        ctx->error(0, "message serialization");
        exit(0);
    }

    stream->sendMessage(buffer.c_str(), buffer.length());
}

void receive(OpenLogReplicator::pb::RedoResponse& response, OpenLogReplicator::Stream* stream, OpenLogReplicator::Ctx* ctx) {
    uint8_t buffer[READ_NETWORK_BUFFER];
    uint64_t length = stream->receiveMessage(buffer, READ_NETWORK_BUFFER);

    response.Clear();
    if (!response.ParseFromArray(buffer, length)) {
        ctx->error(0, "response parse");
        exit(0);
    }
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

    if (argc < 4) {
        ctx.info(0, "use: ClientNetwork [network|zeromq] <uri> <database> {<scn>}");
        return 0;
    }

    OpenLogReplicator::pb::RedoRequest request;
    OpenLogReplicator::pb::RedoResponse response;
    OpenLogReplicator::Stream* stream = nullptr;

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
            throw OpenLogReplicator::RuntimeException(1, "incorrect transport");
        }
        stream->initialize();
        stream->initializeClient();

        request.set_code(OpenLogReplicator::pb::RequestCode::INFO);
        request.set_database_name(argv[3]);
        ctx.info(0, "database: " + request.database_name());
        send(request, stream, &ctx);
        receive(response, stream, &ctx);
        ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())) + ", scn: " + std::to_string(response.scn()));

        uint64_t scn = 0;
        if (response.code() == OpenLogReplicator::pb::ResponseCode::STARTED) {
            scn = response.scn();
        } else if (response.code() == OpenLogReplicator::pb::ResponseCode::READY) {
            request.Clear();
            request.set_code(OpenLogReplicator::pb::RequestCode::START);
            request.set_database_name(argv[3]);
            if (argc > 4) {
                request.set_scn(atoi(argv[4]));
                ctx.info(0, "START scn: " + std::to_string(request.scn()) + ", database: " + request.database_name());
            } else {
                // Start from now, when SCN is not given
                request.set_scn(ZERO_SCN);
                ctx.info(0, "START NOW, database: " + request.database_name());
            }
            send(request, stream, &ctx);
            receive(response, stream, &ctx);
            ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())) + ", scn: " +
                     std::to_string(response.scn()));

            if (response.code() == OpenLogReplicator::pb::ResponseCode::STARTED || response.code() == OpenLogReplicator::pb::ResponseCode::ALREADY_STARTED) {
                scn = response.scn();
            } else {
                ctx.error(0, "returned code: " + std::to_string(response.code()));

                if (stream != nullptr) {
                    delete stream;
                    stream = nullptr;
                }
                return 1;
            }
        } else {
            ctx.error(0, "returned code: " + std::to_string(response.code()));

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
        request.set_database_name(argv[3]);
        ctx.info(0, "REDO database: " + request.database_name() + " scn: " + std::to_string(scn));
        send(request, stream, &ctx);
        receive(response, stream, &ctx);
        ctx.info(0, "- code: " + std::to_string(static_cast<uint64_t>(response.code())));

        if (response.code() != OpenLogReplicator::pb::ResponseCode::STREAMING)
            return 1;

        for (;;) {
            receive(response, stream, &ctx);
            ctx.info(0, "- scn: " + std::to_string(response.scn()) + ", code: " +
                     std::to_string(static_cast<uint64_t>(response.code())) + " payload size: " + std::to_string(response.payload_size()));
            lastScn = response.scn();
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
    } catch (std::bad_alloc& ex) {
        ctx.error(0, "memory allocation failed: " + std::string(ex.what()));
    }

    if (stream != nullptr) {
        delete stream;
        stream = nullptr;
    }

    return 0;
}
