/* Test client for Zero MQ
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

#include <atomic>
#include <stdint.h>

namespace OpenLogReplicator {
    uint64_t trace = 3;
    uint64_t trace2 = 0;
}
#define GLOBAL_H_ 0

#include "OraProtoBuf.pb.h"
#include "NetworkException.h"
#include "RuntimeException.h"
#include "StreamNetwork.h"

#ifdef LINK_LIBRARY_ZEROMQ
#include "StreamZeroMQ.h"
#endif /* LINK_LIBRARY_ZEROMQ */


#define POLL_INTERVAL           10000

void send(OpenLogReplicator::pb::RedoRequest& request, OpenLogReplicator::Stream* stream) {
    std::string buffer;
    bool ret = request.SerializeToString(&buffer);
    if (!ret) {
        ERROR("message serialization");
        exit(0);
    }

    stream->sendMessage(buffer.c_str(), buffer.length());
}

void receive(OpenLogReplicator::pb::RedoResponse& response, OpenLogReplicator::Stream* stream) {
    uint8_t buffer[READ_NETWORK_BUFFER];
    uint64_t length = stream->receiveMessage(buffer, READ_NETWORK_BUFFER);

    response.Clear();
    if (!response.ParseFromArray(buffer, length)) {
        ERROR("response parse");
        exit(0);
    }
}

int main(int argc, char** argv) {
    if (argc < 4) {
        WARNING("use: ClientNetwork [network|zeromq] <uri> <database> {<scn>}");
        return 0;
    }
    INFO("OpenLogReplicator v." PACKAGE_VERSION " StreamClient (C) 2018-2022 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");

    OpenLogReplicator::pb::RedoRequest request;
    OpenLogReplicator::pb::RedoResponse response;
    std::atomic<bool> shutdown(false);
    OpenLogReplicator::Stream* stream = nullptr;

    try {
        if (strcmp(argv[1], "network") == 0) {
            stream = new OpenLogReplicator::StreamNetwork(argv[2], POLL_INTERVAL);
        } else if (strcmp(argv[1], "zeromq") == 0) {
#ifdef LINK_LIBRARY_ZEROMQ
            stream = new OpenLogReplicator::StreamZeroMQ(argv[2], POLL_INTERVAL);
#else
            RUNTIME_FAIL("ZeroMQ is not compiled");
#endif /* LINK_LIBRARY_ZEROMQ */
        } else {
            RUNTIME_FAIL("incorrect transport");
        }
        if (stream == nullptr) {
            ERROR("couldn't create stream");
            exit(0);
        }
        stream->initialize();
        stream->initializeClient(&shutdown);

        request.set_code(OpenLogReplicator::pb::RequestCode::INFO);
        request.set_database_name(argv[3]);
        INFO("INFO database: " << request.database_name());
        send(request, stream);
        receive(response, stream);
        INFO("- code: " << (uint64_t)response.code() << ", scn: " << response.scn());

        uint64_t scn = 0;
        if (response.code() == OpenLogReplicator::pb::ResponseCode::STARTED) {
            scn = response.scn();
        } else if (response.code() == OpenLogReplicator::pb::ResponseCode::READY) {
            request.Clear();
            request.set_code(OpenLogReplicator::pb::RequestCode::START);
            request.set_database_name(argv[3]);
            if (argc > 4) {
                request.set_scn(atoi(argv[4]));
                INFO("START scn: " << std::dec << request.scn() << ", database: " << request.database_name());
            } else {
                //start from now, when SCN is not given
                request.set_scn(ZERO_SCN);
                INFO("START NOW, database: " << request.database_name());
            }
            send(request, stream);
            receive(response, stream);
            INFO("- code: " << (uint64_t)response.code() << ", scn: " << response.scn());

            if (response.code() == OpenLogReplicator::pb::ResponseCode::STARTED || response.code() == OpenLogReplicator::pb::ResponseCode::ALREADY_STARTED) {
                scn = response.scn();
            } else {
                ERROR("returned code: " << response.code());
                return 1;
            }
        } else {
            return 1;
        }

        uint64_t lastScn;
        uint64_t prevScn = 0;
        uint64_t num = 0;

        request.Clear();
        request.set_code(OpenLogReplicator::pb::RequestCode::REDO);
        request.set_database_name(argv[3]);
        INFO("REDO database: " << request.database_name());
        send(request, stream);
        receive(response, stream);
        INFO("- code: " << (uint64_t)response.code());

        if (response.code() != OpenLogReplicator::pb::ResponseCode::STREAMING)
            return 1;

        for (;;) {
            receive(response, stream);
            INFO("- scn: " << std::dec << response.scn() << ", code: " << (uint64_t) response.code() << " payload size: " << response.payload_size());
            lastScn = response.scn();
            ++num;

            //confirm every 1000 messages
            if (num > 1000 && prevScn < lastScn) {
                request.Clear();
                request.set_code(OpenLogReplicator::pb::RequestCode::CONFIRM);
                request.set_scn(prevScn);
                request.set_database_name(argv[3]);
                INFO("CONFIRM scn: " << std::dec << request.scn() << ", database: " << request.database_name());
                send(request, stream);
                num = 0;
            }
            prevScn = lastScn;
        }

    } catch (OpenLogReplicator::RuntimeException& ex) {
    } catch (OpenLogReplicator::NetworkException& ex) {
    }

    if (stream != nullptr) {
        delete stream;
        stream = nullptr;
    }

    return 0;
}
