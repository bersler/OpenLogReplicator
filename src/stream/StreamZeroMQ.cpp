/* Base class for streaming using ZeroMQ
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

#include <unistd.h>
#include <zmq.h>

#include "../common/Ctx.h"
#include "../common/NetworkException.h"
#include "../common/RuntimeException.h"
#include "StreamZeroMQ.h"

namespace OpenLogReplicator {
    StreamZeroMQ::StreamZeroMQ(Ctx* newCtx, const char* newUri) :
        Stream(newCtx, newUri),
        socket(nullptr),
        context(nullptr) {
    }

    StreamZeroMQ::~StreamZeroMQ() {
        zmq_close(socket);
        zmq_ctx_term(context);
    }

    void StreamZeroMQ::initialize() {
        context = zmq_ctx_new();
        if (context == nullptr)
            throw RuntimeException("ZeroMQ context creation error");

        socket = zmq_socket(context, ZMQ_PAIR);
        if (socket == nullptr) {
            throw RuntimeException("ZeroMQ initializing socket error (errno: " + std::to_string(errno) + ")");
        }
    }

    std::string StreamZeroMQ::getName() const {
        return "ZeroMQ:" + uri;
    }

    void StreamZeroMQ::initializeClient() {
        if (zmq_connect(socket, uri.c_str()) != 0)
            throw RuntimeException("ZeroMQ connect to " + uri + " error (errno: " + std::to_string(errno) + ")");
    }

    void StreamZeroMQ::initializeServer() {
        if (zmq_bind(socket, uri.c_str()) != 0)
            throw RuntimeException("ZeroMQ bind to " + uri + " error (errno: " + std::to_string(errno) + ")");
    }

    void StreamZeroMQ::sendMessage(const void* msg, uint64_t length) {
        while (!ctx->softShutdown) {
            int64_t ret = zmq_send(socket, msg, length, ZMQ_NOBLOCK);
            if (ret == static_cast<int64_t>(length))
                return;

            if (ret < 0 && errno == EAGAIN) {
                usleep(ctx->pollIntervalUs);
                continue;
            }

            throw NetworkException("network send error");
        }
    }

    uint64_t StreamZeroMQ::receiveMessage(void* msg, uint64_t length) {
        int64_t ret = zmq_recv(socket, msg, length, 0);

        if (ret < 0)
            throw NetworkException("network receive error");

        return ret;
    }

    uint64_t StreamZeroMQ::receiveMessageNB(void* msg, uint64_t length) {
        int64_t ret = zmq_recv(socket, msg, length, ZMQ_DONTWAIT);
        if (ret < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
                return 0;

            throw NetworkException("network receive error");
        }
        return ret;
    }

    bool StreamZeroMQ::isConnected() {
        return true;
    }
}
