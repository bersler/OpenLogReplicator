/* Base class for streaming using ZeroMQ
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
#include <zmq.h>

#include "NetworkException.h"
#include "RuntimeException.h"
#include "StreamZeroMQ.h"

using namespace std;

namespace OpenLogReplicator {

    StreamZeroMQ::StreamZeroMQ(const char *uri, uint64_t pollInterval) :
        Stream(uri, pollInterval),
        socket(nullptr),
        context(nullptr) {

        context = zmq_ctx_new();
        if (context == nullptr) {
            RUNTIME_FAIL("ZeroMQ context creation error");
        }
        socket = zmq_socket(context, ZMQ_PAIR);
        if (socket == nullptr) {
            RUNTIME_FAIL("ZeroMQ initializing socket error (errno: " << dec << errno << ")");
        }
    }

    StreamZeroMQ::~StreamZeroMQ() {
        zmq_close(socket);
        zmq_ctx_term(context);
    }

    string StreamZeroMQ::getName(void) const {
        return "ZeroMQ:" + uri;
    }

    void StreamZeroMQ::initializeClient(volatile bool *shutdown) {
        this->shutdown = shutdown;

        if (zmq_connect(socket, uri.c_str()) != 0) {
            RUNTIME_FAIL("ZeroMQ connect to " << uri << " error (errno: " << dec << errno << ")");
        }
    }

    void StreamZeroMQ::initializeServer(volatile bool *shutdown) {
        this->shutdown = shutdown;
        if (zmq_bind(socket, uri.c_str()) != 0) {
            RUNTIME_FAIL("ZeroMQ bind to " << uri << " error (errno: " << dec << errno << ")");
        }
    }

    void StreamZeroMQ::sendMessage(const void *msg, uint64_t length) {
        int64_t ret = 0;
        while (!*shutdown) {
            ret = zmq_send(socket, msg, length, ZMQ_NOBLOCK);
            if (ret == length)
                return;

            if (ret < 0 && errno == EAGAIN) {
                usleep(pollInterval);
                continue;
            }

            NETWORK_FAIL("network send error");
        }
    }

    uint64_t StreamZeroMQ::receiveMessage(void *msg, uint64_t length) {
        int64_t ret = zmq_recv(socket, msg, length, 0);

        if (ret < 0) {
            NETWORK_FAIL("network receive error");
        }
        return ret;
    }

    uint64_t StreamZeroMQ::receiveMessageNB(void *msg, uint64_t length) {
        int64_t ret = zmq_recv(socket, msg, length, ZMQ_DONTWAIT);
        if (ret < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
                return 0;

            NETWORK_FAIL("network receive error");
        }
        return ret;
    }

    bool StreamZeroMQ::connected(void) {
        return true;
    }
}
