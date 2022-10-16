/* Base class for streaming using network sockets
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

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/NetworkException.h"
#include "../common/RuntimeException.h"
#include "StreamNetwork.h"

namespace OpenLogReplicator {
    StreamNetwork::StreamNetwork(Ctx* newCtx, const char* newUri) :
        Stream(newCtx, newUri),
        socketFD(-1),
        serverFD(-1),
        readBufferLen(0),
        res(nullptr) {
    }

    StreamNetwork::~StreamNetwork() {
        if (res != nullptr) {
            freeaddrinfo(res);
            res = nullptr;
        }

        if (socketFD != -1) {
            close(socketFD);
            socketFD = -1;
        }

        if (serverFD != -1) {
            close(serverFD);
            serverFD = -1;
        }
    }

    void StreamNetwork::initialize() {
        auto colon = this->uri.find(':');
        if (colon == std::string::npos)
            throw NetworkException("uri is missing ':'");

        host = this->uri.substr(0, colon);
        port = this->uri.substr(colon + 1, this->uri.length() - 1);
    }

    std::string StreamNetwork::getName() const {
        return "Network:" + uri;
    }

    void StreamNetwork::initializeClient() {
        struct sockaddr_in addressC;
        memset((void*)&addressC, 0, sizeof(addressC));
        addressC.sin_family = AF_INET;
        addressC.sin_port = htons(atoi(port.c_str()));

        if ((socketFD = socket(AF_INET, SOCK_STREAM, 0)) == 0)
            throw NetworkException(std::string("socket creation failed - ") + strerror(errno));

        struct hostent* server = gethostbyname(host.c_str());
        if (server == nullptr)
            throw NetworkException("resolving host name: " + host + " - " + strerror(errno));

        memcpy((void*)&addressC.sin_addr.s_addr, (void*)server->h_addr, server->h_length);
        if (connect(socketFD, (struct sockaddr*) &addressC, sizeof(addressC)) < 0)
            throw NetworkException("connecting to uri: " + uri + " - " + strerror(errno));
    }

    void StreamNetwork::initializeServer() {
        struct addrinfo hints;
        memset((void*)&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;

        if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0)
            throw NetworkException(std::string("getting information about host/port - ") + strerror(errno));

        serverFD = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (serverFD == 0)
            throw NetworkException(std::string("socket creation failed - ") + strerror(errno));

        int flags = fcntl(serverFD, F_GETFL);
        if (flags < 0)
            throw NetworkException(std::string("getting socket flags - ") + strerror(errno));
        if (fcntl(serverFD, F_SETFL, flags | O_NONBLOCK) < 0)
            throw NetworkException(std::string("setting socket flags - ") + strerror(errno));

        int64_t opt = 1;
        if (setsockopt(serverFD, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
            throw NetworkException(std::string("socket reusing failed - ") + strerror(errno));

        if (bind(serverFD, res->ai_addr, res->ai_addrlen) < 0)
            throw NetworkException("binding uri: " + uri + " - " + strerror(errno));
        if (listen(serverFD, 1) < 0)
            throw NetworkException(std::string("starting listener - ") + strerror(errno));

        if (res != nullptr) {
            freeaddrinfo(res);
            res = nullptr;
        }
    }

    void StreamNetwork::sendMessage(const void* msg, uint64_t length) {
        uint32_t length32 = length;
        uint64_t sent = 0;

        if (socketFD == -1)
            throw NetworkException("network send error - no connection");

        fd_set wset;
        fd_set w;
        FD_ZERO(&wset);
        FD_SET(socketFD, &wset);

        // Header content
        if (length < 0xFFFFFFFF) {
            // 32-bit length
            while (sent < sizeof(uint32_t)) {
                if (ctx->softShutdown)
                    return;

                w = wset;
                // Blocking select
                select(socketFD + 1, nullptr, &w, nullptr, nullptr);
                ssize_t r = write(socketFD, ((uint8_t*) &length32) + sent, sizeof(uint32_t) - sent);
                if (r <= 0) {
                   if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                       r = 0;
                   else {
                       close(socketFD);
                       socketFD = -1;
                       throw NetworkException(std::string("network send - ") + strerror(errno));
                   }
                }
                sent += r;
            }
        } else {
            // 64-bit length
            length32 = 0xFFFFFFFF;
            while (sent < sizeof(uint32_t)) {
                if (ctx->softShutdown)
                    return;

                w = wset;
                // Blocking select
                select(socketFD + 1, nullptr, &w, nullptr, nullptr);
                ssize_t r = write(socketFD, ((uint8_t*) &length32) + sent, sizeof(uint32_t) - sent);
                if (r <= 0) {
                   if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                       r = 0;
                   else {
                       close(socketFD);
                       socketFD = -1;
                       throw NetworkException(std::string("network send - ") + strerror(errno));
                   }
                }
                sent += r;
            }

            sent = 0;
            while (sent < sizeof(uint64_t)) {
                if (ctx->softShutdown)
                    return;

                w = wset;
                // Blocking select
                select(socketFD + 1, nullptr, &w, nullptr, nullptr);
                ssize_t r = write(socketFD, ((uint8_t*) &length) + sent, sizeof(uint64_t) - sent);
                if (r <= 0) {
                   if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                       r = 0;
                   else {
                       close(socketFD);
                       socketFD = -1;
                       throw NetworkException(std::string("network send - ") + strerror(errno));
                   }
                }
                sent += r;
            }
        }

        sent = 0;
        // Message content
        while (sent < length) {
            if (ctx->softShutdown)
                return;

            w = wset;
            // Blocking select
            select(socketFD + 1, nullptr, &w, nullptr, nullptr);
            ssize_t r = write(socketFD, (char*)msg + sent, length - sent);
            if (r <= 0) {
               if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                   r = 0;
               else {
                   close(socketFD);
                   socketFD = -1;
                   throw NetworkException(std::string("network send - ") + strerror(errno));
               }
            }
            sent += r;
        }
    }

    uint64_t StreamNetwork::receiveMessage(void* msg, uint64_t length) {
        uint64_t recvd = 0;

        // Read message length
        while (recvd < sizeof(uint32_t)) {
            if (ctx->softShutdown)
                return 0;

            int64_t bytes = read(socketFD, (char*)msg + recvd, sizeof(uint32_t) - recvd);

            if (bytes > 0)
                recvd += bytes;
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                throw NetworkException("host disconnected");
            } else {
                close(socketFD);
                socketFD = -1;
                throw NetworkException(std::string("network receive - ") + strerror(errno));
            }
        }

        if (*((uint32_t*)msg) < 0xFFFFFFFF) {
            // 32-bit message length
            if (length < *((uint32_t*) msg))
                throw NetworkException("read buffer too small");

            length = *((uint32_t*) msg);
            recvd = 0;
        } else {
            // 64-bit message length
            recvd = 0;

            while (recvd < sizeof(uint64_t)) {
                if (ctx->softShutdown)
                    return 0;

                int64_t bytes = read(socketFD, (char*)msg + recvd, sizeof(uint64_t) - recvd);

                if (bytes > 0)
                    recvd += bytes;
                else if (bytes == 0) {
                    close(socketFD);
                    socketFD = -1;
                    throw NetworkException("host disconnected");
                } else {
                    close(socketFD);
                    socketFD = -1;
                    throw NetworkException(std::string("network receive - ") + strerror(errno));
                }
            }

            if (length < *((uint64_t*) msg))
                throw NetworkException("read buffer too small");

            length = *((uint64_t*) msg);
            recvd = 0;
        }

        while (recvd < length) {
            if (ctx->softShutdown)
                return 0;

            int64_t bytes = read(socketFD, (char*)msg + recvd, length - recvd);

            if (bytes > 0)
                recvd += bytes;
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                throw NetworkException("host disconnected");
            } else {
                close(socketFD);
                socketFD = -1;
                throw NetworkException(std::string("network receive - ") + strerror(errno));
            }
        }

        return recvd;
    }

    uint64_t StreamNetwork::receiveMessageNB(void* msg, uint64_t length) {
        uint64_t recvd = 0;

        // Read message length
        while (recvd < sizeof(uint32_t)) {
            if (ctx->softShutdown)
                return 0;

            int64_t bytes = read(socketFD, (char*)msg + recvd, sizeof(uint32_t) - recvd);

            if (bytes > 0)
                recvd += bytes;
            // Client disconnected
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                throw NetworkException("host disconnected");
            } else {
                if (recvd == 0)
                    return 0;

                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    usleep(ctx->pollIntervalUs);
                else
                    return 0;
            }
        }

        if (*((uint32_t*)msg) < 0xFFFFFFFF) {
            // 32-bit message length
            if (length < *((uint32_t*) msg))
                throw NetworkException("read buffer too small");

            length = *((uint32_t*) msg);
            recvd = 0;
        } else {
            // 64-bit message length
            recvd = 0;

            while (recvd < sizeof(uint64_t)) {
                if (ctx->softShutdown)
                    return 0;

                int64_t bytes = read(socketFD, (char*)msg + recvd, sizeof(uint64_t) - recvd);

                if (bytes > 0)
                    recvd += bytes;
                // Client disconnected
                else if (bytes == 0) {
                    close(socketFD);
                    socketFD = -1;
                    throw NetworkException("host disconnected");
                } else {
                    if (recvd == 0)
                        return 0;

                    if (errno == EWOULDBLOCK || errno == EAGAIN)
                        usleep(ctx->pollIntervalUs);
                    else
                        return 0;
                }
            }

            if (length < *((uint64_t*) msg))
                throw NetworkException("read buffer too small");

            length = *((uint64_t*) msg);
            recvd = 0;
        }

        while (recvd < length) {
            int64_t bytes = read(socketFD, (char*)msg + recvd, length - recvd);

            if (bytes > 0)
                recvd += bytes;
            // Client disconnected
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                throw NetworkException("host disconnected");
            } else {
                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    usleep(ctx->pollIntervalUs);
                else
                    return 0;
            }
        }

        return recvd;
    }

    bool StreamNetwork::isConnected() {
        if (socketFD != -1)
            return true;

        int64_t addrlen = sizeof(address);
        socketFD = accept(serverFD, (struct sockaddr*) &address, (socklen_t*) &addrlen);
        if (socketFD < 0) {
            if (errno == EWOULDBLOCK)
                return false;

            throw NetworkException(std::string("socket accept failed - ") + strerror(errno));
        }

        int flags = fcntl(socketFD, F_GETFL);
        if (flags < 0)
            throw NetworkException(std::string("getting socket flags - ") + strerror(errno));
        if (fcntl(socketFD, F_SETFL, flags | O_NONBLOCK) < 0)
            throw NetworkException(std::string("setting socket flags - ") + strerror(errno));

        if (socketFD != -1)
            return true;

        return false;
    }
}
