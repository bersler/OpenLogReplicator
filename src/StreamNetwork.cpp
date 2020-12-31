/* Base class for streaming using network sockets
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include "NetworkException.h"
#include "RuntimeException.h"
#include "StreamNetwork.h"

using namespace std;

namespace OpenLogReplicator {

    StreamNetwork::StreamNetwork(const char *uri, uint64_t pollInterval) :
        Stream(uri, pollInterval),
        socketFD(-1),
        serverFD(-1),
        readBufferLen(0) {

        uint64_t colon = this->uri.find(":");
        if (colon == string::npos) {
            RUNTIME_FAIL("uri is missing \":\"");
        }
        host = this->uri.substr(0, colon);
        port = this->uri.substr(colon + 1, this->uri.length() - 1);
    }

    StreamNetwork::~StreamNetwork() {
        if (socketFD != -1) {
            close(socketFD);
            socketFD = -1;
        }

        if (serverFD != -1) {
            close(serverFD);
            serverFD = -1;
        }
    }

    string StreamNetwork::getName(void) const {
        return "Network:" + uri;
    }

    void StreamNetwork::initializeClient(volatile bool *shutdown) {
        this->shutdown = shutdown;
        struct sockaddr_in addressC;
        memset((uint8_t*)&addressC, 0, sizeof(addressC));
        addressC.sin_family = AF_INET;
        addressC.sin_port = htons(atoi(port.c_str()));

        if ((socketFD = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
            RUNTIME_FAIL("socket creation failed");
        }

        struct hostent *server = gethostbyname(host.c_str());
        if (server == NULL) {
            RUNTIME_FAIL("error resolving host name: " << host);
        }

        memcpy((char *)&addressC.sin_addr.s_addr, (char *)server->h_addr, server->h_length);
        if (connect(socketFD, (struct sockaddr *) &addressC, sizeof(addressC)) < 0) {
            RUNTIME_FAIL("error connecting to uri: " << uri);
        }
    }

    void StreamNetwork::initializeServer(volatile bool *shutdown) {
        this->shutdown = shutdown;
        struct addrinfo hints, *res;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;

        getaddrinfo(host.c_str(), port.c_str(), &hints, &res);

        serverFD = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (serverFD == 0) {
            RUNTIME_FAIL("socket creation failed");
        }

        int flags = fcntl(serverFD, F_GETFL);
        if (flags < 0) {
            RUNTIME_FAIL("error getting socket flags");
        }
        if (fcntl(serverFD, F_SETFL, flags | O_NONBLOCK) < 0) {
            RUNTIME_FAIL("error setting socket flags");
        }

        int64_t opt = 1;
        if (setsockopt(serverFD, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
            RUNTIME_FAIL("socket reusing failed");
        }

        if (bind(serverFD, res->ai_addr, res->ai_addrlen) < 0) {
            RUNTIME_FAIL("error binding uri: " << uri);
        }
        if (listen(serverFD, 1) < 0) {
            RUNTIME_FAIL("error starting listener");
        }
    }

    void StreamNetwork::sendMessage(const void *msg, uint64_t length) {
        uint32_t length32 = length;
        uint64_t sent = 0;

        if (socketFD == -1) {
            NETWORK_FAIL("network send error - no connection");
        }

        fd_set wset, w;
        FD_ZERO(&wset);
        FD_SET(socketFD, &wset);

        //header content
        if (length < 0xFFFFFFFF) {
            //32-bit length
            while (sent < sizeof(uint32_t)) {
                if (*shutdown)
                    return;

                w = wset;
                //blocking select
                select(socketFD + 1, NULL, &w, NULL, NULL);
                int r = write(socketFD, ((uint8_t*)&length32) + sent, sizeof(uint32_t) - sent);
                if (r <= 0) {
                   if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                       r = 0;
                   else {
                       close(socketFD);
                       socketFD = -1;
                       NETWORK_FAIL("network send error");
                   }
                }
                sent += r;
            }
        } else {
            //64-bit length
            length32 = 0xFFFFFFFF;
            while (sent < sizeof(uint32_t)) {
                if (*shutdown)
                    return;

                w = wset;
                //blocking select
                select(socketFD + 1, NULL, &w, NULL, NULL);
                int r = write(socketFD, ((uint8_t*)&length32) + sent, sizeof(uint32_t) - sent);
                if (r <= 0) {
                   if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                       r = 0;
                   else {
                       close(socketFD);
                       socketFD = -1;
                       NETWORK_FAIL("network send error");
                   }
                }
                sent += r;
            }

            sent = 0;
            while (sent < sizeof(uint64_t)) {
                if (*shutdown)
                    return;

                w = wset;
                //blocking select
                select(socketFD + 1, NULL, &w, NULL, NULL);
                int r = write(socketFD, ((uint8_t*)&length) + sent, sizeof(uint64_t) - sent);
                if (r <= 0) {
                   if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                       r = 0;
                   else {
                       close(socketFD);
                       socketFD = -1;
                       NETWORK_FAIL("network send error");
                   }
                }
                sent += r;
            }
        }

        sent = 0;
        //message content
        while (sent < length) {
            if (*shutdown)
                return;

            w = wset;
            //blocking select
            select(socketFD + 1, NULL, &w, NULL, NULL);
            int r = write(socketFD, msg + sent, length - sent);
            if (r <= 0) {
               if (r < 0 && (errno == EWOULDBLOCK || errno == EAGAIN))
                   r = 0;
               else {
                   close(socketFD);
                   socketFD = -1;
                   NETWORK_FAIL("network send error");
               }
            }
            sent += r;
        }
    }

    uint64_t StreamNetwork::receiveMessage(void *msg, uint64_t length) {
        uint64_t recvd = 0;

        //read message length
        while (recvd < sizeof(uint32_t)) {
            if (*shutdown)
                return 0;

            int64_t bytes = read(socketFD, msg + recvd, sizeof(uint32_t) - recvd);

            if (bytes > 0)
                recvd += bytes;
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                NETWORK_FAIL("host disconnected");
            } else {
                close(socketFD);
                socketFD = -1;
                NETWORK_FAIL("network receive error");
            }
        }

        if (*((uint32_t*)msg) < 0xFFFFFFFF) {
            //32-bit message length
            if (length < *((uint32_t*)msg)) {
                RUNTIME_FAIL("read buffer too small");
            }

            length = *((uint32_t*)msg);
            recvd = 0;
        } else {
            //64-bit message length
            recvd = 0;

            while (recvd < sizeof(uint64_t)) {
                if (*shutdown)
                    return 0;

                int64_t bytes = read(socketFD, msg + recvd, sizeof(uint64_t) - recvd);

                if (bytes > 0)
                    recvd += bytes;
                else if (bytes == 0) {
                    close(socketFD);
                    socketFD = -1;
                    NETWORK_FAIL("host disconnected");
                } else {
                    close(socketFD);
                    socketFD = -1;
                    NETWORK_FAIL("network receive error");
                }
            }

            if (length < *((uint64_t*)msg)) {
                RUNTIME_FAIL("read buffer too small");
            }

            length = *((uint64_t*)msg);
            recvd = 0;
        }

        while (recvd < length) {
            if (*shutdown)
                return 0;

            int64_t bytes = read(socketFD, msg + recvd, length - recvd);

            if (bytes > 0)
                recvd += bytes;
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                NETWORK_FAIL("host disconnected");
            } else {
                close(socketFD);
                socketFD = -1;
                NETWORK_FAIL("network receive error");
            }
        }

        return recvd;
    }

    uint64_t StreamNetwork::receiveMessageNB(void *msg, uint64_t length) {
        uint64_t recvd = 0;

        //read message length
        while (recvd < sizeof(uint32_t)) {
            if (*shutdown)
                return 0;

            int64_t bytes = read(socketFD, msg + recvd, sizeof(uint32_t) - recvd);

            if (bytes > 0)
                recvd += bytes;
            //client disconnected
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                NETWORK_FAIL("host disconnected");
            } else {
                if (recvd == 0)
                    return 0;

                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    usleep(pollInterval);
                else
                    return 0;
            }
        }

        if (*((uint32_t*)msg) < 0xFFFFFFFF) {
            //32-bit message length
            if (length < *((uint32_t*)msg)) {
                RUNTIME_FAIL("read buffer too small");
            }

            length = *((uint32_t*)msg);
            recvd = 0;
        } else {
            //64-bit message length
            recvd = 0;

            while (recvd < sizeof(uint64_t)) {
                if (*shutdown)
                    return 0;

                int64_t bytes = read(socketFD, msg + recvd, sizeof(uint64_t) - recvd);

                if (bytes > 0)
                    recvd += bytes;
                //client disconnected
                else if (bytes == 0) {
                    close(socketFD);
                    socketFD = -1;
                    NETWORK_FAIL("host disconnected");
                } else {
                    if (recvd == 0)
                        return 0;

                    if (errno == EWOULDBLOCK || errno == EAGAIN)
                        usleep(pollInterval);
                    else
                        return 0;
                }
            }

            if (length < *((uint64_t*)msg)) {
                RUNTIME_FAIL("read buffer too small");
            }

            length = *((uint64_t*)msg);
            recvd = 0;
        }

        while (recvd < length) {
            int64_t bytes = read(socketFD, msg + recvd, length - recvd);

            if (bytes > 0)
                recvd += bytes;
            //client disconnected
            else if (bytes == 0) {
                close(socketFD);
                socketFD = -1;
                NETWORK_FAIL("host disconnected");
            } else {
                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    usleep(pollInterval);
                else
                    return 0;
            }
        }

        return recvd;
    }

    bool StreamNetwork::connected(void) {
        if (socketFD != -1)
            return true;

        int64_t addrlen = sizeof(address);
        socketFD = accept(serverFD, (struct sockaddr*)&address, (socklen_t*)&addrlen);
        if (socketFD < 0) {
            if (errno == EWOULDBLOCK)
                return false;

            RUNTIME_FAIL("socket accept failed");
        }

        int flags = fcntl(socketFD, F_GETFL);
        if (flags < 0) {
            RUNTIME_FAIL("error getting socket flags");
        }
        if (fcntl(socketFD, F_SETFL, flags | O_NONBLOCK) < 0) {
            RUNTIME_FAIL("error setting socket flags");
        }

        if (socketFD != -1)
            return true;

        return false;
    }
}
