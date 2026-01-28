/* Header for StreamNetwork class
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

#ifndef STREAM_NETWORK_H_
#define STREAM_NETWORK_H_

#include <netinet/in.h>
#if defined(__linux__) || defined(__GLIBC__)
    #include <endian.h>
#elif defined(__APPLE__)
    #include <libkern/OSByteOrder.h>
    #define htole16(x) OSSwapHostToLittleInt16(x)
    #define htole32(x) OSSwapHostToLittleInt32(x)
    #define htole64(x) OSSwapHostToLittleInt64(x)
    #define le16toh(x) OSSwapLittleToHostInt16(x)
    #define le32toh(x) OSSwapLittleToHostInt32(x)
    #define le64toh(x) OSSwapLittleToHostInt64(x)
#endif

#include "Stream.h"

namespace OpenLogReplicator {
    class StreamNetwork final : public Stream {
    protected:
        int socketFD{-1};
        int serverFD{-1};
        sockaddr_storage address{};
        std::string host;
        std::string port;
        uint8_t readBuffer[READ_NETWORK_BUFFER]{};
        uint64_t readBufferLen{0};
        struct addrinfo* res{nullptr};
        static constexpr uint32_t MAX_LENGTH = 0xFFFFFFFF;

    public:
        StreamNetwork(Ctx* newCtx, std::string newUri);
        ~StreamNetwork() override;

        void initialize() override;
        [[nodiscard]] std::string getName() const override;
        void initializeClient() override;
        void initializeServer() override;
        void sendMessage(const void* msg, uint64_t length) override;
        uint64_t receiveMessage(void* msg, uint64_t bufferSize) override;
        uint64_t receiveMessageNB(void* msg, uint64_t bufferSize) override;
        [[nodiscard]] bool isConnected() override;
    };
}

#endif
