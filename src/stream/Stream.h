/* Header for Stream class
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

#include "../common/types.h"

#ifndef STREAM_H_
#define STREAM_H_

namespace OpenLogReplicator {
    class Ctx;

    class Stream {
    protected:
        Ctx* ctx;
        std::string uri;

    public:
        static constexpr uint64_t READ_NETWORK_BUFFER = 1024;

        Stream(Ctx* newCtx, const char* newUri);
        virtual ~Stream();

        [[nodiscard]] virtual std::string getName() const = 0;
        virtual void initializeClient() = 0;
        virtual void initializeServer() = 0;
        virtual void sendMessage(const void* msg, uint64_t length) = 0;
        virtual uint64_t receiveMessage(void* msg, uint64_t length) = 0;
        virtual uint64_t receiveMessageNB(void* msg, uint64_t length) = 0;
        [[nodiscard]] virtual bool isConnected() = 0;
        virtual void initialize() = 0;
    };
}

#endif
