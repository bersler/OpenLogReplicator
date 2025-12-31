/* Header for StreamZeroMQ class
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

#ifndef STREAM_ZERO_MQ_H_
#define STREAM_ZERO_MQ_H_

#include "Stream.h"

namespace OpenLogReplicator {
    class StreamZeroMQ final : public Stream {
    protected:
        void* socket{nullptr};
        void* context{nullptr};

    public:
        StreamZeroMQ(Ctx* newCtx, std::string newUri);
        ~StreamZeroMQ() override;
        StreamZeroMQ(const StreamZeroMQ&) = delete;
        StreamZeroMQ& operator=(const StreamZeroMQ&) = delete;

        void initialize() override;
        [[nodiscard]] std::string getName() const override;
        void initializeClient() override;
        void initializeServer() override;
        void sendMessage(const void* msg, uint64_t length) override;
        uint64_t receiveMessage(void* msg, uint64_t length) override;
        uint64_t receiveMessageNB(void* msg, uint64_t length) override;
        [[nodiscard]] bool isConnected() override;
    };
}

#endif
