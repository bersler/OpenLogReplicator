/* Header for StreamNetwork class
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

#include <netinet/in.h>

#include "Stream.h"

#ifndef STREAMNETWORK_H_
#define STREAMNETWORK_H_

using namespace std;

namespace OpenLogReplicator {

    class StreamNetwork : public Stream {
    protected:
        int64_t socketFD, serverFD;
        struct sockaddr_storage address;
        string host, port;
        uint8_t readBuffer[READ_NETWORK_BUFFER];
        uint64_t readBufferLen;

    public:
        virtual string getName(void) const;
        virtual void initializeClient(volatile bool *shutdown);
        virtual void initializeServer(volatile bool *shutdown);
        virtual void sendMessage(const void *msg, uint64_t length);
        virtual uint64_t receiveMessage(void *msg, uint64_t length);
        virtual uint64_t receiveMessageNB(void *msg, uint64_t length);
        virtual bool connected(void);

        StreamNetwork(const char *uri, uint64_t pollInterval);
        virtual ~StreamNetwork();
    };
}

#endif
