/* Header for Stream class
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

#include <atomic>
#include "types.h"

#ifndef STREAM_H_
#define STREAM_H_

#define READ_NETWORK_BUFFER         1024

using namespace std;

namespace OpenLogReplicator {
    class Stream {
    protected:
        atomic<bool> *shutdown;
        uint64_t pollInterval;
        string uri;

    public:
        virtual string getName(void) const = 0;
        virtual void initializeClient(atomic<bool> *shutdown) = 0;
        virtual void initializeServer(atomic<bool> *shutdown) = 0;
        virtual void sendMessage(const void *msg, uint64_t length) = 0;
        virtual uint64_t receiveMessage(void *msg, uint64_t length) = 0;
        virtual uint64_t receiveMessageNB(void *msg, uint64_t length) = 0;
        virtual bool connected(void) = 0;

        Stream(const char *uri, uint64_t pollInterval);
        virtual ~Stream();
    };
}

#endif
