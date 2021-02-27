/* Header for StreamZeroMQ class
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

#include "Stream.h"

#ifndef STREAMZEROMQ_H_
#define STREAMZEROMQ_H_

using namespace std;

namespace OpenLogReplicator {
    class StreamZeroMQ : public Stream {
    protected:
        void *socket;
        void *context;

    public:
        virtual string getName(void) const;
        virtual void initializeClient(atomic<bool> *shutdown);
        virtual void initializeServer(atomic<bool> *shutdown);
        virtual void sendMessage(const void *msg, uint64_t length);
        virtual uint64_t receiveMessage(void *msg, uint64_t length);
        virtual uint64_t receiveMessageNB(void *msg, uint64_t length);
        virtual bool connected(void);

        StreamZeroMQ(const char *uri, uint64_t pollInterval);
        virtual ~StreamZeroMQ();
    };
}

#endif
