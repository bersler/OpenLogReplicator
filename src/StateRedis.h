/* Header for StateRedis class
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

#include <hiredis.h>

#include "State.h"

#ifndef STATEREDIS_H_
#define STATEREDIS_H_

namespace OpenLogReplicator {
    class StateRedis : public State {
    protected:
        std::string server;
        uint16_t port;
        redisContext *c;

    public:
        StateRedis(const char* server, uint16_t port);
        virtual ~StateRedis();

        virtual void list(std::set<std::string>& namesList);
        virtual bool read(std::string& name, uint64_t maxSize, std::string& in, bool noFail);
        virtual void write(std::string& name, std::stringstream& out);
        virtual void drop(std::string& name);
    };
}

#endif
