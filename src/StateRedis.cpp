/* Base class for state in Redis
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

#include "RuntimeException.h"
#include "StateRedis.h"

namespace OpenLogReplicator {
    StateRedis::StateRedis(const char* server, uint16_t port) :
        State(),
        server(server),
        port(port),
        c(nullptr) {

        c = redisConnect(this->server.c_str(), this->port);

        redisReply *reply = (redisReply *)redisCommand(c, "PING");
        DEBUG("REDIS: " << reply->str);
        freeReplyObject(reply);

        if (c->err) {
            RUNTIME_FAIL("Redis error: " << c->errstr);
        }
    }

    StateRedis::~StateRedis() {
        if (c != nullptr) {
            redisFree(c);
            c = nullptr;
        }
    }

    void StateRedis::list(std::set<std::string>& namesList) {

    }

    bool StateRedis::read(std::string& name, uint64_t maxSize, std::string& in, bool noFail) {
        return true;
    }

    void StateRedis::write(std::string& name, std::stringstream& out) {

    }

    void StateRedis::drop(std::string& name) {

    }
}
