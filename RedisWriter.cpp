/* Thread writing to Redis stream
   Copyright (C) 2018-2019 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <sys/stat.h>
#include <string>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <mutex>
#include <unistd.h>
#include <string.h>
#include <hiredis.h>
#include "types.h"
#include "RedisWriter.h"
#include "CommandBuffer.h"

using namespace std;

namespace OpenLogReplicatorRedis {

	RedisWriter::RedisWriter(const string alias, const string host, uint32_t port, CommandBuffer *commandBuffer) :
		Thread(alias, commandBuffer),
		host(host),
		port(port),
		c(nullptr) {
	}

	RedisWriter::~RedisWriter() {
	    redisFree(c);
	}

	void *RedisWriter::run() {
		cout << "- Redis Writer for " << host << ":" << port << endl;

		while (!this->shutdown) {
			uint32_t length, lengthProcessed;
			{
				unique_lock<mutex> lck(commandBuffer->mtx);
				while (commandBuffer->posStart == commandBuffer->posEnd) {
					commandBuffer->readers.wait(lck);

					if (this->shutdown)
						break;
				}
				if (this->shutdown)
					break;

				if (commandBuffer->posStart == commandBuffer->posSize && commandBuffer->posSize > 0) {
					commandBuffer->posStart = 0;
					commandBuffer->posSize = 0;
				}
				length = *((uint32_t*)(commandBuffer->intraThreadBuffer + commandBuffer->posStart));
			}

			redisReply *reply;
			reply = (redisReply *)redisCommand(c, "MULTI");
		    freeReplyObject(reply);

		    lengthProcessed = 4;
		    while (lengthProcessed < length) {
				//FIXME: waste of time to run strlen
		    	const char *key = (const char *)commandBuffer->intraThreadBuffer + commandBuffer->posStart + lengthProcessed;
				uint32_t keylen = strlen(key);
		    	const char *cmd = (const char *)commandBuffer->intraThreadBuffer + commandBuffer->posStart + lengthProcessed + keylen + 1;
				uint32_t cmdlen = strlen(cmd);

		    	cout << "SET [" << key << "] [" << cmd << "]" << endl;
				reply = (redisReply *)redisCommand(c, "SET %s %s", key, cmd);
			    //cout << "RET [" << reply->str << "]" << endl;
				freeReplyObject(reply);

				lengthProcessed += keylen + 1 + cmdlen + 1;
		    }

			reply = (redisReply *)redisCommand(c, "EXEC");
		    freeReplyObject(reply);

			{
				unique_lock<mutex> lck(commandBuffer->mtx);
				commandBuffer->posStart += (length + 3) & 0xFFFFFFFC;

				if (commandBuffer->posStart == commandBuffer->posSize && commandBuffer->posSize > 0) {
					commandBuffer->posStart = 0;
					commandBuffer->posSize = 0;
				}

				commandBuffer->writer.notify_all();
			}
		}

		return 0;
	}

	int RedisWriter::initialize() {
		c = redisConnect(host.c_str(), port);

		redisReply *reply = (redisReply *)redisCommand(c, "PING");
		if (reply != nullptr)
			printf("PING: %s\n", reply->str);
	    freeReplyObject(reply);

	    if (c->err) {
	        cerr << "ERROR: Redis: " << c->errstr << endl;
	        return 0;
	    }

		return 1;
	}
}
