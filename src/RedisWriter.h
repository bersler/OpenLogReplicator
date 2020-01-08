/* Header for RedisWriter class
   Copyright (C) 2018-2020 Adam Leszczynski.

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

#include <set>
#include <queue>
#include <stdint.h>
#include <occi.h>
#include <hiredis.h>
#include "types.h"
#include "Writer.h"

#ifndef REDISWRITER_H_
#define REDISWRITER_H_

using namespace std;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class CommandBuffer;

    class RedisWriter : public Writer {
    protected:
        string host;
        uint32_t port;
        redisContext *c;

    public:
        virtual void *run();

        void addTable(string mask);
        int initialize();

        virtual void beginTran(typescn scn, typexid xid);
        virtual void next();
        virtual void commitTran();
        virtual void parseInsert(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        virtual void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment);
        virtual void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment);
        virtual void parseUpdate(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment);
        virtual void parseDelete(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        virtual void parseDDL(RedoLogRecord *redoLogRecord1, OracleEnvironment *oracleEnvironment);

        RedisWriter(const string alias, const string host, uint32_t port, CommandBuffer *commandBuffer);
        virtual ~RedisWriter();
    };
}

#endif
