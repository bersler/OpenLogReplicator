/* Header for Writer class
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTfY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <string>
#include <pthread.h>
#include "types.h"
#include "Thread.h"

#ifndef WRITER_H_
#define WRITER_H_

using namespace std;

namespace OpenLogReplicator {

    class CommandBuffer;
    class RedoLogRecord;
    class OracleEnvironment;

    class Writer : public Thread {

    public:
        void terminate(void);
        virtual void *run() = 0;
        int initialize();

        void appendValue(RedoLogRecord *redoLogRecord, uint32_t typeNo, uint32_t fieldPos, uint32_t fieldLength);
        virtual void beginTran(typescn scn, typexid xid) = 0;
        virtual void next() = 0;
        virtual void commitTran() = 0;
        virtual void parseInsert(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) = 0;
        virtual void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) = 0;
        virtual void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) = 0;
        virtual void parseUpdate(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleEnvironment *oracleEnvironment) = 0;
        virtual void parseDelete(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) = 0;
        virtual void parseDDL(RedoLogRecord *redoLogRecord1, OracleEnvironment *oracleEnvironment) = 0;

        Writer(const string alias, CommandBuffer *commandBuffer);
        virtual ~Writer();
    };
}

#endif
