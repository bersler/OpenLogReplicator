/* Header for KafkaWriter class
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
#include <librdkafka/rdkafkacpp.h>
#include "types.h"
#include "Writer.h"

#ifndef KAFKAWRITER_H_
#define KAFKAWRITER_H_

using namespace std;
using namespace RdKafka;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class CommandBuffer;

    class KafkaWriter : public Writer {
    protected:
        Conf *conf;
        Conf *tconf;
        string brokers;
        string topic;
        Producer *producer;
        Topic *ktopic;
        uint64_t trace;
        uint64_t trace2;
        typescn lastScn;

    public:
        virtual void *run();

        void addTable(string mask);
        uint64_t initialize();

        virtual void beginTran(typescn scn, typexid xid);
        virtual void next();
        virtual void commitTran();
        virtual void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleReader *oracleReader);
        virtual void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, OracleReader *oracleReader);
        virtual void parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type, OracleReader *oracleReader);
        virtual void parseDDL(RedoLogRecord *redoLogRecord1, OracleReader *oracleReader);

        KafkaWriter(const string alias, const string brokers, const string topic, CommandBuffer *commandBuffer, uint64_t trace, uint64_t trace2,
                uint64_t stream, uint64_t sortColumns, uint64_t metadata, uint64_t singleDml, uint64_t nullColumns, uint64_t test);
        virtual ~KafkaWriter();
    };
}

#endif
