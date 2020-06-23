/* Header for KafkaWriter class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <queue>
#include <set>
#include <occi.h>
#include <stdint.h>
#include <librdkafka/rdkafkacpp.h>

#include "types.h"
#include "Writer.h"

#ifndef KAFKAWRITER_H_
#define KAFKAWRITER_H_

using namespace std;
using namespace RdKafka;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class OracleAnalyser;

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
        typetime lastTime;
        typescn lastScn;

        uint64_t *afterPos;
        uint64_t *beforePos;
        uint16_t *afterLen;
        uint16_t *beforeLen;
        uint8_t *colIsSupp;
        RedoLogRecord **beforeRecord;
        RedoLogRecord **afterRecord;

    public:
        virtual void *run(void);

        void addTable(string &mask);
        void initialize(void);

        virtual void beginTran(typescn scn, typetime time, typexid xid);
        virtual void next(void);
        virtual void commitTran(void);
        virtual void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        virtual void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        virtual void parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type);
        virtual void parseDDL(RedoLogRecord *redoLogRecord1);

        KafkaWriter(string alias, string brokers, string topic, OracleAnalyser *oracleAnalyser, uint64_t trace, uint64_t trace2,
                uint64_t stream, uint64_t metadata, uint64_t singleDml, uint64_t showColumns, uint64_t test, uint64_t timestampFormat);
        virtual ~KafkaWriter();
    };
}

#endif
