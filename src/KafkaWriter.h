/* Header for KafkaWriter class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <queue>
#include <set>
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
        uint8_t *msgBuffer;
        Conf *conf;
        Conf *tconf;
        string brokers;
        string topic;
        Producer *producer;
        Topic *ktopic;
        typetime lastTime;
        typescn lastScn;

        uint64_t afterPos[MAX_NO_COLUMNS];
        uint64_t beforePos[MAX_NO_COLUMNS];
        uint16_t afterLen[MAX_NO_COLUMNS];
        uint16_t beforeLen[MAX_NO_COLUMNS];
        uint8_t colIsSupp[MAX_NO_COLUMNS];
        RedoLogRecord *beforeRecord[MAX_NO_COLUMNS];
        RedoLogRecord *afterRecord[MAX_NO_COLUMNS];

        void sendMessage(uint8_t *buffer, uint64_t length, bool dealloc);

    public:
        void initialize(void);
        virtual void *run(void);
        virtual void beginTran(typescn scn, typetime time, typexid xid);
        virtual void next(void);
        virtual void commitTran(void);
        virtual void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        virtual void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        virtual void parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type);
        virtual void parseDDL(RedoLogRecord *redoLogRecord1);

        KafkaWriter(const char *alias, const char *brokers, const char *topic, OracleAnalyser *oracleAnalyser, uint64_t maxMessageKb, uint64_t stream,
                uint64_t singleDml, uint64_t showColumns, uint64_t test, uint64_t timestampFormat, uint64_t charFormat);
        virtual ~KafkaWriter();
    };
}

#endif
