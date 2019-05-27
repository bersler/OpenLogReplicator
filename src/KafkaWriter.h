/* Header for KafkaWriter class
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

#include <set>
#include <queue>
#include <stdint.h>
#include <occi.h>
#include <librdkafka/rdkafkacpp.h>
#include "types.h"
#include "Thread.h"

#ifndef KAFKAWRITER_H_
#define KAFKAWRITER_H_

using namespace std;
using namespace OpenLogReplicator;
using namespace RdKafka;

namespace OpenLogReplicator {
    class CommandBuffer;
}

namespace OpenLogReplicatorKafka {

    class KafkaWriter : public Thread {
    protected:
        Conf *conf;
        Conf *tconf;
        string brokers;
        string topic;
        Producer *producer;
        Topic *ktopic;

    public:
        virtual void *run();

        void addTable(string mask);
        int initialize();

        KafkaWriter(const string alias, const string brokers, const string topic, CommandBuffer *commandBuffer);
        virtual ~KafkaWriter();
    };
}

#endif
