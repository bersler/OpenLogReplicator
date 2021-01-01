/* Header for Writer class
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

#include "Thread.h"

#ifndef WRITER_H_
#define WRITER_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleAnalyzer;
    class OutputBuffer;
    struct OutputBufferMsg;
    class RedoLogRecord;

    class Writer : public Thread {
    protected:
        OutputBuffer *outputBuffer;
        uint64_t confirmedMessages;
        uint64_t sentMessages;
        uint64_t pollInterval;
        time_t previousCheckpoint;
        uint64_t checkpointInterval;
        uint64_t queueSize;
        uint64_t curQueueSize;
        uint64_t maxQueueSize;
        OutputBufferMsg **queue;
        typeSCN confirmedScn;
        typeSCN checkpointScn;
        typeSCN startScn;
        typeSEQ startSequence;
        string startTime;
        int64_t startTimeRel;
        bool streaming;

        void createMessage(OutputBufferMsg *msg);
        virtual void sendMessage(OutputBufferMsg *msg) = 0;
        virtual string getName(void) const = 0;
        virtual void pollQueue(void) = 0;
        virtual void *run(void);
        virtual void writeCheckpoint(bool force);
        virtual void readCheckpoint(void);
        void startReader(void);
        void sortQueue(void);

    public:
        OracleAnalyzer *oracleAnalyzer;
        uint64_t maxMessageMb;      //maximum message size able to handle by writer
        Writer(const char *alias, OracleAnalyzer *oracleAnalyzer, uint64_t maxMessageMb, uint64_t pollInterval,
                uint64_t checkpointInterval, uint64_t queueSize, typeSCN startScn, typeSEQ startSequence, const char* startTime,
                int64_t startTimeRel);
        virtual ~Writer();
        void confirmMessage(OutputBufferMsg *msg);
    };
}

#endif
