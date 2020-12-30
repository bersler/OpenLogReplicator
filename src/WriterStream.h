/* Header for WriterStream class
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

#include "OraProtoBuf.pb.h"
#include "Writer.h"

#ifndef WRITERSTREAM_H_
#define WRITERSTREAM_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleAnalyzer;
    class Stream;

    class WriterStream : public Writer {
    protected:
        Stream *stream;
        pb::RedoRequest request;
        pb::RedoResponse response;

        virtual string getName(void);
        virtual void readCheckpoint(void);
        void processInfo(void);
        void processStart(void);
        void processRedo(void);
        void processConfirm(void);
        virtual void pollQueue(void);
        virtual void sendMessage(OutputBufferMsg *msg);

    public:
        WriterStream(const char *alias, OracleAnalyzer *oracleAnalyzer, uint64_t pollInterval, uint64_t checkpointInterval,
                uint64_t queueSize, typeSCN startScn, typeSEQ startSeq, const char* startTime, uint64_t startTimeRel,
                Stream *stream);
        virtual ~WriterStream();
    };
}

#endif
