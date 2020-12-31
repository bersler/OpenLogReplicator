/* Header for WriterFile class
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

#include "Writer.h"

#ifndef WRITERFILE_H_
#define WRITERFILE_H_

using namespace std;

namespace OpenLogReplicator {

    class RedoLogRecord;
    class OracleAnalyzer;

    class WriterFile : public Writer {
    protected:
        string name;
        ostream *output;
        bool fileOpen;
        virtual void sendMessage(OutputBufferMsg *msg);
        virtual string getName() const;
        virtual void pollQueue(void);

    public:
        WriterFile(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *name, uint64_t pollInterval,
                uint64_t checkpointInterval, uint64_t queueSize, typeSCN startScn, typeSEQ startSeq, const char* startTime,
                uint64_t startTimeRel);
        virtual ~WriterFile();
    };
}

#endif
