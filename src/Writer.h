/* Header for Writer class
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

#include <string>
#include <pthread.h>

#include "types.h"
#include "Thread.h"

#ifndef WRITER_H_
#define WRITER_H_

using namespace std;

namespace OpenLogReplicator {

#define TRANSACTION_INSERT 1
#define TRANSACTION_DELETE 2
#define TRANSACTION_UPDATE 3

    class OutputBuffer;
    class OracleAnalyser;
    class RedoLogRecord;

    class Writer : public Thread {
    protected:
        OutputBuffer *outputBuffer;
        OracleAnalyser *oracleAnalyser;
        uint64_t shortMessage;
        uint8_t *msgBuffer;

        virtual void sendMessage(uint8_t *buffer, uint64_t length, bool dealloc) = 0;
        virtual string getName() = 0;
        virtual void *run(void);

    public:
        uint64_t maxMessageMb;      //maximum message size able to handle by writer

        void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type);
        void parseDDL(RedoLogRecord *redoLogRecord1);

        Writer(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t shortMessage, uint64_t maxMessageMb);
        virtual ~Writer();
    };
}

#endif
