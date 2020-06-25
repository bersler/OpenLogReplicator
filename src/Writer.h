/* Header for Writer class
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
along with Open Log Replicator; see the file LICENSE;  If not see
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

    class CommandBuffer;
    class OracleAnalyser;
    class RedoLogRecord;

    class Writer : public Thread {
    protected:
        CommandBuffer *commandBuffer;
        OracleAnalyser *oracleAnalyser;
        uint64_t stream;            //1 - JSON, 2 - DBZ-JSON
        uint64_t metadata;          //0 - no metadata in output, 1 - metadata in output
        uint64_t singleDml;         //0 - transactions grouped, 1 - every dml is a single transaction
        uint64_t showColumns;       //0 - default, only changed columns for update, or PK, 1 - show full nulls from insert & delete, 2 - show all from redo
        uint64_t test;              //0 - normal work, 1 - don't connect to Kafka, stream output to log, 2 - like but produce simplified JSON
        uint64_t timestampFormat;   //0 - timestamp in ISO 8601 format, 1 - timestamp in Unix epoch format

    public:
        virtual void *run(void) = 0;
        uint64_t initialize(void);
        virtual void beginTran(typescn scn, typetime time, typexid xid) = 0;
        virtual void next(void) = 0;
        virtual void commitTran(void) = 0;
        virtual void parseInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) = 0;
        virtual void parseDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) = 0;
        virtual void parseDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type) = 0;
        virtual void parseDDL(RedoLogRecord *redoLogRecord1) = 0;

        Writer(const string alias, OracleAnalyser *oracleAnalyser, uint64_t stream, uint64_t metadata, uint64_t singleDml, uint64_t showColumns,
                uint64_t test, uint64_t timestampFormat);
        virtual ~Writer();
    };
}

#endif
