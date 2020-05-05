/* Header for CommandBuffer class
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

#include <stdint.h>
#include <string>
#include <mutex>
#include <condition_variable>
#include "types.h"

#ifndef COMMANDBUFFER_H_
#define COMMANDBUFFER_H_

using namespace std;

namespace OpenLogReplicator {

    class Writer;
    class RedoLogRecord;
    class OracleReader;

    class CommandBuffer {
    protected:
        volatile bool shutdown;
        OracleReader *oracleReader;
    public:
        static char translationMap[65];
        Writer *writer;
        uint8_t *intraThreadBuffer;
        mutex mtx;
        condition_variable readersCond;
        condition_variable writerCond;
        volatile uint64_t posStart;
        volatile uint64_t posEnd;
        volatile uint64_t posEndTmp;
        volatile uint64_t posSize;
        uint64_t outputBufferSize;

        void stop(void);
        void setOracleReader(OracleReader *oracleReader);
        CommandBuffer* appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot);
        CommandBuffer* appendEscape(const uint8_t *str, uint64_t length);
        CommandBuffer* append(const string str);
        CommandBuffer* append(char chr);
        CommandBuffer* appendHex(uint64_t val, uint64_t length);
        CommandBuffer* appendDec(uint64_t val);
        CommandBuffer* appendScn(uint64_t test, typescn scn);
        CommandBuffer* appendOperation(string operation);
        CommandBuffer* appendTable(string owner, string table);
        CommandBuffer* appendValue(string columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength);
        CommandBuffer* appendNull(string columnName);
        CommandBuffer* appendTimestamp(typetime time);
        CommandBuffer* appendXid(typexid xid);
        CommandBuffer* beginTran();
        CommandBuffer* commitTran();
        CommandBuffer* rewind();
        uint64_t currentTranSize();

        CommandBuffer(uint64_t outputBufferSize);
        virtual ~CommandBuffer();
    };
}

#endif
