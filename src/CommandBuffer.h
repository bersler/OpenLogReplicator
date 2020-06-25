/* Header for CommandBuffer class
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

#include <condition_variable>
#include <mutex>
#include <string>
#include <stdint.h>

#include "types.h"

#ifndef COMMANDBUFFER_H_
#define COMMANDBUFFER_H_

using namespace std;

namespace OpenLogReplicator {

    class Writer;
    class RedoLogRecord;
    class OracleAnalyser;
    class OracleObject;

    class CommandBuffer {
    protected:
        volatile bool shutdown;
        OracleAnalyser *oracleAnalyser;
    public:
        static char translationMap[65];
        Writer *writer;
        uint8_t *intraThreadBuffer;
        mutex mtx;
        condition_variable analysersCond;
        condition_variable writerCond;
        volatile uint64_t posStart;
        volatile uint64_t posEnd;
        volatile uint64_t posEndTmp;
        volatile uint64_t posSize;
        uint64_t test;
        uint64_t timestampFormat;
        uint64_t outputBufferSize;

        void stop(void);
        void setOracleAnalyser(OracleAnalyser *oracleAnalyser);
        CommandBuffer* appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot);
        CommandBuffer* appendEscape(const uint8_t *str, uint64_t length);
        CommandBuffer* appendChr(const char* str);
        CommandBuffer* appendStr(string &str);
        CommandBuffer* append(char chr);
        CommandBuffer* appendHex(uint64_t val, uint64_t length);
        CommandBuffer* appendDec(uint64_t val);
        CommandBuffer* appendScn(typescn scn);
        CommandBuffer* appendOperation(char *operation);
        CommandBuffer* appendTable(string &owner, string &table);
        CommandBuffer* appendTimestamp(const uint8_t *data, uint64_t length);
        CommandBuffer* appendValue(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength);
        CommandBuffer* appendNull(string &columnName);
        CommandBuffer* appendMs(char *name, uint64_t time);
        CommandBuffer* appendXid(typexid xid);
        CommandBuffer* appendDbzCols(OracleObject *object);
        CommandBuffer* appendDbzHead(OracleObject *object);
        CommandBuffer* appendDbzTail(OracleObject *object, uint64_t time, typescn scn, char op, typexid xid);

        CommandBuffer* beginTran(void);
        CommandBuffer* commitTran(void);
        CommandBuffer* rewind(void);
        uint64_t currentTranSize(void);

        CommandBuffer(uint64_t outputBufferSize);
        virtual ~CommandBuffer();
    };
}

#endif
