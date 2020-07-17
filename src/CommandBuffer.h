/* Header for CommandBuffer class
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

#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <stdint.h>

#include "types.h"

#ifndef COMMANDBUFFER_H_
#define COMMANDBUFFER_H_

#define MAX_KAFKA_MESSAGE_MB        953

#define KAFKA_BUFFER_NEXT           0
#define KAFKA_BUFFER_END            (sizeof(uint8_t*))
#define KAFKA_BUFFER_DATA           (sizeof(uint8_t*)+sizeof(uint64_t))
#define KAFKA_BUFFER_LENGTH_SIZE    (sizeof(uint64_t))


using namespace std;

namespace OpenLogReplicator {

    class CharacterSet;
    class OracleAnalyser;
    class OracleObject;
    class RedoLogRecord;
    class Writer;

    class CommandBuffer {
    protected:
        OracleAnalyser *oracleAnalyser;
        static char translationMap[65];
        uint64_t test;
        uint64_t timestampFormat;
        uint64_t charFormat;

        uint64_t messageLength;
        void bufferAppend(uint8_t character);
        void bufferShift(uint64_t bytes);

    public:
        //uint64_t outputBufferSize;
        uint64_t defaultCharacterMapId;
        uint64_t defaultCharacterNcharMapId;
        unordered_map<uint64_t, CharacterSet*> characterMap;
        Writer *writer;
        mutex mtx;
        condition_variable analysersCond;
        condition_variable writersCond;

        uint64_t buffersAllocated;
        uint64_t firstBufferPos;
        uint8_t *firstBuffer;
        uint8_t *curBuffer;
        uint64_t curBufferPos;
        uint8_t *lastBuffer;
        uint64_t lastBufferPos;

        CommandBuffer();
        virtual ~CommandBuffer();

        void initialize(OracleAnalyser *oracleAnalyser);
        CommandBuffer* beginMessage(void);
        CommandBuffer* commitMessage(void);
        uint64_t currentMessageSize(void);
        void setParameters(uint64_t test, uint64_t timestampFormat, uint64_t charFormat, Writer *writer);
        void setNlsCharset(string &nlsCharset, string &nlsNcharCharset);
        CommandBuffer* appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot);
        CommandBuffer* appendEscape(const uint8_t *str, uint64_t length);
        CommandBuffer* appendEscapeMap(const uint8_t *str, uint64_t length, uint64_t charsetId);
        CommandBuffer* appendChr(const char* str);
        CommandBuffer* appendStr(string &str);
        CommandBuffer* append(char chr);
        CommandBuffer* appendHex(uint64_t val, uint64_t length);
        CommandBuffer* appendDec(uint64_t val);
        CommandBuffer* appendScn(typescn scn);
        CommandBuffer* appendOperation(char *operation);
        CommandBuffer* appendTable(string &owner, string &table);
        CommandBuffer* appendTimestamp(const uint8_t *data, uint64_t length);
        CommandBuffer* appendUnknown(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength);
        CommandBuffer* appendValue(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t charsetId, uint64_t fieldPos, uint64_t fieldLength);
        CommandBuffer* appendNull(string &columnName);
        CommandBuffer* appendMs(char *name, uint64_t time);
        CommandBuffer* appendXid(typexid xid);
        CommandBuffer* appendDbzCols(OracleObject *object);
        CommandBuffer* appendDbzHead(OracleObject *object);
        CommandBuffer* appendDbzTail(OracleObject *object, uint64_t time, typescn scn, char op, typexid xid);
    };
}

#endif
