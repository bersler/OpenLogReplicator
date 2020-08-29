/* Header for OutputBuffer class
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

#ifndef OUTPUTBUFFER_H_
#define OUTPUTBUFFER_H_

#define OUTPUT_BUFFER_NEXT          0
#define OUTPUT_BUFFER_END           (sizeof(uint8_t*))
#define OUTPUT_BUFFER_DATA          (sizeof(uint8_t*)+sizeof(uint64_t))
#define OUTPUT_BUFFER_LENGTH_SIZE   (sizeof(uint64_t))

using namespace std;

namespace OpenLogReplicator {

    class CharacterSet;
    class OracleAnalyser;
    class OracleObject;
    class RedoLogRecord;
    class Writer;

    class OutputBuffer {
    protected:
        OracleAnalyser *oracleAnalyser;
        static const char translationMap[65];
        uint64_t timestampFormat;
        uint64_t charFormat;
        uint64_t scnFormat;
        uint64_t unknownFormat;
        uint64_t showColumns;
        uint64_t messageLength;
        unordered_map<uint16_t, const char*> timeZoneMap;
        typetime lastTime;
        typescn lastScn;

        void bufferAppend(uint8_t character);
        void bufferShift(uint64_t bytes);
        void beginMessage(void);
        void commitMessage(void);
        void appendChr(const char* str);
        void appendStr(string &str);
        void append(char chr);
        void appendHex(uint64_t val, uint64_t length);
        void appendDec(uint64_t val);
        void appendTimestamp(const uint8_t *data, uint64_t length);

    public:
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

        uint64_t afterPos[MAX_NO_COLUMNS];
        uint64_t beforePos[MAX_NO_COLUMNS];
        uint16_t afterLen[MAX_NO_COLUMNS];
        uint16_t beforeLen[MAX_NO_COLUMNS];
        uint8_t colIsSupp[MAX_NO_COLUMNS];
        RedoLogRecord *beforeRecord[MAX_NO_COLUMNS];
        RedoLogRecord *afterRecord[MAX_NO_COLUMNS];

        OutputBuffer(uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat, uint64_t showColumns);
        virtual ~OutputBuffer();

        void initialize(OracleAnalyser *oracleAnalyser);
        uint64_t currentMessageSize(void);
        void setWriter(Writer *writer);
        void setNlsCharset(string &nlsCharset, string &nlsNcharCharset);

        virtual void appendInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void appendUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void appendDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void appendDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) = 0;
        virtual void next(void);
        virtual void beginTran(typescn scn, typetime time, typexid xid);
        virtual void commitTran(void);
    };
}

#endif
