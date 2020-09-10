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
#include <unordered_set>
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
        uint64_t messageFormat;
        uint64_t xidFormat;
        uint64_t timestampFormat;
        uint64_t charFormat;
        uint64_t scnFormat;
        uint64_t unknownFormat;
        uint64_t schemaFormat;
        uint64_t columnFormat;
        uint64_t messageLength;
        unordered_map<uint16_t, const char*> timeZoneMap;
        unordered_set<OracleObject*> objects;
        typetime lastTime;
        typescn lastScn;
        typexid lastXid;

        void bufferAppend(uint8_t character);
        void bufferShift(uint64_t bytes);
        void beginMessage(void);
        void commitMessage(void);
        void append(const char* str);
        void append(string &str);
        void append(char chr);
        void checkUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid);

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

        OutputBuffer(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat);
        virtual ~OutputBuffer();

        void initialize(OracleAnalyser *oracleAnalyser);
        uint64_t currentMessageSize(void);
        void setWriter(Writer *writer);
        void setNlsCharset(string &nlsCharset, string &nlsNcharCharset);

        virtual void processBegin(typescn scn, typetime time, typexid xid) = 0;
        virtual void processCommit(void) = 0;
        virtual void processInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void processUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) = 0;
        //virtual void processCheckpoint(typescn scn, typetime time) = 0;
        //virtual void processSwitch(typescn scn, typetime time) = 0;
    };
}

#endif
