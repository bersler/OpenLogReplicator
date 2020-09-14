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
    class OracleColumn;
    class RedoLogRecord;
    class Writer;

    class OutputBuffer {
    protected:
        static const char map64[65];
        static const char map16[17];
        OracleAnalyser *oracleAnalyser;
        uint64_t messageFormat;
        uint64_t xidFormat;
        uint64_t timestampFormat;
        uint64_t charFormat;
        uint64_t scnFormat;
        uint64_t unknownFormat;
        uint64_t schemaFormat;
        uint64_t columnFormat;
        uint64_t messageLength;
        char valueBuffer[MAX_FIELD_LENGTH];
        uint64_t valueLength;
        unordered_map<uint16_t, const char*> timeZoneMap;
        unordered_set<OracleObject*> objects;
        typetime lastTime;
        typescn lastScn;
        typexid lastXid;

        void outputBufferShift(uint64_t bytes);
        void outputBufferBegin(void);
        void outputBufferCommit(void);
        void outputBufferAppend(char character);
        void outputBufferAppend(const char* str, uint64_t length);
        void outputBufferAppend(const char* str);
        void outputBufferAppend(string &str);
        void columnUnknown(string &columnName, const uint8_t *data, uint64_t length);
        virtual void columnNull(OracleColumn *column) = 0;
        virtual void columnFloat(string &columnName, float value) = 0;
        virtual void columnDouble(string &columnName, double value) = 0;
        virtual void columnString(string &columnName) = 0;
        virtual void columnNumber(string &columnName, uint64_t precision, uint64_t scale) = 0;
        virtual void columnRaw(string &columnName, const uint8_t *data, uint64_t length) = 0;
        virtual void columnTimestamp(string &columnName, struct tm &time, uint64_t fraction, const char *tz) = 0;
        void valueBufferAppend(uint8_t value);
        void valueBufferAppendHex(typeunicode value, uint64_t length);
        void compactUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        void processValue(OracleColumn *column, const uint8_t *data, uint64_t length, uint64_t typeNo, uint64_t charsetId);
        virtual void appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) = 0;
        virtual void appendHeader(bool first) = 0;
        virtual void appendSchema(OracleObject *object) = 0;

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

        uint8_t *afterPos[MAX_NO_COLUMNS];
        uint8_t *beforePos[MAX_NO_COLUMNS];
        uint16_t afterLen[MAX_NO_COLUMNS];
        uint16_t beforeLen[MAX_NO_COLUMNS];
        uint8_t colIsSupp[MAX_NO_COLUMNS];

        OutputBuffer(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat);
        virtual ~OutputBuffer();

        void initialize(OracleAnalyser *oracleAnalyser);
        uint64_t outputBufferSize(void);
        void setWriter(Writer *writer);
        void setNlsCharset(string &nlsCharset, string &nlsNcharCharset);

        virtual void processBegin(typescn scn, typetime time, typexid xid) = 0;
        virtual void processCommit(void) = 0;
        virtual void processInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void processUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) = 0;
        virtual void processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength) = 0;
        //virtual void processCheckpoint(typescn scn, typetime time) = 0;
        //virtual void processSwitch(typescn scn, typetime time) = 0;
    };
}

#endif
