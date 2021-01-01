/* Header for OutputBuffer class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "types.h"

#ifndef OUTPUTBUFFER_H_
#define OUTPUTBUFFER_H_

using namespace std;

namespace OpenLogReplicator {

    class CharacterSet;
    class OracleAnalyzer;
    class OracleObject;
    class RedoLogRecord;
    class Writer;

    struct ColumnValue {
        uint16_t length[4];
        uint8_t *data[4];
        bool merge;
    };

    struct OutputBufferQueue {
        uint64_t id;
        uint64_t length;
        uint8_t* data;
        OutputBufferQueue *next;
    };

    struct OutputBufferMsg {
        uint64_t id;
        uint64_t queueId;
        uint64_t length;
        typeSCN scn;
        OracleAnalyzer *oracleAnalyzer;
        uint8_t* data;
        uint32_t dictId;
        uint16_t pos;
        uint16_t flags;
    };

    class OutputBuffer {
    protected:
        static const char map64[65];
        static const char map16[17];
        OracleAnalyzer *oracleAnalyzer;
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
        typeSCN lastScn;
        typeXID lastXid;
        map<uint16_t, uint16_t> valuesMap;
        ColumnValue values[MAX_NO_COLUMNS][4];
        uint8_t *merges[MAX_NO_COLUMNS*4];
        uint64_t valuesMax;
        uint64_t mergesMax;
        uint64_t id;

        void valuesRelease();
        void valueSet(uint64_t type, uint16_t column, uint8_t *data, uint16_t length, uint8_t fb);
        void outputBufferRotate(bool copy);
        void outputBufferShift(uint64_t bytes, bool copy);
        void outputBufferBegin(uint32_t dictId);
        void outputBufferCommit(void);
        void outputBufferAppend(char character);
        void outputBufferAppend(const char* str, uint64_t length);
        void outputBufferAppend(const char* str);
        void outputBufferAppend(string &str);
        void columnUnknown(string &columnName, const uint8_t *data, uint64_t length);
        virtual void columnNull(OracleObject *object, typeCOL col) = 0;
        virtual void columnFloat(string &columnName, float value) = 0;
        virtual void columnDouble(string &columnName, double value) = 0;
        virtual void columnString(string &columnName) = 0;
        virtual void columnNumber(string &columnName, uint64_t precision, uint64_t scale) = 0;
        virtual void columnRaw(string &columnName, const uint8_t *data, uint64_t length) = 0;
        virtual void columnTimestamp(string &columnName, struct tm &time, uint64_t fraction, const char *tz) = 0;
        void valueBufferAppend(uint8_t value);
        void valueBufferAppendHex(typeunicode value, uint64_t length);
        void processValue(OracleObject *object, typeCOL col, const uint8_t *data, uint64_t length, uint64_t typeNo, uint64_t charsetId);
        virtual void appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot) = 0;
        virtual void appendHeader(bool first) = 0;
        virtual void appendSchema(OracleObject *object, typeDATAOBJ dataObj) = 0;

    public:
        uint64_t defaultCharacterMapId;
        uint64_t defaultCharacterNcharMapId;
        unordered_map<uint64_t, CharacterSet*> characterMap;
        Writer *writer;
        mutex mtx;
        condition_variable writersCond;

        uint64_t buffersAllocated;
        OutputBufferQueue *firstBuffer;
        OutputBufferQueue *lastBuffer;
        OutputBufferMsg *curMsg;

        OutputBuffer(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat);
        virtual ~OutputBuffer();

        void initialize(OracleAnalyzer *oracleAnalyzer);
        uint64_t outputBufferSize(void) const;
        void setWriter(Writer *writer);
        void setNlsCharset(string &nlsCharset, string &nlsNcharCharset);

        virtual void processBegin(typeSCN scn, typetime time, typeXID xid) = 0;
        virtual void processCommit(void) = 0;
        virtual void processInsert(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) = 0;
        virtual void processUpdate(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) = 0;
        virtual void processDelete(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) = 0;
        virtual void processDDL(OracleObject *object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength) = 0;
        void processInsertMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void processDeleteMultiple(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
        void processDML(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, uint64_t type);
        void processDDLheader(RedoLogRecord *redoLogRecord1);
        //virtual void processCheckpoint(typeSCN scn, typetime time) = 0;
        //virtual void processSwitch(typeSCN scn, typetime time) = 0;
    };
}

#endif
