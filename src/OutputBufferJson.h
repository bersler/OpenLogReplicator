/* Header for OutputBufferJson class
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

#include "OutputBuffer.h"

#ifndef OUTPUTBUFFERJSON_H_
#define OUTPUTBUFFERJSON_H_

using namespace std;

namespace OpenLogReplicator {

    class OutputBufferJson : public OutputBuffer {
    protected:
        bool hasPrevious;

        void appendHex(uint64_t val, uint64_t length);
        void appendDec(uint64_t val);
        void appendSDec(int64_t val);
        void appendTimestamp(const uint8_t *data, uint64_t length);
        void appendEscape(const uint8_t *str, uint64_t length);
        void appendEscapeMap(const uint8_t *str, uint64_t length, uint64_t charsetId);
        void appendNull(string &columnName, bool &prevValue);
        void appendUnknown(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength);
        void appendValue(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t charsetId, uint64_t fieldPos, uint64_t fieldLength, bool &prevValue);
        void appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot);
        void appendHeader(bool first);
        void appendSchema(OracleObject *object);

    public:
        OutputBufferJson(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat);
        virtual ~OutputBufferJson();

        virtual void processBegin(typescn scn, typetime time, typexid xid);
        virtual void processCommit(void);
        virtual void processInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void processUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength);
    };
}

#endif
