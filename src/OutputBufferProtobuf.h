/* Header for OutputBufferProtobuf class
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
#include "OraProtoBuf.pb.h"

#ifndef OUTPUTBUFFERPROTOBUF_H_
#define OUTPUTBUFFERPROTOBUF_H_

using namespace std;

namespace OpenLogReplicator {

    class OutputBufferProtobuf : public OutputBuffer {
protected:
        pb::RedoResponse *redoResponsePB;
        pb::Value *valuePB;
        pb::Payload *payloadPB;
        pb::Schema *schemaPB;

        virtual void columnNull(OracleColumn *column);
        virtual void columnFloat(string &columnName, float value);
        virtual void columnDouble(string &columnName, double value);
        virtual void columnString(string &columnName);
        virtual void columnNumber(string &columnName, uint64_t precision, uint64_t scale);
        virtual void columnRaw(string &columnName, const uint8_t *data, uint64_t length);
        virtual void columnTimestamp(string &columnName, struct tm &time, uint64_t fraction, const char *tz);
        virtual void appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot);
        virtual void appendHeader(bool first);
        virtual void appendSchema(OracleObject *object);
        void numToString(uint64_t value, char *buf, uint64_t length);
public:
        OutputBufferProtobuf(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat);
        virtual ~OutputBufferProtobuf();

        virtual void processBegin(typeSCN scn, typetime time, typexid xid);
        virtual void processCommit(void);
        virtual void processInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void processUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid);
        virtual void processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength);
    };
}

#endif
