/* Memory buffer for handling output data in JSON format
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

#include "CharacterSet.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBufferProtobuf.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Writer.h"

#ifdef LINK_LIBRARY_PROTOBUF
#include "OraProtoBuf.pb.h"
using namespace oraprotobuf;
#endif /* LINK_LIBRARY_PROTOBUF */

namespace OpenLogReplicator {

    OutputBufferProtobuf::OutputBufferProtobuf(uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat, uint64_t showColumns) :
            OutputBuffer(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns) {
    }

    OutputBufferProtobuf::~OutputBufferProtobuf() {
    }

    void OutputBufferProtobuf::appendInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
    }

    void OutputBufferProtobuf::appendUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
    }

    void OutputBufferProtobuf::appendDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
    }

    void OutputBufferProtobuf::appendDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) {
    }

    void OutputBufferProtobuf::next(void) {
    }

    void OutputBufferProtobuf::beginTran(typescn scn, typetime time, typexid xid) {
        OutputBuffer::beginTran(scn, time, xid);
    }

    void OutputBufferProtobuf::commitTran(void) {
        commitMessage();
    }
}
