/* Header for OpCode0513 class
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

#include "OpCode.h"

#ifndef OPCODE0513_H_
#define OPCODE0513_H_

namespace OpenLogReplicator {
    class RedoLogRecord;

    class OpCode0513: public OpCode {
    protected:
        void dumpMsgSessionSerial(uint64_t fieldPos, uint64_t fieldLength) const;
        void dumpMsgFlags(uint64_t fieldPos, uint64_t fieldLength) const;
        void dumpMsgVersion(uint64_t fieldPos, uint64_t fieldLength) const;
        void dumpMsgAuditSessionid(uint64_t fieldPos, uint64_t fieldLength) const;

    public:
        OpCode0513(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord);
        virtual ~OpCode0513();

        virtual void process(void);
    };
}

#endif
