/* Header for OpCode0B0C class
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

#ifndef OPCODE0B0C_H_
#define OPCODE0B0C_H_

namespace OpenLogReplicator {
    class RedoLogRecord;

    class OpCode0B0C: public OpCode {
    public:
        OpCode0B0C(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord);
        virtual ~OpCode0B0C();

        virtual void process(void);
    };
}

#endif
