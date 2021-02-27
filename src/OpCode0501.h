/* Header for OpCode0501 class
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

#ifndef OPCODE0501_H_
#define OPCODE0501_H_

namespace OpenLogReplicator {
    class RedoLogRecord;

    class OpCode0501: public OpCode {
    protected:
        void ktudb(uint64_t fieldPos, uint64_t fieldLength);
        void kteoputrn(uint64_t fieldPos, uint64_t fieldLength);
        void rowDeps(uint64_t fieldPos, uint64_t fieldLength);
        void suppLog(uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength);
        virtual const char* getUndoType(void);

    public:
        OpCode0501(OracleAnalyzer *oracleAnalyzer, RedoLogRecord *redoLogRecord);
        virtual ~OpCode0501();

        virtual void process(void);
    };
}

#endif
