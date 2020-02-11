/* Header for OpCode class
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <string>
#include "types.h"

#ifndef OPCODE_H_
#define OPCODE_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleEnvironment;
    class RedoLogRecord;

    class OpCode {
    protected:
        OracleEnvironment *oracleEnvironment;
        RedoLogRecord *redoLogRecord;

        void ktbRedo(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCode(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeIRP(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeDRP(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeLKR(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeURP(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeORP(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeCFA(uint32_t fieldPos, uint32_t fieldLength);
        void kdoOpCodeSKL(uint32_t fieldPos, uint32_t fieldLength);
        virtual void kdoOpCodeQM(uint32_t fieldPos, uint32_t fieldLength);

        void ktub(uint32_t fieldPos, uint32_t fieldLength);
        virtual const char* getUndoType();
        void dumpCols(uint8_t *data, uint16_t colnum, uint16_t fieldLength, uint8_t isNull);
        void dumpColsVector(uint8_t *data, uint16_t colnum, uint16_t fieldLength);
        void dumpRows(uint8_t *data);
        void dumpVal(uint32_t fieldPos, uint32_t fieldLength, string msg);
        void processFbFlags(uint8_t fb, char *fbStr);

    public:
        OpCode(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord);
        virtual ~OpCode();

        uint16_t getOpCode(void);
        virtual void process();
    };
}

#endif
