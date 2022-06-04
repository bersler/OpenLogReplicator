/* Header for OpCode class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "types.h"

#ifndef OPCODE_H_
#define OPCODE_H_

namespace OpenLogReplicator {
    class OracleAnalyzer;
    class RedoLogRecord;

    class OpCode {
    protected:
        static void ktbRedo(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCode(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeIRP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeDRP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeLKR(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeURP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeORP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeCFA(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeSKL(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void kdoOpCodeQM(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength);
        static void ktub(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength, bool isKtubl);
        static void dumpCols(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum, uint16_t fieldLength, uint8_t isNull);
        static void dumpColsVector(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum, uint16_t fieldLength);
        static void dumpCompressed(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data, uint16_t fieldLength);
        static void dumpRows(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data);
        static void dumpVal(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength, const char* msg);
        static void processFbFlags(uint8_t fb, char* fbStr);

    public:
        static void process(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord);
    };
}

#endif
