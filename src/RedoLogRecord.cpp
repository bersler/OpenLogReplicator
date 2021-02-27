/* Structure used to hold in memory basic information for OpCode
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

#include "OracleAnalyzer.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {
    void RedoLogRecord::dumpHex(ostream &stream, OracleAnalyzer *oracleAnalyzer) const {
        stream << "##: " << dec << fieldLengthsDelta;
        for (uint64_t j = 0; j < fieldLengthsDelta; ++j) {
            if ((j & 0xF) == 0)
                stream << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
            if ((j & 0x07) == 0)
                stream << " ";
            stream << setfill('0') << setw(2) << hex << (uint64_t)data[j] << " ";
        }
        stream << endl;

        uint64_t fieldPosLocal = fieldPos;
        for (uint64_t i = 1; i <= fieldCnt; ++i) {
            uint16_t fieldLength = oracleAnalyzer->read16(data + fieldLengthsDelta + i * 2);
            stream << "##: " << dec << fieldLength << " (" << i << ", " << fieldPosLocal << ")";
            for (uint64_t j = 0; j < fieldLength; ++j) {
                if ((j & 0xF) == 0)
                    stream << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
                if ((j & 0x07) == 0)
                    stream << " ";
                stream << setfill('0') << setw(2) << hex << (uint64_t)data[fieldPosLocal + j] << " ";
            }
            stream << endl;

            fieldPosLocal += (fieldLength + 3) & 0xFFFC;
        }
    }

    ostream& operator<<(ostream& os, const RedoLogRecord& redo) {
        stringstream ss;
        ss << "O scn: " << PRINTSCN64(redo.scnRecord) <<
                " scn: " << dec << redo.scn <<
                " subScn: " << dec << redo.subScn <<
                " xid: " << PRINTXID(redo.xid) <<
                " op: " << setfill('0') << setw(4) << hex << redo.opCode <<
                " cls: " << dec << redo.cls <<
                " rbl: " << dec << redo.rbl <<
                " seq: " << dec << (uint64_t)redo.seq <<
                " typ: " << dec << (uint64_t)redo.typ <<
                " conId: " << dec << redo.conId <<
                " flgRecord: " << dec << redo.flgRecord <<
//                " vectorNo: " << dec << vectorNo <<
                " robj: " << dec << redo.recordObj <<
                " rdataObj: " << dec << redo.recordDataObj <<
//                " scn: " << PRINTSCN64(scn) <<
                " nrow: " << dec << (uint64_t)redo.nrow <<
                " afn: " << dec << redo.afn <<
                " length: " << dec << redo.length <<
                " dba: 0x" << hex << redo.dba <<
                " bdba: 0x" << hex << redo.bdba <<
                " obj: " << dec << redo.obj <<
                " dataObj: " << dec << redo.dataObj <<
                " tsn: " << dec << redo.tsn <<
                " undo: " << dec << redo.undo <<
                " usn: " << dec << redo.usn <<
                " uba: " << PRINTUBA(redo.uba) <<
                " slt: " << dec << (uint64_t)redo.slt <<
                " rci: " << dec << (uint64_t)redo.rci <<
                " flg: " << dec << (uint64_t)redo.flg <<
                " opc: 0x" << hex << redo.opc <<
                " op: " << dec << (uint64_t)redo.op <<
                " cc: " << dec << (uint64_t)redo.cc <<
//                " itli: " << dec << (uint64_t)itli <<
                " slot: " << dec << redo.slot <<
                " flags: 0x" << hex << (uint64_t)redo.flags <<
                " fb: 0x" << hex << (uint64_t)redo.fb <<
                " nrid: 0x" << hex << redo.nridBdba << "." << dec << redo.nridSlot;
        os << ss.str();
        return os;
    }
}
