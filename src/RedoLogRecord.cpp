/* Struct used to hold in memory basic information for OpCode
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

#include <iomanip>
#include <iostream>

#include "OracleAnalyser.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    void RedoLogRecord::dumpHex(ostream &stream, OracleAnalyser *oracleAnalyser) {
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
            uint16_t fieldLength = oracleAnalyser->read16(data + fieldLengthsDelta + i * 2);
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


    void RedoLogRecord::dump(OracleAnalyser *oracleAnalyser) {
        if (oracleAnalyser->version < 0x12200)
            cerr << "O scn: " << PRINTSCN48(scnRecord);
        else
            cerr << "O scn: " << PRINTSCN64(scnRecord);

        cerr << " xid: " << PRINTXID(xid) <<
                " op: " << setfill('0') << setw(4) << hex << opCode <<
                " cls: " << dec << cls <<
                " rbl: " << dec << rbl <<
                " seq: " << dec << (uint64_t)seq <<
                " typ: " << dec << (uint64_t)typ <<
                " con_id: " << dec << conId <<
                " flgRecord: " << dec << flgRecord <<
//                " vectorNo: " << dec << vectorNo <<
                " robjn: " << dec << recordObjn <<
                " robjd: " << dec << recordObjd <<
//                " scn: " << PRINTSCN64(scn) <<
                " nrow: " << dec << nrow <<
                " afn: " << dec << afn <<
                " length: " << dec << length <<
                " dba: 0x" << hex << dba <<
                " bdba: 0x" << hex << bdba <<
                " objn: " << dec << objn <<
                " objd: " << dec << objd <<
                " tsn: " << dec << tsn <<
                " undo: " << dec << undo <<
                " usn: " << dec << usn <<
                " uba: " << PRINTUBA(uba) <<
                " slt: " << dec << (uint64_t)slt <<
                " rci: " << dec << (uint64_t)rci <<
                " flg: " << dec << (uint64_t)flg <<
                " opc: 0x" << hex << opc <<
                " op: " << dec << (uint64_t)op <<
                " cc: " << dec << (uint64_t)cc <<
//                " itli: " << dec << (uint64_t)itli <<
                " slot: " << dec << slot <<
                " flags: 0x" << hex << (uint64_t)flags <<
                " fb: 0x" << hex << (uint64_t)fb <<
                " nrid: 0x" << hex << nridBdba << "." << dec << nridSlot;
    }
}
