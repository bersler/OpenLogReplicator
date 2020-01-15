/* Struct used to hold in memory basic information for OpCode
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

#include <iostream>
#include <iomanip>
#include "types.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    void RedoLogRecord::dump() {
        cerr << "DUMP: opCode: " << hex << opCode <<
                " cls: " << dec << cls <<
                " scnRecord: " << PRINTSCN64(scnRecord) <<
                " rbl: " << dec << rbl <<
                " seq: " << dec << (uint32_t)seq <<
                " typ: " << dec << (uint32_t)typ <<
                " conId: " << dec << conId <<
                " flgRecord: " << dec << flgRecord <<
                " vectorNo: " << dec << vectorNo <<
                " recordObjn: " << dec << recordObjn <<
                " recordObjd: " << dec << recordObjd <<
                " scn: " << PRINTSCN64(scn) <<
                " nrow: " << dec << nrow <<
                " afn: " << dec << afn <<
                " length: " << dec << length <<
                " dba: " << hex << dba <<
                " bdba: " << hex << bdba <<
                " objn: " << dec << objn <<
                " objd: " << dec << objd <<
                " tsn: " << dec << tsn <<
                " undo: " << dec << undo <<
                " usn: " << dec << usn <<
                " xid: " << PRINTXID(xid) <<
                " uba: " << PRINTUBA(uba) <<
                " slt: " << dec << (uint32_t)slt <<
                " rci: " << dec << (uint32_t)rci <<
                " flg: " << dec << (uint32_t)flg <<
                " opc: 0x" << hex << opc <<
                " op: " << dec << (uint32_t)op <<
                " cc: " << dec << (uint32_t)cc <<
                " itli: " << dec << (uint32_t)itli <<
                " slot: " << dec << slot <<
                " flags: 0x" << hex << (uint32_t)flags <<
                " fb: 0x" << hex << (uint32_t)fb <<
                " nridDba: " << hex << nridDba <<
                " nridSlt: " << dec << (uint32_t)nridSlt << endl;
    }
}
