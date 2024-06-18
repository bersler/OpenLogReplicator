/* Structure used to hold in memory basic information for OpCode
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "RedoLogRecord.h"
#include "typeXid.h"

namespace OpenLogReplicator {
    std::ostream& operator<<(std::ostream& os, const RedoLogRecord& redo) {
        std::ostringstream ss;
        ss << "O scn: " << PRINTSCN64(redo.scnRecord) <<
           " scn: " << std::dec << redo.scn <<
           " subScn: " << std::dec << redo.subScn <<
           " xid: " << redo.xid.toString() <<
           " op: " << std::setfill('0') << std::setw(4) << std::hex << redo.opCode <<
           " cls: " << std::dec << redo.cls <<
           " rbl: " << std::dec << redo.rbl <<
           " seq: " << std::dec << static_cast<uint64_t>(redo.seq) <<
           " typ: " << std::dec << static_cast<uint64_t>(redo.typ) <<
           " conId: " << std::dec << redo.conId <<
           " flgRecord: " << std::dec << redo.flgRecord <<
           " robj: " << std::dec << redo.recordObj <<
           " rdataObj: " << std::dec << redo.recordDataObj <<
           " nrow: " << std::dec << static_cast<uint64_t>(redo.nRow) <<
           " afn: " << std::dec << redo.afn <<
           " size: " << std::dec << redo.size <<
           " dba: 0x" << std::hex << redo.dba <<
           " bdba: 0x" << std::hex << redo.bdba <<
           " obj: " << std::dec << redo.obj <<
           " dataobj: " << std::dec << redo.dataObj <<
           " usn: " << std::dec << redo.usn <<
           " slt: " << std::dec << static_cast<uint64_t>(redo.slt) <<
           " flg: " << std::dec << static_cast<uint64_t>(redo.flg) <<
           " opc: 0x" << std::hex << redo.opc <<
           " op: " << std::dec << static_cast<uint64_t>(redo.op) <<
           " cc: " << std::dec << static_cast<uint64_t>(redo.cc) <<
           " slot: " << std::dec << redo.slot <<
           " flags: 0x" << std::hex << static_cast<uint64_t>(redo.flags) <<
           " fb: 0x" << std::hex << static_cast<uint64_t>(redo.fb);
        os << ss.str();
        return os;
    }
}
