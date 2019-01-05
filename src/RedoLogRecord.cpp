/* Struct used to hold in memory basic information for OpCode
   Copyright (C) 2018-2019 Adam Leszczynski.

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

namespace OpenLogReplicatorOracle {
	void RedoLogRecord::dump() {
		cout << "DUMP: opCode: " << hex << opCode <<
			" length: " << dec << length <<
			" dba: " << hex << dba <<
			" bdba: " << hex << bdba <<
			" opc: " << hex << opc <<
			" objn: " << dec << objn <<
			" objd: " << dec << objd <<
			" tsn: " << dec << tsn <<
			" undo: " << dec << undo <<
			" xid: " << PRINTXID(xid) <<
			" uba: " << PRINTUBA(uba) <<
			" slt: " << dec << (int)slt <<
			" rci: " << dec << (int)rci <<
			" flg: " << dec << (int)flg << endl;
	}
}
