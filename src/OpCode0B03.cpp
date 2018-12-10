/* Oracle Redo OpCode: 11.3
   Copyright (C) 2018 Adam Leszczynski.

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
#include "OpCode0B03.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCode0B03::OpCode0B03(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
			OpCode(oracleEnvironment, redoLogRecord) {

		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (int i = 1; i <= redoLogRecord->fieldNum; ++i) {
			if (i == 1) {
				ktbRedo(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 2) {
				kdoOpCode(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			}

			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
	}

	OpCode0B03::~OpCode0B03() {
	}


	uint16_t OpCode0B03::getOpCode(void) {
		return 0x0B03;
	}

	string OpCode0B03::getName() {
		return "REDO DEL   ";
	}

	//insert
	void OpCode0B03::process() {
		dump();
	}
}
