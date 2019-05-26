/* Oracle Redo OpCode: 11.6
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
#include "OpCode0B06.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCode0B06::OpCode0B06(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
			OpCode(oracleEnvironment, redoLogRecord) {

		uint8_t *nullstmp, bits = 1;
		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
			if (i == 1) {
				ktbRedo(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 2) {
				kdoOpCode(fieldPosTmp, redoLogRecord->fieldLengths[i]);
				nullstmp = redoLogRecord->nulls = redoLogRecord->data + fieldPosTmp + 45;
			} else if (i > 2 && i <= 2 + (uint32_t)redoLogRecord->cc) {
				if (oracleEnvironment->dumpLogFile) {
					dumpCols(redoLogRecord->data + fieldPosTmp, i - 3, redoLogRecord->fieldLengths[i], *nullstmp & bits);
					bits <<= 1;
					if (bits == 0) {
						bits = 1;
						++nullstmp;
					}
				}
			}

			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
	}

	OpCode0B06::~OpCode0B06() {
	}


	uint16_t OpCode0B06::getOpCode(void) {
		return 0x0B06;
	}

	string OpCode0B06::getName() {
		return "REDO OVR   ";
	}

	//lock row
	void OpCode0B06::process() {
		dump();
	}
}
