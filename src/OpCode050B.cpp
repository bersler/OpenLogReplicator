/* Oracle Redo OpCode: 5.11
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
#include "types.h"
#include "OpCode050B.h"
#include "RedoLogException.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCode050B::OpCode050B(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
			OpCode(oracleEnvironment, redoLogRecord) {

		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (int i = 1; i <= redoLogRecord->fieldNum; ++i) {
			if (i == 1) {
				ktub(fieldPosTmp, redoLogRecord->fieldLengths[i]);

				if (redoLogRecord->opc == 0x0B15)
					ktubu(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 2) {
				buext(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			}
			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
	}

	uint16_t OpCode050B::getOpCode(void) {
		return 0x050B;
	}

	void OpCode050B::buext(uint32_t fieldPos, uint32_t fieldLength) {
		if (fieldLength < 8)
			throw RedoLogException("to short field buext: ", nullptr, fieldLength);

		if (oracleEnvironment->dumpLogFile) {
			uint8_t idx = 0; //FIXME
			uint8_t flg2 = 0; // FIXME

			cout << "BuExt idx: " << dec << (int)idx <<
					" flg2: " << (int)flg2 << endl;
		}
	}

	OpCode050B::~OpCode050B() {
	}

	const char* OpCode050B::getUndoType() {
		return "User undo done    Begin trans    ";
	}

	string OpCode050B::getName() {
		return "ROLLBACK B ";
	}

	//rollback
	void OpCode050B::process() {
		dump();
	}
}
