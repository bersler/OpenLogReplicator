/* Oracle Redo OpCode for multi-row operations
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
#include "OpCodeMultirow.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCodeMultirow::OpCodeMultirow(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord, bool fill) :
			OpCode(oracleEnvironment, redoLogRecord, fill) {
	}

	OpCodeMultirow::OpCodeMultirow(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
			OpCode(oracleEnvironment, redoLogRecord) {
	}

	void OpCodeMultirow::kdoOpCodeQM(uint32_t fieldPos, uint32_t fieldLength) {
		if (fieldLength < 24) {
			oracleEnvironment->dumpStream << "too short field KDO OpCode QMI: " << fieldLength << endl;
			return;
		}

		redoLogRecord->slots = (uint16_t*)(redoLogRecord->data + fieldPos + 20);
		redoLogRecord->nrow = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 18);

		if (oracleEnvironment->dumpLogFile) {
			uint8_t tabn = redoLogRecord->data[fieldPos + 16];
			uint8_t lock = redoLogRecord->data[fieldPos + 17];

			oracleEnvironment->dumpStream << "tabn: "<< (uint32_t)tabn <<
				" lock: " << dec << (uint32_t)lock <<
				" nrow: " << dec << redoLogRecord->nrow << endl;
		}
	}

	uint16_t OpCodeMultirow::getOpCode(void) {
		return 0;
	}

	string OpCodeMultirow::getName() {
		return "Multirow   ";
	}

	void OpCodeMultirow::process() {
		dump();
	}

	OpCodeMultirow::~OpCodeMultirow() {
	}

}
