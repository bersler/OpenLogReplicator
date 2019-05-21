/* Oracle Redo OpCode: 5.2
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
#include "OpCode0502.h"
#include "RedoLogException.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCode0502::OpCode0502(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord, uint16_t usn) :
		OpCode(oracleEnvironment, redoLogRecord) {

		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
			if (i == 1) {
				ktudh(fieldPosTmp, redoLogRecord->fieldLengths[i], usn);
			} else if (i == 2) {
				if ((redoLogRecord->flg & 0x2) == 0)
					kteop(fieldPosTmp, redoLogRecord->fieldLengths[i], usn);
			}
			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
	}

	OpCode0502::~OpCode0502() {
	}


	uint16_t OpCode0502::getOpCode(void) {
		return 0x0502;
	}

	void OpCode0502::kteop(uint32_t fieldPos, uint32_t fieldLength, uint16_t usn) {
		if (fieldLength < 36) {
			oracleEnvironment->dumpStream << "too short field kteop: " << dec << fieldLength << endl;
			return;
		}

		if (oracleEnvironment->dumpLogFile) {
			uint32_t highwater = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 16);
			uint16_t ext = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 4);
			uint16_t blk = 0; //FIXME
			uint32_t extSize = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12);
			uint16_t blocksFreelist = 0; //FIXME
			uint16_t blocksBelow = 0; //FIXME
			uint32_t mapblk = 0; //FIXME
			uint16_t offset = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 24);

			oracleEnvironment->dumpStream << "kteop redo - redo operation on extent map" << endl;
			oracleEnvironment->dumpStream << "   SETHWM:      " <<
					" Highwater::  0x" << setfill('0') << setw(8) << hex << highwater << " " <<
					" ext#: " << dec << ext << "     " <<
					" blk#: " << dec << blk << "     " <<
					" ext size: " << dec << extSize << "     " << endl;
			oracleEnvironment->dumpStream << "  #blocks in seg. hdr's freelists: " << dec << blocksFreelist << "     " << endl;
			oracleEnvironment->dumpStream << "  #blocks below: " << blocksBelow << "     " << endl;
			oracleEnvironment->dumpStream << "  mapblk  0x" << setfill('0') << setw(8) << hex << mapblk << " " <<
					" offset: " << dec << offset << "     " << endl;
		}
	}

	void OpCode0502::ktudh(uint32_t fieldPos, uint32_t fieldLength, uint16_t usn) {
		if (fieldLength < 32) {
			oracleEnvironment->dumpStream << "too short field ktudh: " << dec << fieldLength << endl;
			return;
		}

		redoLogRecord->xid = XID(usn,
				oracleEnvironment->read16(redoLogRecord->data + fieldPos + 0),
				oracleEnvironment->read32(redoLogRecord->data + fieldPos + 4));
		redoLogRecord->uba = oracleEnvironment->read56(redoLogRecord->data + fieldPos + 8);

		if (oracleEnvironment->dumpLogFile) {
			uint8_t fbi = redoLogRecord->data[fieldPos + 20];
			redoLogRecord->flg = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 16);
			uint16_t siz = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 18);

			uint16_t pxid = XID(oracleEnvironment->read16(redoLogRecord->data + fieldPos + 24),
					oracleEnvironment->read16(redoLogRecord->data + fieldPos + 26),
					oracleEnvironment->read32(redoLogRecord->data + fieldPos + 28));

			oracleEnvironment->dumpStream << "ktudh redo:" <<
					" slt: 0x" << setfill('0') << setw(4) << hex << SLT(redoLogRecord->xid) <<
					" sqn: 0x" << setfill('0') << setw(8) << hex << SQN(redoLogRecord->xid) <<
					" flg: 0x" << setfill('0') << setw(4) << redoLogRecord->flg <<
					" siz: " << dec << siz <<
					" fbi: " << dec << (uint32_t)fbi << endl;
			oracleEnvironment->dumpStream << "           " <<
					" uba: " << PRINTUBA(redoLogRecord->uba) << "   " <<
					" pxid:  " << PRINTXID(pxid) << endl;
		}
	}

	string OpCode0502::getName() {
		return "UNDO START ";
	}

	//undo header
	void OpCode0502::process() {
		dump();
	}
}
