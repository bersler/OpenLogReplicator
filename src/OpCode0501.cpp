/* Oracle Redo OpCode: 5.1
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
#include "OpCode0501.h"
#include "OracleEnvironment.h"
#include "RedoLogException.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCode0501::OpCode0501(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
		OpCode(oracleEnvironment, redoLogRecord) {

		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (int i = 1; i <= redoLogRecord->fieldNum; ++i) {
			if (i == 1) {
				ktudb(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 2) {
				ktub(fieldPosTmp, redoLogRecord->fieldLengths[i]);
				if (redoLogRecord->opc == 0x0507) // (flg & 0x8) != 0)
					ktubl(fieldPosTmp, redoLogRecord->fieldLengths[i]);
				else if (redoLogRecord->opc == 0x0B01)
					ktubu(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 3) {
				if (redoLogRecord->opc == 0x0B01)
					ktbRedo(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 4) {
				if (redoLogRecord->opc == 0x0B01)
					kdoOpCode(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			}

			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
	}

	OpCode0501::~OpCode0501() {
	}


	uint16_t OpCode0501::getOpCode(void) {
		return 0x0501;
	}

	bool OpCode0501::isKdoUndo() {
		return true;
	}

	const char* OpCode0501::getUndoType() {
		return "";
	}

	void OpCode0501::ktudb(uint32_t fieldPos, uint32_t fieldLength) {
		if (fieldLength < 20)
			throw RedoLogException("ERROR: to short field ktudb: ", nullptr, fieldLength);

		redoLogRecord->xid = XID(oracleEnvironment->read16(redoLogRecord->data + fieldPos + 8),
				oracleEnvironment->read16(redoLogRecord->data + fieldPos + 10),
				oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12));

		if (oracleEnvironment->dumpLogFile) {
			uint16_t siz = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 0);
			uint16_t spc = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 2);
			uint16_t flg = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 4);
			uint16_t seq = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 16);
			uint8_t rec = redoLogRecord->data[fieldPos + 18];

			cout << "ktudb redo:" <<
					" siz: " << dec << siz <<
					" spc: " << dec << spc <<
					" flg: 0x" << setfill('0') << setw(4) << hex << flg <<
					" seq: 0x" << setfill('0') << setw(4) << seq <<
					" rec: 0x" << setfill('0') << setw(2) << (int)rec << endl;
			cout << "           " <<
					" xid:  " << PRINTXID(redoLogRecord->xid) << endl;
		}
	}

	void OpCode0501::ktubl(uint32_t fieldPos, uint32_t fieldLength) {
		if (fieldLength < 76)
			throw RedoLogException("ERROR: to short field ktubl.5.7: ", nullptr, fieldLength);

		if (oracleEnvironment->dumpLogFile) {
			uint32_t x1 = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 24);
			typeuba prevCtlUba = oracleEnvironment->read64(redoLogRecord->data + fieldPos + 28);
			typescn prevCtlMaxCmtScn = oracleEnvironment->read48(redoLogRecord->data + fieldPos + 36);
			typescn prevTxCmtScn = oracleEnvironment->read48(redoLogRecord->data + fieldPos + 44);
			typescn txStartScn = oracleEnvironment->read48(redoLogRecord->data + fieldPos + 56);
			uint32_t prevBrb = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 64);
			uint32_t logonUser = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 72);

			string lastBufferSplit = "No";
			string tempObject = "No"; //FIXME
			string tablespaceUndo = "No"; //FIXME
			uint32_t prevBcl = 0; //FIXME
			uint32_t buExtIdx = 0; // FIXME
			uint32_t flg2 = 0; //FIXME

			cout << "ktubl redo:" <<
					" slt: " << dec << (int)redoLogRecord->slt <<
					" rci: " << dec << (int)redoLogRecord->rci <<
					" opc: " << (int)(redoLogRecord->opc >> 8) << "." << (int)(redoLogRecord->opc & 0xFF) <<
					" [objn: " << redoLogRecord->objn << " objd: " << redoLogRecord->objd << " tsn: " << redoLogRecord->tsn << "]" << endl;
			cout << "Undo type:  Regular undo        Begin trans    Last buffer split:  " << lastBufferSplit << endl;
			cout << "Temp Object:  " << tempObject << endl;
			cout << "Tablespace Undo:  " << tablespaceUndo << endl;
			cout << "             0x" << setfill('0') << setw(8) << hex << x1 << " " <<
					" prev ctl uba: " << PRINTUBA(prevCtlUba) << endl;
			cout << "prev ctl max cmt scn:  " << PRINTSCN(prevCtlMaxCmtScn) << " " <<
					" prev tx cmt scn:  " << PRINTSCN(prevTxCmtScn) << endl;
			cout << "txn start scn:  " << PRINTSCN(txStartScn) << " " <<
					" logon user: " << dec << logonUser << " " <<
					" prev brb: " << prevBrb << " " <<
					" prev bcl: " << dec << prevBcl <<
					" BuExt idx: " << dec << buExtIdx <<
					" flg2: " << dec << flg2 << endl;
		}
	}

	string OpCode0501::getName() {
		char output[12] = "UNDO xx.xx ";
		if (((redoLogRecord->opc >> 8) >> 4) <= 9)
			output[5] = (redoLogRecord->opc >> 12) + '0';
		else
			output[5] = ((redoLogRecord->opc >> 12) - 9) + 'a';
		if (((redoLogRecord->opc >> 8) & 0xF) <= 9)
			output[6] = ((redoLogRecord->opc >> 8) & 0xF) + '0';
		else
			output[6] = (((redoLogRecord->opc >> 8) & 0xF) - 9) + 'a';

		if (((redoLogRecord->opc & 0xFF) >> 4) <= 9)
			output[8] = ((redoLogRecord->opc & 0xFF) >> 4) + '0';
		else
			output[8] = (((redoLogRecord->opc & 0xFF) >> 4) - 9) + 'a';
		if ((redoLogRecord->opc & 0xF) <= 9)
			output[9] = (redoLogRecord->opc & 0xF) + '0';
		else
			output[9] = ((redoLogRecord->opc & 0xF) - 9) + 'a';

		return output;
	}

	//undo block
	void OpCode0501::process() {
		dump();
	}
}
