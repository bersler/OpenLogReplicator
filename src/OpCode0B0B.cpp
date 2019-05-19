/* Oracle Redo OpCode: 11.11
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
#include "OpCode0B0B.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	OpCode0B0B::OpCode0B0B(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord, bool fill) :
			OpCodeMultirow(oracleEnvironment, redoLogRecord, fill) {
	}

	OpCode0B0B::OpCode0B0B(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
			OpCodeMultirow(oracleEnvironment, redoLogRecord) {
		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
			if (i == 1) {
				ktbRedo(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 2) {
				kdoOpCode(fieldPosTmp, redoLogRecord->fieldLengths[i]);
			} else if (i == 3) {
				rowLenghs = (uint16_t*)(redoLogRecord->data + fieldPosTmp);
				//FIXME: check field length is large enough
			} else if (i > 3) { //row N
				uint32_t pos = 0;
				char flStr[9] = "--------";

				for (uint32_t j = 0; j < nrow; ++j) {
					oracleEnvironment->dumpStream << "slot[" << dec << j << "]: " << dec << slots[j] << endl;
					uint8_t fl = redoLogRecord->data[fieldPosTmp + pos];
					uint8_t lb = redoLogRecord->data[fieldPosTmp + pos + 1];
					uint8_t jcc = redoLogRecord->data[fieldPosTmp + pos + 2];

					if ((fl & 0x01) == 0x01) flStr[7] = 'N'; else flStr[7] = '-'; //last column continues in Next piece
					if ((fl & 0x02) == 0x02) flStr[6] = 'P'; else flStr[6] = '-'; //first column continues from Previous piece
					if ((fl & 0x04) == 0x04) flStr[5] = 'L'; else flStr[5] = '-'; //Last data piece
					if ((fl & 0x08) == 0x08) flStr[4] = 'F'; else flStr[4] = '-'; //First data piece
					if ((fl & 0x10) == 0x10) flStr[3] = 'D'; else flStr[3] = '-'; //Deleted row
					if ((fl & 0x20) == 0x20) flStr[2] = 'H'; else flStr[2] = '-'; //Head piece of row
					if ((fl & 0x40) == 0x40) flStr[1] = 'C'; else flStr[1] = '-'; //Clustered table member
					if ((fl & 0x80) == 0x80) flStr[0] = 'K'; else flStr[0] = '-'; //cluster Key

					oracleEnvironment->dumpStream << "tl: " << dec <<  rowLenghs[j] <<
							" fb: " << flStr <<
							" lb: 0x" << hex << (uint32_t)lb << " " <<
							" cc: " << dec << (uint32_t)jcc << endl;
					pos += 3;

					for (uint32_t k = 0; k < jcc; ++k) {
						uint16_t fieldLength = redoLogRecord->data[fieldPosTmp + pos];
						++pos;

						if (fieldLength == 0xFF) {
							oracleEnvironment->dumpStream << "col " << setfill(' ') << setw(2) << dec << k << ": *NULL*" << endl;
						} else {
							if (fieldLength == 0xFE) {
								fieldLength = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + pos);
								pos += 2;
							}

							oracleEnvironment->dumpStream << "col " << setfill(' ') << setw(2) << dec << k << ": " <<
									"[" << setfill(' ') << setw(2) << dec << fieldLength << "]";

							if (fieldLength <= 20)
								oracleEnvironment->dumpStream << " ";
							else
								oracleEnvironment->dumpStream << endl;

							for (uint32_t l = 0; l < fieldLength; ++l) {
								oracleEnvironment->dumpStream << " " << setfill('0') << setw(2) << hex << (uint32_t)redoLogRecord->data[fieldPosTmp + pos];
								if ((l % 25) == 24 && l != (uint32_t)fieldLength - 1)
									oracleEnvironment->dumpStream << endl;
								++pos;
							}

							oracleEnvironment->dumpStream << endl;
						}
					}
				}
			}

			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
	}

	OpCode0B0B::~OpCode0B0B() {
	}

	void OpCode0B0B::parseDml() {
	}

	uint16_t OpCode0B0B::getOpCode(void) {
		return 0x0B0B;
	}

	string OpCode0B0B::getName() {
		return "REDO DEL   ";
	}

	void OpCode0B0B::process() {
		dump();
	}
}
