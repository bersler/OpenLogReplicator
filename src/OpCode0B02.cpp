/* Oracle Redo OpCode: 11.2
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
#include "OpCode0B02.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"
#include "JsonBuffer.h"

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

	OpCode0B02::OpCode0B02(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord, bool fill) :
			OpCode(oracleEnvironment, redoLogRecord, fill) {
	}

	OpCode0B02::OpCode0B02(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
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

	OpCode0B02::~OpCode0B02() {
	}

	void OpCode0B02::parseDml() {
		OracleObject *object = redoLogRecord->object;
		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		uint32_t fieldPos2;
		uint32_t colCount;
		uint8_t nulls, bits = 1;
		bool prevValue = false;

		oracleEnvironment->jsonBuffer
				->append("{\"operation\":\"insert\", \"table\": \"")
				->append(redoLogRecord->object->owner)
				->append(".")
				->append(redoLogRecord->object->objectName)
				->append("\", \"after\": {");

		for (uint32_t i = 1; i <= 2; ++i) {
			if (i == 2) {
				uint8_t op = redoLogRecord->data[fieldPosTmp + 10];
				if ((op & 0x1F) != 0x02) {
					cerr << "ERROR: Insert operation with incorrect OP: " << (int)op << endl;
					return;
				}
				colCount = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 18);
				fieldPos2 = fieldPosTmp + 45;
				nulls = redoLogRecord->data[fieldPos2];
			}
			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
		for (uint32_t i = 0; i < colCount; ++i) {
			if ((nulls & bits) != 0 || redoLogRecord->fieldLengths[i + 3] == 0) {
				//omit NULL values

				/*
				oracleEnvironment->jsonBuffer
						->append('"')
						->append(object->columns[i]->columnName.c_str())
						->append("\": NULL");
				*/
			} else {
				uint32_t j, jMax; uint8_t digits;

				if (prevValue)
					oracleEnvironment->jsonBuffer
							->append(", ");

				if (i >= object->columns.size())
					continue;

				oracleEnvironment->jsonBuffer
						->append('"')
						->append(object->columns[i]->columnName)
						->append("\": \"");

				switch(object->columns[i]->typeNo) {
				case 1: //varchar(2)
				case 96: //char
					oracleEnvironment->jsonBuffer
							->appendEscape(redoLogRecord->data + fieldPosTmp, redoLogRecord->fieldLengths[i + 3]);
					break;

				case 2: //numeric
					digits = redoLogRecord->data[fieldPosTmp + 0];
					//just zero
					if (digits == 0x80) {
						oracleEnvironment->jsonBuffer->append('0');
						break;
					}

					j = 1;
					jMax = redoLogRecord->fieldLengths[i + 3] - 1;

					//positive number
					if (digits >= 0xC0 && jMax >= 1) {
						uint32_t val;
						//part of the total
						if (digits == 0xC0)
							oracleEnvironment->jsonBuffer->append('0');
						else {
							digits -= 0xC0;
							//part of the total - omitting first zero for first digit
							val = redoLogRecord->data[fieldPosTmp + j] - 1;
							if (val < 10)
								oracleEnvironment->jsonBuffer
										->append('0' + val);
							else
								oracleEnvironment->jsonBuffer
										->append('0' + (val / 10))
										->append('0' + (val % 10));

							++j;
							--digits;

							while (digits > 0) {
								val = redoLogRecord->data[fieldPosTmp + j] - 1;
								if (j <= jMax) {
									oracleEnvironment->jsonBuffer
											->append('0' + (val / 10))
											->append('0' + (val % 10));
									++j;
								} else {
									oracleEnvironment->jsonBuffer
											->append("00");
								}
								--digits;
							}
						}

						//fraction part
						if (j <= jMax) {
							oracleEnvironment->jsonBuffer
									->append('.');

							while (j <= jMax - 1) {
								val = redoLogRecord->data[fieldPosTmp + j] - 1;
								oracleEnvironment->jsonBuffer
										->append('0' + (val / 10))
										->append('0' + (val % 10));
								++j;
							}

							//last digit - omitting 0 at the end
							val = redoLogRecord->data[fieldPosTmp + j] - 1;
							oracleEnvironment->jsonBuffer
									->append('0' + (val / 10));
							if ((val % 10) != 0)
								oracleEnvironment->jsonBuffer
										->append('0' + (val % 10));
						}
					//negative number
					} else if (digits <= 0x3F && redoLogRecord->fieldLengths[i + 3] >= 2) {
						uint32_t val;
						oracleEnvironment->jsonBuffer->append('-');

						if (redoLogRecord->data[fieldPosTmp + jMax] == 0x66)
							--jMax;

						//part of the total
						if (digits == 0x3F)
							oracleEnvironment->jsonBuffer->append('0');
						else {
							digits = 0x3F - digits;

							val = 101 - redoLogRecord->data[fieldPosTmp + j];
							if (val < 10)
								oracleEnvironment->jsonBuffer
										->append('0' + val);
							else
								oracleEnvironment->jsonBuffer
										->append('0' + (val / 10))
										->append('0' + (val % 10));
							++j;
							--digits;

							while (digits > 0) {
								if (j <= jMax) {
									val = 101 - redoLogRecord->data[fieldPosTmp + j];
									oracleEnvironment->jsonBuffer
											->append('0' + (val / 10))
											->append('0' + (val % 10));
									++j;
								} else {
									oracleEnvironment->jsonBuffer
											->append("00");
								}
								--digits;
							}
						}

						if (j <= jMax) {
							oracleEnvironment->jsonBuffer
									->append('.');

							while (j <= jMax - 1) {
								val = 101 - redoLogRecord->data[fieldPosTmp + j];
								oracleEnvironment->jsonBuffer
										->append('0' + (val / 10))
										->append('0' + (val % 10));
								++j;
							}

							val = 101 - redoLogRecord->data[fieldPosTmp + j];
							oracleEnvironment->jsonBuffer
									->append('0' + (val / 10));
							if ((val % 10) != 0)
								oracleEnvironment->jsonBuffer
										->append('0' + (val % 10));
						}
					} else {
						cerr << "ERROR: unknown value (type: " << object->columns[i]->typeNo << "): " << dec << (uint32_t)(redoLogRecord->fieldLengths[i + 3]) << " - ";
						for (uint32_t j = 0; j < redoLogRecord->fieldLengths[i + 3]; ++j)
							cout << " " << hex << setw(2) << (uint32_t) redoLogRecord->data[fieldPosTmp + j];
						cout << endl;
					}
					break;
				case 12:
				case 180:
					//2012-04-23T18:25:43.511Z - ISO 8601 format
					jMax = redoLogRecord->fieldLengths[i + 3];

					if (jMax != 7) {
						cerr << "ERROR: unknown value (type: " << object->columns[i]->typeNo << "): ";
						for (uint32_t j = 0; j < redoLogRecord->fieldLengths[i + 3]; ++j)
							cout << " " << hex << setw(2) << (int) redoLogRecord->data[fieldPosTmp + j];
						cout << endl;
					} else {
						uint32_t val1 = redoLogRecord->data[fieldPosTmp + 0],
								 val2 = redoLogRecord->data[fieldPosTmp + 1];
						bool bc = false;

						//AD
						if (val1 >= 100 && val2 >= 100) {
							val1 -= 100;
							val2 -= 100;
						//BC
						} else {
							val1 = 100 - val1;
							val2 = 100 - val2;
							bc = true;
						}
						if (val1 > 0) {
							if (val1 > 10)
								oracleEnvironment->jsonBuffer
										->append('0' + (val1 / 10))
										->append('0' + (val1 % 10))
										->append('0' + (val2 / 10))
										->append('0' + (val2 % 10));
							else
								oracleEnvironment->jsonBuffer
										->append('0' + val1)
										->append('0' + (val2 / 10))
										->append('0' + (val2 % 10));
						} else {
							if (val2 > 10)
								oracleEnvironment->jsonBuffer
										->append('0' + (val2 / 10))
										->append('0' + (val2 % 10));
							else
								oracleEnvironment->jsonBuffer
										->append('0' + val2);
						}

						if (bc)
							oracleEnvironment->jsonBuffer
									->append("BC");

						oracleEnvironment->jsonBuffer
								->append('-')
								->append('0' + (redoLogRecord->data[fieldPosTmp + 2] / 10))
								->append('0' + (redoLogRecord->data[fieldPosTmp + 2] % 10))
								->append('-')
								->append('0' + (redoLogRecord->data[fieldPosTmp + 3] / 10))
								->append('0' + (redoLogRecord->data[fieldPosTmp + 3] % 10))
								->append('T')
								->append('0' + ((redoLogRecord->data[fieldPosTmp + 4] - 1) / 10))
								->append('0' + ((redoLogRecord->data[fieldPosTmp + 4] - 1) % 10))
								->append(':')
								->append('0' + ((redoLogRecord->data[fieldPosTmp + 5] - 1) / 10))
								->append('0' + ((redoLogRecord->data[fieldPosTmp + 5] - 1) % 10))
								->append(':')
								->append('0' + ((redoLogRecord->data[fieldPosTmp + 6] - 1) / 10))
								->append('0' + ((redoLogRecord->data[fieldPosTmp + 6] - 1) % 10));
					}
					break;
				default:
					oracleEnvironment->jsonBuffer->append('?');
				}

				oracleEnvironment->jsonBuffer->append('"');
				prevValue = true;
			}
			bits <<= 1;
			if (bits == 0) {
				bits = 1;
				nulls = redoLogRecord->data[fieldPos2 + (i >> 3)];
			}
			fieldPosTmp += (redoLogRecord->fieldLengths[i + 3] + 3) & 0xFFFC;
		}

		/*
		uint32_t colTotal = object->columns.size();
		for (uint32_t i = colCount; i < colTotal; ++i) {
			oracleEnvironment->jsonBuffer
					.append(",  \"")
					.append(object->columns[i]->columnName)
					.append("\": NULL");
		}
		*/
		oracleEnvironment->jsonBuffer
				->append("}}");
	}

	uint16_t OpCode0B02::getOpCode(void) {
		return 0x0B02;
	}

	string OpCode0B02::getName() {
		return "REDO INS   ";
	}

	//insert
	void OpCode0B02::process() {
		dump();
	}
}
