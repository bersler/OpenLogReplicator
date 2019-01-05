/* Oracle Redo OpCode: 11.2
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
#include "OpCode0B02.h"

#include "CommandBuffer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

	OpCode0B02::OpCode0B02(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord, bool fill) :
			OpCode(oracleEnvironment, redoLogRecord, fill) {
	}

	OpCode0B02::OpCode0B02(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord) :
			OpCode(oracleEnvironment, redoLogRecord) {

		uint32_t fieldPosTmp = redoLogRecord->fieldPos;
		for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
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
		uint32_t fieldPosTmp = redoLogRecord->fieldPos, fieldPosTmp2;
		uint32_t fieldPos2;
		uint32_t colCount;
		uint8_t nulls, bits = 1;
		bool prevValue = false;

		for (uint32_t i = 1; i <= 2; ++i) {
			if (i == 2) {
				uint8_t op = redoLogRecord->data[fieldPosTmp + 10];
				if ((op & 0x1F) != 0x02) {
					cerr << "ERROR: Insert operation with incorrect OP: " << dec << (uint32_t)op << endl;
					return;
				}
				colCount = oracleEnvironment->read16(redoLogRecord->data + fieldPosTmp + 18);
				fieldPos2 = fieldPosTmp + 45;
				nulls = redoLogRecord->data[fieldPos2];
			}
			fieldPosTmp += (redoLogRecord->fieldLengths[i] + 3) & 0xFFFC;
		}
		fieldPosTmp2 = fieldPosTmp;

		switch (oracleEnvironment->commandBuffer->type) {
		case COMMAND_BUFFER_JSON:
			oracleEnvironment->commandBuffer
					->append("{\"operation\":\"insert\", \"table\": \"")
					->append(redoLogRecord->object->owner)
					->append('.')
					->append(redoLogRecord->object->objectName)
					->append("\", \"after\": {");
			break;

		case COMMAND_BUFFER_REDIS:
			oracleEnvironment->commandBuffer
					->append(redoLogRecord->object->owner)
					->append('.')
					->append(redoLogRecord->object->objectName)
					->append('.');

			for (uint32_t i = 0; i < colCount; ++i) {
				//is PK or table has no PK
				if (object->columns[i]->numPk > 0 || object->totalPk == 0) {
					if (prevValue) {
						oracleEnvironment->commandBuffer
								->append('.');
					} else
						prevValue = true;

					//NULL values
					if ((nulls & bits) != 0 || redoLogRecord->fieldLengths[i + 3] == 0 || i >= object->columns.size()) {
						oracleEnvironment->commandBuffer
							->append("NULL");
					} else {

						oracleEnvironment->commandBuffer
								->append('"');

						appendValue(object->columns[i]->typeNo, fieldPosTmp, redoLogRecord->fieldLengths[i + 3]);

						oracleEnvironment->commandBuffer
								->append('"');
					}
				}
				bits <<= 1;
				if (bits == 0) {
					bits = 1;
					nulls = redoLogRecord->data[fieldPos2 + (i >> 3)];
				}
				fieldPosTmp += (redoLogRecord->fieldLengths[i + 3] + 3) & 0xFFFC;
			}
			fieldPosTmp = fieldPosTmp2;

			oracleEnvironment->commandBuffer
					->append(0);
			break;
		}
		prevValue = false;

		for (uint32_t i = 0; i < colCount; ++i) {
			//NULL values
			if ((nulls & bits) != 0 || redoLogRecord->fieldLengths[i + 3] == 0 || i >= object->columns.size()) {
				switch (oracleEnvironment->commandBuffer->type) {
				case COMMAND_BUFFER_JSON:
					break;

				case COMMAND_BUFFER_REDIS:
					if (prevValue) {
						oracleEnvironment->commandBuffer
								->append(',');
					} else
						prevValue = true;

					oracleEnvironment->commandBuffer
							->append("NULL");
					break;
				}

			} else {
				if (prevValue) {
					oracleEnvironment->commandBuffer
							->append(',');
				} else
					prevValue = true;

				switch (oracleEnvironment->commandBuffer->type) {
				case COMMAND_BUFFER_JSON:
					oracleEnvironment->commandBuffer
							->append('"')
							->append(object->columns[i]->columnName)
							->append("\": \"");
					break;

				case COMMAND_BUFFER_REDIS:
					oracleEnvironment->commandBuffer
							->append('"');
					break;
				}

				appendValue(object->columns[i]->typeNo, fieldPosTmp, redoLogRecord->fieldLengths[i + 3]);

				switch (oracleEnvironment->commandBuffer->type) {
				case COMMAND_BUFFER_JSON:
					oracleEnvironment->commandBuffer
							->append('"');
					break;

				case COMMAND_BUFFER_REDIS:
					oracleEnvironment->commandBuffer
							->append('"');
					break;
				}
			}
			bits <<= 1;
			if (bits == 0) {
				bits = 1;
				nulls = redoLogRecord->data[fieldPos2 + (i >> 3)];
			}
			fieldPosTmp += (redoLogRecord->fieldLengths[i + 3] + 3) & 0xFFFC;
		}

		if (oracleEnvironment->commandBuffer->type == COMMAND_BUFFER_REDIS) {
			uint32_t colTotal = object->columns.size();
			for (uint32_t i = colCount; i < colTotal; ++i) {
				if (prevValue) {
					oracleEnvironment->commandBuffer
							->append(',');
				} else
					prevValue = true;

				oracleEnvironment->commandBuffer
						->append("NULL");
			}
		}

		switch (oracleEnvironment->commandBuffer->type) {
		case COMMAND_BUFFER_JSON:
			oracleEnvironment->commandBuffer
					->append("}}");
			break;

		case COMMAND_BUFFER_REDIS:
			oracleEnvironment->commandBuffer
					->append(0);
			break;
		}

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
