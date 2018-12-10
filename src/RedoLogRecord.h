/* Header for RedoLogRecord class
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

#include "types.h"

#ifndef REDOLOGRECORD_H_
#define REDOLOGRECORD_H_

using namespace std;

namespace OpenLogReplicatorOracle {

	class OracleObject;

	class RedoLogRecord {
	public:
		typescn scn;			//scn
		uint8_t *data;			//data
	    uint16_t *fieldLengths;
		uint16_t fieldNum;
		uint32_t fieldPos;

		uint32_t length;		//length
		uint32_t dba;
		uint32_t bdba;			//block DBA
		uint32_t objn;			//object ID
		uint32_t objd;			//object version ID
		uint32_t tsn;
		uint32_t undo;
		OracleObject *object;
		typexid xid;			//transaction id
		typeuba uba;			//Undo Block Address

		uint8_t slt;
		uint8_t rci;
		uint8_t flg;			//flag (for opCode 0504)
		uint16_t opCode;		//operation code
		uint16_t opc;			//operation code for UNDO

		void dump();
	};

#define ROW_HEADER_MEMORY (sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeuba)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(typescn))

}

#endif
