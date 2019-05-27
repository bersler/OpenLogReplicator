/* Header for OpCode0504 class
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

#include "OpCode.h"

#ifndef OPCODE0504_H_
#define OPCODE0504_H_

namespace OpenLogReplicatorOracle {

#define OPCODE0504_ROLLBACK 4

	class RedoLogRecord;

	class OpCode0504: public OpCode {
	protected:
		void ktucm(uint32_t fieldPos, uint32_t fieldLength);
		void ktucf(uint32_t fieldPos, uint32_t fieldLength);

	public:
		OpCode0504(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord);
		virtual ~OpCode0504();

		virtual uint16_t getOpCode(void);
		virtual void process();
	};
}

#endif
