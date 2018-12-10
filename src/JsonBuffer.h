/* Header for JsonBuffer class
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

#include <stdint.h>
#include <string>
#include <mutex>
#include <condition_variable>
#include "types.h"

#ifndef JSONBUFFER_H_
#define JSONBUFFER_H_

using namespace std;

namespace OpenLogReplicator {

	class JsonBuffer {
	protected:
		volatile bool shutdown;
	public:
		uint8_t *intraThreadBuffer;
		mutex mtx;
		condition_variable readers;
		condition_variable writer;
		volatile uint64_t posStart;
		volatile uint64_t posEnd;
		volatile uint64_t posEndTmp;
		volatile uint32_t posSize;

		void terminate(void);
		JsonBuffer* appendEscape(const uint8_t *str, uint32_t length);
		JsonBuffer* append(const string str);
		JsonBuffer* append(char chr);
		JsonBuffer* beginTran();
		JsonBuffer* commitTran();
		JsonBuffer* rewind();
		uint32_t currentTranSize();

		JsonBuffer();
		virtual ~JsonBuffer();
	};
}

#endif
