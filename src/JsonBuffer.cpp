/* Memory buffer for handling Json data
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
#include <string.h>
#include "JsonBuffer.h"
#include "types.h"

namespace OpenLogReplicator {

	JsonBuffer::JsonBuffer() :
			shutdown(false),
			posStart(0),
			posEnd(0),
			posEndTmp(0),
			posSize(0) {
		intraThreadBuffer = new uint8_t[INTRA_THREAD_BUFFER_SIZE];
	}

	void JsonBuffer::terminate(void) {
		this->shutdown = true;
	}

	JsonBuffer* JsonBuffer::appendEscape(const uint8_t *str, uint32_t length) {
		if (this->shutdown)
			return this;

		while (length > 0) {
			if (posSize > 0 && posEndTmp + 2 >= posStart) {
				unique_lock<mutex> lck(mtx);
				while (posSize > 0 && posEndTmp + 2 >= posStart) {
					writer.wait(lck);
					if (this->shutdown)
						return this;
				}
				if (this->shutdown)
					return this;
			}

			if (posEndTmp + 2 >= INTRA_THREAD_BUFFER_SIZE) {
				cerr << "ERROR: (1) buffer overflow" << endl;
				return this;
			}

			if (*str == '"' || *str == '\\')
				intraThreadBuffer[posEndTmp++] = '\\';

			intraThreadBuffer[posEndTmp++] = *(str++);

			--length;
		}

		return this;
	}

	JsonBuffer* JsonBuffer::append(const string str) {
		if (this->shutdown)
			return this;

		uint32_t length = str.length();
		if (posSize > 0 && posEndTmp + length >= posStart) {
			unique_lock<mutex> lck(mtx);
			while (posSize > 0 && posEndTmp + length >= posStart) {
				writer.wait(lck);
				if (this->shutdown)
					return this;
			}
		}
		if (posEndTmp + length >= INTRA_THREAD_BUFFER_SIZE) {
			cerr << "ERROR: (2) buffer overflow" << endl;
			return this;
		}

		memcpy(intraThreadBuffer + posEndTmp, str.c_str(), length);
		posEndTmp += length;

		return this;
	}

	JsonBuffer* JsonBuffer::append(char chr) {
		if (this->shutdown)
			return this;

		if (posSize > 0 && posEndTmp + 1 >= posStart) {
			unique_lock<mutex> lck(mtx);
			while (posSize > 0 && posEndTmp + 1 >= posStart) {
				writer.wait(lck);
				if (this->shutdown)
					return this;
			}
		}

		if (posEndTmp + 1 >= INTRA_THREAD_BUFFER_SIZE) {
			cerr << "ERROR: (3) buffer overflow" << endl;
			return this;
		}

		intraThreadBuffer[posEndTmp++] = chr;

		return this;
	}

	JsonBuffer* JsonBuffer::beginTran() {
		if (this->shutdown)
			return this;

		if (posSize > 0 && posEndTmp + 4 >= posStart) {
			unique_lock<mutex> lck(mtx);
			while (posSize > 0 && posEndTmp + 4 >= posStart) {
				writer.wait(lck);
				if (this->shutdown)
					return this;
			}
		}

		if (posEndTmp + 4 >= INTRA_THREAD_BUFFER_SIZE) {
			cerr << "ERROR: (4) buffer overflow" << endl;
			return this;
		}

		posEndTmp += 4;

		return this;
	}

	JsonBuffer* JsonBuffer::commitTran() {
		if (posEndTmp != posEnd) {
			unique_lock<mutex> lck(mtx);
			*((uint32_t*)(intraThreadBuffer + posEnd)) = posEndTmp - posEnd;
			posEndTmp = (posEndTmp + 3) & 0xFFFFFFFC;
			posEnd = posEndTmp;

			readers.notify_all();
		}
		return this;
	}

	JsonBuffer* JsonBuffer::rewind() {
		if (this->shutdown)
			return this;

		if (posSize > 0 || posStart == 0) {
			unique_lock<mutex> lck(mtx);
			while (posSize > 0 || posStart == 0) {
				writer.wait(lck);
				if (this->shutdown)
					return this;
			}
		}

		{
			unique_lock<mutex> lck(mtx);
			posSize = posEnd;
			posEnd = 0;
			posEndTmp = 0;
		}

		return this;
	}

	uint32_t JsonBuffer::currentTranSize() {
		return posEndTmp - posEnd;
	}

	JsonBuffer::~JsonBuffer() {
	}

}

