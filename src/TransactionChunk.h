/* Header for TransactionChunk class
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

#include "types.h"

#ifndef TRANSACTIONCHUNK_H_
#define TRANSACTIONCHUNK_H_

namespace OpenLogReplicatorOracle {

	class TransactionChunk {
	public:
		uint32_t elements;
		uint32_t size;
		uint8_t *buffer;
		TransactionChunk *prev;
		TransactionChunk *next;

		TransactionChunk(TransactionChunk *prev, uint8_t *buffer);
		virtual ~TransactionChunk();
	};
}

#endif
