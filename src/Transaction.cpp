/* Transaction from Oracle database
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
#include <string>
#include "types.h"
#include "OracleEnvironment.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "TransactionChunk.h"
#include "RedoLogRecord.h"
#include "OpCode.h"
#include "OpCode0501.h"
#include "OpCode0502.h"
#include "OpCode0504.h"
#include "OpCode0B02.h"

using namespace std;

namespace OpenLogReplicatorOracle {

	bool Transaction::operator< (Transaction &p) {
		if (isCommit && !p.isCommit)
			return true;
		if (!isCommit && p.isCommit)
			return false;

		bool ret = lastScn < p.lastScn;
		if (ret != false)
			return ret;
		return lastScn == p.lastScn && xid < p.xid;
	}

	void Transaction::touch(typescn scn) {
    	if (firstScn == ZERO_SCN || firstScn > scn)
    		firstScn = scn;
    	if (lastScn == ZERO_SCN || lastScn < scn)
    		lastScn = scn;
	}

    void Transaction::add(uint32_t objn, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2,
    		TransactionBuffer *transactionBuffer) {
    	tcLast = transactionBuffer->addTransactionChunk(tcLast, objn, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
    	++opCodes;
    	touch(redoLogRecord1->scn);
    }

    void Transaction::rollbackLastOp(typescn scn, TransactionBuffer *transactionBuffer) {
    	tcLast = transactionBuffer->rollbackTransactionChunk(tcLast, lastUba, lastDba, lastSlt, lastRci);

    	--opCodes;
    	if (lastScn == ZERO_SCN || lastScn < scn)
    		lastScn = scn;
    }

    void Transaction::flush(OracleEnvironment *oracleEnvironment) {
    	TransactionChunk *tcTemp = tc;
    	bool hasPrev = false;

    	//transaction that has some DML's

    	if (opCodes > 0 && !isRollback) {
			//cout << "Transaction xid:  " << PRINTXID(xid) <<
			//		" SCN: " << PRINTSCN(firstScn) <<
			//		" - " << PRINTSCN(lastScn) <<
			//		" opCodes: " << dec << opCodes <<  endl;

    		if (oracleEnvironment->jsonBuffer->posEnd >= INTRA_THREAD_BUFFER_SIZE - MAX_TRANSACTION_SIZE)
				oracleEnvironment->jsonBuffer->rewind();

			oracleEnvironment->jsonBuffer
					->beginTran()
					->append("{\"scn\": \"")
					->append(to_string(lastScn))
					->append("\", dml: [");

			while (tcTemp != nullptr) {
				uint32_t pos = 0;
				//typescn oldScn = 0;
				for (uint32_t i = 0; i < tcTemp->elements; ++i) {
					uint32_t op = *((uint32_t*)(tcTemp->buffer + pos + 4));
					RedoLogRecord *redoLogRecord1 = ((RedoLogRecord *)(tcTemp->buffer + pos + 8));
					RedoLogRecord *redoLogRecord2 = ((RedoLogRecord *)(tcTemp->buffer + pos + 8 + sizeof(struct RedoLogRecord)));

					//uint32_t objn = *((uint32_t*)(tcTemp->buffer + pos));
					//typescn scn = *((typescn *)(tcTemp->buffer + pos + 20 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
					//		redoLogRecord1->length + redoLogRecord2->length));
					//cout << "Row: " << dec << redoLogRecord1->length << ":" << redoLogRecord2->length <<
					//		" op: " << setfill('0') << setw(8) << hex << op <<
					//		" objn: " << dec << objn <<
					//		" scn: " << PRINTSCN(scn) << endl;
					//if (oldScn != 0 && oldScn > scn)
					//	cerr << "ERROR: SCN swap" << endl;

					redoLogRecord1->data = tcTemp->buffer + pos + 8 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord);
					redoLogRecord2->data = tcTemp->buffer + pos + 8 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) + redoLogRecord1->length;

					switch (op) {
					//single row insert
					case 0x05010B02:
						if (hasPrev)
							oracleEnvironment->jsonBuffer->append(", ");
						OpCode0B02 *opCode0B02 = new OpCode0B02(oracleEnvironment, redoLogRecord2, true);
						opCode0B02->parseDml();
						hasPrev = true;
						delete opCode0B02;
						break;
					}

					pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_MEMORY;

					if (oracleEnvironment->jsonBuffer->currentTranSize() >= MAX_TRANSACTION_SIZE) {
						cerr << "WARNING: Big transaction divided (" << oracleEnvironment->jsonBuffer->currentTranSize() << ")" << endl;
						oracleEnvironment->jsonBuffer
								->append("]}")
								->commitTran();

						if (oracleEnvironment->jsonBuffer->posEnd >= INTRA_THREAD_BUFFER_SIZE - MAX_TRANSACTION_SIZE)
							oracleEnvironment->jsonBuffer->rewind();

						oracleEnvironment->jsonBuffer
								->beginTran()
								->append("{\"scn\": \"")
								->append(to_string(lastScn))
								->append("\", dml: [");
					}

					//oldScn = scn;
				}
				tcTemp = tcTemp->next;
			}
			oracleEnvironment->jsonBuffer
					->append("]}")
					->commitTran();
    	}

    	oracleEnvironment->transactionBuffer.deleteTransactionChunks(tc, tcLast);
    }

	Transaction::Transaction(typexid xid, TransactionBuffer *transactionBuffer) :
			xid(xid),
			firstScn(ZERO_SCN),
			lastScn(ZERO_SCN),
			opCodes(0),
			pos(0),
			lastUba(0),
			lastDba(0),
			lastSlt(0),
			lastRci(0),
			isBegin(false),
			isCommit(false),
			isRollback(false),
			next(nullptr) {
		tc = transactionBuffer->newTransactionChunk();
		tcLast = tc;
	}

	Transaction::~Transaction() {
	}
}

