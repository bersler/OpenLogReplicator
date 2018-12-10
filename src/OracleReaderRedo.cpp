/* Class reading a redo log file
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

#include <string>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <iomanip>
#include <list>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "OracleReader.h"
#include "OracleReaderRedo.h"
#include "RedoLogException.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"
#include "Transaction.h"
#include "TransactionMap.h"
#include "OpCode0501.h"
#include "OpCode0502.h"
#include "OpCode0504.h"
#include "OpCode0506.h"
#include "OpCode050B.h"
#include "OpCode0B02.h"
#include "OpCode0B03.h"

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

	OracleReaderRedo::OracleReaderRedo(OracleEnvironment *oracleEnvironment, int group, typescn firstScn,
				typescn nextScn, typeseq sequence, const char* path) :
			oracleEnvironment(oracleEnvironment),
			group(group),
			curScn(ZERO_SCN),
			firstScn(firstScn),
			nextScn(nextScn),
			blockSize(0),
			blockNumber(0),
			numBlocks(0),
			redoBufferPos(0),
			redoBufferFileStart(0),
			redoBufferFileEnd(0),
			recordPos(0),
			recordLeftToCopy(0),
			lastRead(READ_CHUNK_MIN_SIZE),
			headerBufferFileEnd(0),
			lastReadSuccessfull(false),
			redoOverwritten(false),
			fileDes(-1),
			path(path),
			sequence(sequence) {
	}

	int OracleReaderRedo::checkBlockHeader(uint8_t *buffer, uint32_t blockNumberExpected) {
		if (buffer[0] == 0 && buffer[1] == 0)
			return REDO_EMPTY;

		if (buffer[0] != 1 || buffer[1] != 0x22) {
			cerr << "ERROR: header bad magic number for block " << blockNumberExpected << endl;
			return REDO_ERROR;
		}

		uint32_t sequenceCheck = oracleEnvironment->read32(buffer + 8);
		if (oracleEnvironment->dumpData)
			cout << "sequence on disk: " << dec << sequenceCheck << ", my: " << sequence << ", block: " << blockNumberExpected << "/" << numBlocks << ", nextScn: " << hex << nextScn << endl;

		if (sequence != sequenceCheck)
			return REDO_WRONG_SEQUENCE;

		uint32_t blockNumberCheck = oracleEnvironment->read32(buffer + 4);
		if (blockNumberCheck != blockNumberExpected) {
			cerr << "ERROR: header bad block number for " << blockNumberExpected << ", found: " << blockNumberCheck << endl;
			return REDO_ERROR;
		}

		return REDO_OK;
	}

	int OracleReaderRedo::checkRedoHeader() {
		headerBufferFileEnd = pread(fileDes, oracleEnvironment->headerBuffer, REDO_PAGE_SIZE_MAX * 2, 0);
		if (headerBufferFileEnd < REDO_PAGE_SIZE_MIN * 2) {
			cerr << "ERROR: unable to read redo header for " << path.c_str() << endl;
			return REDO_ERROR;
		}

		//check file header
		if (oracleEnvironment->headerBuffer[0] != 0 ||
				oracleEnvironment->headerBuffer[1] != 0x22 ||
				oracleEnvironment->headerBuffer[28] != 0x7D ||
				oracleEnvironment->headerBuffer[29] != 0x7C ||
				oracleEnvironment->headerBuffer[30] != 0x7B ||
				oracleEnvironment->headerBuffer[31] != 0x7A) {
			cerr << "[0]: " << hex << (int)oracleEnvironment->headerBuffer[0] << endl;
			cerr << "[1]: " << hex << (int)oracleEnvironment->headerBuffer[1] << endl;
			cerr << "[28]: " << hex << (int)oracleEnvironment->headerBuffer[28] << endl;
			cerr << "[29]: " << hex << (int)oracleEnvironment->headerBuffer[29] << endl;
			cerr << "[30]: " << hex << (int)oracleEnvironment->headerBuffer[30] << endl;
			cerr << "[31]: " << hex << (int)oracleEnvironment->headerBuffer[31] << endl;
			cerr << "ERROR: block hader bad magic fields" << endl;
			return REDO_ERROR;
		}

		blockSize = oracleEnvironment->read16(oracleEnvironment->headerBuffer + 20);
		if (blockSize != 512 && blockSize != 1024) {
			cerr << "ERROR: unsupported block size: " << blockSize << endl;
			return REDO_ERROR;
		}

		//check first block
		if (headerBufferFileEnd < blockSize * 2) {
			cerr << "ERROR: unable to read redo header for " << path.c_str() << endl;
			return REDO_ERROR;
		}

		numBlocks = oracleEnvironment->read32(oracleEnvironment->headerBuffer + 24);
		uint32_t databaseVersion = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 20);
		typescn firstScnCheck = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 180);
		typescn nextScnCheck = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 192);

		if (oracleEnvironment->dumpData)
			cout << "* blocks: " << numBlocks << endl;

		if (databaseVersion != 0x0B200400) { //11.2.0.4
			cerr << "ERROR: Unsupported database version: " << databaseVersion << endl;
			return REDO_ERROR;
		}

		int ret = checkBlockHeader(oracleEnvironment->headerBuffer + blockSize, 1);
		if (ret == REDO_ERROR) {
			cerr << "ERROR: bad header" << endl;
			return ret;
		}

		if (firstScnCheck != firstScn) {
			//archive log incorrect sequence
			if (group == 0) {
				cerr << "ERROR: first SCN (" << firstScnCheck << ") does not match database information (" <<
						firstScn << "): " << path.c_str() << endl;
				return REDO_ERROR;
			//redo log switch appeared and header is now overwritten
			} else {
				cerr << "WARNING: first SCN (" << firstScnCheck << ") does not match database information (" <<
						firstScn << "): " << path.c_str() << endl;
				return REDO_WRONG_SEQUENCE_SWITCHED;
			}
		}

		//updating nextScn if changed
		if (nextScn == ZERO_SCN && nextScnCheck != ZERO_SCN) {
			cerr << "WARNING: log switch to " << nextScnCheck << endl;
			nextScn = nextScnCheck;
		} else
		if (nextScn != ZERO_SCN && nextScnCheck != ZERO_SCN && nextScn != nextScnCheck) {
			cerr << "ERROR: next SCN (" << firstScnCheck << ") does not match database information (" <<
					firstScn << "): " << path.c_str() << endl;
			return REDO_ERROR;
		}

		//typescn resetlogsScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 208);
		//typescn threadClosedScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 220);
		memcpy(SID, oracleEnvironment->headerBuffer + blockSize + 28, 8); SID[8] = 0;

		if (oracleEnvironment->dumpData) {
			uint32_t databaseId = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 24);
			cout << "SID: " << SID << " (id: " << databaseId << ")" << endl;
		}

		return ret;
	}

	int OracleReaderRedo::initFile() {
		if (fileDes != -1)
			return REDO_OK;

		fileDes = open(path.c_str(), O_RDONLY | O_LARGEFILE | (oracleEnvironment->directRead ? O_DIRECT : 0));
		if (fileDes <= 0) {
			cerr << "ERROR: can not open: " << path.c_str() << endl;
			return REDO_ERROR;
		}
		return REDO_OK;
	}

	int OracleReaderRedo::readFileMore() {
		uint32_t curRead;
		if (redoBufferPos == REDO_LOG_BUFFER_SIZE)
			redoBufferPos = 0;

		if (lastReadSuccessfull && lastRead * 2 < REDO_LOG_BUFFER_SIZE)
			lastRead *= 2;
		curRead = lastRead;
		if (redoBufferPos == REDO_LOG_BUFFER_SIZE)
			redoBufferPos = 0;

		if (redoBufferPos + curRead > REDO_LOG_BUFFER_SIZE)
			curRead = REDO_LOG_BUFFER_SIZE - redoBufferPos;

		uint32_t bytes = pread(fileDes, oracleEnvironment->redoBuffer + redoBufferPos, curRead, redoBufferFileStart);

		if (bytes < curRead) {
			lastReadSuccessfull = false;
			lastRead = READ_CHUNK_MIN_SIZE;
		} else
			lastReadSuccessfull = true;

		if (oracleEnvironment->dumpData)
			cout << path << " read " << bytes << " bytes" << endl;

		if (bytes > 0) {
			uint32_t maxNumBlock = bytes / blockSize;

			for (uint32_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
				int ret = checkBlockHeader(oracleEnvironment->redoBuffer + redoBufferPos + numBlock * blockSize, blockNumber + numBlock);
				if (ret != REDO_OK) {
					lastReadSuccessfull = false;
					lastRead = READ_CHUNK_MIN_SIZE;

					if (redoBufferFileStart < redoBufferFileEnd)
						return REDO_OK;

					return ret;
				}

				redoBufferFileEnd += blockSize;
			}
		}
		return REDO_OK;
	}

	void OracleReaderRedo::analyzeRecord() {
		bool encrypted = false;
		bool checkpoint = false;
		RedoLogRecord redoLogRecord[2], *redoLogRecordCur, *redoLogRecordPrev;
		uint32_t redoLogCur = 0;
		redoLogRecordCur = &redoLogRecord[redoLogCur];
		redoLogRecordPrev = nullptr;

		uint32_t recordLength = oracleEnvironment->read32(oracleEnvironment->recordBuffer);
		uint8_t vld = oracleEnvironment->recordBuffer[4];
		curScn = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 8);
		uint32_t vectorNo = 1;

		//typescn extScn = read32(recordBuffer + 40);
		//uint32_t timestamp = read32(recordBuffer + 64);
		uint16_t headerLength;
		if ((vld & 4) == 4) {
			checkpoint = true;
			headerLength = 68;
		} else
			headerLength = 24;

		if (oracleEnvironment->dumpData)
			cout << endl;

		if (oracleEnvironment->dumpLogFile) {
			uint8_t subScn = oracleEnvironment->recordBuffer[12];
			cout << endl;
			cout << "SCN: " << PRINTSCN(curScn) <<
			" SUBSCN:" << setfill(' ') << setw(3) << dec << (int)subScn << endl;
		}

		if (headerLength > recordLength)
			throw RedoLogException("ERROR: too small log record: ", path.c_str(), recordLength);
		if (oracleEnvironment->dumpData) {
			for (int j = 0; j < headerLength; ++j)
				cout << hex << setfill('0') << setw(2) << (unsigned int) oracleEnvironment->recordBuffer[j] << " ";
			cout << endl;
		}

		uint32_t objn = 4294967295;
		//uint32_t objd = 4294967295;
		uint32_t pos = headerLength;
		while (pos < recordLength) {
			//uint16_t opc = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos);
			uint16_t cls = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 2);
			uint32_t dba = oracleEnvironment->read32(oracleEnvironment->recordBuffer + pos + 8);
			int16_t usn = (cls >= 15) ? (cls - 15) / 2 : -1;
			uint8_t typ = oracleEnvironment->recordBuffer[pos + 21];
			if ((typ & 0x80) == 0x80)
				encrypted = true;

			if (pos + 24 + 1 >= recordLength)
				throw RedoLogException("ERROR: position of field list outside of record: ", nullptr, pos + 24);
			uint16_t *fieldList = (uint16_t*)(oracleEnvironment->recordBuffer + pos + 24);
			uint16_t fieldNum = (fieldList[0] - 2) / 2;

			memset(redoLogRecordCur, 0, sizeof(struct RedoLogRecord));
			redoLogRecordCur->opCode = (((uint16_t)oracleEnvironment->recordBuffer[pos + 0]) << 8) |
					oracleEnvironment->recordBuffer[pos + 1];
			redoLogRecordCur->length = 24 + ((fieldList[0] + 2) & 0xFFFC);
			redoLogRecordCur->dba = dba;
			redoLogRecordCur->scn = curScn;
			redoLogRecordCur->data = oracleEnvironment->recordBuffer + pos;
			redoLogRecordCur->fieldLengths = (uint16_t*)(redoLogRecordCur->data + 24);
			redoLogRecordCur->fieldNum = (redoLogRecordCur->fieldLengths[0] - 2) / 2;
			redoLogRecordCur->fieldPos = 24 + ((redoLogRecordCur->fieldLengths[0] + 2) & 0xFFFC);
			if (redoLogRecordCur->fieldPos > redoLogRecordCur->length)
				throw RedoLogException("ERROR: incomplete record", nullptr, 0);


			for (int i = 1; i <= fieldNum; ++i) {
				redoLogRecordCur->length += (fieldList[i] + 3) & 0xFFFC;
				if (pos + redoLogRecordCur->length > recordLength)
					throw RedoLogException("ERROR: position of field list outside of record: ", nullptr, pos + 24);
			}

			if (oracleEnvironment->dumpData)
				cout << "Code: " << setfill('0') << setw(4) << hex << redoLogRecordCur->opCode << endl;

			if (oracleEnvironment->dumpLogFile) {
				uint16_t afn = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 4);
				typescn scn2 = oracleEnvironment->read48(oracleEnvironment->recordBuffer + pos + 12);
				uint8_t seq = oracleEnvironment->recordBuffer[pos + 20];
				uint32_t rbl = 0; //FIXME

				cout << "CHANGE #" << dec << vectorNo <<
					" TYP:" << (int)typ <<
					" CLS:" << cls <<
					" AFN:" << afn <<
					" DBA:0x" << setfill('0') << setw(8) << hex << dba <<
					" OBJ:" << dec << objn <<
					" SCN:" << PRINTSCN(scn2) <<
					" SEQ:" << dec << (int)seq <<
					" OP:" << (int)(redoLogRecordCur->opCode >> 8) << "." << (int)(redoLogRecordCur->opCode & 0xFF) <<
					" ENC:" << dec << (int)encrypted <<
					" RBL:" << dec << rbl << endl;
			}

			OpCode *opCode = nullptr;
			pos += redoLogRecordCur->length;
			//begin transaction
			if (redoLogRecordCur->opCode == 0x0502) {
				opCode = new OpCode0502(oracleEnvironment, redoLogRecordCur, usn);
				if (SQN(redoLogRecordCur->xid) > 0)
					appendToTransaction(redoLogRecordCur);
			} else
			//commit transaction (or rollback)
			if (redoLogRecordCur->opCode == 0x0504) {
				opCode = new OpCode0504(oracleEnvironment, redoLogRecordCur, usn);
				appendToTransaction(redoLogRecordCur);
			} else {
				switch (redoLogRecordCur->opCode) {
				//Undo
				case 0x0501:
					opCode = new OpCode0501(oracleEnvironment, redoLogRecordCur);
					break;

				//Partial rollback
				case 0x0506:
					opCode = new OpCode0506(oracleEnvironment, redoLogRecordCur);
					break;
				case 0x050B:
					opCode = new OpCode050B(oracleEnvironment, redoLogRecordCur);
					break;

				//REDO: Insert single row
				case 0x0B02:
					opCode = new OpCode0B02(oracleEnvironment, redoLogRecordCur);
					break;

				//REDO: Delete single row
				case 0x0B03:
					opCode = new OpCode0B03(oracleEnvironment, redoLogRecordCur);
					break;
				}

				if (redoLogRecordCur->opCode != 0) {
					if (redoLogRecordPrev == nullptr) {
						redoLogRecordPrev = redoLogRecordCur;
						redoLogCur = 1 - redoLogCur;
						redoLogRecordCur = &redoLogRecord[redoLogCur];
					} else {
						appendToTransaction(redoLogRecordPrev, redoLogRecordCur);
						redoLogRecordPrev = nullptr;
					}
				}
			}

			if (opCode != nullptr) {
				delete opCode;
				opCode = nullptr;
			}
			++vectorNo;
		}

		if (redoLogRecordPrev != nullptr) {
			appendToTransaction(redoLogRecordPrev);
			redoLogRecordPrev = nullptr;
		}

		if (oracleEnvironment->dumpData)
			cout << endl;

		if (checkpoint) {
			if (oracleEnvironment->dumpData)
				cout << "CHECKPOINT with SCN: " << PRINTSCN(curScn) << endl;
			flushTransactions();
		}
	}

	void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord) {
		if (oracleEnvironment->dumpData) {
			cout << "** Append: " <<
					setfill('0') << setw(4) << hex << redoLogRecord->opCode << endl;
			redoLogRecord->dump();
		}

		if (redoLogRecord->opCode != 0x0502 && redoLogRecord->opCode != 0x0504)
			return;

		Transaction *transaction = oracleEnvironment->xidTransactionMap[redoLogRecord->xid];
		if (transaction == nullptr) {
			transaction = new Transaction(redoLogRecord->xid, &oracleEnvironment->transactionBuffer);
			transaction->touch(curScn);
			oracleEnvironment->xidTransactionMap[redoLogRecord->xid] = transaction;
			oracleEnvironment->transactionHeap.add(transaction);
		} else
			transaction->touch(curScn);

		if (redoLogRecord->opCode == 0x0502) {
	    	transaction->isBegin = true;
		}

		if (redoLogRecord->opCode == 0x0504) {
	    	transaction->isCommit = true;
	        if ((redoLogRecord->flg & OPCODE0504_ROLLBACK) != 0)
	        	transaction->isRollback = true;
			oracleEnvironment->transactionHeap.update(transaction->pos);
		}
	}

	void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
		if (oracleEnvironment->dumpData) {
			cout << "** Append: " <<
					setfill('0') << setw(4) << hex << redoLogRecord1->opCode << " + " <<
					setfill('0') << setw(4) << hex << redoLogRecord2->opCode << endl;
			redoLogRecord1->dump();
			redoLogRecord2->dump();
		}

		uint32_t objn, objd;
		if (redoLogRecord1->objn != 0) {
			objn = redoLogRecord1->objn;
			objd = redoLogRecord1->objd;
		} else {
			objn = redoLogRecord2->objn;
			objd = redoLogRecord2->objd;
		}

		if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord2->bdba != 0) {
			cerr << "ERROR: BDBA does not match!" << endl;
			redoLogRecord1->dump();
			redoLogRecord2->dump();
			return;
		}

		redoLogRecord1->object = oracleEnvironment->checkDict(objn, objd);
		if (redoLogRecord1->object == nullptr)
			return;
		redoLogRecord2->object = redoLogRecord1->object;

		long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;

		switch (opCodeLong) {
		//single row insert
		case 0x05010B02:
		//single row delete
		//case 0x05010B03:
			{
				//cerr << "UBA: " << PRINTUBA(redoLogRecord1->uba) << endl;
				//cerr << "DBA: " << hex << redoLogRecord1->dba << endl;
				//cerr << "SLT: " << hex << (int)redoLogRecord1->slt << endl;
				//redoLogRecord1->dump();
				//redoLogRecord2->dump();
				Transaction *transaction = oracleEnvironment->xidTransactionMap[redoLogRecord1->xid];
				if (transaction == nullptr) {
					transaction = new Transaction(redoLogRecord1->xid, &oracleEnvironment->transactionBuffer);
					transaction->add(objn, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1, redoLogRecord2, &oracleEnvironment->transactionBuffer);
					oracleEnvironment->xidTransactionMap[redoLogRecord1->xid] = transaction;
					oracleEnvironment->transactionHeap.add(transaction);
				} else {
					if (transaction->opCodes > 0)
						oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba, transaction->lastSlt);
					transaction->add(objn, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1, redoLogRecord2, &oracleEnvironment->transactionBuffer);
					oracleEnvironment->transactionHeap.update(transaction->pos);
				}
				transaction->lastUba = redoLogRecord1->uba;
				transaction->lastDba = redoLogRecord1->dba;
				transaction->lastSlt = redoLogRecord1->slt;
				if (oracleEnvironment->lastOpTransactionMap.get(redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt) != nullptr) {
					cerr << "ERROR: last UBA already occupied!" << endl;
					redoLogRecord1->dump();
					redoLogRecord2->dump();
				} else {
					oracleEnvironment->lastOpTransactionMap.set(redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, transaction);
				}
				oracleEnvironment->transactionHeap.update(transaction->pos);
			}
			break;

		//rollback: delete row
		case 0x0B030506:
		case 0x0B03050B:
		//rollback: insert row
		//case 0x0B020506:
		//case 0x0B02050B:
			{
				//cerr << "UNDO UBA: " << PRINTUBA(redoLogRecord1->uba) << endl;
				//cerr << "UNDO DBA: " << hex << redoLogRecord1->dba << endl;
				//cerr << "UNDO SLT: " << hex << (int)redoLogRecord1->slt << endl;
				//redoLogRecord1->dump();
				//redoLogRecord2->dump();
				Transaction *transaction = oracleEnvironment->lastOpTransactionMap.get(redoLogRecord1->uba, redoLogRecord2->dba, redoLogRecord2->slt);
				//match
				if (transaction != nullptr) {
					oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba, transaction->lastSlt);
					transaction->rollbackLastOp(curScn, &oracleEnvironment->transactionBuffer);
					oracleEnvironment->transactionHeap.update(transaction->pos);
					oracleEnvironment->lastOpTransactionMap.set(transaction->lastUba, transaction->lastDba, transaction->lastSlt, transaction);
				} else {
					cerr << "WARNING: can't rollback transaction part, UBA: " << PRINTUBA(redoLogRecord1->uba) <<
							" DBA: " << hex << redoLogRecord2->dba << " SLT: " << dec << (uint32_t)redoLogRecord2->slt << endl;
					//redoLogRecord1->dump();
					//redoLogRecord2->dump();
				}
			}

			break;
		}
	}


	void OracleReaderRedo::flushTransactions() {
		Transaction *transaction = oracleEnvironment->transactionHeap.top();

		if (oracleEnvironment->dumpData) {
			cout << "##########################" << endl <<
					"checkpoint for SCN: " << PRINTSCN(curScn) << endl;
		}

		while (transaction != nullptr) {
			if (oracleEnvironment->dumpData) {
				cout << "FirstScn: " << PRINTSCN(transaction->firstScn) <<
						" lastScn: " << PRINTSCN(transaction->lastScn) <<
						" xid: " << PRINTXID(transaction->xid) <<
						" pos: " << dec << transaction->pos <<
						" opCodes: " << transaction->opCodes <<
						" commit: " << transaction->isCommit <<
						" uba: " << PRINTUBA(transaction->lastUba) <<
						" dba: " << transaction->lastDba <<
						" slt: " << hex << (int)transaction->lastSlt <<
						endl;
			}

			if (transaction->lastScn <= curScn && transaction->isCommit) {
				if (transaction->isBegin)
					//FIXME: it should be checked if transaction begin SCN is within captured range of SCNs
					transaction->flush(oracleEnvironment);
				else
					cerr << "WARNING: skipping transaction with no begin, XID: " << PRINTXID(transaction->xid) << endl;

				oracleEnvironment->transactionHeap.pop();
				if (transaction->opCodes > 0)
					oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba, transaction->lastSlt);
				oracleEnvironment->xidTransactionMap.erase(transaction->xid);

				delete oracleEnvironment->xidTransactionMap[transaction->xid];
				transaction = oracleEnvironment->transactionHeap.top();
			} else
				break;
		}

		if (oracleEnvironment->dumpData) {
			for (auto const& xid : oracleEnvironment->xidTransactionMap) {
				Transaction *transaction = oracleEnvironment->xidTransactionMap[xid.first];
				cout << "Queue: " << PRINTSCN(transaction->firstScn) <<
						" lastScn: " << PRINTSCN(transaction->lastScn) <<
						" xid: " << PRINTXID(transaction->xid) <<
						" pos: " << dec << transaction->pos <<
						" opCodes: " << transaction->opCodes <<
						" commit: " << transaction->isCommit << endl;
			}
			cout << "##########################" << endl;
		}
	}

	int OracleReaderRedo::processBuffer(void) {
		while (redoBufferFileStart < redoBufferFileEnd) {
			if (oracleEnvironment->dumpData)
				cout << "Block: 0x" << hex << setfill('0') << setw(4) << blockNumber << endl;
			int ret = checkBlockHeader(oracleEnvironment->redoBuffer + redoBufferPos, blockNumber);
			if (ret != 0)
				return ret;

			uint32_t blockPos = 16;
			while (blockPos < blockSize) {
				//next part
				if (recordLeftToCopy == 0) {
					if (blockPos + 20 >= blockSize)
						break;

					recordLeftToCopy = (oracleEnvironment->read32(oracleEnvironment->redoBuffer + redoBufferPos + blockPos) + 3) & 0xFFFFFFFC;
					if (recordLeftToCopy > REDO_RECORD_MAX_SIZE)
						throw RedoLogException("ERROR: too big log record: ", path.c_str(), recordLeftToCopy);
					recordPos = 0;
				}

				//nothing more
				if (recordLeftToCopy == 0)
					break;

				uint32_t toCopy;
				if (blockPos + recordLeftToCopy > blockSize)
					toCopy = blockSize - blockPos;
				else
					toCopy = recordLeftToCopy;

				memcpy(oracleEnvironment->recordBuffer + recordPos, oracleEnvironment->redoBuffer + redoBufferPos + blockPos, toCopy);
				recordLeftToCopy -= toCopy;
				blockPos += toCopy;
				recordPos += toCopy;

				if (recordLeftToCopy == 0)
					analyzeRecord();
			}

			++blockNumber;
			redoBufferPos += blockSize;
			redoBufferFileStart += blockSize;
		}
		return REDO_OK;
	}

	int OracleReaderRedo::processLog(OracleReader *oracleReader) {
		//if (oracleEnvironment->dumpData)
			cout << "processLog: " << *this << endl;

		initFile();
		bool reachedEndOfOnlineRedo = false;
		int ret = checkRedoHeader();
		if (ret != REDO_OK)
			return ret;

		redoBufferFileStart = blockSize * 2;
		redoBufferFileEnd = blockSize * 2;
		blockNumber = 2;

		while (blockNumber <= numBlocks && !reachedEndOfOnlineRedo && !oracleReader->shutdown) {
			processBuffer();
			while (redoBufferFileStart == redoBufferFileEnd && blockNumber <= numBlocks && !reachedEndOfOnlineRedo
					&& !oracleReader->shutdown) {
				int ret = readFileMore();

				if (redoBufferFileStart < redoBufferFileEnd)
					break;

				//for archive redo log break on all errors
				if (group == 0) {
					return ret;
				//for online redo log
				} else {
					if (ret == REDO_ERROR || ret == REDO_WRONG_SEQUENCE_SWITCHED)
						return ret;

					//check if sequence has changed
					int ret = checkRedoHeader();
					if (ret != REDO_OK) {
						return ret;
					}

					if (nextScn != ZERO_SCN) {
						reachedEndOfOnlineRedo = true;
						break;
					}

					flushTransactions();
					//online redo log problem
					if (oracleReader->shutdown)
						break;

					usleep(REDO_SLEEP_RETRY);
				}
			}

			if (redoBufferFileStart == redoBufferFileEnd) {
				if (oracleEnvironment->dumpData)
					cout << "* break" << endl;
				break;
			}
		}
		if (oracleEnvironment->dumpData)
			cout << "* last block " << blockNumber << endl << endl;

		if (fileDes > 0) {
			close(fileDes);
			fileDes = 0;
		}
		return REDO_OK;
	}

	OracleReaderRedo::~OracleReaderRedo() {
	}

	ostream& operator<<(ostream& os, const OracleReaderRedo& ors) {
		os << "(" << ors.group << ", " << ors.firstScn << ", " << ors.sequence << ", \"" << ors.path << "\")";
		return os;
	}
}
