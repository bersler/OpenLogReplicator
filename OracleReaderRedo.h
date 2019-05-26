/* Header for OracleReaderRedo class
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
#include "RedoLogRecord.h"

#ifndef ORACLEREADERREDO_H_
#define ORACLEREADERREDO_H_

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

	class OracleReader;
	class OracleEnvironment;
	class OpCode;

	class OracleReaderRedo {
	private:
		OracleEnvironment *oracleEnvironment;
		int group;
		typescn curScn;
		typescn firstScn;
		typescn nextScn;
		uint32_t recordBeginPos;
		uint32_t recordBeginBlock;
		typetime recordTimestmap;
		uint32_t recordObjd;

		uint32_t blockSize;
		uint32_t blockNumber;
		uint32_t numBlocks;
		uint32_t redoBufferPos;
		uint64_t redoBufferFileStart;
		uint64_t redoBufferFileEnd;
		uint32_t recordPos;
		uint32_t recordLeftToCopy;
		uint32_t lastRead;
		uint32_t headerBufferFileEnd;
		bool lastReadSuccessfull;
		bool redoOverwritten;
		char SID[9];
		int fileDes;

		int initFile();
		int readFileMore();
		int checkBlockHeader(uint8_t *buffer, uint32_t blockNumberExpected);
		int checkRedoHeader(bool first);
		int processBuffer();
		void analyzeRecord();
		void flushTransactions();
		void appendToTransaction(RedoLogRecord *redoLogRecord);
		void appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2);
		uint16_t calcChSum(uint8_t *buffer, uint32_t size);

	public:
		string path;
		typeseq sequence;

		int processLog(OracleReader *oracleReader);

		OracleReaderRedo(OracleEnvironment *oracleEnvironment, int group, typescn firstScn,
				typescn nextScn, typeseq sequence, const char* path);
		virtual ~OracleReaderRedo();

		friend ostream& operator<<(ostream& os, const OracleReaderRedo& ors);
	};
}

#endif
