/* Header for OracleReader class
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

#include <set>
#include <queue>
#include <stdint.h>
#include <occi.h>

#include "types.h"
#include "Thread.h"

#ifndef ORACLEREADER_H_
#define ORACLEREADER_H_

using namespace std;
using namespace oracle::occi;
using namespace OpenLogReplicator;

namespace OpenLogReplicator {
	class CommandBuffer;
}

namespace OpenLogReplicatorOracle {

	class OracleEnvironment;
	class OracleReaderRedo;

	struct OracleReaderRedoCompare {
		bool operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2);
	};

	struct OracleReaderRedoCompareReverse {
		bool operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2);
	};


	class OracleReader : public Thread {
	protected:
		OracleEnvironment *oracleEnvironment;
		OracleReaderRedo* currentRedo;
		string database;
		typeseq databaseSequence;
		typeseq databaseSequenceArchMax;
		typescn databaseScn;
		Environment *env;
		Connection *conn;
		string user;
		string passwd;
		string connectString;

		priority_queue<OracleReaderRedo*, vector<OracleReaderRedo*>, OracleReaderRedoCompare> archiveRedoQueue;
		set<OracleReaderRedo*> redoSet;

		void checkConnection(bool reconnect);
		void archLogGetList();
		void onlineLogGetList();

	public:
		virtual void *run();

		void addTable(string mask);
		void readCheckpoint();
		void writeCheckpoint();
		int initialize();

		OracleReader(CommandBuffer *commandBuffer, const string alias, const string database, const string user, const string passwd,
				const string connectString, int trace, bool dumpLogFile, bool dumpData, bool directRead);
		virtual ~OracleReader();
	};
}

#endif
