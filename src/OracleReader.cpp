/* Thread reading Oracle Redo Logs
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

#include <sys/stat.h>
#include <string>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <unistd.h>
#include <rapidjson/document.h>
#include "types.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleReader.h"

#include "CommandBuffer.h"
#include "OracleReaderRedo.h"
#include "OracleEnvironment.h"
#include "RedoLogException.h"
#include "OracleStatement.h"

using namespace std;
using namespace rapidjson;
using namespace oracle::occi;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

	OracleReader::OracleReader(CommandBuffer *commandBuffer, const string alias, const string database, const string user, const string passwd,
			const string connectString, int trace, bool dumpLogFile, bool dumpData, bool directRead) :
		Thread(alias, commandBuffer),
		currentRedo(nullptr),
		database(database.c_str()),
		databaseSequence(0),
		databaseSequenceArchMax(0),
		databaseScn(0),
		env(nullptr),
		conn(nullptr),
		user(user),
		passwd(passwd),
		connectString(connectString) {

		oracleEnvironment = new OracleEnvironment(commandBuffer, trace, dumpLogFile, dumpData, directRead);
		readCheckpoint();
		env = Environment::createEnvironment (Environment::DEFAULT);
	}

	OracleReader::~OracleReader() {
		writeCheckpoint();

		while (!archiveRedoQueue.empty()) {
			OracleReaderRedo *redo = archiveRedoQueue.top();
			archiveRedoQueue.pop();
			delete redo;
		}

		if (conn != nullptr) {
			env->terminateConnection(conn);
			conn = nullptr;
		}
		if (env != nullptr) {
			Environment::terminateEnvironment(env);
			env = nullptr;
		}

		if (oracleEnvironment != nullptr) {
			delete oracleEnvironment;
			oracleEnvironment = nullptr;
		}
	}

	void OracleReader::checkConnection(bool reconnect) {
		while (!this->shutdown) {
			if (conn == nullptr) {
				cout << "- connecting to Oracle database " << database << endl;
				try {
					conn = env->createConnection(user, passwd, connectString);
				} catch(SQLException &ex) {
					cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();
				}
			}

			if (conn != nullptr || !reconnect)
				break;

			cerr << "ERROR: cannot connect to database, retry in 5 sec." << endl;
			sleep(5);
		}
	}

	void *OracleReader::run(void) {
		checkConnection(true);
		cout << "- Oracle Reader for: " << database << endl;

		while (!this->shutdown) {
			//try to read all archive logs
			if (archiveRedoQueue.empty()) {
				archLogGetList();
			}

			while (!archiveRedoQueue.empty()) {
				if (this->shutdown)
					return 0;

				OracleReaderRedo *redo = nullptr;
				redo = archiveRedoQueue.top();

				if (redo->sequence != databaseSequence) {
					cerr << "archive log path: " << redo->path << endl;
					cerr << "archive log sequence: " << redo->sequence << endl;
					cerr << "now should read: " << databaseSequence << endl;
					throw RedoLogException("incorrect archive log sequence", nullptr, 0);
				}

				int ret = redo->processLog(this);
				if (this->shutdown)
					return 0;

				if (ret != REDO_OK)
					throw RedoLogException("read archive log", nullptr, 0);

				databaseSequence = redo->sequence + 1;
				writeCheckpoint();
				archiveRedoQueue.pop();
				delete redo;
				redo = nullptr;
			}

			//switch to online log reading
			for (auto redo: redoSet)
				delete redo;
			redoSet.clear();
			onlineLogGetList();

			while (true) {
				if (this->shutdown)
					return 0;

				OracleReaderRedo *redo = nullptr;

				//find the candidate to read
				for (auto redoTmp: redoSet)
					if (redoTmp->sequence == databaseSequence)
						redo = redoTmp;

				//try to read the oldest sequence
				if (redo == nullptr) {
					for (auto redoTmp: redoSet)
						delete redoTmp;
					redoSet.clear();
					onlineLogGetList();

					bool isHigher = false;
					while (true) {
						for (auto redoTmp: redoSet) {
							if (redoTmp->sequence > databaseSequence)
								isHigher = true;
							if (redoTmp->sequence == databaseSequence)
								redo = redoTmp;
						}

						if (this->shutdown)
							return 0;

						if (redo == nullptr && !isHigher) {
							cerr << "WARNING: Sleeping while waiting for new redo log sequence " << databaseSequence << endl;
							usleep(REDO_SLEEP_RETRY);
						} else
							break;

						for (auto redoTmp: redoSet)
							delete redoTmp;
						redoSet.clear();
						onlineLogGetList();
					}
					if (this->shutdown)
						return 0;
				}

				if (redo == nullptr)
					break;

				//if online redo log is overwritten - then switch to reading archive logs
				int ret = redo->processLog(this);
				if (this->shutdown)
					return 0;

				if (ret != REDO_OK) {
					if (ret == REDO_WRONG_SEQUENCE_SWITCHED)
						break;
					throw RedoLogException("read archive log", nullptr, 0);
				}
				databaseSequence = redo->sequence + 1;
				writeCheckpoint();
			}
			if (this->shutdown)
				return 0;

			//if redo is overwritten and missing in archive logs - then it means that archive log is not accessible
			archLogGetList();
			if (archiveRedoQueue.empty()) {
				cerr << "now should read: " << databaseSequence << endl;
				throw RedoLogException("archive log missing", nullptr, 0);
			}
		}

		return 0;
	}

	void OracleReader::archLogGetList() {
		checkConnection(true);

		try {
			OracleStatement stmt(&conn, env);
			stmt.createStatement("SELECT NAME, SEQUENCE#, FIRST_CHANGE#, FIRST_TIME, NEXT_CHANGE#, NEXT_TIME FROM V$ARCHIVED_LOG WHERE SEQUENCE# >= :i ORDER BY SEQUENCE#, DEST_ID");
			stmt.stmt->setInt(1, databaseSequence);
			stmt.executeQuery();

			string path;
			typescn sequence, firstScn, nextScn;

			while (stmt.rset->next()) {
				path = stmt.rset->getString(1);
				sequence = stmt.rset->getInt(2);
				firstScn = stmt.rset->getNumber(3);
				nextScn = stmt.rset->getNumber(5);

				OracleReaderRedo* redo = new OracleReaderRedo(oracleEnvironment, 0, firstScn, nextScn, sequence, path.c_str());
				archiveRedoQueue.push(redo);
			}
		} catch(SQLException &ex) {
			cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();
		}
	}

	void OracleReader::onlineLogGetList() {
		checkConnection(true);

		typescn firstScn = 0, nextScn = 0;
		int groupLast = -1, group = -1, groupPrev = -1, sequence = -1;
		struct stat fileStat;
		string status, path;

		try {
			OracleStatement stmt(&conn, env);
			stmt.createStatement("SELECT L.SEQUENCE#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, LF.GROUP#, LF.MEMBER FROM V$LOGFILE LF JOIN V$LOG L ON LF.GROUP# = L.GROUP# WHERE LF.TYPE = 'ONLINE' ORDER BY L.SEQUENCE#, LF.GROUP# ASC, LF.IS_RECOVERY_DEST_FILE DESC, LF.MEMBER ASC");
			stmt.executeQuery();

			while (stmt.rset->next()) {
				groupPrev = group;
				sequence = stmt.rset->getInt(1);
				firstScn = stmt.rset->getNumber(2);
				status = stmt.rset->getString(4);
				group = stmt.rset->getInt(5);
				path = stmt.rset->getString(6);

				if (oracleEnvironment->trace >= 1) {
					cout << "Found log: SEQ: " << sequence << ", FIRSTSCN: " << firstScn << ", STATUS: " << status <<
							", GROUP: " << group << ", PATH: " << path << endl;
				}

				if (status.compare("CURRENT") != 0)
					nextScn = stmt.rset->getNumber(3);
				else
					nextScn = ZERO_SCN;
				if (groupPrev != groupLast && group != groupPrev) {
					cerr << "can not read any member from group " << groupPrev << endl;
					throw RedoLogException("can not read any member from group", nullptr, 0);
				}

				if (group != groupLast && stat(path.c_str(), &fileStat) == 0) {
					OracleReaderRedo* redo = new OracleReaderRedo(oracleEnvironment, group, firstScn, nextScn, sequence, path.c_str());
					redoSet.insert(redo);
					groupLast = group;
				}
			}
		} catch(SQLException &ex) {
			cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();
		}

		if (group != groupLast) {
			cerr << "ERROR: can not read any member from group " << groupPrev << endl;
		}
	}

	int OracleReader::initialize() {
		checkConnection(false);
		if (conn == nullptr)
			return 0;

		typescn currentDatabaseScn;

		try {
			OracleStatement stmt(&conn, env);
			//check archivelog mode, supplemental log min
			stmt.createStatement("SELECT D.LOG_MODE, D.SUPPLEMENTAL_LOG_DATA_MIN, TP.ENDIAN_FORMAT, D.CURRENT_SCN FROM V$DATABASE D JOIN V$TRANSPORTABLE_PLATFORM TP ON TP.PLATFORM_NAME = D.PLATFORM_NAME");
			stmt.executeQuery();

			if (stmt.rset->next()) {
				string LOG_MODE = stmt.rset->getString(1);
				if (LOG_MODE.compare("ARCHIVELOG") != 0) {
					cerr << "ERROR: database not in ARCHIVELOG mode. RUN: " << endl;
					cerr << " SHUTDOWN IMMEDIATE;" << endl;
					cerr << " STARTUP MOUNT;" << endl;
					cerr << " ALTER DATABASE ARCHIVELOG;" << endl;
					cerr << " ALTER DATABASE OPEN;" << endl;
					return 0;
				}

				string SUPPLEMENTAL_LOG_MIN = stmt.rset->getString(2);
				if (SUPPLEMENTAL_LOG_MIN.compare("YES") != 0) {
					cerr << "Error: SUPPLEMENTAL_LOG_DATA_MIN missing: RUN:" << endl;
					cerr << " ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;" << endl;
					return 0;
				}

				bool bigEndian = false;
				string ENDIANNESS = stmt.rset->getString(3);
				if (ENDIANNESS.compare("Big") == 0)
					bigEndian = true;
				oracleEnvironment->initialize(bigEndian);

				currentDatabaseScn = stmt.rset->getInt(4);
			} else {
				cerr << "ERROR: reading V$DATABASE table" << endl;
				return 0;
			}
		} catch(SQLException &ex) {
			cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();
			return 0;
		}

		if (databaseSequence == 0 || databaseScn == 0) {
			try {
				OracleStatement stmt(&conn, env);
				stmt.createStatement("select SEQUENCE# from v$log where status = 'CURRENT'");
				stmt.executeQuery();

				if (stmt.rset->next()) {
					databaseSequence = stmt.rset->getNumber(1);
					databaseScn = currentDatabaseScn;
				}
			} catch(SQLException &ex) {
				cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();
			}
		}

		cout << "- sequence: " << databaseSequence << endl;
		cout << "- scn: " << databaseScn << endl;

		if (databaseSequence == 0 || databaseScn == 0)
			return 0;
		else
			return 1;
	}

	void OracleReader::addTable(string mask) {
		checkConnection(false);
		cout << "- reading table schema for: " << mask << endl;

		try {
			OracleStatement stmt(&conn, env);
			OracleStatement stmt2(&conn, env);
			stmt.createStatement(
					"SELECT tab.DATAOBJ# as objd, tab.OBJ# as objn, tab.CLUCOLS as clucols, usr.USERNAME AS owner, obj.NAME AS objectName "
					"FROM SYS.TAB$ tab, SYS.OBJ$ obj, ALL_USERS usr "
					"WHERE tab.OBJ# = obj.OBJ# "
					"AND obj.OWNER# = usr.USER_ID "
					"AND usr.USERNAME || '.' || obj.NAME LIKE :i");
			stmt.stmt->setString(1, mask);
			stmt.executeQuery();

			while (stmt.rset->next()) {
				uint32_t objd = stmt.rset->getInt(1);
				uint32_t objn = stmt.rset->getInt(2);
				uint32_t cluCols = stmt.rset->getInt(3);
				string owner = stmt.rset->getString(4);
				string objectName = stmt.rset->getString(5);
				uint32_t totalPk = 0;
				OracleObject *object = new OracleObject(objd, cluCols, owner.c_str(), objectName.c_str());

				if (oracleEnvironment->trace >= 1)
					cout << "- found: " << owner << "." << objectName << " (OBJD: " << objd << ")" << endl;

				stmt2.createStatement("SELECT C.COL#, C.SEGCOL#, C.NAME, C.TYPE#, C.LENGTH, (SELECT COUNT(*) FROM sys.ccol$ L JOIN sys.cdef$ D on D.con# = L.con# AND D.type# = 2 WHERE L.intcol# = C.intcol# and L.obj# = C.obj#) AS NUMPK FROM SYS.COL$ C WHERE C.OBJ# = :i ORDER BY C.SEGCOL#");
				stmt2.stmt->setInt(1, objn);
				stmt2.executeQuery();

				while (stmt2.rset->next()) {
					uint32_t colNo = stmt2.rset->getInt(1);
					uint32_t segColNo = stmt2.rset->getInt(2);
					string columnName = stmt2.rset->getString(3);
					uint32_t typeNo = stmt2.rset->getInt(4);
					uint32_t length = stmt2.rset->getInt(5);
					uint32_t numPk = stmt2.rset->getInt(6);
					OracleColumn *column = new OracleColumn(colNo, segColNo, columnName.c_str(), typeNo, length, numPk);
					totalPk += numPk;

					object->addColumn(column);
				}

				object->totalPk = totalPk;
				oracleEnvironment->addToDict(object);
			}
		} catch(SQLException &ex) {
			cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();
		}
	}

	void OracleReader::readCheckpoint() {
		FILE *fp = fopen((database + ".cfg").c_str(), "rb");
		if (fp == nullptr)
			return;

		uint8_t *buffer = new uint8_t[CHECKPOINT_SIZE];
		int bytes = fread(buffer, 1, CHECKPOINT_SIZE, fp);
		if (bytes == CHECKPOINT_SIZE) {
			databaseSequence = ((uint32_t)buffer[3] << 24) | ((uint32_t)buffer[2] << 16) |
					((uint32_t)buffer[1] << 8) | (uint32_t)buffer[0];
			databaseScn =
					((typescn)buffer[11] << 56) | ((typescn)buffer[10] << 48) |
					((typescn)buffer[9] << 40) | ((typescn)buffer[8] << 32) |
					((typescn)buffer[7] << 24) | ((typescn)buffer[6] << 16) |
					((typescn)buffer[5] << 8) | buffer[4];
			if (oracleEnvironment->trace >= 1) {
				cout << "Read checkpoint sequence: " << databaseSequence << endl;
				cout << "Read checkpoint scn: " << databaseScn << endl;
			}
		}

		delete[] buffer;
		fclose(fp);
	}

	void OracleReader::writeCheckpoint() {
		if (oracleEnvironment->trace >= 1)
			cout << "Writing checkpoint information" << endl;
		FILE *fp = fopen((database + ".cfg").c_str(), "wb");
		if (fp == nullptr) {
			cerr << "ERROR: Error writing checkpoint data for " << database << endl;
			return;
		}

		if (oracleEnvironment->trace >= 1) {
			cout << "write: databaseSequence: " << databaseSequence << endl;
			cout << "write: databaseScn: " << databaseScn << endl;
		}
		uint8_t *buffer = new uint8_t[CHECKPOINT_SIZE];
		buffer[0] = databaseSequence & 0xFF;
		buffer[1] = (databaseSequence >> 8) & 0xFF;
		buffer[2] = (databaseSequence >> 16) & 0xFF;
		buffer[3] = (databaseSequence >> 24) & 0xFF;

		buffer[4] = databaseScn & 0xFF;
		buffer[5] = (databaseScn >> 8) & 0xFF;
		buffer[6] = (databaseScn >> 16) & 0xFF;
		buffer[7] = (databaseScn >> 24) & 0xFF;
		buffer[8] = (databaseScn >> 32) & 0xFF;
		buffer[9] = (databaseScn >> 40) & 0xFF;
		buffer[10] = (databaseScn >> 48) & 0xFF;
		buffer[11] = (databaseScn >> 56) & 0xFF;
		int bytes = fwrite(buffer, 1, CHECKPOINT_SIZE, fp);
		if (bytes != CHECKPOINT_SIZE)
			cerr << "ERROR: writing checkpoint data for " << database << endl;

		delete[] buffer;
		fclose(fp);
	}


	bool OracleReaderRedoCompare::operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2) {
		return p1->sequence > p2->sequence;
	}

	bool OracleReaderRedoCompareReverse::operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2) {
		return p1->sequence < p2->sequence;
	}
}
