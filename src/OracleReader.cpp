/* Thread reading Oracle Redo Logs
   Copyright (C) 2018-2020 Adam Leszczynski.

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
#include <sstream>
#include <cstdio>
#include <unistd.h>
#include <string.h>
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
#include "Transaction.h"

using namespace std;
using namespace rapidjson;
using namespace oracle::occi;

const Value& getJSONfield(const Document& document, const char* field);

namespace OpenLogReplicator {

    OracleReader::OracleReader(CommandBuffer *commandBuffer, const string alias, const string database, const string user, const string passwd,
            const string connectString, uint64_t trace, uint64_t trace2, uint64_t dumpLogFile, bool dumpData, bool directRead, uint64_t sortCols,
            uint64_t forceCheckpointScn, uint64_t redoBuffers, uint64_t redoBufferSize, uint64_t maxConcurrentTransactions) :
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

        oracleEnvironment = new OracleEnvironment(commandBuffer, trace, trace2, dumpLogFile, dumpData, directRead, sortCols, forceCheckpointScn,
                redoBuffers, redoBufferSize, maxConcurrentTransactions);
        readCheckpoint();
        env = Environment::createEnvironment (Environment::DEFAULT);
    }

    OracleReader::~OracleReader() {
        while (!archiveRedoQueue.empty()) {
            OracleReaderRedo *redoTmp = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete redoTmp;
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
                    cerr << "ERROR: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
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
        onlineLogGetList();
        int ret = REDO_OK;
        OracleReaderRedo *redo = nullptr;
        bool logsProcessed;

        while (true) {
            logsProcessed = false;

            //try to read online redo logs
            if (this->shutdown)
                break;
            if ((oracleEnvironment->trace2 & TRACE2_REDO) != 0)
                cerr << "REDO: checking online redo logs" << endl;
            refreshOnlineLogs();

            while (true) {
                redo = nullptr;
                if ((oracleEnvironment->trace2 & TRACE2_REDO) != 0)
                    cerr << "REDO: searching online redo log for sequence: " << dec << databaseSequence << endl;

                //find the candidate to read
                for (auto redoTmp: onlineRedoSet) {
                    if (redoTmp->sequence == databaseSequence)
                        redo = redoTmp;
                    if ((oracleEnvironment->trace2 & TRACE2_REDO) != 0)
                        cerr << "REDO: " << redoTmp->path << " is " << dec << redoTmp->sequence << endl;
                }

                //keep reading online redo logs while it is possible
                if (redo == nullptr) {
                    bool isHigher = false;
                    while (true) {
                        for (auto redoTmp: onlineRedoSet) {
                            if (redoTmp->sequence > databaseSequence)
                                isHigher = true;
                            if (redoTmp->sequence == databaseSequence)
                                redo = redoTmp;
                        }

                        if (redo == nullptr && !isHigher) {
                            usleep(REDO_SLEEP_RETRY);
                        } else
                            break;

                        if (this->shutdown)
                            break;
                        refreshOnlineLogs();
                    }
                }

                if (redo == nullptr)
                    break;

                //if online redo log is overwritten - then switch to reading archive logs
                if (this->shutdown)
                    break;
                logsProcessed = true;
                ret = redo->processLog(this);

                if (ret != REDO_OK) {
                    if (ret == REDO_WRONG_SEQUENCE_SWITCHED) {
                        if (oracleEnvironment->trace >= TRACE_DETAIL)
                            cerr << "INFO: online redo log overwritten by new data" << endl;
                        break;
                    }
                    throw RedoLogException("read archive log", nullptr, 0);
                }
                databaseSequence = redo->sequence + 1;
                writeCheckpoint();
            }

            //try to read all archived redo logs
            if (this->shutdown)
                break;
            if ((oracleEnvironment->trace2 & TRACE2_REDO) != 0)
                cerr << "REDO: checking archive redo logs" << endl;
            archLogGetList();

            while (!archiveRedoQueue.empty()) {
                OracleReaderRedo *redoPrev = redo;
                redo = archiveRedoQueue.top();
                if ((oracleEnvironment->trace2 & TRACE2_REDO) != 0)
                    cerr << "REDO: searching archived redo log for sequence: " << dec << databaseSequence << endl;

                if (ret == REDO_WRONG_SEQUENCE_SWITCHED && redoPrev != nullptr && redoPrev->sequence == redo->sequence) {
                    if (oracleEnvironment->trace >= TRACE_WARN)
                        cerr << "INFO: continuing broken online redo log read process with archive logs" << endl;
                    redo->clone(redoPrev);
                }

                if (redo->sequence < databaseSequence)
                    continue;
                if (redo->sequence > databaseSequence) {
                    cerr << "ERROR: could not find archive log for sequence: " << dec << databaseSequence << ", found: " << redo->sequence << " instead" << endl;
                    throw RedoLogException("read archive log", nullptr, 0);
                }

                if (this->shutdown)
                    break;
                logsProcessed = true;
                ret = redo->processLog(this);

                if (ret != REDO_OK) {
                    cerr << "ERROR: archive log processing returned: " << dec << ret << endl;
                    throw RedoLogException("read archive log", nullptr, 0);
                }

                ++databaseSequence;
                writeCheckpoint();
                archiveRedoQueue.pop();
                delete redo;
                redo = nullptr;
            }

            if (this->shutdown)
                break;
            if (!logsProcessed)
                usleep(REDO_SLEEP_RETRY);
        }

        if (oracleEnvironment->trace >= TRACE_WARN && oracleEnvironment->transactionHeap.heapSize > 0) {
            cerr << "WARNING: Transactions open at shutdown: " << dec << oracleEnvironment->transactionHeap.heapSize << endl;
            for (uint64_t i = 1; i <= oracleEnvironment->transactionHeap.heapSize; ++i) {
                Transaction *transactionI = oracleEnvironment->transactionHeap.heap[i];
                cerr << "WARNING: transaction[" << i << "] XID: " << PRINTXID(transactionI->xid) <<
                        ", begin: " << transactionI->isBegin <<
                        ", commit: " << transactionI->isCommit <<
                        ", rollback: " << transactionI->isRollback << endl;
            }
        }

        return 0;
    }

    void OracleReader::archLogGetList() {
        checkConnection(true);

        try {
            OracleStatement stmt(&conn, env);
            stmt.createStatement("SELECT NAME, SEQUENCE#, FIRST_CHANGE#, FIRST_TIME, NEXT_CHANGE#, NEXT_TIME FROM SYS.V_$ARCHIVED_LOG WHERE SEQUENCE# >= :i AND RESETLOGS_ID = :i AND NAME IS NOT NULL ORDER BY SEQUENCE#, DEST_ID");
            stmt.stmt->setInt(1, databaseSequence);
            stmt.stmt->setInt(2, oracleEnvironment->resetlogsId);
            stmt.executeQuery();

            string path;
            typeseq sequence;
            typescn firstScn, nextScn;

            while (stmt.rset->next()) {
                path = stmt.rset->getString(1);
                sequence = stmt.rset->getNumber(2);
                firstScn = stmt.rset->getNumber(3);
                nextScn = stmt.rset->getNumber(5);

                OracleReaderRedo* redo = new OracleReaderRedo(oracleEnvironment, 0, path.c_str());
                redo->firstScn = firstScn;
                redo->nextScn = nextScn;
                redo->sequence = sequence;
                archiveRedoQueue.push(redo);
            }
        } catch(SQLException &ex) {
            cerr << "ERROR: getting arch log list: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
        }
    }

    void OracleReader::onlineLogGetList() {
        checkConnection(true);

        int groupLast = -1, group = -1, groupPrev = -1;
        struct stat fileStat;
        string path;

        try {
            OracleStatement stmt(&conn, env);
            stmt.createStatement("SELECT LF.GROUP#, LF.MEMBER FROM SYS.V_$LOGFILE LF ORDER BY LF.GROUP# ASC, LF.IS_RECOVERY_DEST_FILE DESC, LF.MEMBER ASC");
            stmt.executeQuery();

            while (stmt.rset->next()) {
                groupPrev = group;
                group = stmt.rset->getNumber(1);
                path = stmt.rset->getString(2);

                if (groupPrev != groupLast && group != groupPrev) {
                    throw RedoLogException("can't read any member from group", nullptr, 0);
                }

                if (group != groupLast && stat(path.c_str(), &fileStat) == 0) {
                    cerr << "Found log: GROUP: " << group << ", PATH: " << path << endl;
                    OracleReaderRedo* redo = new OracleReaderRedo(oracleEnvironment, group, path.c_str());
                    onlineRedoSet.insert(redo);
                    groupLast = group;
                }
            }
        } catch(SQLException &ex) {
            throw RedoLogException("errog getting online log list", nullptr, 0);
        }

        if (group != groupLast) {
            throw RedoLogException("can't read any member from group", nullptr, 0);
        }
    }

    void OracleReader::refreshOnlineLogs() {
        for (auto redoTmp: onlineRedoSet) {
            redoTmp->reload();
        }
    }

    int OracleReader::initialize() {
        checkConnection(false);
        if (conn == nullptr)
            return 0;

        typescn currentDatabaseScn;
        uint32_t currentResetlogsId;

        try {
            OracleStatement stmt(&conn, env);
            //check archivelog mode, supplemental log min, endian
            stmt.createStatement("SELECT D.LOG_MODE, D.SUPPLEMENTAL_LOG_DATA_MIN, TP.ENDIAN_FORMAT, D.CURRENT_SCN, DI.RESETLOGS_ID, VER.BANNER FROM SYS.V_$DATABASE D JOIN SYS.V_$TRANSPORTABLE_PLATFORM TP ON TP.PLATFORM_NAME = D.PLATFORM_NAME JOIN SYS.V_$VERSION VER ON VER.BANNER LIKE '%Oracle Database%' JOIN SYS.V_$DATABASE_INCARNATION DI ON DI.STATUS = 'CURRENT'");
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

                currentDatabaseScn = stmt.rset->getNumber(4);
                currentResetlogsId = stmt.rset->getNumber(5);
                if (oracleEnvironment->resetlogsId != 0 && currentResetlogsId != oracleEnvironment->resetlogsId) {
                    cerr << "Error: Incorrect database incarnation. Previous resetlogs id:" << dec << oracleEnvironment->resetlogsId << ", current: " << currentResetlogsId << endl;
                    return 0;
                } else {
                    oracleEnvironment->resetlogsId = currentResetlogsId;
                }

                //12+
                string VERSION = stmt.rset->getString(6);

                oracleEnvironment->conId = 0;
                if (VERSION.find("Oracle Database 11g") == string::npos) {
                    OracleStatement stmt(&conn, env);
                    stmt.createStatement("select sys_context('USERENV','CON_ID') CON_ID from DUAL");
                    stmt.executeQuery();

                    if (stmt.rset->next()) {
                        oracleEnvironment->conId = stmt.rset->getNumber(1);
                        cout << "- conId: " << dec << oracleEnvironment->conId << endl;
                    }
                }

            } else {
                cerr << "ERROR: reading SYS.V_$DATABASE" << endl;
                return 0;
            }
        } catch(SQLException &ex) {
            cerr << "ERROR: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            return 0;
        }

        if (databaseSequence == 0 || databaseScn == 0) {
            try {
                OracleStatement stmt(&conn, env);
                stmt.createStatement("select SEQUENCE# from SYS.V_$LOG where status = 'CURRENT'");
                stmt.executeQuery();

                if (stmt.rset->next()) {
                    databaseSequence = stmt.rset->getNumber(1);
                    databaseScn = currentDatabaseScn;
                }
            } catch(SQLException &ex) {
                cerr << "ERROR: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            }
        }

        cout << "- sequence: " << dec << databaseSequence << endl;
        cout << "- scn: " << dec << databaseScn << endl;
        cout << "- resetlogs id: " << dec << oracleEnvironment->resetlogsId << endl;

        if (databaseSequence == 0 || databaseScn == 0)
            return 0;
        else
            return 1;
    }

    void OracleReader::addTable(string mask, uint64_t options) {
        checkConnection(false);
        cout << "- reading table schema for: " << mask;
        uint64_t tabCnt = 0;

        try {
            OracleStatement stmt(&conn, env);
            OracleStatement stmt2(&conn, env);
            stmt.createStatement(
                    "SELECT tab.DATAOBJ# as objd, tab.OBJ# as objn, tab.CLUCOLS as clucols, usr.USERNAME AS owner, obj.NAME AS objectName, decode(bitand(tab.FLAGS, 8388608), 8388608, 1, 0) as dependencies "
                    "FROM SYS.TAB$ tab, SYS.OBJ$ obj, ALL_USERS usr "
                    "WHERE tab.OBJ# = obj.OBJ# "
                    "AND obj.OWNER# = usr.USER_ID "
                    "AND usr.USERNAME || '.' || obj.NAME LIKE :i");
            stmt.stmt->setString(1, mask);

            stmt.executeQuery();
            while (stmt.rset->next()) {
                //skip partitioned/IOT tables
                string owner = stmt.rset->getString(4);
                string objectName = stmt.rset->getString(5);
                typeobj objn = stmt.rset->getNumber(2);
                if (stmt.rset->isNull(1)) {
                    cout << endl << "  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - partitioned or IOT";
                } else {
                    typeobj objd = stmt.rset->getNumber(1);
                    uint64_t cluCols = 0;
                    if (!stmt.rset->isNull(3))
                        stmt.rset->getNumber(3);
                    uint64_t depdendencies = stmt.rset->getNumber(6);
                    uint64_t totalPk = 0, totalCols = 0;
                    OracleObject *object = new OracleObject(objn, objd, depdendencies, cluCols, options, owner.c_str(), objectName.c_str());
                    ++tabCnt;

                    cout << endl << "  * found: " << owner << "." << objectName << " (OBJD: " << dec << objd << ", OBJN: " << dec << objn << ", DEP: " << dec << depdendencies << ")";

                    stmt2.createStatement("SELECT C.COL#, C.SEGCOL#, C.NAME, C.TYPE#, C.LENGTH, (SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D on D.con# = L.con# AND D.type# = 2 WHERE L.intcol# = C.intcol# and L.obj# = C.obj#) AS NUMPK FROM SYS.COL$ C WHERE C.OBJ# = :i ORDER BY C.SEGCOL#");
                    stmt2.stmt->setInt(1, objn);
                    stmt2.executeQuery();

                    while (stmt2.rset->next()) {
                        uint32_t colNo = stmt2.rset->getNumber(1);
                        uint32_t segColNo = stmt2.rset->getNumber(2);
                        string columnName = stmt2.rset->getString(3);
                        uint32_t typeNo = stmt2.rset->getNumber(4);
                        uint32_t length = stmt2.rset->getNumber(5);
                        uint32_t numPk = stmt2.rset->getNumber(6);
                        OracleColumn *column = new OracleColumn(colNo, segColNo, columnName.c_str(), typeNo, length, numPk);
                        totalPk += numPk;
                        ++totalCols;

                        object->addColumn(column);
                    }

                    object->totalCols = totalCols;
                    object->totalPk = totalPk;
                    oracleEnvironment->addToDict(object);
                }
            }
        } catch(SQLException &ex) {
            cerr << "ERROR: getting table metadata: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
        }
        cout << " (total: " << dec << tabCnt << ")" << endl;
    }

    void OracleReader::readCheckpoint() {
        ifstream infile;
        infile.open((database + ".json").c_str(), ios::in);
        if (!infile.is_open())
            return;

        string configJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
            {cerr << "ERROR: parsing " << database << ".json at byte " << dec << document.GetErrorOffset() << endl; infile.close(); return; }

        const Value& databaseJSON = getJSONfield(document, "database");
        if (database.compare(databaseJSON.GetString()) != 0)
            {cerr << "ERROR: bad JSON, invalid database name (" << databaseJSON.GetString() << ")!" << endl; infile.close(); return; }

        const Value& databaseSequenceJSON = getJSONfield(document, "sequence");
        databaseSequence = strtoul(databaseSequenceJSON.GetString(), nullptr, 10);

        const Value& resetlogsIdJSON = getJSONfield(document, "resetlogs-id");
        oracleEnvironment->resetlogsId = strtoul(resetlogsIdJSON.GetString(), nullptr, 10);

        const Value& scnJSON = getJSONfield(document, "scn");
        databaseScn = strtoul(scnJSON.GetString(), nullptr, 10);

        infile.close();
    }

    void OracleReader::writeCheckpoint() {
        typeseq minSequence = 0xFFFFFFFF;
        typescn minScn = ZERO_SCN;
        Transaction *transaction;
        for (uint64_t i = 1; i <= oracleEnvironment->transactionHeap.heapSize; ++i) {
            transaction = oracleEnvironment->transactionHeap.heap[i];
            if (minScn > transaction->firstScn)
                minScn = transaction->firstScn;
            if (minSequence > transaction->firstSequence)
                minSequence = transaction->firstSequence;
        }
        if (minScn == ZERO_SCN)
            minScn = 0;
        if (minSequence == 0xFFFFFFFF)
            minSequence = 0;

        if (oracleEnvironment->trace >= TRACE_DETAIL)
            cerr << "INFO: Writing checkpoint information SEQ: " << dec << minSequence << "/" << databaseSequence <<
            ", SCN: " << PRINTSCN64(minScn) << "/" << PRINTSCN64(databaseScn) << endl;

        ofstream outfile;
        outfile.open((database + ".json").c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            cerr << "ERROR: writing checkpoint data for " << database << endl;
            return;
        }

        stringstream ss;
        ss << "{" << endl
           << "  \"database\": \"" << database << "\"," << endl
           << "  \"min-sequence\": \"" << dec << minSequence << "\"," << endl
           << "  \"sequence\": \"" << dec << databaseSequence << "\"," << endl
           << "  \"min-scn\": \"" << dec << minScn << "\"," << endl
           << "  \"scn\": \"" << dec << databaseScn << "\"," << endl
           << "  \"resetlogs-id\": \"" << dec << oracleEnvironment->resetlogsId << "\"" << endl
           << "}";

        outfile << ss.rdbuf();
        outfile.close();
    }


    bool OracleReaderRedoCompare::operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool OracleReaderRedoCompareReverse::operator()(OracleReaderRedo* const& p1, OracleReaderRedo* const& p2) {
        return p1->sequence < p2->sequence;
    }
}
