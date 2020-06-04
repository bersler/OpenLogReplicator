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

#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <unistd.h>
#include <rapidjson/document.h>

#include "CommandBuffer.h"
#include "ConfigurationException.h"
#include "MemoryException.h"
#include "OracleAnalyser.h"
#include "OracleAnalyserRedoLog.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OracleStatement.h"
#include "Reader.h"
#include "ReaderFilesystem.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Transaction.h"
#include "TransactionChunk.h"

using namespace std;
using namespace rapidjson;
using namespace oracle::occi;

const Value& getJSONfield(const Document& document, const char* field);

namespace OpenLogReplicator {

    string OracleAnalyser::SQL_GET_ARCHIVE_LOG_LIST("SELECT NAME, SEQUENCE#, FIRST_CHANGE#, FIRST_TIME, NEXT_CHANGE#, NEXT_TIME FROM SYS.V_$ARCHIVED_LOG WHERE SEQUENCE# >= :i AND RESETLOGS_ID = :i AND NAME IS NOT NULL ORDER BY SEQUENCE#, DEST_ID");
    string OracleAnalyser::SQL_GET_DATABASE_INFORMATION("SELECT D.LOG_MODE, D.SUPPLEMENTAL_LOG_DATA_MIN, D.SUPPLEMENTAL_LOG_DATA_PK, D.SUPPLEMENTAL_LOG_DATA_ALL, TP.ENDIAN_FORMAT, D.CURRENT_SCN, DI.RESETLOGS_ID, VER.BANNER, SYS_CONTEXT('USERENV','DB_NAME') FROM SYS.V_$DATABASE D JOIN SYS.V_$TRANSPORTABLE_PLATFORM TP ON TP.PLATFORM_NAME = D.PLATFORM_NAME JOIN SYS.V_$VERSION VER ON VER.BANNER LIKE '%Oracle Database%' JOIN SYS.V_$DATABASE_INCARNATION DI ON DI.STATUS = 'CURRENT'");
    string OracleAnalyser::SQL_GET_CON_ID("SELECT SYS_CONTEXT('USERENV','CON_ID') CON_ID FROM DUAL");
    string OracleAnalyser::SQL_GET_CURRENT_SEQUENCE("SELECT SEQUENCE# FROM SYS.V_$LOG WHERE STATUS = 'CURRENT'");
    string OracleAnalyser::SQL_GET_LOGFILE_LIST("SELECT LF.GROUP#, LF.MEMBER FROM SYS.V_$LOGFILE LF ORDER BY LF.GROUP# ASC, LF.IS_RECOVERY_DEST_FILE DESC, LF.MEMBER ASC");
    string OracleAnalyser::SQL_GET_TABLE_LIST("SELECT T.DATAOBJ#, T.OBJ#, T.CLUCOLS, U.NAME, O.NAME, DECODE(BITAND(T.PROPERTY, 1024), 0, 0, 1), DECODE((BITAND(T.PROPERTY, 512)+BITAND(T.FLAGS, 536870912)), 0, 0, 1), DECODE(BITAND(U.SPARE1, 1), 1, 1, 0), DECODE(BITAND(U.SPARE1, 8), 8, 1, 0) FROM SYS.TAB$ T, SYS.OBJ$ O, SYS.USER$ U WHERE T.OBJ# = O.OBJ# AND BITAND(O.flags, 128) = 0 AND O.OWNER# = U.USER# AND U.NAME || '.' || O.NAME LIKE :i ORDER BY 4,5");
    string OracleAnalyser::SQL_GET_COLUMN_LIST("SELECT C.COL#, C.SEGCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.NULL$, (SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#), (SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0) FROM SYS.COL$ C WHERE C.OBJ# = :i AND DECODE(BITAND(C.PROPERTY, 32), 0, 0, 1) = 0 ORDER BY C.COL#");
    string OracleAnalyser::SQL_GET_SUPPLEMNTAL_LOG_TABLE("SELECT C.TYPE# FROM SYS.CON$ OC, SYS.CDEF$ C WHERE OC.CON# = C.CON# AND (C.TYPE# = 14 OR C.TYPE# = 17) AND C.OBJ# = :i");

    OracleAnalyser::OracleAnalyser(CommandBuffer *commandBuffer, const string alias, const string database, const string user,
            const string passwd, const string connectString, uint64_t trace, uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpRawData,
            uint64_t flags, uint64_t disableChecks, uint32_t redoReadSleep, uint64_t checkpointInterval, uint64_t redoBuffers,
            uint64_t redoBufferSize, uint64_t maxConcurrentTransactions) :
        Thread(alias),
        databaseSequence(0),
        env(nullptr),
        conn(nullptr),
        user(user),
        passwd(passwd),
        connectString(connectString),
        database(database),
        archReader(nullptr),
        rolledBack1(nullptr),
        rolledBack2(nullptr),
        suppLogDbPrimary(false),
        suppLogDbAll(false),
        previousCheckpoint(clock()),
        checkpointInterval(checkpointInterval),
        databaseContext(""),
        databaseScn(0),
        lastOpTransactionMap(maxConcurrentTransactions),
        transactionHeap(maxConcurrentTransactions),
        transactionBuffer(new TransactionBuffer(redoBuffers, redoBufferSize)),
        recordBuffer(new uint8_t[REDO_RECORD_MAX_SIZE]),
        commandBuffer(commandBuffer),
        dumpRedoLog(dumpRedoLog),
        dumpRawData(dumpRawData),
        flags(flags),
        disableChecks(disableChecks),
        redoReadSleep(redoReadSleep),
        trace(trace),
        trace2(trace2),
        version(0),
        conId(0),
        resetlogs(0),
        read16(read16Little),
        read32(read32Little),
        read56(read56Little),
        read64(read64Little),
        readSCN(readSCNLittle),
        readSCNr(readSCNrLittle),
        write16(write16Little),
        write32(write32Little),
        write56(write56Little),
        write64(write64Little),
        writeSCN(writeSCNLittle) {

        readCheckpoint();
        env = Environment::createEnvironment (Environment::DEFAULT);
    }

    OracleAnalyser::~OracleAnalyser() {
        readerDropAll();
        freeRollbackList();

        while (!archiveRedoQueue.empty()) {
            OracleAnalyserRedoLog *redoTmp = archiveRedoQueue.top();
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

        delete transactionBuffer;

        for (auto it : objectMap) {
            OracleObject *object = it.second;
            delete object;
        }
        objectMap.clear();

        for (auto it : xidTransactionMap) {
            Transaction *transaction = it.second;
            delete transaction;
        }
        xidTransactionMap.clear();

        if (recordBuffer != nullptr) {
            delete[] recordBuffer;
            recordBuffer = nullptr;
        }
    }

    void OracleAnalyser::writeCheckpoint(bool atShutdown) {
        clock_t now = clock();
        typeseq minSequence = 0xFFFFFFFF;
        Transaction *transaction;

        for (uint64_t i = 1; i <= transactionHeap.heapSize; ++i) {
            transaction = transactionHeap.heap[i];
            if (minSequence > transaction->firstSequence)
                minSequence = transaction->firstSequence;
        }
        if (minSequence == 0xFFFFFFFF)
            minSequence = databaseSequence;

        if (trace >= TRACE_FULL) {
            uint64_t timeSinceCheckpoint = (now - previousCheckpoint) / CLOCKS_PER_SEC;

            if (version >= 0x12200)
                cerr << "INFO: Writing checkpoint information SEQ: " << dec << minSequence << "/" << databaseSequence <<
                " SCN: " << PRINTSCN64(databaseScn) << " after: " << dec << timeSinceCheckpoint << "s" << endl;
            else
                cerr << "INFO: Writing checkpoint information SEQ: " << dec << minSequence << "/" << databaseSequence <<
                " SCN: " << PRINTSCN48(databaseScn) << " after: " << dec << timeSinceCheckpoint << "s" << endl;
        }

        ofstream outfile;
        outfile.open((database + ".json").c_str(), ios::out | ios::trunc);

        if (!outfile.is_open())
            throw RuntimeException("writing checkpoint data");

        stringstream ss;
        ss << "{\"database\":\"" << database
                << "\",\"sequence\":" << dec << minSequence
                << ",\"scn\":" << dec << databaseScn
                << ",\"resetlogs\":" << dec << resetlogs << "}";

        outfile << ss.rdbuf();
        outfile.close();

        if (atShutdown) {
            if (trace >= TRACE_INFO) {
                cerr << "INFO: Writing checkpopint at exit for " << database << endl;
                cerr << "INFO: con_id: " << dec << conId <<
                        " sequence: " << dec << minSequence <<
                        " scn: " << dec << databaseScn <<
                        " resetlogs: " << dec << resetlogs << endl;
            }
        }

        previousCheckpoint = now;
    }

    void OracleAnalyser::readCheckpoint() {
        ifstream infile;
        infile.open((database + ".json").c_str(), ios::in);
        if (!infile.is_open())
            return;

        string configJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
            throw RuntimeException("JSON: parsing of <database>.json");

        const Value& databaseJSON = getJSONfield(document, "database");
        if (database.compare(databaseJSON.GetString()) != 0)
            throw RuntimeException("JSON: parsing of <database>.json - invalid database name");

        const Value& databaseSequenceJSON = getJSONfield(document, "sequence");
        databaseSequence = databaseSequenceJSON.GetUint64();

        const Value& resetlogsJSON = getJSONfield(document, "resetlogs");
        resetlogs = resetlogsJSON.GetUint64();

        const Value& scnJSON = getJSONfield(document, "scn");
        databaseScn = scnJSON.GetUint64();

        infile.close();
    }

    void OracleAnalyser::addToDict(OracleObject *object) {
        if (objectMap[object->objn] == nullptr) {
            objectMap[object->objn] = object;
        }
    }

    void OracleAnalyser::checkConnection(bool reconnect) {
        while (!shutdown) {
            if (conn == nullptr) {
                if (trace >= TRACE_INFO)
                    cerr << "INFO: connecting to Oracle database " << database << endl;
                try {
                    conn = env->createConnection(user, passwd, connectString);
                } catch(SQLException &ex) {
                    cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
                }
            }

            if (conn != nullptr || !reconnect)
                break;

            cerr << "ERROR: cannot connect to database, retry in 5 sec." << endl;
            sleep(5);
        }
    }

    void OracleAnalyser::archLogGetList() {
        checkConnection(true);

        try {
            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_ARCHIVE_LOG_LIST << endl;
            stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
            stmt.stmt->setInt(1, databaseSequence);
            stmt.stmt->setInt(2, resetlogs);
            stmt.executeQuery();

            string path;
            typeseq sequence;
            typescn firstScn, nextScn;

            while (stmt.rset->next()) {
                path = stmt.rset->getString(1);
                sequence = stmt.rset->getNumber(2);
                firstScn = stmt.rset->getNumber(3);
                nextScn = stmt.rset->getNumber(5);

                OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, 0, path.c_str());
                if (redo == nullptr)
                    throw MemoryException("OracleAnalyser::archLogGetList.1", sizeof(OracleAnalyserRedoLog));

                redo->firstScn = firstScn;
                redo->nextScn = nextScn;
                redo->sequence = sequence;
                archiveRedoQueue.push(redo);
            }
        } catch(SQLException &ex) {
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            throw RuntimeException("getting archive log list");
        }
    }

    void OracleAnalyser::updateOnlineLogs() {
        for (OracleAnalyserRedoLog *oracleAnalyserRedoLog : onlineRedoSet) {
            oracleAnalyserRedoLog->resetRedo();
            if (!readerUpdateRedoLog(oracleAnalyserRedoLog->reader)) {
                cerr << "ERROR: updating failed for " << dec << oracleAnalyserRedoLog->path << endl;
                throw RuntimeException("can't update file");
            }
        }
    }

    uint16_t OracleAnalyser::read16Little(const uint8_t* buf) {
        return (uint16_t)buf[0] | ((uint16_t)buf[1] << 8);
    }

    uint16_t OracleAnalyser::read16Big(const uint8_t* buf) {
        return ((uint16_t)buf[0] << 8) | (uint16_t)buf[1];
    }

    uint32_t OracleAnalyser::read32Little(const uint8_t* buf) {
        return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
                ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
    }

    uint32_t OracleAnalyser::read32Big(const uint8_t* buf) {
        return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
                ((uint32_t)buf[2] << 8) | (uint32_t)buf[3];
    }

    uint64_t OracleAnalyser::read56Little(const uint8_t* buf) {
        return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
                ((uint64_t)buf[6] << 48);
    }

    uint64_t OracleAnalyser::read56Big(const uint8_t* buf) {
        return (((uint64_t)buf[0] << 48) | ((uint64_t)buf[1] << 40) |
                ((uint64_t)buf[2] << 32) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[4] << 16) | ((uint64_t)buf[5] << 8) |
                (uint64_t)buf[6]);
    }

    uint64_t OracleAnalyser::read64Little(const uint8_t* buf) {
        return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
                ((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
    }

    uint64_t OracleAnalyser::read64Big(const uint8_t* buf) {
        return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
                ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
                ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
                ((uint64_t)buf[6] << 8) | (uint64_t)buf[7];
    }

    typescn OracleAnalyser::readSCNLittle(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[5] & 0x80) == 0x80)
            return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[6] << 32) | ((uint64_t)buf[7] << 40) |
                ((uint64_t)buf[4] << 48) | ((uint64_t)(buf[5] & 0x7F) << 56);
        else
            return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40);
    }

    typescn OracleAnalyser::readSCNBig(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[0] & 0x80) == 0x80)
            return (uint64_t)buf[5] | ((uint64_t)buf[4] << 8) |
                ((uint64_t)buf[3] << 16) | ((uint64_t)buf[2] << 24) |
                ((uint64_t)buf[7] << 32) | ((uint64_t)buf[6] << 40) |
                ((uint64_t)buf[1] << 48) | ((uint64_t)(buf[0] & 0x7F) << 56);
        else
            return (uint64_t)buf[5] | ((uint64_t)buf[4] << 8) |
                ((uint64_t)buf[3] << 16) | ((uint64_t)buf[2] << 24) |
                ((uint64_t)buf[1] << 32) | ((uint64_t)buf[0] << 40);
    }

    typescn OracleAnalyser::readSCNrLittle(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[1] & 0x80) == 0x80)
            return (uint64_t)buf[2] | ((uint64_t)buf[3] << 8) |
                ((uint64_t)buf[4] << 16) | ((uint64_t)buf[5] << 24) |
                //((uint64_t)buf[6] << 32) | ((uint64_t)buf[7] << 40) |
                ((uint64_t)buf[0] << 48) | ((uint64_t)(buf[1] & 0x7F) << 56);
        else
            return (uint64_t)buf[2] | ((uint64_t)buf[3] << 8) |
                ((uint64_t)buf[4] << 16) | ((uint64_t)buf[5] << 24) |
                ((uint64_t)buf[0] << 32) | ((uint64_t)buf[1] << 40);
    }

    typescn OracleAnalyser::readSCNrBig(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[1] & 0x80) == 0x80)
            return (uint64_t)buf[5] | ((uint64_t)buf[4] << 8) |
                ((uint64_t)buf[3] << 16) | ((uint64_t)buf[2] << 24) |
                //((uint64_t)buf[7] << 32) | ((uint64_t)buf[6] << 40) |
                ((uint64_t)buf[1] << 48) | ((uint64_t)(buf[0] & 0x7F) << 56);
        else
            return (uint64_t)buf[5] | ((uint64_t)buf[4] << 8) |
                ((uint64_t)buf[3] << 16) | ((uint64_t)buf[2] << 24) |
                ((uint64_t)buf[1] << 32) | ((uint64_t)buf[0] << 40);
    }

    void OracleAnalyser::write16Little(uint8_t* buf, uint16_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
    }

    void OracleAnalyser::write16Big(uint8_t* buf, uint16_t val) {
        buf[0] = (val >> 8) & 0xFF;
        buf[1] = val & 0xFF;
    }

    void OracleAnalyser::write32Little(uint8_t* buf, uint32_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
    }

    void OracleAnalyser::write32Big(uint8_t* buf, uint32_t val) {
        buf[0] = (val >> 24) & 0xFF;
        buf[1] = (val >> 16) & 0xFF;
        buf[2] = (val >> 8) & 0xFF;
        buf[3] = val & 0xFF;
    }

    void OracleAnalyser::write56Little(uint8_t* buf, uint64_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 32) & 0xFF;
        buf[5] = (val >> 40) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
    }

    void OracleAnalyser::write56Big(uint8_t* buf, uint64_t val) {
        buf[0] = (val >> 48) & 0xFF;
        buf[1] = (val >> 40) & 0xFF;
        buf[2] = (val >> 32) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 16) & 0xFF;
        buf[5] = (val >> 8) & 0xFF;
        buf[6] = val & 0xFF;
    }

    void OracleAnalyser::write64Little(uint8_t* buf, uint64_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 32) & 0xFF;
        buf[5] = (val >> 40) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
        buf[7] = (val >> 56) & 0xFF;
    }

    void OracleAnalyser::write64Big(uint8_t* buf, uint64_t val) {
        buf[0] = (val >> 56) & 0xFF;
        buf[1] = (val >> 48) & 0xFF;
        buf[2] = (val >> 40) & 0xFF;
        buf[3] = (val >> 32) & 0xFF;
        buf[4] = (val >> 24) & 0xFF;
        buf[5] = (val >> 16) & 0xFF;
        buf[6] = (val >> 8) & 0xFF;
        buf[7] = val & 0xFF;
    }

    void OracleAnalyser::writeSCNLittle(uint8_t* buf, typescn val) {
        if (val < 0x800000000000) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
        } else {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 48) & 0xFF;
            buf[5] = ((val >> 56) & 0xFF) | 0x80;
            buf[6] = (val >> 32) & 0xFF;
            buf[7] = (val >> 40) & 0xFF;
        }
    }

    void OracleAnalyser::writeSCNBig(uint8_t* buf, typescn val) {
        if (val < 0x800000000000) {
            buf[5] = val & 0xFF;
            buf[4] = (val >> 8) & 0xFF;
            buf[3] = (val >> 16) & 0xFF;
            buf[2] = (val >> 24) & 0xFF;
            buf[1] = (val >> 32) & 0xFF;
            buf[0] = (val >> 40) & 0xFF;
        } else {
            buf[5] = val & 0xFF;
            buf[4] = (val >> 8) & 0xFF;
            buf[3] = (val >> 16) & 0xFF;
            buf[2] = (val >> 24) & 0xFF;
            buf[1] = (val >> 48) & 0xFF;
            buf[0] = ((val >> 56) & 0xFF) | 0x80;
            buf[7] = (val >> 32) & 0xFF;
            buf[6] = (val >> 40) & 0xFF;
        }
    }

    void OracleAnalyser::initialize() {
        checkConnection(false);
        if (conn == nullptr)
            throw RuntimeException("connecting to the database");

        typescn currentDatabaseScn;
        typeresetlogs currentResetlogs;

        try {
            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_DATABASE_INFORMATION << endl;
            stmt.createStatement(SQL_GET_DATABASE_INFORMATION);
            stmt.executeQuery();

            if (stmt.rset->next()) {
                string LOG_MODE = stmt.rset->getString(1);
                if (LOG_MODE.compare("ARCHIVELOG") != 0) {
                    cerr << "run: " << endl;
                    cerr << " SHUTDOWN IMMEDIATE;" << endl;
                    cerr << " STARTUP MOUNT;" << endl;
                    cerr << " ALTER DATABASE ARCHIVELOG;" << endl;
                    cerr << " ALTER DATABASE OPEN;" << endl;
                    throw RuntimeException("database not in ARCHIVELOG mode");
                }

                string SUPPLEMENTAL_LOG_MIN = stmt.rset->getString(2);
                if (SUPPLEMENTAL_LOG_MIN.compare("YES") != 0) {
                    cerr << "run:" << endl;
                    cerr << " ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;" << endl;
                    cerr << " ALTER SYSTEM ARCHIVE LOG CURRENT;" << endl;
                    throw RuntimeException("SUPPLEMENTAL_LOG_DATA_MIN missing");
                }

                string SUPPLEMENTAL_LOG_PK = stmt.rset->getString(3);
                if (SUPPLEMENTAL_LOG_PK.compare("YES") == 0)
                    suppLogDbPrimary = true;

                string SUPPLEMENTAL_LOG_ALL = stmt.rset->getString(4);
                if (SUPPLEMENTAL_LOG_ALL.compare("YES") == 0)
                    suppLogDbAll = true;

                string ENDIANNESS = stmt.rset->getString(5);
                if (ENDIANNESS.compare("Big") == 0) {
                    read16 = read16Big;
                    read32 = read32Big;
                    read56 = read56Big;
                    read64 = read64Big;
                    readSCN = readSCNBig;
                    readSCNr = readSCNrBig;
                    write16 = write16Big;
                    write32 = write32Big;
                    write56 = write56Big;
                    write64 = write64Big;
                    writeSCN = writeSCNBig;
                }

                currentDatabaseScn = stmt.rset->getNumber(6);
                currentResetlogs = stmt.rset->getNumber(7);
                if (resetlogs != 0 && currentResetlogs != resetlogs) {
                    cerr << "ERROR: Previous resetlogs:" << dec << resetlogs << ", current: " << currentResetlogs << endl;
                    throw RuntimeException("incorrect database incarnation");
                } else {
                    resetlogs = currentResetlogs;
                }

                //12+
                string VERSION = stmt.rset->getString(8);
                if (trace >= TRACE_INFO)
                    cerr << "INFO: version: " << dec << VERSION << endl;

                conId = 0;
                if (VERSION.find("Oracle Database 11g") == string::npos) {
                    OracleStatement stmt(&conn, env);
                    if ((trace2 & TRACE2_SQL) != 0)
                        cerr << "SQL: " << SQL_GET_CON_ID << endl;
                    stmt.createStatement(SQL_GET_CON_ID);
                    stmt.executeQuery();

                    if (stmt.rset->next())
                        conId = stmt.rset->getNumber(1);
                }

                databaseContext = stmt.rset->getString(9);
            } else {
                throw RuntimeException("reading SYS.V_$DATABASE");
            }

        } catch(SQLException &ex) {
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            throw RuntimeException("getting information about log files");
        }

        if ((disableChecks & DISABLE_CHECK_GRANTS) == 0) {
            checkTableForGrants("SYS.TAB$");
            checkTableForGrants("SYS.OBJ$");
            checkTableForGrants("SYS.COL$");
            checkTableForGrants("SYS.CON$");
            checkTableForGrants("SYS.CCOL$");
            checkTableForGrants("SYS.CDEF$");
            checkTableForGrants("SYS.USER$");
            checkTableForGrants("SYS.V_$ARCHIVED_LOG");
            checkTableForGrants("SYS.V_$LOGFILE");
            checkTableForGrants("SYS.V_$LOG");
            checkTableForGrants("SYS.V_$DATABASE");
            checkTableForGrants("SYS.V_$DATABASE_INCARNATION");
            checkTableForGrants("SYS.V_$TRANSPORTABLE_PLATFORM");
        }

        if (databaseSequence == 0 || databaseScn == 0) {
            try {
                OracleStatement stmt(&conn, env);
                if ((trace2 & TRACE2_SQL) != 0)
                    cerr << "SQL: " << SQL_GET_CURRENT_SEQUENCE << endl;
                stmt.createStatement(SQL_GET_CURRENT_SEQUENCE);
                stmt.executeQuery();

                if (stmt.rset->next()) {
                databaseSequence = stmt.rset->getNumber(1);
                    databaseScn = currentDatabaseScn;
                }
            } catch(SQLException &ex) {
                cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
                throw RuntimeException("getting current log sequence");
            }
        }

        if (trace >= TRACE_INFO) {
            cerr << "INFO: con_id: " << dec << conId <<
                    " sequence: " << dec << databaseSequence <<
                    " scn: " << dec << databaseScn <<
                    " resetlogs: " << dec << resetlogs << endl;
        }

        if (databaseSequence == 0 || databaseScn == 0)
            throw RuntimeException("getting database sequence or current SCN");

        Reader *onlineReader = nullptr;
        int64_t group = -1, groupNew = -1, groupLastOk = -1;
        string path;

        try {
            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_LOGFILE_LIST << endl;
            stmt.createStatement(SQL_GET_LOGFILE_LIST);
            stmt.executeQuery();

            while (stmt.rset->next()) {
                groupNew = stmt.rset->getNumber(1);
                path = stmt.rset->getString(2);

                //new group
                if (groupNew != group) {
                    if (group != groupLastOk || onlineReader != nullptr) {
                        readerDropAll();
                        cerr << "ERROR: can't read any member of group " << dec << group << " - set \"trace2\": " << dec << TRACE2_FILE << " to check which files are read" << endl;
                        throw RuntimeException("can't read any member of group");
                    }

                    group = groupNew;
                    onlineReader = readerCreate(group);
                }

                if (group > groupLastOk && readerCheckRedoLog(onlineReader, path)) {
                    OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, group, path.c_str());
                    if (redo == nullptr) {
                        readerDropAll();
                        throw MemoryException("OracleAnalyser::initialize.1", sizeof(OracleAnalyserRedoLog));
                    }

                    redo->reader = onlineReader;
                    onlineRedoSet.insert(redo);
                    groupLastOk = group;
                    onlineReader = nullptr;
                }
            }

            if (group != groupLastOk) {
                readerDropAll();
                cerr << "ERROR: can't read any member of group " << dec << group << " - set \"trace2\": 2 to check which files are read" << endl;
                throw RuntimeException("can't read any member of group");
            }
        } catch(SQLException &ex) {
            readerDropAll();
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            throw RuntimeException("getting information about log files");
        }

        archReader = readerCreate(0);
    }

    void *OracleAnalyser::run(void) {
        cout << "Starting thread: Oracle Analyser for: " << database << endl;
        checkConnection(true);

        uint64_t ret = REDO_OK;
        OracleAnalyserRedoLog *redo = nullptr;
        bool logsProcessed;

        while (!shutdown) {
            logsProcessed = false;

            //read from line redo log
            if ((trace2 & TRACE2_REDO) != 0)
                cerr << "REDO: checking online redo logs" << endl;
            updateOnlineLogs();

            while (!shutdown) {
                redo = nullptr;
                if ((trace2 & TRACE2_REDO) != 0)
                    cerr << "REDO: searching online redo log for sequence: " << dec << databaseSequence << endl;

                //find the candidate to read
                for (OracleAnalyserRedoLog *oracleAnalyserRedoLog : onlineRedoSet) {
                    if (oracleAnalyserRedoLog->sequence == databaseSequence)
                        redo = oracleAnalyserRedoLog;
                    if ((trace2 & TRACE2_REDO) != 0)
                        cerr << "REDO: " << oracleAnalyserRedoLog->path << " is " << dec << oracleAnalyserRedoLog->sequence << endl;
                }

                //keep reading online redo logs while it is possible
                if (redo == nullptr) {
                    bool isHigher = false;
                    while (!shutdown) {
                        for (OracleAnalyserRedoLog *redoTmp : onlineRedoSet) {
                            if (redoTmp->reader->sequence > databaseSequence)
                                isHigher = true;
                            if (redoTmp->reader->sequence == databaseSequence) {
                                redo = redoTmp;
                                redo->sequence = redoTmp->reader->sequence;
                                redo->firstScn = redoTmp->reader->firstScn;
                                redo->nextScn = redoTmp->reader->nextScn;
                            }
                        }

                        //all so far read, waiting for switch
                        if (redo == nullptr && !isHigher) {
                            usleep(redoReadSleep);
                        } else
                            break;

                        if (shutdown)
                            break;

                        updateOnlineLogs();
                    }
                }

                if (redo == nullptr) {
                    //cout << "- Oracle Analyser for: " << database << " - none found" << endl;
                    break;
                }

                //if online redo log is overwritten - then switch to reading archive logs
                if (shutdown)
                    break;
                logsProcessed = true;
                ret = redo->processLog();

                if (shutdown)
                    break;

                if (ret != REDO_FINISHED) {
                    if (ret == REDO_OVERWRITTEN) {
                        if (trace >= TRACE_INFO)
                            cerr << "INFO: online redo log overwritten by new data, will continue from archived redo log" << endl;
                        break;
                    }
                    throw RuntimeException("read archive log");
                }

                if (rolledBack1 != nullptr)
                    freeRollbackList();

                ++databaseSequence;
                writeCheckpoint(false);
            }

            //try to read all archived redo logs
            if (shutdown)
                break;
            if ((trace2 & TRACE2_REDO) != 0)
                cerr << "REDO: checking archive redo logs" << endl;
            archLogGetList();

            while (!archiveRedoQueue.empty() && !shutdown) {
                OracleAnalyserRedoLog *redoPrev = redo;
                redo = archiveRedoQueue.top();
                if ((trace2 & TRACE2_REDO) != 0)
                    cerr << "REDO: searching archived redo log for sequence: " << dec << databaseSequence << endl;

                if (redo->sequence < databaseSequence)
                    continue;
                else if (redo->sequence > databaseSequence) {
                    cerr << "ERROR: could not find archive log for sequence: " << dec << databaseSequence << ", found: " << redo->sequence << " instead" << endl;
                    throw RuntimeException("getting archive log list");
                }

                logsProcessed = true;
                redo->reader = archReader;

                if (!readerCheckRedoLog(archReader, redo->path)) {
                    cerr << "ERROR: while opening archive log: " << redo->path << endl;
                    throw RuntimeException("open archive log file");
                }

                if (!readerUpdateRedoLog(archReader)) {
                    cerr << "ERROR: while reading archive log: " << redo->path << endl;
                    throw RuntimeException("read archive log file");
                }

                if (ret == REDO_OVERWRITTEN && redoPrev != nullptr && redoPrev->sequence == redo->sequence) {
                    redo->continueRedo(redoPrev);
                } else {
                    redo->resetRedo();
                }

                ret = redo->processLog();

                if (shutdown)
                    break;

                if (ret != REDO_FINISHED) {
                    cerr << "ERROR: archive log processing returned: " << dec << ret << endl;
                    throw RuntimeException("read archive log file");
                }

                ++databaseSequence;
                writeCheckpoint(false);
                archiveRedoQueue.pop();
                delete redo;
                redo = nullptr;
            }

            if (shutdown)
                break;
            if (!logsProcessed)
                usleep(redoReadSleep);
        }

        if (trace >= TRACE_INFO)
            cerr << "INFO: Oracle Analyser for: " << database << " shutting down" << endl;

        writeCheckpoint(true);
        dumpTransactions();
        readerDropAll();

        if (trace >= TRACE_INFO)
            cerr << "INFO: Oracle Analyser for: " << database << " is shut down" << endl;
        return 0;
    }

    void OracleAnalyser::freeRollbackList() {
        RedoLogRecord *tmpRedoLogRecord1, *tmpRedoLogRecord2;
        uint64_t lostElements = 0;

        while (rolledBack1 != nullptr) {
            if (trace >= TRACE_WARN)
                cerr << "WARNING: element on rollback list UBA: " << PRINTUBA(rolledBack1->uba) <<
                        " DBA: 0x" << hex << rolledBack2->dba <<
                        " SLT: " << dec << (uint64_t)rolledBack2->slt <<
                        " RCI: " << dec << (uint64_t)rolledBack2->rci <<
                        " OPFLAGS: " << hex << rolledBack2->opFlags << endl;

            tmpRedoLogRecord1 = rolledBack1;
            tmpRedoLogRecord2 = rolledBack2;
            rolledBack1 = rolledBack1->next;
            rolledBack2 = rolledBack2->next;
            delete tmpRedoLogRecord1;
            delete tmpRedoLogRecord2;
            ++lostElements;
        }
    }

    bool OracleAnalyser::onRollbackList(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        RedoLogRecord *tmpRedoLogRecord1, *tmpRedoLogRecord2;

        tmpRedoLogRecord1 = rolledBack1;
        tmpRedoLogRecord2 = rolledBack2;
        while (tmpRedoLogRecord1 != nullptr) {
            if ((trace2 & TRACE2_UBA) != 0)
                cerr << "WARNING: checking element on rollback list:" <<
                        " UBA: " << PRINTUBA(tmpRedoLogRecord1->uba) <<
                        " DBA: 0x" << hex << tmpRedoLogRecord2->dba <<
                        " SLT: " << dec << (uint64_t)tmpRedoLogRecord2->slt <<
                        " RCI: " << dec << (uint64_t)tmpRedoLogRecord2->rci <<
                        " OPFLAGS: " << hex << tmpRedoLogRecord2->opFlags <<
                        " for: " <<
                        " UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                        " DBA: 0x" << hex << redoLogRecord1->dba <<
                        " SLT: " << dec << (uint64_t)redoLogRecord1->slt <<
                        " RCI: " << dec << (uint64_t)redoLogRecord1->rci <<
                        " OPFLAGS: " << hex << redoLogRecord1->opFlags <<
                        endl;

            if (tmpRedoLogRecord2->slt == redoLogRecord1->slt &&
                    tmpRedoLogRecord2->rci == redoLogRecord1->rci &&
                    tmpRedoLogRecord1->uba == redoLogRecord1->uba &&
                    ((tmpRedoLogRecord2->opFlags & OPFLAG_BEGIN_TRANS) != 0 || tmpRedoLogRecord2->dba == redoLogRecord1->dba)) {

                if (tmpRedoLogRecord1->next != nullptr) {
                    tmpRedoLogRecord1->next->prev = tmpRedoLogRecord1->prev;
                    tmpRedoLogRecord2->next->prev = tmpRedoLogRecord2->prev;
                }

                if (tmpRedoLogRecord1->prev == nullptr) {
                    rolledBack1 = tmpRedoLogRecord1->next;
                    rolledBack2 = tmpRedoLogRecord2->next;
                } else {
                    tmpRedoLogRecord1->prev->next = tmpRedoLogRecord1->next;
                    tmpRedoLogRecord2->prev->next = tmpRedoLogRecord2->next;
                }

                delete tmpRedoLogRecord1;
                delete tmpRedoLogRecord2;
                return true;
            }

            tmpRedoLogRecord1 = tmpRedoLogRecord1->next;
            tmpRedoLogRecord2 = tmpRedoLogRecord2->next;
        }
        return false;
    }

    void OracleAnalyser::addToRollbackList(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        RedoLogRecord *tmpRedoLogRecord1 = new RedoLogRecord();
        if (tmpRedoLogRecord1 == nullptr)
            throw MemoryException("OracleAnalyser::addToRollbackList.1", sizeof(RedoLogRecord));

        RedoLogRecord *tmpRedoLogRecord2 = new RedoLogRecord();
        if (tmpRedoLogRecord2 == nullptr)
            throw MemoryException("OracleAnalyser::addToRollbackList.2", sizeof(RedoLogRecord));

        memcpy(tmpRedoLogRecord1, redoLogRecord1, sizeof(RedoLogRecord));
        memcpy(tmpRedoLogRecord2, redoLogRecord2, sizeof(RedoLogRecord));

        tmpRedoLogRecord1->next = rolledBack1;
        tmpRedoLogRecord2->next = rolledBack2;

        if (rolledBack1 != nullptr) {
            rolledBack1->prev = tmpRedoLogRecord1;
            rolledBack2->prev = tmpRedoLogRecord2;
        }
        rolledBack1 = tmpRedoLogRecord1;
        rolledBack2 = tmpRedoLogRecord2;
    }

    OracleObject *OracleAnalyser::checkDict(typeobj objn, typeobj objd) {
        OracleObject *object = objectMap[objn];
        return object;
    }

    bool OracleAnalyser::readerCheckRedoLog(Reader *reader, string &path) {
        unique_lock<mutex> lck(mtx);
        reader->status = READER_STATUS_CHECK;
        reader->sequence = 0;
        reader->firstScn = ZERO_SCN;
        reader->nextScn = ZERO_SCN;
        reader->updatePath(path);
        readerCond.notify_all();
        sleepingCond.notify_all();
        while (!shutdown) {
            if (reader->status == READER_STATUS_CHECK)
                analyserCond.wait(lck);
            else
                break;
        }
        if (reader->ret == REDO_OK)
            return true;
        else
            return false;
    }

    void OracleAnalyser::readerDropAll() {
        {
            unique_lock<mutex> lck(mtx);
            for (Reader *reader : readers)
                reader->shutdown = true;
            readerCond.notify_all();
            sleepingCond.notify_all();
        }
        for (Reader *reader : readers)
            pthread_join(reader->pthread, nullptr);
        archReader = nullptr;
        readers.clear();
    }

    void OracleAnalyser::checkTableForGrants(string tableName) {
        try {
            stringstream query;
            query << "SELECT 1 FROM " << tableName << " WHERE 0 = 1";

            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << query.str() << endl;
            stmt.createStatement(query.str());
            stmt.executeQuery();
        } catch(SQLException &ex) {
            cerr << "ERROR: run: GRANT SELECT ON " << tableName << " TO " << user << ";" << endl;
            throw ConfigurationException("grants missing");
        }
    }

    Reader *OracleAnalyser::readerCreate(int64_t group) {
        Reader *reader = new ReaderFilesystem(alias, this, group);
        if (reader == nullptr)
            throw MemoryException("OracleAnalyser::readerCreate.1", sizeof(ReaderFilesystem));

        readers.insert(reader);
        if (pthread_create(&reader->pthread, nullptr, &ReaderFilesystem::runStatic, (void*)reader))
            throw ConfigurationException("spawning thread");
        return reader;
    }

    void OracleAnalyser::dumpTransactions() {
        if (trace >= TRACE_INFO) {
            if (transactionHeap.heapSize > 0)
                cerr << "INFO: Transactions open: " << dec << transactionHeap.heapSize << endl;
            for (uint64_t i = 1; i <= transactionHeap.heapSize; ++i)
                cerr << "INFO: transaction[" << i << "]: " << *transactionHeap.heap[i] << endl;
        }
    }

    void OracleAnalyser::addTable(string mask, vector<string> &keys, string &keysStr, uint64_t options) {
        checkConnection(false);
        cout << "- reading table schema for: " << mask << endl;
        uint64_t tabCnt = 0;
        OracleObject *object = nullptr;

        try {
            OracleStatement stmt(&conn, env);
            OracleStatement stmt2(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_TABLE_LIST << endl;
            stmt.createStatement(SQL_GET_TABLE_LIST);
            stmt.stmt->setString(1, mask);

            stmt.executeQuery();
            while (stmt.rset->next()) {
                typeobj objn = stmt.rset->getNumber(2);
                string owner = stmt.rset->getString(4);
                string objectName = stmt.rset->getString(5);
                bool clustered = (((int)stmt.rset->getNumber(6)) != 0);
                bool iot = (((int)stmt.rset->getNumber(7)) != 0);
                bool suppLogSchemaPrimary = (((int)stmt.rset->getNumber(8)) != 0);
                bool suppLogSchemaAll = (((int)stmt.rset->getNumber(9)) != 0);

                //skip Index Organized Tables (IOT)
                if (iot) {
                    cout << "  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - IOT" << endl;
                    continue;
                }

                //skip partitioned tables
                if (stmt.rset->isNull(1)) {
                    cout << "  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - partitioned" << endl;
                    continue;
                }
                typeobj objd = stmt.rset->getNumber(1);

                //table already added with another rule
                if (checkDict(objn, objd) != nullptr) {
                    cout << "  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - already added" << endl;
                    continue;
                }

                uint64_t cluCols = 0, totalPk = 0, totalCols = 0, keysCnt = 0;
                boolean suppLogTablePrimary = false, suppLogTableAll = false, supLogColMissing = false;
                if (!stmt.rset->isNull(3))
                    stmt.rset->getNumber(3);

                OracleObject *object = new OracleObject(objn, objd, cluCols, options, owner, objectName);
                if (object == nullptr)
                    throw MemoryException("OracleAnalyser::addTable.1", sizeof(OracleObject));
                ++tabCnt;

                if ((disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0 && !suppLogDbAll && !suppLogSchemaAll && !suppLogSchemaAll) {
                    if ((trace2 & TRACE2_SQL) != 0)
                        cerr << "SQL: " << SQL_GET_SUPPLEMNTAL_LOG_TABLE << endl;
                    stmt2.createStatement(SQL_GET_SUPPLEMNTAL_LOG_TABLE);
                    stmt2.stmt->setInt(1, objn);
                    stmt2.executeQuery();

                    while (stmt2.rset->next()) {
                        uint64_t typeNo = stmt2.rset->getNumber(1);
                        if (typeNo == 14) suppLogTablePrimary = true;
                        else if (typeNo == 17) suppLogTableAll = true;
                    }
                }

                if ((trace2 & TRACE2_SQL) != 0)
                    cerr << "SQL: " << SQL_GET_COLUMN_LIST << endl;
                stmt2.createStatement(SQL_GET_COLUMN_LIST);
                stmt2.stmt->setInt(1, objn);
                stmt2.executeQuery();

                while (stmt2.rset->next()) {
                    uint64_t colNo = stmt2.rset->getNumber(1);
                    uint64_t segColNo = stmt2.rset->getNumber(2);
                    string columnName = stmt2.rset->getString(3);
                    uint64_t typeNo = stmt2.rset->getNumber(4);
                    uint64_t length = stmt2.rset->getNumber(5);
                    int64_t precision = -1;
                    if (!stmt2.rset->isNull(6))
                        precision = stmt2.rset->getNumber(6);
                    int64_t scale = -1;
                    if (!stmt2.rset->isNull(7))
                        scale = stmt2.rset->getNumber(7);

                    int64_t nullable = stmt2.rset->getNumber(8);
                    uint64_t numPk = stmt2.rset->getNumber(9);
                    uint64_t numSup = stmt2.rset->getNumber(10);

                    //column part of defined primary key
                    if (keys.size() > 0) {
                        numPk = 0;
                        for (vector<string>::iterator it = keys.begin(); it != keys.end(); ++it) {
                            if (columnName.compare(it->c_str()) == 0) {
                                numPk = 1;
                                ++keysCnt;
                                if (numSup == 0)
                                    supLogColMissing = true;
                                break;
                            }
                        }
                    } else {
                        if (numPk > 0 && numSup == 0)
                            supLogColMissing = true;
                    }

                    if (trace >= TRACE_FULL)
                        cout << "    - col: " << dec << colNo << ": " << columnName << " (pk: " << dec << numPk << ")" << endl;

                    OracleColumn *column = new OracleColumn(colNo, segColNo, columnName, typeNo, length, precision, scale, numPk, (nullable == 0));
                    if (column == nullptr)
                        throw MemoryException("OracleAnalyser::addTable.2", sizeof(OracleColumn));

                    totalPk += numPk;
                    ++totalCols;

                    object->addColumn(column);
                }

                //check if table has all listed columns
                if (keys.size() != keysCnt) {
                    delete object;
                    object = nullptr;
                    cerr << "ERROR: table " << owner << "." << objectName << " could not find all column set (" << keysStr << ")" << endl;
                    throw ConfigurationException("column not found");
                }

                cout << "  * found: " << owner << "." << objectName << " (OBJD: " << dec << objd << ", OBJN: " << dec << objn << ")";
                if (clustered)
                    cout << " part of cluster";

                if ((disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0) {
                    //use default primary key
                    if (keys.size() == 0) {
                        if (totalPk == 0)
                            cout << " - primary key missing" << endl;
                        else if (!suppLogTablePrimary && !suppLogTableAll &&
                                !suppLogSchemaPrimary && !suppLogSchemaAll &&
                                !suppLogDbPrimary && !suppLogDbAll && supLogColMissing)
                            cout << " - supplemental log missing, try: ALTER TABLE " << owner << "." << objectName << " ADD SUPPLEMENTAL LOG GROUP DATA (PRIMARY KEY) COLUMNS;" << endl;
                        else
                            cout << endl;
                    //user defined primary key
                    } else {
                        if (!suppLogTableAll && !suppLogSchemaAll && !suppLogDbAll && supLogColMissing)
                            cout << " - supplemental log missing, try: ALTER TABLE " << owner << "." << objectName << " ADD SUPPLEMENTAL LOG GROUP GRP" << dec << objn << " (" << keysStr << ") ALWAYS;" << endl;
                        else
                            cout << endl;
                    }
                } else
                    cout << endl;

                object->totalCols = totalCols;
                object->totalPk = totalPk;
                addToDict(object);
                object = nullptr;
            }
        } catch(SQLException &ex) {
            if (object != nullptr) {
                delete object;
                object = nullptr;
            }
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            throw RuntimeException("getting table metadata");
        }
        cout << "  * total: " << dec << tabCnt << " tables" << endl;
    }

    void OracleAnalyser::checkForCheckpoint() {
        uint64_t timeSinceCheckpoint = (clock() - previousCheckpoint) / CLOCKS_PER_SEC;
        if (timeSinceCheckpoint > checkpointInterval) {
            if (trace >= TRACE_FULL) {
                cerr << "INFO: Time since last checkpoint: " << dec << timeSinceCheckpoint << "s, forcing checkpoint" << endl;
            }
            writeCheckpoint(false);
        } else {
            if (trace >= TRACE_FULL) {
                cerr << "INFO: Time since last checkpoint: " << dec << timeSinceCheckpoint << "s" << endl;
            }
        }
    }

    bool OracleAnalyser::readerUpdateRedoLog(Reader *reader) {
        unique_lock<mutex> lck(mtx);
        reader->status = READER_STATUS_UPDATE;
        readerCond.notify_all();
        sleepingCond.notify_all();
        while (!shutdown) {
            if (reader->status == READER_STATUS_UPDATE)
                analyserCond.wait(lck);
            else
                break;
        }

        if (reader->ret == REDO_OK)
            return true;
        else
            return false;
    }

    void OracleAnalyser::stop(void) {
        shutdown = true;
        {
            unique_lock<mutex> lck(mtx);
            readerCond.notify_all();
            sleepingCond.notify_all();
            analyserCond.notify_all();
        }
    }

    void OracleAnalyser::addPathMapping(const string source, const string target) {
        if ((trace2 & TRACE2_FILE) != 0)
            cerr << "FILE: added mapping [" << source << "] -> [" << target << "]" << endl;
        string sourceMaping = source, targetMapping = target;
        pathMapping.push_back(sourceMaping);
        pathMapping.push_back(targetMapping);
    }

    void OracleAnalyser::nextField(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        ++fieldNum;
        if (fieldNum > redoLogRecord->fieldCnt) {
            cerr << "ERROR: field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", op: " << hex << redoLogRecord->opCode <<
                    ", cc: " << dec << redoLogRecord->cc <<
                    ", suppCC: " << dec << redoLogRecord->suppLogCC << endl;
            throw RedoLogException("field missing in vector");
        }

        if (fieldNum == 1)
            fieldPos = redoLogRecord->fieldPos;
        else
            fieldPos += (fieldLength + 3) & 0xFFFC;
        fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + fieldNum * 2);

        if (fieldPos + fieldLength > redoLogRecord->length) {
            cerr << "ERROR: field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt << endl;
            cerr << "ERROR: pos: " << dec << fieldPos << ", length:" << fieldLength << " max: " << redoLogRecord->length << endl;
            throw RedoLogException("field length out of vector");
        }
    }

    bool OracleAnalyser::hasNextField(RedoLogRecord *redoLogRecord, uint64_t &fieldNum) {
        return fieldNum < redoLogRecord->fieldCnt;
    }

    bool OracleAnalyserRedoLogCompare::operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool OracleAnalyserRedoLogCompareReverse::operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2) {
        return p1->sequence < p2->sequence;
    }
}
