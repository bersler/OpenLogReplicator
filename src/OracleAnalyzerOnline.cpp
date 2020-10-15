/* Thread reading Oracle Redo Logs using online mode
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <unistd.h>

#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "OracleAnalyzerOnline.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBuffer.h"
#include "Reader.h"
#include "RedoLog.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SchemaElement.h"

using namespace std;

namespace OpenLogReplicator {

    const char* OracleAnalyzerOnline::SQL_GET_ARCHIVE_LOG_LIST(
            "SELECT"
            "   NAME"
            ",  SEQUENCE#"
            ",  FIRST_CHANGE#"
            "   NEXT_CHANGE#"
            " FROM"
            "   SYS.V_$ARCHIVED_LOG"
            " WHERE"
            "   SEQUENCE# >= :i"
            "   AND RESETLOGS_ID = :j"
            "   AND ACTIVATION# = :k"
            "   AND NAME IS NOT NULL"
            " ORDER BY"
            "   SEQUENCE#"
            ",  DEST_ID");

    const char* OracleAnalyzerOnline::SQL_GET_DATABASE_INFORMATION(
            "SELECT"
            "   DECODE(D.LOG_MODE, 'ARCHIVELOG', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_MIN, 'YES', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_PK, 'YES', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_ALL, 'YES', 1, 0)"
            ",  DECODE(TP.ENDIAN_FORMAT, 'Big', 1, 0)"
            ",  D.CURRENT_SCN"
            ",  DI.RESETLOGS_ID"
            ",  D.ACTIVATION#"
            ",  VER.BANNER"
            ",  SYS_CONTEXT('USERENV','DB_NAME')"
            " FROM"
            "   SYS.V_$DATABASE D"
            " JOIN"
            "   SYS.V_$TRANSPORTABLE_PLATFORM TP ON"
            "     TP.PLATFORM_NAME = D.PLATFORM_NAME"
            " JOIN"
            "   SYS.V_$VERSION VER ON"
            "     VER.BANNER LIKE '%Oracle Database%'"
            " JOIN"
            "   SYS.V_$DATABASE_INCARNATION DI ON"
            "     DI.STATUS = 'CURRENT'");

    const char* OracleAnalyzerOnline::SQL_GET_CON_INFO(
            "SELECT"
            "   SYS_CONTEXT('USERENV','CON_ID')"
            ",  SYS_CONTEXT('USERENV','CON_NAME')"
            " FROM"
            "   DUAL");

    const char* OracleAnalyzerOnline::SQL_GET_SCN_FROM_SEQUENCE(
            "SELECT"
            "   FIRST_CHANGE#"
            " FROM"
            "   SYS.V_$ARCHIVED_LOG"
            " WHERE"
            "   SEQUENCE# = :i"
            "   AND RESETLOGS_ID = :j"
            "   AND ACTIVATION# = :k"
            " UNION ALL "
            "SELECT"
            "   FIRST_CHANGE#"
            " FROM"
            "   SYS.V_$LOG"
            " WHERE"
            "   SEQUENCE# = :i");

    const char* OracleAnalyzerOnline::SQL_GET_SCN_FROM_SEQUENCE_STANDBY(
            "SELECT"
            "   FIRST_CHANGE#"
            " FROM"
            "   SYS.V_$ARCHIVED_LOG"
            " WHERE"
            "   SEQUENCE# = :i"
            "   AND RESETLOGS_ID = :j"
            "   AND ACTIVATION# = :k"
            " UNION ALL "
            "SELECT"
            "   FIRST_CHANGE#"
            " FROM"
            "   SYS.V_$STANDBY_LOG"
            " WHERE"
            "   SEQUENCE# = :i");

    const char* OracleAnalyzerOnline::SQL_GET_SCN_FROM_TIME(
            "SELECT TIMESTAMP_TO_SCN(TO_DATE('YYYY-MM-DD HH24:MI:SS', :i) FROM DUAL");

    const char* OracleAnalyzerOnline::SQL_GET_SCN_FROM_TIME_RELATIVE(
            "SELECT TIMESTAMP_TO_SCN(SYSDATE - (:i/24/3600)) FROM DUAL");

    const char* OracleAnalyzerOnline::SQL_GET_SEQUENCE_FROM_SCN(
            "SELECT MAX(SEQUENCE#) FROM ("
            "  SELECT"
            "     SEQUENCE#"
            "   FROM"
            "     SYS.V_$LOG"
            "   WHERE"
            "     FIRST_CHANGE# <= :i"
            "   UNION "
            "  SELECT"
            "     SEQUENCE#"
            "   FROM"
            "     SYS.V_$ARCHIVED_LOG"
            "   WHERE"
            "     FIRST_CHANGE# <= :i)");

    const char* OracleAnalyzerOnline::SQL_GET_SEQUENCE_FROM_SCN_STANDBY(
            "SELECT"
            "   MAX(SEQUENCE#)"
            " FROM"
            "   SYS.V_$STANDBY_LOG"
            " WHERE"
            "   FIRST_CHANGE# <= :i");

    const char* OracleAnalyzerOnline::SQL_GET_LOGFILE_LIST(
            "SELECT"
            "   LF.GROUP#"
            ",  LF.MEMBER"
            " FROM"
            "   SYS.V_$LOGFILE LF"
            " WHERE"
            "   TYPE = :i"
            " ORDER BY"
            "   LF.GROUP# ASC"
            ",  LF.IS_RECOVERY_DEST_FILE DESC"
            ",  LF.MEMBER ASC");

    const char* OracleAnalyzerOnline::SQL_GET_TABLE_LIST(
            "SELECT"
            "   T.DATAOBJ#"
            ",  T.OBJ#"
            ",  T.CLUCOLS"
            ",  U.NAME"
            ",  O.NAME"
            ",  DECODE(BITAND(T.PROPERTY, 1024), 0, 0, 1)"
            //7: IOT overflow segment
            ",  DECODE((BITAND(T.PROPERTY, 512)+BITAND(T.FLAGS, 536870912)), 0, 0, 1)"
            ",  DECODE(BITAND(U.SPARE1, 1), 1, 1, 0)"
            ",  DECODE(BITAND(U.SPARE1, 8), 8, 1, 0)"
            //10: partitioned
            ",  CASE WHEN BITAND(T.PROPERTY, 32) = 32 THEN 1 ELSE 0 END"
            //11: temporary, secondary, in-memory temp
            ",  DECODE(BITAND(O.FLAGS,2)+BITAND(O.FLAGS,16)+BITAND(O.FLAGS,32), 0, 0, 1)"
            //12: nested
            ",  DECODE(BITAND(T.PROPERTY, 8192), 8192, 1, 0)"
            ",  DECODE(BITAND(T.FLAGS, 131072), 131072, 1, 0)"
            ",  DECODE(BITAND(T.FLAGS, 8388608), 8388608, 1, 0)"
            //15: compressed
            ",  CASE WHEN (BITAND(T.PROPERTY, 32) = 32) THEN 0 WHEN (BITAND(T.PROPERTY, 17179869184) = 17179869184) THEN DECODE(BITAND(DS.FLAGS_STG, 4), 4, 1, 0) ELSE DECODE(BITAND(S.SPARE1, 2048), 2048, 1, 0) END "
            " FROM"
            "   SYS.OBJ$ O"
            " JOIN"
            "   SYS.TAB$ T ON"
            "     T.OBJ# = O.OBJ#"
            " JOIN"
            "   SYS.USER$ U ON"
            "     O.OWNER# = U.USER#"
            " LEFT OUTER JOIN"
            "   SYS.SEG$ S ON "
            "     T.FILE# = S.FILE# AND T.BLOCK# = S.BLOCK# AND T.TS# = S.TS#"
            " LEFT OUTER JOIN"
            "   SYS.DEFERRED_STG$ DS ON"
            "     T.OBJ# = DS.OBJ#"
            " WHERE"
            "   BITAND(O.flags, 128) = 0"
            "   AND U.NAME || '.' || O.NAME LIKE UPPER(:i)"
            " ORDER BY"
            "   4,5");

    const char* OracleAnalyzerOnline::SQL_GET_COLUMN_LIST(
            "SELECT"
            "   C.COL#"
            ",  C.SEGCOL#"
            ",  C.NAME"
            ",  C.TYPE#"
            ",  C.LENGTH"
            ",  C.PRECISION#"
            ",  C.SCALE"
            ",  C.CHARSETFORM"
            ",  C.CHARSETID"
            ",  C.NULL$"
            //11: invisible
            ",  DECODE(BITAND(C.PROPERTY, 32), 32, 1, 0)"
            //12: stored as lob
            ",  DECODE(BITAND(C.PROPERTY, 128), 128, 1, 0)"
            //13: constraint
            ",  DECODE(BITAND(C.PROPERTY, 256), 256, 1, 0)"
            //14: added column
            ",  DECODE(BITAND(C.PROPERTY, 1073741824), 1073741824, 1, 0)"
            //15: guard column
            ",  DECODE(BITAND(C.PROPERTY, 549755813888), 549755813888, 1, 0)"
            ",  E.GUARD_ID"
            //17: number of primary key constraints
            ",  (SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#)"
            //18: number of supplementary columns
            ",  (SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0)"
            " FROM"
            "   SYS.COL$ C"
            " LEFT OUTER JOIN"
            "   SYS.ECOL$ E ON"
            "     E.TABOBJ# = C.OBJ#"
            "     AND E.COLNUM = C.SEGCOL#"
            " WHERE"
            "   C.SEGCOL# > 0"
            "   AND C.OBJ# = :i"
            " ORDER BY"
            "   2");

    const char* OracleAnalyzerOnline::SQL_GET_COLUMN_LIST11(
            "SELECT"
            "   C.COL#"
            ",  C.SEGCOL#"
            ",  C.NAME"
            ",  C.TYPE#"
            ",  C.LENGTH"
            ",  C.PRECISION#"
            ",  C.SCALE"
            ",  C.CHARSETFORM"
            ",  C.CHARSETID"
            ",  C.NULL$"
            //11: invisible
            ",  DECODE(BITAND(C.PROPERTY, 32), 32, 1, 0)"
            //12: stored as lob
            ",  DECODE(BITAND(C.PROPERTY, 128), 128, 1, 0)"
            //13: constraint
            ",  DECODE(BITAND(C.PROPERTY, 256), 256, 1, 0)"
            //14: added column
            ",  DECODE(BITAND(C.PROPERTY, 1073741824), 1073741824, 1, 0)"
            //15: guard column
            ",  DECODE(BITAND(C.PROPERTY, 549755813888), 549755813888, 1, 0)"
            ",  (SELECT COUNT(*) FROM SYS.COL$ C2 WHERE C2.SEGCOL# > 0 AND C2.SEGCOL# < C.SEGCOL# AND C2.OBJ# = C.OBJ# AND DECODE(BITAND(C2.PROPERTY, 1073741824), 1073741824, 1, 0) = 1)"
            //17: number of primary key constraints
            ",  (SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#)"
            //18: number of supplementary columns
            ",  (SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0)"
            " FROM"
            "   SYS.COL$ C"
            " WHERE"
            "   C.SEGCOL# > 0"
            "   AND C.OBJ# = :i"
            " ORDER BY"
            "   2");

    const char* OracleAnalyzerOnline::SQL_GET_PARTITION_LIST(
            "SELECT"
            "   T.OBJ#"
            ",  T.DATAOBJ#"
            " FROM"
            "   SYS.TABPART$ T"
            " WHERE"
            "   T.BO# = :1"
            " UNION ALL"
            " SELECT"
            "   TSP.OBJ#"
            ",  TSP.DATAOBJ#"
            " FROM"
            "   SYS.TABSUBPART$ TSP"
            " JOIN"
            "   SYS.TABCOMPART$ TCP ON"
            "     TCP.OBJ# = TSP.POBJ#"
            " WHERE"
            "   TCP.BO# = :1");

    const char* OracleAnalyzerOnline::SQL_GET_SUPPLEMNTAL_LOG_TABLE(
            "SELECT"
            "   C.TYPE#"
            " FROM"
            "   SYS.CON$ OC"
            " JOIN"
            "   SYS.CDEF$ C ON"
            "     OC.CON# = C.CON#"
            " WHERE"
            "   C.OBJ# = :i"
            "   AND (C.TYPE# = 14 OR C.TYPE# = 17)");

    const char* OracleAnalyzerOnline::SQL_GET_PARAMETER(
            "SELECT"
            "   VALUE"
            " FROM"
            "   SYS.V_$PARAMETER"
            " WHERE"
            "   NAME = :i");

    const char* OracleAnalyzerOnline::SQL_GET_PROPERTY(
            "SELECT"
            "   PROPERTY_VALUE"
            " FROM"
            "   DATABASE_PROPERTIES"
            " WHERE"
            "   PROPERTY_NAME = :1");

    OracleAnalyzerOnline::OracleAnalyzerOnline(OutputBuffer *outputBuffer, const char *alias, const char *database, uint64_t trace,
            uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpRawData, uint64_t flags, uint64_t disableChecks,
            uint64_t redoReadSleep, uint64_t archReadSleep, uint64_t memoryMinMb, uint64_t memoryMaxMb,
            const char *user, const char *password, const char *connectString, bool isStandby) :
                    OracleAnalyzer(outputBuffer, alias, database, trace, trace2, dumpRedoLog, dumpRawData, flags,
            disableChecks, redoReadSleep, archReadSleep, memoryMinMb, memoryMaxMb),
            isStandby(isStandby),
            user(user),
            password(password),
            connectString(connectString),
            env(nullptr),
            conn(nullptr),
            keepConnection(false) {

        env = new DatabaseEnvironment();
    }

    OracleAnalyzerOnline::~OracleAnalyzerOnline() {
        closeConnection();

        if (env != nullptr) {
            delete env;
            env = nullptr;
        }
    }

    void OracleAnalyzerOnline::initialize(void) {
        checkConnection();
        if (shutdown)
            return;

        typescn currentDatabaseScn;
        typeresetlogs currentResetlogs;
        typeactivation currentActivation;

        if ((disableChecks & DISABLE_CHECK_GRANTS) == 0) {
            checkTableForGrants("SYS.CCOL$");
            checkTableForGrants("SYS.CDEF$");
            checkTableForGrants("SYS.COL$");
            checkTableForGrants("SYS.CON$");
            checkTableForGrants("SYS.DEFERRED_STG$");
            checkTableForGrants("SYS.ECOL$");
            checkTableForGrants("SYS.ICOL$");
            checkTableForGrants("SYS.IND$");
            checkTableForGrants("SYS.OBJ$");
            checkTableForGrants("SYS.SEG$");
            checkTableForGrants("SYS.TAB$");
            checkTableForGrants("SYS.TABCOMPART$");
            checkTableForGrants("SYS.TABPART$");
            checkTableForGrants("SYS.TABSUBPART$");
            checkTableForGrants("SYS.USER$");
            checkTableForGrants("SYS.V_$ARCHIVED_LOG");
            checkTableForGrants("SYS.V_$DATABASE");
            checkTableForGrants("SYS.V_$DATABASE_INCARNATION");
            checkTableForGrants("SYS.V_$LOG");
            checkTableForGrants("SYS.V_$LOGFILE");
            checkTableForGrants("SYS.V_$PARAMETER");
            checkTableForGrants("SYS.V_$STANDBY_LOG");
            checkTableForGrants("SYS.V_$TRANSPORTABLE_PLATFORM");
        }

        {
            DatabaseStatement stmt(conn);
            TRACE_(TRACE2_SQL, SQL_GET_DATABASE_INFORMATION);
            stmt.createStatement(SQL_GET_DATABASE_INFORMATION);
            uint64_t logMode; stmt.defineUInt64(1, logMode);
            uint64_t supplementalLogMin; stmt.defineUInt64(2, supplementalLogMin);
            stmt.defineUInt64(3, suppLogDbPrimary);
            stmt.defineUInt64(4, suppLogDbAll);
            stmt.defineUInt64(5, isBigEndian);
            stmt.defineUInt64(6, currentDatabaseScn);
            stmt.defineUInt32(7, currentResetlogs);
            stmt.defineUInt32(8, currentActivation);
            char bannerStr[81]; stmt.defineString(9, bannerStr, sizeof(bannerStr));
            char databaseContextStr[81]; stmt.defineString(10, databaseContextStr, sizeof(databaseContextStr));

            if (stmt.executeQuery()) {
                if (logMode == 0) {
                    RUNTIME_FAIL("database not in ARCHIVELOG mode" << endl <<
                            "HINT run: SHUTDOWN IMMEDIATE;" << endl <<
                            "HINT run: STARTUP MOUNT;" << endl <<
                            "HINT run: ALTER DATABASE ARCHIVELOG;" << endl <<
                            "HINT run: ALTER DATABASE OPEN;");
                }

                if (supplementalLogMin == 0) {
                    RUNTIME_FAIL("SUPPLEMENTAL_LOG_DATA_MIN missing" << endl <<
                            "HINT run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;" << endl <<
                            "HINT run: ALTER SYSTEM ARCHIVE LOG CURRENT;");
                }

                if (isBigEndian)
                    setBigEndian();

                if (resetlogs != 0 && currentResetlogs != resetlogs) {
                    RUNTIME_FAIL("database resetlogs:" << dec << currentResetlogs << ", expected: " << resetlogs);
                } else {
                    resetlogs = currentResetlogs;
                }

                if (activation != 0 && currentActivation != activation) {
                    RUNTIME_FAIL("database activation: " << dec << currentActivation << ", expected: " << activation);
                } else {
                    activation = currentActivation;
                }

                //12+
                conId = 0;
                if (memcmp(bannerStr, "Oracle Database 11g", 19) != 0) {
                    version12 = true;
                    DatabaseStatement stmt2(conn);
                    TRACE_(TRACE2_SQL, SQL_GET_CON_INFO);
                    stmt2.createStatement(SQL_GET_CON_INFO);
                    stmt2.defineUInt16(1, conId);
                    char conNameChar[81];
                    stmt2.defineString(2, conNameChar, sizeof(conNameChar));
                    if (stmt2.executeQuery())
                        conName = conNameChar;
                }
                databaseContext = databaseContextStr;

                INFO_("version: " << dec << bannerStr << ", database: " << databaseContext << ", resetlogs: " << dec << resetlogs <<
                        ", activation: " << activation << ", con_id: " << conId << ", con_name: " << conName);
            } else {
                RUNTIME_FAIL("trying to read SYS.V_$DATABASE");
            }
        }

        dbRecoveryFileDest = getParameterValue("db_recovery_file_dest");
        logArchiveDest = getParameterValue("log_archive_dest");
        logArchiveFormat = getParameterValue("log_archive_format");
        nlsCharacterSet = getPropertyValue("NLS_CHARACTERSET");
        nlsNcharCharacterSet = getPropertyValue("NLS_NCHAR_CHARACTERSET");
        outputBuffer->setNlsCharset(nlsCharacterSet, nlsNcharCharacterSet);

        {
            DatabaseStatement stmt(conn);
            TRACE_(TRACE2_SQL, SQL_GET_LOGFILE_LIST << endl <<
                    "PARAM1: " << isStandby);
            stmt.createStatement(SQL_GET_LOGFILE_LIST);
            if (isStandby)
                stmt.bindString(1, "STANDBY");
            else
                stmt.bindString(1, "ONLINE");

            int64_t group = -1; stmt.defineInt64(1, group);
            char pathStr[514]; stmt.defineString(2, pathStr, sizeof(pathStr));
            int64_t ret = stmt.executeQuery();

            Reader *onlineReader = nullptr;
            int64_t lastGroup = -1;
            string path;

            while (ret) {
                if (group != lastGroup) {
                    onlineReader = readerCreate(group);
                    lastGroup = group;
                }
                path = pathStr;
                onlineReader->paths.push_back(path);
                ret = stmt.next();
            }

            if (readers.size() == 0) {
                if (isStandby) {
                    RUNTIME_FAIL("failed to find standby redo log files");
                } else {
                    RUNTIME_FAIL("failed to find online redo log files");
                }
            }
        }

        checkOnlineRedoLogs();
        archReader = readerCreate(0);

        //position by sequence
        if (startSequence > 0) {
            DatabaseStatement stmt(conn);
            if (isStandby) {
                TRACE_(TRACE2_SQL, SQL_GET_SCN_FROM_SEQUENCE_STANDBY);
                stmt.createStatement(SQL_GET_SCN_FROM_SEQUENCE_STANDBY);
            } else {
                TRACE_(TRACE2_SQL, SQL_GET_SCN_FROM_SEQUENCE);
                stmt.createStatement(SQL_GET_SCN_FROM_SEQUENCE);
            }

            stmt.bindUInt32(1, startSequence);
            stmt.bindUInt32(2, resetlogs);
            stmt.bindUInt32(3, activation);
            stmt.bindUInt32(4, startSequence);
            stmt.defineUInt64(1, databaseScn);

            if (!stmt.executeQuery()) {
                RUNTIME_FAIL("can't find redo sequence " << dec << databaseSequence);
            }
            databaseSequence = startSequence;

        //position by time
        } else if (startTime.length() > 0) {
            DatabaseStatement stmt(conn);
            if (isStandby) {
                RUNTIME_FAIL("can't position by time for standby database");
            } else {
                TRACE_(TRACE2_SQL, SQL_GET_SCN_FROM_TIME);
                stmt.createStatement(SQL_GET_SCN_FROM_TIME);
            }
            stringstream ss;
            stmt.bindString(1, startTime);
            stmt.defineUInt64(1, databaseScn);

            if (!stmt.executeQuery()) {
                RUNTIME_FAIL("can't find SCN for: " << startTime);
            }

        } else if (startTimeRel > 0) {
            DatabaseStatement stmt(conn);
            if (isStandby) {
                RUNTIME_FAIL("can't position by relative time for standby database");
            } else {
                TRACE_(TRACE2_SQL, SQL_GET_SCN_FROM_TIME_RELATIVE);
                stmt.createStatement(SQL_GET_SCN_FROM_TIME_RELATIVE);
            }

            stmt.bindInt64(1, startTimeRel);
            stmt.defineUInt64(1, databaseScn);

            if (!stmt.executeQuery()) {
                RUNTIME_FAIL("can't find SCN for " << dec << startTime);
            }

        } else if (startScn > 0) {
            databaseScn = startScn;
        } else {
            //NOW
            databaseScn = currentDatabaseScn;
        }

        if (databaseScn == 0) {
            RUNTIME_FAIL("getting database SCN");
        }

        OracleAnalyzer::initialize();

        if (databaseSequence == 0) {
            FULL_("starting sequence not found - starting with new batch");

            DatabaseStatement stmt(conn);
            if (isStandby) {
                TRACE_(TRACE2_SQL, SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
            } else {
                TRACE_(TRACE2_SQL, SQL_GET_SEQUENCE_FROM_SCN);
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN);
            }
            stmt.bindUInt64(1, databaseScn);
            stmt.defineUInt32(1, databaseSequence);

            if (!stmt.executeQuery()) {
                RUNTIME_FAIL("getting database sequence for SCN: " << dec << databaseScn);
            }
        }

        FULL_("start SEQ: " << dec << databaseSequence);

        if (!keepConnection)
            closeConnection();
    }

    void OracleAnalyzerOnline::checkConnection(void) {
        while (!shutdown) {
            if (conn == nullptr) {
                INFO_("connecting to Oracle instance of " << database << " to " << connectString);

                try {
                    conn = new DatabaseConnection(env, user, password, connectString, false);
                } catch(RuntimeException &ex) {
                    //
                }
            }

            if (conn != nullptr)
                break;

            WARNING_("cannot connect to database, retry in 5 sec.");
            sleep(5);
        }
    }

    void OracleAnalyzerOnline::closeConnection(void) {
        if (conn != nullptr) {
            delete conn;
            conn = nullptr;
        }
    }

    string OracleAnalyzerOnline::getParameterValue(const char *parameter) {
        char value[4001];
        DatabaseStatement stmt(conn);
        TRACE_(TRACE2_SQL, SQL_GET_PARAMETER << endl <<
                "PARAM1: " << parameter);
        stmt.createStatement(SQL_GET_PARAMETER);
        stmt.bindString(1, parameter);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery())
            return value;

        //no value found
        RUNTIME_FAIL("can't get parameter value for " << parameter);
    }

    string OracleAnalyzerOnline::getPropertyValue(const char *property) {
        char value[4001];
        DatabaseStatement stmt(conn);
        TRACE_(TRACE2_SQL, SQL_GET_PROPERTY << endl <<
                "PARAM1: " << property);
        stmt.createStatement(SQL_GET_PROPERTY);
        stmt.bindString(1, property);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery())
            return value;

        //no value found
        RUNTIME_FAIL("can't get proprty value for " << property);
    }

    void OracleAnalyzerOnline::checkTableForGrants(string tableName) {
        try {
            string query("SELECT 1 FROM " + tableName + " WHERE 0 = 1");

            DatabaseStatement stmt(conn);
            TRACE_(TRACE2_SQL, query);
            stmt.createStatement(query.c_str());
            uint64_t dummy; stmt.defineUInt64(1, dummy);
            stmt.executeQuery();
        } catch (RuntimeException &ex) {
            if (conId > 0) {
                RUNTIME_FAIL("grants missing" << endl <<
                        "HINT run: ALTER SESSION SET CONTAINER = " << conName << ";" << endl <<
                        "HINT run: GRANT SELECT ON " << tableName << " TO " << user << ";");
            } else {
                RUNTIME_FAIL("grants missing" << endl << "HINT run: GRANT SELECT ON " << tableName << " TO " << user << ";");
            }
            throw RuntimeException (ex.msg);
        }
    }

    void OracleAnalyzerOnline::refreshSchema(void) {
        for (SchemaElement *element : schema->elements)
            addTable(element->mask, element->keys, element->keysStr, element->options);
    }

    void OracleAnalyzerOnline::addTable(string &mask, vector<string> &keys, string &keysStr, uint64_t options) {
        INFO_("- reading table schema for: " << mask);
        uint64_t tabCnt = 0;
        DatabaseStatement stmt(conn), stmtCol(conn), stmtPart(conn), stmtSupp(conn);

        TRACE_(TRACE2_SQL, SQL_GET_TABLE_LIST << endl <<
                "PARAM1: " << mask);
        stmt.createStatement(SQL_GET_TABLE_LIST);
        typeobj objd; stmt.defineUInt32(1, objd);
        typeobj objn; stmt.defineUInt32(2, objn);
        uint64_t cluCols; stmt.defineUInt64(3, cluCols);
        char owner[129]; stmt.defineString(4, owner, sizeof(owner));
        char name[129]; stmt.defineString(5, name, sizeof(name));
        uint64_t clustered; stmt.defineUInt64(6, clustered);
        uint64_t iot; stmt.defineUInt64(7, iot);
        uint64_t suppLogSchemaPrimary; stmt.defineUInt64(8, suppLogSchemaPrimary);
        uint64_t suppLogSchemaAll; stmt.defineUInt64(9, suppLogSchemaAll);
        uint64_t partitioned; stmt.defineUInt64(10, partitioned);
        uint64_t temporary; stmt.defineUInt64(11, temporary);
        uint64_t nested; stmt.defineUInt64(12, nested);
        uint64_t rowMovement; stmt.defineUInt64(13, rowMovement);
        uint64_t dependencies; stmt.defineUInt64(14, dependencies);
        uint64_t compressed; stmt.defineUInt64(15, compressed);

        if (version12) {
            TRACE_(TRACE2_SQL, SQL_GET_COLUMN_LIST << endl <<
                    "PARAM1: " << dec << objn);
            stmtCol.createStatement(SQL_GET_COLUMN_LIST);
        } else {
            TRACE_(TRACE2_SQL, SQL_GET_COLUMN_LIST11 << endl <<
                    "PARAM1: " << dec << objn);
            stmtCol.createStatement(SQL_GET_COLUMN_LIST11);
        }
        typecol colNo; stmtCol.defineInt64(1, colNo);
        typecol segColNo; stmtCol.defineInt64(2, segColNo);
        char columnName[129]; stmtCol.defineString(3, columnName, sizeof(columnName));
        uint64_t typeNo; stmtCol.defineUInt64(4, typeNo);
        uint64_t length; stmtCol.defineUInt64(5, length);
        int64_t precision; stmtCol.defineInt64(6, precision);
        int64_t scale; stmtCol.defineInt64(7, scale);
        uint64_t charsetForm; stmtCol.defineUInt64(8, charsetForm);
        uint64_t charmapId; stmtCol.defineUInt64(9, charmapId);
        int64_t nullable; stmtCol.defineInt64(10, nullable);
        int64_t invisible; stmtCol.defineInt64(11, invisible);
        int64_t storedAsLob; stmtCol.defineInt64(12, storedAsLob);
        int64_t constraint; stmtCol.defineInt64(13, constraint);
        int64_t added; stmtCol.defineInt64(14, added);
        int64_t guard; stmtCol.defineInt64(15, guard);
        typecol guardSegNo; stmtCol.defineInt64(16, guardSegNo);
        uint64_t numPk; stmtCol.defineUInt64(17, numPk);
        uint64_t numSup; stmtCol.defineUInt64(18, numSup);
        stmtCol.bindUInt32(1, objn);

        TRACE_(TRACE2_SQL, SQL_GET_PARTITION_LIST << endl <<
                "PARAM1: " << dec << objn << endl <<
                "PARAM2: " << dec << objn);
        stmtPart.createStatement(SQL_GET_PARTITION_LIST);
        typeobj partitionObjn; stmtPart.defineUInt32(1, partitionObjn);
        typeobj partitionObjd; stmtPart.defineUInt32(2, partitionObjd);
        stmtPart.bindUInt32(1, objn);
        stmtPart.bindUInt32(2, objn);

        TRACE_(TRACE2_SQL, SQL_GET_SUPPLEMNTAL_LOG_TABLE << endl <<
                "PARAM1: " << dec << objn);
        stmtSupp.createStatement(SQL_GET_SUPPLEMNTAL_LOG_TABLE);
        uint64_t typeNo2; stmtSupp.defineUInt64(1, typeNo2);
        stmtSupp.bindUInt32(1, objn);

        stmt.bindString(1, mask.c_str());
        cluCols = 0;
        objd = 0;
        int64_t ret = stmt.executeQuery();

        while (ret) {
            //skip Index Organized Tables (IOT)
            if (iot) {
                INFO_("  * skipped: " << owner << "." << name << " (OBJN: " << dec << objn << ") - IOT");
                cluCols = 0;
                objd = 0;
                ret = stmt.next();
                continue;
            }

            //skip temporary tables
            if (temporary) {
                INFO_("  * skipped: " << owner << "." << name << " (OBJN: " << dec << objn << ") - temporary table");
                cluCols = 0;
                objd = 0;
                ret = stmt.next();
                continue;
            }

            //skip nested tables
            if (nested) {
                INFO_("  * skipped: " << owner << "." << name << " (OBJN: " << dec << objn << ") - nested table");
                cluCols = 0;
                objd = 0;
                ret = stmt.next();
                continue;
            }

            //skip compressed tables
            if (compressed) {
                INFO_("  * skipped: " << owner << "." << name << " (OBJN: " << dec << objn << ") - compressed table");
                objd = 0;
                cluCols = 0;
                ret = stmt.next();
                continue;
            }

            //table already added with another rule
            if (schema->checkDict(objn, objd) != nullptr) {
                INFO_("  * skipped: " << owner << "." << name << " (OBJN: " << dec << objn << ") - already added");
                objd = 0;
                cluCols = 0;
                ret = stmt.next();
                continue;
            }

            uint64_t totalPk = 0, maxSegCol = 0, keysCnt = 0;
            bool suppLogTablePrimary = false, suppLogTableAll = false, supLogColMissing = false;

            schema->object = new OracleObject(objn, objd, cluCols, options, owner, name);
            if (schema->object == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleObject) << " bytes memory (for: object creation)");
            }
            ++tabCnt;

            if (partitioned) {
                int64_t ret2 = stmtPart.executeQuery();

                while (ret2) {
                    schema->object->addPartition(partitionObjn, partitionObjd);
                    ret2 = stmtPart.next();
                }
            }

            if ((disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0 && !suppLogDbAll && !suppLogSchemaAll && !suppLogSchemaAll) {
                int64_t ret2 = stmtSupp.executeQuery();

                while (ret2) {
                    if (typeNo2 == 14) suppLogTablePrimary = true;
                    else if (typeNo2 == 17) suppLogTableAll = true;
                    ret2 = stmtSupp.next();
                }
            }

            precision = -1;
            scale = -1;
            guardSegNo = -1;
            int64_t ret2 = stmtCol.executeQuery();

            while (ret2) {
                if (charsetForm == 1)
                    charmapId = outputBuffer->defaultCharacterMapId;
                else if (charsetForm == 2)
                    charmapId = outputBuffer->defaultCharacterNcharMapId;

                //check character set for char and varchar2
                if (typeNo == 1 || typeNo == 96) {
                    auto it = outputBuffer->characterMap.find(charmapId);
                    if (it == outputBuffer->characterMap.end()) {
                        RUNTIME_FAIL("table " << owner << "." << name << " - unsupported character set id: " << dec << charmapId <<
                                " for column: " << columnName << endl <<
                                "HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << dec << charmapId << ") FROM DUAL;");
                    }
                }

                //column part of defined primary key
                if (keys.size() > 0) {
                    //manually defined pk overlaps with table pk
                    if (numPk > 0 && (suppLogTablePrimary || suppLogSchemaPrimary || suppLogDbPrimary))
                        numSup = 1;
                    numPk = 0;
                    for (vector<string>::iterator it = keys.begin(); it != keys.end(); ++it) {
                        if (strcmp(columnName, it->c_str()) == 0) {
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

                FULL_("    - col: " << dec << segColNo << ": " << columnName << " (pk: " << dec << numPk << ", G: " << dec << guardSegNo << ")");

                OracleColumn *column = new OracleColumn(colNo, guardSegNo, segColNo, columnName, typeNo, length, precision, scale, numPk,
                        charmapId, nullable, invisible, storedAsLob, constraint, added, guard);
                if (column == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleColumn) << " bytes memory (for: column creation)");
                }

                totalPk += numPk;
                if (segColNo > maxSegCol)
                    maxSegCol = segColNo;

                schema->object->addColumn(column);

                precision = -1;
                scale = -1;
                guardSegNo = -1;
                ret2 = stmtCol.next();
            }

            //check if table has all listed columns
            if (keys.size() != keysCnt) {
                RUNTIME_FAIL("table " << owner << "." << name << " couldn't find all column set (" << keysStr << ")");
            }

            stringstream ss;
            ss << "  * found: " << owner << "." << name << " (OBJD: " << dec << objd << ", OBJN: " << dec << objn << ")";
            if (clustered)
                ss << ", part of cluster";
            if (partitioned)
                ss << ", partitioned";
            if (dependencies)
                ss << ", row dependencies";
            if (rowMovement)
                ss << ", row movement enabled";

            if ((disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0) {
                //use default primary key
                if (keys.size() == 0) {
                    if (totalPk == 0)
                        ss << " - primary key missing";
                    else if (!suppLogTablePrimary && !suppLogTableAll &&
                            !suppLogSchemaPrimary && !suppLogSchemaAll &&
                            !suppLogDbPrimary && !suppLogDbAll && supLogColMissing)
                        ss << " - supplemental log missing, try: ALTER TABLE " << owner << "." << name << " ADD SUPPLEMENTAL LOG GROUP DATA (PRIMARY KEY) COLUMNS;";
                //user defined primary key
                } else {
                    if (!suppLogTableAll && !suppLogSchemaAll && !suppLogDbAll && supLogColMissing)
                        ss << " - supplemental log missing, try: ALTER TABLE " << owner << "." << name << " ADD SUPPLEMENTAL LOG GROUP GRP" << dec << objn << " (" << keysStr << ") ALWAYS;";
                }
            }
            INFO_(ss.str());

            schema->object->maxSegCol = maxSegCol;
            schema->object->totalPk = totalPk;
            schema->object->updatePK();
            schema->addToDict(schema->object);
            schema->object = nullptr;

            objd = 0;
            cluCols = 0;
            ret = stmt.next();
        }
        INFO_("  * total: " << dec << tabCnt << " tables");
    }

    void OracleAnalyzerOnline::archGetLogOnline(OracleAnalyzer *oracleAnalyzer) {
        ((OracleAnalyzerOnline*)oracleAnalyzer)->checkConnection();

        DatabaseStatement stmt(((OracleAnalyzerOnline*)oracleAnalyzer)->conn);
        TRACE(TRACE2_SQL, SQL_GET_ARCHIVE_LOG_LIST << endl <<
                "PARAM1: " << dec << ((OracleAnalyzerOnline*)oracleAnalyzer)->databaseSequence << endl <<
                "PARAM2: " << dec << oracleAnalyzer->resetlogs << endl <<
                "PARAM3: " << dec << oracleAnalyzer->activation);

        stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
        stmt.bindUInt32(1, ((OracleAnalyzerOnline*)oracleAnalyzer)->databaseSequence);
        stmt.bindUInt32(2, oracleAnalyzer->resetlogs);
        stmt.bindUInt32(3, oracleAnalyzer->activation);

        char path[513]; stmt.defineString(1, path, sizeof(path));
        typeseq sequence; stmt.defineUInt32(2, sequence);
        typescn firstScn; stmt.defineUInt64(3, firstScn);
        typescn nextScn; stmt.defineUInt64(4, nextScn);
        int64_t ret = stmt.executeQuery();

        while (ret) {
            string mappedPath = oracleAnalyzer->applyMapping(path);

            RedoLog* redo = new RedoLog(oracleAnalyzer, 0, mappedPath.c_str());
            if (redo == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(RedoLog) << " bytes memory (arch log list#1)");
            }

            redo->firstScn = firstScn;
            redo->nextScn = nextScn;
            redo->sequence = sequence;
            ((OracleAnalyzerOnline*)oracleAnalyzer)->archiveRedoQueue.push(redo);
            ret = stmt.next();
        }

        if (!((OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection)
            ((OracleAnalyzerOnline*)oracleAnalyzer)->closeConnection();
    }


    const char* OracleAnalyzerOnline::getModeName(void) {
        return "online";
    }
}
