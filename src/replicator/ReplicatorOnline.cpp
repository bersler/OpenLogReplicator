/* Thread reading Oracle Redo Logs using online mode
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <algorithm>
#include <regex>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/OracleColumn.h"
#include "../common/OracleTable.h"
#include "../common/OracleIncarnation.h"
#include "../common/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../metadata/SchemaElement.h"
#include "../parser/Parser.h"
#include "../parser/TransactionBuffer.h"
#include "../reader/Reader.h"
#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "ReplicatorOnline.h"

namespace OpenLogReplicator {
    const char* ReplicatorOnline::SQL_GET_ARCHIVE_LOG_LIST(
            "SELECT"
            "   NAME"
            ",  SEQUENCE#"
            ",  FIRST_CHANGE#"
            ",  NEXT_CHANGE#"
            " FROM"
            "   SYS.V_$ARCHIVED_LOG"
            " WHERE"
            "   SEQUENCE# >= :i"
            "   AND RESETLOGS_ID = :j"
            "   AND NAME IS NOT NULL"
            "   AND IS_RECOVERY_DEST_FILE = 'YES'"
            " ORDER BY"
            "   SEQUENCE#"
            ",  DEST_ID");

    const char* ReplicatorOnline::SQL_GET_DATABASE_INFORMATION(
            "SELECT"
            "   DECODE(D.LOG_MODE, 'ARCHIVELOG', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_MIN, 'NO', 0, 1)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_PK, 'YES', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_ALL, 'YES', 1, 0)"
            ",  DECODE(TP.ENDIAN_FORMAT, 'Big', 1, 0)"
            ",  VER.BANNER"
            ",  SYS_CONTEXT('USERENV','DB_NAME')"
            ",  CURRENT_SCN"
            " FROM"
            "   SYS.V_$DATABASE D"
            " JOIN"
            "   SYS.V_$TRANSPORTABLE_PLATFORM TP ON"
            "     TP.PLATFORM_NAME = D.PLATFORM_NAME"
            " JOIN"
            "   SYS.V_$VERSION VER ON"
            "     VER.BANNER LIKE '%Oracle Database%'");

    const char* ReplicatorOnline::SQL_GET_DATABASE_INCARNATION(
            "SELECT"
            "   INCARNATION#"
            ",  RESETLOGS_CHANGE#"
            ",  PRIOR_RESETLOGS_CHANGE#"
            ",  STATUS"
            ",  RESETLOGS_ID"
            ",  PRIOR_INCARNATION#"
            " FROM"
            "   SYS.V_$DATABASE_INCARNATION");

    const char* ReplicatorOnline::SQL_GET_DATABASE_ROLE(
            "SELECT"
            "   DATABASE_ROLE"
            " FROM"
            "   SYS.V_$DATABASE");

    const char* ReplicatorOnline::SQL_GET_DATABASE_SCN(
            "SELECT"
            "   D.CURRENT_SCN"
            " FROM"
            "   SYS.V_$DATABASE D");

    const char* ReplicatorOnline::SQL_GET_CON_INFO(
            "SELECT"
            "   SYS_CONTEXT('USERENV','CON_ID')"
            ",  SYS_CONTEXT('USERENV','CON_NAME')"
            " FROM"
            "   DUAL");

    const char* ReplicatorOnline::SQL_GET_SCN_FROM_TIME(
            "SELECT TIMESTAMP_TO_SCN(TO_DATE('YYYY-MM-DD HH24:MI:SS', :i) FROM DUAL");

    const char* ReplicatorOnline::SQL_GET_SCN_FROM_TIME_RELATIVE(
            "SELECT TIMESTAMP_TO_SCN(SYSDATE - (:i/24/3600)) FROM DUAL");

    const char* ReplicatorOnline::SQL_GET_SEQUENCE_FROM_SCN(
            "SELECT MAX(SEQUENCE#) FROM ("
            "  SELECT"
            "     SEQUENCE#"
            "   FROM"
            "     SYS.V_$LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            " UNION"
            "  SELECT"
            "     SEQUENCE#"
            "   FROM"
            "     SYS.V_$ARCHIVED_LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            "     AND RESETLOGS_ID = :j)");

    const char* ReplicatorOnline::SQL_GET_SEQUENCE_FROM_SCN_STANDBY(
            "SELECT MAX(SEQUENCE#) FROM ("
            "  SELECT"
            "     SEQUENCE#"
            "   FROM"
            "     SYS.V_$STANDBY_LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            " UNION"
            "  SELECT"
            "     SEQUENCE#"
            "   FROM"
            "     SYS.V_$ARCHIVED_LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            "     AND RESETLOGS_ID = :j)");

    const char* ReplicatorOnline::SQL_GET_LOGFILE_LIST(
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

    const char* ReplicatorOnline::SQL_GET_PARAMETER(
            "SELECT"
            "   VALUE"
            " FROM"
            "   SYS.V_$PARAMETER"
            " WHERE"
            "   NAME = :i");

    const char* ReplicatorOnline::SQL_GET_PROPERTY(
            "SELECT"
            "   PROPERTY_VALUE"
            " FROM"
            "   DATABASE_PROPERTIES"
            " WHERE"
            "   PROPERTY_NAME = :i");

    const char* ReplicatorOnline::SQL_GET_SYS_CCOL_USER(
            "SELECT"
            "   L.ROWID, L.CON#, L.INTCOL#, L.OBJ#, MOD(L.SPARE1, 18446744073709551616) AS SPARE11,"
            "   MOD(TRUNC(L.SPARE1 / 18446744073709551616), 18446744073709551616) AS SPARE12"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.CCOL$ AS OF SCN :j L ON"
            "     O.OBJ# = L.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_CCOL_OBJ(
            "SELECT"
            "   L.ROWID, L.CON#, L.INTCOL#, L.OBJ#, MOD(L.SPARE1, 18446744073709551616) AS SPARE11,"
            "   MOD(TRUNC(L.SPARE1 / 18446744073709551616), 18446744073709551616) AS SPARE12"
            " FROM"
            "   SYS.CCOL$ AS OF SCN :j L"
            " WHERE"
            "   L.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_CDEF_USER(
            "SELECT"
            "   D.ROWID, D.CON#, D.OBJ#, D.TYPE#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.CDEF$ AS OF SCN :j D ON"
            "     O.OBJ# = D.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_CDEF_OBJ(
            "SELECT"
            "   D.ROWID, D.CON#, D.OBJ#, D.TYPE#"
            " FROM"
            "   SYS.CDEF$ AS OF SCN :j D"
            " WHERE"
            "   D.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_COL_USER(
            "SELECT"
            "   C.ROWID, C.OBJ#, C.COL#, C.SEGCOL#, C.INTCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.CHARSETFORM, C.CHARSETID, C.NULL$,"
            "   MOD(C.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(C.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.COL$ AS OF SCN :j C ON"
            "     O.OBJ# = C.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_COL_OBJ(
            "SELECT"
            "   C.ROWID, C.OBJ#, C.COL#, C.SEGCOL#, C.INTCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.CHARSETFORM, C.CHARSETID, C.NULL$,"
            "   MOD(C.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(C.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.COL$ AS OF SCN :j C"
            " WHERE"
            "   C.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_DEFERRED_STG_USER(
            "SELECT"
            "   DS.ROWID, DS.OBJ#, MOD(DS.FLAGS_STG, 18446744073709551616) AS FLAGS_STG1,"
            "   MOD(TRUNC(DS.FLAGS_STG / 18446744073709551616), 18446744073709551616) AS FLAGS_STG2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.DEFERRED_STG$ AS OF SCN :j DS ON"
            "     O.OBJ# = DS.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_DEFERRED_STG_OBJ(
            "SELECT"
            "   DS.ROWID, DS.OBJ#, MOD(DS.FLAGS_STG, 18446744073709551616) AS FLAGS_STG1,"
            "   MOD(TRUNC(DS.FLAGS_STG / 18446744073709551616), 18446744073709551616) AS FLAGS_STG2"
            " FROM"
            "   SYS.DEFERRED_STG$ AS OF SCN :j DS"
            " WHERE"
            "   DS.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_ECOL_USER(
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, E.GUARD_ID"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.ECOL$ AS OF SCN :j E ON"
            "     O.OBJ# = E.TABOBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_ECOL_OBJ(
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, E.GUARD_ID"
            " FROM"
            "   SYS.ECOL$ AS OF SCN :j E"
            " WHERE"
            "   E.TABOBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_ECOL11_USER(
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, -1 AS GUARD_ID"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.ECOL$ AS OF SCN :j E ON"
            "     O.OBJ# = E.TABOBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_ECOL11_OBJ(
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, -1 AS GUARD_ID"
            " FROM"
            "   SYS.ECOL$ AS OF SCN :j E"
            " WHERE"
            "   E.TABOBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_LOB_USER(
            "SELECT"
            "   L.ROWID, L.OBJ#, L.COL#, L.INTCOL#, L.LOBJ#, L.TS#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.LOB$ AS OF SCN :j L ON"
            "     O.OBJ# = L.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_LOB_OBJ(
            "SELECT"
            "   L.ROWID, L.OBJ#, L.COL#, L.INTCOL#, L.LOBJ#, L.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :i L"
            " WHERE"
            "   L.OBJ# = :j");

    const char* ReplicatorOnline::SQL_GET_SYS_LOB_COMP_PART_USER(
            "SELECT"
            "   LCP.ROWID, LCP.PARTOBJ#, LCP.LOBJ#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.LOB$ AS OF SCN :j L ON"
            "     O.OBJ# = L.OBJ#"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :k LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " WHERE"
            "   O.OWNER# = :l");

    const char* ReplicatorOnline::SQL_GET_SYS_LOB_COMP_PART_OBJ(
            "SELECT"
            "   LCP.ROWID, LCP.PARTOBJ#, LCP.LOBJ#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :i L"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :j LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " WHERE"
            "   L.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_LOB_FRAG_USER(
            "SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.LOB$ AS OF SCN :j L ON"
            "     O.OBJ# = L.OBJ#"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :k LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :l LF ON"
            "     LCP.PARTOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   O.OWNER# = :m"
            " UNION ALL"
            " SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :n O"
            " JOIN"
            "   SYS.LOB$ AS OF SCN :o L ON"
            "     O.OBJ# = L.OBJ#"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :p LF ON"
            "     L.LOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   O.OWNER# = :q");

    const char* ReplicatorOnline::SQL_GET_SYS_LOB_FRAG_OBJ(
            "SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :i L"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :j LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :k LF ON"
            "     LCP.PARTOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   L.OBJ# = :l"
            " UNION ALL"
            " SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :m L"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :n LF ON"
            "     L.LOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   L.OBJ# = :o");

    const char* ReplicatorOnline::SQL_GET_SYS_OBJ_USER(
            "SELECT"
            "   O.ROWID, O.OWNER#, O.OBJ#, O.DATAOBJ#, O.NAME, O.TYPE#,"
            "   MOD(O.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(O.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " WHERE"
            "   O.OWNER# = :j");

    const char* ReplicatorOnline::SQL_GET_SYS_OBJ_NAME(
            "SELECT"
            "   O.ROWID, O.OWNER#, O.OBJ#, O.DATAOBJ#, O.NAME, O.TYPE#,"
            "   MOD(O.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(O.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " WHERE"
            "   O.OWNER# = :j AND REGEXP_LIKE(O.NAME, :k)");

    const char* ReplicatorOnline::SQL_GET_SYS_TAB_USER(
            "SELECT"
            "   T.ROWID, T.OBJ#, T.DATAOBJ#, T.CLUCOLS,"
            "   MOD(T.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(T.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2,"
            "   MOD(T.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(T.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.TAB$ AS OF SCN :j T ON"
            "     O.OBJ# = T.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TAB_OBJ(
            "SELECT"
            "   T.ROWID, T.OBJ#, T.DATAOBJ#, T.CLUCOLS,"
            "   MOD(T.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(T.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2,"
            "   MOD(T.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(T.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.TAB$ AS OF SCN :j T"
            " WHERE"
            "   T.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TABCOMPART_USER(
            "SELECT"
            "   TCP.ROWID, TCP.OBJ#, TCP.DATAOBJ#, TCP.BO#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.TABCOMPART$ AS OF SCN :j TCP ON"
            "     O.OBJ# = TCP.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TABCOMPART_OBJ(
            "SELECT"
            "   TCP.ROWID, TCP.OBJ#, TCP.DATAOBJ#, TCP.BO#"
            " FROM"
            "   SYS.TABCOMPART$ AS OF SCN :j TCP"
            " WHERE"
            "   TCP.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TABPART_USER(
            "SELECT"
            "   TP.ROWID, TP.OBJ#, TP.DATAOBJ#, TP.BO#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.TABPART$ AS OF SCN :j TP ON"
            "     O.OBJ# = TP.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TABPART_OBJ(
            "SELECT"
            "   TP.ROWID, TP.OBJ#, TP.DATAOBJ#, TP.BO#"
            " FROM"
            "   SYS.TABPART$ AS OF SCN :j TP"
            " WHERE"
            "   TP.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TABSUBPART_USER(
            "SELECT"
            "   TSP.ROWID, TSP.OBJ#, TSP.DATAOBJ#, TSP.POBJ#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.TABSUBPART$ AS OF SCN :j TSP ON"
            "     O.OBJ# = TSP.OBJ#"
            " WHERE"
            "   O.OWNER# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TABSUBPART_OBJ(
            "SELECT"
            "   TSP.ROWID, TSP.OBJ#, TSP.DATAOBJ#, TSP.POBJ#"
            " FROM"
            "   SYS.TABSUBPART$ AS OF SCN :j TSP"
            " WHERE"
            "   TSP.OBJ# = :k");

    const char* ReplicatorOnline::SQL_GET_SYS_TS(
            "SELECT"
            "   T.ROWID, T.TS#, T.NAME, T.BLOCKSIZE"
            " FROM"
            "   SYS.TS$ AS OF SCN :i T");

    const char* ReplicatorOnline::SQL_GET_SYS_USER(
            "SELECT"
            "   U.ROWID, U.USER#, U.NAME, MOD(U.SPARE1, 18446744073709551616) AS SPARE11,"
            "   MOD(TRUNC(U.SPARE1 / 18446744073709551616), 18446744073709551616) AS SPARE12"
            " FROM"
            "   SYS.USER$ AS OF SCN :i U"
            " WHERE"
            "   REGEXP_LIKE(U.NAME, :j)");

    const char* ReplicatorOnline::SQL_CHECK_CONNECTION(
            "SELECT 1 FROM DUAL");

    ReplicatorOnline::ReplicatorOnline(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                                       TransactionBuffer* newTransactionBuffer, const std::string& newAlias, const char* newDatabase, const char* newUser,
                                       const char* newPassword, const char* newConnectString, bool newKeepConnection) :
            Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, newAlias, newDatabase),
            standby(false),
            keepConnection(newKeepConnection) {

        env = new DatabaseEnvironment(ctx);
        env->initialize();
        conn = new DatabaseConnection(env, newUser, newPassword, newConnectString, false);
    }

    ReplicatorOnline::~ReplicatorOnline() {
        if (conn != nullptr) {
            delete conn;
            conn = nullptr;
        }

        if (env != nullptr) {
            delete env;
            env = nullptr;
        }
    }

    void ReplicatorOnline::loadDatabaseMetadata() {
        if (!checkConnection())
            return;

        typeScn currentScn;
        if (!DISABLE_CHECKS(DISABLE_CHECKS_GRANTS)) {
            checkTableForGrants("SYS.V_$ARCHIVED_LOG");
            checkTableForGrants("SYS.V_$DATABASE");
            checkTableForGrants("SYS.V_$DATABASE_INCARNATION");
            checkTableForGrants("SYS.V_$LOG");
            checkTableForGrants("SYS.V_$LOGFILE");
            checkTableForGrants("SYS.V_$PARAMETER");
            checkTableForGrants("SYS.V_$STANDBY_LOG");
            checkTableForGrants("SYS.V_$TRANSPORTABLE_PLATFORM");
        }

        updateOnlineRedoLogData();

        archReader = readerCreate(0);
        {
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_DATABASE_INFORMATION)
            stmt.createStatement(SQL_GET_DATABASE_INFORMATION);
            uint64_t logMode; stmt.defineUInt64(1, logMode);
            uint64_t supplementalLogMin; stmt.defineUInt64(2, supplementalLogMin);
            uint64_t suppLogDbPrimary; stmt.defineUInt64(3, suppLogDbPrimary);
            uint64_t suppLogDbAll; stmt.defineUInt64(4, suppLogDbAll);
            uint64_t bigEndian; stmt.defineUInt64(5, bigEndian);
            char banner[81]; stmt.defineString(6, banner, sizeof(banner));
            char context[81]; stmt.defineString(7, context, sizeof(context));
            stmt.defineUInt64(8, currentScn);

            if (stmt.executeQuery()) {
                if (logMode == 0) {
                    ERROR("HINT: run: SHUTDOWN IMMEDIATE;")
                    ERROR("HINT: run: STARTUP MOUNT;")
                    ERROR("HINT: run: ALTER DATABASE ARCHIVELOG;")
                    ERROR("HINT: run: ALTER DATABASE OPEN;")
                    throw RuntimeException("database not in ARCHIVELOG mode");
                }

                if (supplementalLogMin == 0) {
                    ERROR("HINT: run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;")
                    ERROR("HINT: run: ALTER SYSTEM ARCHIVE LOG CURRENT;")
                    throw RuntimeException("SUPPLEMENTAL_LOG_DATA_MIN missing");
                }

                if (bigEndian)
                    ctx->setBigEndian();

                metadata->suppLogDbPrimary = suppLogDbPrimary;
                metadata->suppLogDbAll = suppLogDbAll;
                metadata->context = context;

                // 12+
                metadata->conId = 0;
                if (memcmp(banner, "Oracle Database 11g", 19) != 0) {
                    ctx->version12 = true;
                    DatabaseStatement stmt2(conn);
                    TRACE(TRACE2_SQL, "SQL: " << SQL_GET_CON_INFO)
                    stmt2.createStatement(SQL_GET_CON_INFO);
                    typeConId conId; stmt2.defineInt16(1, conId);
                    char conNameChar[81]; stmt2.defineString(2, conNameChar, sizeof(conNameChar));

                    if (stmt2.executeQuery()) {
                        metadata->conId = conId;
                        metadata->conName = conNameChar;
                    }
                }

                INFO("version: " << std::dec << banner << ", context: " << metadata->context << ", resetlogs: " << std::dec << metadata->resetlogs <<
                        ", activation: " << metadata->activation << ", con_id: " << metadata->conId << ", con_name: " << metadata->conName)
            } else {
                throw RuntimeException("trying to read SYS.V_$DATABASE");
            }
        }

        if (!DISABLE_CHECKS(DISABLE_CHECKS_GRANTS) && !standby) {
            checkTableForGrantsFlashback("SYS.CCOL$", currentScn);
            checkTableForGrantsFlashback("SYS.CDEF$", currentScn);
            checkTableForGrantsFlashback("SYS.COL$", currentScn);
            checkTableForGrantsFlashback("SYS.DEFERRED_STG$", currentScn);
            checkTableForGrantsFlashback("SYS.ECOL$", currentScn);
            checkTableForGrantsFlashback("SYS.LOB$", currentScn);
            checkTableForGrantsFlashback("SYS.LOBCOMPPART$", currentScn);
            checkTableForGrantsFlashback("SYS.LOBFRAG$", currentScn);
            checkTableForGrantsFlashback("SYS.OBJ$", currentScn);
            checkTableForGrantsFlashback("SYS.TAB$", currentScn);
            checkTableForGrantsFlashback("SYS.TABCOMPART$", currentScn);
            checkTableForGrantsFlashback("SYS.TABPART$", currentScn);
            checkTableForGrantsFlashback("SYS.TABSUBPART$", currentScn);
            checkTableForGrantsFlashback("SYS.TS$", currentScn);
            checkTableForGrantsFlashback("SYS.USER$", currentScn);
        }

        metadata->dbRecoveryFileDest = getParameterValue("db_recovery_file_dest");
        if (metadata->dbRecoveryFileDest.length() > 0 && metadata->dbRecoveryFileDest.back() == '/') {
            while (metadata->dbRecoveryFileDest.length() > 0 && metadata->dbRecoveryFileDest.back() == '/')
                metadata->dbRecoveryFileDest.pop_back();
            WARNING("stripping trailing '/' from db_recovery_file_dest parameter; new value: " << metadata->dbRecoveryFileDest)
        }
        metadata->logArchiveDest = getParameterValue("log_archive_dest");
        if (metadata->logArchiveDest.length() > 0 && metadata->logArchiveDest.back() == '/') {
            while (metadata->logArchiveDest.length() > 0 && metadata->logArchiveDest.back() == '/')
                metadata->logArchiveDest.pop_back();
            WARNING("stripping trailing '/' from log_archive_dest parameter; new value: " << metadata->logArchiveDest)
        }
        metadata->dbBlockChecksum = getParameterValue("db_block_checksum");
        std::transform(metadata->dbBlockChecksum.begin(), metadata->dbBlockChecksum.end(), metadata->dbBlockChecksum.begin(), ::toupper);
        if (metadata->dbRecoveryFileDest.length() == 0)
            metadata->logArchiveFormat = getParameterValue("log_archive_format");
        metadata->nlsCharacterSet = getPropertyValue("NLS_CHARACTERSET");
        metadata->nlsNcharCharacterSet = getPropertyValue("NLS_NCHAR_CHARACTERSET");

        INFO("loading character mapping for " << metadata->nlsCharacterSet)
        INFO("loading character mapping for " << metadata->nlsNcharCharacterSet)
        metadata->setNlsCharset(metadata->nlsCharacterSet, metadata->nlsNcharCharacterSet);
        metadata->onlineData = true;
    }

    void ReplicatorOnline::positionReader() {
        // Position by time
        if (metadata->startTime.length() > 0) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw RuntimeException("can't position by time for standby database");

            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SCN_FROM_TIME)
            TRACE(TRACE2_SQL, "PARAM1: " << metadata->startTime)
            stmt.createStatement(SQL_GET_SCN_FROM_TIME);

            std::ostringstream ss;
            stmt.bindString(1, metadata->startTime);
            typeScn firstDataScn; stmt.defineUInt64(1, firstDataScn);

            if (!stmt.executeQuery())
                throw RuntimeException("can't find scn for: " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;

        } else if (metadata->startTimeRel > 0) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw RuntimeException("can't position by relative time for standby database");

            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SCN_FROM_TIME_RELATIVE)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << metadata->startTimeRel)
            stmt.createStatement(SQL_GET_SCN_FROM_TIME_RELATIVE);
            stmt.bindInt64(1, metadata->startTimeRel);
            typeScn firstDataScn; stmt.defineUInt64(1, firstDataScn);

            if (!stmt.executeQuery())
                throw RuntimeException("can't find scn for " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;

        // NOW
        } else if (metadata->firstDataScn == ZERO_SCN || metadata->firstDataScn == 0) {
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_DATABASE_SCN)
            stmt.createStatement(SQL_GET_DATABASE_SCN);
            typeScn firstDataScn; stmt.defineUInt64(1, firstDataScn);

            if (!stmt.executeQuery())
                throw RuntimeException("can't find database current scn");
            metadata->firstDataScn = firstDataScn;
        }

        // First sequence
        if (metadata->startSequence != ZERO_SEQ) {
            metadata->setSeqOffset(metadata->startSequence, 0);
            if (metadata->firstDataScn == ZERO_SCN)
                metadata->firstDataScn = 0;
        } else {
            DatabaseStatement stmt(conn);
            if (standby) {
                TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SEQUENCE_FROM_SCN_STANDBY)
                TRACE(TRACE2_SQL, "PARAM1: " << std::dec << metadata->firstDataScn)
                TRACE(TRACE2_SQL, "PARAM2: " << std::dec << metadata->firstDataScn)
                TRACE(TRACE2_SQL, "PARAM3: " << std::dec << metadata->resetlogs)
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
                stmt.bindUInt64(1, metadata->firstDataScn);
                stmt.bindUInt64(2, metadata->firstDataScn);
                stmt.bindUInt32(3, metadata->resetlogs);
            } else {
                TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SEQUENCE_FROM_SCN)
                TRACE(TRACE2_SQL, "PARAM1: " << std::dec << metadata->firstDataScn)
                TRACE(TRACE2_SQL, "PARAM2: " << std::dec << metadata->firstDataScn)
                TRACE(TRACE2_SQL, "PARAM3: " << std::dec << metadata->resetlogs)
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN);
                stmt.bindUInt64(1, metadata->firstDataScn);
                stmt.bindUInt64(2, metadata->firstDataScn);
                stmt.bindUInt32(3, metadata->resetlogs);
            }
            typeSeq sequence; stmt.defineUInt32(1, sequence);

            if (!stmt.executeQuery())
                throw RuntimeException("getting database sequence for scn: " + std::to_string(metadata->firstDataScn));

            metadata->setSeqOffset(sequence, 0);
            INFO("starting sequence not found - starting with new batch with seq: " << std::dec << metadata->sequence)
        }

        if (metadata->firstDataScn == ZERO_SCN)
            throw RuntimeException("getting database scn");
    }

    bool ReplicatorOnline::checkConnection() {
        if (!conn->connected) {
            INFO("connecting to Oracle instance of " << database << " to " << conn->connectString)
        }

        while (!ctx->softShutdown) {
            if (!conn->connected) {
                try {
                    conn->connect();
                } catch (RuntimeException& ex) {
                }
            }

            if (conn->connected) {
                try {
                    DatabaseStatement stmt(conn);
                    TRACE(TRACE2_SQL, "SQL: " << SQL_CHECK_CONNECTION)
                    stmt.createStatement(SQL_CHECK_CONNECTION);
                    uint64_t dummy; stmt.defineUInt64(1, dummy);

                    stmt.executeQuery();
                } catch (RuntimeException& ex) {
                    conn->disconnect();
                    usleep(ctx->redoReadSleepUs);
                    INFO("re-connecting to Oracle instance of " << database << " to " << conn->connectString)
                    continue;
                }

                return true;
            }

            DEBUG("cannot connect to database, retry in 5 sec.")
            sleep(5);
        }

        return false;
    }

    void ReplicatorOnline::goStandby() {
        if (!keepConnection)
            conn->disconnect();
    }

    std::string ReplicatorOnline::getParameterValue(const char* parameter) const {
        char value[VPARAMETER_LENGTH + 1];
        DatabaseStatement stmt(conn);
        TRACE(TRACE2_SQL, "SQL: " << SQL_GET_PARAMETER)
        TRACE(TRACE2_SQL, "PARAM1: " << parameter)
        stmt.createStatement(SQL_GET_PARAMETER);
        stmt.bindString(1, parameter);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery())
            return value;

        // No value found
        throw RuntimeException(std::string("can't get parameter value for ") + parameter);
    }

    std::string ReplicatorOnline::getPropertyValue(const char* property) const {
        char value[VPROPERTY_LENGTH + 1];
        DatabaseStatement stmt(conn);
        TRACE(TRACE2_SQL, "SQL: " << SQL_GET_PROPERTY)
        TRACE(TRACE2_SQL, "PARAM1: " << property)
        stmt.createStatement(SQL_GET_PROPERTY);
        stmt.bindString(1, property);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery())
            return value;

        // No value found
        throw RuntimeException(std::string("can't get proprty value for ") + property);
    }

    void ReplicatorOnline::checkTableForGrants(const char* tableName) {
        try {
            std::string query("SELECT 1 FROM " + std::string(tableName) + " WHERE 0 = 1");
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << query)
            stmt.createStatement(query.c_str());
            uint64_t dummy; stmt.defineUInt64(1, dummy);

            stmt.executeQuery();
        } catch (RuntimeException& ex) {
            if (metadata->conId > 0) {
                ERROR("HINT: run: ALTER SESSION SET CONTAINER = " << metadata->conName << ";")
                ERROR("HINT: run: GRANT SELECT ON " << tableName << " TO " << conn->user << ";")
            } else {
                ERROR("HINT: run: GRANT SELECT ON " << tableName << " TO " << conn->user << ";")
            }
            throw RuntimeException("grants missing");
        }
    }

    void ReplicatorOnline::checkTableForGrantsFlashback(const char* tableName, typeScn scn) {
        try {
            std::string query("SELECT 1 FROM " + std::string(tableName) + " AS OF SCN " + std::to_string(scn) + " WHERE 0 = 1");
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << query)
            stmt.createStatement(query.c_str());
            uint64_t dummy; stmt.defineUInt64(1, dummy);

            stmt.executeQuery();
        } catch (RuntimeException& ex) {
            if (metadata->conId > 0) {
                ERROR("HINT: run: ALTER SESSION SET CONTAINER = " << metadata->conName << ";")
                ERROR("HINT: run: GRANT SELECT, FLASHBACK ON " << tableName << " TO " << conn->user << ";")
            } else {
                ERROR("HINT: run: GRANT SELECT, FLASHBACK ON " << tableName << " TO " << conn->user << ";")
            }
            throw RuntimeException("grants missing");
        }
    }

    void ReplicatorOnline::verifySchema(typeScn currentScn) {
        if (!FLAG(REDO_FLAGS_VERIFY_SCHEMA))
            return;
        if (!checkConnection())
            return;

        INFO("verifying schema for SCN: " << std::dec << currentScn)

        Schema otherSchema(ctx, metadata->locales);
        try {
            readSystemDictionariesMetadata(&otherSchema, currentScn);
            for (SchemaElement* element : metadata->schemaElements)
                readSystemDictionaries(&otherSchema, currentScn, element->owner, element->table, element->options);
            std::string errMsg;
            bool result = metadata->schema->compare(&otherSchema, errMsg);
            if (result) {
                WARNING("Schema incorrect: " << errMsg)
            }
        } catch (RuntimeException& e) {
            WARNING("aborting compare")
        } catch (std::bad_alloc& ex) {
            ERROR("memory allocation failed: " << ex.what())
        }
    }

    void ReplicatorOnline::createSchema() {
        if (!checkConnection())
            return;

        INFO("reading dictionaries for scn: " << std::dec << metadata->firstDataScn)

        std::set <std::string> msgs;
        {
            std::unique_lock<std::mutex> lck(metadata->mtx);
            metadata->schema->purge();
            metadata->schema->scn = metadata->firstDataScn;
            metadata->firstSchemaScn = metadata->firstDataScn;
            readSystemDictionariesMetadata(metadata->schema, metadata->firstDataScn);

            for (SchemaElement* element : metadata->schemaElements)
                createSchemaForTable(metadata->firstDataScn, element->owner, element->table, element->keys,
                                     element->keysStr, element->options, msgs);
            metadata->schema->resetTouched();
            metadata->allowedCheckpoints = true;
        }

        for (auto msg: msgs) {
            INFO("- found: " << msg);
        }
    }

    void ReplicatorOnline::readSystemDictionariesMetadata(Schema* schema, typeScn targetScn) {
        DEBUG("- reading metadata")

        try {
            DatabaseStatement stmtTs(conn);

            // Reading SYS.TS$
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TS)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            stmtTs.createStatement(SQL_GET_SYS_TS);
            stmtTs.bindUInt64(1, targetScn);
            char tsRowid[19]; stmtTs.defineString(1, tsRowid, sizeof(tsRowid));
            typeTs tsTs; stmtTs.defineUInt32(2, tsTs);
            char tsName[129]; stmtTs.defineString(3, tsName, sizeof(tsName));
            uint32_t tsBlockSize; stmtTs.defineUInt32(4, tsBlockSize);

            int64_t retTs = stmtTs.executeQuery();
            while (retTs) {
                schema->dictSysTsAdd(tsRowid, tsTs, tsName, tsBlockSize);
                retTs = stmtTs.next();
            }
        } catch (RuntimeException& ex) {
            ERROR(ex.msg)
            throw RuntimeException("Error reading metadata from flashback, try some later scn for start");
        }
    }

    void ReplicatorOnline::readSystemDictionariesDetails(Schema* schema, typeScn targetScn, typeUser user, typeObj obj) {
        DEBUG("read dictionaries for user: " << std::dec << user << ", object: " << obj)

        // Reading SYS.CCOL$
        DatabaseStatement stmtCCol(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_CCOL_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtCCol.createStatement(SQL_GET_SYS_CCOL_OBJ);
            stmtCCol.bindUInt64(1, targetScn);
            stmtCCol.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_CCOL_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtCCol.createStatement(SQL_GET_SYS_CCOL_USER);
            stmtCCol.bindUInt64(1, targetScn);
            stmtCCol.bindUInt64(2, targetScn);
            stmtCCol.bindUInt32(3, user);
        }

        char ccolRowid[19]; stmtCCol.defineString(1, ccolRowid, sizeof(ccolRowid));
        typeCon ccolCon; stmtCCol.defineUInt32(2, ccolCon);
        typeCol ccolIntCol; stmtCCol.defineInt16(3, ccolIntCol);
        typeObj ccolObj; stmtCCol.defineUInt32(4, ccolObj);
        uint64_t ccolSpare11 = 0; stmtCCol.defineUInt64(5, ccolSpare11);
        uint64_t ccolSpare12 = 0; stmtCCol.defineUInt64(6, ccolSpare12);

        int64_t ccolRet = stmtCCol.executeQuery();
        while (ccolRet) {
            schema->dictSysCColAdd(ccolRowid, ccolCon, ccolIntCol, ccolObj, ccolSpare11, ccolSpare12);
            ccolSpare11 = 0;
            ccolSpare12 = 0;
            ccolRet = stmtCCol.next();
        }

        // Reading SYS.CDEF$
        DatabaseStatement stmtCDef(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_CDEF_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtCDef.createStatement(SQL_GET_SYS_CDEF_OBJ);
            stmtCDef.bindUInt64(1, targetScn);
            stmtCDef.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_CDEF_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtCDef.createStatement(SQL_GET_SYS_CDEF_USER);
            stmtCDef.bindUInt64(1, targetScn);
            stmtCDef.bindUInt64(2, targetScn);
            stmtCDef.bindUInt32(3, user);
        }

        char cdefRowid[19]; stmtCDef.defineString(1, cdefRowid, sizeof(cdefRowid));
        typeCon cdefCon; stmtCDef.defineUInt32(2, cdefCon);
        typeObj cdefObj; stmtCDef.defineUInt32(3, cdefObj);
        uint64_t cdefType; stmtCDef.defineUInt64(4, cdefType);

        int64_t cdefRet = stmtCDef.executeQuery();
        while (cdefRet) {
            schema->dictSysCDefAdd(cdefRowid, cdefCon, cdefObj, cdefType);
            cdefRet = stmtCDef.next();
        }

        // Reading SYS.COL$
        DatabaseStatement stmtCol(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_COL_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtCol.createStatement(SQL_GET_SYS_COL_OBJ);
            stmtCol.bindUInt64(1, targetScn);
            stmtCol.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_COL_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtCol.createStatement(SQL_GET_SYS_COL_USER);
            stmtCol.bindUInt64(1, targetScn);
            stmtCol.bindUInt64(2, targetScn);
            stmtCol.bindUInt32(3, user);
        }

        char colRowid[19]; stmtCol.defineString(1, colRowid, sizeof(colRowid));
        typeObj colObj; stmtCol.defineUInt32(2, colObj);
        typeCol colCol; stmtCol.defineInt16(3, colCol);
        typeCol colSegCol; stmtCol.defineInt16(4, colSegCol);
        typeCol colIntCol; stmtCol.defineInt16(5, colIntCol);
        char colName[129]; stmtCol.defineString(6, colName, sizeof(colName));
        uint64_t colType; stmtCol.defineUInt64(7, colType);
        uint64_t colLength; stmtCol.defineUInt64(8, colLength);
        int64_t colPrecision = -1; stmtCol.defineInt64(9, colPrecision);
        int64_t colScale = -1; stmtCol.defineInt64(10, colScale);
        uint64_t colCharsetForm = 0; stmtCol.defineUInt64(11, colCharsetForm);
        uint64_t colCharsetId = 0; stmtCol.defineUInt64(12, colCharsetId);
        int64_t colNull; stmtCol.defineInt64(13, colNull);
        uint64_t colProperty1; stmtCol.defineUInt64(14, colProperty1);
        uint64_t colProperty2; stmtCol.defineUInt64(15, colProperty2);

        int64_t colRet = stmtCol.executeQuery();
        while (colRet) {
            schema->dictSysColAdd(colRowid, colObj, colCol, colSegCol, colIntCol, colName, colType, colLength,
                                  colPrecision, colScale, colCharsetForm, colCharsetId, colNull, colProperty1,
                                  colProperty2);
            colPrecision = -1;
            colScale = -1;
            colCharsetForm = 0;
            colCharsetId = 0;
            colRet = stmtCol.next();
        }

        // Reading SYS.DEFERRED_STG$
        DatabaseStatement stmtDeferredStg(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_DEFERRED_STG_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtDeferredStg.createStatement(SQL_GET_SYS_DEFERRED_STG_OBJ);
            stmtDeferredStg.bindUInt64(1, targetScn);
            stmtDeferredStg.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_DEFERRED_STG_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtDeferredStg.createStatement(SQL_GET_SYS_DEFERRED_STG_USER);
            stmtDeferredStg.bindUInt64(1, targetScn);
            stmtDeferredStg.bindUInt64(2, targetScn);
            stmtDeferredStg.bindUInt32(3, user);
        }

        char deferredStgRowid[19]; stmtDeferredStg.defineString(1, deferredStgRowid, sizeof(deferredStgRowid));
        typeObj deferredStgObj; stmtDeferredStg.defineUInt32(2, deferredStgObj);
        uint64_t deferredStgFlagsStg1 = 0; stmtDeferredStg.defineUInt64(3, deferredStgFlagsStg1);
        uint64_t deferredStgFlagsStg2 = 0; stmtDeferredStg.defineUInt64(4, deferredStgFlagsStg2);

        int64_t deferredStgRet = stmtDeferredStg.executeQuery();
        while (deferredStgRet) {
            schema->dictSysDeferredStgAdd(deferredStgRowid, deferredStgObj, deferredStgFlagsStg1, deferredStgFlagsStg2);
            deferredStgFlagsStg1 = 0;
            deferredStgFlagsStg2 = 0;
            deferredStgRet = stmtDeferredStg.next();
        }

        // Reading SYS.ECOL$
        DatabaseStatement stmtECol(conn);
        if (ctx->version12) {
            if (obj != 0) {
                TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_ECOL_OBJ)
                TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
                TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
                stmtECol.createStatement(SQL_GET_SYS_ECOL_OBJ);
                stmtECol.bindUInt64(1, targetScn);
                stmtECol.bindUInt32(2, obj);
            } else {
                TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_ECOL_USER)
                TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
                TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
                TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
                stmtECol.createStatement(SQL_GET_SYS_ECOL_USER);
                stmtECol.bindUInt64(1, targetScn);
                stmtECol.bindUInt64(2, targetScn);
                stmtECol.bindUInt32(3, user);
            }
        } else {
            if (obj != 0) {
                TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_ECOL11_OBJ)
                TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
                TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
                stmtECol.createStatement(SQL_GET_SYS_ECOL11_OBJ);
                stmtECol.bindUInt64(1, targetScn);
                stmtECol.bindUInt32(2, obj);
            } else {
                TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_ECOL11_USER)
                TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
                TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
                TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
                stmtECol.createStatement(SQL_GET_SYS_ECOL11_USER);
                stmtECol.bindUInt64(1, targetScn);
                stmtECol.bindUInt64(2, targetScn);
                stmtECol.bindUInt32(3, user);
            }
        }

        char ecolRowid[19]; stmtECol.defineString(1, ecolRowid, sizeof(ecolRowid));
        typeObj ecolTabObj; stmtECol.defineUInt32(2, ecolTabObj);
        typeCol ecolColNum = 0; stmtECol.defineInt16(3, ecolColNum);
        typeCol ecolGuardId = -1; stmtECol.defineInt16(4, ecolGuardId);

        int64_t ecolRet = stmtECol.executeQuery();
        while (ecolRet) {
            schema->dictSysEColAdd(ecolRowid, ecolTabObj, ecolColNum, ecolGuardId);
            ecolColNum = 0;
            ecolGuardId = -1;
            ecolRet = stmtECol.next();
        }

        // Reading SYS.LOB$
        DatabaseStatement stmtLob(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_LOB_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtLob.createStatement(SQL_GET_SYS_LOB_OBJ);
            stmtLob.bindUInt64(1, targetScn);
            stmtLob.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_LOB_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtLob.createStatement(SQL_GET_SYS_LOB_USER);
            stmtLob.bindUInt64(1, targetScn);
            stmtLob.bindUInt64(2, targetScn);
            stmtLob.bindUInt32(3, user);
        }

        char lobRowid[19]; stmtLob.defineString(1, lobRowid, sizeof(lobRowid));
        typeObj lobObj; stmtLob.defineUInt32(2, lobObj);
        typeCol lobCol = 0; stmtLob.defineInt16(3, lobCol);
        typeCol lobIntCol = 0; stmtLob.defineInt16(4, lobIntCol);
        typeObj lobLObj; stmtLob.defineUInt32(5, lobLObj);
        typeTs lobTs; stmtLob.defineUInt32(6, lobTs);

        int64_t lobRet = stmtLob.executeQuery();
        while (lobRet) {
            schema->dictSysLobAdd(lobRowid, lobObj, lobCol, lobIntCol, lobLObj, lobTs);
            lobRet = stmtLob.next();
        }

        // Reading SYS.LOBCOMPPART$
        DatabaseStatement stmtLobCompPart(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_LOB_COMP_PART_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << obj)
            stmtLobCompPart.createStatement(SQL_GET_SYS_LOB_COMP_PART_OBJ);
            stmtLobCompPart.bindUInt64(1, targetScn);
            stmtLobCompPart.bindUInt64(2, targetScn);
            stmtLobCompPart.bindUInt32(3, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_LOB_COMP_PART_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM4: " << std::dec << user)
            stmtLobCompPart.createStatement(SQL_GET_SYS_LOB_COMP_PART_USER);
            stmtLobCompPart.bindUInt64(1, targetScn);
            stmtLobCompPart.bindUInt64(2, targetScn);
            stmtLobCompPart.bindUInt64(3, targetScn);
            stmtLobCompPart.bindUInt32(4, user);
        }

        char lobCompPartRowid[19]; stmtLobCompPart.defineString(1, lobCompPartRowid, sizeof(lobCompPartRowid));
        typeObj lobCompPartPartObj; stmtLobCompPart.defineUInt32(2, lobCompPartPartObj);
        typeObj lobCompPartLObj; stmtLobCompPart.defineUInt32(3, lobCompPartLObj);

        int64_t lobCompPartRet = stmtLobCompPart.executeQuery();
        while (lobCompPartRet) {
            schema->dictSysLobCompPartAdd(lobCompPartRowid, lobCompPartPartObj, lobCompPartLObj);
            lobCompPartRet = stmtLobCompPart.next();
        }

        // Reading SYS.LOBFRAG$
        DatabaseStatement stmtLobFrag(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_LOB_FRAG_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM4: " << std::dec << obj)
            TRACE(TRACE2_SQL, "PARAM5: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM6: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM7: " << std::dec << obj)
            stmtLobFrag.createStatement(SQL_GET_SYS_LOB_FRAG_OBJ);
            stmtLobFrag.bindUInt64(1, targetScn);
            stmtLobFrag.bindUInt64(2, targetScn);
            stmtLobFrag.bindUInt64(3, targetScn);
            stmtLobFrag.bindUInt32(4, obj);
            stmtLobFrag.bindUInt64(5, targetScn);
            stmtLobFrag.bindUInt64(6, targetScn);
            stmtLobFrag.bindUInt32(7, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_LOB_FRAG_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM4: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM5: " << std::dec << user)
            TRACE(TRACE2_SQL, "PARAM6: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM7: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM8: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM9: " << std::dec << user)
            stmtLobFrag.createStatement(SQL_GET_SYS_LOB_FRAG_USER);
            stmtLobFrag.bindUInt64(1, targetScn);
            stmtLobFrag.bindUInt64(2, targetScn);
            stmtLobFrag.bindUInt64(3, targetScn);
            stmtLobFrag.bindUInt64(4, targetScn);
            stmtLobFrag.bindUInt32(5, user);
            stmtLobFrag.bindUInt64(6, targetScn);
            stmtLobFrag.bindUInt64(7, targetScn);
            stmtLobFrag.bindUInt64(8, targetScn);
            stmtLobFrag.bindUInt32(9, user);
        }

        char lobFragRowid[19]; stmtLobFrag.defineString(1, lobFragRowid, sizeof(lobFragRowid));
        typeObj lobFragFragObj; stmtLobFrag.defineUInt32(2, lobFragFragObj);
        typeObj lobFragParentObj; stmtLobFrag.defineUInt32(3, lobFragParentObj);
        typeTs lobFragTs; stmtLobFrag.defineUInt32(4, lobFragTs);

        int64_t lobFragRet = stmtLobFrag.executeQuery();
        while (lobFragRet) {
            schema->dictSysLobFragAdd(lobFragRowid, lobFragFragObj, lobFragParentObj, lobFragTs);
            lobFragRet = stmtLobFrag.next();
        }

        // Reading SYS.TAB$
        DatabaseStatement stmtTab(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TAB_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtTab.createStatement(SQL_GET_SYS_TAB_OBJ);
            stmtTab.bindUInt64(1, targetScn);
            stmtTab.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TAB_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtTab.createStatement(SQL_GET_SYS_TAB_USER);
            stmtTab.bindUInt64(1, targetScn);
            stmtTab.bindUInt64(2, targetScn);
            stmtTab.bindUInt32(3, user);
        }

        char tabRowid[19]; stmtTab.defineString(1, tabRowid, sizeof(tabRowid));
        typeObj tabObj; stmtTab.defineUInt32(2, tabObj);
        typeDataObj tabDataObj = 0; stmtTab.defineUInt32(3, tabDataObj);
        typeCol tabCluCols = 0; stmtTab.defineInt16(4, tabCluCols);
        uint64_t tabFlags1; stmtTab.defineUInt64(5, tabFlags1);
        uint64_t tabFlags2; stmtTab.defineUInt64(6, tabFlags2);
        uint64_t tabProperty1; stmtTab.defineUInt64(7, tabProperty1);
        uint64_t tabProperty2; stmtTab.defineUInt64(8, tabProperty2);

        int64_t tabRet = stmtTab.executeQuery();
        while (tabRet) {
            schema->dictSysTabAdd(tabRowid, tabObj, tabDataObj, tabCluCols, tabFlags1, tabFlags2,
                                  tabProperty1, tabProperty2);
            tabDataObj = 0;
            tabCluCols = 0;
            tabRet = stmtTab.next();
        }

        // Reading SYS.TABCOMPART$
        DatabaseStatement stmtTabComPart(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TABCOMPART_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtTabComPart.createStatement(SQL_GET_SYS_TABCOMPART_OBJ);
            stmtTabComPart.bindUInt64(1, targetScn);
            stmtTabComPart.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TABCOMPART_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtTabComPart.createStatement(SQL_GET_SYS_TABCOMPART_USER);
            stmtTabComPart.bindUInt64(1, targetScn);
            stmtTabComPart.bindUInt64(2, targetScn);
            stmtTabComPart.bindUInt32(3, user);
        }

        char tabComPartRowid[19]; stmtTabComPart.defineString(1, tabComPartRowid, sizeof(tabComPartRowid));
        typeObj tabComPartObj; stmtTabComPart.defineUInt32(2, tabComPartObj);
        typeDataObj tabComPartDataObj = 0; stmtTabComPart.defineUInt32(3, tabComPartDataObj);
        typeObj tabComPartBo; stmtTabComPart.defineUInt32(4, tabComPartBo);

        int64_t tabComPartRet = stmtTabComPart.executeQuery();
        while (tabComPartRet) {
            schema->dictSysTabComPartAdd(tabComPartRowid, tabComPartObj, tabComPartDataObj, tabComPartBo);
            tabComPartDataObj = 0;
            tabComPartRet = stmtTabComPart.next();
        }

        // Reading SYS.TABPART$
        DatabaseStatement stmtTabPart(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TABPART_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtTabPart.createStatement(SQL_GET_SYS_TABPART_OBJ);
            stmtTabPart.bindUInt64(1, targetScn);
            stmtTabPart.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TABPART_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtTabPart.createStatement(SQL_GET_SYS_TABPART_USER);
            stmtTabPart.bindUInt64(1, targetScn);
            stmtTabPart.bindUInt64(2, targetScn);
            stmtTabPart.bindUInt32(3, user);
        }

        char tabPartRowid[19]; stmtTabPart.defineString(1, tabPartRowid, sizeof(tabPartRowid));
        typeObj tabPartObj; stmtTabPart.defineUInt32(2, tabPartObj);
        typeDataObj tabPartDataObj = 0; stmtTabPart.defineUInt32(3, tabPartDataObj);
        typeObj tabPartBo; stmtTabPart.defineUInt32(4, tabPartBo);

        int64_t tabPartRet = stmtTabPart.executeQuery();
        while (tabPartRet) {
            schema->dictSysTabPartAdd(tabPartRowid, tabPartObj, tabPartDataObj, tabPartBo);
            tabPartDataObj = 0;
            tabPartRet = stmtTabPart.next();
        }

        // Reading SYS.TABSUBPART$
        DatabaseStatement stmtTabSubPart(conn);
        if (obj != 0) {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TABSUBPART_OBJ)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << obj)
            stmtTabSubPart.createStatement(SQL_GET_SYS_TABSUBPART_OBJ);
            stmtTabSubPart.bindUInt64(1, targetScn);
            stmtTabSubPart.bindUInt32(2, obj);
        } else {
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_TABSUBPART_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM3: " << std::dec << user)
            stmtTabSubPart.createStatement(SQL_GET_SYS_TABSUBPART_USER);
            stmtTabSubPart.bindUInt64(1, targetScn);
            stmtTabSubPart.bindUInt64(2, targetScn);
            stmtTabSubPart.bindUInt32(3, user);
        }

        char tabSubPartRowid[19]; stmtTabSubPart.defineString(1, tabSubPartRowid, sizeof(tabSubPartRowid));
        typeObj tabSubPartObj; stmtTabSubPart.defineUInt32(2, tabSubPartObj);
        typeDataObj tabSubPartDataObj = 0; stmtTabSubPart.defineUInt32(3, tabSubPartDataObj);
        typeObj tabSubPartPobj; stmtTabSubPart.defineUInt32(4, tabSubPartPobj);

        int64_t tabSubPartRet = stmtTabSubPart.executeQuery();
        while (tabSubPartRet) {
            schema->dictSysTabSubPartAdd(tabSubPartRowid, tabSubPartObj, tabSubPartDataObj, tabSubPartPobj);
            tabSubPartDataObj = 0;
            tabSubPartRet = stmtTabSubPart.next();
        }
    }

    void ReplicatorOnline::readSystemDictionaries(Schema* schema, typeScn targetScn, const std::string& owner, const std::string& table, typeOptions options) {
        std::string ownerRegexp("^" + owner + "$");
        std::string tableRegexp("^" + table + "$");
        bool single = ((options & OPTIONS_SYSTEM_TABLE) != 0);
        DEBUG("read dictionaries for owner: " << owner << ", table: " << table << ", options: " << std::dec << static_cast<uint64_t>(options))

        try {
            DatabaseStatement stmtUser(conn);

            // Reading SYS.USER$
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_USER)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
            TRACE(TRACE2_SQL, "PARAM2: " << ownerRegexp)
            stmtUser.createStatement(SQL_GET_SYS_USER);
            stmtUser.bindUInt64(1, targetScn);
            stmtUser.bindString(2, ownerRegexp);
            char userRowid[19]; stmtUser.defineString(1, userRowid, sizeof(userRowid));
            typeUser userUser; stmtUser.defineUInt32(2, userUser);
            char userName[129]; stmtUser.defineString(3, userName, sizeof(userName));
            uint64_t userSpare11 = 0; stmtUser.defineUInt64(4, userSpare11);
            uint64_t userSpare12 = 0; stmtUser.defineUInt64(5, userSpare12);

            int64_t retUser = stmtUser.executeQuery();
            while (retUser) {
                if (!schema->dictSysUserAdd(userRowid, userUser, userName, userSpare11, userSpare12,
                        (options & OPTIONS_SYSTEM_TABLE) != 0, true)) {
                    userSpare11 = 0;
                    userSpare12 = 0;
                    retUser = stmtUser.next();
                    continue;
                }

                DatabaseStatement stmtObj(conn);
                // Reading SYS.OBJ$
                if ((options & OPTIONS_SYSTEM_TABLE) == 0) {
                    TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_OBJ_USER)
                    TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
                    TRACE(TRACE2_SQL, "PARAM2: " << std::dec << userUser)
                    stmtObj.createStatement(SQL_GET_SYS_OBJ_USER);
                    stmtObj.bindUInt64(1, targetScn);
                    stmtObj.bindUInt32(2, userUser);
                } else {
                    TRACE(TRACE2_SQL, "SQL: " << SQL_GET_SYS_OBJ_NAME)
                    TRACE(TRACE2_SQL, "PARAM1: " << std::dec << targetScn)
                    TRACE(TRACE2_SQL, "PARAM2: " << std::dec << userUser)
                    TRACE(TRACE2_SQL, "PARAM3: " << table)
                    stmtObj.createStatement(SQL_GET_SYS_OBJ_NAME);
                    stmtObj.bindUInt64(1, targetScn);
                    stmtObj.bindUInt32(2, userUser);
                    stmtObj.bindString(3, tableRegexp);
                }

                char objRowid[19]; stmtObj.defineString(1, objRowid, sizeof(objRowid));
                typeUser objOwner; stmtObj.defineUInt32(2, objOwner);
                typeObj objObj; stmtObj.defineUInt32(3, objObj);
                typeDataObj objDataObj = 0; stmtObj.defineUInt32(4, objDataObj);
                char objName[129]; stmtObj.defineString(5, objName, sizeof(objName));
                uint64_t objType = 0; stmtObj.defineUInt64(6, objType);
                uint64_t objFlags1; stmtObj.defineUInt64(7, objFlags1);
                uint64_t objFlags2; stmtObj.defineUInt64(8, objFlags2);

                int64_t objRet = stmtObj.executeQuery();
                while (objRet) {
                    if (schema->dictSysObjAdd(objRowid, objOwner, objObj, objDataObj, objType, objName, objFlags1,
                                              objFlags2, single)) {
                        if (single)
                            readSystemDictionariesDetails(schema, targetScn, userUser, objObj);
                    }
                    objDataObj = 0;
                    objFlags1 = 0;
                    objFlags2 = 0;
                    objRet = stmtObj.next();
                }

                if (!single)
                    readSystemDictionariesDetails(schema, targetScn, userUser, 0);

                userSpare11 = 0;
                userSpare12 = 0;
                retUser = stmtUser.next();
            }
        } catch (RuntimeException& ex) {
            ERROR(ex.msg)
            throw RuntimeException("Error reading schema from flashback, try some later scn for start");
        }
    }

    void ReplicatorOnline::createSchemaForTable(typeScn targetScn, const std::string& owner, const std::string& table, const std::vector<std::string>& keys,
                                                const std::string& keysStr, typeOptions options, std::set <std::string> &msgs) {
        DEBUG("- creating table schema for owner: " << owner << " table: " << table << " options: " << static_cast<uint64_t>(options))

        readSystemDictionaries(metadata->schema, targetScn, owner, table, options);

        metadata->schema->buildMaps(owner, table, keys, keysStr, options, msgs, metadata->suppLogDbPrimary,
                                    metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                    metadata->defaultCharacterNcharMapId);

        if ((options & OPTIONS_SYSTEM_TABLE) == 0 && metadata->users.find(owner) == metadata->users.end())
            metadata->users.insert(owner);
    }

    void ReplicatorOnline::updateOnlineRedoLogData() {
        if (!checkConnection())
            return;

        // Reload incarnation ctx
        typeResetlogs oldResetlogs = metadata->resetlogs;
        for (OracleIncarnation* oi : metadata->oracleIncarnations)
            delete oi;
        metadata->oracleIncarnations.clear();
        metadata->oracleIncarnationCurrent = nullptr;

        {
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_DATABASE_ROLE)
            stmt.createStatement(SQL_GET_DATABASE_ROLE);
            char databaseRole[129]; stmt.defineString(1, databaseRole, sizeof(databaseRole));

            if (stmt.executeQuery()) {
                std::string roleStr(databaseRole);
                if (roleStr == "PRIMARY") {
                    if (standby) {
                        standby = false;
                        INFO("changed database role to: " << roleStr)
                    }
                } else if (roleStr == "PHYSICAL STANDBY") {
                    if (!standby) {
                        standby = true;
                        INFO("changed database role to: " << roleStr)
                    }
                } else {
                    throw RuntimeException("unknown database role: " + roleStr);
                }
            }
        }

        {
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_DATABASE_INCARNATION)
            stmt.createStatement(SQL_GET_DATABASE_INCARNATION);
            uint32_t incarnation; stmt.defineUInt32(1, incarnation);
            typeScn resetlogsScn; stmt.defineUInt64(2, resetlogsScn);
            typeScn priorResetlogsScn; stmt.defineUInt64(3, priorResetlogsScn);
            char status[129]; stmt.defineString(4, status, sizeof(status));
            typeResetlogs resetlogs; stmt.defineUInt32(5, resetlogs);
            uint32_t priorIncarnation; stmt.defineUInt32(6, priorIncarnation);

            int64_t ret = stmt.executeQuery();
            while (ret) {
                auto oi = new OracleIncarnation(incarnation, resetlogsScn, priorResetlogsScn, status,
                                                resetlogs, priorIncarnation);
                metadata->oracleIncarnations.insert(oi);

                // Search prev value
                if (oldResetlogs != 0 && oi->resetlogs == oldResetlogs) {
                    metadata->oracleIncarnationCurrent = oi;
                } else
                if (oi->current && metadata->oracleIncarnationCurrent == nullptr) {
                    metadata->oracleIncarnationCurrent = oi;
                    metadata->setResetlogs(oi->resetlogs);
                }

                ret = stmt.next();
            }
        }

        // Reload online redo log ctx
        {
            DatabaseStatement stmt(conn);
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_LOGFILE_LIST)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << standby)
            stmt.createStatement(SQL_GET_LOGFILE_LIST);
            if (standby)
                stmt.bindString(1, "STANDBY");
            else
                stmt.bindString(1, "ONLINE");

            int64_t group = -1; stmt.defineInt64(1, group);
            char pathStr[514]; stmt.defineString(2, pathStr, sizeof(pathStr));
            Reader* onlineReader = nullptr;
            int64_t lastGroup = -1;
            std::string path;

            int64_t ret = stmt.executeQuery();
            while (ret) {
                if (group != lastGroup) {
                    onlineReader = readerCreate(group);
                    onlineReader->paths.clear();
                    lastGroup = group;
                }
                path = pathStr;
                onlineReader->paths.push_back(path);
                auto redoLog = new RedoLog(group, pathStr);
                metadata->redoLogs.insert(redoLog);

                ret = stmt.next();
            }

            if (readers.empty()) {
                if (standby)
                    throw RuntimeException("failed to find standby redo log files");
                 else
                    throw RuntimeException("failed to find online redo log files");
            }
        }
        checkOnlineRedoLogs();
    }

    void ReplicatorOnline::archGetLogOnline(Replicator* replicator) {
        if (!(reinterpret_cast<ReplicatorOnline*>(replicator))->checkConnection())
            return;

        Ctx* ctx = replicator->ctx;
        {
            DatabaseStatement stmt((dynamic_cast<ReplicatorOnline*>(replicator))->conn);
            TRACE(TRACE2_SQL, "SQL: " << SQL_GET_ARCHIVE_LOG_LIST)
            TRACE(TRACE2_SQL, "PARAM1: " << std::dec << (reinterpret_cast<ReplicatorOnline*>(replicator))->metadata->sequence)
            TRACE(TRACE2_SQL, "PARAM2: " << std::dec << replicator->metadata->resetlogs)

            stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
            stmt.bindUInt32(1, (reinterpret_cast<ReplicatorOnline*>(replicator))->metadata->sequence);
            stmt.bindUInt32(2, replicator->metadata->resetlogs);

            char path[513]; stmt.defineString(1, path, sizeof(path));
            typeSeq sequence; stmt.defineUInt32(2, sequence);
            typeScn firstScn; stmt.defineUInt64(3, firstScn);
            typeScn nextScn; stmt.defineUInt64(4, nextScn);

            int64_t ret = stmt.executeQuery();
            while (ret) {
                std::string mappedPath(path);
                replicator->applyMapping(mappedPath);

                auto parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                         replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = firstScn;
                parser->nextScn = nextScn;
                parser->sequence = sequence;
                (reinterpret_cast<ReplicatorOnline*>(replicator))->archiveRedoQueue.push(parser);
                ret = stmt.next();
            }
        }
        replicator->goStandby();
    }

    const char* ReplicatorOnline::getModeName() const {
        return "online";
    }
}
