/* Thread reading Oracle Redo Logs using online mode
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/XmlCtx.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/table/XdbTtSet.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXQn.h"
#include "../common/table/XdbXPt.h"
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
            ",  DBTIMEZONE"
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
            ",  NVL(SYS_CONTEXT('USERENV','CDB_NAME'), SYS_CONTEXT('USERENV','DB_NAME'))"
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
            "   T.ROWID, T.OBJ#, T.DATAOBJ#, T.TS#, T.CLUCOLS,"
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
            "   T.ROWID, T.OBJ#, T.DATAOBJ#, T.TS#, T.CLUCOLS,"
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

    const char* ReplicatorOnline::SQL_GET_XDB_TTSET(
            "SELECT"
            "   T.ROWID, T.GUID, T.TOKSUF, T.FLAGS, T.OBJ#"
            " FROM"
            "   XDB.XDB$TTSET AS OF SCN :i T");

    const char* ReplicatorOnline::SQL_CHECK_CONNECTION(
            "SELECT 1 FROM DUAL");

    ReplicatorOnline::ReplicatorOnline(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
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
        if (!ctx->disableChecksSet(Ctx::DISABLE_CHECKS_GRANTS)) {
            checkTableForGrants("SYS.V_$ARCHIVED_LOG");
            checkTableForGrants("SYS.V_$DATABASE");
            checkTableForGrants("SYS.V_$DATABASE_INCARNATION");
            checkTableForGrants("SYS.V_$LOG");
            checkTableForGrants("SYS.V_$LOGFILE");
            checkTableForGrants("SYS.V_$PARAMETER");
            checkTableForGrants("SYS.V_$STANDBY_LOG");
            checkTableForGrants("SYS.V_$TRANSPORTABLE_PLATFORM");
        }

        archReader = readerCreate(0);

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_DATABASE_INFORMATION);
            stmt.createStatement(SQL_GET_DATABASE_INFORMATION);
            uint64_t logMode;
            stmt.defineUInt64(1, logMode);
            uint64_t supplementalLogMin;
            stmt.defineUInt64(2, supplementalLogMin);
            uint64_t suppLogDbPrimary;
            stmt.defineUInt64(3, suppLogDbPrimary);
            uint64_t suppLogDbAll;
            stmt.defineUInt64(4, suppLogDbAll);
            uint64_t bigEndian;
            stmt.defineUInt64(5, bigEndian);
            char banner[81];
            stmt.defineString(6, banner, sizeof(banner));
            char context[81];
            stmt.defineString(7, context, sizeof(context));
            stmt.defineUInt64(8, currentScn);
            char dbTimezoneStr[81];
            stmt.defineString(9, dbTimezoneStr, sizeof(dbTimezoneStr));

            if (stmt.executeQuery()) {
                if (logMode == 0) {
                    ctx->hint("run: SHUTDOWN IMMEDIATE;");
                    ctx->hint("run: STARTUP MOUNT;");
                    ctx->hint("run: ALTER DATABASE ARCHIVELOG;");
                    ctx->hint("run: ALTER DATABASE OPEN;");
                    throw RuntimeException(10021, "database not in ARCHIVELOG mode");
                }

                if (supplementalLogMin == 0) {
                    ctx->hint("run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;");
                    ctx->hint("run: ALTER SYSTEM ARCHIVE LOG CURRENT;");
                    throw RuntimeException(10022, "SUPPLEMENTAL_LOG_DATA_MIN missing");
                }

                if (bigEndian)
                    ctx->setBigEndian();

                metadata->suppLogDbPrimary = suppLogDbPrimary;
                metadata->suppLogDbAll = suppLogDbAll;
                metadata->context = context;
                metadata->dbTimezoneStr = dbTimezoneStr;
                if (metadata->ctx->dbTimezone != Ctx::BAD_TIMEZONE) {
                    metadata->dbTimezone = metadata->ctx->dbTimezone;
                } else {
                    if (!ctx->parseTimezone(dbTimezoneStr, metadata->dbTimezone))
                        throw RuntimeException(10068, "invalid DBTIMEZONE value: " + std::string(dbTimezoneStr));
                }

                // 12+
                metadata->conId = 0;
                if (memcmp(banner, "Oracle Database 11g", 19) != 0) {
                    ctx->version12 = true;
                    DatabaseStatement stmt2(conn);
                    if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                        ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_CON_INFO);
                    stmt2.createStatement(SQL_GET_CON_INFO);
                    typeConId conId;
                    stmt2.defineInt16(1, conId);
                    char conNameChar[81];
                    stmt2.defineString(2, conNameChar, sizeof(conNameChar));
                    char conContext[81];
                    stmt2.defineString(3, conContext, sizeof(conContext));

                    if (stmt2.executeQuery()) {
                        metadata->conId = conId;
                        metadata->conName = conNameChar;
                        metadata->context = conContext;
                    }
                }

                ctx->info(0, "version: " + std::string(banner) + ", context: " + metadata->context + ", resetlogs: " +
                             std::to_string(metadata->resetlogs) + ", activation: " + std::to_string(metadata->activation) + ", con_id: " +
                             std::to_string(metadata->conId) + ", con_name: " + metadata->conName);
            } else {
                throw RuntimeException(10023, "no data in SYS.V_$DATABASE");
            }
        }

        if (!ctx->disableChecksSet(Ctx::DISABLE_CHECKS_GRANTS) && !standby) {
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
            checkTableForGrantsFlashback("XDB.XDB$TTSET", currentScn);
        }

        metadata->dbRecoveryFileDest = getParameterValue("db_recovery_file_dest");
        if (metadata->dbRecoveryFileDest.length() > 0 && metadata->dbRecoveryFileDest.back() == '/') {
            while (metadata->dbRecoveryFileDest.length() > 0 && metadata->dbRecoveryFileDest.back() == '/')
                metadata->dbRecoveryFileDest.pop_back();
            ctx->warning(60026, "stripping trailing '/' from db_recovery_file_dest parameter; new value: " + metadata->dbRecoveryFileDest);
        }
        metadata->logArchiveDest = getParameterValue("log_archive_dest");
        if (metadata->logArchiveDest.length() > 0 && metadata->logArchiveDest.back() == '/') {
            while (metadata->logArchiveDest.length() > 0 && metadata->logArchiveDest.back() == '/')
                metadata->logArchiveDest.pop_back();
            ctx->warning(60026, "stripping trailing '/' from log_archive_dest parameter; new value: " + metadata->logArchiveDest);
        }
        metadata->dbBlockChecksum = getParameterValue("db_block_checksum");
        std::transform(metadata->dbBlockChecksum.begin(), metadata->dbBlockChecksum.end(), metadata->dbBlockChecksum.begin(), ::toupper);
        if (metadata->dbRecoveryFileDest.length() == 0)
            metadata->logArchiveFormat = getParameterValue("log_archive_format");
        metadata->nlsCharacterSet = getPropertyValue("NLS_CHARACTERSET");
        metadata->nlsNcharCharacterSet = getPropertyValue("NLS_NCHAR_CHARACTERSET");

        ctx->info(0, "loading character mapping for " + metadata->nlsCharacterSet);
        ctx->info(0, "loading character mapping for " + metadata->nlsNcharCharacterSet);
        metadata->setNlsCharset(metadata->nlsCharacterSet, metadata->nlsNcharCharacterSet);
        metadata->onlineData = true;
    }

    void ReplicatorOnline::positionReader() {
        // Position by time
        if (metadata->startTime.length() > 0) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw BootException(10024, "can't position by time for standby database");

            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SCN_FROM_TIME);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + metadata->startTime);
            }
            stmt.createStatement(SQL_GET_SCN_FROM_TIME);

            stmt.bindString(1, metadata->startTime);
            typeScn firstDataScn;
            stmt.defineUInt64(1, firstDataScn);

            if (!stmt.executeQuery())
                throw BootException(10025, "can't find scn for: " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;

        } else if (metadata->startTimeRel > 0) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw BootException(10026, "can't position by relative time for standby database");

            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SCN_FROM_TIME_RELATIVE);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(metadata->startTimeRel));
            }
            stmt.createStatement(SQL_GET_SCN_FROM_TIME_RELATIVE);
            stmt.bindUInt64(1, metadata->startTimeRel);
            typeScn firstDataScn;
            stmt.defineUInt64(1, firstDataScn);

            if (!stmt.executeQuery())
                throw BootException(10025, "can't find scn for " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;

        } else if (metadata->firstDataScn == Ctx::ZERO_SCN || metadata->firstDataScn == 0) {
            // NOW
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_DATABASE_SCN);
            stmt.createStatement(SQL_GET_DATABASE_SCN);
            typeScn firstDataScn;
            stmt.defineUInt64(1, firstDataScn);

            if (!stmt.executeQuery())
                throw BootException(10029, "can't find database current scn");
            metadata->firstDataScn = firstDataScn;
        }

        // First sequence
        if (metadata->startSequence != Ctx::ZERO_SEQ) {
            metadata->setSeqOffset(metadata->startSequence, 0);
            if (metadata->firstDataScn == Ctx::ZERO_SCN)
                metadata->firstDataScn = 0;
        } else {
            DatabaseStatement stmt(conn);
            if (standby) {
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(metadata->firstDataScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(metadata->firstDataScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(metadata->resetlogs));
                }
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
                stmt.bindUInt64(1, metadata->firstDataScn);
                stmt.bindUInt64(2, metadata->firstDataScn);
                stmt.bindUInt32(3, metadata->resetlogs);
            } else {
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SEQUENCE_FROM_SCN);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(metadata->firstDataScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(metadata->firstDataScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(metadata->resetlogs));
                }
                stmt.createStatement(SQL_GET_SEQUENCE_FROM_SCN);
                stmt.bindUInt64(1, metadata->firstDataScn);
                stmt.bindUInt64(2, metadata->firstDataScn);
                stmt.bindUInt32(3, metadata->resetlogs);
            }
            typeSeq sequence;
            stmt.defineUInt32(1, sequence);

            if (!stmt.executeQuery())
                throw BootException(10030, "getting database sequence for scn: " + std::to_string(metadata->firstDataScn));

            metadata->setSeqOffset(sequence, 0);
            ctx->info(0, "starting sequence not found - starting with new batch with seq: " + std::to_string(metadata->sequence));
        }

        if (metadata->firstDataScn == Ctx::ZERO_SCN)
            throw BootException(10031, "getting database scn");
    }

    bool ReplicatorOnline::checkConnection() {
        if (!conn->connected) {
            ctx->info(0, "connecting to Oracle instance of " + database + " to " + conn->connectString);
        }

        while (!ctx->softShutdown) {
            if (!conn->connected) {
                try {
                    conn->connect();
                } catch (RuntimeException& ex) {
                    ctx->error(ex.code, ex.msg);
                }
            }

            if (conn->connected) {
                try {
                    DatabaseStatement stmt(conn);
                    if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                        ctx->logTrace(Ctx::TRACE_SQL, SQL_CHECK_CONNECTION);
                    stmt.createStatement(SQL_CHECK_CONNECTION);
                    uint64_t dummy;
                    stmt.defineUInt64(1, dummy);

                    stmt.executeQuery();
                } catch (RuntimeException& ex) {
                    conn->disconnect();
                    usleep(ctx->redoReadSleepUs);
                    ctx->info(0, "reconnecting to Oracle instance of " + database + " to " + conn->connectString);
                    continue;
                }

                return true;
            }

            if (unlikely(ctx->trace & Ctx::TRACE_REDO))
                ctx->logTrace(Ctx::TRACE_REDO, "cannot connect to database, retry in 5 sec.");
            sleep(5);
        }

        return false;
    }

    void ReplicatorOnline::goStandby() {
        if (!keepConnection)
            conn->disconnect();
    }

    std::string ReplicatorOnline::getParameterValue(const char* parameter) const {
        char value[OracleTable::VPARAMETER_LENGTH + 1];
        DatabaseStatement stmt(conn);
        if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
            ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_PARAMETER);
            ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::string(parameter));
        }
        stmt.createStatement(SQL_GET_PARAMETER);
        stmt.bindString(1, parameter);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery())
            return value;

        // No value found
        throw RuntimeException(10032, "can't get parameter value for " + std::string(parameter));
    }

    std::string ReplicatorOnline::getPropertyValue(const char* property) const {
        char value[OracleTable::VPROPERTY_LENGTH + 1];
        DatabaseStatement stmt(conn);
        if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
            ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_PROPERTY);
            ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::string(property));
        }
        stmt.createStatement(SQL_GET_PROPERTY);
        stmt.bindString(1, property);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery())
            return value;

        // No value found
        throw RuntimeException(10033, "can't get property value for " + std::string(property));
    }

    void ReplicatorOnline::checkTableForGrants(const char* tableName) {
        try {
            std::string query("SELECT 1 FROM " + std::string(tableName) + " WHERE 0 = 1");
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                ctx->logTrace(Ctx::TRACE_SQL, query);
            stmt.createStatement(query.c_str());
            uint64_t dummy;
            stmt.defineUInt64(1, dummy);

            stmt.executeQuery();
        } catch (RuntimeException& ex) {
            if (ex.supCode == 1031) {
                if (metadata->conId > 0) {
                    ctx->hint("run: ALTER SESSION SET CONTAINER = " + metadata->conName + ";");
                    ctx->hint("run: GRANT SELECT ON " + std::string(tableName) + " TO " + conn->user + ";");
                } else {
                    ctx->hint("run: GRANT SELECT ON " + std::string(tableName) + " TO " + conn->user + ";");
                }
                throw RuntimeException(10034, "grants missing for table " + std::string(tableName));
            } else {
                throw RuntimeException(ex.code, ex.msg);
            }
        }
    }

    void ReplicatorOnline::checkTableForGrantsFlashback(const char* tableName, typeScn scn) {
        try {
            std::string query("SELECT 1 FROM " + std::string(tableName) + " AS OF SCN " + std::to_string(scn) + " WHERE 0 = 1");
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                ctx->logTrace(Ctx::TRACE_SQL, query);
            stmt.createStatement(query.c_str());
            uint64_t dummy;
            stmt.defineUInt64(1, dummy);

            stmt.executeQuery();
        } catch (RuntimeException& ex) {
            if (ex.supCode == 1031) {
                if (metadata->conId > 0) {
                    ctx->hint("run: ALTER SESSION SET CONTAINER = " + metadata->conName + ";");
                    ctx->hint("run: GRANT SELECT, FLASHBACK ON " + std::string(tableName) + " TO " + conn->user + ";");
                } else {
                    ctx->hint("run: GRANT SELECT, FLASHBACK ON " + std::string(tableName) + " TO " + conn->user + ";");
                }
                throw RuntimeException(10034, "grants missing for table " + std::string(tableName));
            } else if (ex.supCode == 8181)
                throw RuntimeException(10035, "specified SCN number is not a valid system change number");
            else {
                throw RuntimeException(ex.code, ex.msg);
            }
        }
    }

    void ReplicatorOnline::verifySchema(typeScn currentScn) {
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS_VERIFY_SCHEMA))
            return;
        if (!checkConnection())
            return;

        ctx->info(0, "verifying schema for SCN: " + std::to_string(currentScn));

        Schema otherSchema(ctx, metadata->locales);
        try {
            readSystemDictionariesMetadata(&otherSchema, currentScn);
            for (const SchemaElement* element: metadata->schemaElements)
                readSystemDictionaries(&otherSchema, currentScn, element->owner, element->table, element->options);
            std::string errMsg;
            bool result = metadata->schema->compare(&otherSchema, errMsg);
            if (result) {
                ctx->warning(70000, "schema incorrect: " + errMsg);
            }
        } catch (RuntimeException& e) {
            ctx->error(e.code, e.msg);
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }
    }

    void ReplicatorOnline::createSchema() {
        if (!checkConnection())
            return;

        ctx->info(0, "reading dictionaries for scn: " + std::to_string(metadata->firstDataScn));

        std::vector<std::string> msgs;
        {
            std::unique_lock<std::mutex> lck(metadata->mtxSchema);
            metadata->schema->purgeMetadata();
            metadata->schema->purgeDicts();
            metadata->schema->scn = metadata->firstDataScn;
            metadata->firstSchemaScn = metadata->firstDataScn;
            readSystemDictionariesMetadata(metadata->schema, metadata->firstDataScn);

            for (const SchemaElement* element: metadata->schemaElements)
                createSchemaForTable(metadata->firstDataScn, element->owner, element->table, element->keys, element->keysStr, element->conditionStr,
                                     element->options, msgs);
            metadata->schema->resetTouched();

            if (unlikely(metadata->ctx->trace & Ctx::TRACE_CHECKPOINT))
                metadata->ctx->logTrace(Ctx::TRACE_CHECKPOINT, "schema creation completed, allowing checkpoints");
            metadata->allowCheckpoints();
        }

        for (const auto& msg: msgs) {
            ctx->info(0, "- found: " + msg);
        }
    }

    void ReplicatorOnline::readSystemDictionariesMetadata(Schema* schema, typeScn targetScn) {
        if (unlikely(ctx->trace & Ctx::TRACE_REDO))
            ctx->logTrace(Ctx::TRACE_REDO, "reading metadata");

        try {
            // Reading SYS.TS$
            DatabaseStatement sysTsStmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TS);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
            }
            sysTsStmt.createStatement(SQL_GET_SYS_TS);
            sysTsStmt.bindUInt64(1, targetScn);
            char sysTsRowid[19];
            sysTsStmt.defineString(1, sysTsRowid, sizeof(sysTsRowid));
            typeTs sysTsTs;
            sysTsStmt.defineUInt32(2, sysTsTs);
            char sysTsName[129];
            sysTsStmt.defineString(3, sysTsName, sizeof(sysTsName));
            uint32_t sysTsBlockSize;
            sysTsStmt.defineUInt32(4, sysTsBlockSize);

            int64_t sysTsRet = sysTsStmt.executeQuery();
            while (sysTsRet) {
                schema->dictSysTsAdd(sysTsRowid, sysTsTs, sysTsName, sysTsBlockSize);
                sysTsRet = sysTsStmt.next();
            }

            // Reading XDB.XDB$TTSET
            DatabaseStatement xdbTtSetStmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_XDB_TTSET);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
            }
            xdbTtSetStmt.createStatement(SQL_GET_XDB_TTSET);
            xdbTtSetStmt.bindUInt64(1, targetScn);
            char xdbTtSetRowid[19];
            xdbTtSetStmt.defineString(1, xdbTtSetRowid, sizeof(xdbTtSetRowid));
            char xdbTtSetGuid[XdbTtSet::GUID_LENGTH + 1];
            xdbTtSetStmt.defineString(2, xdbTtSetGuid, sizeof(xdbTtSetGuid));
            char xdbTtSetTokSuf[XdbTtSet::TOKSUF_LENGTH + 1];
            xdbTtSetStmt.defineString(3, xdbTtSetTokSuf, sizeof(xdbTtSetTokSuf));
            uint64_t xdbTtSetFlags;
            xdbTtSetStmt.defineUInt64(4, xdbTtSetFlags);
            uint32_t xdbTtSetObj;
            xdbTtSetStmt.defineUInt32(5, xdbTtSetObj);

            int64_t xdbTtSetRet = xdbTtSetStmt.executeQuery();
            while (xdbTtSetRet) {
                schema->dictXdbTtSetAdd(xdbTtSetRowid, xdbTtSetGuid, xdbTtSetTokSuf, xdbTtSetFlags, xdbTtSetObj);
                xdbTtSetRet = xdbTtSetStmt.next();
            }

            for (auto ttSetIt: schema->xdbTtSetMapRowId) {
                XmlCtx* xmlCtx = new XmlCtx(ctx, ttSetIt.second->tokSuf, ttSetIt.second->flags);
                schema->schemaXmlMap.insert_or_assign(ttSetIt.second->tokSuf, xmlCtx);

                // Check permissions before reading data
                std::string tableName = "XDB.X$NM" + ttSetIt.second->tokSuf;
                checkTableForGrantsFlashback(tableName.c_str(), targetScn);
                tableName = "XDB.X$PT" + ttSetIt.second->tokSuf;
                checkTableForGrantsFlashback(tableName.c_str(), targetScn);
                tableName = "XDB.X$QN" + ttSetIt.second->tokSuf;
                checkTableForGrantsFlashback(tableName.c_str(), targetScn);

                // Reading XDB.X$NMxxxx
                DatabaseStatement xdbXNmStmt(conn);
                std::string SQL_GET_XDB_XNM = "SELECT"
                                              "   T.ROWID, T.NMSPCURI, T.ID "
                                              " FROM"
                                              "   XDB.X$NM" + ttSetIt.second->tokSuf + " AS OF SCN :i T";
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_XDB_XNM);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                }
                xdbXNmStmt.createStatement(SQL_GET_XDB_XNM.c_str());
                xdbXNmStmt.bindUInt64(1, targetScn);
                char xdbXNmRowid[19];
                xdbXNmStmt.defineString(1, xdbXNmRowid, sizeof(xdbXNmRowid));
                char xdbNmNmSpcUri[XdbXNm::NMSPCURI_LENGTH + 1];
                xdbXNmStmt.defineString(2, xdbNmNmSpcUri, sizeof(xdbNmNmSpcUri));
                char xdbNmId[XdbXNm::ID_LENGTH + 1];
                xdbXNmStmt.defineString(3, xdbNmId, sizeof(xdbNmId));

                int64_t xdbXNmRet = xdbXNmStmt.executeQuery();
                while (xdbXNmRet) {
                    schema->dictXdbXNmAdd(xmlCtx, xdbXNmRowid, xdbNmNmSpcUri, xdbNmId);
                    xdbXNmRet = xdbXNmStmt.next();
                }

                // Reading XDB.X$PTxxxx
                DatabaseStatement xdbXPtStmt(conn);
                std::string SQL_GET_XDB_XPT = "SELECT"
                                              "   T.ROWID, T.PATH, T.ID "
                                              " FROM"
                                              "   XDB.X$PT" + ttSetIt.second->tokSuf + " AS OF SCN :i T";
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_XDB_XPT);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                }
                xdbXPtStmt.createStatement(SQL_GET_XDB_XPT.c_str());
                xdbXPtStmt.bindUInt64(1, targetScn);
                char xdbXPtRowid[19];
                xdbXPtStmt.defineString(1, xdbXPtRowid, sizeof(xdbXPtRowid));
                char xdbPtPath[XdbXPt::PATH_LENGTH + 1];
                xdbXPtStmt.defineString(2, xdbPtPath, sizeof(xdbPtPath));
                char xdbPtId[XdbXPt::ID_LENGTH + 1];
                xdbXPtStmt.defineString(3, xdbPtId, sizeof(xdbPtId));

                int64_t xdbXPtRet = xdbXPtStmt.executeQuery();
                while (xdbXPtRet) {
                    schema->dictXdbXPtAdd(xmlCtx, xdbXPtRowid, xdbPtPath, xdbPtId);
                    xdbXPtRet = xdbXPtStmt.next();
                }

                // Reading XDB.X$QNxxxx
                DatabaseStatement xdbXQnStmt(conn);
                std::string SQL_GET_XDB_XQN = "SELECT"
                                              "   T.ROWID, T.NMSPCID, T.LOCALNAME, T.FLAGS, T.ID "
                                              " FROM"
                                              "   XDB.X$QN" + ttSetIt.second->tokSuf + " AS OF SCN :i T";
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_XDB_XQN);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                }
                xdbXQnStmt.createStatement(SQL_GET_XDB_XQN.c_str());
                xdbXQnStmt.bindUInt64(1, targetScn);
                char xdbXQnRowid[19];
                xdbXQnStmt.defineString(1, xdbXQnRowid, sizeof(xdbXQnRowid));
                char xdbXQnNmSpcId[XdbXQn::NMSPCID_LENGTH + 1];
                xdbXQnStmt.defineString(2, xdbXQnNmSpcId, sizeof(xdbXQnNmSpcId));
                char xdbXQnLocalName[XdbXQn::LOCALNAME_LENGTH + 1];
                xdbXQnStmt.defineString(3, xdbXQnLocalName, sizeof(xdbXQnLocalName));
                char xdbXQnFlags[XdbXQn::FLAGS_LENGTH + 1];
                xdbXQnStmt.defineString(4, xdbXQnFlags, sizeof(xdbXQnFlags));
                char xdbXQnId[XdbXQn::ID_LENGTH + 1];
                xdbXQnStmt.defineString(5, xdbXQnId, sizeof(xdbXQnId));

                int64_t xdbXQnRet = xdbXQnStmt.executeQuery();
                while (xdbXQnRet) {
                    schema->dictXdbXQnAdd(xmlCtx, xdbXQnRowid, xdbXQnNmSpcId, xdbXQnLocalName, xdbXQnFlags, xdbXQnId);
                    xdbXQnRet = xdbXQnStmt.next();
                }
            }
        } catch (RuntimeException& ex) {
            if (ex.supCode == 8181) {
                throw BootException(10035, "can't read metadata from flashback, provide a valid starting SCN value");
            } else {
                throw BootException(ex.code, ex.msg);
            }
        }
    }

    void ReplicatorOnline::readSystemDictionariesDetails(Schema* schema, typeScn targetScn, typeUser user, typeObj obj) {
        if (unlikely(ctx->trace & Ctx::TRACE_REDO))
            ctx->logTrace(Ctx::TRACE_REDO, "read dictionaries for user: " + std::to_string(user) + ", object: " + std::to_string(obj));

        // Reading SYS.CCOL$
        DatabaseStatement sysCColStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_CCOL_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysCColStmt.createStatement(SQL_GET_SYS_CCOL_OBJ);
            sysCColStmt.bindUInt64(1, targetScn);
            sysCColStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_CCOL_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysCColStmt.createStatement(SQL_GET_SYS_CCOL_USER);
            sysCColStmt.bindUInt64(1, targetScn);
            sysCColStmt.bindUInt64(2, targetScn);
            sysCColStmt.bindUInt32(3, user);
        }

        char sysCColRowid[19];
        sysCColStmt.defineString(1, sysCColRowid, sizeof(sysCColRowid));
        typeCon sysCColCon;
        sysCColStmt.defineUInt32(2, sysCColCon);
        typeCol sysCColIntCol;
        sysCColStmt.defineInt16(3, sysCColIntCol);
        typeObj sysCColObj;
        sysCColStmt.defineUInt32(4, sysCColObj);
        uint64_t sysCColSpare11 = 0;
        sysCColStmt.defineUInt64(5, sysCColSpare11);
        uint64_t sysCColSpare12 = 0;
        sysCColStmt.defineUInt64(6, sysCColSpare12);

        int64_t sysCColRet = sysCColStmt.executeQuery();
        while (sysCColRet) {
            schema->dictSysCColAdd(sysCColRowid, sysCColCon, sysCColIntCol, sysCColObj, sysCColSpare11, sysCColSpare12);
            sysCColSpare11 = 0;
            sysCColSpare12 = 0;
            sysCColRet = sysCColStmt.next();
        }

        // Reading SYS.CDEF$
        DatabaseStatement sysCDefStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_CDEF_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysCDefStmt.createStatement(SQL_GET_SYS_CDEF_OBJ);
            sysCDefStmt.bindUInt64(1, targetScn);
            sysCDefStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_CDEF_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysCDefStmt.createStatement(SQL_GET_SYS_CDEF_USER);
            sysCDefStmt.bindUInt64(1, targetScn);
            sysCDefStmt.bindUInt64(2, targetScn);
            sysCDefStmt.bindUInt32(3, user);
        }

        char sysCDefRowid[19];
        sysCDefStmt.defineString(1, sysCDefRowid, sizeof(sysCDefRowid));
        typeCon sysCDefCon;
        sysCDefStmt.defineUInt32(2, sysCDefCon);
        typeObj sysCDefObj;
        sysCDefStmt.defineUInt32(3, sysCDefObj);
        uint64_t sysCDefType;
        sysCDefStmt.defineUInt64(4, sysCDefType);

        int64_t sysCDefRet = sysCDefStmt.executeQuery();
        while (sysCDefRet) {
            schema->dictSysCDefAdd(sysCDefRowid, sysCDefCon, sysCDefObj, sysCDefType);
            sysCDefRet = sysCDefStmt.next();
        }

        // Reading SYS.COL$
        DatabaseStatement sysColStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_COL_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysColStmt.createStatement(SQL_GET_SYS_COL_OBJ);
            sysColStmt.bindUInt64(1, targetScn);
            sysColStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_COL_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysColStmt.createStatement(SQL_GET_SYS_COL_USER);
            sysColStmt.bindUInt64(1, targetScn);
            sysColStmt.bindUInt64(2, targetScn);
            sysColStmt.bindUInt32(3, user);
        }

        char sysColRowid[19];
        sysColStmt.defineString(1, sysColRowid, sizeof(sysColRowid));
        typeObj sysColObj;
        sysColStmt.defineUInt32(2, sysColObj);
        typeCol sysColCol;
        sysColStmt.defineInt16(3, sysColCol);
        typeCol sysColSegCol;
        sysColStmt.defineInt16(4, sysColSegCol);
        typeCol sysColIntCol;
        sysColStmt.defineInt16(5, sysColIntCol);
        char sysColName[129];
        sysColStmt.defineString(6, sysColName, sizeof(sysColName));
        uint64_t sysColType;
        sysColStmt.defineUInt64(7, sysColType);
        uint64_t sysColLength;
        sysColStmt.defineUInt64(8, sysColLength);
        int64_t sysColPrecision = -1;
        sysColStmt.defineInt64(9, sysColPrecision);
        int64_t sysColScale = -1;
        sysColStmt.defineInt64(10, sysColScale);
        uint64_t sycColCharsetForm = 0;
        sysColStmt.defineUInt64(11, sycColCharsetForm);
        uint64_t sysColCharsetId = 0;
        sysColStmt.defineUInt64(12, sysColCharsetId);
        int64_t sysColNull;
        sysColStmt.defineInt64(13, sysColNull);
        uint64_t sysColProperty1;
        sysColStmt.defineUInt64(14, sysColProperty1);
        uint64_t sysColProperty2;
        sysColStmt.defineUInt64(15, sysColProperty2);

        int64_t sysColRet = sysColStmt.executeQuery();
        while (sysColRet) {
            schema->dictSysColAdd(sysColRowid, sysColObj, sysColCol, sysColSegCol, sysColIntCol, sysColName,
                                  sysColType, sysColLength, sysColPrecision, sysColScale, sycColCharsetForm,
                                  sysColCharsetId, sysColNull, sysColProperty1, sysColProperty2);
            sysColPrecision = -1;
            sysColScale = -1;
            sycColCharsetForm = 0;
            sysColCharsetId = 0;
            sysColRet = sysColStmt.next();
        }

        // Reading SYS.DEFERRED_STG$
        DatabaseStatement sysDeferredStgStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_DEFERRED_STG_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysDeferredStgStmt.createStatement(SQL_GET_SYS_DEFERRED_STG_OBJ);
            sysDeferredStgStmt.bindUInt64(1, targetScn);
            sysDeferredStgStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_DEFERRED_STG_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysDeferredStgStmt.createStatement(SQL_GET_SYS_DEFERRED_STG_USER);
            sysDeferredStgStmt.bindUInt64(1, targetScn);
            sysDeferredStgStmt.bindUInt64(2, targetScn);
            sysDeferredStgStmt.bindUInt32(3, user);
        }

        char sysDeferredStgRowid[19];
        sysDeferredStgStmt.defineString(1, sysDeferredStgRowid, sizeof(sysDeferredStgRowid));
        typeObj sysDeferredStgObj;
        sysDeferredStgStmt.defineUInt32(2, sysDeferredStgObj);
        uint64_t sysDeferredStgFlagsStg1 = 0;
        sysDeferredStgStmt.defineUInt64(3, sysDeferredStgFlagsStg1);
        uint64_t sysDeferredStgFlagsStg2 = 0;
        sysDeferredStgStmt.defineUInt64(4, sysDeferredStgFlagsStg2);

        int64_t sysDeferredStgRet = sysDeferredStgStmt.executeQuery();
        while (sysDeferredStgRet) {
            schema->dictSysDeferredStgAdd(sysDeferredStgRowid, sysDeferredStgObj, sysDeferredStgFlagsStg1,
                                          sysDeferredStgFlagsStg2);
            sysDeferredStgFlagsStg1 = 0;
            sysDeferredStgFlagsStg2 = 0;
            sysDeferredStgRet = sysDeferredStgStmt.next();
        }

        // Reading SYS.ECOL$
        DatabaseStatement sysEColStmt(conn);
        if (ctx->version12) {
            if (obj != 0) {
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_ECOL_OBJ);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL_OBJ);
                sysEColStmt.bindUInt64(1, targetScn);
                sysEColStmt.bindUInt32(2, obj);
            } else {
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_ECOL_USER);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL_USER);
                sysEColStmt.bindUInt64(1, targetScn);
                sysEColStmt.bindUInt64(2, targetScn);
                sysEColStmt.bindUInt32(3, user);
            }
        } else {
            if (obj != 0) {
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_ECOL11_OBJ);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL11_OBJ);
                sysEColStmt.bindUInt64(1, targetScn);
                sysEColStmt.bindUInt32(2, obj);
            } else {
                if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                    ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_ECOL11_USER);
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                    ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
                }
                sysEColStmt.createStatement(SQL_GET_SYS_ECOL11_USER);
                sysEColStmt.bindUInt64(1, targetScn);
                sysEColStmt.bindUInt64(2, targetScn);
                sysEColStmt.bindUInt32(3, user);
            }
        }

        char sysEColRowid[19];
        sysEColStmt.defineString(1, sysEColRowid, sizeof(sysEColRowid));
        typeObj sysEColTabObj;
        sysEColStmt.defineUInt32(2, sysEColTabObj);
        typeCol sysEColColNum = 0;
        sysEColStmt.defineInt16(3, sysEColColNum);
        typeCol sysEColGuardId = -1;
        sysEColStmt.defineInt16(4, sysEColGuardId);

        int64_t sysEColRet = sysEColStmt.executeQuery();
        while (sysEColRet) {
            schema->dictSysEColAdd(sysEColRowid, sysEColTabObj, sysEColColNum, sysEColGuardId);
            sysEColColNum = 0;
            sysEColGuardId = -1;
            sysEColRet = sysEColStmt.next();
        }

        // Reading SYS.LOB$
        DatabaseStatement sysLobStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_LOB_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysLobStmt.createStatement(SQL_GET_SYS_LOB_OBJ);
            sysLobStmt.bindUInt64(1, targetScn);
            sysLobStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_LOB_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysLobStmt.createStatement(SQL_GET_SYS_LOB_USER);
            sysLobStmt.bindUInt64(1, targetScn);
            sysLobStmt.bindUInt64(2, targetScn);
            sysLobStmt.bindUInt32(3, user);
        }

        char sysLobRowid[19];
        sysLobStmt.defineString(1, sysLobRowid, sizeof(sysLobRowid));
        typeObj sysLobObj;
        sysLobStmt.defineUInt32(2, sysLobObj);
        typeCol sysLobCol = 0;
        sysLobStmt.defineInt16(3, sysLobCol);
        typeCol sysLobIntCol = 0;
        sysLobStmt.defineInt16(4, sysLobIntCol);
        typeObj sysLobLObj;
        sysLobStmt.defineUInt32(5, sysLobLObj);
        typeTs sysLobTs;
        sysLobStmt.defineUInt32(6, sysLobTs);

        int64_t sysLobRet = sysLobStmt.executeQuery();
        while (sysLobRet) {
            schema->dictSysLobAdd(sysLobRowid, sysLobObj, sysLobCol, sysLobIntCol, sysLobLObj, sysLobTs);
            sysLobRet = sysLobStmt.next();
        }

        // Reading SYS.LOBCOMPPART$
        DatabaseStatement sysLobCompPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_LOB_COMP_PART_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(obj));
            }
            sysLobCompPartStmt.createStatement(SQL_GET_SYS_LOB_COMP_PART_OBJ);
            sysLobCompPartStmt.bindUInt64(1, targetScn);
            sysLobCompPartStmt.bindUInt64(2, targetScn);
            sysLobCompPartStmt.bindUInt32(3, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_LOB_COMP_PART_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM4: " + std::to_string(user));
            }
            sysLobCompPartStmt.createStatement(SQL_GET_SYS_LOB_COMP_PART_USER);
            sysLobCompPartStmt.bindUInt64(1, targetScn);
            sysLobCompPartStmt.bindUInt64(2, targetScn);
            sysLobCompPartStmt.bindUInt64(3, targetScn);
            sysLobCompPartStmt.bindUInt32(4, user);
        }

        char sysLobCompPartRowid[19];
        sysLobCompPartStmt.defineString(1, sysLobCompPartRowid, sizeof(sysLobCompPartRowid));
        typeObj sysLobCompPartPartObj;
        sysLobCompPartStmt.defineUInt32(2, sysLobCompPartPartObj);
        typeObj sysLobCompPartLObj;
        sysLobCompPartStmt.defineUInt32(3, sysLobCompPartLObj);

        int64_t sysLobCompPartRet = sysLobCompPartStmt.executeQuery();
        while (sysLobCompPartRet) {
            schema->dictSysLobCompPartAdd(sysLobCompPartRowid, sysLobCompPartPartObj, sysLobCompPartLObj);
            sysLobCompPartRet = sysLobCompPartStmt.next();
        }

        // Reading SYS.LOBFRAG$
        DatabaseStatement sysLobFragStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_LOB_FRAG_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM4: " + std::to_string(obj));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM5: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM6: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM7: " + std::to_string(obj));
            }
            sysLobFragStmt.createStatement(SQL_GET_SYS_LOB_FRAG_OBJ);
            sysLobFragStmt.bindUInt64(1, targetScn);
            sysLobFragStmt.bindUInt64(2, targetScn);
            sysLobFragStmt.bindUInt64(3, targetScn);
            sysLobFragStmt.bindUInt32(4, obj);
            sysLobFragStmt.bindUInt64(5, targetScn);
            sysLobFragStmt.bindUInt64(6, targetScn);
            sysLobFragStmt.bindUInt32(7, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_LOB_FRAG_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM4: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM5: " + std::to_string(user));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM6: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM7: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM8: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM9: " + std::to_string(user));
            }
            sysLobFragStmt.createStatement(SQL_GET_SYS_LOB_FRAG_USER);
            sysLobFragStmt.bindUInt64(1, targetScn);
            sysLobFragStmt.bindUInt64(2, targetScn);
            sysLobFragStmt.bindUInt64(3, targetScn);
            sysLobFragStmt.bindUInt64(4, targetScn);
            sysLobFragStmt.bindUInt32(5, user);
            sysLobFragStmt.bindUInt64(6, targetScn);
            sysLobFragStmt.bindUInt64(7, targetScn);
            sysLobFragStmt.bindUInt64(8, targetScn);
            sysLobFragStmt.bindUInt32(9, user);
        }

        char sysLobFragRowid[19];
        sysLobFragStmt.defineString(1, sysLobFragRowid, sizeof(sysLobFragRowid));
        typeObj sysLobFragFragObj;
        sysLobFragStmt.defineUInt32(2, sysLobFragFragObj);
        typeObj sysLobFragParentObj;
        sysLobFragStmt.defineUInt32(3, sysLobFragParentObj);
        typeTs sysLobFragTs;
        sysLobFragStmt.defineUInt32(4, sysLobFragTs);

        int64_t sysLobFragRet = sysLobFragStmt.executeQuery();
        while (sysLobFragRet) {
            schema->dictSysLobFragAdd(sysLobFragRowid, sysLobFragFragObj, sysLobFragParentObj, sysLobFragTs);
            sysLobFragRet = sysLobFragStmt.next();
        }

        // Reading SYS.TAB$
        DatabaseStatement sysTabStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TAB_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabStmt.createStatement(SQL_GET_SYS_TAB_OBJ);
            sysTabStmt.bindUInt64(1, targetScn);
            sysTabStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TAB_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabStmt.createStatement(SQL_GET_SYS_TAB_USER);
            sysTabStmt.bindUInt64(1, targetScn);
            sysTabStmt.bindUInt64(2, targetScn);
            sysTabStmt.bindUInt32(3, user);
        }

        char sysTabRowid[19];
        sysTabStmt.defineString(1, sysTabRowid, sizeof(sysTabRowid));
        typeObj sysTabObj;
        sysTabStmt.defineUInt32(2, sysTabObj);
        typeDataObj sysTabDataObj = 0;
        sysTabStmt.defineUInt32(3, sysTabDataObj);
        typeTs sysTabTs;
        sysTabStmt.defineUInt32(4, sysTabTs);
        typeCol sysTabCluCols = 0;
        sysTabStmt.defineInt16(5, sysTabCluCols);
        uint64_t sysTabFlags1;
        sysTabStmt.defineUInt64(6, sysTabFlags1);
        uint64_t sysTabFlags2;
        sysTabStmt.defineUInt64(7, sysTabFlags2);
        uint64_t sysTabProperty1;
        sysTabStmt.defineUInt64(8, sysTabProperty1);
        uint64_t sysTabProperty2;
        sysTabStmt.defineUInt64(9, sysTabProperty2);

        int64_t sysTabRet = sysTabStmt.executeQuery();
        while (sysTabRet) {
            schema->dictSysTabAdd(sysTabRowid, sysTabObj, sysTabDataObj, sysTabTs, sysTabCluCols, sysTabFlags1,
                                  sysTabFlags2, sysTabProperty1, sysTabProperty2);
            sysTabDataObj = 0;
            sysTabCluCols = 0;
            sysTabRet = sysTabStmt.next();
        }

        // Reading SYS.TABCOMPART$
        DatabaseStatement sysTabComPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TABCOMPART_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabComPartStmt.createStatement(SQL_GET_SYS_TABCOMPART_OBJ);
            sysTabComPartStmt.bindUInt64(1, targetScn);
            sysTabComPartStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TABCOMPART_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabComPartStmt.createStatement(SQL_GET_SYS_TABCOMPART_USER);
            sysTabComPartStmt.bindUInt64(1, targetScn);
            sysTabComPartStmt.bindUInt64(2, targetScn);
            sysTabComPartStmt.bindUInt32(3, user);
        }

        char sysTabComPartRowid[19];
        sysTabComPartStmt.defineString(1, sysTabComPartRowid, sizeof(sysTabComPartRowid));
        typeObj sysTabComPartObj;
        sysTabComPartStmt.defineUInt32(2, sysTabComPartObj);
        typeDataObj sysTabComPartDataObj = 0;
        sysTabComPartStmt.defineUInt32(3, sysTabComPartDataObj);
        typeObj sysTabComPartBo;
        sysTabComPartStmt.defineUInt32(4, sysTabComPartBo);

        int64_t sysTabComPartRet = sysTabComPartStmt.executeQuery();
        while (sysTabComPartRet) {
            schema->dictSysTabComPartAdd(sysTabComPartRowid, sysTabComPartObj, sysTabComPartDataObj, sysTabComPartBo);
            sysTabComPartDataObj = 0;
            sysTabComPartRet = sysTabComPartStmt.next();
        }

        // Reading SYS.TABPART$
        DatabaseStatement sysTabPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TABPART_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabPartStmt.createStatement(SQL_GET_SYS_TABPART_OBJ);
            sysTabPartStmt.bindUInt64(1, targetScn);
            sysTabPartStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TABPART_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabPartStmt.createStatement(SQL_GET_SYS_TABPART_USER);
            sysTabPartStmt.bindUInt64(1, targetScn);
            sysTabPartStmt.bindUInt64(2, targetScn);
            sysTabPartStmt.bindUInt32(3, user);
        }

        char sysTabPartRowid[19];
        sysTabPartStmt.defineString(1, sysTabPartRowid, sizeof(sysTabPartRowid));
        typeObj sysTabPartObj;
        sysTabPartStmt.defineUInt32(2, sysTabPartObj);
        typeDataObj sysTabPartDataObj = 0;
        sysTabPartStmt.defineUInt32(3, sysTabPartDataObj);
        typeObj sysTabPartBo;
        sysTabPartStmt.defineUInt32(4, sysTabPartBo);

        int64_t sysTabPartRet = sysTabPartStmt.executeQuery();
        while (sysTabPartRet) {
            schema->dictSysTabPartAdd(sysTabPartRowid, sysTabPartObj, sysTabPartDataObj, sysTabPartBo);
            sysTabPartDataObj = 0;
            sysTabPartRet = sysTabPartStmt.next();
        }

        // Reading SYS.TABSUBPART$
        DatabaseStatement sysTabSubPartStmt(conn);
        if (obj != 0) {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TABSUBPART_OBJ);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(obj));
            }
            sysTabSubPartStmt.createStatement(SQL_GET_SYS_TABSUBPART_OBJ);
            sysTabSubPartStmt.bindUInt64(1, targetScn);
            sysTabSubPartStmt.bindUInt32(2, obj);
        } else {
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_TABSUBPART_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + std::to_string(user));
            }
            sysTabSubPartStmt.createStatement(SQL_GET_SYS_TABSUBPART_USER);
            sysTabSubPartStmt.bindUInt64(1, targetScn);
            sysTabSubPartStmt.bindUInt64(2, targetScn);
            sysTabSubPartStmt.bindUInt32(3, user);
        }

        char sysTabSubPartRowid[19];
        sysTabSubPartStmt.defineString(1, sysTabSubPartRowid, sizeof(sysTabSubPartRowid));
        typeObj sysTabSubPartObj;
        sysTabSubPartStmt.defineUInt32(2, sysTabSubPartObj);
        typeDataObj sysTabSubPartDataObj = 0;
        sysTabSubPartStmt.defineUInt32(3, sysTabSubPartDataObj);
        typeObj sysTabSubPartPobj;
        sysTabSubPartStmt.defineUInt32(4, sysTabSubPartPobj);

        int64_t sysTabSubPartRet = sysTabSubPartStmt.executeQuery();
        while (sysTabSubPartRet) {
            schema->dictSysTabSubPartAdd(sysTabSubPartRowid, sysTabSubPartObj, sysTabSubPartDataObj, sysTabSubPartPobj);
            sysTabSubPartDataObj = 0;
            sysTabSubPartRet = sysTabSubPartStmt.next();
        }
    }

    void ReplicatorOnline::readSystemDictionaries(Schema* schema, typeScn targetScn, const std::string& owner, const std::string& table, typeOptions options) {
        std::string ownerRegexp("^" + owner + "$");
        std::string tableRegexp("^" + table + "$");
        bool single = ((options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0);
        if (unlikely(ctx->trace & Ctx::TRACE_REDO))
            ctx->logTrace(Ctx::TRACE_REDO, "read dictionaries for owner: " + owner + ", table: " + table + ", options: " +
                                           std::to_string(static_cast<uint64_t>(options)));

        try {
            DatabaseStatement sysUserStmt(conn);

            // Reading SYS.USER$
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_USER);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + ownerRegexp);
            }
            sysUserStmt.createStatement(SQL_GET_SYS_USER);
            sysUserStmt.bindUInt64(1, targetScn);
            sysUserStmt.bindString(2, ownerRegexp);
            char sysUserRowid[19];
            sysUserStmt.defineString(1, sysUserRowid, sizeof(sysUserRowid));
            typeUser sysUserUser;
            sysUserStmt.defineUInt32(2, sysUserUser);
            char sysUserName[129];
            sysUserStmt.defineString(3, sysUserName, sizeof(sysUserName));
            uint64_t sysUserSpare11 = 0;
            sysUserStmt.defineUInt64(4, sysUserSpare11);
            uint64_t sysUserSpare12 = 0;
            sysUserStmt.defineUInt64(5, sysUserSpare12);

            int64_t sysUserRet = sysUserStmt.executeQuery();
            while (sysUserRet) {
                if (!schema->dictSysUserAdd(sysUserRowid, sysUserUser, sysUserName, sysUserSpare11, sysUserSpare12,
                                            (options & OracleTable::OPTIONS_SYSTEM_TABLE) != 0)) {
                    sysUserSpare11 = 0;
                    sysUserSpare12 = 0;
                    sysUserRet = sysUserStmt.next();
                    continue;
                }

                DatabaseStatement sysObjStmt(conn);
                // Reading SYS.OBJ$
                if ((options & OracleTable::OPTIONS_SYSTEM_TABLE) == 0) {
                    if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                        ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_OBJ_USER);
                        ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                        ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(sysUserUser));
                    }
                    sysObjStmt.createStatement(SQL_GET_SYS_OBJ_USER);
                    sysObjStmt.bindUInt64(1, targetScn);
                    sysObjStmt.bindUInt32(2, sysUserUser);
                } else {
                    if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                        ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_SYS_OBJ_NAME);
                        ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(targetScn));
                        ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(sysUserUser));
                        ctx->logTrace(Ctx::TRACE_SQL, "PARAM3: " + table);
                    }
                    sysObjStmt.createStatement(SQL_GET_SYS_OBJ_NAME);
                    sysObjStmt.bindUInt64(1, targetScn);
                    sysObjStmt.bindUInt32(2, sysUserUser);
                    sysObjStmt.bindString(3, tableRegexp);
                }

                char sysObjRowid[19];
                sysObjStmt.defineString(1, sysObjRowid, sizeof(sysObjRowid));
                typeUser sysObjOwner;
                sysObjStmt.defineUInt32(2, sysObjOwner);
                typeObj sysObjObj;
                sysObjStmt.defineUInt32(3, sysObjObj);
                typeDataObj sysObjDataObj = 0;
                sysObjStmt.defineUInt32(4, sysObjDataObj);
                char sysObjName[129];
                sysObjStmt.defineString(5, sysObjName, sizeof(sysObjName));
                uint64_t sysObjType = 0;
                sysObjStmt.defineUInt64(6, sysObjType);
                uint64_t sysObjFlags1;
                sysObjStmt.defineUInt64(7, sysObjFlags1);
                uint64_t sysObjFlags2;
                sysObjStmt.defineUInt64(8, sysObjFlags2);

                int64_t sysObjRet = sysObjStmt.executeQuery();
                while (sysObjRet) {
                    if (schema->dictSysObjAdd(sysObjRowid, sysObjOwner, sysObjObj, sysObjDataObj, sysObjType, sysObjName,
                                              sysObjFlags1, sysObjFlags2, single)) {
                        if (single)
                            readSystemDictionariesDetails(schema, targetScn, sysUserUser, sysObjObj);
                    }
                    sysObjDataObj = 0;
                    sysObjFlags1 = 0;
                    sysObjFlags2 = 0;
                    sysObjRet = sysObjStmt.next();
                }

                if (!single)
                    readSystemDictionariesDetails(schema, targetScn, sysUserUser, 0);

                sysUserSpare11 = 0;
                sysUserSpare12 = 0;
                sysUserRet = sysUserStmt.next();
            }
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
            throw BootException(10035, "can't read schema from flashback, provide a valid starting SCN value");
        } catch (RuntimeException& ex) {
            if (ex.supCode == 8181) {
                throw BootException(10035, "can't read schema from flashback, provide a valid starting SCN value");
            } else {
                throw BootException(ex.code, ex.msg);
            }
        }
    }

    void ReplicatorOnline::createSchemaForTable(typeScn targetScn, const std::string& owner, const std::string& table, const std::vector<std::string>& keys,
                                                const std::string& keysStr, const std::string& conditionStr, typeOptions options,
                                                std::vector<std::string>& msgs) {
        if (unlikely(ctx->trace & Ctx::TRACE_REDO))
            ctx->logTrace(Ctx::TRACE_REDO, "creating table schema for owner: " + owner + " table: " + table + " options: " +
                                           std::to_string(static_cast<uint64_t>(options)));

        readSystemDictionaries(metadata->schema, targetScn, owner, table, options);

        metadata->schema->buildMaps(owner, table, keys, keysStr, conditionStr, options, msgs, metadata->suppLogDbPrimary,
                                    metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                    metadata->defaultCharacterNcharMapId);
    }

    void ReplicatorOnline::updateOnlineRedoLogData() {
        if (!checkConnection())
            return;

        std::unique_lock<std::mutex> lck(metadata->mtxCheckpoint);

        // Reload incarnation ctx
        typeResetlogs oldResetlogs = metadata->resetlogs;
        for (OracleIncarnation* oi: metadata->oracleIncarnations)
            delete oi;
        metadata->oracleIncarnations.clear();
        metadata->oracleIncarnationCurrent = nullptr;

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_DATABASE_ROLE);
            stmt.createStatement(SQL_GET_DATABASE_ROLE);
            char databaseRole[129];
            stmt.defineString(1, databaseRole, sizeof(databaseRole));

            if (stmt.executeQuery()) {
                std::string roleStr(databaseRole);
                if (roleStr == "PRIMARY") {
                    if (standby) {
                        standby = false;
                        ctx->info(0, "changed database role to: " + roleStr);
                    }
                } else if (roleStr == "PHYSICAL STANDBY") {
                    if (!standby) {
                        standby = true;
                        ctx->info(0, "changed database role to: " + roleStr);
                    }
                } else {
                    throw RuntimeException(10038, "unknown database role: " + roleStr);
                }
            }
        }

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL))
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_DATABASE_INCARNATION);
            stmt.createStatement(SQL_GET_DATABASE_INCARNATION);
            uint32_t incarnation;
            stmt.defineUInt32(1, incarnation);
            typeScn resetlogsScn;
            stmt.defineUInt64(2, resetlogsScn);
            typeScn priorResetlogsScn;
            stmt.defineUInt64(3, priorResetlogsScn);
            char status[129];
            stmt.defineString(4, status, sizeof(status));
            typeResetlogs resetlogs;
            stmt.defineUInt32(5, resetlogs);
            uint32_t priorIncarnation;
            stmt.defineUInt32(6, priorIncarnation);

            int64_t ret = stmt.executeQuery();
            while (ret) {
                auto oi = new OracleIncarnation(incarnation, resetlogsScn, priorResetlogsScn, status,
                                                resetlogs, priorIncarnation);
                metadata->oracleIncarnations.insert(oi);

                // Search prev value
                if (oldResetlogs != 0 && oi->resetlogs == oldResetlogs) {
                    metadata->oracleIncarnationCurrent = oi;
                } else if (oi->current && metadata->oracleIncarnationCurrent == nullptr) {
                    metadata->oracleIncarnationCurrent = oi;
                    metadata->setResetlogs(oi->resetlogs);
                }

                ret = stmt.next();
            }
        }

        // Reload online redo log ctx
        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->trace & Ctx::TRACE_SQL)) {
                ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_LOGFILE_LIST);
                ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " + std::to_string(standby));
            }
            stmt.createStatement(SQL_GET_LOGFILE_LIST);
            if (standby)
                stmt.bindString(1, "STANDBY");
            else
                stmt.bindString(1, "ONLINE");

            int64_t group = -1;
            stmt.defineInt64(1, group);
            char pathStr[514];
            stmt.defineString(2, pathStr, sizeof(pathStr));
            Reader* onlineReader = nullptr;
            int64_t lastGroup = -1;

            int64_t ret = stmt.executeQuery();
            while (ret) {
                if (group != lastGroup) {
                    onlineReader = readerCreate(group);
                    onlineReader->paths.clear();
                    lastGroup = group;
                }
                std::string path = pathStr;
                onlineReader->paths.push_back(path);
                auto redoLog = new RedoLog(group, pathStr);
                metadata->redoLogs.insert(redoLog);

                ret = stmt.next();
            }

            if (readers.empty()) {
                if (standby)
                    throw RuntimeException(10036, "failed to find standby redo log files");
                else
                    throw RuntimeException(10037, "failed to find online redo log files");
            }
        }
        checkOnlineRedoLogs();
    }

    void ReplicatorOnline::archGetLogOnline(Replicator* replicator) {
        ReplicatorOnline* replicatorOnline = dynamic_cast<ReplicatorOnline*>(replicator);
        if (replicatorOnline == nullptr)
            return;

        if (!replicatorOnline->checkConnection())
            return;

        {
            DatabaseStatement stmt(replicatorOnline->conn);
            if (unlikely(replicator->ctx->trace & Ctx::TRACE_SQL)) {
                replicator->ctx->logTrace(Ctx::TRACE_SQL, SQL_GET_ARCHIVE_LOG_LIST);
                replicator->ctx->logTrace(Ctx::TRACE_SQL, "PARAM1: " +
                                                          std::to_string(replicatorOnline->metadata->sequence));
                replicator->ctx->logTrace(Ctx::TRACE_SQL, "PARAM2: " + std::to_string(replicator->metadata->resetlogs));
            }

            stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
            stmt.bindUInt32(1, (reinterpret_cast<ReplicatorOnline*>(replicator))->metadata->sequence);
            stmt.bindUInt32(2, replicator->metadata->resetlogs);

            char path[513];
            stmt.defineString(1, path, sizeof(path));
            typeSeq sequence;
            stmt.defineUInt32(2, sequence);
            typeScn firstScn;
            stmt.defineUInt64(3, firstScn);
            typeScn nextScn;
            stmt.defineUInt64(4, nextScn);

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
