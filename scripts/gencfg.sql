/* Create schema file for offline mode.
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

-- The script is intended to use for databases where no object/user creation is possible.
-- This allows to use OFFLINE mode for OpenLogReplicator - and requires no connection to the database from the program.
--
-- It will create output, please remove first and last lines to leave just JSON content
-- and save it to proper file with proper SCN in the name
-- Please do not rename the file to different than printed, as this may cause incorrect behavior
--

SET LINESIZE 32767
SET SERVEROUTPUT ON FORMAT TRUNCATED
SET TRIMSPOOL ON
DECLARE
    TYPE VARCHAR2TABLE IS TABLE OF VARCHAR2(30);
    v_USERNAME_LIST VARCHAR2TABLE;
    TYPE NUMBERTABLE IS TABLE OF NUMBER;
    v_USER_LIST NUMBERTABLE;
    v_LOG_MODE SYS.V_$DATABASE.LOG_MODE%TYPE;
    v_SUPPLEMENTAL_LOG_DATA_MIN SYS.V_$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN%TYPE;
    v_SUPPLEMENTAL_LOG_DATA_PK SYS.V_$DATABASE.SUPPLEMENTAL_LOG_DATA_PK%TYPE;
    v_SUPPLEMENTAL_LOG_DATA_ALL SYS.V_$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL%TYPE;
    v_ENDIAN_FORMAT SYS.V_$TRANSPORTABLE_PLATFORM.ENDIAN_FORMAT%TYPE;
    v_SUPPLEMENTAL_LOG_DB_PRIMARY NUMBER;
    v_SUPPLEMENTAL_LOG_DB_ALL NUMBER;
    v_BIG_ENDIAN NUMBER;
    v_CURRENT_SCN SYS.V_$DATABASE.CURRENT_SCN%TYPE;
    v_ACTIVATION# SYS.V_$DATABASE.ACTIVATION#%TYPE;
    v_BANNER SYS.V_$VERSION.BANNER%TYPE;
    v_DB_ID NUMBER;
    v_DB_NAME VARCHAR2(100);
    v_DB_TIMEZONE VARCHAR2(100);
    v_CON_ID NUMBER;
    v_CON_NAME VARCHAR2(100);
    v_DB_RECOVERY_FILE_DEST SYS.V_$PARAMETER.VALUE%TYPE;
    v_DB_BLOCK_CHECKSUM SYS.V_$PARAMETER.VALUE%TYPE;
    v_LOG_ARCHIVE_DEST SYS.V_$PARAMETER.VALUE%TYPE;
    v_LOG_ARCHIVE_FORMAT SYS.V_$PARAMETER.VALUE%TYPE;
    v_NLS_CHARACTERSET DATABASE_PROPERTIES.PROPERTY_VALUE%TYPE;
    v_NLS_NCHAR_CHARACTERSET DATABASE_PROPERTIES.PROPERTY_VALUE%TYPE;
    v_SEQUENCE# SYS.V_$LOG.SEQUENCE#%TYPE;
    v_RESETLOGS SYS.V_$DATABASE_INCARNATION.RESETLOGS_ID%TYPE;
    v_PREV BOOLEAN;
    v_VERSION11 BOOLEAN;
    v_SYS_SINGLE NUMBER;
    v_XDB_SINGLE NUMBER;
    v_USER_SINGLE NUMBER;
    v_OBJECT_SINGLE NUMBER;
    v_USERS NUMBER;
    v_SCN NUMBER;
    v_FILE VARCHAR2(256);
    v_NAME VARCHAR2(256);
    v_GROUP NUMBER;
    TYPE T_REF_CUR IS REF CURSOR;
    CV T_REF_CUR;
    TYPE T_XDB_X_NM IS RECORD (
        ROWID UROWID,
        NMSPCURI VARCHAR2(2000),
        ID RAW(8)
    );
    v_XDB_X_NM T_XDB_X_NM;
    TYPE T_XDB_X_QN IS RECORD (
        ROWID UROWID,
        NMSPCID RAW(8),
        LOCALNAME VARCHAR2(2000),
        FLAGS RAW(4),
        ID RAW(8)
    );
    v_XDB_X_QN T_XDB_X_QN;
    TYPE T_XDB_X_PT IS RECORD (
        ROWID UROWID,
        PATH RAW(2000),
        ID RAW(8)
    );
    v_XDB_X_PT T_XDB_X_PT;
    TYPE T_SYS_ECOL IS RECORD (
        ROWID UROWID,
        TABOBJ# NUMBER,
        COLNUM NUMBER,
        GUARD_ID NUMBER
    );
    v_SYS_ECOL T_SYS_ECOL;
    SQLTEXT VARCHAR2(2000);
BEGIN
    -- set script parameters:
    --------------------------------------------
    -- name of the server (defined in OpenLogReplicator, not necessary the same as SID):
    v_NAME := 'DB';
    -- list of users with replicated tables, table list is not needed:
    v_USERNAME_LIST := VARCHAR2TABLE('USR1', 'USR2');
    -- SCN for schema to create:
    SELECT CURRENT_SCN INTO v_SCN FROM SYS.V_$DATABASE;
    -- when defined SCN value please update and use this line instead:
    -- v_SCN := 12345678;
    --------------------------------------------

    DBMS_OUTPUT.PUT_LINE('SCN: ' || v_SCN);

    v_USER_LIST := NUMBERTABLE();
    v_USERS := 0;

    v_SYS_SINGLE := 1;
    v_XDB_SINGLE := 1;
    FOR I IN v_USERNAME_LIST.FIRST .. v_USERNAME_LIST.LAST LOOP
        IF v_USERNAME_LIST(I) = 'SYS' THEN
            v_SYS_SINGLE := 0;
        END IF;
        IF v_USERNAME_LIST(I) = 'XDB' THEN
            v_XDB_SINGLE := 0;
        END IF;
    END LOOP;

    IF v_SYS_SINGLE = 1 THEN
        v_USERNAME_LIST.EXTEND;
        v_USERNAME_LIST(v_USERNAME_LIST.LAST) := 'SYS';
    END IF;
    IF v_XDB_SINGLE = 1 THEN
        v_USERNAME_LIST.EXTEND;
        v_USERNAME_LIST(v_USERNAME_LIST.LAST) := 'XDB';
    END IF;

    SELECT D.LOG_MODE, D.SUPPLEMENTAL_LOG_DATA_MIN, D.SUPPLEMENTAL_LOG_DATA_PK, D.SUPPLEMENTAL_LOG_DATA_ALL, TP.ENDIAN_FORMAT, D.CURRENT_SCN, D.ACTIVATION#,
        VER.BANNER
    INTO v_LOG_MODE, v_SUPPLEMENTAL_LOG_DATA_MIN, v_SUPPLEMENTAL_LOG_DATA_PK, v_SUPPLEMENTAL_LOG_DATA_ALL, v_ENDIAN_FORMAT, v_CURRENT_SCN, v_ACTIVATION#,
        v_BANNER
    FROM SYS.V_$DATABASE D
    JOIN SYS.V_$TRANSPORTABLE_PLATFORM TP ON TP.PLATFORM_NAME = D.PLATFORM_NAME
    JOIN SYS.V_$VERSION VER ON VER.BANNER LIKE '%Oracle%Database%';

    IF v_LOG_MODE <> 'ARCHIVELOG' THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: database not in ARCHIVELOG mode');
        DBMS_OUTPUT.PUT_LINE('HINT run: SHUTDOWN IMMEDIATE;');
        DBMS_OUTPUT.PUT_LINE('HINT run: STARTUP MOUNT;');
        DBMS_OUTPUT.PUT_LINE('HINT run: ALTER DATABASE ARCHIVELOG;');
        DBMS_OUTPUT.PUT_LINE('HINT run: ALTER DATABASE OPEN;');
        RETURN;
    END IF;

    IF v_SUPPLEMENTAL_LOG_DATA_MIN <> 'YES' THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: SUPPLEMENTAL_LOG_DATA_MIN missing');
        DBMS_OUTPUT.PUT_LINE('HINT run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;');
        DBMS_OUTPUT.PUT_LINE('HINT run: ALTER SYSTEM ARCHIVE LOG CURRENT;');
        RETURN;
    END IF;

    IF INSTR(v_BANNER, 'Oracle Database 11g') = 0 THEN
        EXECUTE IMMEDIATE 'SELECT SYS_CONTEXT(''USERENV'',''CON_ID''), SYS_CONTEXT(''USERENV'',''CON_NAME''), NVL(SYS_CONTEXT(''USERENV'',''CDB_NAME''), ' ||
            'SYS_CONTEXT(''USERENV'',''DB_NAME'')), DBTIMEZONE, ' ||
            'NVL((SELECT NVL(P.DBID, 0) FROM SYS.V_$PDBS P WHERE P.CON_ID = SYS_CONTEXT(''USERENV'',''CON_ID'')), 0) FROM DUAL'
        INTO v_CON_ID, v_CON_NAME, v_DB_NAME, v_DB_TIMEZONE, v_DB_ID;
        v_VERSION11 := FALSE;
    ELSE
        EXECUTE IMMEDIATE 'SELECT SYS_CONTEXT(''USERENV'',''DB_NAME''), DBTIMEZONE FROM DUAL' INTO v_DB_NAME, v_DB_TIMEZONE;
        v_DB_ID := 0;
        v_CON_ID := 0;
        v_VERSION11 := TRUE;
    END IF;

    IF v_SUPPLEMENTAL_LOG_DATA_PK = 'YES' THEN
        v_SUPPLEMENTAL_LOG_DB_PRIMARY := 1;
    ELSE
        v_SUPPLEMENTAL_LOG_DB_PRIMARY := 0;
    END IF;

    IF v_SUPPLEMENTAL_LOG_DATA_ALL = 'YES' THEN
        v_SUPPLEMENTAL_LOG_DB_ALL := 1;
    ELSE
        v_SUPPLEMENTAL_LOG_DB_ALL := 0;
    END IF;

    IF v_ENDIAN_FORMAT = 'Big' THEN
        v_BIG_ENDIAN := 1;
    ELSE
        v_BIG_ENDIAN := 0;
    END IF;

    SELECT RESETLOGS_ID INTO v_RESETLOGS FROM SYS.V_$DATABASE_INCARNATION WHERE STATUS = 'CURRENT';
    SELECT VALUE INTO v_DB_RECOVERY_FILE_DEST FROM SYS.V_$PARAMETER WHERE NAME = 'db_recovery_file_dest';
    SELECT VALUE INTO v_DB_BLOCK_CHECKSUM FROM SYS.V_$PARAMETER WHERE NAME = 'db_block_checksum';
    SELECT VALUE INTO v_LOG_ARCHIVE_DEST FROM SYS.V_$PARAMETER WHERE NAME = 'log_archive_dest';
    IF LENGTH(v_DB_RECOVERY_FILE_DEST) = 0 THEN
        SELECT VALUE INTO v_LOG_ARCHIVE_FORMAT FROM SYS.V_$PARAMETER WHERE NAME = 'log_archive_format';
    ELSE
        v_LOG_ARCHIVE_FORMAT := 'o1_mf_%t_%s_%h_.arc';
    END IF;
    SELECT PROPERTY_VALUE INTO v_NLS_CHARACTERSET FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME = 'NLS_CHARACTERSET';
    SELECT PROPERTY_VALUE INTO v_NLS_NCHAR_CHARACTERSET FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME = 'NLS_NCHAR_CHARACTERSET';
    SELECT SEQUENCE# INTO v_SEQUENCE# FROM SYS.V_$LOG WHERE STATUS = 'CURRENT';

    v_FILE := v_NAME || '-schema-' || v_SCN || '.json';
    DBMS_OUTPUT.PUT('CONTENT OF: ' || v_FILE);
    DBMS_OUTPUT.NEW_LINE();
    DBMS_OUTPUT.PUT('{"database":"' || v_NAME || '"');
    DBMS_OUTPUT.PUT(',"scn":' || v_SCN);
    DBMS_OUTPUT.PUT(',"resetlogs":' || v_RESETLOGS);
    DBMS_OUTPUT.PUT(',"activation":' || v_ACTIVATION#);
    DBMS_OUTPUT.PUT(',"time":0');
    DBMS_OUTPUT.PUT(',"seq":' || v_SEQUENCE#);
    DBMS_OUTPUT.PUT(',"offset":0');
    DBMS_OUTPUT.PUT(',"big-endian":' || v_BIG_ENDIAN);
    DBMS_OUTPUT.PUT(',"context":"' || v_DB_NAME || '"');
    DBMS_OUTPUT.PUT(',"con-id":' || v_CON_ID);
    DBMS_OUTPUT.PUT(',"con-name":"' || v_CON_NAME || '"');
    DBMS_OUTPUT.PUT(',"db-id":' || v_DB_ID);
    DBMS_OUTPUT.PUT(',"db-timezone":"' || v_DB_TIMEZONE || '"');
    DBMS_OUTPUT.PUT(',"db-recovery-file-dest":"' || v_DB_RECOVERY_FILE_DEST || '"');
    DBMS_OUTPUT.PUT(',"db-block-checksum":"' || v_DB_BLOCK_CHECKSUM || '"');
    DBMS_OUTPUT.PUT(',"log-archive-dest":"' || v_LOG_ARCHIVE_DEST || '"');
    DBMS_OUTPUT.PUT(',"log-archive-format":"' || v_LOG_ARCHIVE_FORMAT || '"');
    DBMS_OUTPUT.PUT(',"nls-character-set":"' || v_NLS_CHARACTERSET || '"');
    DBMS_OUTPUT.PUT(',"nls-nchar-character-set":"' || v_NLS_NCHAR_CHARACTERSET || '"');
    DBMS_OUTPUT.PUT(',"supp-log-db-primary":' || v_SUPPLEMENTAL_LOG_DB_PRIMARY);
    DBMS_OUTPUT.PUT(',"supp-log-db-all":' || v_SUPPLEMENTAL_LOG_DB_ALL);
    DBMS_OUTPUT.NEW_LINE();

    DBMS_OUTPUT.PUT(',"incarnations":[');
    v_PREV := FALSE;
    FOR v_INCARNATION IN (
        SELECT INCARNATION#, RESETLOGS_CHANGE#, PRIOR_RESETLOGS_CHANGE#, STATUS, RESETLOGS_ID, PRIOR_INCARNATION#
        FROM V$DATABASE_INCARNATION
    ) LOOP
        IF v_PREV = TRUE THEN
            DBMS_OUTPUT.PUT(',');
        ELSE
            v_PREV := TRUE;
        END IF;

        DBMS_OUTPUT.NEW_LINE();
        DBMS_OUTPUT.PUT('{"incarnation":' || v_INCARNATION.INCARNATION# ||
            ',"resetlogs-scn":' || v_INCARNATION.RESETLOGS_CHANGE# ||
            ',"prior-resetlogs-scn":' || v_INCARNATION.PRIOR_RESETLOGS_CHANGE# ||
            ',"status":"' || v_INCARNATION.STATUS || '"' ||
            ',"resetlogs":' || v_INCARNATION.RESETLOGS_ID ||
            ',"prior-incarnation":' || v_INCARNATION.PRIOR_INCARNATION# || '}');
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    DBMS_OUTPUT.PUT(',"online-redo":[');
    v_GROUP := -1;
    v_PREV := FALSE;
    FOR v_REDO IN (
        SELECT LF.GROUP#, REGEXP_REPLACE(LF.MEMBER,'(\\"|")','\"') AS MEMBER
        FROM SYS.V_$LOGFILE LF
        ORDER BY LF.GROUP# ASC, LF.IS_RECOVERY_DEST_FILE DESC, LF.MEMBER ASC
    ) LOOP
        IF v_GROUP <> v_REDO.GROUP# THEN
            IF v_GROUP <> -1 THEN
                DBMS_OUTPUT.PUT(']},');
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"group":' || v_REDO.GROUP# || ',"path":["' || v_REDO.MEMBER || '"');
            v_GROUP := v_REDO.GROUP#;
        ELSE
            DBMS_OUTPUT.PUT(',"' || v_REDO.MEMBER || '"');
        END IF;
    END LOOP;
    IF v_GROUP <> -1 THEN
        DBMS_OUTPUT.PUT(']}');
    END IF;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"users":[');
    FOR I IN v_USERNAME_LIST.FIRST .. v_USERNAME_LIST.LAST LOOP
        FOR v_SYS_USER IN (
            SELECT U.NAME, U.USER#
            FROM SYS.USER$ AS OF SCN v_SCN U
            WHERE U.NAME = v_USERNAME_LIST(I)
            ORDER BY U.NAME
        ) LOOP
            FOR J in 1 .. v_USERS LOOP
                IF v_USER_LIST(J) = v_SYS_USER.USER# THEN
                    RAISE_APPLICATION_ERROR(-20000, 'Duplicate user in list: ' || v_SYS_USER.NAME);
                END IF;
            END LOOP;

            IF (v_SYS_USER.NAME <> 'SYS' OR v_SYS_SINGLE = 0) AND (v_SYS_USER.NAME <> 'XDB' OR v_XDB_SINGLE = 0) THEN
                IF v_PREV = TRUE THEN
                    DBMS_OUTPUT.PUT(',');
                ELSE
                    v_PREV := TRUE;
                END IF;
                DBMS_OUTPUT.NEW_LINE();
                DBMS_OUTPUT.PUT('"' || v_SYS_USER.NAME || '"');
            END IF;

            v_USERS := v_USERS + 1;
            v_USER_LIST.EXTEND;
            v_USER_LIST(v_USERS) := v_SYS_USER.USER#;
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    DBMS_OUTPUT.PUT(',"schema-scn":' || v_SCN);
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-ccol":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_CCOL IN (
            SELECT L.ROWID, L.CON#, L.INTCOL#, L.OBJ#, MOD(NVL(L.SPARE1, 0), 18446744073709551616) AS SPARE11,
            MOD(TRUNC(NVL(L.SPARE1, 0) / 18446744073709551616), 18446744073709551616) AS SPARE12
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.CCOL$ AS OF SCN v_SCN L
            WHERE O.OBJ# = L.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY L.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_CCOL.ROWID || '"' ||
                ',"con":' || v_SYS_CCOL.CON# ||
                ',"int-col":' || v_SYS_CCOL.INTCOL# ||
                ',"obj":' || v_SYS_CCOL.OBJ# ||
                ',"spare1":[' || v_SYS_CCOL.SPARE11 || ',' || v_SYS_CCOL.SPARE12 || ']}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-cdef":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_CDEF IN (
            SELECT D.ROWID, D.CON#, D.OBJ#, D.TYPE#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.CDEF$ AS OF SCN v_SCN D
            WHERE O.OBJ# = D.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY D.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_CDEF.ROWID || '"' ||
                ',"con":' || v_SYS_CDEF.CON# ||
                ',"obj":' || v_SYS_CDEF.OBJ# ||
                ',"type":' || v_SYS_CDEF.TYPE# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-col":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_COL IN (
            SELECT C.ROWID, C.OBJ#, C.COL#, C.SEGCOL#, C.INTCOL#, C.NAME, C.TYPE#, C.LENGTH, NVL(C.PRECISION#, -1) AS PRECISION#, NVL(C.SCALE, -1) AS SCALE,
                NVL(C.CHARSETFORM, 0) AS CHARSETFORM, NVL(C.CHARSETID, 0) AS CHARSETID, C.NULL$,
                MOD(C.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(C.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.COL$ AS OF SCN v_SCN C
            WHERE O.OBJ# = C.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY C.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_COL.ROWID || '"' ||
                ',"obj":' || v_SYS_COL.OBJ# ||
                ',"col":' || v_SYS_COL.COL# ||
                ',"seg-col":' || v_SYS_COL.SEGCOL# ||
                ',"int-col":' || v_SYS_COL.INTCOL# ||
                ',"name":"' || v_SYS_COL.NAME || '"' ||
                ',"type":' || v_SYS_COL.TYPE# ||
                ',"length":' || v_SYS_COL.LENGTH ||
                ',"precision":' || v_SYS_COL.PRECISION# ||
                ',"scale":' || v_SYS_COL.SCALE ||
                ',"charset-form":' || v_SYS_COL.CHARSETFORM ||
                ',"charset-id":' || v_SYS_COL.CHARSETID ||
                ',"null":' || v_SYS_COL.NULL$ ||
                ',"property":[' || v_SYS_COL.PROPERTY1 || ',' || v_SYS_COL.PROPERTY2 || ']}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-deferredstg":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_DEFERRED_STG IN (
            SELECT DS.ROWID, DS.OBJ#, MOD(NVL(DS.FLAGS_STG, 0), 18446744073709551616) AS FLAGS_STG1,
                MOD(TRUNC(NVL(DS.FLAGS_STG, 0) / 18446744073709551616), 18446744073709551616) AS FLAGS_STG2
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.DEFERRED_STG$ AS OF SCN v_SCN DS
            WHERE O.OBJ# = DS.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY DS.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_DEFERRED_STG.ROWID || '"' ||
                ',"obj":' || v_SYS_DEFERRED_STG.OBJ# ||
                ',"flags-stg":[' || v_SYS_DEFERRED_STG.FLAGS_STG1 || ',' || v_SYS_DEFERRED_STG.FLAGS_STG2 || ']}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-ecol":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        IF v_VERSION11 = TRUE THEN
            SQLTEXT := 'SELECT E.ROWID, E.TABOBJ#, NVL(E.COLNUM, 0) AS COLNUM, -1 as GUARD_ID ' ||
                'FROM SYS.OBJ$ AS OF SCN ' || TO_CHAR(v_SCN) || ' O, SYS.ECOL$ AS OF SCN ' || TO_CHAR(v_SCN) || ' E ' ||
                'WHERE O.OBJ# = E.TABOBJ# AND O.OWNER# = ''' || v_USER_LIST(I) || '''' ||
                ' AND (''' || v_USERNAME_LIST(I) || ''' <> ''SYS'' OR ' || TO_CHAR(v_SYS_SINGLE) || ' = 0' ||
                ' OR O.NAME IN (''CCOL$'', ''CDEF$'', ''COL$'', ''DEFERRED_STG$'', ''ECOL$'', ''LOB$'', ''LOBFRAG$'', ''LOBCOMPPART$'', ''OBJ$'', ' ||
                '''TAB$'', ''TABCOMPART$'', ''TABPART$'', ''TABSUBPART$'', ''TS$'', ''USER$''))' ||
                ' AND (''' || v_USERNAME_LIST(I) || ''' <> ''XDB'' OR ' || TO_CHAR(v_XDB_SINGLE) || ' = 0' ||
                ' OR O.NAME = ''XDB$TTSET'' OR O.NAME LIKE ''X$NM%'' OR O.NAME LIKE ''X$QN%'' OR O.NAME LIKE ''X$PT%'')' ||
                ' ORDER BY E.ROWID';
        ELSE
            SQLTEXT := 'SELECT E.ROWID, E.TABOBJ#, NVL(E.COLNUM, 0) AS COLNUM, NVL(E.GUARD_ID, -1) as GUARD_ID ' ||
                'FROM SYS.OBJ$ AS OF SCN ' || TO_CHAR(v_SCN) || ' O, SYS.ECOL$ AS OF SCN ' || TO_CHAR(v_SCN) || ' E ' ||
                'WHERE O.OBJ# = E.TABOBJ# AND O.OWNER# = ''' || v_USER_LIST(I) || '''' ||
                ' AND (''' || v_USERNAME_LIST(I) || ''' <> ''SYS'' OR ' || TO_CHAR(v_SYS_SINGLE) || ' = 0' ||
                ' OR O.NAME IN (''CCOL$'', ''CDEF$'', ''COL$'', ''DEFERRED_STG$'', ''ECOL$'', ''LOB$'', ''LOBFRAG$'', ''LOBCOMPPART$'', ''OBJ$'', ' ||
                '''TAB$'', ''TABCOMPART$'', ''TABPART$'', ''TABSUBPART$'', ''TS$'', ''USER$''))' ||
                ' AND (''' || v_USERNAME_LIST(I) || ''' <> ''XDB'' OR ' || TO_CHAR(v_XDB_SINGLE) || ' = 0' ||
                ' OR O.NAME = ''XDB$TTSET'' OR O.NAME LIKE ''X$NM%'' OR O.NAME LIKE ''X$QN%'' OR O.NAME LIKE ''X$PT%'')' ||
                ' ORDER BY E.ROWID';
        END IF;

        OPEN CV FOR SQLTEXT;
        LOOP
        FETCH CV INTO v_SYS_ECOL;
            EXIT WHEN cv%NOTFOUND;

            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_ECOL.ROWID || '"' ||
                ',"tab-obj":' || v_SYS_ECOL.TABOBJ# ||
                ',"col-num":' || v_SYS_ECOL.COLNUM ||
                ',"guard-id":' || v_SYS_ECOL.GUARD_ID || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-lob":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_LOB IN (
            SELECT L.ROWID, L.OBJ#, L.COL#, L.INTCOL#, L.LOBJ#, L.TS#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.LOB$ AS OF SCN v_SCN L
            WHERE O.OBJ# = L.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY L.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_LOB.ROWID || '"' ||
                ',"obj":' || v_SYS_LOB.OBJ# ||
                ',"col":' || v_SYS_LOB.COL# ||
                ',"int-col":' || v_SYS_LOB.INTCOL# ||
                ',"l-obj":' || v_SYS_LOB.LOBJ# ||
                ',"ts":' || v_SYS_LOB.TS# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-lob-comp-part":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_LOB_COMP_PART IN (
            SELECT LCP.ROWID, LCP.PARTOBJ#, LCP.LOBJ#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.LOB$ AS OF SCN v_SCN L, SYS.LOBCOMPPART$ AS OF SCN v_SCN LCP
            WHERE O.OBJ# = L.OBJ# AND L.LOBJ# = LCP.LOBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY LCP.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_LOB_COMP_PART.ROWID || '"' ||
                ',"part-obj":' || v_SYS_LOB_COMP_PART.PARTOBJ# ||
                ',"l-obj":' || v_SYS_LOB_COMP_PART.LOBJ# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-lob-frag":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_LOB_FRAG IN (
            SELECT LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.LOB$ AS OF SCN v_SCN L, SYS.LOBCOMPPART$ AS OF SCN v_SCN LCP, SYS.LOBFRAG$ AS OF SCN v_SCN LF
            WHERE O.OBJ# = L.OBJ# AND L.LOBJ# = LCP.LOBJ# AND LCP.PARTOBJ# = LF.PARENTOBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            UNION ALL
            SELECT LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.LOB$ AS OF SCN v_SCN L, SYS.LOBFRAG$ AS OF SCN v_SCN LF
            WHERE O.OBJ# = L.OBJ# AND L.LOBJ# = LF.PARENTOBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY 4
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_LOB_FRAG.ROWID || '"' ||
                ',"frag-obj":' || v_SYS_LOB_FRAG.FRAGOBJ# ||
                ',"parent-obj":' || v_SYS_LOB_FRAG.PARENTOBJ# ||
                ',"ts":' || v_SYS_LOB_FRAG.TS# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-obj":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_OBJ IN (
            SELECT O.ROWID, O.OWNER#, O.OBJ#, NVL(O.DATAOBJ#, 0) AS DATAOBJ#, O.NAME, O.TYPE#, MOD(NVL(O.FLAGS, 0), 18446744073709551616) AS FLAGS1,
            MOD(TRUNC(NVL(O.FLAGS, 0) / 18446744073709551616), 18446744073709551616) AS FLAGS2
            FROM SYS.OBJ$ AS OF SCN v_SCN O WHERE O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY O.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            IF (v_USERNAME_LIST(I) = 'SYS' AND v_SYS_SINGLE = 1) OR (v_USERNAME_LIST(I) = 'XDB' AND v_XDB_SINGLE = 1) THEN
                v_OBJECT_SINGLE := 1;
            ELSE
                v_OBJECT_SINGLE := 0;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_OBJ.ROWID || '"' ||
                ',"owner":' || v_SYS_OBJ.OWNER# ||
                ',"obj":' || v_SYS_OBJ.OBJ# ||
                ',"data-obj":' || v_SYS_OBJ.DATAOBJ# ||
                ',"name":"' || v_SYS_OBJ.NAME || '"' ||
                ',"type":' || v_SYS_OBJ.TYPE# ||
                ',"flags":[' || v_SYS_OBJ.FLAGS1 || ',' || v_SYS_OBJ.FLAGS2 || ']' ||
                ',"single":' || v_OBJECT_SINGLE || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-tab":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_TAB IN (
            SELECT T.ROWID, T.OBJ#, NVL(T.DATAOBJ#, 0) AS DATAOBJ#, T.TS#, NVL(T.CLUCOLS, 0) AS CLUCOLS,
                MOD(T.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(T.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2,
                MOD(T.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(T.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.TAB$ AS OF SCN v_SCN T
            WHERE O.OBJ# = T.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY T.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_TAB.ROWID || '"' ||
                ',"obj":' || v_SYS_TAB.OBJ# ||
                ',"data-obj":' || v_SYS_TAB.DATAOBJ# ||
                ',"ts":' || v_SYS_TAB.TS# ||
                ',"clu-cols":' || v_SYS_TAB.CLUCOLS ||
                ',"flags":[' || v_SYS_TAB.FLAGS1 || ',' || v_SYS_TAB.FLAGS2 || ']' ||
                ',"property":[' || v_SYS_TAB.PROPERTY1 || ',' || v_SYS_TAB.PROPERTY2 || ']}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-tabcompart":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_TABCOMPART IN (
            SELECT TCP.ROWID, TCP.OBJ#, NVL(TCP.DATAOBJ#, 0) AS DATAOBJ#, TCP.BO#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.TABCOMPART$ AS OF SCN v_SCN TCP
            WHERE O.OBJ# = TCP.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY TCP.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_TABCOMPART.ROWID || '"' ||
                ',"obj":' || v_SYS_TABCOMPART.OBJ# ||
                ',"data-obj":' || v_SYS_TABCOMPART.DATAOBJ# ||
                ',"bo":' || v_SYS_TABCOMPART.BO# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-tabpart":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_TABPART IN (
            SELECT TP.ROWID, TP.OBJ#, NVL(TP.DATAOBJ#, 0) AS DATAOBJ#, TP.BO#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.TABPART$ AS OF SCN v_SCN TP
            WHERE O.OBJ# = TP.OBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY TP.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_TABPART.ROWID || '"' ||
                ',"obj":' || v_SYS_TABPART.OBJ# ||
                ',"data-obj":' || v_SYS_TABPART.DATAOBJ# ||
                ',"bo":' || v_SYS_TABPART.BO# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-tabsubpart":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_TABSUBPART IN (
            SELECT TSP.ROWID, TSP.OBJ#, NVL(TSP.DATAOBJ#, 0) AS DATAOBJ#, TSP.POBJ#
            FROM SYS.OBJ$ AS OF SCN v_SCN O, SYS.TABSUBPART$ AS OF SCN v_SCN TSP
            WHERE O.OBJ# = TSP.POBJ# AND O.OWNER# = v_USER_LIST(I)
                AND (v_USERNAME_LIST(I) <> 'SYS' OR v_SYS_SINGLE = 0 OR O.NAME IN ('CCOL$', 'CDEF$', 'COL$', 'DEFERRED_STG$', 'ECOL$', 'LOB$', 'LOBFRAG$',
                    'LOBCOMPPART$', 'OBJ$', 'TAB$', 'TABCOMPART$', 'TABPART$', 'TABSUBPART$', 'TS$', 'USER$'))
                AND (v_USERNAME_LIST(I) <> 'XDB' OR v_XDB_SINGLE = 0 OR O.NAME = 'XDB$TTSET' OR O.NAME LIKE 'X$NM%' OR O.NAME LIKE 'X$QN%' OR
                    O.NAME LIKE 'X$PT%')
            ORDER BY TSP.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_TABSUBPART.ROWID || '"' ||
                ',"obj":' || v_SYS_TABSUBPART.OBJ# ||
                ',"data-obj":' || v_SYS_TABSUBPART.DATAOBJ# ||
                ',"p-obj":' || v_SYS_TABSUBPART.POBJ# || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-ts":[');
    FOR v_SYS_TS IN (
        SELECT T.ROWID, T.TS#, T.NAME, T.BLOCKSIZE
        FROM SYS.TS$ AS OF SCN v_SCN T
        ORDER BY T.ROWID
    ) LOOP
        IF v_PREV = TRUE THEN
            DBMS_OUTPUT.PUT(',');
        ELSE
            v_PREV := TRUE;
        END IF;

        DBMS_OUTPUT.NEW_LINE();
        DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_TS.ROWID || '"' ||
            ',"ts":' || v_SYS_TS.TS# ||
            ',"name":"' || v_SYS_TS.NAME || '"' ||
            ',"block-size":' || v_SYS_TS.BLOCKSIZE || '}');
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"sys-user":[');
    FOR I IN v_USER_LIST.FIRST .. v_USER_LIST.LAST LOOP
        FOR v_SYS_USER IN (
            SELECT U.ROWID, U.USER#, U.NAME, MOD(NVL(U.SPARE1, 0), 18446744073709551616) AS SPARE11,
                MOD(TRUNC(NVL(U.SPARE1, 0) / 18446744073709551616), 18446744073709551616) AS SPARE12
            FROM SYS.USER$ AS OF SCN v_SCN U
            WHERE U.USER# = v_USER_LIST(I)
            ORDER BY U.ROWID
        ) LOOP
            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            IF (v_USERNAME_LIST(I) = 'SYS' AND v_SYS_SINGLE = 1) OR (v_USERNAME_LIST(I) = 'XDB' AND v_XDB_SINGLE = 1) THEN
                v_USER_SINGLE := 1;
            ELSE
                v_USER_SINGLE := 0;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_SYS_USER.ROWID || '"' ||
                ',"user":' || v_SYS_USER.USER# ||
                ',"name":"' || v_SYS_USER.NAME || '"' ||
                ',"spare1":[' || v_SYS_USER.SPARE11 || ',' || v_SYS_USER.SPARE12 ||']' ||
                ',"single":' || v_USER_SINGLE || '}');
        END LOOP;
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    v_PREV := FALSE;
    DBMS_OUTPUT.PUT(',"xdb-ttset":[');
    FOR v_XDB_TTSET IN (
        SELECT T.ROWID, T.GUID, T.TOKSUF, T.FLAGS, T.OBJ#
        FROM XDB.XDB$TTSET AS OF SCN v_SCN T
        ORDER BY T.ROWID
    ) LOOP
        IF v_PREV = TRUE THEN
            DBMS_OUTPUT.PUT(',');
        ELSE
            v_PREV := TRUE;
        END IF;

        DBMS_OUTPUT.NEW_LINE();
        DBMS_OUTPUT.PUT('{"row-id":"' || v_XDB_TTSET.ROWID || '"' ||
            ',"guid":"' || v_XDB_TTSET.GUID || '"' ||
            ',"toksuf":"' || v_XDB_TTSET.TOKSUF || '"' ||
            ',"flags":' || v_XDB_TTSET.FLAGS ||
            ',"obj":' || v_XDB_TTSET.OBJ# || '}');
    END LOOP;
    DBMS_OUTPUT.PUT(']');
    DBMS_OUTPUT.NEW_LINE();

    FOR v_XDB_TTSET IN (
        SELECT T.ROWID, T.GUID, T.TOKSUF, T.FLAGS, T.OBJ#
        FROM XDB.XDB$TTSET AS OF SCN v_SCN T
        ORDER BY T.ROWID
    ) LOOP
        v_PREV := FALSE;
        DBMS_OUTPUT.PUT(',"xdb-xnm' || v_XDB_TTSET.TOKSUF || '":[');
        OPEN CV FOR 'SELECT ROWID, NMSPCURI, ID FROM XDB.X$NM' || v_XDB_TTSET.TOKSUF || ' AS OF SCN ' || TO_CHAR(v_SCN);
        LOOP
            FETCH CV INTO v_XDB_X_NM;
            EXIT WHEN cv%NOTFOUND;

            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_XDB_X_NM.ROWID || '"' ||
                ',"nmspcuri":"' || v_XDB_X_NM.NMSPCURI || '"' ||
                ',"id":"' || v_XDB_X_NM.ID || '"}');
        END LOOP;
        DBMS_OUTPUT.PUT(']');
        DBMS_OUTPUT.NEW_LINE();

        v_PREV := FALSE;
        DBMS_OUTPUT.PUT(',"xdb-xpt' || v_XDB_TTSET.TOKSUF || '":[');
        OPEN CV FOR 'SELECT ROWID, PATH, ID FROM XDB.X$PT' || v_XDB_TTSET.TOKSUF || ' AS OF SCN ' || TO_CHAR(v_SCN);
        LOOP
            FETCH CV INTO v_XDB_X_PT;
            EXIT WHEN cv%NOTFOUND;

            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_XDB_X_PT.ROWID || '"' ||
                ',"path":"' || v_XDB_X_PT.PATH || '"' ||
                ',"id":"' || v_XDB_X_PT.ID|| '"}');
        END LOOP;
        DBMS_OUTPUT.PUT(']');
        DBMS_OUTPUT.NEW_LINE();

        v_PREV := FALSE;
        DBMS_OUTPUT.PUT(',"xdb-xqn' || v_XDB_TTSET.TOKSUF || '":[');
            OPEN CV FOR 'SELECT ROWID, NMSPCID, LOCALNAME, FLAGS, ID FROM XDB.X$QN' || v_XDB_TTSET.TOKSUF || ' AS OF SCN ' || TO_CHAR(v_SCN);
            LOOP
            FETCH CV INTO v_XDB_X_QN;
            EXIT WHEN cv%NOTFOUND;

            IF v_PREV = TRUE THEN
                DBMS_OUTPUT.PUT(',');
            ELSE
                v_PREV := TRUE;
            END IF;

            DBMS_OUTPUT.NEW_LINE();
            DBMS_OUTPUT.PUT('{"row-id":"' || v_XDB_X_QN.ROWID || '"' ||
                ',"nmspcid":"' || v_XDB_X_QN.NMSPCID || '"' ||
                ',"localname":"' || v_XDB_X_QN.LOCALNAME || '"' ||
                ',"flags":"' || v_XDB_X_QN.FLAGS || '"' ||
                ',"id":"' || v_XDB_X_QN.ID || '"}');
        END LOOP;
        DBMS_OUTPUT.PUT(']');
        DBMS_OUTPUT.NEW_LINE();
    END LOOP;

    DBMS_OUTPUT.PUT('}');
    DBMS_OUTPUT.NEW_LINE();
END;
/
