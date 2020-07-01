/* Thread reading Oracle Redo Logs
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
along with Open Log Replicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */


#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <dirent.h>
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

using namespace rapidjson;
using namespace std;
#ifdef ONLINE_MODEIMPL_OCCI
    using namespace oracle::occi;
#endif /* ONLINE_MODEIMPL_OCCI */

const Value& getJSONfield(string &fileName, const Value& value, const char* field);
const Value& getJSONfield(string &fileName, const Document& document, const char* field);

void stopMain();

namespace OpenLogReplicator {

    string OracleAnalyser::SQL_GET_ARCHIVE_LOG_LIST("SELECT NAME, SEQUENCE#, FIRST_CHANGE#, FIRST_TIME, NEXT_CHANGE#, NEXT_TIME FROM SYS.V_$ARCHIVED_LOG WHERE SEQUENCE# >= :i AND RESETLOGS_ID = :i AND ACTIVATION# = :i AND NAME IS NOT NULL ORDER BY SEQUENCE#, DEST_ID");
    string OracleAnalyser::SQL_GET_DATABASE_INFORMATION("SELECT D.LOG_MODE, D.SUPPLEMENTAL_LOG_DATA_MIN, D.SUPPLEMENTAL_LOG_DATA_PK, D.SUPPLEMENTAL_LOG_DATA_ALL, TP.ENDIAN_FORMAT, D.CURRENT_SCN, DI.RESETLOGS_ID, D.ACTIVATION#, VER.BANNER, SYS_CONTEXT('USERENV','DB_NAME') FROM SYS.V_$DATABASE D JOIN SYS.V_$TRANSPORTABLE_PLATFORM TP ON TP.PLATFORM_NAME = D.PLATFORM_NAME JOIN SYS.V_$VERSION VER ON VER.BANNER LIKE '%Oracle Database%' JOIN SYS.V_$DATABASE_INCARNATION DI ON DI.STATUS = 'CURRENT'");
    string OracleAnalyser::SQL_GET_CON_INFO("SELECT SYS_CONTEXT('USERENV','CON_ID'), SYS_CONTEXT('USERENV','CON_NAME') FROM DUAL");
    string OracleAnalyser::SQL_GET_CURRENT_SEQUENCE("SELECT SEQUENCE# FROM SYS.V_$LOG WHERE STATUS = 'CURRENT'");
    string OracleAnalyser::SQL_GET_LOGFILE_LIST("SELECT LF.GROUP#, LF.MEMBER FROM SYS.V_$LOGFILE LF ORDER BY LF.GROUP# ASC, LF.IS_RECOVERY_DEST_FILE DESC, LF.MEMBER ASC");
    string OracleAnalyser::SQL_GET_TABLE_LIST("SELECT T.DATAOBJ#, T.OBJ#, T.CLUCOLS, U.NAME, O.NAME, DECODE(BITAND(T.PROPERTY, 1024), 0, 0, 1), DECODE((BITAND(T.PROPERTY, 512)+BITAND(T.FLAGS, 536870912)), 0, 0, 1), DECODE(BITAND(U.SPARE1, 1), 1, 1, 0), DECODE(BITAND(U.SPARE1, 8), 8, 1, 0) FROM SYS.TAB$ T, SYS.OBJ$ O, SYS.USER$ U WHERE T.OBJ# = O.OBJ# AND BITAND(O.flags, 128) = 0 AND O.OWNER# = U.USER# AND U.NAME || '.' || O.NAME LIKE UPPER(:i) ORDER BY 4,5");
    string OracleAnalyser::SQL_GET_COLUMN_LIST("SELECT C.COL#, C.SEGCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.CHARSETFORM, C.CHARSETID, C.NULL$, (SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#), (SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0) FROM SYS.COL$ C WHERE C.SEGCOL# > 0 AND C.OBJ# = :i AND DECODE(BITAND(C.PROPERTY, 256), 0, 0, 1) = 0 ORDER BY C.SEGCOL#");
    string OracleAnalyser::SQL_GET_COLUMN_LIST_INV("SELECT C.COL#, C.SEGCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.CHARSETFORM, C.CHARSETID, C.NULL$, (SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#), (SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0) FROM SYS.COL$ C WHERE C.SEGCOL# > 0 AND C.OBJ# = :i AND DECODE(BITAND(C.PROPERTY, 256), 0, 0, 1) = 0 AND DECODE(BITAND(C.PROPERTY, 32), 0, 0, 1) = 0 ORDER BY C.SEGCOL#");
    string OracleAnalyser::SQL_GET_SUPPLEMNTAL_LOG_TABLE("SELECT C.TYPE# FROM SYS.CON$ OC, SYS.CDEF$ C WHERE OC.CON# = C.CON# AND (C.TYPE# = 14 OR C.TYPE# = 17) AND C.OBJ# = :i");
    string OracleAnalyser::SQL_GET_PARAMETER("SELECT VALUE FROM SYS.V_$PARAMETER WHERE NAME = :i");
    string OracleAnalyser::SQL_GET_PROPERTY("SELECT PROPERTY_VALUE FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME = :1");

    OracleAnalyser::OracleAnalyser(CommandBuffer *commandBuffer, const string alias, const string database, const string user,
            const string passwd, const string connectString, uint64_t trace, uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpRawData,
            uint64_t flags, uint64_t mode, uint64_t disableChecks, uint32_t redoReadSleep, uint64_t checkpointInterval, uint64_t redoBuffers,
            uint64_t redoBufferSize, uint64_t maxConcurrentTransactions) :
        Thread(alias),
#ifdef ONLINE_MODEIMPL_OCCI
        env(nullptr),
        conn(nullptr),
#endif /* ONLINE_MODEIMPL_OCCI */
        databaseSequence(0),
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
        mode(mode),
        disableChecks(disableChecks),
        redoReadSleep(redoReadSleep),
        trace(trace),
        trace2(trace2),
        version(0),
        conId(0),
        resetlogs(0),
        activation(0),
        isBigEndian(false),
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

        populateTimeZone();
#ifdef ONLINE_MODEIMPL_OCCI
            env = Environment::createEnvironment (Environment::DEFAULT);
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    OracleAnalyser::~OracleAnalyser() {
        readerDropAll();
        freeRollbackList();

        while (!archiveRedoQueue.empty()) {
            OracleAnalyserRedoLog *redoTmp = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete redoTmp;
        }

        closeDbConnection();

        delete transactionBuffer;

        for (auto it : objectMap) {
            OracleObject *object = it.second;
            delete object;
        }
        objectMap.clear();
        timeZoneMap.clear();

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

    stringstream& OracleAnalyser::writeEscapeValue(stringstream &ss, string &str) {
        const char *c_str = str.c_str();
        for (uint64_t i = 0; i < str.length(); ++i) {
            if (*c_str == '\t' || *c_str == '\r' || *c_str == '\n' || *c_str == '\b') {
                //skip
            } else if (*c_str == '"' || *c_str == '\\' || *c_str == '/') {
                ss << '\\' << *c_str;
            } else {
                ss << *c_str;
            }
            ++c_str;
        }
        return ss;
    }

    string OracleAnalyser::getParameterValue(const char *parameter) {
#ifdef ONLINE_MODEIMPL_OCCI
        try {
            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_PARAMETER << endl;
            stmt.createStatement(SQL_GET_PARAMETER);
            stmt.stmt->setString(1, parameter);
            stmt.executeQuery();

            if (stmt.rset->next())
                return stmt.rset->getString(1);
        } catch(SQLException &ex) {
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage() << " while getting parameter value for " << parameter;
            throw RuntimeException("getting parameter value");
        }

        //no value found
        throw RuntimeException("getting parameter value");
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    string OracleAnalyser::getPropertyValue(const char *property) {
#ifdef ONLINE_MODEIMPL_OCCI
        try {
            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_PROPERTY << endl;
            stmt.createStatement(SQL_GET_PROPERTY);
            stmt.stmt->setString(1, property);
            stmt.executeQuery();

            if (stmt.rset->next())
                return stmt.rset->getString(1);
        } catch(SQLException &ex) {
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage() << " while getting property value for " << property;
            throw RuntimeException("getting property value");
        }

        //no value found
        throw RuntimeException("getting property value");
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    void OracleAnalyser::populateTimeZone() {
        timeZoneMap[0x80a8] = "Africa/Abidjan";
        timeZoneMap[0x80c8] = "Africa/Accra";
        timeZoneMap[0x80bc] = "Africa/Addis_Ababa";
        timeZoneMap[0x8078] = "Africa/Algiers";
        timeZoneMap[0x80b8] = "Africa/Asmara";
        timeZoneMap[0x88b8] = "Africa/Asmera";
        timeZoneMap[0x80e8] = "Africa/Bamako";
        timeZoneMap[0x8094] = "Africa/Bangui";
        timeZoneMap[0x80c4] = "Africa/Banjul";
        timeZoneMap[0x80d0] = "Africa/Bissau";
        timeZoneMap[0x80e4] = "Africa/Blantyre";
        timeZoneMap[0x80a4] = "Africa/Brazzaville";
        timeZoneMap[0x808c] = "Africa/Bujumbura";
        timeZoneMap[0x80b0] = "Africa/Cairo";
        timeZoneMap[0x80f4] = "Africa/Casablanca";
        timeZoneMap[0x8144] = "Africa/Ceuta";
        timeZoneMap[0x80cc] = "Africa/Conakry";
        timeZoneMap[0x8114] = "Africa/Dakar";
        timeZoneMap[0x812c] = "Africa/Dar_es_Salaam";
        timeZoneMap[0x80ac] = "Africa/Djibouti";
        timeZoneMap[0x8090] = "Africa/Douala";
        timeZoneMap[0x80f8] = "Africa/El_Aaiun";
        timeZoneMap[0x8118] = "Africa/Freetown";
        timeZoneMap[0x8084] = "Africa/Gaborone";
        timeZoneMap[0x8140] = "Africa/Harare";
        timeZoneMap[0x8120] = "Africa/Johannesburg";
        timeZoneMap[0x8504] = "Africa/Juba";
        timeZoneMap[0x8138] = "Africa/Kampala";
        timeZoneMap[0x8124] = "Africa/Khartoum";
        timeZoneMap[0x810c] = "Africa/Kigali";
        timeZoneMap[0x809c] = "Africa/Kinshasa";
        timeZoneMap[0x8108] = "Africa/Lagos";
        timeZoneMap[0x80c0] = "Africa/Libreville";
        timeZoneMap[0x8130] = "Africa/Lome";
        timeZoneMap[0x807c] = "Africa/Luanda";
        timeZoneMap[0x80a0] = "Africa/Lubumbashi";
        timeZoneMap[0x813c] = "Africa/Lusaka";
        timeZoneMap[0x80b4] = "Africa/Malabo";
        timeZoneMap[0x80fc] = "Africa/Maputo";
        timeZoneMap[0x80d8] = "Africa/Maseru";
        timeZoneMap[0x8128] = "Africa/Mbabane";
        timeZoneMap[0x811c] = "Africa/Mogadishu";
        timeZoneMap[0x80dc] = "Africa/Monrovia";
        timeZoneMap[0x80d4] = "Africa/Nairobi";
        timeZoneMap[0x8098] = "Africa/Ndjamena";
        timeZoneMap[0x8104] = "Africa/Niamey";
        timeZoneMap[0x80f0] = "Africa/Nouakchott";
        timeZoneMap[0x8088] = "Africa/Ouagadougou";
        timeZoneMap[0x8080] = "Africa/Porto-Novo";
        timeZoneMap[0x8110] = "Africa/Sao_Tome";
        timeZoneMap[0x88e8] = "Africa/Timbuktu";
        timeZoneMap[0x80e0] = "Africa/Tripoli";
        timeZoneMap[0x8134] = "Africa/Tunis";
        timeZoneMap[0x8100] = "Africa/Windhoek";
        timeZoneMap[0x81b0] = "America/Adak";
        timeZoneMap[0x81a8] = "America/Anchorage";
        timeZoneMap[0x8248] = "America/Anguilla";
        timeZoneMap[0x824c] = "America/Antigua";
        timeZoneMap[0x82e8] = "America/Araguaina";
        timeZoneMap[0x8abc] = "America/Argentina/Buenos_Aires";
        timeZoneMap[0x8acc] = "America/Argentina/Catamarca";
        timeZoneMap[0x92cc] = "America/Argentina/ComodRivadavia";
        timeZoneMap[0x8ac4] = "America/Argentina/Cordoba";
        timeZoneMap[0x8ac8] = "America/Argentina/Jujuy";
        timeZoneMap[0x818c] = "America/Argentina/La_Rioja";
        timeZoneMap[0x8ad0] = "America/Argentina/Mendoza";
        timeZoneMap[0x8188] = "America/Argentina/Rio_Gallegos";
        timeZoneMap[0x83b4] = "America/Argentina/Salta";
        timeZoneMap[0x8394] = "America/Argentina/San_Juan";
        timeZoneMap[0x8184] = "America/Argentina/San_Luis";
        timeZoneMap[0x8390] = "America/Argentina/Tucuman";
        timeZoneMap[0x82c0] = "America/Argentina/Ushuaia";
        timeZoneMap[0x82d4] = "America/Aruba";
        timeZoneMap[0x8320] = "America/Asuncion";
        timeZoneMap[0x8374] = "America/Atikokan";
        timeZoneMap[0x89b0] = "America/Atka";
        timeZoneMap[0x8168] = "America/Bahia";
        timeZoneMap[0x817c] = "America/Bahia_Banderas";
        timeZoneMap[0x8254] = "America/Barbados";
        timeZoneMap[0x82e0] = "America/Belem";
        timeZoneMap[0x8258] = "America/Belize";
        timeZoneMap[0x8380] = "America/Blanc-Sablon";
        timeZoneMap[0x82fc] = "America/Boa_Vista";
        timeZoneMap[0x830c] = "America/Bogota";
        timeZoneMap[0x81b8] = "America/Boise";
        timeZoneMap[0x82bc] = "America/Buenos_Aires";
        timeZoneMap[0x821c] = "America/Cambridge_Bay";
        timeZoneMap[0x8378] = "America/Campo_Grande";
        timeZoneMap[0x8230] = "America/Cancun";
        timeZoneMap[0x8334] = "America/Caracas";
        timeZoneMap[0x82cc] = "America/Catamarca";
        timeZoneMap[0x8318] = "America/Cayenne";
        timeZoneMap[0x825c] = "America/Cayman";
        timeZoneMap[0x8194] = "America/Chicago";
        timeZoneMap[0x8238] = "America/Chihuahua";
        timeZoneMap[0x8b74] = "America/Coral_Harbour";
        timeZoneMap[0x82c4] = "America/Cordoba";
        timeZoneMap[0x8260] = "America/Costa_Rica";
        timeZoneMap[0x8514] = "America/Creston";
        timeZoneMap[0x82f4] = "America/Cuiaba";
        timeZoneMap[0x8310] = "America/Curacao";
        timeZoneMap[0x837c] = "America/Danmarkshavn";
        timeZoneMap[0x822c] = "America/Dawson";
        timeZoneMap[0x820c] = "America/Dawson_Creek";
        timeZoneMap[0x8198] = "America/Denver";
        timeZoneMap[0x81d0] = "America/Detroit";
        timeZoneMap[0x8268] = "America/Dominica";
        timeZoneMap[0x8204] = "America/Edmonton";
        timeZoneMap[0x8384] = "America/Eirunepe";
        timeZoneMap[0x8270] = "America/El_Salvador";
        timeZoneMap[0x8a44] = "America/Ensenada";
        timeZoneMap[0x82e4] = "America/Fortaleza";
        timeZoneMap[0x855c] = "America/Fort_Nelson";
        timeZoneMap[0x89bc] = "America/Fort_Wayne";
        timeZoneMap[0x81e4] = "America/Glace_Bay";
        timeZoneMap[0x833c] = "America/Godthab";
        timeZoneMap[0x81dc] = "America/Goose_Bay";
        timeZoneMap[0x82b0] = "America/Grand_Turk";
        timeZoneMap[0x8274] = "America/Grenada";
        timeZoneMap[0x8278] = "America/Guadeloupe";
        timeZoneMap[0x827c] = "America/Guatemala";
        timeZoneMap[0x8314] = "America/Guayaquil";
        timeZoneMap[0x831c] = "America/Guyana";
        timeZoneMap[0x81e0] = "America/Halifax";
        timeZoneMap[0x8264] = "America/Havana";
        timeZoneMap[0x823c] = "America/Hermosillo";
        timeZoneMap[0x99bc] = "America/Indiana/Indianapolis";
        timeZoneMap[0x81c4] = "America/Indiana/Knox";
        timeZoneMap[0x81c0] = "America/Indiana/Marengo";
        timeZoneMap[0x8348] = "America/Indiana/Petersburg";
        timeZoneMap[0x81bc] = "America/Indianapolis";
        timeZoneMap[0x8178] = "America/Indiana/Tell_City";
        timeZoneMap[0x81c8] = "America/Indiana/Vevay";
        timeZoneMap[0x8344] = "America/Indiana/Vincennes";
        timeZoneMap[0x8368] = "America/Indiana/Winamac";
        timeZoneMap[0x8224] = "America/Inuvik";
        timeZoneMap[0x8214] = "America/Iqaluit";
        timeZoneMap[0x8288] = "America/Jamaica";
        timeZoneMap[0x82c8] = "America/Jujuy";
        timeZoneMap[0x81a0] = "America/Juneau";
        timeZoneMap[0x89cc] = "America/Kentucky/Louisville";
        timeZoneMap[0x816c] = "America/Kentucky/Monticello";
        timeZoneMap[0x89c4] = "America/Knox_IN";
        timeZoneMap[0x850c] = "America/Kralendijk";
        timeZoneMap[0x82d8] = "America/La_Paz";
        timeZoneMap[0x8324] = "America/Lima";
        timeZoneMap[0x819c] = "America/Los_Angeles";
        timeZoneMap[0x81cc] = "America/Louisville";
        timeZoneMap[0x8508] = "America/Lower_Princes";
        timeZoneMap[0x82ec] = "America/Maceio";
        timeZoneMap[0x8294] = "America/Managua";
        timeZoneMap[0x8300] = "America/Manaus";
        timeZoneMap[0x8a78] = "America/Marigot";
        timeZoneMap[0x828c] = "America/Martinique";
        timeZoneMap[0x815c] = "America/Matamoros";
        timeZoneMap[0x8240] = "America/Mazatlan";
        timeZoneMap[0x82d0] = "America/Mendoza";
        timeZoneMap[0x81d4] = "America/Menominee";
        timeZoneMap[0x8388] = "America/Merida";
        timeZoneMap[0x84fc] = "America/Metlakatla";
        timeZoneMap[0x8234] = "America/Mexico_City";
        timeZoneMap[0x82a8] = "America/Miquelon";
        timeZoneMap[0x8170] = "America/Moncton";
        timeZoneMap[0x838c] = "America/Monterrey";
        timeZoneMap[0x8330] = "America/Montevideo";
        timeZoneMap[0x81e8] = "America/Montreal";
        timeZoneMap[0x8290] = "America/Montserrat";
        timeZoneMap[0x8250] = "America/Nassau";
        timeZoneMap[0x8190] = "America/New_York";
        timeZoneMap[0x81f0] = "America/Nipigon";
        timeZoneMap[0x81ac] = "America/Nome";
        timeZoneMap[0x82dc] = "America/Noronha";
        timeZoneMap[0x8500] = "America/North_Dakota/Beulah";
        timeZoneMap[0x8160] = "America/North_Dakota/Center";
        timeZoneMap[0x8164] = "America/North_Dakota/New_Salem";
        timeZoneMap[0x8174] = "America/Ojinaga";
        timeZoneMap[0x8298] = "America/Panama";
        timeZoneMap[0x8210] = "America/Pangnirtung";
        timeZoneMap[0x8328] = "America/Paramaribo";
        timeZoneMap[0x81b4] = "America/Phoenix";
        timeZoneMap[0x8280] = "America/Port-au-Prince";
        timeZoneMap[0x8304] = "America/Porto_Acre";
        timeZoneMap[0x832c] = "America/Port_of_Spain";
        timeZoneMap[0x82f8] = "America/Porto_Velho";
        timeZoneMap[0x829c] = "America/Puerto_Rico";
        timeZoneMap[0x8628] = "America/Punta_Arenas";
        timeZoneMap[0x81f4] = "America/Rainy_River";
        timeZoneMap[0x8218] = "America/Rankin_Inlet";
        timeZoneMap[0x8158] = "America/Recife";
        timeZoneMap[0x81fc] = "America/Regina";
        timeZoneMap[0x836c] = "America/Resolute";
        timeZoneMap[0x9304] = "America/Rio_Branco";
        timeZoneMap[0x92c4] = "America/Rosario";
        timeZoneMap[0x8180] = "America/Santa_Isabel";
        timeZoneMap[0x814c] = "America/Santarem";
        timeZoneMap[0x8308] = "America/Santiago";
        timeZoneMap[0x826c] = "America/Santo_Domingo";
        timeZoneMap[0x82f0] = "America/Sao_Paulo";
        timeZoneMap[0x8338] = "America/Scoresbysund";
        timeZoneMap[0x9998] = "America/Shiprock";
        timeZoneMap[0x84f8] = "America/Sitka";
        timeZoneMap[0x9278] = "America/St_Barthelemy";
        timeZoneMap[0x81d8] = "America/St_Johns";
        timeZoneMap[0x82a0] = "America/St_Kitts";
        timeZoneMap[0x82a4] = "America/St_Lucia";
        timeZoneMap[0x82b8] = "America/St_Thomas";
        timeZoneMap[0x82ac] = "America/St_Vincent";
        timeZoneMap[0x8200] = "America/Swift_Current";
        timeZoneMap[0x8284] = "America/Tegucigalpa";
        timeZoneMap[0x8340] = "America/Thule";
        timeZoneMap[0x81ec] = "America/Thunder_Bay";
        timeZoneMap[0x8244] = "America/Tijuana";
        timeZoneMap[0x8370] = "America/Toronto";
        timeZoneMap[0x82b4] = "America/Tortola";
        timeZoneMap[0x8208] = "America/Vancouver";
        timeZoneMap[0x8ab8] = "America/Virgin";
        timeZoneMap[0x8228] = "America/Whitehorse";
        timeZoneMap[0x81f8] = "America/Winnipeg";
        timeZoneMap[0x81a4] = "America/Yakutat";
        timeZoneMap[0x8220] = "America/Yellowknife";
        timeZoneMap[0x8398] = "Antarctica/Casey";
        timeZoneMap[0x839c] = "Antarctica/Davis";
        timeZoneMap[0x83a4] = "Antarctica/DumontDUrville";
        timeZoneMap[0x8154] = "Antarctica/Macquarie";
        timeZoneMap[0x83a0] = "Antarctica/Mawson";
        timeZoneMap[0x83b0] = "Antarctica/McMurdo";
        timeZoneMap[0x83ac] = "Antarctica/Palmer";
        timeZoneMap[0x8148] = "Antarctica/Rothera";
        timeZoneMap[0x8bb0] = "Antarctica/South_Pole";
        timeZoneMap[0x83a8] = "Antarctica/Syowa";
        timeZoneMap[0x8524] = "Antarctica/Troll";
        timeZoneMap[0x80ec] = "Antarctica/Vostok";
        timeZoneMap[0x8e34] = "Arctic/Longyearbyen";
        timeZoneMap[0x84b8] = "Asia/Aden";
        timeZoneMap[0x8434] = "Asia/Almaty";
        timeZoneMap[0x8430] = "Asia/Amman";
        timeZoneMap[0x84e0] = "Asia/Anadyr";
        timeZoneMap[0x843c] = "Asia/Aqtau";
        timeZoneMap[0x8438] = "Asia/Aqtobe";
        timeZoneMap[0x84a4] = "Asia/Ashgabat";
        timeZoneMap[0x8ca4] = "Asia/Ashkhabad";
        timeZoneMap[0x85ac] = "Asia/Atyrau";
        timeZoneMap[0x8424] = "Asia/Baghdad";
        timeZoneMap[0x83cc] = "Asia/Bahrain";
        timeZoneMap[0x83c8] = "Asia/Baku";
        timeZoneMap[0x84a0] = "Asia/Bangkok";
        timeZoneMap[0x859c] = "Asia/Barnaul";
        timeZoneMap[0x8454] = "Asia/Beirut";
        timeZoneMap[0x8440] = "Asia/Bishkek";
        timeZoneMap[0x83d8] = "Asia/Brunei";
        timeZoneMap[0x8410] = "Asia/Calcutta";
        timeZoneMap[0x853c] = "Asia/Chita";
        timeZoneMap[0x84f0] = "Asia/Choibalsan";
        timeZoneMap[0x8bec] = "Asia/Chongqing";
        timeZoneMap[0x83ec] = "Asia/Chungking";
        timeZoneMap[0x8494] = "Asia/Colombo";
        timeZoneMap[0x83d0] = "Asia/Dacca";
        timeZoneMap[0x8498] = "Asia/Damascus";
        timeZoneMap[0x8bd0] = "Asia/Dhaka";
        timeZoneMap[0x840c] = "Asia/Dili";
        timeZoneMap[0x84a8] = "Asia/Dubai";
        timeZoneMap[0x849c] = "Asia/Dushanbe";
        timeZoneMap[0x85a8] = "Asia/Famagusta";
        timeZoneMap[0x8474] = "Asia/Gaza";
        timeZoneMap[0x83e4] = "Asia/Harbin";
        timeZoneMap[0x8510] = "Asia/Hebron";
        timeZoneMap[0x8cb4] = "Asia/Ho_Chi_Minh";
        timeZoneMap[0x83f8] = "Asia/Hong_Kong";
        timeZoneMap[0x8460] = "Asia/Hovd";
        timeZoneMap[0x84cc] = "Asia/Irkutsk";
        timeZoneMap[0x965c] = "Asia/Istanbul";
        timeZoneMap[0x8414] = "Asia/Jakarta";
        timeZoneMap[0x841c] = "Asia/Jayapura";
        timeZoneMap[0x8428] = "Asia/Jerusalem";
        timeZoneMap[0x83c0] = "Asia/Kabul";
        timeZoneMap[0x84dc] = "Asia/Kamchatka";
        timeZoneMap[0x8470] = "Asia/Karachi";
        timeZoneMap[0x83f4] = "Asia/Kashgar";
        timeZoneMap[0x8c74] = "Asia/Kathmandu";
        timeZoneMap[0x8468] = "Asia/Katmandu";
        timeZoneMap[0x8518] = "Asia/Khandyga";
        timeZoneMap[0x8c10] = "Asia/Kolkata";
        timeZoneMap[0x84c8] = "Asia/Krasnoyarsk";
        timeZoneMap[0x8458] = "Asia/Kuala_Lumpur";
        timeZoneMap[0x845c] = "Asia/Kuching";
        timeZoneMap[0x844c] = "Asia/Kuwait";
        timeZoneMap[0x8400] = "Asia/Macao";
        timeZoneMap[0x8c00] = "Asia/Macau";
        timeZoneMap[0x84d8] = "Asia/Magadan";
        timeZoneMap[0x8c18] = "Asia/Makassar";
        timeZoneMap[0x8478] = "Asia/Manila";
        timeZoneMap[0x846c] = "Asia/Muscat";
        timeZoneMap[0x8404] = "Asia/Nicosia";
        timeZoneMap[0x8150] = "Asia/Novokuznetsk";
        timeZoneMap[0x84c4] = "Asia/Novosibirsk";
        timeZoneMap[0x84c0] = "Asia/Omsk";
        timeZoneMap[0x84ec] = "Asia/Oral";
        timeZoneMap[0x83e0] = "Asia/Phnom_Penh";
        timeZoneMap[0x84e4] = "Asia/Pontianak";
        timeZoneMap[0x8448] = "Asia/Pyongyang";
        timeZoneMap[0x847c] = "Asia/Qatar";
        timeZoneMap[0x84e8] = "Asia/Qyzylorda";
        timeZoneMap[0x83dc] = "Asia/Rangoon";
        timeZoneMap[0x8480] = "Asia/Riyadh";
        timeZoneMap[0x84b4] = "Asia/Saigon";
        timeZoneMap[0x84f4] = "Asia/Sakhalin";
        timeZoneMap[0x84ac] = "Asia/Samarkand";
        timeZoneMap[0x8444] = "Asia/Seoul";
        timeZoneMap[0x83e8] = "Asia/Shanghai";
        timeZoneMap[0x8490] = "Asia/Singapore";
        timeZoneMap[0x8554] = "Asia/Srednekolymsk";
        timeZoneMap[0x83fc] = "Asia/Taipei";
        timeZoneMap[0x84b0] = "Asia/Tashkent";
        timeZoneMap[0x8408] = "Asia/Tbilisi";
        timeZoneMap[0x8420] = "Asia/Tehran";
        timeZoneMap[0x8c28] = "Asia/Tel_Aviv";
        timeZoneMap[0x8bd4] = "Asia/Thimbu";
        timeZoneMap[0x83d4] = "Asia/Thimphu";
        timeZoneMap[0x842c] = "Asia/Tokyo";
        timeZoneMap[0x85a0] = "Asia/Tomsk";
        timeZoneMap[0x8418] = "Asia/Ujung_Pandang";
        timeZoneMap[0x8464] = "Asia/Ulaanbaatar";
        timeZoneMap[0x8c64] = "Asia/Ulan_Bator";
        timeZoneMap[0x83f0] = "Asia/Urumqi";
        timeZoneMap[0x851c] = "Asia/Ust-Nera";
        timeZoneMap[0x8450] = "Asia/Vientiane";
        timeZoneMap[0x84d4] = "Asia/Vladivostok";
        timeZoneMap[0x84d0] = "Asia/Yakutsk";
        timeZoneMap[0x85a4] = "Asia/Yangon";
        timeZoneMap[0x84bc] = "Asia/Yekaterinburg";
        timeZoneMap[0x83c4] = "Asia/Yerevan";
        timeZoneMap[0x8540] = "Atlantic/Azores";
        timeZoneMap[0x8528] = "Atlantic/Bermuda";
        timeZoneMap[0x8548] = "Atlantic/Canary";
        timeZoneMap[0x854c] = "Atlantic/Cape_Verde";
        timeZoneMap[0x8d34] = "Atlantic/Faeroe";
        timeZoneMap[0x8534] = "Atlantic/Faroe";
        timeZoneMap[0x9634] = "Atlantic/Jan_Mayen";
        timeZoneMap[0x8544] = "Atlantic/Madeira";
        timeZoneMap[0x8538] = "Atlantic/Reykjavik";
        timeZoneMap[0x8530] = "Atlantic/South_Georgia";
        timeZoneMap[0x852c] = "Atlantic/Stanley";
        timeZoneMap[0x8550] = "Atlantic/St_Helena";
        timeZoneMap[0x8d80] = "Australia/ACT";
        timeZoneMap[0x8574] = "Australia/Adelaide";
        timeZoneMap[0x856c] = "Australia/Brisbane";
        timeZoneMap[0x8584] = "Australia/Broken_Hill";
        timeZoneMap[0x9580] = "Australia/Canberra";
        timeZoneMap[0x858c] = "Australia/Currie";
        timeZoneMap[0x8564] = "Australia/Darwin";
        timeZoneMap[0x8590] = "Australia/Eucla";
        timeZoneMap[0x8578] = "Australia/Hobart";
        timeZoneMap[0x8d88] = "Australia/LHI";
        timeZoneMap[0x8570] = "Australia/Lindeman";
        timeZoneMap[0x8588] = "Australia/Lord_Howe";
        timeZoneMap[0x857c] = "Australia/Melbourne";
        timeZoneMap[0x8d64] = "Australia/North";
        timeZoneMap[0x9d80] = "Australia/NSW";
        timeZoneMap[0x8568] = "Australia/Perth";
        timeZoneMap[0x8d6c] = "Australia/Queensland";
        timeZoneMap[0x8d74] = "Australia/South";
        timeZoneMap[0x8580] = "Australia/Sydney";
        timeZoneMap[0x8d78] = "Australia/Tasmania";
        timeZoneMap[0x8d7c] = "Australia/Victoria";
        timeZoneMap[0x8d68] = "Australia/West";
        timeZoneMap[0x8d84] = "Australia/Yancowinna";
        timeZoneMap[0x8b04] = "Brazil/Acre";
        timeZoneMap[0x8adc] = "Brazil/DeNoronha";
        timeZoneMap[0x8af0] = "Brazil/East";
        timeZoneMap[0x8b00] = "Brazil/West";
        timeZoneMap[0x89e0] = "Canada/Atlantic";
        timeZoneMap[0x89f8] = "Canada/Central";
        timeZoneMap[0x89e8] = "Canada/Eastern";
        timeZoneMap[0x89fc] = "Canada/East-Saskatchewan";
        timeZoneMap[0x8a04] = "Canada/Mountain";
        timeZoneMap[0x89d8] = "Canada/Newfoundland";
        timeZoneMap[0x8a08] = "Canada/Pacific";
        timeZoneMap[0x91fc] = "Canada/Saskatchewan";
        timeZoneMap[0x8a28] = "Canada/Yukon";
        timeZoneMap[0x85b8] = "CET";
        timeZoneMap[0x8b08] = "Chile/Continental";
        timeZoneMap[0x8f0c] = "Chile/EasterIsland";
        timeZoneMap[0x9994] = "CST";
        timeZoneMap[0x835c] = "CST6CDT";
        timeZoneMap[0x8a64] = "Cuba";
        timeZoneMap[0x85c0] = "EET";
        timeZoneMap[0x88b0] = "Egypt";
        timeZoneMap[0x8dcc] = "Eire";
        timeZoneMap[0x834c] = "EST";
        timeZoneMap[0x8358] = "EST5EDT";
        timeZoneMap[0x9004] = "Etc/GMT+0";
        timeZoneMap[0xa004] = "Etc/GMT-0";
        timeZoneMap[0xb004] = "Etc/GMT0";
        timeZoneMap[0x8004] = "Etc/GMT";
        timeZoneMap[0x8018] = "Etc/GMT-10";
        timeZoneMap[0x8064] = "Etc/GMT+10";
        timeZoneMap[0x803c] = "Etc/GMT-1";
        timeZoneMap[0x8040] = "Etc/GMT+1";
        timeZoneMap[0x8014] = "Etc/GMT-11";
        timeZoneMap[0x8068] = "Etc/GMT+11";
        timeZoneMap[0x8010] = "Etc/GMT-12";
        timeZoneMap[0x806c] = "Etc/GMT+12";
        timeZoneMap[0x800c] = "Etc/GMT-13";
        timeZoneMap[0x8008] = "Etc/GMT-14";
        timeZoneMap[0x8038] = "Etc/GMT-2";
        timeZoneMap[0x8044] = "Etc/GMT+2";
        timeZoneMap[0x8034] = "Etc/GMT-3";
        timeZoneMap[0x8048] = "Etc/GMT+3";
        timeZoneMap[0x8030] = "Etc/GMT-4";
        timeZoneMap[0x804c] = "Etc/GMT+4";
        timeZoneMap[0x802c] = "Etc/GMT-5";
        timeZoneMap[0x8050] = "Etc/GMT+5";
        timeZoneMap[0x8028] = "Etc/GMT-6";
        timeZoneMap[0x8054] = "Etc/GMT+6";
        timeZoneMap[0x8024] = "Etc/GMT-7";
        timeZoneMap[0x8058] = "Etc/GMT+7";
        timeZoneMap[0x8020] = "Etc/GMT-8";
        timeZoneMap[0x805c] = "Etc/GMT+8";
        timeZoneMap[0x801c] = "Etc/GMT-9";
        timeZoneMap[0x8060] = "Etc/GMT+9";
        timeZoneMap[0xc004] = "Etc/Greenwich";
        timeZoneMap[0x8074] = "Etc/UCT";
        timeZoneMap[0x8870] = "Etc/Universal";
        timeZoneMap[0x8070] = "Etc/UTC";
        timeZoneMap[0x9870] = "Etc/Zulu";
        timeZoneMap[0x8630] = "Europe/Amsterdam";
        timeZoneMap[0x85d4] = "Europe/Andorra";
        timeZoneMap[0x8560] = "Europe/Astrakhan";
        timeZoneMap[0x8604] = "Europe/Athens";
        timeZoneMap[0x85c8] = "Europe/Belfast";
        timeZoneMap[0x8670] = "Europe/Belgrade";
        timeZoneMap[0x85fc] = "Europe/Berlin";
        timeZoneMap[0x8de8] = "Europe/Bratislava";
        timeZoneMap[0x85e0] = "Europe/Brussels";
        timeZoneMap[0x8640] = "Europe/Bucharest";
        timeZoneMap[0x8608] = "Europe/Budapest";
        timeZoneMap[0x8520] = "Europe/Busingen";
        timeZoneMap[0x8624] = "Europe/Chisinau";
        timeZoneMap[0x85ec] = "Europe/Copenhagen";
        timeZoneMap[0x85cc] = "Europe/Dublin";
        timeZoneMap[0x8600] = "Europe/Gibraltar";
        timeZoneMap[0xa5c4] = "Europe/Guernsey";
        timeZoneMap[0x85f4] = "Europe/Helsinki";
        timeZoneMap[0xadc4] = "Europe/Isle_of_Man";
        timeZoneMap[0x865c] = "Europe/Istanbul";
        timeZoneMap[0x9dc4] = "Europe/Jersey";
        timeZoneMap[0x8644] = "Europe/Kaliningrad";
        timeZoneMap[0x8660] = "Europe/Kiev";
        timeZoneMap[0x8594] = "Europe/Kirov";
        timeZoneMap[0x863c] = "Europe/Lisbon";
        timeZoneMap[0x8e70] = "Europe/Ljubljana";
        timeZoneMap[0x85c4] = "Europe/London";
        timeZoneMap[0x861c] = "Europe/Luxembourg";
        timeZoneMap[0x8650] = "Europe/Madrid";
        timeZoneMap[0x8620] = "Europe/Malta";
        timeZoneMap[0x8df4] = "Europe/Mariehamn";
        timeZoneMap[0x85dc] = "Europe/Minsk";
        timeZoneMap[0x862c] = "Europe/Monaco";
        timeZoneMap[0x8648] = "Europe/Moscow";
        timeZoneMap[0x8c04] = "Europe/Nicosia";
        timeZoneMap[0x8634] = "Europe/Oslo";
        timeZoneMap[0x85f8] = "Europe/Paris";
        timeZoneMap[0xae70] = "Europe/Podgorica";
        timeZoneMap[0x85e8] = "Europe/Prague";
        timeZoneMap[0x8610] = "Europe/Riga";
        timeZoneMap[0x860c] = "Europe/Rome";
        timeZoneMap[0x864c] = "Europe/Samara";
        timeZoneMap[0x960c] = "Europe/San_Marino";
        timeZoneMap[0x9670] = "Europe/Sarajevo";
        timeZoneMap[0x85b0] = "Europe/Saratov";
        timeZoneMap[0x866c] = "Europe/Simferopol";
        timeZoneMap[0x9e70] = "Europe/Skopje";
        timeZoneMap[0x85e4] = "Europe/Sofia";
        timeZoneMap[0x8654] = "Europe/Stockholm";
        timeZoneMap[0x85f0] = "Europe/Tallinn";
        timeZoneMap[0x85d0] = "Europe/Tirane";
        timeZoneMap[0x8e24] = "Europe/Tiraspol";
        timeZoneMap[0x8598] = "Europe/Ulyanovsk";
        timeZoneMap[0x8664] = "Europe/Uzhgorod";
        timeZoneMap[0x8614] = "Europe/Vaduz";
        timeZoneMap[0x8e0c] = "Europe/Vatican";
        timeZoneMap[0x85d8] = "Europe/Vienna";
        timeZoneMap[0x8618] = "Europe/Vilnius";
        timeZoneMap[0x8674] = "Europe/Volgograd";
        timeZoneMap[0x8638] = "Europe/Warsaw";
        timeZoneMap[0xa670] = "Europe/Zagreb";
        timeZoneMap[0x8668] = "Europe/Zaporozhye";
        timeZoneMap[0x8658] = "Europe/Zurich";
        timeZoneMap[0x8dc4] = "GB";
        timeZoneMap[0x95c4] = "GB-Eire";
        timeZoneMap[0x9804] = "GMT+0";
        timeZoneMap[0xa804] = "GMT-0";
        timeZoneMap[0xb804] = "GMT0";
        timeZoneMap[0x8804] = "GMT";
        timeZoneMap[0xc804] = "Greenwich";
        timeZoneMap[0x8bf8] = "Hongkong";
        timeZoneMap[0x8354] = "HST";
        timeZoneMap[0x8d38] = "Iceland";
        timeZoneMap[0x86d8] = "Indian/Antananarivo";
        timeZoneMap[0x86d0] = "Indian/Chagos";
        timeZoneMap[0x86dc] = "Indian/Christmas";
        timeZoneMap[0x86e0] = "Indian/Cocos";
        timeZoneMap[0x86e4] = "Indian/Comoro";
        timeZoneMap[0x86cc] = "Indian/Kerguelen";
        timeZoneMap[0x86e8] = "Indian/Mahe";
        timeZoneMap[0x86d4] = "Indian/Maldives";
        timeZoneMap[0x86ec] = "Indian/Mauritius";
        timeZoneMap[0x86f0] = "Indian/Mayotte";
        timeZoneMap[0x86f4] = "Indian/Reunion";
        timeZoneMap[0x8c20] = "Iran";
        timeZoneMap[0x9428] = "Israel";
        timeZoneMap[0x8a88] = "Jamaica";
        timeZoneMap[0x8c2c] = "Japan";
        timeZoneMap[0x8f40] = "Kwajalein";
        timeZoneMap[0x88e0] = "Libya";
        timeZoneMap[0x85bc] = "MET";
        timeZoneMap[0x9244] = "Mexico/BajaNorte";
        timeZoneMap[0x8a40] = "Mexico/BajaSur";
        timeZoneMap[0x8a34] = "Mexico/General";
        timeZoneMap[0x8350] = "MST";
        timeZoneMap[0x8360] = "MST7MDT";
        timeZoneMap[0x8998] = "Navajo";
        timeZoneMap[0x8f5c] = "NZ";
        timeZoneMap[0x8f60] = "NZ-CHAT";
        timeZoneMap[0x877c] = "Pacific/Apia";
        timeZoneMap[0x875c] = "Pacific/Auckland";
        timeZoneMap[0x8558] = "Pacific/Bougainville";
        timeZoneMap[0x8760] = "Pacific/Chatham";
        timeZoneMap[0x83b8] = "Pacific/Chuuk";
        timeZoneMap[0x870c] = "Pacific/Easter";
        timeZoneMap[0x87a0] = "Pacific/Efate";
        timeZoneMap[0x8730] = "Pacific/Enderbury";
        timeZoneMap[0x8788] = "Pacific/Fakaofo";
        timeZoneMap[0x8718] = "Pacific/Fiji";
        timeZoneMap[0x8790] = "Pacific/Funafuti";
        timeZoneMap[0x8710] = "Pacific/Galapagos";
        timeZoneMap[0x871c] = "Pacific/Gambier";
        timeZoneMap[0x8784] = "Pacific/Guadalcanal";
        timeZoneMap[0x8728] = "Pacific/Guam";
        timeZoneMap[0x8708] = "Pacific/Honolulu";
        timeZoneMap[0x8794] = "Pacific/Johnston";
        timeZoneMap[0x8734] = "Pacific/Kiritimati";
        timeZoneMap[0x8750] = "Pacific/Kosrae";
        timeZoneMap[0x8740] = "Pacific/Kwajalein";
        timeZoneMap[0x873c] = "Pacific/Majuro";
        timeZoneMap[0x8720] = "Pacific/Marquesas";
        timeZoneMap[0x8798] = "Pacific/Midway";
        timeZoneMap[0x8754] = "Pacific/Nauru";
        timeZoneMap[0x8764] = "Pacific/Niue";
        timeZoneMap[0x8768] = "Pacific/Norfolk";
        timeZoneMap[0x8758] = "Pacific/Noumea";
        timeZoneMap[0x8778] = "Pacific/Pago_Pago";
        timeZoneMap[0x876c] = "Pacific/Palau";
        timeZoneMap[0x8774] = "Pacific/Pitcairn";
        timeZoneMap[0x83bc] = "Pacific/Pohnpei";
        timeZoneMap[0x874c] = "Pacific/Ponape";
        timeZoneMap[0x8770] = "Pacific/Port_Moresby";
        timeZoneMap[0x8714] = "Pacific/Rarotonga";
        timeZoneMap[0x8738] = "Pacific/Saipan";
        timeZoneMap[0x9778] = "Pacific/Samoa";
        timeZoneMap[0x8724] = "Pacific/Tahiti";
        timeZoneMap[0x872c] = "Pacific/Tarawa";
        timeZoneMap[0x878c] = "Pacific/Tongatapu";
        timeZoneMap[0x8748] = "Pacific/Truk";
        timeZoneMap[0x879c] = "Pacific/Wake";
        timeZoneMap[0x87a4] = "Pacific/Wallis";
        timeZoneMap[0x8f48] = "Pacific/Yap";
        timeZoneMap[0x8e38] = "Poland";
        timeZoneMap[0x8e3c] = "Portugal";
        timeZoneMap[0x8be8] = "PRC";
        timeZoneMap[0xa19c] = "PST";
        timeZoneMap[0x8364] = "PST8PDT";
        timeZoneMap[0x8bfc] = "ROC";
        timeZoneMap[0x8c44] = "ROK";
        timeZoneMap[0x8c90] = "Singapore";
        timeZoneMap[0x8e5c] = "Turkey";
        timeZoneMap[0x8874] = "UCT";
        timeZoneMap[0x9070] = "Universal";
        timeZoneMap[0x89a8] = "US/Alaska";
        timeZoneMap[0x91b0] = "US/Aleutian";
        timeZoneMap[0x89b4] = "US/Arizona";
        timeZoneMap[0x8994] = "US/Central";
        timeZoneMap[0x8990] = "US/Eastern";
        timeZoneMap[0x91bc] = "US/East-Indiana";
        timeZoneMap[0x8f08] = "US/Hawaii";
        timeZoneMap[0x91c4] = "US/Indiana-Starke";
        timeZoneMap[0x89d0] = "US/Michigan";
        timeZoneMap[0x9198] = "US/Mountain";
        timeZoneMap[0x899c] = "US/Pacific";
        timeZoneMap[0x999c] = "US/Pacific-New";
        timeZoneMap[0x8f78] = "US/Samoa";
        timeZoneMap[0xd004] = "UTC";
        timeZoneMap[0x85b4] = "WET";
        timeZoneMap[0x8e48] = "W-SU";
        timeZoneMap[0xa070] = "Zulu";
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

        string fileName = database + "-chkpt.json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open())
            throw RuntimeException("writing checkpoint data to <database>-chkpt.json");

        stringstream ss;
        ss << "{\"database\":\"" << database
                << "\",\"sequence\":" << dec << minSequence
                << ",\"scn\":" << dec << databaseScn
                << ",\"resetlogs\":" << dec << resetlogs
                << ",\"activation\":" << dec << activation << "}";

        outfile << ss.rdbuf();
        outfile.close();

        if (atShutdown) {
            if (trace >= TRACE_INFO) {
                cerr << "INFO: Writing checkpoint at exit for " << database << endl;
                cerr << "INFO: sequence: " << dec << minSequence <<
                        " scn: " << dec << databaseScn <<
                        " resetlogs: " << dec << resetlogs <<
                        " activation: " << dec << activation;
                if (conId > 0)
                    cerr << " con_id: " << dec << conId <<
                            " con_name: " << conName;
                cerr << endl;
            }
        }

        previousCheckpoint = now;
    }

    void OracleAnalyser::readCheckpoint(void) {
        ifstream infile;
        string fileName = database + "-chkpt.json";
        infile.open(fileName.c_str(), ios::in);
        if (!infile.is_open()) {
            if (mode == MODE_OFFLINE || mode == MODE_ARCHIVELOG)
                throw RuntimeException("checkpoint file <database>-chkpt.json is required for offline mode");
            return;
        }

        string configJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
            throw RuntimeException("parsing of <database>-chkpt.json");

        const Value& databaseJSON = getJSONfield(fileName, document, "database");
        if (database.compare(databaseJSON.GetString()) != 0)
            throw RuntimeException("parsing of <database>-chkpt.json - invalid database name");

        const Value& databaseSequenceJSON = getJSONfield(fileName, document, "sequence");
        databaseSequence = databaseSequenceJSON.GetUint64();

        const Value& resetlogsJSON = getJSONfield(fileName, document, "resetlogs");
        typeresetlogs resetlogsRead = resetlogsJSON.GetUint64();
        if (resetlogs != resetlogsRead) {
            cerr << "ERROR: resetlogs id read from checkpoint JSON: " << dec << resetlogsRead << ", current: " << resetlogs << endl;
            throw RuntimeException("parsing of <database>-chkpt.json - incorrect resetlogs");
        }

        const Value& activationJSON = getJSONfield(fileName, document, "activation");
        typeactivation activationRead = activationJSON.GetUint64();
        if (activation != activationRead) {
            cerr << "ERROR: activation id read from checkpoint JSON: " << dec << activationRead << ", current: " << activation << endl;
            throw RuntimeException("parsing of <database>-chkpt.json - incorrect activation");
        }

        const Value& scnJSON = getJSONfield(fileName, document, "scn");
        databaseScn = scnJSON.GetUint64();

        infile.close();
    }

    void OracleAnalyser::addToDict(OracleObject *object) {
        if (objectMap[object->objn] == nullptr) {
            objectMap[object->objn] = object;
        }
    }

    void OracleAnalyser::checkConnection(bool reconnect) {
#ifdef ONLINE_MODEIMPL_OCCI
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
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    void OracleAnalyser::archLogGetList(void) {
        if (mode == MODE_ONLINE) {
#ifdef ONLINE_MODEIMPL_OCCI
            checkConnection(true);

            try {
                OracleStatement stmt(&conn, env);
                if ((trace2 & TRACE2_SQL) != 0)
                    cerr << "SQL: " << SQL_GET_ARCHIVE_LOG_LIST << endl;
                stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
                stmt.stmt->setInt(1, databaseSequence);
                stmt.stmt->setInt(2, resetlogs);
                stmt.stmt->setInt(3, activation);
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
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
        } else if (mode == MODE_OFFLINE || mode == MODE_ARCHIVELOG) {
            if (dbRecoveryFileDest.length() > 0) {

                string path = applyMapping(dbRecoveryFileDest + "/" + database + "/archivelog");

                DIR *dir;
                if ((dir = opendir(path.c_str())) == nullptr) {
                    cerr << "ERROR: can't access directory: " << path << endl;
                    throw RuntimeException("reading archive log list");
                }

                if ((trace2 & TRACE2_ARCHIVE_LIST) != 0)
                    cerr << "ARCH_LIST: checking path: " << path << endl;

                string newLastCheckedDay;
                struct dirent *ent;
                while ((ent = readdir(dir)) != nullptr) {
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                        continue;
                    if (ent->d_type != DT_DIR)
                        continue;

                    //skip earlier days
                    if (lastCheckedDay.length() > 0 && lastCheckedDay.compare(ent->d_name) > 0)
                        continue;

                    if ((trace2 & TRACE2_ARCHIVE_LIST) != 0)
                        cerr << "ARCH_LIST: checking path: " << path << "/" << ent->d_name << endl;

                    string path2 = path + "/" + ent->d_name;
                    DIR *dir2;
                    if ((dir2 = opendir(path2.c_str())) == nullptr) {
                        closedir(dir);
                        cerr << "ERROR: can't access directory: " << path2 << endl;
                        throw RuntimeException("reading archive log list");
                    }

                    struct dirent *ent2;
                    while ((ent2 = readdir(dir2)) != nullptr) {
                        if (strcmp(ent2->d_name, ".") == 0 || strcmp(ent2->d_name, "..") == 0)
                            continue;

                        string filename = path + "/" + ent->d_name + "/" + ent2->d_name;
                        if ((trace2 & TRACE2_ARCHIVE_LIST) != 0)
                            cerr << "ARCH_LIST: checking path: " << filename << endl;

                        //checking if file name looks something like o1_mf_1_SSSS_XXXXXXXX_.arc
                        //SS - sequence number

                        uint64_t sequence = 0, i, j, iMax = strnlen(ent2->d_name, 256);
                        for (i = 0; i < iMax; ++i)
                            if (ent2->d_name[i] == '_')
                                break;

                        //first '_'
                        if (i >= iMax || ent2->d_name[i] != '_')
                            continue;

                        for (++i; i < iMax; ++i)
                            if (ent2->d_name[i] == '_')
                                break;

                        //second '_'
                        if (i >= iMax || ent2->d_name[i] != '_')
                            continue;

                        for (++i; i < iMax; ++i)
                            if (ent2->d_name[i] == '_')
                                break;

                        //third '_'
                        if (i >= iMax || ent2->d_name[i] != '_')
                            continue;

                        for (++i; i < iMax; ++i)
                            if (ent2->d_name[i] >= '0' && ent2->d_name[i] <= '9')
                                sequence = sequence * 10 + (ent2->d_name[i] - '0');
                            else
                                break;

                        //forth '_'
                        if (i >= iMax || ent2->d_name[i] != '_')
                            continue;

                        for (++i; i < iMax; ++i)
                            if (ent2->d_name[i] == '_')
                                break;

                        if (i >= iMax || ent2->d_name[i] != '_')
                            continue;

                        //fifth '_'
                        if (strncmp(ent2->d_name + i, "_.arc", 5) != 0)
                            continue;

                        if ((trace2 & TRACE2_ARCHIVE_LIST) != 0)
                            cerr << "ARCH_LIST: found sequence: " << sequence << endl;

                        if (sequence >= databaseSequence) {
                            OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, 0, filename.c_str());
                            if (redo == nullptr)
                                throw MemoryException("OracleAnalyser::archLogGetList.2", sizeof(OracleAnalyserRedoLog));

                            redo->firstScn = ZERO_SCN;
                            redo->nextScn = ZERO_SCN;
                            redo->sequence = sequence;
                            archiveRedoQueue.push(redo);
                        }
                    }
                    closedir(dir2);

                    if (newLastCheckedDay.length() == 0 ||
                        (newLastCheckedDay.length() > 0 && newLastCheckedDay.compare(ent->d_name) < 0))
                        newLastCheckedDay = ent->d_name;
                }
                closedir(dir);

                if (newLastCheckedDay.length() != 0 &&
                        (lastCheckedDay.length() == 0 || (lastCheckedDay.length() > 0 && lastCheckedDay.compare(newLastCheckedDay) < 0))) {
                    if ((trace2 & TRACE2_ARCHIVE_LIST) != 0)
                        cerr << "ARCH_LIST: updating last checked day to: " << newLastCheckedDay << endl;
                    lastCheckedDay = newLastCheckedDay;
                }

            } else if (logArchiveDest.length() > 0 && logArchiveFormat.length() > 0) {
                throw RuntimeException("only db_recovery_file_dest location of archvied red logs is supported for offline mode");
            } else
                throw RuntimeException("missing location of archived redo logs for offline mode");
        }
    }

    void OracleAnalyser::updateOnlineLogs(void) {
        for (OracleAnalyserRedoLog *oracleAnalyserRedoLog : onlineRedoSet) {
            oracleAnalyserRedoLog->resetRedo();
            if (!readerUpdateRedoLog(oracleAnalyserRedoLog->reader)) {
                cerr << "ERROR: updating failed for " << dec << oracleAnalyserRedoLog->path << endl;
                throw RuntimeException("can't update file");
            } else {
                oracleAnalyserRedoLog->sequence = oracleAnalyserRedoLog->reader->sequence;
                oracleAnalyserRedoLog->firstScn = oracleAnalyserRedoLog->reader->firstScn;
                oracleAnalyserRedoLog->nextScn = oracleAnalyserRedoLog->reader->nextScn;
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

    void OracleAnalyser::initializeOnlineMode(void) {
#ifdef ONLINE_MODEIMPL_OCCI
        checkConnection(false);
        if (conn == nullptr)
            throw RuntimeException("connecting to the database");

        typescn currentDatabaseScn;
        typeresetlogs currentResetlogs;
        typeactivation currentActivation;

        try {
            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << SQL_GET_DATABASE_INFORMATION << endl;
            stmt.createStatement(SQL_GET_DATABASE_INFORMATION);
            stmt.executeQuery();

            if (stmt.rset->next()) {
                string LOG_MODE = stmt.rset->getString(1);
                if (LOG_MODE.compare("ARCHIVELOG") != 0) {
                    cerr << "HINT run: SHUTDOWN IMMEDIATE;" << endl;
                    cerr << "HINT run: STARTUP MOUNT;" << endl;
                    cerr << "HINT run: ALTER DATABASE ARCHIVELOG;" << endl;
                    cerr << "HINT run: ALTER DATABASE OPEN;" << endl;
                    throw RuntimeException("database not in ARCHIVELOG mode");
                }

                string SUPPLEMENTAL_LOG_MIN = stmt.rset->getString(2);
                if (SUPPLEMENTAL_LOG_MIN.compare("YES") != 0) {
                    cerr << "HINT run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;" << endl;
                    cerr << "HINT run: ALTER SYSTEM ARCHIVE LOG CURRENT;" << endl;
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
                    isBigEndian = true;
                }

                currentDatabaseScn = stmt.rset->getNumber(6);
                currentResetlogs = stmt.rset->getNumber(7);
                if (resetlogs != 0 && currentResetlogs != resetlogs) {
                    cerr << "ERROR: Previous resetlogs:" << dec << resetlogs << ", current: " << currentResetlogs << endl;
                    throw RuntimeException("incorrect database incarnation");
                } else {
                    resetlogs = currentResetlogs;
                }

                currentActivation = stmt.rset->getNumber(8);
                if (activation != 0 && currentActivation != activation) {
                    cerr << "ERROR: Previous activation: " << dec << activation << ", current: " << currentActivation << endl;
                    throw RuntimeException("incorrect database activation id");
                } else {
                    activation = currentActivation;
                }

                //12+
                string VERSION = stmt.rset->getString(9);
                if (trace >= TRACE_INFO)
                    cerr << "INFO: version: " << dec << VERSION << endl;

                conId = 0;
                if (VERSION.find("Oracle Database 11g") == string::npos) {
                    OracleStatement stmt(&conn, env);
                    if ((trace2 & TRACE2_SQL) != 0)
                        cerr << "SQL: " << SQL_GET_CON_INFO << endl;
                    stmt.createStatement(SQL_GET_CON_INFO);
                    stmt.executeQuery();

                    if (stmt.rset->next()) {
                        conId = stmt.rset->getNumber(1);
                        conName = stmt.rset->getString(2);
                    }
                }

                databaseContext = stmt.rset->getString(10);
            } else {
                throw RuntimeException("reading SYS.V_$DATABASE");
            }

        } catch(SQLException &ex) {
            cerr << "ERROR: Oracle: " << dec << ex.getErrorCode() << ": " << ex.getMessage();
            throw RuntimeException("getting information about log files");
        }

        if ((disableChecks & DISABLE_CHECK_GRANTS) == 0) {
            checkTableForGrants("SYS.CCOL$");
            checkTableForGrants("SYS.CDEF$");
            checkTableForGrants("SYS.COL$");
            checkTableForGrants("SYS.CON$");
            checkTableForGrants("SYS.OBJ$");
            checkTableForGrants("SYS.TAB$");
            checkTableForGrants("SYS.USER$");
            checkTableForGrants("SYS.V_$ARCHIVED_LOG");
            checkTableForGrants("SYS.V_$DATABASE");
            checkTableForGrants("SYS.V_$DATABASE_INCARNATION");
            checkTableForGrants("SYS.V_$LOG");
            checkTableForGrants("SYS.V_$LOGFILE");
            checkTableForGrants("SYS.V_$PARAMETER");
            checkTableForGrants("SYS.V_$TRANSPORTABLE_PLATFORM");
        }

        dbRecoveryFileDest = getParameterValue("db_recovery_file_dest");
        logArchiveDest = getParameterValue("log_archive_dest");
        logArchiveFormat = getParameterValue("log_archive_format");
        nlsCharacterSet = getPropertyValue("NLS_CHARACTERSET");
        nlsNcharCharacterSet = getPropertyValue("NLS_NCHAR_CHARACTERSET");
        commandBuffer->setNlsCharset(nlsCharacterSet, nlsNcharCharacterSet);

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
            cerr << "INFO: sequence: " << dec << databaseSequence <<
                    " scn: " << dec << databaseScn <<
                    " resetlogs: " << dec << resetlogs <<
                    " activation: " << dec << activation;
            if (conId > 0)
                cerr << " con_id: " << dec << conId <<
                        " con_name: " << conName;
            cerr << endl;
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
        readCheckpoint();
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    bool OracleAnalyser::readSchema(void) {
        ifstream infile;
        string fileName = database + "-schema.json";
        infile.open(fileName.c_str(), ios::in);

        if (trace >= TRACE_INFO)
            cerr << "INFO: reading schema from JSON for " << database << endl;

        if (!infile.is_open())
            return false;

        string schemaJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError())
            throw RuntimeException("parsing of <database>-schema.json");

        const Value& databaseJSON = getJSONfield(fileName, document, "database");
        database = databaseJSON.GetString();

        const Value& bigEndianJSON = getJSONfield(fileName, document, "big-endian");
        isBigEndian = bigEndianJSON.GetUint64();

        const Value& resetlogsJSON = getJSONfield(fileName, document, "resetlogs");
        resetlogs = resetlogsJSON.GetUint64();

        const Value& activationJSON = getJSONfield(fileName, document, "activation");
        activation = activationJSON.GetUint64();

        const Value& databaseContextJSON = getJSONfield(fileName, document, "database-context");
        databaseContext = databaseContextJSON.GetString();

        const Value& conIdJSON = getJSONfield(fileName, document, "con-id");
        conId = conIdJSON.GetUint64();

        const Value& conNameJSON = getJSONfield(fileName, document, "con-name");
        conName = conNameJSON.GetString();

        const Value& dbRecoveryFileDestJSON = getJSONfield(fileName, document, "db-recovery-file-dest");
        dbRecoveryFileDest = dbRecoveryFileDestJSON.GetString();

        const Value& logArchiveFormatJSON = getJSONfield(fileName, document, "log-archive-format");
        logArchiveFormat = logArchiveFormatJSON.GetString();

        const Value& logArchiveDestJSON = getJSONfield(fileName, document, "log-archive-dest");
        logArchiveDest = logArchiveDestJSON.GetString();

        const Value& nlsCharacterSetJSON = getJSONfield(fileName, document, "nls-character-set");
        nlsCharacterSet = nlsCharacterSetJSON.GetString();

        const Value& nlsNcharCharacterSetJSON = getJSONfield(fileName, document, "nls-nchar-character-set");
        nlsNcharCharacterSet = nlsNcharCharacterSetJSON.GetString();

        const Value& onlineRedo = getJSONfield(fileName, document, "online-redo");
        if (!onlineRedo.IsArray())
            throw ConfigurationException("bad JSON in <database>-schema.json, online-redo should be an array");

        for (SizeType i = 0; i < onlineRedo.Size(); ++i) {

            const Value& groupJSON = getJSONfield(fileName, onlineRedo[i], "group");
            uint64_t group = groupJSON.GetInt64();

            const Value& pathJSON = getJSONfield(fileName, onlineRedo[i], "path");
            string path = pathJSON.GetString();

            Reader *reader = readerCreate(group);
            if (!readerCheckRedoLog(reader, path))
                throw ConfigurationException("redo log not available");

            OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, group, path.c_str());
            if (redo == nullptr) {
                readerDropAll();
                throw MemoryException("OracleAnalyser::readSchema.1", sizeof(OracleAnalyserRedoLog));
            }

            redo->reader = reader;
            onlineRedoSet.insert(redo);
        }
        archReader = readerCreate(0);

        const Value& schema = getJSONfield(fileName, document, "schema");
        if (!schema.IsArray())
            throw ConfigurationException("bad JSON in <database>-schema.json, schema should be an array");

        for (SizeType i = 0; i < schema.Size(); ++i) {

            const Value& objnJSON = getJSONfield(fileName, schema[i], "objn");
            typeobj objn = objnJSON.GetInt64();

            const Value& objdJSON = getJSONfield(fileName, schema[i], "objd");
            typeobj objd = objdJSON.GetInt64();

            const Value& cluColsJSON = getJSONfield(fileName, schema[i], "clu-cols");
            uint64_t cluCols = cluColsJSON.GetInt64();

            const Value& totalPkJSON = getJSONfield(fileName, schema[i], "total-pk");
            uint64_t totalPk = totalPkJSON.GetInt64();

            const Value& optionsJSON = getJSONfield(fileName, schema[i], "options");
            uint64_t options = optionsJSON.GetInt64();

            const Value& maxSegColJSON = getJSONfield(fileName, schema[i], "max-seg-col");
            uint64_t maxSegCol = maxSegColJSON.GetInt64();

            const Value& ownerJSON = getJSONfield(fileName, schema[i], "owner");
            string owner = ownerJSON.GetString();

            const Value& objectNameJSON = getJSONfield(fileName, schema[i], "object-name");
            string objectName = objectNameJSON.GetString();

            OracleObject *object = new OracleObject(objn, objd, cluCols, options, owner, objectName);
            object->totalPk = totalPk;
            object->maxSegCol = maxSegCol;

            const Value& columns = getJSONfield(fileName, schema[i], "columns");
            if (!columns.IsArray())
                throw ConfigurationException("bad JSON in <database>-schema.json, columns should be an array");

            for (SizeType j = 0; j < columns.Size(); ++j) {

                const Value& colNoJSON = getJSONfield(fileName, columns[j], "col-no");
                uint64_t colNo = colNoJSON.GetUint64();

                const Value& segColNoJSON = getJSONfield(fileName, columns[j], "seg-col-no");
                uint64_t segColNo = segColNoJSON.GetUint64();
                if (segColNo > 1000)
                    throw ConfigurationException("bad JSON in <database>-schema.json, invalid seg-col-no value");

                const Value& columnNameJSON = getJSONfield(fileName, columns[j], "column-name");
                string columnName = columnNameJSON.GetString();

                const Value& typeNoJSON = getJSONfield(fileName, columns[j], "type-no");
                uint64_t typeNo = typeNoJSON.GetUint64();

                const Value& lengthJSON = getJSONfield(fileName, columns[j], "length");
                uint64_t length = lengthJSON.GetUint64();

                const Value& precisionJSON = getJSONfield(fileName, columns[j], "precision");
                int64_t precision = precisionJSON.GetInt64();

                const Value& scaleJSON = getJSONfield(fileName, columns[j], "scale");
                int64_t scale = scaleJSON.GetInt64();

                const Value& numPkJSON = getJSONfield(fileName, columns[j], "num-pk");
                uint64_t numPk = numPkJSON.GetUint64();

                const Value& charsetIdJSON = getJSONfield(fileName, columns[j], "charset-id");
                uint64_t charsetId = charsetIdJSON.GetUint64();

                const Value& nullableJSON = getJSONfield(fileName, columns[j], "nullable");
                bool nullable = nullableJSON.GetUint64();

                OracleColumn *column = new OracleColumn(colNo, segColNo, columnName, typeNo, length, precision, scale, numPk, charsetId, nullable);

                while (segColNo > object->columns.size() + 1)
                    object->columns.push_back(nullptr);

                object->columns.push_back(column);
            }

            addToDict(object);
        }

        infile.close();

        readCheckpoint();
        return true;
    }

    void OracleAnalyser::writeSchema(void) {
        if (trace >= TRACE_INFO) {
            cerr << "INFO: Writing schema information for " << database << endl;
        }

        string fileName = database + "-schema.json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open())
            throw RuntimeException("writing schema data");

        stringstream ss;
        ss << "{\"database\":\"" << database << "\"," <<
                "\"big-endian\":" << dec << isBigEndian << "," <<
                "\"resetlogs\":" << dec << resetlogs << "," <<
                "\"activation\":" << dec << activation << "," <<
                "\"database-context\":\"" << databaseContext << "\"," <<
                "\"con-id\":" << dec << conId << "," <<
                "\"con-name\":\"" << conName << "\"," <<
                "\"db-recovery-file-dest\":\"";
        writeEscapeValue(ss, dbRecoveryFileDest);
        ss << "\"," << "\"log-archive-dest\":\"";
        writeEscapeValue(ss, logArchiveDest);
        ss << "\"," << "\"log-archive-format\":\"";
        writeEscapeValue(ss, logArchiveFormat);
        ss << "\"," << "\"nls-character-set\":\"";
        writeEscapeValue(ss, nlsCharacterSet);
        ss << "\"," << "\"nls-nchar-character-set\":\"";
        writeEscapeValue(ss, nlsNcharCharacterSet);

        ss << "\"," << "\"online-redo\":[";

        bool hasPrev = false;
        for (Reader *reader : readers) {
            if (reader->group == 0)
                continue;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"group\":" << reader->group << ",\"path\":\"";
            writeEscapeValue(ss, reader->pathOrig);
            ss << "\"}";
        }
        ss << "]," << "\"schema\":[";

        hasPrev = false;
        for (auto it : objectMap) {
            OracleObject *object = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"objn\":" << dec << object->objn << "," <<
                    "\"objd\":" << dec << object->objd << "," <<
                    "\"clu-cols\":" << dec << object->cluCols << "," <<
                    "\"total-pk\":" << dec << object->totalPk << "," <<
                    "\"options\":" << dec << object->options << "," <<
                    "\"max-seg-col\":" << dec << object->maxSegCol << "," <<
                    "\"owner\":\"" << object->owner << "\"," <<
                    "\"object-name\":\"" << object->objectName << "\"," <<
                    "\"columns\":[";

            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                if (i > 0)
                    ss << ",";
                ss << "{\"col-no\":" << dec << object->columns[i]->colNo << "," <<
                        "\"seg-col-no\":" << dec << object->columns[i]->segColNo << "," <<
                        "\"column-name\":\"" << object->columns[i]->columnName << "\"," <<
                        "\"type-no\":" << dec << object->columns[i]->typeNo << "," <<
                        "\"length\":" << dec << object->columns[i]->length << "," <<
                        "\"precision\":" << dec << object->columns[i]->precision << "," <<
                        "\"scale\":" << dec << object->columns[i]->scale << "," <<
                        "\"num-pk\":" << dec << object->columns[i]->numPk << "," <<
                        "\"charset-id\":" << dec << object->columns[i]->charsetId << "," <<
                        "\"nullable\":" << dec << object->columns[i]->nullable << "}";
            }

            ss << "]}";
        }

        ss << "]}";
        outfile << ss.rdbuf();
        outfile.close();
    }

    void OracleAnalyser::closeDbConnection(void) {
#ifdef ONLINE_MODEIMPL_OCCI
        if (conn != nullptr) {
            env->terminateConnection(conn);
            conn = nullptr;
        }
        if (env != nullptr) {
            Environment::terminateEnvironment(env);
            env = nullptr;
        }
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    void *OracleAnalyser::run(void) {
        string modeStr;
        if (mode == MODE_ONLINE)
            modeStr = "online";
        else if (mode == MODE_OFFLINE)
            modeStr = "offline";
        else if (mode == MODE_ARCHIVELOG)
            modeStr = "archivelog";

        cout << "Starting thread: Oracle Analyser for: " << database << " in " << modeStr << " mode" << endl;
        if (mode == MODE_ONLINE)
            checkConnection(true);

        uint64_t ret = REDO_OK;
        OracleAnalyserRedoLog *redo = nullptr;
        bool logsProcessed;

        try {
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
                                if (redoTmp->reader->sequence == databaseSequence)
                                    redo = redoTmp;
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

                    if (redo == nullptr)
                        break;

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
                                cerr << "INFO: online redo log has been overwritten by new data, continuing reading from archived redo log" << endl;
                            break;
                        }
                        if (redo->group == 0)
                            throw RuntimeException("read archived redo log");
                        else
                            throw RuntimeException("read online redo log");
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
        } catch(ConfigurationException &ex) {
            cerr << "ERROR: configuration error: " << ex.msg << endl;
            stopMain();
        } catch(RuntimeException &ex) {
            cerr << "ERROR: runtime: " << ex.msg << endl;
            stopMain();
        } catch (MemoryException &e) {
            cerr << "ERROR: memory allocation error for " << e.msg << " for " << e.bytes << " bytes" << endl;
            stopMain();
        }

        if (trace >= TRACE_INFO)
            cerr << "INFO: Oracle Analyser for: " << database << " shutting down" << endl;

        writeCheckpoint(true);
        if (trace >= TRACE_FULL)
            dumpTransactions();
        readerDropAll();

        if (trace >= TRACE_INFO)
            cerr << "INFO: Oracle Analyser for: " << database << " is shut down" << endl;
        return 0;
    }

    void OracleAnalyser::freeRollbackList(void) {
        RedoLogRecord *tmpRedoLogRecord1, *tmpRedoLogRecord2;
        uint64_t lostElements = 0;

        while (rolledBack1 != nullptr) {
            if (trace >= TRACE_WARN)
                cerr << "WARNING: element on rollback list UBA: " << PRINTUBA(rolledBack1->uba) <<
                        " DBA: 0x" << hex << rolledBack2->dba <<
                        " SLT: " << dec << (uint64_t)rolledBack2->slt <<
                        " RCI: " << dec << (uint64_t)rolledBack2->rci <<
                        " SCN: " << PRINTSCN64(rolledBack2->scnRecord) <<
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
        RedoLogRecord *rollbackRedoLogRecord1, *rollbackRedoLogRecord2;

        rollbackRedoLogRecord1 = rolledBack1;
        rollbackRedoLogRecord2 = rolledBack2;
        while (rollbackRedoLogRecord1 != nullptr) {
            if (Transaction::matchesForRollback(redoLogRecord1, redoLogRecord2, rollbackRedoLogRecord1, rollbackRedoLogRecord2)) {

                printRollbackInfo(rollbackRedoLogRecord1, rollbackRedoLogRecord2, nullptr, "rolled back from list");
                if (rollbackRedoLogRecord1->next != nullptr) {
                    rollbackRedoLogRecord1->next->prev = rollbackRedoLogRecord1->prev;
                    rollbackRedoLogRecord2->next->prev = rollbackRedoLogRecord2->prev;
                }

                if (rollbackRedoLogRecord1->prev == nullptr) {
                    rolledBack1 = rollbackRedoLogRecord1->next;
                    rolledBack2 = rollbackRedoLogRecord2->next;
                } else {
                    rollbackRedoLogRecord1->prev->next = rollbackRedoLogRecord1->next;
                    rollbackRedoLogRecord2->prev->next = rollbackRedoLogRecord2->next;
                }

                delete rollbackRedoLogRecord1;
                delete rollbackRedoLogRecord2;
                return true;
            }

            rollbackRedoLogRecord1 = rollbackRedoLogRecord1->next;
            rollbackRedoLogRecord2 = rollbackRedoLogRecord2->next;
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

    bool OracleAnalyser::readerCheckRedoLog(Reader *reader, string path) {
        unique_lock<mutex> lck(mtx);
        reader->status = READER_STATUS_CHECK;
        reader->sequence = 0;
        reader->firstScn = ZERO_SCN;
        reader->nextScn = ZERO_SCN;
        reader->pathOrig = path;
        reader->path = applyMapping(path);
        readerCond.notify_all();
        sleepingCond.notify_all();
        while (reader->status == READER_STATUS_CHECK) {
            if (shutdown)
                break;
            analyserCond.wait(lck);
        }
        if (reader->ret == REDO_OK)
            return true;
        else
            return false;
    }

    void OracleAnalyser::readerDropAll(void) {
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
#ifdef ONLINE_MODEIMPL_OCCI
        try {
            stringstream query;
            query << "SELECT 1 FROM " << tableName << " WHERE 0 = 1";

            OracleStatement stmt(&conn, env);
            if ((trace2 & TRACE2_SQL) != 0)
                cerr << "SQL: " << query.str() << endl;
            stmt.createStatement(query.str());
            stmt.executeQuery();
        } catch(SQLException &ex) {
            if (conId > 0)
                cerr << "HINT run: ALTER SESSION SET CONTAINER = " << conName << ";" << endl;
            cerr << "HINT run: GRANT SELECT ON " << tableName << " TO " << user << ";" << endl;
            throw ConfigurationException("grants missing");
        }
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
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

    void OracleAnalyser::dumpTransactions(void) {
        if (trace >= TRACE_INFO) {
            if (transactionHeap.heapSize > 0)
                cerr << "INFO: Transactions open: " << dec << transactionHeap.heapSize << endl;
            for (uint64_t i = 1; i <= transactionHeap.heapSize; ++i)
                cerr << "INFO: transaction[" << i << "]: " << *transactionHeap.heap[i] << endl;
        }
    }

    void OracleAnalyser::addTable(string mask, vector<string> &keys, string &keysStr, uint64_t options) {
#ifdef ONLINE_MODEIMPL_OCCI
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

                typeobj objd = 0;
                //null for partitioned tables
                if (!stmt.rset->isNull(1))
                    objd = stmt.rset->getNumber(1);

                //table already added with another rule
                if (checkDict(objn, objd) != nullptr) {
                    cout << "  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - already added" << endl;
                    continue;
                }

                uint64_t cluCols = 0, totalPk = 0, maxSegCol = 0, keysCnt = 0;
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

                if ((flags & REDO_FLAGS_HIDE_INVISIBLE_COLUMNS) != 0) {
                    if ((trace2 & TRACE2_SQL) != 0)
                        cerr << "SQL: " << SQL_GET_COLUMN_LIST_INV << endl;
                    stmt2.createStatement(SQL_GET_COLUMN_LIST_INV);
                } else {
                    if ((trace2 & TRACE2_SQL) != 0)
                        cerr << "SQL: " << SQL_GET_COLUMN_LIST << endl;
                    stmt2.createStatement(SQL_GET_COLUMN_LIST);
                }
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

                    uint64_t charmapId = 0;
                    uint64_t charsetForm = stmt2.rset->getNumber(8);
                    if (charsetForm == 1)
                        charmapId = commandBuffer->defaultCharacterMapId;
                    else if (charsetForm == 2)
                        charmapId = commandBuffer->defaultCharacterNcharMapId;
                    else if (charsetForm == 3) {
                        charmapId = stmt2.rset->getNumber(9);
                    }

                    //check character set for char and varchar2
                    if (typeNo == 1 || typeNo == 96) {
                        if (charmapId != ORA_CHARSET_CODE_UTF8 &&
                                charmapId != ORA_CHARSET_CODE_AL32UTF8 &&
                                charmapId != ORA_CHARSET_CODE_AL16UTF16 &&
                                commandBuffer->characterMapName[charmapId] == nullptr) {
                            cerr << "ERROR: Table " << owner << "." << objectName << " - unsupported character set id: " << dec << charmapId <<
                                    " for column: " << columnName << endl;
                            cerr << "HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << dec << charmapId << ") FROM DUAL;" << endl;
                            throw RuntimeException("unsupported character set id");
                        }
                    }

                    int64_t nullable = stmt2.rset->getNumber(10);
                    uint64_t numPk = stmt2.rset->getNumber(11);
                    uint64_t numSup = stmt2.rset->getNumber(12);

                    //column part of defined primary key
                    if (keys.size() > 0) {
                        //manually defined pk overlaps with table pk
                        if (numPk > 0 && (suppLogTablePrimary || suppLogSchemaPrimary || suppLogDbPrimary))
                            numSup = 1;
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
                        cout << "    - col: " << dec << segColNo << ": " << columnName << " (pk: " << dec << numPk << ")" << endl;

                    OracleColumn *column = new OracleColumn(colNo, segColNo, columnName, typeNo, length, precision, scale, numPk, charmapId, (nullable == 0));
                    if (column == nullptr)
                        throw MemoryException("OracleAnalyser::addTable.2", sizeof(OracleColumn));

                    totalPk += numPk;
                    if (segColNo > maxSegCol)
                        maxSegCol = segColNo;

                    object->addColumn(this, column);
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

                object->maxSegCol = maxSegCol;
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
#else
        throw RuntimeException("online mode is not compiled");
#endif /* ONLINE_MODEIMPL_OCCI */
    }

    void OracleAnalyser::checkForCheckpoint(void) {
        uint64_t timeSinceCheckpoint = (clock() - previousCheckpoint) / CLOCKS_PER_SEC;
        if (timeSinceCheckpoint > checkpointInterval) {
            if (trace >= TRACE_FULL) {
                cerr << "FULL: Time since last checkpoint: " << dec << timeSinceCheckpoint << "s, forcing checkpoint" << endl;
            }
            writeCheckpoint(false);
        } else {
            if (trace >= TRACE_FULL) {
                cerr << "FULL: Time since last checkpoint: " << dec << timeSinceCheckpoint << "s" << endl;
            }
        }
    }

    bool OracleAnalyser::readerUpdateRedoLog(Reader *reader) {
        unique_lock<mutex> lck(mtx);
        reader->status = READER_STATUS_UPDATE;
        readerCond.notify_all();
        sleepingCond.notify_all();
        while (reader->status == READER_STATUS_UPDATE) {
            if (shutdown)
                break;
            analyserCond.wait(lck);
        }

        if (reader->ret == REDO_OK) {

            return true;
        } else
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

    void OracleAnalyser::skipEmptyFields(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        uint16_t nextFieldLength;
        while (fieldNum + 1 <= redoLogRecord->fieldCnt) {
            nextFieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (fieldNum + 1) * 2);
            if (nextFieldLength != 0)
                return;
            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = nextFieldLength;

            if (fieldPos + fieldLength > redoLogRecord->length) {
                cerr << "ERROR: field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt << endl;
                cerr << "ERROR: pos: " << dec << fieldPos << ", length:" << fieldLength << " max: " << redoLogRecord->length << endl;
                throw RedoLogException("field length out of vector");
            }
        }
    }

    void OracleAnalyser::nextField(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        ++fieldNum;
        if (fieldNum > redoLogRecord->fieldCnt) {
            cerr << "ERROR: field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", data: " << dec << redoLogRecord->rowData <<
                    ", objn: " << dec << redoLogRecord->objn <<
                    ", objd: " << dec << redoLogRecord->objd <<
                    ", op: " << hex << redoLogRecord->opCode <<
                    ", cc: " << dec << (uint64_t)redoLogRecord->cc <<
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

    bool OracleAnalyser::nextFieldOpt(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        if (fieldNum >= redoLogRecord->fieldCnt)
            return false;

        ++fieldNum;

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
        return true;
    }

    string OracleAnalyser::applyMapping(string path) {
        uint64_t sourceLength, targetLength, newPathLength = path.length();
        char pathBuffer[MAX_PATH_LENGTH];

        for (uint64_t i = 0; i < pathMapping.size() / 2; ++i) {
            sourceLength = pathMapping[i * 2].length();
            targetLength = pathMapping[i * 2 + 1].length();

            if (sourceLength <= newPathLength &&
                    newPathLength - sourceLength + targetLength < MAX_PATH_LENGTH - 1 &&
                    memcmp(path.c_str(), pathMapping[i * 2].c_str(), sourceLength) == 0) {

                memcpy(pathBuffer, pathMapping[i * 2 + 1].c_str(), targetLength);
                memcpy(pathBuffer + targetLength, path.c_str() + sourceLength, newPathLength - sourceLength);
                pathBuffer[newPathLength - sourceLength + targetLength] = 0;
                path = pathBuffer;
                break;
            }
        }

        return path;
    }


    void OracleAnalyser::printRollbackInfo(RedoLogRecord *redoLogRecord, Transaction *transaction, const char *msg) {
        if ((trace2 & TRACE2_COMMIT_ROLLBACK) == 0)
            return;
        cerr << "ROLLBACK:" <<
                " OP: " << setw(4) << setfill('0') << hex << redoLogRecord->opCode << "    " <<
                " DBA: 0x" << hex << redoLogRecord->dba << "." << (uint64_t)redoLogRecord->slot <<
                " DBA: 0x" << hex << redoLogRecord->dba <<
                " SLT: " << dec << (uint64_t)redoLogRecord->slt <<
                " RCI: " << dec << (uint64_t)redoLogRecord->rci <<
                " SCN: " << PRINTSCN64(redoLogRecord->scn) <<
                " REC: " << PRINTSCN64(redoLogRecord->scnRecord);
        if (transaction != nullptr)
            cerr << " XID: " << PRINTXID(transaction->xid);
        cerr << " " << msg << endl;
    }

    void OracleAnalyser::printRollbackInfo(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, Transaction *transaction, const char *msg) {
        if ((trace2 & TRACE2_COMMIT_ROLLBACK) == 0)
            return;

        cerr << "ROLLBACK:" <<
                " OP: " << setw(4) << setfill('0') << hex << redoLogRecord1->opCode << redoLogRecord2->opCode <<
                " UBA: " << PRINTUBA(redoLogRecord1->uba);
        if (redoLogRecord1->opCode == 0x0501)
            cerr <<
                    " DBA: 0x" << hex << redoLogRecord2->dba << "." << (uint64_t)redoLogRecord2->slot <<
                    " SLT: " << setfill(' ') << setw(3) << dec << (uint64_t)redoLogRecord1->slt <<
                    " RCI: " << setfill(' ') << setw(3) << dec << (uint64_t)redoLogRecord1->rci;
        else
            cerr <<
                    " DBA: 0x" << hex << redoLogRecord1->dba << "." << (uint64_t)redoLogRecord1->slot <<
                    " SLT: " << setfill(' ') << setw(3) << dec << (uint64_t)redoLogRecord2->slt <<
                    " RCI: " << setfill(' ') << setw(3) << dec << (uint64_t)redoLogRecord2->rci;

        cerr <<
                " SCN: " << PRINTSCN64(redoLogRecord1->scn) <<
                "." << setfill(' ') << setw(5) << dec << redoLogRecord1->subScn <<
                " REC: " << PRINTSCN64(redoLogRecord1->scnRecord);
        if (transaction != nullptr)
            cerr << " XID: " << PRINTXID(transaction->xid);
        if (redoLogRecord2->opCode == 0x0506 || redoLogRecord2->opCode == 0x050B)
            cerr << " OPFLAGS: " << hex << redoLogRecord2->opFlags;
        cerr << " " << msg << endl;
    }

    bool OracleAnalyserRedoLogCompare::operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool OracleAnalyserRedoLogCompareReverse::operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2) {
        return p1->sequence < p2->sequence;
    }
}
