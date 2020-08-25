/* Thread reading Oracle Redo Logs
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

#include <thread>
#include <dirent.h>
#include <unistd.h>
#include <rapidjson/document.h>
#include <sys/stat.h>

#include "ConfigurationException.h"
#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "OracleAnalyser.h"
#include "OracleAnalyserRedoLog.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBuffer.h"
#include "Reader.h"
#include "ReaderASM.h"
#include "ReaderFilesystem.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "TransactionMap.h"

using namespace rapidjson;
using namespace std;

const Value& getJSONfield(string &fileName, const Value& value, const char* field);
const Value& getJSONfield(string &fileName, const Document& document, const char* field);

void stopMain();

namespace OpenLogReplicator {

    string OracleAnalyser::SQL_GET_ARCHIVE_LOG_LIST("SELECT "
            "NAME, "
            "SEQUENCE#, "
            "FIRST_CHANGE#, "
            "NEXT_CHANGE# "
            "FROM SYS.V_$ARCHIVED_LOG WHERE SEQUENCE# >= :i AND RESETLOGS_ID = :j AND ACTIVATION# = :k AND NAME IS NOT NULL ORDER BY SEQUENCE#, DEST_ID");
    string OracleAnalyser::SQL_GET_DATABASE_INFORMATION("SELECT "
            "DECODE(D.LOG_MODE, 'ARCHIVELOG', 1, 0), "
            "DECODE(D.SUPPLEMENTAL_LOG_DATA_MIN, 'YES', 1, 0), "
            "DECODE(D.SUPPLEMENTAL_LOG_DATA_PK, 'YES', 1, 0), "
            "DECODE(D.SUPPLEMENTAL_LOG_DATA_ALL, 'YES', 1, 0), "
            "DECODE(TP.ENDIAN_FORMAT, 'Big', 1, 0), "
            "D.CURRENT_SCN, "
            "DI.RESETLOGS_ID, "
            "D.ACTIVATION#, "
            "VER.BANNER, "
            "SYS_CONTEXT('USERENV','DB_NAME') "
            "FROM SYS.V_$DATABASE D JOIN SYS.V_$TRANSPORTABLE_PLATFORM TP ON TP.PLATFORM_NAME = D.PLATFORM_NAME JOIN SYS.V_$VERSION VER ON VER.BANNER LIKE '%Oracle Database%' JOIN SYS.V_$DATABASE_INCARNATION DI ON DI.STATUS = 'CURRENT'");
    string OracleAnalyser::SQL_GET_CON_INFO("SELECT "
            " SYS_CONTEXT('USERENV','CON_ID'), "
            "SYS_CONTEXT('USERENV','CON_NAME') "
            "FROM DUAL");
    string OracleAnalyser::SQL_GET_CURRENT_SEQUENCE("SELECT "
            "SEQUENCE# "
            "FROM SYS.V_$LOG WHERE STATUS = 'CURRENT'");
    string OracleAnalyser::SQL_GET_LOGFILE_LIST("SELECT "
            "LF.GROUP#, "
            "LF.MEMBER "
            "FROM SYS.V_$LOGFILE LF WHERE TYPE = :i ORDER BY LF.GROUP# ASC, LF.IS_RECOVERY_DEST_FILE DESC, LF.MEMBER ASC");
    string OracleAnalyser::SQL_GET_TABLE_LIST("SELECT "
            "T.DATAOBJ#, "
            "T.OBJ#, "
            "T.CLUCOLS, "
            "U.NAME, "
            "O.NAME, "
            "DECODE(BITAND(T.PROPERTY, 1024), 0, 0, 1), "
            "DECODE((BITAND(T.PROPERTY, 512)+BITAND(T.FLAGS, 536870912)), 0, 0, 1), "    //IOT overflow segment,
            "DECODE(BITAND(U.SPARE1, 1), 1, 1, 0), "
            "DECODE(BITAND(U.SPARE1, 8), 8, 1, 0), "
            "DECODE(BITAND(T.PROPERTY, 32), 32, 0, 1), "                                 //nullable
            "DECODE(BITAND(O.FLAGS,2)+BITAND(O.FLAGS,16)+BITAND(O.FLAGS,32), 0, 0, 1), " //temporary, secondary, in-memory temp
            "DECODE(BITAND(T.PROPERTY, 8192), 8192, 1, 0), "                             //nested
            "DECODE(BITAND(T.FLAGS, 131072), 131072, 1, 0), "
            "DECODE(BITAND(T.FLAGS, 8388608), 8388608, 1, 0), "
            "CASE WHEN (BITAND(T.PROPERTY, 32) = 32) THEN 1 ELSE 0 END "
            "FROM SYS.TAB$ T, SYS.OBJ$ O, SYS.USER$ U WHERE T.OBJ# = O.OBJ# AND BITAND(O.flags, 128) = 0 AND O.OWNER# = U.USER# AND U.NAME || '.' || O.NAME LIKE UPPER(:i) ORDER BY 4,5");
    string OracleAnalyser::SQL_GET_COLUMN_LIST("SELECT "
            "C.COL#, "
            "C.SEGCOL#, "
            "C.NAME, "
            "C.TYPE#, "
            "C.LENGTH, "
            "C.PRECISION#, "
            "C.SCALE, "
            "C.CHARSETFORM, "
            "C.CHARSETID, "
            "C.NULL$, "
            "(SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#), "
            "(SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0) "
            "FROM SYS.COL$ C WHERE C.SEGCOL# > 0 AND C.OBJ# = :i AND DECODE(BITAND(C.PROPERTY, 256), 0, 0, 1) = 0 ORDER BY C.SEGCOL#");
    string OracleAnalyser::SQL_GET_COLUMN_LIST_INV("SELECT "
            "C.COL#, "
            "C.SEGCOL#, "
            "C.NAME, "
            "C.TYPE#, "
            "C.LENGTH, "
            "C.PRECISION#, "
            "C.SCALE, "
            "C.CHARSETFORM, "
            "C.CHARSETID, "
            "C.NULL$, "
            "(SELECT COUNT(*) FROM SYS.CCOL$ L JOIN SYS.CDEF$ D ON D.CON# = L.CON# AND D.TYPE# = 2 WHERE L.INTCOL# = C.INTCOL# and L.OBJ# = C.OBJ#), "
            "(SELECT COUNT(*) FROM SYS.CCOL$ L, SYS.CDEF$ D WHERE D.TYPE# = 12 AND D.CON# = L.CON# AND L.OBJ# = C.OBJ# AND L.INTCOL# = C.INTCOL# AND L.SPARE1 = 0) "
            "FROM SYS.COL$ C WHERE C.SEGCOL# > 0 AND C.OBJ# = :i AND DECODE(BITAND(C.PROPERTY, 256), 0, 0, 1) = 0 AND DECODE(BITAND(C.PROPERTY, 32), 0, 0, 1) = 0 ORDER BY C.SEGCOL#");
    string OracleAnalyser::SQL_GET_PARTITION_LIST("SELECT "
            "T.OBJ#, "
            "T.DATAOBJ# "
            "FROM SYS.TABPART$ T where T.BO# = :1 "
            "UNION ALL "
            "SELECT "
            "TSP.OBJ#, "
            "TSP.DATAOBJ# "
            "FROM SYS.TABSUBPART$ TSP JOIN SYS.TABCOMPART$ TCP ON TCP.OBJ# = TSP.POBJ# WHERE TCP.BO# = :1");
    string OracleAnalyser::SQL_GET_SUPPLEMNTAL_LOG_TABLE("SELECT "
            "C.TYPE# "
            "FROM SYS.CON$ OC, SYS.CDEF$ C WHERE OC.CON# = C.CON# AND (C.TYPE# = 14 OR C.TYPE# = 17) AND C.OBJ# = :i");
    string OracleAnalyser::SQL_GET_PARAMETER("SELECT "
            "VALUE "
            "FROM SYS.V_$PARAMETER WHERE NAME = :i");
    string OracleAnalyser::SQL_GET_PROPERTY("SELECT "
            "PROPERTY_VALUE "
            "FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME = :1");

    OracleAnalyser::OracleAnalyser(OutputBuffer *outputBuffer, const string alias, const string database, const string user, const string password, const string connectString,
            const string userASM, const string passwordASM, const string connectStringASM, uint64_t trace, uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpRawData,
            uint64_t flags, uint64_t modeType, uint64_t disableChecks, uint64_t redoReadSleep, uint64_t archReadSleep, uint64_t checkpointInterval,
            uint64_t memoryMinMb, uint64_t memoryMaxMb) :
        Thread(alias),
        databaseSequence(0),
        user(user),
        password(password),
        connectString(connectString),
        userASM(userASM),
        passwordASM(passwordASM),
        connectStringASM(connectStringASM),
        database(database),
        archReader(nullptr),
        rolledBack1(nullptr),
        rolledBack2(nullptr),
        suppLogDbPrimary(0),
        suppLogDbAll(0),
        previousCheckpoint(clock()),
        checkpointInterval(checkpointInterval),
        memoryMinMb(memoryMinMb),
        memoryMaxMb(memoryMaxMb),
        memoryChunks(nullptr),
        memoryChunksMin(memoryMinMb / MEMORY_CHUNK_SIZE_MB),
        memoryChunksAllocated(0),
        memoryChunksFree(0),
        memoryChunksMax(memoryMaxMb / MEMORY_CHUNK_SIZE_MB),
        memoryChunksHWM(0),
        memoryChunksSupplemental(0),
        object(nullptr),
        env(nullptr),
        conn(nullptr),
        connASM(nullptr),
        waitingForKafkaWriter(false),
        databaseContext(""),
        databaseScn(0),
        lastOpTransactionMap(nullptr),
        transactionHeap(nullptr),
        transactionBuffer(nullptr),
        outputBuffer(outputBuffer),
        dumpRedoLog(dumpRedoLog),
        dumpRawData(dumpRawData),
        flags(flags),
        modeType(modeType),
        disableChecks(disableChecks),
        redoReadSleep(redoReadSleep),
        archReadSleep(archReadSleep),
        trace(trace),
        trace2(trace2),
        version(0),
        conId(0),
        resetlogs(0),
        activation(0),
        isBigEndian(false),
        suppLogSize(0),
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

        memoryChunks = new uint8_t*[memoryMaxMb / MEMORY_CHUNK_SIZE_MB];
        if (memoryChunks == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) << " bytes memory for (reason: memory chunks#1)");
        }

        for (uint64_t i = 0; i < memoryChunksMin; ++i) {
            memoryChunks[i] = new uint8_t[MEMORY_CHUNK_SIZE];

            if (memoryChunks[i] == nullptr) {
                RUNTIME_FAIL("could not allocate " << dec << MEMORY_CHUNK_SIZE_MB << " bytes memory for (reason: memory chunks#2)");
            }
            ++memoryChunksAllocated;
            ++memoryChunksFree;
        }
        memoryChunksHWM = memoryChunksMin;

        uint64_t maps = (memoryMinMb / 1024) + 1;
        if (maps > MAPS_MAX)
            maps = MAPS_MAX;
        lastOpTransactionMap = new TransactionMap(this, maps);
        if (lastOpTransactionMap == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << sizeof(TransactionMap) << " bytes memory for (reason: memory chunks#3)");
        }

        transactionHeap = new TransactionHeap(this);
        if (transactionHeap == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << sizeof(TransactionHeap) << " bytes memory for (reason: memory chunks#4)");
        }

        transactionBuffer = new TransactionBuffer(this);
        if (transactionBuffer == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << sizeof(TransactionBuffer) << " bytes memory for (reason: memory chunks#5)");
        }

        env = new DatabaseEnvironment();
    }

    OracleAnalyser::~OracleAnalyser() {
        if (object != nullptr) {
            delete object;
            object = nullptr;
        }
        readerDropAll();
        freeRollbackList();

        while (!archiveRedoQueue.empty()) {
            OracleAnalyserRedoLog *redoTmp = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete redoTmp;
        }

        for (OracleAnalyserRedoLog *oracleAnalyserRedoLog : onlineRedoSet)
            delete oracleAnalyserRedoLog;
        onlineRedoSet.clear();

        partitionMap.clear();
        for (auto it : objectMap) {
            OracleObject *objectTmp = it.second;
            delete objectTmp;
        }
        objectMap.clear();

        for (auto it : xidTransactionMap) {
            Transaction *transaction = it.second;
            delete transaction;
        }
        xidTransactionMap.clear();

        if (transactionBuffer != nullptr) {
            delete transactionBuffer;
            transactionBuffer = nullptr;
        }

        if (transactionHeap != nullptr) {
            delete transactionHeap;
            transactionHeap = nullptr;
        }

        if (lastOpTransactionMap != nullptr) {
            delete lastOpTransactionMap;
            lastOpTransactionMap = nullptr;
        }

        while (memoryChunksAllocated > 0) {
            --memoryChunksAllocated;
            delete[] memoryChunks[memoryChunksAllocated];
            memoryChunks[memoryChunksAllocated] = nullptr;
        }

        if (memoryChunks != nullptr) {
            delete[] memoryChunks;
            memoryChunks = nullptr;
        }

        if (conn != nullptr) {
            delete conn;
            conn = nullptr;
        }

        if (connASM != nullptr) {
            delete connASM;
            connASM = nullptr;
        }

        if (env != nullptr) {
            delete env;
            env = nullptr;
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
        char value[4001];
        DatabaseStatement stmt(conn);
        TRACE_(TRACE2_SQL, SQL_GET_PARAMETER << endl << "PARAM1: " << parameter);
        stmt.createStatement(SQL_GET_PARAMETER);
        stmt.bindString(1, parameter);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery()) {
            string valueString(value);
            return valueString;
        }

        //no value found
        RUNTIME_FAIL("can't get parameter value for " << parameter);
    }

    string OracleAnalyser::getPropertyValue(const char *property) {
        char value[4001];
        DatabaseStatement stmt(conn);
        TRACE_(TRACE2_SQL, SQL_GET_PROPERTY << endl << "PARAM1: " << property);
        stmt.createStatement(SQL_GET_PROPERTY);
        stmt.bindString(1, property);
        stmt.defineString(1, value, sizeof(value));

        if (stmt.executeQuery()) {
            string valueString(value);
            return valueString;
        }

        //no value found
        RUNTIME_FAIL("can't get proprty value for " << property);
    }

    void OracleAnalyser::writeCheckpoint(bool atShutdown) {
        clock_t now = clock();
        typeseq minSequence = 0xFFFFFFFF;
        Transaction *transaction;

        for (uint64_t i = 1; i <= transactionHeap->size; ++i) {
            transaction = transactionHeap->at(i);
            if (minSequence > transaction->firstSequence)
                minSequence = transaction->firstSequence;
        }
        if (minSequence == 0xFFFFFFFF)
            minSequence = databaseSequence;

        uint64_t timeSinceCheckpoint = (now - previousCheckpoint) / CLOCKS_PER_SEC;

        FULL_("writing checkpoint information scn: " << PRINTSCN64(databaseScn) <<
                " sequence: " << dec << minSequence << "/" << databaseSequence <<
                " after: " << dec << timeSinceCheckpoint << "s");

        string fileName = database + "-chkpt.json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            RUNTIME_FAIL("writing checkpoint data to <database>-chkpt.json");
        }

        stringstream ss;
        ss << "{\"database\":\"" << database
                << "\",\"sequence\":" << dec << minSequence
                << ",\"scn\":" << dec << databaseScn
                << ",\"resetlogs\":" << dec << resetlogs
                << ",\"activation\":" << dec << activation << "}";

        outfile << ss.rdbuf();
        outfile.close();

        if (atShutdown) {
            INFO_("writing checkpoint at exit for " << database << ":" <<
                        " scn: " << dec << databaseScn <<
                        " sequence: " << dec << minSequence <<
                        " resetlogs: " << dec << resetlogs <<
                        " activation: " << dec << activation <<
                        " con_id: " << dec << conId <<
                        " con_name: " << conName);
        }

        previousCheckpoint = now;
    }

    void OracleAnalyser::readCheckpoint(void) {
        ifstream infile;
        string fileName = database + "-chkpt.json";
        infile.open(fileName.c_str(), ios::in);
        if (!infile.is_open()) {
            if ((flags & REDO_FLAGS_ARCH_ONLY) != 0) {
                RUNTIME_FAIL("checkpoint file <database>-chkpt.json is required for archive log mode");
            }
            return;
        }

        string configJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError()) {
            RUNTIME_FAIL("parsing of <database>-chkpt.json");
        }

        const Value& databaseJSON = getJSONfield(fileName, document, "database");
        if (database.compare(databaseJSON.GetString()) != 0) {
            RUNTIME_FAIL("parsing of <database>-chkpt.json - invalid database name");
        }

        const Value& databaseSequenceJSON = getJSONfield(fileName, document, "sequence");
        databaseSequence = databaseSequenceJSON.GetUint64();

        const Value& resetlogsJSON = getJSONfield(fileName, document, "resetlogs");
        typeresetlogs resetlogsRead = resetlogsJSON.GetUint64();
        if (resetlogs != resetlogsRead) {
            RUNTIME_FAIL("resetlogs id read from checkpoint JSON: " << dec << resetlogsRead << ", expected: " << resetlogs);
        }

        const Value& activationJSON = getJSONfield(fileName, document, "activation");
        typeactivation activationRead = activationJSON.GetUint64();
        if (activation != activationRead) {
            RUNTIME_FAIL("activation id read from checkpoint JSON: " << dec << activationRead << ", expected: " << activation);
        }

        const Value& scnJSON = getJSONfield(fileName, document, "scn");
        databaseScn = scnJSON.GetUint64();

        infile.close();
    }

    void OracleAnalyser::addToDict(OracleObject *object) {
        if (objectMap[object->objn] == nullptr) {
            objectMap[object->objn] = object;
        } else {
            CONFIG_FAIL("can't add object objn: " << dec << object->objn << ", objd: " << object->objd << " - another object with the same id");
        }

        if (partitionMap[object->objn] == nullptr) {
            partitionMap[object->objn] = object;
        } else {
            CONFIG_FAIL("can't add object objn: " << dec << object->objn << ", objd: " << object->objn << " - another object with the same id");
        }

        for (typeobj2 objx : object->partitions) {
            typeobj partitionObjn = objx >> 32;
            typeobj partitionObjd = objx & 0xFFFFFFFF;

            if (partitionMap[partitionObjn] == nullptr) {
                partitionMap[partitionObjn] = object;
            } else {
                CONFIG_FAIL("can't add object objn: " << dec << partitionObjn << ", objd: " << partitionObjd << " - another object with the same id");
            }
        }
    }

    void OracleAnalyser::checkConnection(bool reconnect) {
        while (!shutdown) {
            if (conn == nullptr) {
                INFO_("connecting to Oracle instance of " << database << " to " << connectString);

                try {
                    conn = new DatabaseConnection(env, user, password, connectString, false);
                } catch(RuntimeException &ex) {
                    //
                }
            }

            if (conn != nullptr || !reconnect)
                break;

            WARNING_("cannot connect to database, retry in 5 sec.");
            sleep(5);
        }

        if (modeType == MODE_ASM) {
            while (!shutdown) {
                if (connASM == nullptr) {
                    INFO_("connecting to ASM instance of " << database << " to " << connectStringASM);

                    try {
                        connASM = new DatabaseConnection(env, userASM, passwordASM, connectStringASM, true);
                    } catch(RuntimeException &ex) {
                        //
                    }
                }

                if (connASM != nullptr || !reconnect)
                    break;

                WARNING_("cannot connect to ASM, retry in 5 sec.");
                sleep(5);
            }
        }
    }

    void OracleAnalyser::archLogGetList(void) {
        if (modeType == MODE_ONLINE || modeType == MODE_ASM || modeType == MODE_STANDBY) {
            checkConnection(true);

            DatabaseStatement stmt(conn);
            TRACE_(TRACE2_SQL, SQL_GET_ARCHIVE_LOG_LIST << endl << "PARAM1: " << dec << databaseSequence << endl << "PARAM2: " << dec << resetlogs << endl << "PARAM3: " << dec << activation);

            stmt.createStatement(SQL_GET_ARCHIVE_LOG_LIST);
            stmt.bindUInt32(1, databaseSequence);
            stmt.bindUInt32(2, resetlogs);
            stmt.bindUInt32(3, activation);

            char path[513]; stmt.defineString(1, path, sizeof(path));
            typeseq sequence; stmt.defineUInt32(2, sequence);
            typescn firstScn; stmt.defineUInt64(3, firstScn);
            typescn nextScn; stmt.defineUInt64(4, nextScn);
            int64_t ret = stmt.executeQuery();

            while (ret) {
                string mappedPath = applyMapping(path);

                OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, 0, mappedPath.c_str());
                if (redo == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleAnalyserRedoLog) << " bytes memory for (arch log list#1)");
                }

                redo->firstScn = firstScn;
                redo->nextScn = nextScn;
                redo->sequence = sequence;
                archiveRedoQueue.push(redo);
                ret = stmt.next();
            }
        } else if (modeType == MODE_OFFLINE) {
            if (dbRecoveryFileDest.length() == 0) {
                if (logArchiveDest.length() > 0 && logArchiveFormat.length() > 0) {
                    RUNTIME_FAIL("only db_recovery_file_dest location of archived redo logs is supported for offline mode");
                } else {
                    RUNTIME_FAIL("missing location of archived redo logs for offline mode");
                }
            }

            string mappedPath = applyMapping(dbRecoveryFileDest + "/" + database + "/archivelog");
            TRACE_(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath);

            DIR *dir;
            if ((dir = opendir(mappedPath.c_str())) == nullptr) {
                RUNTIME_FAIL("can't access directory: " << mappedPath);
            }

            string newLastCheckedDay;
            struct dirent *ent;
            while ((ent = readdir(dir)) != nullptr) {
                if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                    continue;

                struct stat fileStat;
                string mappedSubPath = mappedPath + "/" + ent->d_name;
                if (stat(mappedSubPath.c_str(), &fileStat)) {
                    WARNING_("can't read file information for: " << mappedSubPath);
                    continue;
                }

                if (!S_ISDIR(fileStat.st_mode))
                    continue;

                //skip earlier days
                if (lastCheckedDay.length() > 0 && lastCheckedDay.compare(ent->d_name) > 0)
                    continue;

                TRACE_(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath << "/" << ent->d_name);

                string mappedPathWithFile = mappedPath + "/" + ent->d_name;
                DIR *dir2;
                if ((dir2 = opendir(mappedPathWithFile.c_str())) == nullptr) {
                    closedir(dir);
                    RUNTIME_FAIL("can't access directory: " << mappedPathWithFile);
                }

                struct dirent *ent2;
                while ((ent2 = readdir(dir2)) != nullptr) {
                    if (strcmp(ent2->d_name, ".") == 0 || strcmp(ent2->d_name, "..") == 0)
                        continue;

                    string fileName = mappedPath + "/" + ent->d_name + "/" + ent2->d_name;
                    TRACE_(TRACE2_ARCHIVE_LIST, "checking path: " << fileName);

                    uint64_t sequence = getSequenceFromFileName(ent2->d_name);

                    TRACE_(TRACE2_ARCHIVE_LIST, "found sequence: " << sequence);

                    if (sequence == 0 || sequence < databaseSequence)
                        continue;

                    OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, 0, fileName.c_str());
                    if (redo == nullptr) {
                        RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleAnalyserRedoLog) << " bytes memory for (arch log list#2)");
                    }

                    redo->firstScn = ZERO_SCN;
                    redo->nextScn = ZERO_SCN;
                    redo->sequence = sequence;
                    archiveRedoQueue.push(redo);
                }
                closedir(dir2);

                if (newLastCheckedDay.length() == 0 ||
                    (newLastCheckedDay.length() > 0 && newLastCheckedDay.compare(ent->d_name) < 0))
                    newLastCheckedDay = ent->d_name;
            }
            closedir(dir);

            if (newLastCheckedDay.length() != 0 &&
                    (lastCheckedDay.length() == 0 || (lastCheckedDay.length() > 0 && lastCheckedDay.compare(newLastCheckedDay) < 0))) {
                TRACE_(TRACE2_ARCHIVE_LIST, "updating last checked day to: " << newLastCheckedDay);
                lastCheckedDay = newLastCheckedDay;
            }

        } else if (modeType == MODE_BATCH) {
            for (string path1 : redoLogsBatch) {
                string mappedPath = applyMapping(path1);

                TRACE_(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath);

                struct stat fileStat;
                if (stat(mappedPath.c_str(), &fileStat)) {
                    WARNING_("can't read file information for: " << mappedPath);
                    continue;
                }

                //single file
                if (!S_ISDIR(fileStat.st_mode)) {
                    TRACE_(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath);

                    //getting file name from path
                    const char *fileName = mappedPath.c_str();
                    uint64_t j = mappedPath.length();
                    while (j > 0) {
                        if (fileName[j - 1] == '/')
                            break;
                        --j;
                    }
                    uint64_t sequence = getSequenceFromFileName(fileName + j);

                    TRACE_(TRACE2_ARCHIVE_LIST, "found sequence: " << sequence);

                    if (sequence == 0 || sequence < databaseSequence)
                        continue;

                    OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, 0, mappedPath.c_str());
                    if (redo == nullptr) {
                        RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleAnalyserRedoLog) << " bytes memory for (arch log list#3)");
                    }

                    redo->firstScn = ZERO_SCN;
                    redo->nextScn = ZERO_SCN;
                    redo->sequence = sequence;
                    archiveRedoQueue.push(redo);
                //dir, check all files
                } else {
                    DIR *dir;
                    if ((dir = opendir(mappedPath.c_str())) == nullptr) {
                        RUNTIME_FAIL("can't access directory: " << mappedPath);
                    }

                    struct dirent *ent;
                    while ((ent = readdir(dir)) != nullptr) {
                        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                            continue;

                        string fileName = mappedPath + "/" + ent->d_name;
                        TRACE_(TRACE2_ARCHIVE_LIST, "checking path: " << fileName);

                        uint64_t sequence = getSequenceFromFileName(ent->d_name);

                        TRACE_(TRACE2_ARCHIVE_LIST, "found sequence: " << sequence);

                        if (sequence == 0 || sequence < databaseSequence)
                            continue;

                        OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, 0, fileName.c_str());
                        if (redo == nullptr) {
                            RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleAnalyserRedoLog) << " bytes memory for (arch log list#4)");
                        }

                        redo->firstScn = ZERO_SCN;
                        redo->nextScn = ZERO_SCN;
                        redo->sequence = sequence;
                        archiveRedoQueue.push(redo);
                    }
                    closedir(dir);
                }
            }
        }
    }

    void OracleAnalyser::updateOnlineLogs(void) {
        for (OracleAnalyserRedoLog *oracleAnalyserRedoLog : onlineRedoSet) {
            oracleAnalyserRedoLog->resetRedo();
            if (!readerUpdateRedoLog(oracleAnalyserRedoLog->reader)) {
                RUNTIME_FAIL("updating failed for " << dec << oracleAnalyserRedoLog->path);
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
        return (((uint64_t)buf[0] << 24) | ((uint64_t)buf[1] << 16) |
                ((uint64_t)buf[2] << 8) | ((uint64_t)buf[3]) |
                ((uint64_t)buf[4] << 40) | ((uint64_t)buf[5] << 32) |
                ((uint64_t)buf[6] << 48));
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
        if ((buf[4] & 0x80) == 0x80)
            return (uint64_t)buf[3] | ((uint64_t)buf[2] << 8) |
                ((uint64_t)buf[1] << 16) | ((uint64_t)buf[0] << 24) |
                ((uint64_t)buf[7] << 32) | ((uint64_t)buf[6] << 40) |
                ((uint64_t)buf[5] << 48) | ((uint64_t)(buf[4] & 0x7F) << 56);
        else
            return (uint64_t)buf[3] | ((uint64_t)buf[2] << 8) |
                ((uint64_t)buf[1] << 16) | ((uint64_t)buf[0] << 24) |
                ((uint64_t)buf[5] << 32) | ((uint64_t)buf[6] << 40);
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
        if ((buf[0] & 0x80) == 0x80)
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
        checkConnection(false);
        if (conn == nullptr) {
            RUNTIME_FAIL("connecting to the database");
        }

        typescn currentDatabaseScn;
        typeresetlogs currentResetlogs;
        typeactivation currentActivation;

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

            if (isBigEndian) {
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

            if (resetlogs != 0 && currentResetlogs != resetlogs) {
                RUNTIME_FAIL("previous resetlogs:" << dec << resetlogs << ", expected: " << currentResetlogs);
            } else {
                resetlogs = currentResetlogs;
            }

            if (activation != 0 && currentActivation != activation) {
                RUNTIME_FAIL("previous activation: " << dec << activation << ", expected: " << currentActivation);
            } else {
                activation = currentActivation;
            }

            INFO_("version: " << dec << bannerStr);

            //12+
            conId = 0;
            if (memcmp(bannerStr, "Oracle Database 11g", 19) != 0) {
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
        } else {
            RUNTIME_FAIL("trying to read SYS.V_$DATABASE");
        }

        if ((disableChecks & DISABLE_CHECK_GRANTS) == 0) {
            checkTableForGrants("SYS.CCOL$");
            checkTableForGrants("SYS.CDEF$");
            checkTableForGrants("SYS.COL$");
            checkTableForGrants("SYS.CON$");
            checkTableForGrants("SYS.OBJ$");
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
            checkTableForGrants("SYS.V_$TRANSPORTABLE_PLATFORM");
        }

        dbRecoveryFileDest = getParameterValue("db_recovery_file_dest");
        logArchiveDest = getParameterValue("log_archive_dest");
        logArchiveFormat = getParameterValue("log_archive_format");
        nlsCharacterSet = getPropertyValue("NLS_CHARACTERSET");
        nlsNcharCharacterSet = getPropertyValue("NLS_NCHAR_CHARACTERSET");
        outputBuffer->setNlsCharset(nlsCharacterSet, nlsNcharCharacterSet);

        if (databaseSequence == 0 || databaseScn == 0) {
            DatabaseStatement stmt(conn);
            TRACE_(TRACE2_SQL, SQL_GET_CURRENT_SEQUENCE);
            stmt.createStatement(SQL_GET_CURRENT_SEQUENCE);
            stmt.defineUInt32(1, databaseSequence);

            if (stmt.executeQuery())
                databaseScn = currentDatabaseScn;
        }

        INFO_("starting with:" <<
                    " scn: " << dec << databaseScn <<
                    " sequence: " << dec << databaseSequence <<
                    " resetlogs: " << dec << resetlogs <<
                    " activation: " << dec << activation <<
                    " con_id: " << dec << conId <<
                    " con_name: " << conName);

        if (databaseSequence == 0 || databaseScn == 0) {
            RUNTIME_FAIL("getting database sequence or current SCN");
        }

        TRACE_(TRACE2_SQL, SQL_GET_LOGFILE_LIST << endl << "PARAM1: " << modeType);
        stmt.createStatement(SQL_GET_LOGFILE_LIST);
        if (modeType == MODE_ONLINE || modeType == MODE_ASM)
            stmt.bindString(1, "ONLINE");
        else if (modeType == MODE_STANDBY)
            stmt.bindString(1, "STANDBY");
        else {
            RUNTIME_FAIL("unsupported log mode when looking for online redo logs");
        }
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

        if (modeType == MODE_ONLINE || modeType == MODE_ASM || modeType == MODE_STANDBY) {
            if (readers.size() == 0) {
                if (modeType == MODE_STANDBY) {
                    RUNTIME_FAIL("failed to find standby redo log files");
                } else {
                    RUNTIME_FAIL("failed to find online redo log files");
                }
            }
            checkOnlineRedoLogs();
        }
        archReader = readerCreate(0);
        readCheckpoint();
    }

    bool OracleAnalyser::readSchema(void) {
        ifstream infile;
        string fileName = database + "-schema.json";
        infile.open(fileName.c_str(), ios::in);

        INFO_("reading schema from JSON for " << database);

        if (!infile.is_open())
            return false;

        string schemaJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError()) {
            RUNTIME_FAIL("parsing of <database>-schema.json");
        }

        const Value& databaseJSON = getJSONfield(fileName, document, "database");
        database = databaseJSON.GetString();

        const Value& bigEndianJSON = getJSONfield(fileName, document, "big-endian");
        isBigEndian = bigEndianJSON.GetUint64();
        if (isBigEndian) {
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
        if (!onlineRedo.IsArray()) {
            CONFIG_FAIL("bad JSON in <database>-schema.json, online-redo should be an array");
        }

        for (SizeType i = 0; i < onlineRedo.Size(); ++i) {
            const Value& groupJSON = getJSONfield(fileName, onlineRedo[i], "group");
            uint64_t group = groupJSON.GetInt64();

            const Value& path = onlineRedo[i]["path"];
            if (!path.IsArray()) {
                CONFIG_FAIL("bad JSON, path-mapping should be array");
            }

            Reader *onlineReader = readerCreate(group);
            for (SizeType j = 0; j < path.Size(); ++j) {
                const Value& pathVal = path[j];
                onlineReader->paths.push_back(pathVal.GetString());
            }
        }

        if ((flags & REDO_FLAGS_ARCH_ONLY) == 0)
            checkOnlineRedoLogs();
        archReader = readerCreate(0);

        const Value& schema = getJSONfield(fileName, document, "schema");
        if (!schema.IsArray()) {
            CONFIG_FAIL("bad JSON in <database>-schema.json, schema should be an array");
        }

        for (SizeType i = 0; i < schema.Size(); ++i) {
            const Value& objnJSON = getJSONfield(fileName, schema[i], "objn");
            typeobj objn = objnJSON.GetInt64();

            typeobj objd = 0;
            if (schema[i].HasMember("objd")) {
                const Value& objdJSON = getJSONfield(fileName, schema[i], "objd");
                objd = objdJSON.GetInt64();
            }

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

            object = new OracleObject(objn, objd, cluCols, options, owner, objectName);
            object->totalPk = totalPk;
            object->maxSegCol = maxSegCol;

            const Value& columns = getJSONfield(fileName, schema[i], "columns");
            if (!columns.IsArray()) {
                CONFIG_FAIL("bad JSON in <database>-schema.json, columns should be an array");
            }

            for (SizeType j = 0; j < columns.Size(); ++j) {
                const Value& colNoJSON = getJSONfield(fileName, columns[j], "col-no");
                uint64_t colNo = colNoJSON.GetUint64();

                const Value& segColNoJSON = getJSONfield(fileName, columns[j], "seg-col-no");
                uint64_t segColNo = segColNoJSON.GetUint64();
                if (segColNo > 1000) {
                    CONFIG_FAIL("bad JSON in <database>-schema.json, invalid seg-col-no value");
                }

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

            if (schema[i].HasMember("partitions")) {
                const Value& partitions = getJSONfield(fileName, schema[i], "partitions");
                if (!columns.IsArray()) {
                    CONFIG_FAIL("bad JSON in <database>-schema.json, partitions should be an array");
                }

                for (SizeType j = 0; j < partitions.Size(); ++j) {
                    const Value& partitionObjnJSON = getJSONfield(fileName, partitions[j], "objn");
                    uint64_t partitionObjn = partitionObjnJSON.GetUint64();

                    const Value& partitionObjdJSON = getJSONfield(fileName, partitions[j], "objd");
                    uint64_t partitionObjd = partitionObjdJSON.GetUint64();

                    typeobj2 objx = (((typeobj2)partitionObjn)<<32) | ((typeobj2)partitionObjd);
                    object->partitions.push_back(objx);
                }
            }

            addToDict(object);
            object = nullptr;
        }

        infile.close();

        readCheckpoint();
        return true;
    }

    void OracleAnalyser::writeSchema(void) {
        INFO_("writing schema information for " << database);

        string fileName = database + "-schema.json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            RUNTIME_FAIL("writing schema data");
        }

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

        bool hasPrev = false, hasPrev2;
        for (Reader *reader : readers) {
            if (reader->group == 0)
                continue;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            hasPrev2 = false;
            ss << "{\"group\":" << reader->group << ",\"path\":[";
            for (string path : reader->paths) {
                if (hasPrev2)
                    ss << ",";
                else
                    hasPrev2 = true;

                ss << "\"";
                writeEscapeValue(ss, path);
                ss << "\"";
            }
            ss << "]}";
        }
        ss << "]," << "\"schema\":[";

        hasPrev = false;
        for (auto it : objectMap) {
            OracleObject *objectTmp = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"objn\":" << dec << objectTmp->objn << "," <<
                    "\"objd\":" << dec << objectTmp->objd << "," <<
                    "\"clu-cols\":" << dec << objectTmp->cluCols << "," <<
                    "\"total-pk\":" << dec << objectTmp->totalPk << "," <<
                    "\"options\":" << dec << objectTmp->options << "," <<
                    "\"max-seg-col\":" << dec << objectTmp->maxSegCol << "," <<
                    "\"owner\":\"" << objectTmp->owner << "\"," <<
                    "\"object-name\":\"" << objectTmp->objectName << "\"," <<
                    "\"columns\":[";

            for (uint64_t i = 0; i < objectTmp->columns.size(); ++i) {
                if (objectTmp->columns[i] == nullptr)
                    continue;

                if (i > 0)
                    ss << ",";
                ss << "{\"col-no\":" << dec << objectTmp->columns[i]->colNo << "," <<
                        "\"seg-col-no\":" << dec << objectTmp->columns[i]->segColNo << "," <<
                        "\"column-name\":\"" << objectTmp->columns[i]->columnName << "\"," <<
                        "\"type-no\":" << dec << objectTmp->columns[i]->typeNo << "," <<
                        "\"length\":" << dec << objectTmp->columns[i]->length << "," <<
                        "\"precision\":" << dec << objectTmp->columns[i]->precision << "," <<
                        "\"scale\":" << dec << objectTmp->columns[i]->scale << "," <<
                        "\"num-pk\":" << dec << objectTmp->columns[i]->numPk << "," <<
                        "\"charset-id\":" << dec << objectTmp->columns[i]->charsetId << "," <<
                        "\"nullable\":" << dec << objectTmp->columns[i]->nullable << "}";
            }
            ss << "]";

            if (objectTmp->partitions.size() > 0) {
                ss << ",\"partitions\":[";
                for (uint64_t i = 0; i < objectTmp->partitions.size(); ++i) {
                    if (i > 0)
                        ss << ",";
                    typeobj partitionObjn = objectTmp->partitions[i] >> 32;
                    typeobj partitionObjd = objectTmp->partitions[i] & 0xFFFFFFFF;
                    ss << "{\"objn\":" << dec << partitionObjn << "," <<
                            "\"objd\":" << dec << partitionObjd << "}";
                }
                ss << "]";
            }
            ss << "}";
        }

        ss << "]}";
        outfile << ss.rdbuf();
        outfile.close();
    }

    void *OracleAnalyser::run(void) {
        const char* modeStr;
        if (modeType == MODE_ONLINE)
            modeStr = "online";
        else if (modeType == MODE_ASM)
            modeStr = "asm";
        else if (modeType == MODE_OFFLINE)
            modeStr = "offline";
        else if (modeType == MODE_STANDBY)
            modeStr = "sandby";
        else if (modeType == MODE_BATCH)
            modeStr = "batch";

        TRACE_(TRACE2_THREADS, "ANALYSER (" << hex << this_thread::get_id() << ") START");

        INFO_("Oracle Analyser for " << database << " in " << modeStr << " mode is starting");
        if (modeType == MODE_ONLINE || modeType == MODE_ASM || modeType == MODE_STANDBY)
            checkConnection(true);

        uint64_t ret = REDO_OK;
        OracleAnalyserRedoLog *redo = nullptr;
        bool logsProcessed;

        try {
            while (!shutdown) {
                logsProcessed = false;

                //
                //ONLINE REDO LOGS READ
                //
                if ((flags & REDO_FLAGS_ARCH_ONLY) == 0) {
                    TRACE_(TRACE2_REDO, "checking online redo logs");
                    updateOnlineLogs();

                    while (!shutdown) {
                        redo = nullptr;
                        TRACE_(TRACE2_REDO, "searching online redo log for sequence: " << dec << databaseSequence);

                        //find the candidate to read
                        for (OracleAnalyserRedoLog *oracleAnalyserRedoLog : onlineRedoSet) {
                            if (oracleAnalyserRedoLog->sequence == databaseSequence)
                                redo = oracleAnalyserRedoLog;
                            TRACE_(TRACE2_REDO, oracleAnalyserRedoLog->path << " is " << dec << oracleAnalyserRedoLog->sequence);
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
                                INFO_("online redo log has been overwritten by new data, continuing reading from archived redo log");
                                break;
                            }
                            if (redo->group == 0) {
                                RUNTIME_FAIL("read archived redo log");
                            } else {
                                RUNTIME_FAIL("read online redo log");
                            }
                        }

                        if (rolledBack1 != nullptr)
                            freeRollbackList();

                        ++databaseSequence;
                        writeCheckpoint(false);
                    }
                }

                //
                //ARCHIVED REDO LOGS READ
                //
                if (shutdown)
                    break;
                TRACE_(TRACE2_REDO, "checking archive redo logs");
                archLogGetList();

                if (archiveRedoQueue.empty()) {
                    if ((flags & REDO_FLAGS_ARCH_ONLY) != 0) {
                        TRACE_(TRACE2_ARCHIVE_LIST, "archived redo log missing for sequence: " << dec << databaseSequence << ", sleeping");
                        usleep(archReadSleep);
                    } else {
                        RUNTIME_FAIL("could not find archive log for sequence: " << dec << databaseSequence);
                    }
                }

                while (!archiveRedoQueue.empty() && !shutdown) {
                    OracleAnalyserRedoLog *redoPrev = redo;
                    redo = archiveRedoQueue.top();
                    TRACE_(TRACE2_REDO, "searching archived redo log for sequence: " << dec << databaseSequence);

                    //when no checkpoint exists start processing from first file
                    if (databaseSequence == 0)
                        databaseSequence = redo->sequence;

                    //skip older archived redo logs
                    if (redo->sequence < databaseSequence) {
                        archiveRedoQueue.pop();
                        delete redo;
                        continue;
                    } else if (redo->sequence > databaseSequence) {
                        RUNTIME_FAIL("could not find archive log for sequence: " << dec << databaseSequence << ", found: " << redo->sequence << " instead");
                    }

                    logsProcessed = true;
                    redo->reader = archReader;

                    archReader->pathMapped = redo->path;
                    if (!readerCheckRedoLog(archReader)) {
                        RUNTIME_FAIL("opening archive log: " << redo->path);
                    }

                    if (!readerUpdateRedoLog(archReader)) {
                        RUNTIME_FAIL("reading archive log: " << redo->path);
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
                        RUNTIME_FAIL("archive log processing returned: " << dec << ret);
                    }

                    ++databaseSequence;
                    writeCheckpoint(false);
                    archiveRedoQueue.pop();
                    delete redo;
                    redo = nullptr;
                }

                if (shutdown)
                    break;

                if (modeType == MODE_BATCH) {
                    INFO_("finished batch processing, exiting");
                    stopMain();
                    break;
                }

                if (!logsProcessed)
                    usleep(redoReadSleep);
            }
        } catch(ConfigurationException &ex) {
            stopMain();
        } catch(RuntimeException &ex) {
            stopMain();
        }

        INFO_("Oracle analyser for: " << database << " is shutting down");

        writeCheckpoint(true);
        FULL_(*this);
        readerDropAll();

        INFO_("Oracle analyser for: " << database << " is shut down, allocated at most " << dec <<
                (memoryChunksHWM * MEMORY_CHUNK_SIZE_MB) << "MB memory");

        TRACE_(TRACE2_THREADS, "ANALYSER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }

    void OracleAnalyser::freeRollbackList(void) {
        RedoLogRecord *tmpRedoLogRecord1, *tmpRedoLogRecord2;
        uint64_t lostElements = 0;

        while (rolledBack1 != nullptr) {
            WARNING_("element on rollback list UBA: " << PRINTUBA(rolledBack1->uba) <<
                        " DBA: 0x" << hex << rolledBack2->dba <<
                        " SLT: " << dec << (uint64_t)rolledBack2->slt <<
                        " RCI: " << dec << (uint64_t)rolledBack2->rci <<
                        " SCN: " << PRINTSCN64(rolledBack2->scnRecord) <<
                        " OPFLAGS: " << hex << rolledBack2->opFlags);

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
        if (tmpRedoLogRecord1 == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << sizeof(RedoLogRecord) << " bytes memory for (rollback list#1)");
        }

        RedoLogRecord *tmpRedoLogRecord2 = new RedoLogRecord();
        if (tmpRedoLogRecord2 == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << sizeof(RedoLogRecord) << " bytes memory for (rollback list#2)");
        }

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
        OracleObject *objectTmp = partitionMap[objn];
        return objectTmp;
    }

    bool OracleAnalyser::readerCheckRedoLog(Reader *reader) {
        unique_lock<mutex> lck(mtx);
        reader->status = READER_STATUS_CHECK;
        reader->sequence = 0;
        reader->firstScn = ZERO_SCN;
        reader->nextScn = ZERO_SCN;

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
        for (Reader *reader : readers) {
            if (reader->started)
                pthread_join(reader->pthread, nullptr);
            delete reader;
        }
        archReader = nullptr;
        readers.clear();
    }

    void OracleAnalyser::checkTableForGrants(string tableName) {
        try {
            stringstream query;
            query << "SELECT 1 FROM " << tableName << " WHERE 0 = 1";
            string queryString(query.str());

            DatabaseStatement stmt(conn);
            TRACE_(TRACE2_SQL, queryString);
            stmt.createStatement(queryString);
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

    Reader *OracleAnalyser::readerCreate(int64_t group) {
        Reader *reader;

        if (modeType == MODE_ASM) {
            reader = new ReaderASM(alias, this, group);
        } else {
            reader = new ReaderFilesystem(alias, this, group);
        }

        if (reader == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << sizeof(ReaderFilesystem) << " bytes memory for (disk reader creation)");
        }

        readers.insert(reader);
        if (pthread_create(&reader->pthread, nullptr, &Reader::runStatic, (void*)reader)) {
            CONFIG_FAIL("spawning thread");
        }
        return reader;
    }

    void OracleAnalyser::checkOnlineRedoLogs() {
        for (Reader *reader : readers) {
            if (reader->group == 0)
                continue;

            bool foundPath = false;
            for (string path : reader->paths) {
                reader->pathMapped = applyMapping(path);
                if (readerCheckRedoLog(reader)) {
                    foundPath = true;
                    OracleAnalyserRedoLog* redo = new OracleAnalyserRedoLog(this, reader->group, reader->pathMapped);
                    if (redo == nullptr) {
                        readerDropAll();
                        RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleAnalyserRedoLog) << " bytes memory for (online redo logs)");
                    }

                    redo->reader = reader;
                    onlineRedoSet.insert(redo);
                    break;
                }
            }

            if (!foundPath) {
                uint64_t badGroup = reader->group;
                for (string path : reader->paths) {
                    string pathMapped = applyMapping(path);
                    ERROR("can't read: " << pathMapped);
                }
                readerDropAll();
                RUNTIME_FAIL("can't read any member of group " << dec << badGroup);
            }
        }
    }

    //checking if file name looks something like o1_mf_1_SSSS_XXXXXXXX_.arc
    //SS - sequence number
    uint64_t OracleAnalyser::getSequenceFromFileName(const char *file) {
        uint64_t sequence = 0, i, j, iMax = strnlen(file, 256);
        for (i = 0; i < iMax; ++i)
            if (file[i] == '_')
                break;

        //first '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] == '_')
                break;

        //second '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] == '_')
                break;

        //third '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] >= '0' && file[i] <= '9')
                sequence = sequence * 10 + (file[i] - '0');
            else
                break;

        //forth '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] == '_')
                break;

        if (i >= iMax || file[i] != '_')
            return 0;

        //fifth '_'
        if (strncmp(file + i, "_.arc", 5) != 0)
            return 0;

        return sequence;
    }

    void OracleAnalyser::addTable(string mask, vector<string> &keys, string &keysStr, uint64_t options) {
        checkConnection(false);
        INFO_("- reading table schema for: " << mask);
        uint64_t tabCnt = 0;
        DatabaseStatement stmt(conn);
        DatabaseStatement stmt2(conn);

        TRACE_(TRACE2_SQL, SQL_GET_TABLE_LIST << endl << "PARAM1: " << mask);
        stmt.createStatement(SQL_GET_TABLE_LIST);
        stmt.bindString(1, mask);
        typeobj objd; stmt.defineUInt32(1, objd);
        typeobj objn; stmt.defineUInt32(2, objn);
        uint64_t cluCols; stmt.defineUInt64(3, cluCols);
        char ownerStr[129]; stmt.defineString(4, ownerStr, sizeof(ownerStr));
        char objectNameStr[129]; stmt.defineString(5, objectNameStr, sizeof(objectNameStr));
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
        int64_t ret = stmt.executeQuery();

        while (ret) {
            string owner(ownerStr), objectName(objectNameStr);

            //skip Index Organized Tables (IOT)
            if (iot) {
                INFO_("  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - IOT");
                ret = stmt.next();
                continue;
            }

            //skip temporary tables
            if (temporary) {
                INFO_("  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - temporary table");
                ret = stmt.next();
                continue;
            }

            //skip nested tables
            if (nested) {
                INFO_("  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - nested table");
                ret = stmt.next();
                continue;
            }

            //skip compressed tables
            if (compressed) {
                INFO_("  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - compressed table");
                ret = stmt.next();
                continue;
            }

            if (stmt.isNull(1))
                objd = 0;

            //table already added with another rule
            if (checkDict(objn, objd) != nullptr) {
                INFO_("  * skipped: " << owner << "." << objectName << " (OBJN: " << dec << objn << ") - already added");
                ret = stmt.next();
                continue;
            }

            uint64_t totalPk = 0, maxSegCol = 0, keysCnt = 0;
            bool suppLogTablePrimary = false, suppLogTableAll = false, supLogColMissing = false;
            if (stmt.isNull(3))
                cluCols = 0;

            object = new OracleObject(objn, objd, cluCols, options, owner, objectName);
            if (object == nullptr) {
                RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleObject) << " bytes memory for (object creation)");
            }
            ++tabCnt;

            if (partitioned) {
                TRACE_(TRACE2_SQL, SQL_GET_PARTITION_LIST << endl << "PARAM1: " << dec << objn << endl << "PARAM2: " << dec << objn);
                stmt2.createStatement(SQL_GET_PARTITION_LIST);
                stmt2.bindUInt32(1, objn);
                stmt2.bindUInt32(2, objn);
                typeobj partitionObjn; stmt2.defineUInt32(1, partitionObjn);
                typeobj partitionObjd; stmt2.defineUInt32(2, partitionObjd);
                int64_t ret2 = stmt2.executeQuery();

                while (ret2) {
                    object->addPartition(partitionObjn, partitionObjd);
                    ret2 = stmt2.next();
                }
            }

            if ((disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0 && !suppLogDbAll && !suppLogSchemaAll && !suppLogSchemaAll) {
                TRACE_(TRACE2_SQL, SQL_GET_SUPPLEMNTAL_LOG_TABLE << endl << "PARAM1: " << dec << objn);
                stmt2.createStatement(SQL_GET_SUPPLEMNTAL_LOG_TABLE);
                stmt2.bindUInt32(1, objn);
                uint64_t typeNo2; stmt2.defineUInt64(1, typeNo2);
                int64_t ret2 = stmt2.executeQuery();

                while (ret2) {
                    if (typeNo2 == 14) suppLogTablePrimary = true;
                    else if (typeNo2 == 17) suppLogTableAll = true;
                    ret2 = stmt2.next();
                }
            }

            if ((flags & REDO_FLAGS_HIDE_INVISIBLE_COLUMNS) != 0) {
                TRACE_(TRACE2_SQL, SQL_GET_COLUMN_LIST_INV << endl << "PARAM1: " << dec << objn);
                stmt2.createStatement(SQL_GET_COLUMN_LIST_INV);
            } else {
                TRACE_(TRACE2_SQL, SQL_GET_COLUMN_LIST << endl << "PARAM1: " << dec << objn);
                stmt2.createStatement(SQL_GET_COLUMN_LIST);
            }
            stmt2.bindUInt32(1, objn);
            uint64_t colNo; stmt2.defineUInt64(1, colNo);
            uint64_t segColNo; stmt2.defineUInt64(2, segColNo);
            char columnNameStr[129]; stmt2.defineString(3, columnNameStr, sizeof(columnNameStr));
            uint64_t typeNo; stmt2.defineUInt64(4, typeNo);
            uint64_t length; stmt2.defineUInt64(5, length);
            int64_t precision; stmt2.defineInt64(6, precision);
            int64_t scale;stmt2.defineInt64(7, scale);
            uint64_t charsetForm; stmt2.defineUInt64(8, charsetForm);
            uint64_t charmapId; stmt2.defineUInt64(9, charmapId);
            int64_t nullable; stmt2.defineInt64(10, nullable);
            uint64_t numPk; stmt2.defineUInt64(11, numPk);
            uint64_t numSup; stmt2.defineUInt64(12, numSup);
            int64_t ret2 = stmt2.executeQuery();

            while (ret2) {
                string columnName(columnNameStr);
                if (stmt2.isNull(6))
                    precision = -1;
                if (stmt2.isNull(7))
                    scale = -1;

                if (charsetForm == 1)
                    charmapId = outputBuffer->defaultCharacterMapId;
                else if (charsetForm == 2)
                    charmapId = outputBuffer->defaultCharacterNcharMapId;

                //check character set for char and varchar2
                if (typeNo == 1 || typeNo == 96) {
                    if (outputBuffer->characterMap[charmapId] == nullptr) {
                        RUNTIME_FAIL("table " << owner << "." << objectName << " - unsupported character set id: " << dec << charmapId <<
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

                FULL_("    - col: " << dec << segColNo << ": " << columnName << " (pk: " << dec << numPk << ")");

                OracleColumn *column = new OracleColumn(colNo, segColNo, columnName, typeNo, length, precision, scale, numPk, charmapId, nullable);
                if (column == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleColumn) << " bytes memory for (column creation)");
                }

                totalPk += numPk;
                if (segColNo > maxSegCol)
                    maxSegCol = segColNo;

                object->addColumn(column);
                ret2 = stmt2.next();
            }

            //check if table has all listed columns
            if (keys.size() != keysCnt) {
                RUNTIME_FAIL("table " << owner << "." << objectName << " could not find all column set (" << keysStr << ")");
            }

            stringstream ss;
            ss << "  * found: " << owner << "." << objectName << " (OBJD: " << dec << objd << ", OBJN: " << dec << objn << ")";
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
                        ss << " - supplemental log missing, try: ALTER TABLE " << owner << "." << objectName << " ADD SUPPLEMENTAL LOG GROUP DATA (PRIMARY KEY) COLUMNS;";
                //user defined primary key
                } else {
                    if (!suppLogTableAll && !suppLogSchemaAll && !suppLogDbAll && supLogColMissing)
                        ss << " - supplemental log missing, try: ALTER TABLE " << owner << "." << objectName << " ADD SUPPLEMENTAL LOG GROUP GRP" << dec << objn << " (" << keysStr << ") ALWAYS;";
                }
            }
            INFO_(ss.str());

            object->maxSegCol = maxSegCol;
            object->totalPk = totalPk;
            addToDict(object);
            object = nullptr;
            ret = stmt.next();
        }
        INFO_("  * total: " << dec << tabCnt << " tables");
    }

    void OracleAnalyser::checkForCheckpoint(void) {
        uint64_t timeSinceCheckpoint = (clock() - previousCheckpoint) / CLOCKS_PER_SEC;
        if (timeSinceCheckpoint > checkpointInterval) {
            FULL_("time since last checkpoint: " << dec << timeSinceCheckpoint << "s, forcing checkpoint");
            writeCheckpoint(false);
        } else {
            FULL_("time since last checkpoint: " << dec << timeSinceCheckpoint << "s");
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
            memoryCond.notify_all();
        }
    }

    void OracleAnalyser::addPathMapping(const string source, const string target) {
        TRACE_(TRACE2_FILE, "added mapping [" << source << "] -> [" << target << "]");
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
                REDOLOG_FAIL("field length out of vector: field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                ", pos: " << dec << fieldPos << ", length:" << fieldLength << ", max: " << redoLogRecord->length);
            }
        }
    }

    void OracleAnalyser::addRedoLogsBatch(const string path) {
        redoLogsBatch.push_back(path);
    }

    void OracleAnalyser::nextField(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        ++fieldNum;
        if (fieldNum > redoLogRecord->fieldCnt) {
            REDOLOG_FAIL("field missing in vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", data: " << dec << redoLogRecord->rowData <<
                    ", objn: " << dec << redoLogRecord->objn <<
                    ", objd: " << dec << redoLogRecord->objd <<
                    ", op: " << hex << redoLogRecord->opCode <<
                    ", cc: " << dec << (uint64_t)redoLogRecord->cc <<
                    ", suppCC: " << dec << redoLogRecord->suppLogCC);
        }

        if (fieldNum == 1)
            fieldPos = redoLogRecord->fieldPos;
        else
            fieldPos += (fieldLength + 3) & 0xFFFC;
        fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + fieldNum * 2);

        if (fieldPos + fieldLength > redoLogRecord->length) {
            REDOLOG_FAIL("field length out of vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", pos: " << dec << fieldPos << ", length:" << fieldLength << " max: " << redoLogRecord->length);
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
            REDOLOG_FAIL("field length out of vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", pos: " << dec << fieldPos << ", length:" << fieldLength << " max: " << redoLogRecord->length);
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
        TRACE_(TRACE2_COMMIT_ROLLBACK, "ROLLBACK:" <<
                " OP: " << setw(4) << setfill('0') << hex << redoLogRecord->opCode << "    " <<
                " DBA: 0x" << hex << redoLogRecord->dba << "." << (uint64_t)redoLogRecord->slot <<
                " DBA: 0x" << hex << redoLogRecord->dba <<
                " SLT: " << dec << (uint64_t)redoLogRecord->slt <<
                " RCI: " << dec << (uint64_t)redoLogRecord->rci <<
                " SCN: " << PRINTSCN64(redoLogRecord->scn) <<
                " REC: " << PRINTSCN64(redoLogRecord->scnRecord) <<
                " " << msg);

        if (transaction != nullptr)
            TRACE_(TRACE2_COMMIT_ROLLBACK, "XID: " << PRINTXID(transaction->xid));
    }

    void OracleAnalyser::printRollbackInfo(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2, Transaction *transaction, const char *msg) {
        stringstream ss;
        TRACE_(TRACE2_ROLLBACK, "ROLLBACK: " << *redoLogRecord1 << " " << msg);
        TRACE_(TRACE2_ROLLBACK, "ROLLBACK: " << *redoLogRecord2 << " " << msg);
        if (transaction != nullptr) {
            TRACE_(TRACE2_ROLLBACK, "XID: " << PRINTXID(transaction->xid));
        }
    }

    uint8_t *OracleAnalyser::getMemoryChunk(const char *module, bool supp) {
        TRACE_(TRACE2_MEMORY, module << " - get at: " << dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            unique_lock<mutex> lck(mtx);

            if (memoryChunksFree == 0) {
                if (memoryChunksAllocated == memoryChunksMax) {
                    if (memoryChunksSupplemental > 0 && waitingForKafkaWriter) {
                        WARNING_("out of memory, sleeping until Kafka buffers are free and release some");
                        memoryCond.wait(lck);
                    }
                    if (memoryChunksAllocated == memoryChunksMax) {
                        RUNTIME_FAIL("used all memory up to memory-max-mb parameter, restart with higher value, module: " << module);
                    }
                }

                memoryChunks[0] = new uint8_t[MEMORY_CHUNK_SIZE];
                if (memoryChunks[0] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << (MEMORY_CHUNK_SIZE_MB) << " bytes memory for (reason: memory chunks#6)");
                }
                ++memoryChunksFree;
                ++memoryChunksAllocated;

                if (memoryChunksAllocated > memoryChunksHWM)
                    memoryChunksHWM = memoryChunksAllocated;
            }

            --memoryChunksFree;
            if (supp)
                ++memoryChunksSupplemental;
            return memoryChunks[memoryChunksFree];
        }
    }

    void OracleAnalyser::freeMemoryChunk(const char *module, uint8_t *chunk, bool supp) {
        TRACE_(TRACE2_MEMORY, module << " - free at: " << dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            unique_lock<mutex> lck(mtx);

            if (memoryChunksFree == memoryChunksAllocated) {
                RUNTIME_FAIL("trying to free unknown memory block for module: " << module);
            }

            //keep 25% reserved
            if (memoryChunksAllocated > memoryChunksMin && memoryChunksFree > memoryChunksAllocated / 4) {
                delete[] chunk;
                --memoryChunksAllocated;
            } else {
                memoryChunks[memoryChunksFree] = chunk;
                ++memoryChunksFree;
            }
            if (supp)
                --memoryChunksSupplemental;
        }
    }

    bool OracleAnalyserRedoLogCompare::operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool OracleAnalyserRedoLogCompareReverse::operator()(OracleAnalyserRedoLog* const& p1, OracleAnalyserRedoLog* const& p2) {
        return p1->sequence < p2->sequence;
    }

    ostream& operator<<(ostream& os, const OracleAnalyser& oracleAnalyser) {
        if (oracleAnalyser.transactionHeap->size > 0)
            os << "Transactions open: " << dec << oracleAnalyser.transactionHeap->size << endl;
        for (uint64_t i = 1; i <= oracleAnalyser.transactionHeap->size; ++i)
            os << "transaction[" << i << "]: " << *oracleAnalyser.transactionHeap->at(i) << endl;
        return os;
    }
}
