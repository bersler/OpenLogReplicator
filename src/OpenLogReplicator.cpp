/* Main program
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <execinfo.h>
#include <fcntl.h>
#include <list>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

uint64_t trace = 3, trace2 = 0;
#define TRACEVAR

#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleAnalyzerBatch.h"
#include "OutputBuffer.h"
#include "OutputBufferJson.h"
#include "RowId.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SchemaElement.h"
#include "Writer.h"
#include "WriterFile.h"

#ifdef LINK_LIBRARY_RDKAFKA
#include "WriterKafka.h"
#endif /* LINK_LIBRARY_RDKAFKA */

#ifdef LINK_LIBRARY_OCI
#include "OracleAnalyzerOnline.h"
#include "OracleAnalyzerOnlineASM.h"
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#include "OutputBufferProtobuf.h"
#include "StreamNetwork.h"
#include "WriterStream.h"
#ifdef LINK_LIBRARY_ZEROMQ
#include "StreamZeroMQ.h"
#endif /* LINK_LIBRARY_ZEROMQ */
#endif /* LINK_LIBRARY_PROTOBUF */

using namespace std;
using namespace rapidjson;
using namespace OpenLogReplicator;

const Value& getJSONfieldV(string &fileName, const Value& value, const char* field) {
    if (!value.HasMember(field)) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
    }
    return value[field];
}

const Value& getJSONfieldD(string &fileName, const Document& document, const char* field) {
    if (!document.HasMember(field)) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
    }
    return document[field];
}

mutex mainMtx;
condition_variable mainThread;
bool exitOnSignal = false;
bool mainShutdown = false;

void stopMain(void) {
    unique_lock<mutex> lck(mainMtx);

    mainShutdown = true;
    TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") STOP ALL");
    mainThread.notify_all();
}

void signalHandler(int s) {
    if (!exitOnSignal) {
        WARNING("caught signal " << s << ", exiting");
        exitOnSignal = true;
        stopMain();
    }
}

void signalCrash(int sig) {
    void *array[32];
    size_t size = backtrace(array, 32);
    ERROR("signal " << dec << sig);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
}

int main(int argc, char **argv) {
    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
    signal(SIGSEGV, signalCrash);
    uintX_t::initializeBASE10();

    INFO("OpenLogReplicator v." PACKAGE_VERSION " (C) 2018-2021 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");

    list<OracleAnalyzer *> analyzers;
    list<Writer *> writers;
    list<OutputBuffer *> buffers;
    OracleAnalyzer *oracleAnalyzer = nullptr;
    Writer *writer = nullptr;
    int fid = -1;
    char *configFileBuffer = nullptr;

    try {
        struct stat fileStat;
        string configFileName = "OpenLogReplicator.json";
        fid = open(configFileName.c_str(), O_RDONLY);
        if (fid == -1) {
            CONFIG_FAIL("can't open file " << configFileName);
        }

        if (flock(fid, LOCK_EX | LOCK_NB)) {
            CONFIG_FAIL("can't lock file " << configFileName << ", another process may be running");
        }

        int ret = stat(configFileName.c_str(), &fileStat);
        if (ret != 0) {
            CONFIG_FAIL("can't check file size of " << configFileName);
        }
        if (fileStat.st_size == 0) {
            CONFIG_FAIL("file " << configFileName << " is empty");
        }

        configFileBuffer = new char[fileStat.st_size + 1];
        if (configFileBuffer == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << (fileStat.st_size + 1) << " bytes memory (for: reading " << configFileName << ")");
        }
        if (read(fid, configFileBuffer, fileStat.st_size) != fileStat.st_size) {
            CONFIG_FAIL("can't read file " << configFileName);
        }
        configFileBuffer[fileStat.st_size] = 0;

        Document document;
        if (document.Parse(configFileBuffer).HasParseError()) {
            CONFIG_FAIL("parsing " << configFileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()));
        }

        const Value& versionJSON = getJSONfieldD(configFileName, document, "version");
        if (strcmp(versionJSON.GetString(), PACKAGE_VERSION) != 0) {
            CONFIG_FAIL("bad JSON, incompatible \"version\" value, expected: " << PACKAGE_VERSION << ", got: " << versionJSON.GetString());
        }

        //optional
        uint64_t dumpRedoLog = 0;
        if (document.HasMember("dump-redo-log")) {
            const Value& dumpRedoLogJSON = document["dump-redo-log"];
            dumpRedoLog = dumpRedoLogJSON.GetUint64();
        }

        //optional
        if (document.HasMember("trace")) {
            const Value& traceJSON = document["trace"];
            trace = traceJSON.GetUint64();
        }

        //optional
        if (document.HasMember("trace2")) {
            const Value& traceJSON = document["trace2"];
            trace2 = traceJSON.GetUint64();
        }
        TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") START");

        //optional
        uint64_t dumpRawData = 0;
        if (document.HasMember("dump-raw-data")) {
            const Value& dumpRawDataJSON = document["dump-raw-data"];
            dumpRawData = dumpRawDataJSON.GetUint64();
        }

        //iterate through sources
        const Value& sourcesJSON = getJSONfieldD(configFileName, document, "sources");
        if (!sourcesJSON.IsArray()) {
            CONFIG_FAIL("bad JSON, \"sources\" should be an array");
        }

        for (SizeType i = 0; i < sourcesJSON.Size(); ++i) {
            const Value& sourceJSON = sourcesJSON[i];
            const Value& aliasJSON = getJSONfieldV(configFileName, sourceJSON, "alias");
            INFO("adding source: " << aliasJSON.GetString());

            //optional
            uint64_t memoryMinMb = 32;
            if (sourceJSON.HasMember("memory-min-mb")) {
                const Value& memoryMinMbJSON = sourceJSON["memory-min-mb"];
                memoryMinMb = memoryMinMbJSON.GetUint64();
                memoryMinMb = (memoryMinMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMinMb < MEMORY_CHUNK_MIN_MB) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value must be at least " MEMORY_CHUNK_MIN_MB_CHR);
                }
            }

            //optional
            uint64_t memoryMaxMb = 1024;
            if (sourceJSON.HasMember("memory-max-mb")) {
                const Value& memoryMaxMbJSON = sourceJSON["memory-max-mb"];
                memoryMaxMb = memoryMaxMbJSON.GetUint64();
                memoryMaxMb = (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMaxMb < memoryMinMb) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value can't be greater than \"memory-max-mb\" value");
                }
            }

            //optional
            uint64_t readBufferMax = memoryMaxMb / 4 / MEMORY_CHUNK_SIZE_MB;
            if (readBufferMax > 256 / MEMORY_CHUNK_SIZE_MB)
                readBufferMax = 256 / MEMORY_CHUNK_SIZE_MB;

            if (sourceJSON.HasMember("read-buffer-max-mb")) {
                const Value& readBufferMaxMbJSON = sourceJSON["read-buffer-max-mb"];
                readBufferMax = readBufferMaxMbJSON.GetUint64() / MEMORY_CHUNK_SIZE_MB;
                if (readBufferMax * MEMORY_CHUNK_SIZE_MB > memoryMaxMb) {
                    CONFIG_FAIL("bad JSON, \"read-buffer-max-mb\" value can't be greater than \"memory-max-mb\" value");
                }
                if (readBufferMax <= 1) {
                    CONFIG_FAIL("bad JSON, \"read-buffer-max-mb\" value should be at least " << dec << MEMORY_CHUNK_SIZE_MB * 2);
                }
            }

            const Value& nameJSON = getJSONfieldV(configFileName, sourceJSON, "name");

            //FORMAT
            const Value& formatJSON = getJSONfieldV(configFileName, sourceJSON, "format");

            //optional
            uint64_t messageFormat = 0;
            if (formatJSON.HasMember("message")) {
                const Value& messageFormatJSON = formatJSON["message"];
                messageFormat = messageFormatJSON.GetUint64();
                if (messageFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"message\" value: " << messageFormatJSON.GetString() << ", expected one of: {0, 1}");
                }
            }

            //optional
            uint64_t xidFormat = XID_FORMAT_TEXT;
            if (formatJSON.HasMember("xid")) {
                const Value& xidFormatJSON = formatJSON["xid"];
                xidFormat = xidFormatJSON.GetUint64();
                if (xidFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"xid\" value: " << xidFormatJSON.GetString() << ", expected one of: {0, 1}");
                }
            }

            //optional
            uint64_t timestampFormat = TIMESTAMP_FORMAT_UNIX;
            if (formatJSON.HasMember("timestamp")) {
                const Value& timestampFormatJSON = formatJSON["timestamp"];
                timestampFormat = timestampFormatJSON.GetUint64();
                if (timestampFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"timestamp\" value: " << timestampFormatJSON.GetString() << ", expected one of: {0, 1, 2, 3}");
                }
            }

            //optional
            uint64_t charFormat = CHAR_FORMAT_UTF8;
            if (formatJSON.HasMember("char")) {
                const Value& charFormatJSON = formatJSON["char"];
                charFormat = charFormatJSON.GetUint64();
                if (charFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"char\" value: " << charFormatJSON.GetString() << ", expected one of: {0, 1, 2, 3}");
                }
            }

            //optional
            uint64_t scnFormat = SCN_FORMAT_NUMERIC;
            if (formatJSON.HasMember("scn")) {
                const Value& scnFormatJSON = formatJSON["scn"];
                scnFormat = scnFormatJSON.GetUint64();
                if (scnFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"scn\" value: " << scnFormatJSON.GetString() << ", expected one of: {0, 1, 2, 3}");
                }
            }

            //optional
            uint64_t unknownFormat = UNKNOWN_FORMAT_QUESTION;
            if (formatJSON.HasMember("unknown")) {
                const Value& unknownFormatJSON = formatJSON["unknown"];
                unknownFormat = unknownFormatJSON.GetUint64();
                if (unknownFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"unknown\" value: " << unknownFormatJSON.GetString() << ", expected one of: {0, 1}");
                }
            }

            //optional
            uint64_t schemaFormat = SCHEMA_FORMAT_NAME;
            if (formatJSON.HasMember("schema")) {
                const Value& schemaFormatJSON = formatJSON["schema"];
                schemaFormat = schemaFormatJSON.GetUint64();
                if (schemaFormat > 7) {
                    CONFIG_FAIL("bad JSON, invalid \"schema\" value: " << schemaFormatJSON.GetString() << ", expected one of: {0, 1, 2, 3, 4, 5, 6, 7}");
                }
            }

            //optional
            uint64_t columnFormat = COLUMN_FORMAT_CHANGED;
            if (formatJSON.HasMember("column")) {
                const Value& columnFormatJSON = formatJSON["column"];
                columnFormat = columnFormatJSON.GetUint64();
                if (columnFormat > 2) {
                    CONFIG_FAIL("bad JSON, invalid \"column\" value: " << columnFormatJSON.GetString() << ", expected one of: {0, 1, 2}");
                }
            }

            //optional
            uint64_t unknownType = UNKNOWN_TYPE_HIDE;
            if (formatJSON.HasMember("unknown-type")) {
                const Value& unknownTypeJSON = formatJSON["unknown-type"];
                unknownType = unknownTypeJSON.GetUint64();
                if (unknownType > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"unknown-type\" value: " << unknownTypeJSON.GetString() << ", expected one of: {0, 1}");
                }
            }

            const Value& formatTypeJSON = getJSONfieldV(configFileName, formatJSON, "type");

            OutputBuffer *outputBuffer = nullptr;
            if (strcmp("json", formatTypeJSON.GetString()) == 0) {
                outputBuffer = new OutputBufferJson(messageFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                        schemaFormat, columnFormat, unknownType);
            } else if (strcmp("protobuf", formatTypeJSON.GetString()) == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                outputBuffer = new OutputBufferProtobuf(messageFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                        schemaFormat, columnFormat, unknownType);
#else
                RUNTIME_FAIL("format \"protobuf\" is not compiled, exiting");
#endif /* LINK_LIBRARY_PROTOBUF */
                } else {
                CONFIG_FAIL("bad JSON, invalid \"type\" value: " << formatTypeJSON.GetString());
            }

            if (outputBuffer == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OutputBuffer) << " bytes memory (for: command buffer)");
            }
            buffers.push_back(outputBuffer);


            //READER
            const Value& readerJSON = getJSONfieldV(configFileName, sourceJSON, "reader");

            //optional
            uint64_t disableChecks = 0;
            if (readerJSON.HasMember("disable-checks")) {
                const Value& disableChecksJSON = readerJSON["disable-checks"];
                disableChecks = disableChecksJSON.GetUint64();
            }

            const Value& readerTypeJSON = getJSONfieldV(configFileName, readerJSON, "type");
            if (strcmp(readerTypeJSON.GetString(), "online") == 0 ||
                    strcmp(readerTypeJSON.GetString(), "online-standby") == 0) {
#ifdef LINK_LIBRARY_OCI
                bool isStandby = false;
                if (strcmp(readerTypeJSON.GetString(), "online-standby") == 0)
                    isStandby = true;

                const char *user = "";
                const Value& userJSON = getJSONfieldV(configFileName, readerJSON, "user");
                user = userJSON.GetString();

                const char *password = "";
                const Value& passwordJSON = getJSONfieldV(configFileName, readerJSON, "password");
                password = passwordJSON.GetString();

                const char *server = "";
                const Value& serverJSON = getJSONfieldV(configFileName, readerJSON, "server");
                server = serverJSON.GetString();

                oracleAnalyzer = new OracleAnalyzerOnline(outputBuffer, dumpRedoLog, dumpRawData, aliasJSON.GetString(),
                        nameJSON.GetString(), memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, user, password, server, isStandby);

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }

                if (readerJSON.HasMember("path-mapping")) {
                    const Value& pathMapping = readerJSON["path-mapping"];
                    if (!pathMapping.IsArray()) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should be array");
                    }
                    if ((pathMapping.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (SizeType j = 0; j < pathMapping.Size() / 2; ++j) {
                        const Value& sourceMapping = pathMapping[j * 2];
                        const Value& targetMapping = pathMapping[j * 2 + 1];
                        oracleAnalyzer->addPathMapping(sourceMapping.GetString(), targetMapping.GetString());
                    }
                }

                //optional
                if (sourceJSON.HasMember("arch")) {
                    const Value& archJSON = sourceJSON["arch"];
                    if (strcmp(archJSON.GetString(), "path") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzer::archGetLogPath;
                    else if (strcmp(archJSON.GetString(), "online") == 0) {
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                        ((OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection = false;
                    } else if (strcmp(archJSON.GetString(), "online-keep") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                    else {
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << archJSON.GetString() << ", expected one of (\"path\", \"online\", \"online-keep\") reader");
                    }
                }
#else
                RUNTIME_FAIL("reader type \"online\" is not compiled, exiting");
#endif /*LINK_LIBRARY_OCI*/

            } else if (strcmp(readerTypeJSON.GetString(), "offline") == 0) {

                oracleAnalyzer = new OracleAnalyzer(outputBuffer, dumpRedoLog, dumpRawData, aliasJSON.GetString(),
                        nameJSON.GetString(), memoryMinMb, memoryMaxMb, readBufferMax, disableChecks);

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }

                if (readerJSON.HasMember("path-mapping")) {
                    const Value& pathMapping = readerJSON["path-mapping"];
                    if (!pathMapping.IsArray()) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should be array");
                    }
                    if ((pathMapping.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (SizeType j = 0; j < pathMapping.Size() / 2; ++j) {
                        const Value& sourceMapping = pathMapping[j * 2];
                        const Value& targetMapping = pathMapping[j * 2 + 1];
                        oracleAnalyzer->addPathMapping(sourceMapping.GetString(), targetMapping.GetString());
                    }
                }

            } else if (strcmp(readerTypeJSON.GetString(), "asm") == 0 ||
                    strcmp(readerTypeJSON.GetString(), "asm-standby") == 0) {
#ifdef LINK_LIBRARY_OCI
                bool isStandby = false;
                if (strcmp(readerTypeJSON.GetString(), "asm-standby") == 0)
                    isStandby = true;

                const char *user = "";
                const Value& userJSON = getJSONfieldV(configFileName, readerJSON, "user");
                user = userJSON.GetString();

                const char *password = "";
                const Value& passwordJSON = getJSONfieldV(configFileName, readerJSON, "password");
                password = passwordJSON.GetString();

                const char *server = "";
                const Value& serverJSON = getJSONfieldV(configFileName, readerJSON, "server");
                server = serverJSON.GetString();

                const char *userASM = "";
                const Value& userASMJSON = getJSONfieldV(configFileName, readerJSON, "user-asm");
                userASM = userASMJSON.GetString();

                const char *passwordASM = "";
                const Value& passwordASMJSON = getJSONfieldV(configFileName, readerJSON, "password-asm");
                passwordASM = passwordASMJSON.GetString();

                const char *serverASM = "";
                const Value& serverASMJSON = getJSONfieldV(configFileName, readerJSON, "server-asm");
                serverASM = serverASMJSON.GetString();

                oracleAnalyzer = new OracleAnalyzerOnlineASM(outputBuffer, dumpRedoLog, dumpRawData, aliasJSON.GetString(),
                        nameJSON.GetString(), memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, user, password, server,
                        userASM, passwordASM, serverASM, isStandby);

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }

                //optional
                if (sourceJSON.HasMember("arch")) {
                    const Value& archJSON = sourceJSON["arch"];
                    if (strcmp(archJSON.GetString(), "path") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzer::archGetLogPath;
                    else if (strcmp(archJSON.GetString(), "online") == 0) {
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                        ((OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection = false;
                    } else if (strcmp(archJSON.GetString(), "online-keep") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                    else {
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << archJSON.GetString() << ", expected one of (\"path\", \"online\", \"online-keep\") reader");
                    }
                }
#else
                RUNTIME_FAIL("reader types \"online\", \"asm\" are not compiled, exiting");
#endif /*LINK_LIBRARY_OCI*/

            } else if (strcmp(readerTypeJSON.GetString(), "batch") == 0) {

                 //optional
                 typeconid conId = 0;
                 if (readerJSON.HasMember("con-id")) {
                     const Value& conIdJSON = readerJSON["con-id"];
                     conId = conIdJSON.GetUint();
                 }

                 oracleAnalyzer = new OracleAnalyzerBatch(outputBuffer, dumpRedoLog, dumpRawData, aliasJSON.GetString(),
                         nameJSON.GetString(), memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, conId);
                 oracleAnalyzer->flags |= REDO_FLAGS_ARCH_ONLY;

                 if (oracleAnalyzer == nullptr) {
                     RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzerBatch) << " bytes memory (for: oracle analyzer)");
                 }

                 if (!readerJSON.HasMember("redo-logs")) {
                     CONFIG_FAIL("bad JSON, missing \"redo-logs\" element which is required in \"batch\" reader type");
                 }

                 const Value& redoLogsBatch = readerJSON["redo-logs"];
                 if (!redoLogsBatch.IsArray()) {
                     CONFIG_FAIL("bad JSON, \"redo-logs\" field should be array");
                 }

                 for (SizeType j = 0; j < redoLogsBatch.Size(); ++j) {
                     const Value& path = redoLogsBatch[j];
                     oracleAnalyzer->addRedoLogsBatch(path.GetString());
                 }

                 oracleAnalyzer->archGetLog = OracleAnalyzer::archGetLogList;

            } else {
                CONFIG_FAIL("bad JSON, invalid \"format\" value: " << readerTypeJSON.GetString());
            }

            outputBuffer->initialize(oracleAnalyzer);

            if (sourceJSON.HasMember("event-table")) {
                const Value& eventtableJSON = sourceJSON["event-table"];
                SchemaElement *element = oracleAnalyzer->schema->addElement();
                element->mask = eventtableJSON.GetString();
                element->options = 1;
            }

            const Value& tablesJSON = getJSONfieldV(configFileName, sourceJSON, "tables");
            if (!tablesJSON.IsArray()) {
                CONFIG_FAIL("bad JSON, field \"tables\" should be array");
            }

            for (SizeType j = 0; j < tablesJSON.Size(); ++j) {
                const Value& tableJSON = getJSONfieldV(configFileName, tablesJSON[j], "table");
                SchemaElement *element = oracleAnalyzer->schema->addElement();
                element->mask = tableJSON.GetString();

                if (tablesJSON[j].HasMember("key")) {
                    const Value& key = tablesJSON[j]["key"];
                    element->keysStr = key.GetString();
                    stringstream keyStream(element->keysStr);

                    while (keyStream.good()) {
                        string keyCol, keyCol2;
                        getline(keyStream, keyCol, ',');
                        keyCol.erase(remove(keyCol.begin(), keyCol.end(), ' '), keyCol.end());
                        transform(keyCol.begin(), keyCol.end(),keyCol.begin(), ::toupper);
                        element->keys.push_back(keyCol);
                    }
                } else
                    element->keysStr = "";
            }

            //optional
            if (sourceJSON.HasMember("flags")) {
                const Value& flagsJSON = sourceJSON["flags"];
                oracleAnalyzer->flags |= flagsJSON.GetUint64();
            }

            //optional
            if (sourceJSON.HasMember("redo-verify-delay-us")) {
                const Value& redoVerifyDelayUSJSON = sourceJSON["redo-verify-delay-us"];
                oracleAnalyzer->redoVerifyDelayUS = redoVerifyDelayUSJSON.GetUint64();
            }

            //optional
            if (sourceJSON.HasMember("arch-read-sleep-us")) {
                const Value& archReadSleepUSJSON = sourceJSON["arch-read-sleep-us"];
                oracleAnalyzer->archReadSleepUS = archReadSleepUSJSON.GetUint64();
            }

            //optional
            if (sourceJSON.HasMember("arch-read-retry")) {
                const Value& archReadRetryJSON = sourceJSON["arch-read-retry"];
                oracleAnalyzer->archReadRetry = archReadRetryJSON.GetUint64();
            }

            //optional
            if (sourceJSON.HasMember("redo-read-sleep-us")) {
                const Value& redoReadSleepUSJSON = sourceJSON["redo-read-sleep-us"];
                oracleAnalyzer->redoReadSleepUS = redoReadSleepUSJSON.GetUint64();
            }

            //optional
            if (readerJSON.HasMember("redo-copy-path")) {
                const Value& redoCopyPathJSON = readerJSON["redo-copy-path"];
                oracleAnalyzer->redoCopyPath = redoCopyPathJSON.GetString();
            }

            //optional
            if (readerJSON.HasMember("log-archive-format")) {
                const Value& logArchiveFormatJSON = readerJSON["log-archive-format"];
                oracleAnalyzer->logArchiveFormat = logArchiveFormatJSON.GetString();
            }

            if (pthread_create(&oracleAnalyzer->pthread, nullptr, &Thread::runStatic, (void*)oracleAnalyzer)) {
                RUNTIME_FAIL("error spawning thread - oracle analyzer");
            }

            analyzers.push_back(oracleAnalyzer);
            oracleAnalyzer = nullptr;
        }

        //iterate through targets
        const Value& targetsJSON = getJSONfieldD(configFileName, document, "targets");
        if (!targetsJSON.IsArray()) {
            CONFIG_FAIL("bad JSON, field \"targets\" should be array");
        }
        for (SizeType i = 0; i < targetsJSON.Size(); ++i) {
            const Value& targetJSON = targetsJSON[i];
            const Value& aliasJSON = getJSONfieldV(configFileName, targetJSON, "alias");
            INFO("adding target: " << aliasJSON.GetString());

            const Value& sourceJSON = getJSONfieldV(configFileName, targetJSON, "source");
            OracleAnalyzer *oracleAnalyzer = nullptr;
            for (OracleAnalyzer *analyzer : analyzers)
                if (analyzer->alias.compare(sourceJSON.GetString()) == 0)
                    oracleAnalyzer = (OracleAnalyzer*)analyzer;
            if (oracleAnalyzer == nullptr) {
                CONFIG_FAIL("bad JSON, couldn't find reader for \"source\" value: " << sourceJSON.GetString());
            }

            //writer
            const Value& writerJSON = getJSONfieldV(configFileName, targetJSON, "writer");
            const Value& writerTypeJSON = getJSONfieldV(configFileName, writerJSON, "type");

            //optional
            uint64_t pollIntervalUS = 100000;
            if (writerJSON.HasMember("poll-interval-us")) {
                const Value& pollIntervalUSJSON = writerJSON["poll-interval-us"];
                pollIntervalUS = pollIntervalUSJSON.GetUint64();
                if (pollIntervalUS < 100 || pollIntervalUS > 3600000000) {
                    CONFIG_FAIL("bad JSON, invalid \"poll-interval-us\" value: " << pollIntervalUSJSON.GetString() << ", expected from 100 to 3600000000");
                }
            }

            //optional
            typeSCN startScn = 0;
            if (writerJSON.HasMember("start-scn")) {
                const Value& startScnJSON = writerJSON["start-scn"];
                startScn = startScnJSON.GetUint64();
            }

            //optional
            typeSEQ startSequence = 0;
            if (writerJSON.HasMember("start-seq")) {
                if (startScn > 0) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-seq\"");
                }
                const Value& startSequenceJSON = writerJSON["start-seq"];
                startSequence = startSequenceJSON.GetUint64();
            }

            //optional
            int64_t startTimeRel = 0;
            if (writerJSON.HasMember("start-time-rel")) {
                if (startScn > 0) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-time-rel\"");
                }
                if (startSequence > 0) {
                    CONFIG_FAIL("bad JSON, \"start-seq\" used together with \"start-time-rel\"");
                }
                const Value& startTimeRelJSON = writerJSON["start-time-rel"];
                startTimeRel = startTimeRelJSON.GetInt64();
            }

            //optional
            const char *startTime = "";
            if (writerJSON.HasMember("start-time")) {
                if (startScn > 0) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-time\"");
                }
                if (startSequence > 0) {
                    CONFIG_FAIL("bad JSON, \"start-seq\" used together with \"start-time\"");
                }
                if (startTimeRel > 0) {
                    CONFIG_FAIL("bad JSON, \"start-time-rel\" used together with \"start-time\"");
                }
                const Value& startTimeJSON = writerJSON["start-time"];
                startTime = startTimeJSON.GetString();
            }

            //optional
            uint64_t checkpointIntervalS = 10;
            if (writerJSON.HasMember("checkpoint-interval-s")) {
                const Value& checkpointIntervalSJSON = writerJSON["checkpoint-interval-s"];
                checkpointIntervalS = checkpointIntervalSJSON.GetUint64();
            }

            //optional
            uint64_t queueSize = 65536;
            if (writerJSON.HasMember("queue-size")) {
                const Value& queueSizeJSON = writerJSON["queue-size"];
                queueSize = queueSizeJSON.GetUint64();
                if (queueSize < 1 || queueSize > 1000000) {
                    CONFIG_FAIL("bad JSON, invalid \"queue-size\" value: " << queueSizeJSON.GetString() << ", expected from 1 to 1000000");
                }
            }

            if (strcmp(writerTypeJSON.GetString(), "file") == 0) {
                const char *name = "";
                if (writerJSON.HasMember("name")) {
                    const Value& nameJSON = writerJSON["name"];
                    name = nameJSON.GetString();
                }

                writer = new WriterFile(aliasJSON.GetString(), oracleAnalyzer, name, pollIntervalUS, checkpointIntervalS, queueSize,
                        startScn, startSequence, startTime, startTimeRel);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterFile) << " bytes memory (for: file writer)");
                }
            } else if (strcmp(writerTypeJSON.GetString(), "kafka") == 0) {
#ifdef LINK_LIBRARY_RDKAFKA
                uint64_t maxMessageMb = 100;
                if (writerJSON.HasMember("max-message-mb")) {
                    const Value& maxMessageMbJSON = writerJSON["max-message-mb"];
                    maxMessageMb = maxMessageMbJSON.GetUint64();
                    if (maxMessageMb < 1)
                        maxMessageMb = 1;
                    if (maxMessageMb > MAX_KAFKA_MESSAGE_MB)
                        maxMessageMb = MAX_KAFKA_MESSAGE_MB;
                }

                uint64_t maxMessages = 100000;
                if (writerJSON.HasMember("max-messages")) {
                    const Value& maxMessagesJSON = writerJSON["max-messages"];
                    maxMessages = maxMessagesJSON.GetUint64();
                    if (maxMessages < 1)
                        maxMessages = 1;
                    if (maxMessages > MAX_KAFKA_MAX_MESSAGES)
                        maxMessages = MAX_KAFKA_MAX_MESSAGES;
                }

                //optional
                uint64_t enableIdempocence = 1;
                if (writerJSON.HasMember("enable-idempocence")) {
                    const Value& enableIdempocenceJSON = writerJSON["enable-idempocence"];
                    enableIdempocence = enableIdempocenceJSON.GetUint64();
                    if (enableIdempocence > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"enable-idempocence\" value: " << enableIdempocenceJSON.GetString() << ", expected values {0, 1}");
                    }
                }

                const Value& brokersJSON = getJSONfieldV(configFileName, writerJSON, "brokers");
                const Value& topicJSON = getJSONfieldV(configFileName, writerJSON, "topic");

                writer = new WriterKafka(aliasJSON.GetString(), oracleAnalyzer, brokersJSON.GetString(),
                        topicJSON.GetString(), maxMessageMb, maxMessages, pollIntervalUS, checkpointIntervalS, queueSize,
                        startScn, startSequence, startTime, startTimeRel, enableIdempocence);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterKafka) << " bytes memory (for: Kafka writer)");
                }
#else
                RUNTIME_FAIL("writer Kafka is not compiled, exiting")
#endif /* LINK_LIBRARY_RDKAFKA */
            } else if (strcmp(writerTypeJSON.GetString(), "zeromq") == 0) {
#if defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ)
                const Value& uriJSON = getJSONfieldV(configFileName, writerJSON, "uri");

                StreamZeroMQ *stream = new StreamZeroMQ(uriJSON.GetString(), pollIntervalUS);
                if (stream == nullptr) {
                    RUNTIME_FAIL("network stream creation failed");
                }

                writer = new WriterStream(aliasJSON.GetString(), oracleAnalyzer, pollIntervalUS, checkpointIntervalS,
                        queueSize, startScn, startSequence, startTime, startTimeRel, stream);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterStream) << " bytes memory (for: ZeroMQ writer)");
                }
#else
                RUNTIME_FAIL("writer ZeroMQ is not compiled, exiting")
#endif /* defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ) */
            } else if (strcmp(writerTypeJSON.GetString(), "network") == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                const Value& uriJSON = getJSONfieldV(configFileName, writerJSON, "uri");

                StreamNetwork *stream = new StreamNetwork(uriJSON.GetString(), pollIntervalUS);
                if (stream == nullptr) {
                    RUNTIME_FAIL("network stream creation failed");
                }

                writer = new WriterStream(aliasJSON.GetString(), oracleAnalyzer, pollIntervalUS, checkpointIntervalS,
                        queueSize, startScn, startSequence, startTime, startTimeRel, stream);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterStream) << " bytes memory (for: ZeroMQ writer)");
                }
#else
                RUNTIME_FAIL("writer Network is not compiled, exiting")
#endif /* LINK_LIBRARY_PROTOBUF */
            } else {
                CONFIG_FAIL("bad JSON: invalid \"type\" value: " << writerTypeJSON.GetString());
            }

            oracleAnalyzer->outputBuffer->setWriter(writer);
            if (pthread_create(&writer->pthread, nullptr, &Thread::runStatic, (void*)writer)) {
                RUNTIME_FAIL("error spawning thread - kafka writer");
            }

            writers.push_back(writer);
            writer = nullptr;
        }

        //sleep until killed
        {
            unique_lock<mutex> lck(mainMtx);
            if (!mainShutdown)
                mainThread.wait(lck);
        }

    } catch (ConfigurationException &ex) {
    } catch (RuntimeException &ex) {
    }

    if (oracleAnalyzer != nullptr)
        analyzers.push_back(oracleAnalyzer);

    if (writer != nullptr)
        writers.push_back(writer);

    //shut down all analyzers
    for (OracleAnalyzer *analyzer : analyzers)
        analyzer->doShutdown();
    for (OracleAnalyzer *analyzer : analyzers)
        if (analyzer->started)
            pthread_join(analyzer->pthread, nullptr);

    //shut down writers
    for (Writer *writer : writers) {
        writer->doStop();
        if ((writer->oracleAnalyzer->flags & REDO_FLAGS_FLUSH_QUEUE_ON_EXIT) == 0)
            writer->doShutdown();
    }
    for (OutputBuffer *outputBuffer : buffers) {
        unique_lock<mutex> lck(outputBuffer->mtx);
        outputBuffer->writersCond.notify_all();
    }
    for (Writer *writer : writers) {
        if (writer->started)
            pthread_join(writer->pthread, nullptr);
        delete writer;
    }
    writers.clear();

    for (OutputBuffer *outputBuffer : buffers)
        delete outputBuffer;
    buffers.clear();

    for (OracleAnalyzer *analyzer : analyzers)
        delete analyzer;
    analyzers.clear();

    if (fid != -1)
        close(fid);
    if (configFileBuffer != nullptr)
        delete[] configFileBuffer;

    TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") STOP");
    return 0;
}
