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
#include <errno.h>
#include <fcntl.h>
#include <list>
#include <regex>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "global.h"
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

int main(int argc, char** argv) {
    mainThread = pthread_self();
    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
    signal(SIGSEGV, signalCrash);
    signal(SIGUSR1, signalDump);
    uintX_t::initializeBASE10();

    INFO("OpenLogReplicator v." PACKAGE_VERSION " (C) 2018-2021 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");

    list<OracleAnalyzer*> analyzers;
    list<Writer*> writers;
    list<OutputBuffer*> buffers;
    OracleAnalyzer* oracleAnalyzer = nullptr;
    Writer* writer = nullptr;
    int fid = -1;
    char* configFileBuffer = nullptr;

    try {
        TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") START");

        regex regexTest(".*");
        string regexString("check if matches!");
        bool regexWorks = regex_search(regexString, regexTest);
        if (!regexWorks) {
            CONFIG_FAIL("binaries are build with no regex implementation, check if you have gcc version >= 4.9");
        }

        string fileName("scripts/OpenLogReplicator.json");
        if (argc == 2 && (strncmp(argv[1], "-v", 2) == 0 || strncmp(argv[1], "--version", 9) == 0)) {
            // print banner and exit
            return 0;
        } else if (argc == 3 && (strncmp(argv[1], "-f", 2) == 0 || strncmp(argv[1], "--file", 6) == 0)) {
            // custom config path
            fileName = argv[2];
        } else if (argc > 1) {
            CONFIG_FAIL("invalid arguments, run: " << argv[0] << " [-v|--version] or [-f|--file CONFIG] default path for CONFIG file is " << fileName);
        }

        if (getuid() == 0) {
            CONFIG_FAIL("program is run as root, you should never do that");
        }

        struct stat fileStat;
        fid = open(fileName.c_str(), O_RDONLY);
        if (fid == -1) {
            CONFIG_FAIL("opening in read mode file: " << fileName << " - " << strerror(errno));
        }

        if (flock(fid, LOCK_EX | LOCK_NB)) {
            CONFIG_FAIL("locking file: " << fileName << ", another process may be running - " << strerror(errno));
        }

        int ret = stat(fileName.c_str(), &fileStat);
        if (ret != 0) {
            CONFIG_FAIL("reading information for file: " << fileName << " - " << strerror(errno));
        }
        if (fileStat.st_size == 0) {
            CONFIG_FAIL("file " << fileName << " is empty");
        }

        configFileBuffer = new char[fileStat.st_size + 1];
        if (configFileBuffer == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << (fileStat.st_size + 1) << " bytes memory (for: reading " << fileName << ")");
        }
        if (read(fid, configFileBuffer, fileStat.st_size) != fileStat.st_size) {
            CONFIG_FAIL("can't read file " << fileName);
        }
        configFileBuffer[fileStat.st_size] = 0;

        Document document;
        if (document.Parse(configFileBuffer).HasParseError()) {
            CONFIG_FAIL("parsing " << fileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()));
        }

        const char* version = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, document, "version");
        if (strcmp(version, CONFIG_SCHEMA_VERSION) != 0) {
            CONFIG_FAIL("bad JSON, incompatible \"version\" value: " << version << ", expected: " << CONFIG_SCHEMA_VERSION);
        }

        uint64_t dumpRedoLog = 0;
        if (document.HasMember("dump-redo-log")) {
            dumpRedoLog = getJSONfieldU(fileName, document, "dump-redo-log");
            if (dumpRedoLog > 2) {
                CONFIG_FAIL("bad JSON, invalid \"dump-redo-log\" value: " << dec << dumpRedoLog << ", expected one of: {0, 1, 2}");
            }
        }

        if (document.HasMember("trace")) {
            trace = getJSONfieldU(fileName, document, "trace");
            if (trace > 4) {
                CONFIG_FAIL("bad JSON, invalid \"trace\" value: " << dec << trace << ", expected one of: {0, 1, 2, 3, 4}");
            }
        }

        if (document.HasMember("trace2")) {
            trace2 = getJSONfieldU(fileName, document, "trace2");
            if (trace2 > 65535) {
                CONFIG_FAIL("bad JSON, invalid \"trace2\" value: " << dec << trace2 << ", expected one of: {0 .. 65535}");
            }
        }

        uint64_t dumpRawData = 0;
        if (document.HasMember("dump-raw-data")) {
            dumpRawData = getJSONfieldU(fileName, document, "dump-raw-data");
            if (dumpRawData > 1) {
                CONFIG_FAIL("bad JSON, invalid \"dump-raw-data\" value: " << dec << dumpRawData << ", expected one of: {0, 1}");
            }
        }

        //iterate through sources
        const Value& sourceArrayJSON = getJSONfieldA(fileName, document, "source");

        for (SizeType i = 0; i < sourceArrayJSON.Size(); ++i) {
            const Value& sourceJSON = getJSONfieldO(fileName, sourceArrayJSON, "source", i);

            const char* alias = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "alias");
            INFO("adding source: " << alias);

            uint64_t memoryMinMb = 32;
            if (sourceJSON.HasMember("memory-min-mb")) {
                memoryMinMb = getJSONfieldU(fileName, sourceJSON, "memory-min-mb");
                memoryMinMb = (memoryMinMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMinMb < MEMORY_CHUNK_MIN_MB) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value must be at least " MEMORY_CHUNK_MIN_MB_CHR);
                }
            }

            uint64_t memoryMaxMb = 1024;
            if (sourceJSON.HasMember("memory-max-mb")) {
                memoryMaxMb = getJSONfieldU(fileName, sourceJSON, "memory-max-mb");
                memoryMaxMb = (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMaxMb < memoryMinMb) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value can't be greater than \"memory-max-mb\" value");
                }
            }

            uint64_t readBufferMax = memoryMaxMb / 4 / MEMORY_CHUNK_SIZE_MB;
            if (readBufferMax > 32 / MEMORY_CHUNK_SIZE_MB)
                readBufferMax = 32 / MEMORY_CHUNK_SIZE_MB;

            if (sourceJSON.HasMember("read-buffer-max-mb")) {
                readBufferMax = getJSONfieldU(fileName, sourceJSON, "read-buffer-max-mb") / MEMORY_CHUNK_SIZE_MB;
                if (readBufferMax * MEMORY_CHUNK_SIZE_MB > memoryMaxMb) {
                    CONFIG_FAIL("bad JSON, \"read-buffer-max-mb\" value can't be greater than \"memory-max-mb\" value");
                }
                if (readBufferMax <= 1) {
                    CONFIG_FAIL("bad JSON, \"read-buffer-max-mb\" value should be at least " << dec << MEMORY_CHUNK_SIZE_MB * 2);
                }
            }

            const char* name = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "name");

            //FORMAT
            const Value& formatJSON = getJSONfieldO(fileName, sourceJSON, "format");

            uint64_t messageFormat = MESSAGE_FORMAT_DEFAULT;
            if (formatJSON.HasMember("message")) {
                messageFormat = getJSONfieldU(fileName, formatJSON, "message");
                if (messageFormat > 15) {
                    CONFIG_FAIL("bad JSON, invalid \"message\" value: " << dec << messageFormat << ", expected one of: {0.. 15}");
                }
                if ((messageFormat & MESSAGE_FORMAT_FULL) != 0 &&
                        (messageFormat & (MESSAGE_FORMAT_SKIP_BEGIN | MESSAGE_FORMAT_SKIP_COMMIT)) != 0) {
                    CONFIG_FAIL("bad JSON, invalid \"message\" value: " << dec << messageFormat <<
                            ", you are not allowed to use BEGIN/COMMIT flag (" << dec << MESSAGE_FORMAT_SKIP_BEGIN << "/" <<
                            MESSAGE_FORMAT_SKIP_COMMIT << ") together with FULL mode (" << dec << MESSAGE_FORMAT_FULL << ")");
                }
            }

            uint64_t ridFormat = RID_FORMAT_SKIP;
            if (formatJSON.HasMember("rid")) {
                ridFormat = getJSONfieldU(fileName, formatJSON, "rid");
                if (ridFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"rid\" value: " << dec << ridFormat << ", expected one of: {0, 1}");
                }
            }

            uint64_t xidFormat = XID_FORMAT_TEXT;
            if (formatJSON.HasMember("xid")) {
                xidFormat = getJSONfieldU(fileName, formatJSON, "xid");
                if (xidFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"xid\" value: " << dec << xidFormat << ", expected one of: {0, 1}");
                }
            }

            uint64_t timestampFormat = TIMESTAMP_FORMAT_UNIX;
            if (formatJSON.HasMember("timestamp")) {
                timestampFormat = getJSONfieldU(fileName, formatJSON, "timestamp");
                if (timestampFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"timestamp\" value: " << dec << timestampFormat << ", expected one of: {0, 1, 2, 3}");
                }
            }

            uint64_t charFormat = CHAR_FORMAT_UTF8;
            if (formatJSON.HasMember("char")) {
                charFormat = getJSONfieldU(fileName, formatJSON, "char");
                if (charFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"char\" value: " << dec << charFormat << ", expected one of: {0, 1, 2, 3}");
                }
            }

            uint64_t scnFormat = SCN_FORMAT_NUMERIC;
            if (formatJSON.HasMember("scn")) {
                scnFormat = getJSONfieldU(fileName, formatJSON, "scn");
                if (scnFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"scn\" value: " << dec << scnFormat << ", expected one of: {0, 1, 2, 3}");
                }
            }

            uint64_t unknownFormat = UNKNOWN_FORMAT_QUESTION_MARK;
            if (formatJSON.HasMember("unknown")) {
                unknownFormat = getJSONfieldU(fileName, formatJSON, "unknown");
                if (unknownFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"unknown\" value: " << dec << unknownFormat << ", expected one of: {0, 1}");
                }
            }

            uint64_t schemaFormat = SCHEMA_FORMAT_NAME;
            if (formatJSON.HasMember("schema")) {
                schemaFormat = getJSONfieldU(fileName, formatJSON, "schema");
                if (schemaFormat > 7) {
                    CONFIG_FAIL("bad JSON, invalid \"schema\" value: " << dec << schemaFormat << ", expected one of: {0 .. 7}");
                }
            }

            uint64_t columnFormat = COLUMN_FORMAT_CHANGED;
            if (formatJSON.HasMember("column")) {
                columnFormat = getJSONfieldU(fileName, formatJSON, "column");
                if (columnFormat > 2) {
                    CONFIG_FAIL("bad JSON, invalid \"column\" value: " << dec << columnFormat << ", expected one of: {0, 1, 2}");
                }
            }

            uint64_t unknownType = UNKNOWN_TYPE_HIDE;
            if (formatJSON.HasMember("unknown-type")) {
                unknownType = getJSONfieldU(fileName, formatJSON, "unknown-type");
                if (unknownType > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"unknown-type\" value: " << dec << unknownType << ", expected one of: {0, 1}");
                }
            }

            uint64_t flushBuffer = 1048576;
            if (formatJSON.HasMember("flush-buffer"))
                flushBuffer = getJSONfieldU(fileName, formatJSON, "flush-buffer");

            const char* formatType = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, formatJSON, "type");

            OutputBuffer* outputBuffer = nullptr;
            if (strcmp("json", formatType) == 0) {
                outputBuffer = new OutputBufferJson(messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                        schemaFormat, columnFormat, unknownType, flushBuffer);
            } else if (strcmp("protobuf", formatType) == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                outputBuffer = new OutputBufferProtobuf(messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                        schemaFormat, columnFormat, unknownType, flushBuffer);
#else
                RUNTIME_FAIL("format \"protobuf\" is not compiled, exiting");
#endif /* LINK_LIBRARY_PROTOBUF */
                } else {
                CONFIG_FAIL("bad JSON, invalid \"type\" value: " << formatType);
            }

            if (outputBuffer == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OutputBuffer) << " bytes memory (for: command buffer)");
            }
            buffers.push_back(outputBuffer);


            //READER
            const Value& readerJSON = getJSONfieldO(fileName, sourceJSON, "reader");

            uint64_t disableChecks = 0;
            if (readerJSON.HasMember("disable-checks")) {
                disableChecks = getJSONfieldU(fileName, readerJSON, "disable-checks");
                if (disableChecks > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"disable-checks\" value: " << dec << disableChecks << ", expected one of: {0, 1}");
                }
            }

            const char* readerType = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, readerJSON, "type");

            if (strcmp(readerType, "online") == 0 ||
                    strcmp(readerType, "online-standby") == 0) {
#ifdef LINK_LIBRARY_OCI
                bool standby = false;
                if (strcmp(readerType, "online-standby") == 0)
                    standby = true;

                const char* user = getJSONfieldS(fileName, JSON_USERNAME_LENGTH, readerJSON, "user");
                const char* password = getJSONfieldS(fileName, JSON_PASSWORD_LENGTH, readerJSON, "password");
                const char* server = getJSONfieldS(fileName, JSON_SERVER_LENGTH, readerJSON, "server");

                oracleAnalyzer = new OracleAnalyzerOnline(outputBuffer, dumpRedoLog, dumpRawData, alias,
                        name, memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, user, password, server, standby);

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }

                if (readerJSON.HasMember("path-mapping")) {
                    const Value& pathMappingArrayJSON = getJSONfieldA(fileName, readerJSON, "path-mapping");
                    if ((pathMappingArrayJSON.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (SizeType j = 0; j < pathMappingArrayJSON.Size() / 2; ++j) {
                        const char* sourceMapping = getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2);
                        const char* targetMapping = getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2 + 1);
                        oracleAnalyzer->addPathMapping(sourceMapping, targetMapping);
                    }
                }

                if (sourceJSON.HasMember("arch")) {
                    const char* arch = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "arch");

                    if (strcmp(arch, "path") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzer::archGetLogPath;
                    else if (strcmp(arch, "online") == 0) {
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                    } else if (strcmp(arch, "online-keep") == 0) {
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                        ((OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection = true;
                    } else {
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << arch << ", expected one of: {\"path\", \"online\", \"online-keep\"}");
                    }
                } else
                    oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
#else
                RUNTIME_FAIL("reader type \"online\" is not compiled, exiting");
#endif /*LINK_LIBRARY_OCI*/

            } else if (strcmp(readerType, "offline") == 0) {

                oracleAnalyzer = new OracleAnalyzer(outputBuffer, dumpRedoLog, dumpRawData, alias,
                        name, memoryMinMb, memoryMaxMb, readBufferMax, disableChecks);

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }

                if (readerJSON.HasMember("path-mapping")) {
                    const Value& pathMappingArrayJSON = getJSONfieldA(fileName, readerJSON, "path-mapping");

                    if ((pathMappingArrayJSON.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (SizeType j = 0; j < pathMappingArrayJSON.Size() / 2; ++j) {
                        const char* sourceMapping = getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2);
                        const char* targetMapping = getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2 + 1);
                        oracleAnalyzer->addPathMapping(sourceMapping, targetMapping);
                    }
                }

            } else if (strcmp(readerType, "asm") == 0 || strcmp(readerType, "asm-standby") == 0) {
#ifdef LINK_LIBRARY_OCI
                bool standby = false;
                if (strcmp(readerType, "asm-standby") == 0)
                    standby = true;

                const char* user = getJSONfieldS(fileName, JSON_USERNAME_LENGTH, readerJSON, "user");
                const char* password = getJSONfieldS(fileName, JSON_PASSWORD_LENGTH, readerJSON, "password");
                const char* server = getJSONfieldS(fileName, JSON_SERVER_LENGTH, readerJSON, "server");
                const char* userASM = getJSONfieldS(fileName, JSON_USERNAME_LENGTH, readerJSON, "user-asm");
                const char* passwordASM = getJSONfieldS(fileName, JSON_PASSWORD_LENGTH, readerJSON, "password-asm");
                const char* serverASM = getJSONfieldS(fileName, JSON_SERVER_LENGTH, readerJSON, "server-asm");

                oracleAnalyzer = new OracleAnalyzerOnlineASM(outputBuffer, dumpRedoLog, dumpRawData, alias, name, memoryMinMb, memoryMaxMb,
                        readBufferMax, disableChecks, user, password, server, userASM, passwordASM, serverASM, standby);

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }

                if (sourceJSON.HasMember("arch")) {
                    const char* arch = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "arch");

                    if (strcmp(arch, "path") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzer::archGetLogPath;
                    else if (strcmp(arch, "online") == 0) {
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                        ((OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection = false;
                    } else if (strcmp(arch, "online-keep") == 0)
                        oracleAnalyzer->archGetLog = OracleAnalyzerOnline::archGetLogOnline;
                    else {
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << arch << ", expected one of: {\"path\", \"online\", \"online-keep\"}");
                    }
                }
#else
                RUNTIME_FAIL("reader types \"online\", \"asm\" are not compiled, exiting");
#endif /*LINK_LIBRARY_OCI*/

            } else if (strcmp(readerType, "batch") == 0) {

                typeCONID conId = 0;
                if (readerJSON.HasMember("con-id"))
                    conId = getJSONfieldI(fileName, readerJSON, "con-id");

                oracleAnalyzer = new OracleAnalyzerBatch(outputBuffer, dumpRedoLog, dumpRawData, alias,
                        name, memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, conId);
                oracleAnalyzer->flags |= REDO_FLAGS_ARCH_ONLY;

                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleAnalyzerBatch) << " bytes memory (for: oracle analyzer)");
                }

                const Value& redoLogBatchArrayJSON = getJSONfieldA(fileName, readerJSON, "redo-log");

                for (SizeType j = 0; j < redoLogBatchArrayJSON.Size(); ++j)
                    oracleAnalyzer->addRedoLogsBatch(getJSONfieldS(fileName, MAX_PATH_LENGTH, redoLogBatchArrayJSON, "redo-log", j));

                oracleAnalyzer->archGetLog = OracleAnalyzer::archGetLogList;

            } else {
                CONFIG_FAIL("bad JSON, invalid \"format\" value: " << readerType);
            }

            outputBuffer->initialize(oracleAnalyzer);

            if (sourceJSON.HasMember("debug")) {
                const Value& debugJSON = getJSONfieldO(fileName, sourceJSON, "debug");

                if (debugJSON.HasMember("stop-log-switches")) {
                    oracleAnalyzer->stopLogSwitches = getJSONfieldU(fileName, debugJSON, "stop-log-switches");
                    INFO("will shutdown after " << dec << oracleAnalyzer->stopLogSwitches << " log switches");
                }

                if (debugJSON.HasMember("stop-checkpoints")) {
                    oracleAnalyzer->stopCheckpoints = getJSONfieldU(fileName, debugJSON, "stop-checkpoints");
                    INFO("will shutdown after " << dec << oracleAnalyzer->stopCheckpoints << " checkpoints");
                }

                if (debugJSON.HasMember("stop-transactions")) {
                    oracleAnalyzer->stopTransactions = getJSONfieldU(fileName, debugJSON, "stop-transactions");
                    INFO("will shutdown after " << dec << oracleAnalyzer->stopTransactions << " transactions");
                }

                if (debugJSON.HasMember("flush-buffer")) {
                    uint64_t stopFlushBuffer = getJSONfieldU(fileName, debugJSON, "flush-buffer");
                    if (stopFlushBuffer == 1) {
                        oracleAnalyzer->stopFlushBuffer = stopFlushBuffer;
                    } else
                    if (stopFlushBuffer > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"flush-buffer\" value: " << dec << stopFlushBuffer << ", expected one of: {0, 1}");
                    }
                }

                if (debugJSON.HasMember("owner") || debugJSON.HasMember("table")) {
                    const char* debugOwner = getJSONfieldS(fileName, SYSUSER_NAME_LENGTH, debugJSON, "owner");
                    const char* debugTable = getJSONfieldS(fileName, SYSOBJ_NAME_LENGTH, debugJSON, "table");

                    oracleAnalyzer->schema->addElement(debugOwner, debugTable, OPTIONS_DEBUG_TABLE);
                    INFO("will shutdown after committed DML in " << debugOwner << "." << debugTable);
                }
            }

            oracleAnalyzer->schema->addElement("SYS", "CCOL\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "CDEF\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "COL\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "DEFERRED_STG\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "ECOL\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "OBJ\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "SEG\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TAB\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TABPART\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TABCOMPART\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TABSUBPART\\$", OPTIONS_SCHEMA_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "USER\\$", OPTIONS_SCHEMA_TABLE);

            if (sourceJSON.HasMember("filter")) {
                const Value& filterJSON = getJSONfieldO(fileName, sourceJSON, "filter");

                if (filterJSON.HasMember("table")) {
                    const Value& tableArrayJSON = getJSONfieldA(fileName, filterJSON, "table");

                    for (SizeType j = 0; j < tableArrayJSON.Size(); ++j) {
                        const Value& tableElementJSON = getJSONfieldO(fileName, tableArrayJSON, "table", j);

                        const char* owner = getJSONfieldS(fileName, SYSUSER_NAME_LENGTH, tableElementJSON, "owner");
                        const char* table = getJSONfieldS(fileName, SYSOBJ_NAME_LENGTH, tableElementJSON, "table");
                        SchemaElement* element = oracleAnalyzer->schema->addElement(owner, table, 0);

                        if (tableElementJSON.HasMember("key")) {
                            element->keysStr = getJSONfieldS(fileName, JSON_KEY_LENGTH, tableElementJSON, "key");
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
                }

                if (filterJSON.HasMember("skip-xid")) {
                    const Value& skipXidArrayJSON = getJSONfieldA(fileName, filterJSON, "skip-xid");
                    for (SizeType j = 0; j < skipXidArrayJSON.Size(); ++j) {
                        const char* skipXid = getJSONfieldS(fileName, JSON_XID_LIST_LENGTH, skipXidArrayJSON, "skip-xid", j);

                        typeXID xid = 0;
                        bool invalid = false;
                        string usn, slt, sqn;

                        uint64_t length = strnlen(skipXid, 25);
                        //UUUUSSSSQQQQQQQQ
                        if (length == 16) {
                            for (uint64_t i = 0; i < 16; ++i)
                                if (!iswxdigit(skipXid[i])) {
                                    invalid = true;
                                    break;
                                }
                            if (!invalid) {
                                usn.assign(skipXid, 4);
                                slt.assign(skipXid + 4, 4);
                                sqn.assign(skipXid + 8, 8);
                            }
                        } else
                        //UUUU.SSS.QQQQQQQQ
                        if (length == 17) {
                            for (uint64_t i = 0; i < 17; ++i)
                                if (!iswxdigit(skipXid[i]) && i != 4 && i != 8) {
                                    invalid = true;
                                    break;
                                }
                            if (skipXid[4] != '.' || skipXid[8] != '.')
                                invalid = true;
                            if (!invalid) {
                                usn.assign(skipXid, 4);
                                slt.assign(skipXid + 5, 3);
                                sqn.assign(skipXid + 9, 8);
                            }
                        } else
                        //UUUU.SSSS.QQQQQQQQ
                        if (length == 18) {
                            for (uint64_t i = 0; i < 18; ++i)
                                if (!iswxdigit(skipXid[i]) && i != 4 && i != 9) {
                                    invalid = true;
                                    break;
                                }
                            if (skipXid[4] != '.' || skipXid[9] != '.')
                                invalid = true;
                            if (!invalid) {
                                usn.assign(skipXid, 4);
                                slt.assign(skipXid + 5, 4);
                                sqn.assign(skipXid + 10, 8);
                            }
                        } else
                        //0xUUUU.SSS.QQQQQQQQ
                        if (length == 19) {
                            for (uint64_t i = 2; i < 19; ++i)
                                if (!iswxdigit(skipXid[i]) && i != 6 && i != 10) {
                                    invalid = true;
                                    break;
                                }
                            if (skipXid[0] != '0' || skipXid[1] != 'x' || skipXid[6] != '.' || skipXid[10] != '.')
                                invalid = true;
                            if (!invalid) {
                                usn.assign(skipXid + 2, 4);
                                slt.assign(skipXid + 7, 3);
                                sqn.assign(skipXid + 11, 8);
                            }
                        } else
                        //0xUUUU.SSSS.QQQQQQQQ
                        if (length == 20) {
                            for (uint64_t i = 2; i < 20; ++i)
                                if (!iswxdigit(skipXid[i]) && i != 6 && i != 11) {
                                    invalid = true;
                                    break;
                                }
                            if (skipXid[0] != '0' || skipXid[1] != 'x' || skipXid[6] != '.' || skipXid[11] != '.')
                                invalid = true;
                            if (!invalid) {
                                usn.assign(skipXid + 2, 4);
                                slt.assign(skipXid + 7, 4);
                                sqn.assign(skipXid + 12, 8);
                            }
                        } else
                            invalid = true;

                        if (invalid) {
                            CONFIG_FAIL("bad JSON, field \"skip-xid\" has invalid value: " << skipXid);
                        }

                        xid = XID(stoul(usn, nullptr, 16), stoul(slt, nullptr, 16), stoul(sqn, nullptr, 16));
                        INFO("adding XID to skip list: " << PRINTXID(xid));
                        oracleAnalyzer->skipXidList.insert(xid);
                    }
                }

                if (filterJSON.HasMember("transaction-max-mb")) {
                    uint64_t transactionMaxMb = getJSONfieldU(fileName, filterJSON, "transaction-max-mb");
                    if (transactionMaxMb > memoryMaxMb) {
                        CONFIG_FAIL("bad JSON, \"transaction-max-mb\" (" << dec << transactionMaxMb <<
                                ") is bigger than \"memory-max-mb\" (" << memoryMaxMb << ")");
                    }
                    oracleAnalyzer->transactionMax = transactionMaxMb * 1024 * 1024;
                }
            }

            if (sourceJSON.HasMember("flags")) {
                uint64_t flags = getJSONfieldU(fileName, sourceJSON, "flags");
                if (flags > 4095) {
                    CONFIG_FAIL("bad JSON, invalid \"flags\" value: " << dec << flags << ", expected one of: {0 .. 4095}");
                }
                oracleAnalyzer->flags |= flags;
            }

            if (sourceJSON.HasMember("redo-verify-delay-us"))
                oracleAnalyzer->redoVerifyDelayUS = getJSONfieldU(fileName, sourceJSON, "redo-verify-delay-us");

            if (sourceJSON.HasMember("arch-read-sleep-us"))
                oracleAnalyzer->archReadSleepUS = getJSONfieldU(fileName, sourceJSON, "arch-read-sleep-us");

            if (sourceJSON.HasMember("arch-read-tries")) {
                oracleAnalyzer->archReadTries = getJSONfieldU(fileName, sourceJSON, "arch-read-tries");
                if (oracleAnalyzer->archReadTries < 1 || oracleAnalyzer->archReadTries > 1000000000) {
                    CONFIG_FAIL("bad JSON, invalid \"arch-read-tries\" value: " << dec << oracleAnalyzer->archReadTries << ", expected one of: {1, 1000000000}");
                }
            }

            if (sourceJSON.HasMember("redo-read-sleep-us"))
                oracleAnalyzer->redoReadSleepUS = getJSONfieldU(fileName, sourceJSON, "redo-read-sleep-us");

            if (readerJSON.HasMember("redo-copy-path"))
                oracleAnalyzer->redoCopyPath = getJSONfieldS(fileName, MAX_PATH_LENGTH, readerJSON, "redo-copy-path");

            if (readerJSON.HasMember("log-archive-format"))
                oracleAnalyzer->logArchiveFormat = getJSONfieldS(fileName, VPARAMETER_LENGTH, readerJSON, "log-archive-format");

            if (sourceJSON.HasMember("checkpoint")) {
                const Value& checkpointJSON = getJSONfieldO(fileName, sourceJSON, "checkpoint");

                if (checkpointJSON.HasMember("path"))
                    oracleAnalyzer->checkpointPath = getJSONfieldS(fileName, MAX_PATH_LENGTH, checkpointJSON, "path");

                if (checkpointJSON.HasMember("interval-s"))
                    oracleAnalyzer->checkpointIntervalS = getJSONfieldU(fileName, checkpointJSON, "interval-s");

                if (checkpointJSON.HasMember("interval-mb"))
                    oracleAnalyzer->checkpointIntervalMB = getJSONfieldU(fileName, checkpointJSON, "interval-mb");

                if (checkpointJSON.HasMember("all")) {
                    uint64_t all = getJSONfieldU(fileName, checkpointJSON, "all");
                    if (all <= 1)
                        oracleAnalyzer->checkpointAll = all;
                    else if (all > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"all\" value: " << dec << all << ", expected one of: {0, 1}");
                    }
                }

                if (checkpointJSON.HasMember("output-checkpoint")) {
                    uint64_t outputCheckpoint = getJSONfieldU(fileName, checkpointJSON, "output-checkpoint");
                    if (outputCheckpoint <= 1)
                        oracleAnalyzer->checkpointOutputCheckpoint = outputCheckpoint;
                    else if (outputCheckpoint > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"output-checkpoint\" value: " << dec << outputCheckpoint << ", expected one of: {0, 1}");
                    }
                }

                if (checkpointJSON.HasMember("output-log-switch")) {
                    uint64_t outputLogSwitch = getJSONfieldU(fileName, checkpointJSON, "output-log-switch");
                    if (outputLogSwitch <= 1)
                        oracleAnalyzer->checkpointOutputLogSwitch = outputLogSwitch;
                    else if (outputLogSwitch > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"output-log-switch\" value: " << dec << outputLogSwitch << ", expected one of: {0, 1}");
                    }
                }
            }

            if (pthread_create(&oracleAnalyzer->pthread, nullptr, &Thread::runStatic, (void*)oracleAnalyzer)) {
                RUNTIME_FAIL("spawning thread - oracle analyzer");
            }

            analyzers.push_back(oracleAnalyzer);
            oracleAnalyzer = nullptr;
        }

        //iterate through targets
        const Value& targetArrayJSON = getJSONfieldA(fileName, document, "target");

        for (SizeType i = 0; i < targetArrayJSON.Size(); ++i) {
            const Value& targetJSON = targetArrayJSON[i];
            const char* alias = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, targetJSON, "alias");
            const char* source = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, targetJSON, "source");

            INFO("adding target: " << alias);
            OracleAnalyzer* oracleAnalyzer = nullptr;
            for (OracleAnalyzer* analyzer : analyzers)
                if (analyzer->alias.compare(source) == 0)
                    oracleAnalyzer = (OracleAnalyzer*)analyzer;
            if (oracleAnalyzer == nullptr) {
                CONFIG_FAIL("bad JSON, couldn't find reader for \"source\" value: " << source);
            }

            //writer
            const Value& writerJSON = getJSONfieldO(fileName, targetJSON, "writer");
            const char* writerType = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "type");

            uint64_t pollIntervalUS = 100000;
            if (writerJSON.HasMember("poll-interval-us")) {
                pollIntervalUS = getJSONfieldU(fileName, writerJSON, "poll-interval-us");
                if (pollIntervalUS < 100 || pollIntervalUS > 3600000000) {
                    CONFIG_FAIL("bad JSON, invalid \"poll-interval-us\" value: " << dec << pollIntervalUS << ", expected one of: {100 .. 3600000000}");
                }
            }

            typeSCN startScn = ZERO_SCN;
            if (writerJSON.HasMember("start-scn"))
                startScn = getJSONfieldU(fileName, writerJSON, "start-scn");

            typeSEQ startSequence = ZERO_SEQ;
            if (writerJSON.HasMember("start-seq"))
                startSequence = getJSONfieldU(fileName, writerJSON, "start-seq");

            int64_t startTimeRel = 0;
            if (writerJSON.HasMember("start-time-rel")) {
                if (startScn != ZERO_SCN) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-time-rel\"");
                }
                startTimeRel = getJSONfieldI(fileName, writerJSON, "start-time-rel");
            }

            const char* startTime = "";
            if (writerJSON.HasMember("start-time")) {
                if (startScn != ZERO_SCN) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-time\"");
                }
                if (startTimeRel > 0) {
                    CONFIG_FAIL("bad JSON, \"start-time-rel\" used together with \"start-time\"");
                }

                startTime = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "start-time");
            }

            uint64_t checkpointIntervalS = 10;
            if (writerJSON.HasMember("checkpoint-interval-s"))
                checkpointIntervalS = getJSONfieldU(fileName, writerJSON, "checkpoint-interval-s");

            uint64_t queueSize = 65536;
            if (writerJSON.HasMember("queue-size")) {
                queueSize = getJSONfieldU(fileName, writerJSON, "queue-size");
                if (queueSize < 1 || queueSize > 1000000) {
                    CONFIG_FAIL("bad JSON, invalid \"queue-size\" value: " << dec << queueSize << ", expected one of: {1 .. 1000000}");
                }
            }

            if (strcmp(writerType, "file") == 0) {
                uint64_t maxSize = 0;
                if (writerJSON.HasMember("max-size"))
                    maxSize = getJSONfieldU(fileName, writerJSON, "max-size");

                const char* format = "%F_%T";
                if (writerJSON.HasMember("format"))
                    format = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "format");

                const char* output = "";
                if (writerJSON.HasMember("output"))
                    output = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "output");
                else
                if (maxSize > 0) {
                    RUNTIME_FAIL("parameter \"max-size\" should be 0 when \"output\" is not set (for: file writer)");
                }

                uint64_t newLine = 1;
                if (writerJSON.HasMember("new-line")) {
                    newLine = getJSONfieldU(fileName, writerJSON, "new-line");
                    if (newLine > 2) {
                        CONFIG_FAIL("bad JSON, invalid \"new-line\" value: " << dec << newLine << ", expected one of: {0, 1, 2}");
                    }
                }

                uint64_t append = 1;
                if (writerJSON.HasMember("append")) {
                    append = getJSONfieldU(fileName, writerJSON, "append");
                    if (append > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"append\" value: " << dec << append << ", expected one of: {0, 1}");
                    }
                }

                writer = new WriterFile(alias, oracleAnalyzer, output, format, maxSize, newLine, append, pollIntervalUS,
                        checkpointIntervalS, queueSize, startScn, startSequence, startTime, startTimeRel);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterFile) << " bytes memory (for: file writer)");
                }
            } else if (strcmp(writerType, "kafka") == 0) {
#ifdef LINK_LIBRARY_RDKAFKA
                uint64_t maxMessageMb = 100;
                if (writerJSON.HasMember("max-message-mb")) {
                    maxMessageMb = getJSONfieldU(fileName, writerJSON, "max-message-mb");
                    if (maxMessageMb < 1 || maxMessageMb > MAX_KAFKA_MESSAGE_MB) {
                        CONFIG_FAIL("bad JSON, invalid \"max-message-mb\" value: " << dec << maxMessageMb << ", expected one of: {1 .. " << MAX_KAFKA_MESSAGE_MB << "}");
                    }
                }

                uint64_t maxMessages = 100000;
                if (writerJSON.HasMember("max-messages")) {
                    maxMessages = getJSONfieldU(fileName, writerJSON, "max-messages");
                    if (maxMessages < 1 || maxMessages > MAX_KAFKA_MAX_MESSAGES) {
                        CONFIG_FAIL("bad JSON, invalid \"max-messages\" value: " << dec << maxMessages << ", expected one of: {1 .. " << MAX_KAFKA_MAX_MESSAGES << "}");
                    }
                }

                bool enableIdempotence = true;
                if (writerJSON.HasMember("enable-idempotence")) {
                    uint64_t enableIdempotenceInt = getJSONfieldU(fileName, writerJSON, "enable-idempotence");
                    if (enableIdempotenceInt == 1)
                        enableIdempotence = true;
                    else if (enableIdempotence > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"enable-idempotence\" value: " << dec << enableIdempotenceInt << ", expected one of: {0, 1}");
                    }
                }

                const char* brokers = getJSONfieldS(fileName, JSON_BROKERS_LENGTH, writerJSON, "brokers");
                const char* topic = getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "topic");

                writer = new WriterKafka(alias, oracleAnalyzer, brokers, topic, maxMessageMb, maxMessages, pollIntervalUS, checkpointIntervalS,
                        queueSize, startScn, startSequence, startTime, startTimeRel, enableIdempotence);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterKafka) << " bytes memory (for: Kafka writer)");
                }
#else
                RUNTIME_FAIL("writer Kafka is not compiled, exiting")
#endif /* LINK_LIBRARY_RDKAFKA */
            } else if (strcmp(writerType, "zeromq") == 0) {
#if defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ)
                const char* uri = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "uri");
                StreamZeroMQ* stream = new StreamZeroMQ(uri, pollIntervalUS);
                if (stream == nullptr) {
                    RUNTIME_FAIL("network stream creation failed");
                }

                writer = new WriterStream(alias, oracleAnalyzer, pollIntervalUS, checkpointIntervalS,
                        queueSize, startScn, startSequence, startTime, startTimeRel, stream);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterStream) << " bytes memory (for: ZeroMQ writer)");
                }
#else
                RUNTIME_FAIL("writer ZeroMQ is not compiled, exiting")
#endif /* defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ) */
            } else if (strcmp(writerType, "network") == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                const char* uri = getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "uri");

                StreamNetwork* stream = new StreamNetwork(uri, pollIntervalUS);
                if (stream == nullptr) {
                    RUNTIME_FAIL("network stream creation failed");
                }

                writer = new WriterStream(alias, oracleAnalyzer, pollIntervalUS, checkpointIntervalS,
                        queueSize, startScn, startSequence, startTime, startTimeRel, stream);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(WriterStream) << " bytes memory (for: ZeroMQ writer)");
                }
#else
                RUNTIME_FAIL("writer Network is not compiled, exiting")
#endif /* LINK_LIBRARY_PROTOBUF */
            } else {
                CONFIG_FAIL("bad JSON: invalid \"type\" value: " << writerType);
            }

            oracleAnalyzer->outputBuffer->setWriter(writer);
            if (pthread_create(&writer->pthread, nullptr, &Thread::runStatic, (void*)writer)) {
                RUNTIME_FAIL("spawning thread - kafka writer");
            }

            writers.push_back(writer);
            writer = nullptr;
        }

        //sleep until killed
        {
            unique_lock<mutex> lck(mainMtx);
            if (!mainShutdown)
                mainCV.wait(lck);
        }

    } catch (ConfigurationException& ex) {
    } catch (RuntimeException& ex) {
    }

    if (oracleAnalyzer != nullptr)
        analyzers.push_back(oracleAnalyzer);

    if (writer != nullptr)
        writers.push_back(writer);

    //shut down all analyzers
    for (OracleAnalyzer* analyzer : analyzers)
        analyzer->doShutdown();
    for (OracleAnalyzer* analyzer : analyzers)
        if (analyzer->started)
            pthread_join(analyzer->pthread, nullptr);

    //shut down writers
    for (Writer* writer : writers) {
        writer->doStop();
        if (writer->oracleAnalyzer->stopFlushBuffer == 0)
            writer->doShutdown();
    }
    for (OutputBuffer* outputBuffer : buffers) {
        unique_lock<mutex> lck(outputBuffer->mtx);
        outputBuffer->writersCond.notify_all();
    }
    for (Writer* writer : writers) {
        if (writer->started)
            pthread_join(writer->pthread, nullptr);
        delete writer;
    }
    writers.clear();

    for (OutputBuffer* outputBuffer : buffers)
        delete outputBuffer;
    buffers.clear();

    for (OracleAnalyzer* analyzer : analyzers)
        delete analyzer;
    analyzers.clear();

    if (fid != -1)
        close(fid);
    if (configFileBuffer != nullptr)
        delete[] configFileBuffer;

    TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") STOP");
    return 0;
}
