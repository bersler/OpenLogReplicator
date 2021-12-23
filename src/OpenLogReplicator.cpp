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
#include "StateDisk.h"
#include "WriterFile.h"

#ifdef LINK_LIBRARY_HIREDIS
#include "StateRedis.h"
#endif /* LINK_LIBRARY_HIREDIS */

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

#ifdef LINK_LIBRARY_RDKAFKA
#include "WriterKafka.h"
#endif /* LINK_LIBRARY_RDKAFKA */

#ifdef LINK_LIBRARY_ROCKETMQ
#include "WriterRocketMQ.h"
#endif /* LINK_LIBRARY_ROCKETMQ */

int main(int argc, char** argv) {
    OpenLogReplicator::mainThread = pthread_self();
    signal(SIGINT, OpenLogReplicator::signalHandler);
    signal(SIGPIPE, OpenLogReplicator::signalHandler);
    signal(SIGSEGV, OpenLogReplicator::signalCrash);
    signal(SIGUSR1, OpenLogReplicator::signalDump);
    OpenLogReplicator::uintX_t::initializeBASE10();

    INFO("OpenLogReplicator v." PACKAGE_VERSION " (C) 2018-2021 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information, linked modules:"
#ifdef LINK_LIBRARY_RDKAFKA
" Kafka"
#endif /* LINK_LIBRARY_RDKAFKA */
#ifdef LINK_LIBRARY_OCI
" OCI"
#endif /* LINK_LIBRARY_OCI */
#ifdef LINK_LIBRARY_PROTOBUF
" Probobuf"
#ifdef LINK_LIBRARY_ZEROMQ
" ZeroMQ"
#endif /* LINK_LIBRARY_ZEROMQ */
#endif /* LINK_LIBRARY_PROTOBUF */
#ifdef LINK_LIBRARY_HIREDIS
" Redis"
#endif /* LINK_LIBRARY_HIREDIS */
#ifdef LINK_LIBRARY_ROCKETMQ
" RocketMQ"
#endif /* LINK_LIBRARY_ROCKETMQ */
    );

    std::list<OpenLogReplicator::OracleAnalyzer*> analyzers;
    std::list<OpenLogReplicator::Writer*> writers;
    std::list<OpenLogReplicator::OutputBuffer*> buffers;
    OpenLogReplicator::OracleAnalyzer* oracleAnalyzer = nullptr;
    int fid = -1;
    char* configFileBuffer = nullptr;

    try {
        TRACE(TRACE2_THREADS, "THREADS: MAIN (" << std::hex << std::this_thread::get_id() << ") START");

        std::regex regexTest(".*");
        std::string regexString("check if matches!");
        bool regexWorks = regex_search(regexString, regexTest);
        if (!regexWorks) {
            CONFIG_FAIL("binaries are build with no regex implementation, check if you have gcc version >= 4.9");
        }

        std::string fileName("scripts/OpenLogReplicator.json");
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
        if (fileStat.st_size > CONFIG_FILE_MAX_SIZE || fileStat.st_size == 0) {
            CONFIG_FAIL("file " << fileName << " wrong size: " << std::dec << fileStat.st_size);
        }

        configFileBuffer = new char[fileStat.st_size + 1];
        if (configFileBuffer == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << (fileStat.st_size + 1) << " bytes memory (for: reading " << fileName << ")");
        }
        if (read(fid, configFileBuffer, fileStat.st_size) != fileStat.st_size) {
            CONFIG_FAIL("can't read file " << fileName);
        }
        configFileBuffer[fileStat.st_size] = 0;

        rapidjson::Document document;
        if (document.Parse(configFileBuffer).HasParseError()) {
            CONFIG_FAIL("parsing " << fileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()));
        }

        const char* version = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, document, "version");
        if (strcmp(version, CONFIG_SCHEMA_VERSION) != 0) {
            CONFIG_FAIL("bad JSON, incompatible \"version\" value: " << version << ", expected: " << CONFIG_SCHEMA_VERSION);
        }

        uint64_t dumpRedoLog = 0;
        if (document.HasMember("dump-redo-log")) {
            dumpRedoLog = OpenLogReplicator::getJSONfieldU64(fileName, document, "dump-redo-log");
            if (dumpRedoLog > 2) {
                CONFIG_FAIL("bad JSON, invalid \"dump-redo-log\" value: " << std::dec << dumpRedoLog << ", expected one of: {0, 1, 2}");
            }
        }

        const char* dumpPath = ".";
        if (document.HasMember("dump-path"))
            dumpPath = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, document, "dump-path");

        uint64_t dumpRawData = 0;
        if (document.HasMember("dump-raw-data")) {
            dumpRawData = OpenLogReplicator::getJSONfieldU64(fileName, document, "dump-raw-data");
            if (dumpRawData > 1) {
                CONFIG_FAIL("bad JSON, invalid \"dump-raw-data\" value: " << std::dec << dumpRawData << ", expected one of: {0, 1}");
            }
        }

        if (document.HasMember("trace")) {
            OpenLogReplicator::trace = OpenLogReplicator::getJSONfieldU64(fileName, document, "trace");
            if (OpenLogReplicator::trace > 4) {
                CONFIG_FAIL("bad JSON, invalid \"trace\" value: " << std::dec << OpenLogReplicator::trace << ", expected one of: {0, 1, 2, 3, 4}");
            }
        }

        if (document.HasMember("trace2")) {
            OpenLogReplicator::trace2 = OpenLogReplicator::getJSONfieldU64(fileName, document, "trace2");
            if (OpenLogReplicator::trace2 > 65535) {
                CONFIG_FAIL("bad JSON, invalid \"trace2\" value: " << std::dec << OpenLogReplicator::trace2 << ", expected one of: {0 .. 65535}");
            }
        }

        //iterate through sources
        const rapidjson::Value& sourceArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, document, "source");

        for (rapidjson::SizeType i = 0; i < sourceArrayJSON.Size(); ++i) {
            const rapidjson::Value& sourceJSON = OpenLogReplicator::getJSONfieldO(fileName, sourceArrayJSON, "source", i);

            const char* alias = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "alias");
            INFO("adding source: " << alias);

            uint64_t memoryMinMb = 32;
            if (sourceJSON.HasMember("memory-min-mb")) {
                memoryMinMb = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "memory-min-mb");
                memoryMinMb = (memoryMinMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMinMb < MEMORY_CHUNK_MIN_MB) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value must be at least " MEMORY_CHUNK_MIN_MB_CHR);
                }
            }

            uint64_t memoryMaxMb = 1024;
            if (sourceJSON.HasMember("memory-max-mb")) {
                memoryMaxMb = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "memory-max-mb");
                memoryMaxMb = (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMaxMb < memoryMinMb) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value can't be greater than \"memory-max-mb\" value");
                }
            }

            uint64_t readBufferMax = memoryMaxMb / 4 / MEMORY_CHUNK_SIZE_MB;
            if (readBufferMax > 32 / MEMORY_CHUNK_SIZE_MB)
                readBufferMax = 32 / MEMORY_CHUNK_SIZE_MB;

            if (sourceJSON.HasMember("read-buffer-max-mb")) {
                readBufferMax = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "read-buffer-max-mb") / MEMORY_CHUNK_SIZE_MB;
                if (readBufferMax * MEMORY_CHUNK_SIZE_MB > memoryMaxMb) {
                    CONFIG_FAIL("bad JSON, \"read-buffer-max-mb\" value can't be greater than \"memory-max-mb\" value");
                }
                if (readBufferMax <= 1) {
                    CONFIG_FAIL("bad JSON, \"read-buffer-max-mb\" value should be at least " << std::dec << MEMORY_CHUNK_SIZE_MB * 2);
                }
            }

            const char* name = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "name");

            //FORMAT
            const rapidjson::Value& formatJSON = OpenLogReplicator::getJSONfieldO(fileName, sourceJSON, "format");

            uint64_t messageFormat = MESSAGE_FORMAT_DEFAULT;
            if (formatJSON.HasMember("message")) {
                messageFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "message");
                if (messageFormat > 15) {
                    CONFIG_FAIL("bad JSON, invalid \"message\" value: " << std::dec << messageFormat << ", expected one of: {0.. 15}");
                }
                if ((messageFormat & MESSAGE_FORMAT_FULL) != 0 &&
                        (messageFormat & (MESSAGE_FORMAT_SKIP_BEGIN | MESSAGE_FORMAT_SKIP_COMMIT)) != 0) {
                    CONFIG_FAIL("bad JSON, invalid \"message\" value: " << std::dec << messageFormat <<
                            ", you are not allowed to use BEGIN/COMMIT flag (" << std::dec << MESSAGE_FORMAT_SKIP_BEGIN << "/" <<
                            MESSAGE_FORMAT_SKIP_COMMIT << ") together with FULL mode (" << std::dec << MESSAGE_FORMAT_FULL << ")");
                }
            }

            uint64_t ridFormat = RID_FORMAT_SKIP;
            if (formatJSON.HasMember("rid")) {
                ridFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "rid");
                if (ridFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"rid\" value: " << std::dec << ridFormat << ", expected one of: {0, 1}");
                }
            }

            uint64_t xidFormat = XID_FORMAT_TEXT;
            if (formatJSON.HasMember("xid")) {
                xidFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "xid");
                if (xidFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"xid\" value: " << std::dec << xidFormat << ", expected one of: {0, 1}");
                }
            }

            uint64_t timestampFormat = TIMESTAMP_FORMAT_UNIX;
            if (formatJSON.HasMember("timestamp")) {
                timestampFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "timestamp");
                if (timestampFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"timestamp\" value: " << std::dec << timestampFormat << ", expected one of: {0, 1, 2, 3}");
                }
            }

            uint64_t charFormat = CHAR_FORMAT_UTF8;
            if (formatJSON.HasMember("char")) {
                charFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "char");
                if (charFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"char\" value: " << std::dec << charFormat << ", expected one of: {0, 1, 2, 3}");
                }
            }

            uint64_t scnFormat = SCN_FORMAT_NUMERIC;
            if (formatJSON.HasMember("scn")) {
                scnFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "scn");
                if (scnFormat > 3) {
                    CONFIG_FAIL("bad JSON, invalid \"scn\" value: " << std::dec << scnFormat << ", expected one of: {0, 1, 2, 3}");
                }
            }

            uint64_t unknownFormat = UNKNOWN_FORMAT_QUESTION_MARK;
            if (formatJSON.HasMember("unknown")) {
                unknownFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "unknown");
                if (unknownFormat > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"unknown\" value: " << std::dec << unknownFormat << ", expected one of: {0, 1}");
                }
            }

            uint64_t schemaFormat = SCHEMA_FORMAT_NAME;
            if (formatJSON.HasMember("schema")) {
                schemaFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "schema");
                if (schemaFormat > 7) {
                    CONFIG_FAIL("bad JSON, invalid \"schema\" value: " << std::dec << schemaFormat << ", expected one of: {0 .. 7}");
                }
            }

            uint64_t columnFormat = COLUMN_FORMAT_CHANGED;
            if (formatJSON.HasMember("column")) {
                columnFormat = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "column");
                if (columnFormat > 2) {
                    CONFIG_FAIL("bad JSON, invalid \"column\" value: " << std::dec << columnFormat << ", expected one of: {0, 1, 2}");
                }
            }

            uint64_t unknownType = UNKNOWN_TYPE_HIDE;
            if (formatJSON.HasMember("unknown-type")) {
                unknownType = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "unknown-type");
                if (unknownType > 1) {
                    CONFIG_FAIL("bad JSON, invalid \"unknown-type\" value: " << std::dec << unknownType << ", expected one of: {0, 1}");
                }
            }

            uint64_t flushBuffer = 1048576;
            if (formatJSON.HasMember("flush-buffer"))
                flushBuffer = OpenLogReplicator::getJSONfieldU64(fileName, formatJSON, "flush-buffer");

            const char* formatType = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, formatJSON, "type");

            OpenLogReplicator::OutputBuffer* outputBuffer = nullptr;
            if (strcmp("json", formatType) == 0) {
                outputBuffer = new OpenLogReplicator::OutputBufferJson(messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                        schemaFormat, columnFormat, unknownType, flushBuffer);
            } else if (strcmp("protobuf", formatType) == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                outputBuffer = new OpenLogReplicator::OutputBufferProtobuf(messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                        schemaFormat, columnFormat, unknownType, flushBuffer);
#else
                RUNTIME_FAIL("format \"protobuf\" is not compiled, exiting");
#endif /* LINK_LIBRARY_PROTOBUF */
                } else {
                CONFIG_FAIL("bad JSON, invalid \"type\" value: " << formatType);
            }

            if (outputBuffer == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::OutputBuffer) << " bytes memory (for: command buffer)");
            }
            buffers.push_back(outputBuffer);


            //READER
            const rapidjson::Value& readerJSON = OpenLogReplicator::getJSONfieldO(fileName, sourceJSON, "reader");

            uint64_t disableChecks = 0;
            if (readerJSON.HasMember("disable-checks")) {
                disableChecks = OpenLogReplicator::getJSONfieldU64(fileName, readerJSON, "disable-checks");
                if (disableChecks > 7) {
                    CONFIG_FAIL("bad JSON, invalid \"disable-checks\" value: " << std::dec << disableChecks << ", expected one of: {0 .. 7}");
                }
            }

            const char* readerType = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, readerJSON, "type");

            if (strcmp(readerType, "online") == 0) {
#ifdef LINK_LIBRARY_OCI
                const char* user = OpenLogReplicator::getJSONfieldS(fileName, JSON_USERNAME_LENGTH, readerJSON, "user");
                const char* password = OpenLogReplicator::getJSONfieldS(fileName, JSON_PASSWORD_LENGTH, readerJSON, "password");
                const char* server = OpenLogReplicator::getJSONfieldS(fileName, JSON_SERVER_LENGTH, readerJSON, "server");

                oracleAnalyzer = new OpenLogReplicator::OracleAnalyzerOnline(outputBuffer, dumpRedoLog, dumpRawData, dumpPath, alias,
                        name, memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, user, password, server);
                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }
                oracleAnalyzer->initialize();

                if (readerJSON.HasMember("path-mapping")) {
                    const rapidjson::Value& pathMappingArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, readerJSON, "path-mapping");
                    if ((pathMappingArrayJSON.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (rapidjson::SizeType j = 0; j < pathMappingArrayJSON.Size() / 2; ++j) {
                        const char* sourceMapping = OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2);
                        const char* targetMapping = OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2 + 1);
                        oracleAnalyzer->addPathMapping(sourceMapping, targetMapping);
                    }
                }

                if (sourceJSON.HasMember("arch")) {
                    const char* arch = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "arch");

                    if (strcmp(arch, "path") == 0)
                        oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzer::archGetLogPath;
                    else if (strcmp(arch, "online") == 0) {
                        oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzerOnline::archGetLogOnline;
                    } else if (strcmp(arch, "online-keep") == 0) {
                        oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzerOnline::archGetLogOnline;
                        ((OpenLogReplicator::OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection = true;
                    } else {
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << arch << ", expected one of: {\"path\", \"online\", \"online-keep\"}");
                    }
                } else
                    oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzerOnline::archGetLogOnline;
#else
                RUNTIME_FAIL("reader type \"online\" is not compiled, exiting");
#endif /*LINK_LIBRARY_OCI*/

            } else if (strcmp(readerType, "offline") == 0) {

                oracleAnalyzer = new OpenLogReplicator::OracleAnalyzer(outputBuffer, dumpRedoLog, dumpRawData, dumpPath, alias,
                        name, memoryMinMb, memoryMaxMb, readBufferMax, disableChecks);
                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::OracleAnalyzer) << " bytes memory (for: oracle analyzer)");
                }
                oracleAnalyzer->initialize();

                if (readerJSON.HasMember("path-mapping")) {
                    const rapidjson::Value& pathMappingArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, readerJSON, "path-mapping");

                    if ((pathMappingArrayJSON.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (rapidjson::SizeType j = 0; j < pathMappingArrayJSON.Size() / 2; ++j) {
                        const char* sourceMapping = OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2);
                        const char* targetMapping = OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJSON, "path-mapping", j * 2 + 1);
                        oracleAnalyzer->addPathMapping(sourceMapping, targetMapping);
                    }
                }

            } else if (strcmp(readerType, "asm") == 0) {
#ifdef LINK_LIBRARY_OCI
                WARNING("experimental feature is used: read using ASM");

                const char* user = OpenLogReplicator::getJSONfieldS(fileName, JSON_USERNAME_LENGTH, readerJSON, "user");
                const char* password = OpenLogReplicator::getJSONfieldS(fileName, JSON_PASSWORD_LENGTH, readerJSON, "password");
                const char* server = OpenLogReplicator::getJSONfieldS(fileName, JSON_SERVER_LENGTH, readerJSON, "server");
                const char* userASM = OpenLogReplicator::getJSONfieldS(fileName, JSON_USERNAME_LENGTH, readerJSON, "user-asm");
                const char* passwordASM = OpenLogReplicator::getJSONfieldS(fileName, JSON_PASSWORD_LENGTH, readerJSON, "password-asm");
                const char* serverASM = OpenLogReplicator::getJSONfieldS(fileName, JSON_SERVER_LENGTH, readerJSON, "server-asm");

                oracleAnalyzer = new OpenLogReplicator::OracleAnalyzerOnlineASM(outputBuffer, dumpRedoLog, dumpRawData, dumpPath, alias, name,
                        memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, user, password, server, userASM, passwordASM,
                        serverASM);
                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::OracleAnalyzerOnlineASM) << " bytes memory (for: oracle analyzer)");
                }
                oracleAnalyzer->initialize();

                if (sourceJSON.HasMember("arch")) {
                    const char* arch = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, sourceJSON, "arch");

                    if (strcmp(arch, "path") == 0)
                        oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzer::archGetLogPath;
                    else if (strcmp(arch, "online") == 0) {
                        oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzerOnline::archGetLogOnline;
                        ((OpenLogReplicator::OracleAnalyzerOnline*)oracleAnalyzer)->keepConnection = false;
                    } else if (strcmp(arch, "online-keep") == 0)
                        oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzerOnline::archGetLogOnline;
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
                    conId = OpenLogReplicator::getJSONfieldI16(fileName, readerJSON, "con-id");

                oracleAnalyzer = new OpenLogReplicator::OracleAnalyzerBatch(outputBuffer, dumpRedoLog, dumpRawData, dumpPath, alias,
                        name, memoryMinMb, memoryMaxMb, readBufferMax, disableChecks, conId);
                if (oracleAnalyzer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::OracleAnalyzerBatch) << " bytes memory (for: oracle analyzer)");
                }
                oracleAnalyzer->initialize();
                oracleAnalyzer->flags |= REDO_FLAGS_ARCH_ONLY;

                const rapidjson::Value& redoLogBatchArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, readerJSON, "redo-log");

                for (rapidjson::SizeType j = 0; j < redoLogBatchArrayJSON.Size(); ++j)
                    oracleAnalyzer->addRedoLogsBatch(OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, redoLogBatchArrayJSON, "redo-log", j));

                oracleAnalyzer->archGetLog = OpenLogReplicator::OracleAnalyzer::archGetLogList;

            } else {
                CONFIG_FAIL("bad JSON, invalid \"format\" value: " << readerType);
            }

            outputBuffer->initialize(oracleAnalyzer);

            if (sourceJSON.HasMember("debug")) {
                const rapidjson::Value& debugJSON = OpenLogReplicator::getJSONfieldO(fileName, sourceJSON, "debug");

                if (debugJSON.HasMember("stop-log-switches")) {
                    oracleAnalyzer->stopLogSwitches = OpenLogReplicator::getJSONfieldU64(fileName, debugJSON, "stop-log-switches");
                    INFO("will shutdown after " << std::dec << oracleAnalyzer->stopLogSwitches << " log switches");
                }

                if (debugJSON.HasMember("stop-checkpoints")) {
                    oracleAnalyzer->stopCheckpoints = OpenLogReplicator::getJSONfieldU64(fileName, debugJSON, "stop-checkpoints");
                    INFO("will shutdown after " << std::dec << oracleAnalyzer->stopCheckpoints << " checkpoints");
                }

                if (debugJSON.HasMember("stop-transactions")) {
                    oracleAnalyzer->stopTransactions = OpenLogReplicator::getJSONfieldU64(fileName, debugJSON, "stop-transactions");
                    INFO("will shutdown after " << std::dec << oracleAnalyzer->stopTransactions << " transactions");
                }

                if (debugJSON.HasMember("flush-buffer")) {
                    uint64_t stopFlushBuffer = OpenLogReplicator::getJSONfieldU64(fileName, debugJSON, "flush-buffer");
                    if (stopFlushBuffer == 1) {
                        oracleAnalyzer->stopFlushBuffer = stopFlushBuffer;
                    } else
                    if (stopFlushBuffer > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"flush-buffer\" value: " << std::dec << stopFlushBuffer << ", expected one of: {0, 1}");
                    }
                }

                if (debugJSON.HasMember("owner") || debugJSON.HasMember("table")) {
                    const char* debugOwner = OpenLogReplicator::getJSONfieldS(fileName, SYSUSER_NAME_LENGTH, debugJSON, "owner");
                    const char* debugTable = OpenLogReplicator::getJSONfieldS(fileName, SYSOBJ_NAME_LENGTH, debugJSON, "table");

                    oracleAnalyzer->schema->addElement(debugOwner, debugTable, OPTIONS_DEBUG_TABLE);
                    INFO("will shutdown after committed DML in " << debugOwner << "." << debugTable);
                }
            }

            oracleAnalyzer->schema->addElement("SYS", "CCOL\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "CDEF\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "COL\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "DEFERRED_STG\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "ECOL\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "OBJ\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TAB\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TABPART\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TABCOMPART\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "TABSUBPART\\$", OPTIONS_SYSTEM_TABLE);
            oracleAnalyzer->schema->addElement("SYS", "USER\\$", OPTIONS_SYSTEM_TABLE);

            if (sourceJSON.HasMember("filter")) {
                const rapidjson::Value& filterJSON = OpenLogReplicator::getJSONfieldO(fileName, sourceJSON, "filter");

                if (filterJSON.HasMember("table")) {
                    const rapidjson::Value& tableArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, filterJSON, "table");

                    for (rapidjson::SizeType j = 0; j < tableArrayJSON.Size(); ++j) {
                        const rapidjson::Value& tableElementJSON = OpenLogReplicator::getJSONfieldO(fileName, tableArrayJSON, "table", j);

                        const char* owner = OpenLogReplicator::getJSONfieldS(fileName, SYSUSER_NAME_LENGTH, tableElementJSON, "owner");
                        const char* table = OpenLogReplicator::getJSONfieldS(fileName, SYSOBJ_NAME_LENGTH, tableElementJSON, "table");
                        OpenLogReplicator::SchemaElement* element = oracleAnalyzer->schema->addElement(owner, table, 0);

                        if (tableElementJSON.HasMember("key")) {
                            element->keysStr = OpenLogReplicator::getJSONfieldS(fileName, JSON_KEY_LENGTH, tableElementJSON, "key");
                            std::stringstream keyStream(element->keysStr);

                            while (keyStream.good()) {
                                std::string keyCol;
                                std::string keyCol2;
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
                    const rapidjson::Value& skipXidArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, filterJSON, "skip-xid");
                    for (rapidjson::SizeType j = 0; j < skipXidArrayJSON.Size(); ++j) {
                        const char* skipXid = OpenLogReplicator::getJSONfieldS(fileName, JSON_XID_LIST_LENGTH, skipXidArrayJSON, "skip-xid", j);

                        typeXID xid = 0;
                        bool invalid = false;
                        std::string usn;
                        std::string slt;
                        std::string sqn;

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
                    uint64_t transactionMaxMb = OpenLogReplicator::getJSONfieldU64(fileName, filterJSON, "transaction-max-mb");
                    if (transactionMaxMb > memoryMaxMb) {
                        CONFIG_FAIL("bad JSON, \"transaction-max-mb\" (" << std::dec << transactionMaxMb <<
                                ") is bigger than \"memory-max-mb\" (" << memoryMaxMb << ")");
                    }
                    oracleAnalyzer->transactionMax = transactionMaxMb * 1024 * 1024;
                }
            }

            if (sourceJSON.HasMember("flags")) {
                uint64_t flags = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "flags");
                if (flags > 16383) {
                    CONFIG_FAIL("bad JSON, invalid \"flags\" value: " << std::dec << flags << ", expected one of: {0 .. 16383}");
                }
                if ((flags & REDO_FLAGS_SCHEMALESS) != 0 && columnFormat > 0) {
                    CONFIG_FAIL("bad JSON, invalid \"column\" value: " << std::dec << columnFormat << " is invalid for schemaless mode");
                }
                oracleAnalyzer->flags |= flags;
            }

            if (sourceJSON.HasMember("redo-verify-delay-us"))
                oracleAnalyzer->redoVerifyDelayUs = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "redo-verify-delay-us");

            if (sourceJSON.HasMember("refresh-interval-us"))
                oracleAnalyzer->refreshIntervalUs = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "refresh-interval-us");

            if (sourceJSON.HasMember("arch-read-sleep-us"))
                oracleAnalyzer->archReadSleepUs = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "arch-read-sleep-us");

            if (sourceJSON.HasMember("arch-read-tries")) {
                oracleAnalyzer->archReadTries = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "arch-read-tries");
                if (oracleAnalyzer->archReadTries < 1 || oracleAnalyzer->archReadTries > 1000000000) {
                    CONFIG_FAIL("bad JSON, invalid \"arch-read-tries\" value: " << std::dec << oracleAnalyzer->archReadTries << ", expected one of: {1, 1000000000}");
                }
            }

            if (sourceJSON.HasMember("redo-read-sleep-us"))
                oracleAnalyzer->redoReadSleepUs = OpenLogReplicator::getJSONfieldU64(fileName, sourceJSON, "redo-read-sleep-us");

            if (readerJSON.HasMember("redo-copy-path"))
                oracleAnalyzer->redoCopyPath = OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, readerJSON, "redo-copy-path");

            if (readerJSON.HasMember("log-archive-format"))
                oracleAnalyzer->logArchiveFormat = OpenLogReplicator::getJSONfieldS(fileName, VPARAMETER_LENGTH, readerJSON, "log-archive-format");

            uint64_t stateType = STATE_TYPE_DISK;
            const char* statePath = "checkpoint";
            const char* stateServer = "localhost";
            uint16_t statePort = 6379;

            if (sourceJSON.HasMember("state")) {
                const rapidjson::Value& stateJSON = OpenLogReplicator::getJSONfieldO(fileName, sourceJSON, "state");

                if (stateJSON.HasMember("type")) {
                    const char* stateTypeStr = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, stateJSON, "type");
                    if (strcmp(stateTypeStr, "disk") == 0) {
                        stateType = STATE_TYPE_DISK;
                        if (stateJSON.HasMember("path"))
                            statePath = OpenLogReplicator::getJSONfieldS(fileName, MAX_PATH_LENGTH, stateJSON, "path");
                    } else if (strcmp(stateTypeStr, "redis") == 0) {
                        WARNING("experimental feature is used: writing state in Redis (not yet implemented)");
                        stateType = STATE_TYPE_REDIS;
                        if (stateJSON.HasMember("server"))
                            stateServer = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, stateJSON, "server");
                        if (stateJSON.HasMember("port"))
                            statePort = OpenLogReplicator::getJSONfieldU16(fileName, stateJSON, "port");
                    } else {
                        CONFIG_FAIL("bad JSON, invalid \"type\" value: " << std::dec << stateTypeStr << ", expected one of: {\"disk\", \"redis\"}");
                    }
                }

                if (stateJSON.HasMember("interval-s"))
                    oracleAnalyzer->checkpointIntervalS = OpenLogReplicator::getJSONfieldU64(fileName, stateJSON, "interval-s");

                if (stateJSON.HasMember("interval-mb"))
                    oracleAnalyzer->checkpointIntervalMB = OpenLogReplicator::getJSONfieldU64(fileName, stateJSON, "interval-mb");

                if (stateJSON.HasMember("all-checkpoints")) {
                    uint64_t allCheckpoints = OpenLogReplicator::getJSONfieldU64(fileName, stateJSON, "all-checkpoints");
                    if (allCheckpoints <= 1)
                        oracleAnalyzer->checkpointAll = allCheckpoints;
                    else if (allCheckpoints > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"all-checkpoints\" value: " << std::dec << allCheckpoints << ", expected one of: {0, 1}");
                    }
                }

                if (stateJSON.HasMember("output-checkpoint")) {
                    uint64_t outputCheckpoint = OpenLogReplicator::getJSONfieldU64(fileName, stateJSON, "output-checkpoint");
                    if (outputCheckpoint <= 1)
                        oracleAnalyzer->checkpointOutputCheckpoint = outputCheckpoint;
                    else if (outputCheckpoint > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"output-checkpoint\" value: " << std::dec << outputCheckpoint << ", expected one of: {0, 1}");
                    }
                }

                if (stateJSON.HasMember("output-log-switch")) {
                    uint64_t outputLogSwitch = OpenLogReplicator::getJSONfieldU64(fileName, stateJSON, "output-log-switch");
                    if (outputLogSwitch <= 1)
                        oracleAnalyzer->checkpointOutputLogSwitch = outputLogSwitch;
                    else if (outputLogSwitch > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"output-log-switch\" value: " << std::dec << outputLogSwitch << ", expected one of: {0, 1}");
                    }
                }
            }

            if (stateType == STATE_TYPE_DISK) {
                oracleAnalyzer->state = new OpenLogReplicator::StateDisk(statePath);
            } else
            if (stateType == STATE_TYPE_REDIS) {
#ifdef LINK_LIBRARY_HIREDIS
                oracleAnalyzer->state = new OpenLogReplicator::StateRedis(stateServer, statePort);
#else
            RUNTIME_FAIL("format \"redis\" is not compiled, exiting");
#endif /* LINK_LIBRARY_HIREDIS */
            }

            if (pthread_create(&oracleAnalyzer->pthread, nullptr, &OpenLogReplicator::Thread::runStatic, (void*)oracleAnalyzer)) {
                RUNTIME_FAIL("spawning thread - oracle analyzer");
            }

            analyzers.push_back(oracleAnalyzer);
            oracleAnalyzer = nullptr;
        }

        //iterate through targets
        const rapidjson::Value& targetArrayJSON = OpenLogReplicator::getJSONfieldA(fileName, document, "target");

        for (rapidjson::SizeType i = 0; i < targetArrayJSON.Size(); ++i) {
            const rapidjson::Value& targetJSON = targetArrayJSON[i];
            const char* alias = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, targetJSON, "alias");
            const char* source = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, targetJSON, "source");

            INFO("adding target: " << alias);
            OpenLogReplicator::OracleAnalyzer* oracleAnalyzer = nullptr;
            for (OpenLogReplicator::OracleAnalyzer* analyzer : analyzers)
                if (analyzer->alias.compare(source) == 0)
                    oracleAnalyzer = (OpenLogReplicator::OracleAnalyzer*)analyzer;
            if (oracleAnalyzer == nullptr) {
                CONFIG_FAIL("bad JSON, couldn't find reader for \"source\" value: " << source);
            }

            //writer
            OpenLogReplicator::Writer* writer = nullptr;
            const rapidjson::Value& writerJSON = OpenLogReplicator::getJSONfieldO(fileName, targetJSON, "writer");
            const char* writerType = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "type");

            uint64_t pollIntervalUs = 100000;
            if (writerJSON.HasMember("poll-interval-us")) {
                pollIntervalUs = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "poll-interval-us");
                if (pollIntervalUs < 100 || pollIntervalUs > 3600000000) {
                    CONFIG_FAIL("bad JSON, invalid \"poll-interval-us\" value: " << std::dec << pollIntervalUs << ", expected one of: {100 .. 3600000000}");
                }
            }

            typeSCN startScn = ZERO_SCN;
            if (writerJSON.HasMember("start-scn"))
                startScn = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "start-scn");

            typeSEQ startSequence = ZERO_SEQ;
            if (writerJSON.HasMember("start-seq"))
                startSequence = OpenLogReplicator::getJSONfieldU32(fileName, writerJSON, "start-seq");

            int64_t startTimeRel = 0;
            if (writerJSON.HasMember("start-time-rel")) {
                if (startScn != ZERO_SCN) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-time-rel\"");
                }
                startTimeRel = OpenLogReplicator::getJSONfieldI64(fileName, writerJSON, "start-time-rel");
            }

            const char* startTime = "";
            if (writerJSON.HasMember("start-time")) {
                if (startScn != ZERO_SCN) {
                    CONFIG_FAIL("bad JSON, \"start-scn\" used together with \"start-time\"");
                }
                if (startTimeRel > 0) {
                    CONFIG_FAIL("bad JSON, \"start-time-rel\" used together with \"start-time\"");
                }

                startTime = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "start-time");
            }

            uint64_t checkpointIntervalS = 10;
            if (writerJSON.HasMember("checkpoint-interval-s"))
                checkpointIntervalS = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "checkpoint-interval-s");

            uint64_t queueSize = 65536;
            if (writerJSON.HasMember("queue-size")) {
                queueSize = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "queue-size");
                if (queueSize < 1 || queueSize > 1000000) {
                    CONFIG_FAIL("bad JSON, invalid \"queue-size\" value: " << std::dec << queueSize << ", expected one of: {1 .. 1000000}");
                }
            }

            if (strcmp(writerType, "file") == 0) {
                uint64_t maxSize = 0;
                if (writerJSON.HasMember("max-size"))
                    maxSize = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "max-size");

                const char* format = "%F_%T";
                if (writerJSON.HasMember("format"))
                    format = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "format");

                const char* output = "";
                if (writerJSON.HasMember("output"))
                    output = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "output");
                else
                if (maxSize > 0) {
                    RUNTIME_FAIL("parameter \"max-size\" should be 0 when \"output\" is not set (for: file writer)");
                }

                uint64_t newLine = 1;
                if (writerJSON.HasMember("new-line")) {
                    newLine = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "new-line");
                    if (newLine > 2) {
                        CONFIG_FAIL("bad JSON, invalid \"new-line\" value: " << std::dec << newLine << ", expected one of: {0, 1, 2}");
                    }
                }

                uint64_t append = 1;
                if (writerJSON.HasMember("append")) {
                    append = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "append");
                    if (append > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"append\" value: " << std::dec << append << ", expected one of: {0, 1}");
                    }
                }

                writer = new OpenLogReplicator::WriterFile(alias, oracleAnalyzer, pollIntervalUs, checkpointIntervalS, queueSize, startScn,
                        startSequence, startTime, startTimeRel, output, format, maxSize, newLine, append);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::WriterFile) << " bytes memory (for: file writer)");
                }
            } else if (strcmp(writerType, "kafka") == 0) {
#ifdef LINK_LIBRARY_RDKAFKA
                uint64_t maxMessageMb = 100;
                if (writerJSON.HasMember("max-message-mb")) {
                    maxMessageMb = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "max-message-mb");
                    if (maxMessageMb < 1 || maxMessageMb > MAX_KAFKA_MESSAGE_MB) {
                        CONFIG_FAIL("bad JSON, invalid \"max-message-mb\" value: " << std::dec << maxMessageMb << ", expected one of: {1 .. " << MAX_KAFKA_MESSAGE_MB << "}");
                    }
                }

                uint64_t maxMessages = 100000;
                if (writerJSON.HasMember("max-messages")) {
                    maxMessages = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "max-messages");
                    if (maxMessages < 1 || maxMessages > MAX_KAFKA_MAX_MESSAGES) {
                        CONFIG_FAIL("bad JSON, invalid \"max-messages\" value: " << std::dec << maxMessages << ", expected one of: {1 .. " << MAX_KAFKA_MAX_MESSAGES << "}");
                    }
                }

                bool enableIdempotence = true;
                if (writerJSON.HasMember("enable-idempotence")) {
                    uint64_t enableIdempotenceInt = OpenLogReplicator::getJSONfieldU64(fileName, writerJSON, "enable-idempotence");
                    if (enableIdempotenceInt == 1)
                        enableIdempotence = true;
                    else if (enableIdempotence > 1) {
                        CONFIG_FAIL("bad JSON, invalid \"enable-idempotence\" value: " << std::dec << enableIdempotenceInt << ", expected one of: {0, 1}");
                    }
                }

                const char* brokers = OpenLogReplicator::getJSONfieldS(fileName, JSON_BROKERS_LENGTH, writerJSON, "brokers");

                const char* topic = OpenLogReplicator::getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "topic");

                writer = new OpenLogReplicator::WriterKafka(alias, oracleAnalyzer, pollIntervalUs, checkpointIntervalS, queueSize, startScn,
                        startSequence, startTime, startTimeRel, brokers, topic, maxMessages, maxMessageMb, enableIdempotence);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::WriterKafka) << " bytes memory (for: Kafka writer)");
                }
#else
                RUNTIME_FAIL("writer Kafka is not compiled, exiting")
#endif /* LINK_LIBRARY_RDKAFKA */
            } else if (strcmp(writerType, "rocketmq") == 0) {
#ifdef LINK_LIBRARY_ROCKETMQ
                WARNING("experimental feature is used: RocketMQ output");
                const char *groupId = "OpenLogReplicator";
                if (writerJSON.HasMember("group-id"))
                    groupId = OpenLogReplicator::getJSONfieldS(fileName, JSON_BROKERS_LENGTH, writerJSON, "group-id");

                const char* address = nullptr;
                if (writerJSON.HasMember("address"))
                    address = OpenLogReplicator::getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "address");

                const char* domain = nullptr;
                if (writerJSON.HasMember("domain"))
                    domain = OpenLogReplicator::getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "domain");

                const char* topic = OpenLogReplicator::getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "topic");

                if (address == nullptr && domain == nullptr) {
                    CONFIG_FAIL("bad JSON, for RocketMQ writer, either \"address\" or \"domain\" is required");
                }
                if (address != nullptr && domain != nullptr) {
                    CONFIG_FAIL("bad JSON, for RocketMQ writer, both \"address\" and \"domain\" have been used");
                }

                const char* tags = "";
                if (writerJSON.HasMember("tags"))
                    domain = OpenLogReplicator::getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "tags");

                const char* keys = "";
                if (writerJSON.HasMember("keys"))
                    domain = OpenLogReplicator::getJSONfieldS(fileName, JSON_TOPIC_LENGTH, writerJSON, "keys");

                writer = new OpenLogReplicator::WriterRocketMQ(alias, oracleAnalyzer, pollIntervalUs, checkpointIntervalS, queueSize, startScn,
                        startSequence, startTime, startTimeRel, groupId, address, domain, topic, tags, keys);
                if (writer == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::WriterRocketMQ) << " bytes memory (for: RocketMQ writer)");
                }
#else
                RUNTIME_FAIL("writer RocketMQ is not compiled, exiting")
#endif /* LINK_LIBRARY_RDKAFKA */
            } else if (strcmp(writerType, "zeromq") == 0) {
#if defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ)
                const char* uri = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "uri");
                OpenLogReplicator::StreamZeroMQ* stream = new OpenLogReplicator::StreamZeroMQ(uri, pollIntervalUs);
                if (stream == nullptr) {
                    RUNTIME_FAIL("network stream creation failed");
                }

                stream->initialize();
                writer = new OpenLogReplicator::WriterStream(alias, oracleAnalyzer, pollIntervalUs, checkpointIntervalS,
                        queueSize, startScn, startSequence, startTime, startTimeRel, stream);
                if (writer == nullptr) {
                    if (stream != nullptr) {
                        delete stream;
                        stream = nullptr;
                    }
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::WriterStream) << " bytes memory (for: ZeroMQ writer)");
                }
#else
                RUNTIME_FAIL("writer ZeroMQ is not compiled, exiting")
#endif /* defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ) */
            } else if (strcmp(writerType, "network") == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                const char* uri = OpenLogReplicator::getJSONfieldS(fileName, JSON_PARAMETER_LENGTH, writerJSON, "uri");

                OpenLogReplicator::StreamNetwork* stream = new OpenLogReplicator::StreamNetwork(uri, pollIntervalUs);
                if (stream == nullptr) {
                    RUNTIME_FAIL("network stream creation failed");
                }

                stream->initialize();
                writer = new OpenLogReplicator::WriterStream(alias, oracleAnalyzer, pollIntervalUs, checkpointIntervalS, queueSize, startScn,
                        startSequence, startTime, startTimeRel, stream);
                if (writer == nullptr) {
                    if (stream != nullptr) {
                        delete stream;
                        stream = nullptr;
                    }
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OpenLogReplicator::WriterStream) << " bytes memory (for: ZeroMQ writer)");
                }
#else
                RUNTIME_FAIL("writer Network is not compiled, exiting")
#endif /* LINK_LIBRARY_PROTOBUF */
            } else {
                CONFIG_FAIL("bad JSON: invalid \"type\" value: " << writerType);
            }

            writers.push_back(writer);
            writer->initialize();

            oracleAnalyzer->outputBuffer->setWriter(writer);
            if (pthread_create(&writer->pthread, nullptr, &OpenLogReplicator::Thread::runStatic, (void*)writer)) {
                RUNTIME_FAIL("spawning thread - network writer");
            }
        }

        //sleep until killed
        {
            std::unique_lock<std::mutex> lck(OpenLogReplicator::mainMtx);
            if (!OpenLogReplicator::mainShutdown)
                OpenLogReplicator::mainCV.wait(lck);
        }

    } catch (OpenLogReplicator::ConfigurationException& ex) {
    } catch (OpenLogReplicator::RuntimeException& ex) {
    }

    if (oracleAnalyzer != nullptr)
        analyzers.push_back(oracleAnalyzer);

    //shut down all analyzers
    for (OpenLogReplicator::OracleAnalyzer* analyzer : analyzers)
        analyzer->doShutdown();
    for (OpenLogReplicator::OracleAnalyzer* analyzer : analyzers)
        if (analyzer->started)
            pthread_join(analyzer->pthread, nullptr);

    //shut down writers
    for (OpenLogReplicator::Writer* writer : writers) {
        writer->doStop();
        if (writer->oracleAnalyzer->stopFlushBuffer == 0)
            writer->doShutdown();
    }
    for (OpenLogReplicator::OutputBuffer* outputBuffer : buffers) {
        std::unique_lock<std::mutex> lck(outputBuffer->mtx);
        outputBuffer->writersCond.notify_all();
    }
    for (OpenLogReplicator::Writer* writer : writers) {
        if (writer->started)
            pthread_join(writer->pthread, nullptr);
        delete writer;
    }
    writers.clear();

    for (OpenLogReplicator::OutputBuffer* outputBuffer : buffers)
        delete outputBuffer;
    buffers.clear();

    for (OpenLogReplicator::OracleAnalyzer* analyzer : analyzers)
        delete analyzer;
    analyzers.clear();

    if (fid != -1)
        close(fid);
    if (configFileBuffer != nullptr)
        delete[] configFileBuffer;

    TRACE(TRACE2_THREADS, "THREADS: MAIN (" << std::hex << std::this_thread::get_id() << ") STOP");
    return 0;
}
