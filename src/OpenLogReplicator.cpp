/* Create main thread instances
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <cerrno>
#include <fcntl.h>
#include <regex>
#include <sys/file.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "builder/BuilderJson.h"
#include "common/types.h"
#include "common/ConfigurationException.h"
#include "common/RuntimeException.h"
#include "common/SysObj.h"
#include "common/SysUser.h"
#include "common/Thread.h"
#include "locales/Locales.h"
#include "metadata/Checkpoint.h"
#include "metadata/Metadata.h"
#include "metadata/SchemaElement.h"
#include "parser/TransactionBuffer.h"
#include "replicator/Replicator.h"
#include "replicator/ReplicatorBatch.h"
#include "state/StateDisk.h"
#include "writer/WriterFile.h"
#include "OpenLogReplicator.h"

#ifdef LINK_LIBRARY_OCI
#include "replicator/ReplicatorOnline.h"
#define HAS_OCI " OCI"
#else
#define HAS_OCI ""
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#include "builder/BuilderProtobuf.h"
#include "stream/StreamNetwork.h"
#include "writer/WriterStream.h"
#define HAS_PROTOBUF " Probobuf"
#ifdef LINK_LIBRARY_ZEROMQ
#include "stream/StreamZeroMQ.h"
#define HAS_ZEROMQ " ZeroMQ"
#else
#define HAS_ZEROMQ ""
#endif /* LINK_LIBRARY_ZEROMQ */
#else
#define HAS_PROTOBUF ""
#define HAS_ZEROMQ ""
#endif /* LINK_LIBRARY_PROTOBUF */

#ifdef LINK_LIBRARY_RDKAFKA
#include "writer/WriterKafka.h"
#else
#endif /* LINK_LIBRARY_RDKAFKA */


namespace OpenLogReplicator {

    OpenLogReplicator::OpenLogReplicator(std::string fileName, Ctx* ctx) :
            replicator(nullptr),
            fid(-1),
            configFileBuffer(nullptr),
            fileName(fileName),
            ctx(ctx) {
        typeINTX::initializeBASE10();
    }

    OpenLogReplicator::~OpenLogReplicator() {
        if (replicator != nullptr)
            replicators.push_back(replicator);

        ctx->stopSoft();
        ctx->mainFinish();

        for (Writer* writer : writers)
            delete writer;
        writers.clear();

        for (Replicator* replicatorTmp : replicators)
            delete replicatorTmp;
        replicators.clear();

        for (Builder* builder : builders)
            delete builder;
        builders.clear();

        for (Checkpoint* checkpoint : checkpoints)
            delete checkpoint;
        checkpoints.clear();

        for (TransactionBuffer* transactionBuffer : transactionBuffers)
            delete transactionBuffer;
        transactionBuffers.clear();

        for (Metadata* metadata : metadatas)
            delete metadata;
        metadatas.clear();

        for (Locales* locales : localess)
            delete locales;
        localess.clear();

        if (fid != -1)
            close(fid);
        if (configFileBuffer != nullptr)
            delete[] configFileBuffer;
    }

    int OpenLogReplicator::run() {
        auto locales = new Locales();
        localess.push_back(locales);
        locales->initialize();

        TRACE(TRACE2_THREADS, "THREADS: MAIN (" << std::hex << std::this_thread::get_id() << ") START")

        struct stat fileStat;
        fid = open(fileName.c_str(), O_RDONLY);
        if (fid == -1)
            throw ConfigurationException("opening in read mode file: " + fileName + " - " + strerror(errno));

        if (flock(fid, LOCK_EX | LOCK_NB))
            throw ConfigurationException("locking file: " + fileName + ", another process may be running - " + strerror(errno));

        int ret = stat(fileName.c_str(), &fileStat);
        if (ret != 0)
            throw ConfigurationException("reading information for file: " + fileName + " - " + strerror(errno));
        if (fileStat.st_size > CONFIG_FILE_MAX_SIZE || fileStat.st_size == 0)
            throw ConfigurationException("file " + fileName + " wrong size: " + std::to_string(fileStat.st_size));

        configFileBuffer = new char[fileStat.st_size + 1];
        if (read(fid, configFileBuffer, fileStat.st_size) != fileStat.st_size)
            throw ConfigurationException("can't read file " + fileName);
        configFileBuffer[fileStat.st_size] = 0;

        rapidjson::Document document;
        if (document.Parse(configFileBuffer).HasParseError())
            throw ConfigurationException("parsing " + fileName + " at offset: " + std::to_string(document.GetErrorOffset()) +
                                         ", message: " + GetParseError_En(document.GetParseError()));

        const char* version = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, document, "version");
        if (strcmp(version, CONFIG_SCHEMA_VERSION) != 0)
            throw ConfigurationException(std::string("bad JSON, incompatible 'version' value: ") + version + ", expected: " + CONFIG_SCHEMA_VERSION);

        if (document.HasMember("dump-redo-log")) {
            ctx->dumpRedoLog = Ctx::getJsonFieldU64(fileName, document, "dump-redo-log");
            if (ctx->dumpRedoLog > 2)
                throw ConfigurationException("bad JSON, invalid 'dump-redo-log' value: " + std::to_string(ctx->dumpRedoLog) +
                                             ", expected one of: {0, 1, 2}");
        }

        if (document.HasMember("dump-path"))
            ctx->dumpPath = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, document, "dump-path");

        if (document.HasMember("dump-raw-data")) {
            ctx->dumpRawData = Ctx::getJsonFieldU64(fileName, document, "dump-raw-data");
            if (ctx->dumpRawData > 1)
                throw ConfigurationException("bad JSON, invalid 'dump-raw-data' value: " + std::to_string(ctx->dumpRawData) +
                                             ", expected one of: {0, 1}");
        }

        if (document.HasMember("trace")) {
            ctx->trace = Ctx::getJsonFieldU64(fileName, document, "trace");
            if (ctx->trace > 4)
                throw ConfigurationException("bad JSON, invalid 'trace' value: " + std::to_string(ctx->trace) +
                                             ", expected one of: {0, 1, 2, 3, 4}");
        }

        if (document.HasMember("trace2")) {
            ctx->trace2 = Ctx::getJsonFieldU64(fileName, document, "trace2");
            if (ctx->trace2 > 16383)
                throw ConfigurationException("bad JSON, invalid 'trace2' value: " + std::to_string(ctx->trace2) +
                                             ", expected one of: {0 .. 65535}");
        }

        // Iterate through sources
        const rapidjson::Value& sourceArrayJson = Ctx::getJsonFieldA(fileName, document, "source");
        if (sourceArrayJson.Size() > 1) {
            throw ConfigurationException("bad JSON, only one 'source' element is allowed");
        }

        for (rapidjson::SizeType j = 0; j < sourceArrayJson.Size(); ++j) {
            const rapidjson::Value& sourceJson = Ctx::getJsonFieldO(fileName, sourceArrayJson, "source", j);

            const char* alias = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, sourceJson, "alias");
            INFO("adding source: " << alias)

            uint64_t memoryMinMb = 32;
            if (sourceJson.HasMember("memory-min-mb")) {
                memoryMinMb = Ctx::getJsonFieldU64(fileName, sourceJson, "memory-min-mb");
                memoryMinMb = (memoryMinMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMinMb < MEMORY_CHUNK_MIN_MB)
                    throw ConfigurationException("bad JSON, 'memory-min-mb' value must be at least " + std::to_string(MEMORY_CHUNK_MIN_MB));
            }

            uint64_t memoryMaxMb = 1024;
            if (sourceJson.HasMember("memory-max-mb")) {
                memoryMaxMb = Ctx::getJsonFieldU64(fileName, sourceJson, "memory-max-mb");
                memoryMaxMb = (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMaxMb < memoryMinMb)
                    throw ConfigurationException("bad JSON, 'memory-min-mb' value can't be greater than 'memory-max-mb' value");
            }

            uint64_t readBufferMax = memoryMaxMb / 4 / MEMORY_CHUNK_SIZE_MB;
            if (readBufferMax > 32 / MEMORY_CHUNK_SIZE_MB)
                readBufferMax = 32 / MEMORY_CHUNK_SIZE_MB;

            if (sourceJson.HasMember("read-buffer-max-mb")) {
                readBufferMax = Ctx::getJsonFieldU64(fileName, sourceJson, "read-buffer-max-mb") / MEMORY_CHUNK_SIZE_MB;
                if (readBufferMax * MEMORY_CHUNK_SIZE_MB > memoryMaxMb)
                    throw ConfigurationException("bad JSON, 'read-buffer-max-mb' value can't be greater than 'memory-max-mb' value");
                if (readBufferMax <= 1)
                    throw ConfigurationException("bad JSON, 'read-buffer-max-mb' value should be at least " + std::to_string(MEMORY_CHUNK_SIZE_MB * 2));
            }

            const char* name = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, sourceJson, "name");
            const rapidjson::Value& readerJson = Ctx::getJsonFieldO(fileName, sourceJson, "reader");

            if (sourceJson.HasMember("flags")) {
                ctx->flags = Ctx::getJsonFieldU64(fileName, sourceJson, "flags");
                if (ctx->flags > 32767)
                    throw ConfigurationException("bad JSON, invalid 'flags' value: " + std::to_string(ctx->flags) +
                                                 ", expected one of: {0 .. 16383}");
                if (FLAG(REDO_FLAGS_DIRECT_DISABLE))
                    ctx->redoVerifyDelayUs = 500000;
            }

            if (readerJson.HasMember("disable-checks")) {
                ctx->disableChecks = Ctx::getJsonFieldU64(fileName, readerJson, "disable-checks");
                if (ctx->disableChecks > 7)
                    throw ConfigurationException("bad JSON, invalid 'disable-checks' value: " + std::to_string(ctx->disableChecks) +
                                                 ", expected one of: {0 .. 7}");
            }

            typeScn startScn = ZERO_SCN;
            if (readerJson.HasMember("start-scn"))
                startScn = Ctx::getJsonFieldU64(fileName, readerJson, "start-scn");

            typeSeq startSequence = ZERO_SEQ;
            if (readerJson.HasMember("start-seq"))
                startSequence = Ctx::getJsonFieldU32(fileName, readerJson, "start-seq");

            int64_t startTimeRel = 0;
            if (readerJson.HasMember("start-time-rel")) {
                if (startScn != ZERO_SCN)
                    throw ConfigurationException("bad JSON, 'start-scn' used together with 'start-time-rel'");
                startTimeRel = Ctx::getJsonFieldI64(fileName, readerJson, "start-time-rel");
            }

            const char* startTime = "";
            if (readerJson.HasMember("start-time")) {
                if (startScn != ZERO_SCN)
                    throw ConfigurationException("bad JSON, 'start-scn' used together with 'start-time'");
                if (startTimeRel > 0)
                    throw ConfigurationException("bad JSON, 'start-time-rel' used together with 'start-time'");

                startTime = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, readerJson, "start-time");
            }

            uint64_t stateType = STATE_TYPE_DISK;
            const char* statePath = "checkpoint";

            if (sourceJson.HasMember("state")) {
                const rapidjson::Value& stateJson = Ctx::getJsonFieldO(fileName, sourceJson, "state");

                if (stateJson.HasMember("type")) {
                    const char* stateTypeStr = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, stateJson, "type");
                    if (strcmp(stateTypeStr, "disk") == 0) {
                        stateType = STATE_TYPE_DISK;
                        if (stateJson.HasMember("path"))
                            statePath = Ctx::getJsonFieldS(fileName, MAX_PATH_LENGTH, stateJson, "path");
                    } else
                        throw ConfigurationException(std::string("bad JSON, invalid 'type' value: ") + stateTypeStr +
                                                     ", expected one of: {'disk'}");
                }

                if (stateJson.HasMember("interval-s"))
                    ctx->checkpointIntervalS = Ctx::getJsonFieldU64(fileName, stateJson, "interval-s");

                if (stateJson.HasMember("interval-mb"))
                    ctx->checkpointIntervalMb = Ctx::getJsonFieldU64(fileName, stateJson, "interval-mb");

                if (stateJson.HasMember("keep-checkpoints"))
                    ctx->checkpointKeep = Ctx::getJsonFieldU64(fileName, stateJson, "keep-checkpoints");

                if (stateJson.HasMember("schema-force-interval"))
                    ctx->schemaForceInterval = Ctx::getJsonFieldU64(fileName, stateJson, "schema-force-interval");
            }

            const char* debugOwner = nullptr;
            const char* debugTable = nullptr;

            if (sourceJson.HasMember("debug") && !FLAG(REDO_FLAGS_SCHEMALESS)) {
                const rapidjson::Value& debugJson = Ctx::getJsonFieldO(fileName, sourceJson, "debug");

                if (debugJson.HasMember("stop-log-switches")) {
                    ctx->stopLogSwitches = Ctx::getJsonFieldU64(fileName, debugJson, "stop-log-switches");
                    INFO("will shutdown after " + std::to_string(ctx->stopLogSwitches) + " log switches");
                }

                if (debugJson.HasMember("stop-checkpoints")) {
                    ctx->stopCheckpoints = Ctx::getJsonFieldU64(fileName, debugJson, "stop-checkpoints");
                    INFO("will shutdown after " + std::to_string(ctx->stopCheckpoints) + " checkpoints")
                }

                if (debugJson.HasMember("stop-transactions")) {
                    ctx->stopTransactions = Ctx::getJsonFieldU64(fileName, debugJson, "stop-transactions");
                    INFO("will shutdown after " + std::to_string(ctx->stopTransactions) + " transactions")
                }

                if (debugJson.HasMember("owner") || debugJson.HasMember("table")) {
                    debugOwner = Ctx::getJsonFieldS(fileName, SYS_USER_NAME_LENGTH, debugJson, "owner");
                    debugTable = Ctx::getJsonFieldS(fileName, SYS_OBJ_NAME_LENGTH, debugJson, "table");
                    INFO("will shutdown after committed DML in " << debugOwner << "." << debugTable)
                }
            }

            typeConId conId = -1;
            if (readerJson.HasMember("con-id"))
                conId = Ctx::getJsonFieldI16(fileName, readerJson, "con-id");

            if (sourceJson.HasMember("transaction-max-mb")) {
                uint64_t transactionMaxMb = Ctx::getJsonFieldU64(fileName, sourceJson, "transaction-max-mb");
                if (transactionMaxMb > memoryMaxMb)
                    throw ConfigurationException("bad JSON, 'transaction-max-mb' (" + std::to_string(transactionMaxMb) +
                                                 ") is bigger than 'memory-max-mb' (" + std::to_string(memoryMaxMb) + ")");
                ctx->transactionSizeMax = transactionMaxMb * 1024 * 1024;
            }

            // MEMORY MANAGER
            ctx->initialize(memoryMinMb, memoryMaxMb, readBufferMax);

            // METADATA
            Metadata* metadata = new Metadata(ctx, locales, name, conId, startScn, startSequence, startTime, startTimeRel);
            metadatas.push_back(metadata);

            metadata->addElement("SYS", "CCOL\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "CDEF\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "COL\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "DEFERRED_STG\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "ECOL\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "LOB\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "OBJ\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "TAB\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "TABPART\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "TABCOMPART\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "TABSUBPART\\$", OPTIONS_SYSTEM_TABLE);
            metadata->addElement("SYS", "USER\\$", OPTIONS_SYSTEM_TABLE);
            if (debugOwner != nullptr && debugTable != nullptr)
                metadata->addElement(debugOwner, debugTable, OPTIONS_DEBUG_TABLE);
            if (FLAG(REDO_FLAGS_ADAPTIVE_SCHEMA))
                metadata->addElement(".*", ".*", 0);

            if (stateType == STATE_TYPE_DISK) {
                metadata->initializeDisk(statePath);
            } else
                throw RuntimeException("incorrect state chosen: " + std::to_string(stateType));

            // CHECKPOINT
            auto checkpoint = new Checkpoint(ctx, metadata, std::string(alias) + "-checkpoint");
            checkpoints.push_back(checkpoint);
            ctx->spawnThread(checkpoint);

            // TRANSACTION BUFFER
            TransactionBuffer* transactionBuffer = new TransactionBuffer(ctx);
            transactionBuffers.push_back(transactionBuffer);

            // FORMAT
            const rapidjson::Value& formatJson = Ctx::getJsonFieldO(fileName, sourceJson, "format");

            uint64_t messageFormat = MESSAGE_FORMAT_DEFAULT;
            if (formatJson.HasMember("message")) {
                messageFormat = Ctx::getJsonFieldU64(fileName, formatJson, "message");
                if (messageFormat > 15)
                    throw ConfigurationException("bad JSON, invalid 'message' value: " + std::to_string(messageFormat) +
                                                 ", expected one of: {0.. 15}");
                if ((messageFormat & MESSAGE_FORMAT_FULL) != 0 &&
                        (messageFormat & (MESSAGE_FORMAT_SKIP_BEGIN | MESSAGE_FORMAT_SKIP_COMMIT)) != 0)
                    throw ConfigurationException("bad JSON, invalid 'message' value: " + std::to_string(messageFormat) +
                                                 ", you are not allowed to use BEGIN/COMMIT flag (" + std::to_string(MESSAGE_FORMAT_SKIP_BEGIN) +
                                                 "/" + std::to_string(MESSAGE_FORMAT_SKIP_COMMIT) + ") together with FULL mode (" +
                                                 std::to_string(MESSAGE_FORMAT_FULL) + ")");
            }

            uint64_t ridFormat = RID_FORMAT_SKIP;
            if (formatJson.HasMember("rid")) {
                ridFormat = Ctx::getJsonFieldU64(fileName, formatJson, "rid");
                if (ridFormat > 1)
                    throw ConfigurationException("bad JSON, invalid 'rid' value: " + std::to_string(ridFormat) +
                                                 ", expected one of: {0, 1}");
            }

            uint64_t xidFormat = XID_FORMAT_TEXT;
            if (formatJson.HasMember("xid")) {
                xidFormat = Ctx::getJsonFieldU64(fileName, formatJson, "xid");
                if (xidFormat > 1)
                    throw ConfigurationException("bad JSON, invalid 'xid' value: " + std::to_string(xidFormat) +
                                                 ", expected one of: {0, 1}");
            }

            uint64_t timestampFormat = TIMESTAMP_FORMAT_UNIX;
            if (formatJson.HasMember("timestamp")) {
                timestampFormat = Ctx::getJsonFieldU64(fileName, formatJson, "timestamp");
                if (timestampFormat > 3)
                    throw ConfigurationException("bad JSON, invalid 'timestamp' value: " + std::to_string(timestampFormat) +
                                                 ", expected one of: {0, 1, 2, 3}");
            }

            uint64_t charFormat = CHAR_FORMAT_UTF8;
            if (formatJson.HasMember("char")) {
                charFormat = Ctx::getJsonFieldU64(fileName, formatJson, "char");
                if (charFormat > 3)
                    throw ConfigurationException("bad JSON, invalid 'char' value: " + std::to_string(charFormat) +
                                                 ", expected one of: {0, 1, 2, 3}");
            }

            uint64_t scnFormat = SCN_FORMAT_NUMERIC;
            if (formatJson.HasMember("scn")) {
                scnFormat = Ctx::getJsonFieldU64(fileName, formatJson, "scn");
                if (scnFormat > 3)
                    throw ConfigurationException("bad JSON, invalid 'scn' value: " + std::to_string(scnFormat) +
                                                 ", expected one of: {0, 1, 2, 3}");
            }

            uint64_t unknownFormat = UNKNOWN_FORMAT_QUESTION_MARK;
            if (formatJson.HasMember("unknown")) {
                unknownFormat = Ctx::getJsonFieldU64(fileName, formatJson, "unknown");
                if (unknownFormat > 1)
                    throw ConfigurationException("bad JSON, invalid 'unknown' value: " + std::to_string(unknownFormat) +
                                                 ", expected one of: {0, 1}");
            }

            uint64_t schemaFormat = SCHEMA_FORMAT_NAME;
            if (formatJson.HasMember("schema")) {
                schemaFormat = Ctx::getJsonFieldU64(fileName, formatJson, "schema");
                if (schemaFormat > 7)
                    throw ConfigurationException("bad JSON, invalid 'schema' value: " + std::to_string(schemaFormat) +
                                                 ", expected one of: {0 .. 7}");
            }

            uint64_t columnFormat = COLUMN_FORMAT_CHANGED;
            if (formatJson.HasMember("column")) {
                columnFormat = Ctx::getJsonFieldU64(fileName, formatJson, "column");
                if (columnFormat > 2)
                    throw ConfigurationException("bad JSON, invalid 'column' value: " + std::to_string(columnFormat) +
                                                 ", expected one of: {0, 1, 2}");

                if (FLAG(REDO_FLAGS_SCHEMALESS) && columnFormat != 0)
                    throw ConfigurationException("bad JSON, invalid 'column' value: " + std::to_string(columnFormat) +
                                                 " is invalid for schemaless mode");
            }

            uint64_t unknownType = UNKNOWN_TYPE_HIDE;
            if (formatJson.HasMember("unknown-type")) {
                unknownType = Ctx::getJsonFieldU64(fileName, formatJson, "unknown-type");
                if (unknownType > 1)
                    throw ConfigurationException("bad JSON, invalid 'unknown-type' value: " + std::to_string(unknownType) +
                                                 ", expected one of: {0, 1}");
            }

            uint64_t flushBuffer = 1048576;
            if (formatJson.HasMember("flush-buffer"))
                flushBuffer = Ctx::getJsonFieldU64(fileName, formatJson, "flush-buffer");

            const char* formatType = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, formatJson, "type");

            Builder* builder;
            if (strcmp("json", formatType) == 0) {
                builder = new BuilderJson(ctx, locales, metadata, messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat,
                                          schemaFormat, columnFormat, unknownType, flushBuffer);
            } else if (strcmp("protobuf", formatType) == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                builder = new BuilderProtobuf(ctx, locales, metadata, messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat,
                                              unknownFormat, schemaFormat, columnFormat, unknownType, flushBuffer);
#else
                throw RuntimeException("format 'protobuf' is not compiled, exiting");
#endif /* LINK_LIBRARY_PROTOBUF */
            } else
                throw ConfigurationException(std::string("bad JSON, invalid 'type' value: ") + formatType);
            builders.push_back(builder);
            builder->initialize();

            // READER
            const char* readerType = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, readerJson, "type");
            void (*archGetLog)(Replicator* replicator) = Replicator::archGetLogPath;

            if (sourceJson.HasMember("redo-read-sleep-us"))
                ctx->redoReadSleepUs = Ctx::getJsonFieldU64(fileName, sourceJson, "redo-read-sleep-us");

            if (sourceJson.HasMember("arch-read-sleep-us"))
                ctx->archReadSleepUs = Ctx::getJsonFieldU64(fileName, sourceJson, "arch-read-sleep-us");

            if (sourceJson.HasMember("arch-read-tries")) {
                ctx->archReadTries = Ctx::getJsonFieldU64(fileName, sourceJson, "arch-read-tries");
                if (ctx->archReadTries < 1 || ctx->archReadTries > 1000000000)
                    throw ConfigurationException("bad JSON, invalid 'arch-read-tries' value: " + std::to_string(ctx->archReadTries) +
                                                 ", expected one of: {1, 1000000000}");
            }

            if (sourceJson.HasMember("redo-verify-delay-us"))
                ctx->redoVerifyDelayUs = Ctx::getJsonFieldU64(fileName, sourceJson, "redo-verify-delay-us");

            if (sourceJson.HasMember("refresh-interval-us"))
                ctx->refreshIntervalUs = Ctx::getJsonFieldU64(fileName, sourceJson, "refresh-interval-us");

            if (readerJson.HasMember("redo-copy-path"))
                ctx->redoCopyPath = Ctx::getJsonFieldS(fileName, MAX_PATH_LENGTH, readerJson, "redo-copy-path");

            if (strcmp(readerType, "online") == 0) {
#ifdef LINK_LIBRARY_OCI
                const char* user = Ctx::getJsonFieldS(fileName, JSON_USERNAME_LENGTH, readerJson, "user");
                const char* password = Ctx::getJsonFieldS(fileName, JSON_PASSWORD_LENGTH, readerJson, "password");
                const char* server = Ctx::getJsonFieldS(fileName, JSON_SERVER_LENGTH, readerJson, "server");
                bool keepConnection = false;

                if (sourceJson.HasMember("arch")) {
                    const char* arch = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, sourceJson, "arch");

                    if (strcmp(arch, "path") == 0)
                        archGetLog = Replicator::archGetLogPath;
                    else if (strcmp(arch, "online") == 0) {
                        archGetLog = ReplicatorOnline::archGetLogOnline;
                    } else if (strcmp(arch, "online-keep") == 0) {
                        archGetLog = ReplicatorOnline::archGetLogOnline;
                        keepConnection = true;
                    } else
                        throw ConfigurationException(std::string("bad JSON, invalid 'arch' value: ") + arch +
                                                     ", expected one of: {'path', 'online', 'online-keep'}");
                } else
                    archGetLog = ReplicatorOnline::archGetLogOnline;

                replicator = new ReplicatorOnline(ctx, archGetLog, builder, metadata, transactionBuffer, alias, name, user, password,
                                                  server, keepConnection);
                replicator->initialize();
                mainProcessMapping(readerJson);
#else
                throw RuntimeException("reader type 'online' is not compiled, exiting");
#endif /*LINK_LIBRARY_OCI*/

            } else if (strcmp(readerType, "offline") == 0) {
                if (strcmp(startTime, "") != 0)
                    throw RuntimeException("starting by time is not supported for offline mode");
                if (startTimeRel != 0)
                    throw RuntimeException("starting by relative time is not supported for offline mode");

                replicator = new Replicator(ctx, archGetLog, builder, metadata, transactionBuffer, alias, name);
                replicator->initialize();
                mainProcessMapping(readerJson);

            } else if (strcmp(readerType, "batch") == 0) {
                if (strcmp(startTime, "") != 0)
                    throw ConfigurationException("starting by time is not supported for batch mode");
                if (startTimeRel != 0)
                    throw RuntimeException("starting by relative time is not supported for batch mode");

                archGetLog = Replicator::archGetLogList;
                replicator = new ReplicatorBatch(ctx, archGetLog, builder, metadata, transactionBuffer, alias, name);
                replicator->initialize();

                const rapidjson::Value& redoLogBatchArrayJson = Ctx::getJsonFieldA(fileName, readerJson, "redo-log");

                for (rapidjson::SizeType k = 0; k < redoLogBatchArrayJson.Size(); ++k)
                    replicator->addRedoLogsBatch(
                            Ctx::getJsonFieldS(fileName, MAX_PATH_LENGTH, redoLogBatchArrayJson, "redo-log", k));

            } else
                throw ConfigurationException(std::string("bad JSON, invalid 'format' value: ") + readerType);

            if (sourceJson.HasMember("filter") && !FLAG(REDO_FLAGS_SCHEMALESS)) {
                const rapidjson::Value& filterJson = Ctx::getJsonFieldO(fileName, sourceJson, "filter");

                if (filterJson.HasMember("table")) {
                    const rapidjson::Value& tableArrayJson = Ctx::getJsonFieldA(fileName, filterJson, "table");

                    for (rapidjson::SizeType k = 0; k < tableArrayJson.Size(); ++k) {
                        const rapidjson::Value& tableElementJson = Ctx::getJsonFieldO(fileName, tableArrayJson, "table",
                                                                                      k);

                        const char* owner = Ctx::getJsonFieldS(fileName, SYS_USER_NAME_LENGTH, tableElementJson, "owner");
                        const char* table = Ctx::getJsonFieldS(fileName, SYS_OBJ_NAME_LENGTH, tableElementJson, "table");
                        SchemaElement* element = metadata->addElement(owner, table, 0);

                        if (tableElementJson.HasMember("key")) {
                            element->keysStr = Ctx::getJsonFieldS(fileName, JSON_KEY_LENGTH, tableElementJson, "key");
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

                if (filterJson.HasMember("skip-xid")) {
                    const rapidjson::Value& skipXidArrayJson = Ctx::getJsonFieldA(fileName, filterJson, "skip-xid");
                    for (rapidjson::SizeType k = 0; k < skipXidArrayJson.Size(); ++k) {
                        typeXid xid(Ctx::getJsonFieldS(fileName, JSON_XID_LIST_LENGTH, skipXidArrayJson, "skip-xid", k));
                        INFO("adding XID to skip list: " << xid)
                        transactionBuffer->skipXidList.insert(xid);
                    }
                }
            }

            if (readerJson.HasMember("log-archive-format"))
                replicator->metadata->logArchiveFormat = Ctx::getJsonFieldS(fileName, VPARAMETER_LENGTH, readerJson,
                                                                            "log-archive-format");

            replicators.push_back(replicator);
            ctx->spawnThread(replicator);
            replicator = nullptr;
        }

        // Iterate through targets
        const rapidjson::Value& targetArrayJson = Ctx::getJsonFieldA(fileName, document, "target");
        if (targetArrayJson.Size() > 1) {
            throw ConfigurationException("bad JSON, only one 'target' element is allowed");
        }

        for (rapidjson::SizeType j = 0; j < targetArrayJson.Size(); ++j) {
            const rapidjson::Value& targetJson = targetArrayJson[j];
            const char* alias = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, targetJson, "alias");
            const char* source = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, targetJson, "source");

            INFO("adding target: " << alias)
            Replicator* replicator2 = nullptr;
            for (Replicator* replicatorTmp : replicators)
                if (replicatorTmp->alias == source)
                    replicator2 = (Replicator*)replicatorTmp;
            if (replicator2 == nullptr)
                throw ConfigurationException(std::string("bad JSON, couldn't find reader for 'source' value: ") + source);

            // Writer
            Writer* writer;
            const rapidjson::Value& writerJson = Ctx::getJsonFieldO(fileName, targetJson, "writer");
            const char* writerType = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, writerJson, "type");

            if (writerJson.HasMember("poll-interval-us")) {
                ctx->pollIntervalUs = Ctx::getJsonFieldU64(fileName, writerJson, "poll-interval-us");
                if (ctx->pollIntervalUs < 100 || ctx->pollIntervalUs > 3600000000)
                    throw ConfigurationException("bad JSON, invalid 'poll-interval-us' value: " + std::to_string(ctx->pollIntervalUs) +
                                                 ", expected one of: {100 .. 3600000000}");
            }

            if (writerJson.HasMember("queue-size")) {
                ctx->queueSize = Ctx::getJsonFieldU64(fileName, writerJson, "queue-size");
                if (ctx->queueSize < 1 || ctx->queueSize > 1000000)
                    throw ConfigurationException("bad JSON, invalid 'queue-size' value: " + std::to_string(ctx->queueSize) +
                                                 ", expected one of: {1 .. 1000000}");
            }

            if (strcmp(writerType, "file") == 0) {
                uint64_t maxSize = 0;
                if (writerJson.HasMember("max-size"))
                    maxSize = Ctx::getJsonFieldU64(fileName, writerJson, "max-size");

                const char* format = "%F_%T";
                if (writerJson.HasMember("format"))
                    format = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, writerJson, "format");

                const char* output = "";
                if (writerJson.HasMember("output"))
                    output = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, writerJson, "output");
                else if (maxSize > 0)
                    throw ConfigurationException("parameter 'max-size' should be 0 when 'output' is not set (for: file writer)");

                uint64_t newLine = 1;
                if (writerJson.HasMember("new-line")) {
                    newLine = Ctx::getJsonFieldU64(fileName, writerJson, "new-line");
                    if (newLine > 2)
                        throw ConfigurationException("bad JSON, invalid 'new-line' value: " + std::to_string(newLine) +
                                                     ", expected one of: {0, 1, 2}");
                }

                uint64_t append = 1;
                if (writerJson.HasMember("append")) {
                    append = Ctx::getJsonFieldU64(fileName, writerJson, "append");
                    if (append > 1)
                        throw ConfigurationException("bad JSON, invalid 'append' value: " + std::to_string(append) +
                                                     ", expected one of: {0, 1}");
                }

                writer = new WriterFile(ctx, std::string(alias) + "-writer", replicator2->database, replicator2->builder,
                                        replicator2->metadata, output, format, maxSize, newLine, append);
            } else if (strcmp(writerType, "kafka") == 0) {
#ifdef LINK_LIBRARY_RDKAFKA
                uint64_t maxMessageMb = 100;
                if (writerJson.HasMember("max-message-mb")) {
                    maxMessageMb = Ctx::getJsonFieldU64(fileName, writerJson, "max-message-mb");
                    if (maxMessageMb < 1 || maxMessageMb > MAX_KAFKA_MESSAGE_MB)
                        throw ConfigurationException("bad JSON, invalid 'max-message-mb' value: " + std::to_string(maxMessageMb) +
                                                     ", expected one of: {1 .. " + std::to_string(MAX_KAFKA_MESSAGE_MB) + "}");
                }
                replicator2->builder->setMaxMessageMb(maxMessageMb);

                uint64_t maxMessages = 100000;
                if (writerJson.HasMember("max-messages")) {
                    maxMessages = Ctx::getJsonFieldU64(fileName, writerJson, "max-messages");
                    if (maxMessages < 1 || maxMessages > MAX_KAFKA_MAX_MESSAGES)
                        throw ConfigurationException("bad JSON, invalid 'max-messages' value: " + std::to_string(maxMessages) +
                                                     ", expected one of: {1 .. " + std::to_string(MAX_KAFKA_MAX_MESSAGES) + "}");
                }

                bool enableIdempotence = true;
                if (writerJson.HasMember("enable-idempotence")) {
                    uint64_t enableIdempotenceInt = Ctx::getJsonFieldU64(fileName, writerJson, "enable-idempotence");
                    if (enableIdempotenceInt == 1)
                        enableIdempotence = true;
                    else if (enableIdempotenceInt > 1)
                        throw ConfigurationException("bad JSON, invalid 'enable-idempotence' value: " + std::to_string(enableIdempotenceInt) +
                                                     ", expected one of: {0, 1}");
                }

                const char* brokers = Ctx::getJsonFieldS(fileName, JSON_BROKERS_LENGTH, writerJson, "brokers");

                const char* topic = Ctx::getJsonFieldS(fileName, JSON_TOPIC_LENGTH, writerJson, "topic");

                writer = new WriterKafka(ctx, std::string(alias) + "-writer", replicator2->database, replicator2->builder,
                                         replicator2->metadata, brokers, topic, maxMessages, enableIdempotence);
#else
                throw RuntimeException("writer Kafka is not compiled, exiting");
#endif /* LINK_LIBRARY_RDKAFKA */
            } else if (strcmp(writerType, "zeromq") == 0) {
#if defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ)
                const char* uri = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, writerJson, "uri");
                StreamZeroMQ* stream = new StreamZeroMQ(ctx, uri);
                stream->initialize();
                writer = new WriterStream(ctx, std::string(alias) + "-writer", replicator2->database, replicator2->builder, replicator2->metadata, stream);
#else
                throw RuntimeException("writer ZeroMQ is not compiled, exiting");
#endif /* defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ) */
            } else if (strcmp(writerType, "network") == 0) {
#ifdef LINK_LIBRARY_PROTOBUF
                const char* uri = Ctx::getJsonFieldS(fileName, JSON_PARAMETER_LENGTH, writerJson, "uri");

                StreamNetwork* stream = new StreamNetwork(ctx, uri);
                stream->initialize();
                writer = new WriterStream(ctx, std::string(alias) + "-writer", replicator2->database, replicator2->builder,
                                          replicator2->metadata, stream);
#else
                throw RuntimeException("writer Network is not compiled, exiting");
#endif /* LINK_LIBRARY_PROTOBUF */
            } else
                throw ConfigurationException(std::string("bad JSON: invalid 'type' value: ") + writerType);

            writers.push_back(writer);
            writer->initialize();
            ctx->spawnThread(writer);
        }

        ctx->mainLoop();

        return 0;
    }

    void OpenLogReplicator::mainProcessMapping(const rapidjson::Value& readerJson) {
        if (readerJson.HasMember("path-mapping")) {
            const rapidjson::Value& pathMappingArrayJson = Ctx::getJsonFieldA(fileName, readerJson, "path-mapping");

            if ((pathMappingArrayJson.Size() % 2) != 0)
                throw ConfigurationException("bad JSON, 'path-mapping' should contain even number of schemaElements");

            for (rapidjson::SizeType k = 0; k < pathMappingArrayJson.Size() / 2; ++k) {
                const char* sourceMapping = Ctx::getJsonFieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJson,
                                                               "path-mapping", k * 2);
                const char* targetMapping = Ctx::getJsonFieldS(fileName, MAX_PATH_LENGTH, pathMappingArrayJson,
                                                               "path-mapping", k * 2 + 1);
                replicator->addPathMapping(sourceMapping, targetMapping);
            }
        }
    }
}
