/* Create main thread instances
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <utility>
#include <unistd.h>

#include "builder/BuilderJson.h"
#include "common/Ctx.h"
#include "common/MemoryManager.h"
#include "common/exception/ConfigurationException.h"
#include "common/exception/RuntimeException.h"
#include "common/metrics/Metrics.h"
#include "common/table/SysObj.h"
#include "common/table/SysUser.h"
#include "common/types/Types.h"
#include "locales/Locales.h"
#include "metadata/Checkpoint.h"
#include "metadata/Metadata.h"
#include "metadata/SchemaElement.h"
#include "metadata/SerializerJson.h"
#include "parser/TransactionBuffer.h"
#include "replicator/Replicator.h"
#include "replicator/ReplicatorBatch.h"
#include "state/StateDisk.h"
#include "writer/WriterDiscard.h"
#include "writer/WriterFile.h"
#include "OpenLogReplicator.h"

#ifdef LINK_LIBRARY_OCI
#include "replicator/ReplicatorOnline.h"
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#include "builder/BuilderProtobuf.h"
#include "stream/StreamNetwork.h"
#include "writer/WriterStream.h"
#ifdef LINK_LIBRARY_ZEROMQ
#include "stream/StreamZeroMQ.h"
#endif /* LINK_LIBRARY_ZEROMQ */
#endif /* LINK_LIBRARY_PROTOBUF */

#ifdef LINK_LIBRARY_RDKAFKA
#include "writer/WriterKafka.h"
#endif /* LINK_LIBRARY_RDKAFKA */

#ifdef LINK_LIBRARY_PROMETHEUS
#include "common/metrics/MetricsPrometheus.h"
#endif /* LINK_LIBRARY_PROMETHEUS */

namespace OpenLogReplicator {
    OpenLogReplicator::OpenLogReplicator(std::string  newConfigFileName, Ctx* newCtx) :
            configFileName(std::move(newConfigFileName)),
            ctx(newCtx) {
        IntX::initializeBASE10();
    }

    OpenLogReplicator::~OpenLogReplicator() {
        if (replicator != nullptr)
            replicators.push_back(replicator);

        ctx->stopSoft();
        ctx->mainFinish();

        for (Writer* writer: writers) {
            writer->flush();
            delete writer;
        }
        writers.clear();

        for (Builder* builder: builders)
            delete builder;
        builders.clear();

        for (Replicator* replicatorTmp: replicators)
            delete replicatorTmp;
        replicators.clear();

        for (Checkpoint* checkpoint: checkpoints)
            delete checkpoint;
        checkpoints.clear();

        for (TransactionBuffer* transactionBuffer: transactionBuffers)
            delete transactionBuffer;
        transactionBuffers.clear();

        for (Metadata* metadata: metadatas)
            delete metadata;
        metadatas.clear();

        for (Locales* locales: localess)
            delete locales;
        localess.clear();

        for (MemoryManager* memoryManager: memoryManagers)
            delete memoryManager;
        memoryManagers.clear();

        if (fid != -1)
            close(fid);
        delete[] configFileBuffer;
        configFileBuffer = nullptr;
    }

    int OpenLogReplicator::run(const Start& start) {
        auto* locales = new Locales();
        localess.push_back(locales);
        locales->initialize();

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "main (" + ss.str() + ") start");
        }

        struct stat configFileStat{};
        fid = open(configFileName.c_str(), O_RDONLY);
        if (fid == -1)
            throw RuntimeException(10001, "file: " + configFileName + " - open for read returned: " + strerror(errno));

        if (flock(fid, LOCK_EX | LOCK_NB) != 0)
            throw RuntimeException(10002, "file: " + configFileName + " - lock operation returned: " + strerror(errno));

        if (stat(configFileName.c_str(), &configFileStat) != 0)
            throw RuntimeException(10003, "file: " + configFileName + " - get metadata returned: " + strerror(errno));

        if (configFileStat.st_size > Checkpoint::CONFIG_FILE_MAX_SIZE || configFileStat.st_size == 0)
            throw ConfigurationException(10004, "file: " + configFileName + " - wrong size: " + std::to_string(configFileStat.st_size));

        configFileBuffer = new char[configFileStat.st_size + 1];
        const uint64_t bytesRead = read(fid, configFileBuffer, configFileStat.st_size);
        if (bytesRead != static_cast<uint64_t>(configFileStat.st_size))
            throw RuntimeException(10005, "file: " + configFileName + " - " + std::to_string(bytesRead) + " bytes read instead of " +
                                          std::to_string(configFileStat.st_size));
        configFileBuffer[configFileStat.st_size] = 0;

        rapidjson::Document document;
        if (document.Parse(configFileBuffer).HasParseError())
            throw DataException(20001, "file: " + configFileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                       " - parse error: " + GetParseError_En(document.GetParseError()));

        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
            static const std::vector<std::string> documentNames {"version", "dump-path", "dump-raw-data", "dump-redo-log", "log-level", "trace", "source",
                                                                 "target"};
            Ctx::checkJsonFields(configFileName, document, documentNames);
        }

        const std::string version = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, document, "version");
        if (version != OpenLogReplicator_SCHEMA_VERSION)
            throw ConfigurationException(30001, "bad JSON, invalid \"version\" value: " + version + ", expected: " +
                                                OpenLogReplicator_SCHEMA_VERSION);

        if (document.HasMember("dump-redo-log")) {
            ctx->dumpRedoLog = Ctx::getJsonFieldU(configFileName, document, "dump-redo-log");
            if (ctx->dumpRedoLog > 2)
                throw ConfigurationException(30001, "bad JSON, invalid \"dump-redo-log\" value: " + std::to_string(ctx->dumpRedoLog) +
                                                    ", expected: one of {0 .. 2}");

            if (ctx->dumpRedoLog > 0) {
                if (document.HasMember("dump-path"))
                    ctx->dumpPath = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, document, "dump-path");

                if (document.HasMember("dump-raw-data")) {
                    ctx->dumpRawData = Ctx::getJsonFieldU(configFileName, document, "dump-raw-data");
                    if (ctx->dumpRawData > 1)
                        throw ConfigurationException(30001, "bad JSON, invalid \"dump-raw-data\" value: " +
                                                            std::to_string(ctx->dumpRawData) + ", expected: one of {0, 1}");
                }
            }
        }

        if (document.HasMember("log-level")) {
            ctx->logLevel = static_cast<Ctx::LOG>(Ctx::getJsonFieldU(configFileName, document, "log-level"));
            if (ctx->logLevel > Ctx::LOG::DEBUG)
                throw ConfigurationException(30001, "bad JSON, invalid \"log-level\" value: " + std::to_string(static_cast<uint>(ctx->logLevel)) +
                                                    ", expected: one of {0 .. 4}");
        }

        if (document.HasMember("trace")) {
            ctx->trace = Ctx::getJsonFieldU64(configFileName, document, "trace");
            if (ctx->trace > 524287)
                throw ConfigurationException(30001, "bad JSON, invalid \"trace\" value: " + std::to_string(ctx->trace) +
                                                    ", expected: one of {0 .. 524287}");
        }

        // Iterate through sources
        const rapidjson::Value& sourceArrayJson = Ctx::getJsonFieldA(configFileName, document, "source");
        if (sourceArrayJson.Size() != 1) {
            throw ConfigurationException(30001, "bad JSON, invalid \"source\" value: " + std::to_string(sourceArrayJson.Size()) +
                                                " elements, expected: 1 element");
        }

        for (rapidjson::SizeType j = 0; j < sourceArrayJson.Size(); ++j) {
            const rapidjson::Value& sourceJson = Ctx::getJsonFieldO(configFileName, sourceArrayJson, "source", j);

            if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sourceNames {"alias", "memory", "name", "reader", "flags", "state", "debug", "transaction-max-mb",
                                                                   "metrics", "format", "redo-read-sleep-us", "arch-read-sleep-us", "arch-read-tries",
                                                                   "redo-verify-delay-us", "refresh-interval-us", "arch", "filter"};
                Ctx::checkJsonFields(configFileName, sourceJson, sourceNames);
            }

            const std::string alias = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, sourceJson, "alias");
            ctx->info(0, "adding source: " + alias);

            uint64_t memoryMinMb = 32;
            uint64_t memoryMaxMb = 2048;
            uint64_t memoryReadBufferMaxMb = 128;
            uint64_t memoryReadBufferMinMb = 4;
            uint64_t memorySwapMb = memoryMaxMb * 3 / 4;
            std::string memorySwapPath{"."};
            uint64_t memoryUnswapBufferMinMb = 4;
            uint64_t memoryWriteBufferMaxMb = memoryMaxMb;
            uint64_t memoryWriteBufferMinMb = 4;

            // MEMORY
            if (sourceJson.HasMember("memory")) {
                const rapidjson::Value& memoryJson = Ctx::getJsonFieldO(configFileName, sourceJson, "memory");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> memoryNames {"min-mb", "max-mb", "read-buffer-max-mb", "read-buffer-min-mb", "swap-mb", "swap-path",
                                                                       "unswap-buffer-min-mb", "write-buffer-max-mb", "write-buffer-min-mb"};
                    Ctx::checkJsonFields(configFileName, memoryJson, memoryNames);
                }

                if (memoryJson.HasMember("min-mb")) {
                    memoryMinMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "min-mb");
                    memoryMinMb = (memoryMinMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memoryMinMb < Ctx::MEMORY_CHUNK_MIN_MB)
                        throw ConfigurationException(30001, "bad JSON, invalid \"min-mb\" value: " + std::to_string(memoryMinMb) +
                                                            ", expected: at least " + std::to_string(Ctx::MEMORY_CHUNK_MIN_MB));
                }

                if (memoryJson.HasMember("max-mb")) {
                    memoryMaxMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "max-mb");
                    memoryMaxMb = (memoryMaxMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memoryMaxMb < memoryMinMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"max-mb\" value: " + std::to_string(memoryMaxMb) +
                                                            ", expected: at least like \"min-mb\" value (" + std::to_string(memoryMinMb) + ")");

                    memoryReadBufferMaxMb = std::min<uint64_t>(memoryMaxMb / 8, 128);
                    memoryWriteBufferMaxMb = std::min<uint64_t>(memoryMaxMb, 2048);
                    memorySwapMb = memoryMaxMb * 3 / 4;
                }

                if (memoryJson.HasMember("unswap-buffer-min-mb")) {
                    memoryUnswapBufferMinMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "unswap-buffer-min-mb");
                    memoryUnswapBufferMinMb = (memoryUnswapBufferMinMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                }

                if (memoryJson.HasMember("swap-mb")) {
                    memorySwapMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "swap-mb");
                    memorySwapMb = (memorySwapMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memorySwapMb > memoryMaxMb - 4)
                        throw ConfigurationException(30001, "bad JSON, invalid \"swap-mb\" value: " + std::to_string(memorySwapMb) +
                                                            ", expected maximum \"max-mb\"-1 value (" + std::to_string(memoryMaxMb - 4) + ")");
                }

                if (memoryJson.HasMember("read-buffer-min-mb")) {
                    memoryReadBufferMinMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "read-buffer-min-mb");
                    memoryReadBufferMinMb = (memoryReadBufferMinMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memoryReadBufferMinMb > memoryMaxMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"read-buffer-min-mb\" value: " +
                                                            std::to_string(memoryReadBufferMaxMb) +
                                                            ", expected: not greater than \"max-mb\" value (" + std::to_string(memoryMaxMb) + ")");
                    if (memoryReadBufferMinMb < 4)
                        throw ConfigurationException(30001, "bad JSON, invalid \"read-buffer-min-mb\" value: " +
                                                            std::to_string(memoryReadBufferMaxMb) + ", expected: at least: 4");
                }

                if (memoryJson.HasMember("read-buffer-max-mb")) {
                    memoryReadBufferMaxMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "read-buffer-max-mb");
                    memoryReadBufferMaxMb = (memoryReadBufferMaxMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memoryReadBufferMaxMb > memoryMaxMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"read-buffer-max-mb\" value: " +
                                                            std::to_string(memoryReadBufferMaxMb) +
                                                            ", expected: not greater than \"max-mb\" value (" + std::to_string(memoryMaxMb) + ")");
                    if (memoryReadBufferMaxMb < memoryReadBufferMinMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"read-buffer-max-mb\" value: " +
                                                            std::to_string(memoryReadBufferMaxMb) + ", expected: at least: \"read-buffer-min-mb\" value (" +
                                                            std::to_string(memoryReadBufferMinMb) + ")");
                }

                if (memoryJson.HasMember("write-buffer-min-mb")) {
                    memoryWriteBufferMinMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "write-buffer-min-mb");
                    memoryWriteBufferMinMb = (memoryWriteBufferMinMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memoryWriteBufferMinMb > memoryMaxMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"write-buffer-min-mb\" value: " +
                                                            std::to_string(memoryWriteBufferMinMb) +
                                                            ", expected: not greater than \"max-mb\" value (" + std::to_string(memoryMaxMb) + ")");
                    if (memoryWriteBufferMinMb < 4)
                        throw ConfigurationException(30001, "bad JSON, invalid \"write-buffer-min-mb\" value: " +
                                                            std::to_string(memoryWriteBufferMinMb) + ", expected: at least: 4");
                }

                if (memoryJson.HasMember("write-buffer-max-mb")) {
                    memoryWriteBufferMaxMb = Ctx::getJsonFieldU64(configFileName, memoryJson, "write-buffer-max-mb");
                    memoryWriteBufferMaxMb = (memoryWriteBufferMaxMb / Ctx::MEMORY_CHUNK_SIZE_MB) * Ctx::MEMORY_CHUNK_SIZE_MB;
                    if (memoryWriteBufferMaxMb > memoryMaxMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"write-buffer-max-mb\" value: " +
                                                            std::to_string(memoryWriteBufferMaxMb) +
                                                            ", expected: not greater than \"max-mb\" value (" + std::to_string(memoryMaxMb) + ")");
                    if (memoryWriteBufferMaxMb < memoryWriteBufferMinMb)
                        throw ConfigurationException(30001, "bad JSON, invalid \"write-buffer-max-mb\" value: " +
                                                            std::to_string(memoryWriteBufferMaxMb) + ", expected: at least: \"write-buffer-min-mb\" value (" +
                                                            std::to_string(memoryWriteBufferMinMb) + ")");
                }

                if (memoryJson.HasMember("swap-path") && memorySwapMb > 0)
                    memorySwapPath = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, memoryJson, "swap-path");

                if (memoryUnswapBufferMinMb + memoryReadBufferMinMb + memoryWriteBufferMinMb + 4 > memoryMaxMb)
                    throw ConfigurationException(30001, R"(bad JSON, invalid "unswap-buffer-min-mb" + "read-buffer-min-mb" + "write-buffer-min-mb" + 4 ()" +
                                                        std::to_string(memoryUnswapBufferMinMb) + " + " + std::to_string(memoryReadBufferMinMb) +
                                                        " + " + std::to_string(memoryWriteBufferMinMb) + " + 4) is greater than \"max-mb\" value (" +
                                                        std::to_string(memoryMaxMb) + ")");
            }

            const std::string name = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, sourceJson, "name");
            const rapidjson::Value& readerJson = Ctx::getJsonFieldO(configFileName, sourceJson, "reader");

            if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> readerNames {"disable-checks", "con-id", "type", "redo-copy-path", "db-timezone", "host-timezone",
                                                                   "log-timezone", "user", "password", "server", "redo-log", "path-mapping",
                                                                   "log-archive-format"};
                Ctx::checkJsonFields(configFileName, readerJson, readerNames);
            }

            if (sourceJson.HasMember("flags")) {
                ctx->flags = Ctx::getJsonFieldU64(configFileName, sourceJson, "flags");
                if (ctx->flags > 524287)
                    throw ConfigurationException(30001, "bad JSON, invalid \"flags\" value: " + std::to_string(ctx->flags) +
                                                        ", expected: one of {0 .. 524287}");
                if (ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE))
                    ctx->redoVerifyDelayUs = 500000;
            }

            if (readerJson.HasMember("disable-checks")) {
                ctx->disableChecks = Ctx::getJsonFieldU64(configFileName, readerJson, "disable-checks");
                if (ctx->disableChecks > 15)
                    throw ConfigurationException(30001, "bad JSON, invalid \"disable-checks\" value: " +
                                                        std::to_string(ctx->disableChecks) + ", expected: one of {0 .. 15}");
            }

            uint64_t stateType = State::TYPE_DISK;
            std::string statePath = "checkpoint";

            if (sourceJson.HasMember("state")) {
                const rapidjson::Value& stateJson = Ctx::getJsonFieldO(configFileName, sourceJson, "state");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> stateNames {"type", "path", "interval-s", "interval-mb", "keep-checkpoints", "schema-force-interval"};
                    Ctx::checkJsonFields(configFileName, stateJson, stateNames);
                }

                if (stateJson.HasMember("type")) {
                    const std::string stateTypeStr = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, stateJson, "type");
                    if (stateTypeStr == "disk") {
                        stateType = State::TYPE_DISK;
                        if (stateJson.HasMember("path"))
                            statePath = Ctx::getJsonFieldS(configFileName, Ctx::MAX_PATH_LENGTH, stateJson, "path");
                    } else
                        throw ConfigurationException(30001, std::string("bad JSON, invalid \"type\" value: ") + stateTypeStr + ", expected: one of {\"disk\"}");
                }

                if (stateJson.HasMember("interval-s"))
                    ctx->checkpointIntervalS = Ctx::getJsonFieldU64(configFileName, stateJson, "interval-s");

                if (stateJson.HasMember("interval-mb"))
                    ctx->checkpointIntervalMb = Ctx::getJsonFieldU64(configFileName, stateJson, "interval-mb");

                if (stateJson.HasMember("keep-checkpoints"))
                    ctx->checkpointKeep = Ctx::getJsonFieldU64(configFileName, stateJson, "keep-checkpoints");

                if (stateJson.HasMember("schema-force-interval"))
                    ctx->schemaForceInterval = Ctx::getJsonFieldU64(configFileName, stateJson, "schema-force-interval");
            }

            std::string debugOwner;
            std::string debugTable;

            if (sourceJson.HasMember("debug")) {
                const rapidjson::Value& debugJson = Ctx::getJsonFieldO(configFileName, sourceJson, "debug");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> debugNames {"stop-log-switches", "stop-checkpoints", "stop-transactions", "owner", "table"};
                    Ctx::checkJsonFields(configFileName, debugJson, debugNames);
                }

                if (debugJson.HasMember("stop-log-switches")) {
                    ctx->stopLogSwitches = Ctx::getJsonFieldU64(configFileName, debugJson, "stop-log-switches");
                    ctx->info(0, "will shutdown after " + std::to_string(ctx->stopLogSwitches) + " log switches");
                }

                if (debugJson.HasMember("stop-checkpoints")) {
                    ctx->stopCheckpoints = Ctx::getJsonFieldU64(configFileName, debugJson, "stop-checkpoints");
                    ctx->info(0, "will shutdown after " + std::to_string(ctx->stopCheckpoints) + " checkpoints");
                }

                if (debugJson.HasMember("stop-transactions")) {
                    ctx->stopTransactions = Ctx::getJsonFieldU64(configFileName, debugJson, "stop-transactions");
                    ctx->info(0, "will shutdown after " + std::to_string(ctx->stopTransactions) + " transactions");
                }

                if (!ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS) && (debugJson.HasMember("owner") || debugJson.HasMember("table"))) {
                    debugOwner = Ctx::getJsonFieldS(configFileName, SysUser::NAME_LENGTH, debugJson, "owner");
                    debugTable = Ctx::getJsonFieldS(configFileName, SysObj::NAME_LENGTH, debugJson, "table");
                    ctx->info(0, "will shutdown after committed DML in " + debugOwner + "." + debugTable);
                }
            }

            typeConId conId = -1;
            if (readerJson.HasMember("con-id"))
                conId = Ctx::getJsonFieldI16(configFileName, readerJson, "con-id");

            if (sourceJson.HasMember("transaction-max-mb")) {
                const uint64_t transactionMaxMb = Ctx::getJsonFieldU64(configFileName, sourceJson, "transaction-max-mb");
                if (transactionMaxMb > memoryMaxMb)
                    throw ConfigurationException(30001, "bad JSON, invalid \"transaction-max-mb\" value: " + std::to_string(transactionMaxMb) +
                                                        ", expected: smaller than \"max-mb\" (" + std::to_string(memoryMaxMb) + ")");
                ctx->transactionSizeMax = transactionMaxMb * 1024 * 1024;
            }

            // MEMORY MANAGER
            ctx->initialize(memoryMinMb, memoryMaxMb, memoryReadBufferMaxMb, memoryReadBufferMinMb, memorySwapMb, memoryUnswapBufferMinMb,
                            memoryWriteBufferMaxMb, memoryWriteBufferMinMb);

            // METADATA
            auto* metadata = new Metadata(ctx, locales, name, conId, startNow, startScn, startSequence, startTime, startTimeRel);
            metadatas.push_back(metadata);
            metadata->resetElements();
            if (!debugOwner.empty())
                metadata->users.insert(debugOwner);

            if (!debugOwner.empty() && !debugTable.empty())
                metadata->addElement(debugOwner, debugTable, DbTable::OPTIONS::DEBUG_TABLE);
            if (ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA))
                metadata->addElement(".*", ".*", DbTable::OPTIONS::DEFAULT);

            if (stateType == State::TYPE_DISK) {
                metadata->state = new StateDisk(ctx, statePath);
                metadata->stateDisk = new StateDisk(ctx, "scripts");
                metadata->serializer = new SerializerJson();
            }

            // CHECKPOINT
            auto* checkpoint = new Checkpoint(ctx, metadata, alias + "-checkpoint", configFileName, configFileStat.st_mtime);
            checkpoints.push_back(checkpoint);
            ctx->spawnThread(checkpoint);

            // MEMORY MANAGER
            auto* memoryManager = new MemoryManager(ctx, alias + "-memory-manager", memorySwapPath);
            memoryManager->initialize();
            memoryManagers.push_back(memoryManager);
            ctx->spawnThread(memoryManager);

            // TRANSACTION BUFFER
            auto* transactionBuffer = new TransactionBuffer(ctx);
            transactionBuffers.push_back(transactionBuffer);

            // METRICS
            if (sourceJson.HasMember("metrics")) {
                const rapidjson::Value& metricsJson = Ctx::getJsonFieldO(configFileName, sourceJson, "metrics");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> metricsNames {"type", "bind", "tag-names"};
                    Ctx::checkJsonFields(configFileName, metricsJson, metricsNames);
                }

                if (metricsJson.HasMember("type")) {
                    const std::string metricsType = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, metricsJson, "type");
                    Metrics::TAG_NAMES tagNames = Metrics::TAG_NAMES::NONE;

                    if (metricsJson.HasMember("tag-names")) {
                        const std::string tagNamesStr = Ctx::getJsonFieldS(configFileName, Ctx::JSON_TOPIC_LENGTH, metricsJson, "tag-names");

                        if (tagNamesStr == "none")
                            tagNames = Metrics::TAG_NAMES::NONE;
                        else if (tagNamesStr == "filter")
                            tagNames = Metrics::TAG_NAMES::FILTER;
                        else if (tagNamesStr ==  "sys")
                            tagNames = Metrics::TAG_NAMES::SYS;
                        else if (tagNamesStr == "all")
                            tagNames = static_cast<Metrics::TAG_NAMES>(static_cast<uint>(Metrics::TAG_NAMES::FILTER) |
                                    static_cast<uint>(Metrics::TAG_NAMES::SYS));
                        else
                            throw ConfigurationException(30001, "bad JSON, invalid \"tag-names\" value: " + tagNamesStr +
                                                                R"(, expected: one of {"all", "filter", "none", "sys"})");
                    }

                    if (metricsType == "prometheus") {
#ifdef LINK_LIBRARY_PROMETHEUS
                        const std::string prometheusBind = Ctx::getJsonFieldS(configFileName, Ctx::JSON_TOPIC_LENGTH, metricsJson, "bind");

                        ctx->metrics = new MetricsPrometheus(tagNames, prometheusBind);
                        ctx->metrics->initialize(ctx);
#else
                        throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: \"" + metricsType +
                                                    "\", expected: not \"prometheus\" since the code is not compiled");
#endif /*LINK_LIBRARY_PROMETHEUS*/
                    } else {
                        throw ConfigurationException(30001, R"(bad JSON, invalid "type" value: ")" + metricsType + R"(", expected: one of {"prometheus"})");
                    }
                }
            }

            // FORMAT
            const rapidjson::Value& formatJson = Ctx::getJsonFieldO(configFileName, sourceJson, "format");

            if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> formatNames {"db", "attributes", "interval-dts", "interval-ytm", "message", "rid", "xid", "timestamp",
                                                                   "timestamp-tz", "timestamp-all", "char", "scn", "scn-type", "unknown", "schema", "column",
                                                                   "unknown-type", "flush-buffer", "type"};
                Ctx::checkJsonFields(configFileName, formatJson, formatNames);
            }

            Format::DB_FORMAT dbFormat = Format::DB_FORMAT::DEFAULT;
            if (formatJson.HasMember("db")) {
                const uint val = Ctx::getJsonFieldU64(configFileName, formatJson, "db");
                if (val > 3)
                    throw ConfigurationException(30001, "bad JSON, invalid \"db\" value: " + std::to_string(val) + ", expected: one of {0 .. 3}");
                dbFormat = static_cast<Format::DB_FORMAT>(val);
            }

            Format::ATTRIBUTES_FORMAT attributesFormat = Format::ATTRIBUTES_FORMAT::DEFAULT;
            if (formatJson.HasMember("attributes")) {
                const uint val = Ctx::getJsonFieldU64(configFileName, formatJson, "attributes");
                if (val > 7)
                    throw ConfigurationException(30001, "bad JSON, invalid \"attributes\" value: " + std::to_string(val) + ", expected: one of {0 .. 7}");
                attributesFormat = static_cast<Format::ATTRIBUTES_FORMAT>(val);
            }

            Format::INTERVAL_DTS_FORMAT intervalDtsFormat = Format::INTERVAL_DTS_FORMAT::UNIX_NANO;
            if (formatJson.HasMember("interval-dts")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "interval-dts");
                if (val > 10)
                    throw ConfigurationException(30001, "bad JSON, invalid \"interval-dts\" value: " + std::to_string(val) + ", expected: one of {0 .. 10}");
                intervalDtsFormat = static_cast<Format::INTERVAL_DTS_FORMAT>(val);
            }

            Format::INTERVAL_YTM_FORMAT intervalYtmFormat = Format::INTERVAL_YTM_FORMAT::MONTHS;
            if (formatJson.HasMember("interval-ytm")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "interval-ytm");
                if (val > 4)
                    throw ConfigurationException(30001, "bad JSON, invalid \"interval-ytm\" value: " + std::to_string(val) + ", expected: one of {0 .. 4}");
                intervalYtmFormat = static_cast<Format::INTERVAL_YTM_FORMAT>(val);
            }

            Format::MESSAGE_FORMAT messageFormat = Format::MESSAGE_FORMAT::DEFAULT;
            if (formatJson.HasMember("message")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "message");
                if (val > 31)
                    throw ConfigurationException(30001, "bad JSON, invalid \"message\" value: " + std::to_string(val) + ", expected: one of {0 .. 31}");
                if ((val & static_cast<uint>(Format::MESSAGE_FORMAT::FULL)) != 0 &&
                    (val & (static_cast<uint>(Format::MESSAGE_FORMAT::SKIP_BEGIN) |
                            static_cast<uint>(Format::MESSAGE_FORMAT::SKIP_COMMIT))) != 0)
                    throw ConfigurationException(30001, "bad JSON, invalid \"message\" value: " + std::to_string(val) +
                                                        ", expected: BEGIN/COMMIT flag is unset (" +
                                                        std::to_string(static_cast<uint>(Format::MESSAGE_FORMAT::SKIP_BEGIN)) +
                                                        "/" + std::to_string(static_cast<uint>(Format::MESSAGE_FORMAT::SKIP_COMMIT)) +
                                                        ") together with FULL mode (" +
                                                        std::to_string(static_cast<uint>(Format::MESSAGE_FORMAT::FULL)) + ")");
                messageFormat = static_cast<Format::MESSAGE_FORMAT>(val);
            }

            Format::RID_FORMAT ridFormat = Format::RID_FORMAT::SKIP;
            if (formatJson.HasMember("rid")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "rid");
                if (val > 1)
                    throw ConfigurationException(30001, "bad JSON, invalid \"rid\" value: " + std::to_string(val) + ", expected: one of {0, 1}");
                ridFormat = static_cast<Format::RID_FORMAT>(val);
            }

            Format::XID_FORMAT xidFormat = Format::XID_FORMAT::TEXT_HEX;
            if (formatJson.HasMember("xid")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "xid");
                if (val > 2)
                    throw ConfigurationException(30001, "bad JSON, invalid \"xid\" value: " + std::to_string(val) + ", expected: one of {0 .. 2}");
                xidFormat = static_cast<Format::XID_FORMAT>(val);
            }

            Format::TIMESTAMP_FORMAT timestampFormat = Format::TIMESTAMP_FORMAT::UNIX_NANO;
            if (formatJson.HasMember("timestamp")) {
                const uint val  = Ctx::getJsonFieldU(configFileName, formatJson, "timestamp");
                if (val > 15)
                    throw ConfigurationException(30001, "bad JSON, invalid \"timestamp\" value: " + std::to_string(val) + ", expected: one of {0 .. 15}");
                timestampFormat = static_cast<Format::TIMESTAMP_FORMAT>(val);
            }

            Format::TIMESTAMP_TZ_FORMAT timestampTzFormat = Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING;
            if (formatJson.HasMember("timestamp-tz")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "timestamp-tz");
                if (val > 11)
                    throw ConfigurationException(30001, "bad JSON, invalid \"timestamp-tz\" value: " + std::to_string(val) + ", expected: one of {0 .. 11}");
                timestampTzFormat = static_cast<Format::TIMESTAMP_TZ_FORMAT>(val);
            }

            Format::TIMESTAMP_ALL timestampAll = Format::TIMESTAMP_ALL::JUST_BEGIN;
            if (formatJson.HasMember("timestamp-all")) {
                const uint val = Ctx::getJsonFieldU64(configFileName, formatJson, "timestamp-all");
                if (val > 1)
                    throw ConfigurationException(30001, "bad JSON, invalid \"timestamp-all\" value: " + std::to_string(val) + ", expected: one of {0, 1}");
                timestampAll = static_cast<Format::TIMESTAMP_ALL>(val);
            }

            Format::CHAR_FORMAT charFormat = Format::CHAR_FORMAT::UTF8;
            if (formatJson.HasMember("char")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "char");
                if (val > 3)
                    throw ConfigurationException(30001, "bad JSON, invalid \"char\" value: " + std::to_string(val) + ", expected: one of {0 .. 3}");
                charFormat = static_cast<Format::CHAR_FORMAT>(val);
            }

            Format::SCN_FORMAT scnFormat = Format::SCN_FORMAT::NUMERIC;
            if (formatJson.HasMember("scn")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "scn");
                if (val > 1)
                    throw ConfigurationException(30001, "bad JSON, invalid \"scn\" value: " + std::to_string(val) + ", expected: one of {0, 1}");
                scnFormat = static_cast<Format::SCN_FORMAT>(val);
            }

            Format::SCN_TYPE scnType = Format::SCN_TYPE::NONE;
            if (formatJson.HasMember("scn-type")) {
                const uint val = Ctx::getJsonFieldU64(configFileName, formatJson, "scn-type");
                if (val > 3)
                    throw ConfigurationException(30001, "bad JSON, invalid \"scn-type\" value: " + std::to_string(val) + ", expected: one of {0, 3}");
                scnType = static_cast<Format::SCN_TYPE>(val);
            }

            Format::UNKNOWN_FORMAT unknownFormat = Format::UNKNOWN_FORMAT::QUESTION_MARK;
            if (formatJson.HasMember("unknown")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "unknown");
                if (val > 1)
                    throw ConfigurationException(30001, "bad JSON, invalid \"unknown\" value: " + std::to_string(val) + ", expected: one of {0, 1}");
                unknownFormat = static_cast<Format::UNKNOWN_FORMAT>(val);
            }

            Format::SCHEMA_FORMAT schemaFormat = Format::SCHEMA_FORMAT::DEFAULT;
            if (formatJson.HasMember("schema")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "schema");
                if (val > 7)
                    throw ConfigurationException(30001, "bad JSON, invalid \"schema\" value: " + std::to_string(val) + ", expected: one of {0 .. 7}");
                schemaFormat = static_cast<Format::SCHEMA_FORMAT>(val);
            }

            Format::COLUMN_FORMAT columnFormat = Format::COLUMN_FORMAT::CHANGED;
            if (formatJson.HasMember("column")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "column");
                if (val > 2)
                    throw ConfigurationException(30001, "bad JSON, invalid \"column\" value: " + std::to_string(val) + ", expected: one of {0 .. 2}");

                if (ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS) && val != 0)
                    throw ConfigurationException(30001, "bad JSON, invalid \"column\" value: " + std::to_string(val) +
                                                        ", expected: not used when flags has set schemaless mode (flags: " + std::to_string(ctx->flags) + ")");
                columnFormat = static_cast<Format::COLUMN_FORMAT>(val);
            }

            Format::UNKNOWN_TYPE unknownType = Format::UNKNOWN_TYPE::HIDE;
            if (formatJson.HasMember("unknown-type")) {
                const uint val = Ctx::getJsonFieldU(configFileName, formatJson, "unknown-type");
                if (val > 1)
                    throw ConfigurationException(30001, "bad JSON, invalid \"unknown-type\" value: " + std::to_string(val) + ", expected: one of {0, 1}");
                unknownType = static_cast<Format::UNKNOWN_TYPE>(val);
            }

            uint64_t flushBuffer = 1048576;
            if (formatJson.HasMember("flush-buffer"))
                flushBuffer = Ctx::getJsonFieldU64(configFileName, formatJson, "flush-buffer");

            const std::string formatType = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, formatJson, "type");

            Builder* builder;
            Format format(dbFormat, attributesFormat, intervalDtsFormat, intervalYtmFormat, messageFormat, ridFormat, xidFormat, timestampFormat,
                          timestampTzFormat, timestampAll, charFormat, scnFormat, scnType, unknownFormat, schemaFormat, columnFormat, unknownType);
            if (formatType == "json") {
                builder = new BuilderJson(ctx, locales, metadata, format, flushBuffer);
            } else if (formatType == "protobuf") {
#ifdef LINK_LIBRARY_PROTOBUF
                builder = new BuilderProtobuf(ctx, locales, metadata, format, flushBuffer);
#else
                throw ConfigurationException(30001, "bad JSON, invalid \"format\" value: " + formatType +
                                             ", expected: not \"protobuf\" since the code is not compiled");
#endif /* LINK_LIBRARY_PROTOBUF */
            } else
                throw ConfigurationException(30001, "bad JSON, invalid \"format\" value: " + formatType + R"(, expected: "protobuf" or "json")");
            builders.push_back(builder);

            // READER
            const std::string readerType = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, readerJson, "type");
            void (* archGetLog)(Replicator* replicator) = Replicator::archGetLogPath;

            if (sourceJson.HasMember("redo-read-sleep-us"))
                ctx->redoReadSleepUs = Ctx::getJsonFieldU64(configFileName, sourceJson, "redo-read-sleep-us");

            if (sourceJson.HasMember("arch-read-sleep-us"))
                ctx->archReadSleepUs = Ctx::getJsonFieldU64(configFileName, sourceJson, "arch-read-sleep-us");

            if (sourceJson.HasMember("arch-read-tries")) {
                ctx->archReadTries = Ctx::getJsonFieldU(configFileName, sourceJson, "arch-read-tries");
                if (ctx->archReadTries < 1 || ctx->archReadTries > 1000000000)
                    throw ConfigurationException(30001, "bad JSON, invalid \"arch-read-tries\" value: " +
                                                        std::to_string(ctx->archReadTries) + ", expected: one of: {1 .. 1000000000}");
            }

            if (sourceJson.HasMember("redo-verify-delay-us"))
                ctx->redoVerifyDelayUs = Ctx::getJsonFieldU64(configFileName, sourceJson, "redo-verify-delay-us");

            if (sourceJson.HasMember("refresh-interval-us"))
                ctx->refreshIntervalUs = Ctx::getJsonFieldU64(configFileName, sourceJson, "refresh-interval-us");

            if (readerJson.HasMember("redo-copy-path"))
                ctx->redoCopyPath = Ctx::getJsonFieldS(configFileName, Ctx::MAX_PATH_LENGTH, readerJson, "redo-copy-path");

            if (readerJson.HasMember("db-timezone")) {
                const std::string dbTimezone = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, readerJson, "db-timezone");
                if (!Data::parseTimezone(dbTimezone, ctx->dbTimezone))
                    throw ConfigurationException(30001, "bad JSON, invalid \"db-timezone\" value: " + dbTimezone + ", expected value: {\"+/-HH:MM\"}");
            }

            if (readerJson.HasMember("host-timezone")) {
                const std::string hostTimezone = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, readerJson, "host-timezone");
                if (!Data::parseTimezone(hostTimezone, ctx->hostTimezone))
                    throw ConfigurationException(30001, "bad JSON, invalid \"host-timezone\" value: " + hostTimezone + ", expected value: {\"+/-HH:MM\"}");
            }

            if (readerJson.HasMember("log-timezone")) {
                const std::string logTimezone = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, readerJson, "log-timezone");
                if (!Data::parseTimezone(logTimezone, ctx->logTimezone))
                    throw ConfigurationException(30001, "bad JSON, invalid \"log-timezone\" value: " + logTimezone + ", expected value: {\"+/-HH:MM\"}");
            }

            if (readerType == "online") {
#ifdef LINK_LIBRARY_OCI
                const std::string user = Ctx::getJsonFieldS(configFileName, Ctx::JSON_USERNAME_LENGTH, readerJson, "user");
                const std::string password = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PASSWORD_LENGTH, readerJson, "password");
                const std::string server = Ctx::getJsonFieldS(configFileName, Ctx::JSON_SERVER_LENGTH, readerJson, "server");
                bool keepConnection = false;

                if (sourceJson.HasMember("arch")) {
                    const std::string arch = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, sourceJson, "arch");

                    if (arch == "path")
                        archGetLog = Replicator::archGetLogPath;
                    else if (arch == "online") {
                        archGetLog = ReplicatorOnline::archGetLogOnline;
                    } else if (arch == "online-keep") {
                        archGetLog = ReplicatorOnline::archGetLogOnline;
                        keepConnection = true;
                    } else
                        throw ConfigurationException(30001, "bad JSON, invalid \"arch\" value: " + arch +
                                                     ", expected: one of {\"path\", \"online\", \"online-keep\"}");
                } else
                    archGetLog = ReplicatorOnline::archGetLogOnline;

                replicator = new ReplicatorOnline(ctx, archGetLog, builder, metadata, transactionBuffer, alias, name, user, password, server, keepConnection);
                builder->initialize();
                replicator->initialize();
                mainProcessMapping(readerJson);
#else
                throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: " + readerType +
                                                    ", expected: not \"online\" since the code is not compiled");
#endif /*LINK_LIBRARY_OCI*/

            } else if (readerType == "offline") {
                replicator = new Replicator(ctx, archGetLog, builder, metadata, transactionBuffer, alias, name);
                builder->initialize();
                replicator->initialize();
                mainProcessMapping(readerJson);

            } else if (readerType == "batch") {
                archGetLog = Replicator::archGetLogList;
                replicator = new ReplicatorBatch(ctx, archGetLog, builder, metadata, transactionBuffer, alias, name);
                builder->initialize();
                replicator->initialize();

                const rapidjson::Value& redoLogBatchArrayJson = Ctx::getJsonFieldA(configFileName, readerJson, "redo-log");

                for (rapidjson::SizeType k = 0; k < redoLogBatchArrayJson.Size(); ++k)
                    replicator->addRedoLogsBatch(Ctx::getJsonFieldS(configFileName, Ctx::MAX_PATH_LENGTH, redoLogBatchArrayJson, "redo-log", k));

            } else
                throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: " + readerType + R"(, expected: one of {"online", "offline", "batch"})");

            if (sourceJson.HasMember("filter")) {
                const rapidjson::Value& filterJson = Ctx::getJsonFieldO(configFileName, sourceJson, "filter");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> filterNames {"table", "skip-xid", "separator", "dump-xid"};
                    Ctx::checkJsonFields(configFileName, filterJson, filterNames);
                }

                std::string separator{","};
                if (filterJson.HasMember("separator"))
                    separator = Ctx::getJsonFieldS(configFileName, Ctx::JSON_FORMAT_SEPARATOR_LENGTH, filterJson, "separator");

                if (filterJson.HasMember("table") && !ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                    const rapidjson::Value& tableArrayJson = Ctx::getJsonFieldA(configFileName, filterJson, "table");

                    for (rapidjson::SizeType k = 0; k < tableArrayJson.Size(); ++k) {
                        const rapidjson::Value& tableElementJson = Ctx::getJsonFieldO(configFileName, tableArrayJson, "table", k);

                        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                            static const std::vector<std::string> tableElementNames {"owner", "table", "key", "condition", "tag"};
                            Ctx::checkJsonFields(configFileName, tableElementJson, tableElementNames);
                        }

                        const std::string owner = Ctx::getJsonFieldS(configFileName, SysUser::NAME_LENGTH, tableElementJson, "owner");
                        const std::string table = Ctx::getJsonFieldS(configFileName, SysObj::NAME_LENGTH, tableElementJson, "table");
                        SchemaElement* element = metadata->addElement(owner, table, DbTable::OPTIONS::DEFAULT);

                        metadata->users.insert(owner);

                        if (tableElementJson.HasMember("key")) {
                            element->key = Ctx::getJsonFieldS(configFileName, Ctx::JSON_KEY_LENGTH, tableElementJson, "key");
                            element->parseKey(element->key, separator);
                        };

                        if (tableElementJson.HasMember("condition"))
                            element->condition = Ctx::getJsonFieldS(configFileName, Ctx::JSON_CONDITION_LENGTH, tableElementJson, "condition");

                        if (tableElementJson.HasMember("tag")) {
                            element->tag = Ctx::getJsonFieldS(configFileName, Ctx::JSON_TAG_LENGTH, tableElementJson, "tag");
                            element->parseTag(element->tag, separator);
                        }
                    }
                }

                if (filterJson.HasMember("skip-xid")) {
                    const rapidjson::Value& skipXidArrayJson = Ctx::getJsonFieldA(configFileName, filterJson, "skip-xid");
                    for (rapidjson::SizeType k = 0; k < skipXidArrayJson.Size(); ++k) {
                        const Xid xid(Ctx::getJsonFieldS(configFileName, Ctx::JSON_XID_LENGTH, skipXidArrayJson, "skip-xid", k));
                        ctx->info(0, "adding XID to skip list: " + xid.toString());
                        transactionBuffer->skipXidList.insert(xid);
                    }
                }

                if (filterJson.HasMember("dump-xid")) {
                    const rapidjson::Value& dumpXidArrayJson = Ctx::getJsonFieldA(configFileName, filterJson, "dump-xid");
                    for (rapidjson::SizeType k = 0; k < dumpXidArrayJson.Size(); ++k) {
                        const Xid xid(Ctx::getJsonFieldS(configFileName, Ctx::JSON_XID_LENGTH, dumpXidArrayJson, "dump-xid", k));
                        ctx->info(0, "adding XID to dump list: " + xid.toString());
                        transactionBuffer->dumpXidList.insert(xid);
                    }
                }
            }

            if (readerJson.HasMember("log-archive-format")) {
                replicator->metadata->logArchiveFormatCustom = true;
                replicator->metadata->logArchiveFormat = Ctx::getJsonFieldS(configFileName, DbTable::VPARAMETER_LENGTH, readerJson, "log-archive-format");
            }

            metadata->commitElements();
            replicators.push_back(replicator);
            ctx->spawnThread(replicator);
            replicator = nullptr;
        }

        // Iterate through targets
        const rapidjson::Value& targetArrayJson = Ctx::getJsonFieldA(configFileName, document, "target");
        if (targetArrayJson.Size() != 1) {
            throw ConfigurationException(30001, "bad JSON, invalid \"target\" value: " + std::to_string(targetArrayJson.Size()) +
                                                " elements, expected: 1 element");
        }

        for (rapidjson::SizeType j = 0; j < targetArrayJson.Size(); ++j) {
            const rapidjson::Value& targetJson = targetArrayJson[j];
            const std::string alias = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, targetJson, "alias");
            const std::string source = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, targetJson, "source");

            ctx->info(0, "adding target: " + alias);
            Replicator* replicator2 = nullptr;
            for (Replicator* replicatorTmp: replicators)
                if (replicatorTmp->alias == source)
                    replicator2 = replicatorTmp;
            if (replicator2 == nullptr)
                throw ConfigurationException(30001, "bad JSON, invalid \"source\" value: " + source + ", expected: value used earlier in \"source\" field");

            // Writer
            Writer* writer;
            const rapidjson::Value& writerJson = Ctx::getJsonFieldO(configFileName, targetJson, "writer");
            const std::string writerType = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, writerJson, "type");

            if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> writerNames {"type", "poll-interval-us", "queue-size", "max-file-size", "timestamp-format", "output",
                                                                   "new-line", "append", "max-message-mb", "topic", "properties", "uri",
                                                                   "write-buffer-flush-size"};
                Ctx::checkJsonFields(configFileName, writerJson, writerNames);
            }

            if (writerJson.HasMember("poll-interval-us")) {
                ctx->pollIntervalUs = Ctx::getJsonFieldU64(configFileName, writerJson, "poll-interval-us");
                if (ctx->pollIntervalUs < 100 || ctx->pollIntervalUs > 3600000000)
                    throw ConfigurationException(30001, "bad JSON, invalid \"poll-interval-us\" value: " +
                                                        std::to_string(ctx->pollIntervalUs) + ", expected: one of {100 .. 3600000000}");
            }

            if (writerJson.HasMember("queue-size")) {
                ctx->queueSize = Ctx::getJsonFieldU64(configFileName, writerJson, "queue-size");
                if (ctx->queueSize < 1 || ctx->queueSize > 1000000)
                    throw ConfigurationException(30001, "bad JSON, invalid \"queue-size\" value: " + std::to_string(ctx->queueSize) +
                                                        ", expected: one of {1 .. 1000000}");
            }

            if (writerType == "file") {
                uint64_t maxFileSize = 0;
                if (writerJson.HasMember("max-file-size"))
                    maxFileSize = Ctx::getJsonFieldU64(configFileName, writerJson, "max-file-size");

                std::string timestampFormat = "%F_%T";
                if (writerJson.HasMember("timestamp-format"))
                    timestampFormat = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, writerJson, "timestamp-format");

                std::string output;
                if (writerJson.HasMember("output"))
                    output = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, writerJson, "output");
                else if (maxFileSize > 0)
                    throw ConfigurationException(30001, "bad JSON, invalid \"output\" value: " + output +
                                                        ", expected: to be set when \"max-file-size\" is set (" + std::to_string(maxFileSize) + ")");

                uint64_t newLine = 1;
                if (writerJson.HasMember("new-line")) {
                    newLine = Ctx::getJsonFieldU64(configFileName, writerJson, "new-line");
                    if (newLine > 2)
                        throw ConfigurationException(30001, "bad JSON, invalid \"new-line\" value: " + std::to_string(newLine) + ", expected: one of {0 .. 2}");
                }

                uint64_t append = 1;
                if (writerJson.HasMember("append")) {
                    append = Ctx::getJsonFieldU64(configFileName, writerJson, "append");
                    if (append > 1)
                        throw ConfigurationException(30001, "bad JSON, invalid \"append\" value: " + std::to_string(append) + ", expected: one of {0, 1}");
                }

                uint writeBufferFlushSize = 1048576;
                if (writerJson.HasMember("write-buffer-flush-size")) {
                    writeBufferFlushSize = Ctx::getJsonFieldU(configFileName, writerJson, "write-buffer-flush-size");
                    if (writeBufferFlushSize > 1048576)
                        throw ConfigurationException(30001, "bad JSON, invalid \"write-buffer-flush-size\" value: " +
                                                            std::to_string(writeBufferFlushSize) + ", expected: one of {0 .. 1048576}");
                }

                writer = new WriterFile(ctx, alias + "-writer", replicator2->database, replicator2->builder, replicator2->metadata, output, timestampFormat,
                                        maxFileSize, newLine, append, writeBufferFlushSize);
            } else if (writerType == "discard") {
                writer = new WriterDiscard(ctx, alias + "-writer", replicator2->database, replicator2->builder, replicator2->metadata);
            } else if (writerType == "kafka") {
#ifdef LINK_LIBRARY_RDKAFKA
                uint64_t maxMessageMb = 100;
                if (writerJson.HasMember("max-message-mb")) {
                    maxMessageMb = Ctx::getJsonFieldU64(configFileName, writerJson, "max-message-mb");
                    if (maxMessageMb < 1 || maxMessageMb > WriterKafka::MAX_KAFKA_MESSAGE_MB)
                        throw ConfigurationException(30001, "bad JSON, invalid \"max-message-mb\" value: " + std::to_string(maxMessageMb) +
                                                            ", expected: one of {1 .. " + std::to_string(WriterKafka::MAX_KAFKA_MESSAGE_MB) + "}");
                }
                replicator2->builder->setMaxMessageMb(maxMessageMb);

                const std::string topic = Ctx::getJsonFieldS(configFileName, Ctx::JSON_TOPIC_LENGTH, writerJson, "topic");

                writer = new WriterKafka(ctx, alias + "-writer", replicator2->database, replicator2->builder, replicator2->metadata, topic);

                if (writerJson.HasMember("properties")) {
                    const rapidjson::Value& propertiesJson = Ctx::getJsonFieldO(configFileName, writerJson, "properties");

                    for (rapidjson::Value::ConstMemberIterator itr = propertiesJson.MemberBegin(); itr != propertiesJson.MemberEnd(); ++itr) {
                        const std::string key = itr->name.GetString();
                        const std::string value = itr->value.GetString();
                        reinterpret_cast<WriterKafka*>(writer)->addProperty(key, value);
                    }
                }
#else
                throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: " + writerType +
                                             ", expected: not \"kafka\" since the code is not compiled");
#endif /* LINK_LIBRARY_RDKAFKA */
            } else if (writerType == "zeromq") {
#if defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ)
                const std::string uri = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, writerJson, "uri");
                auto* stream = new StreamZeroMQ(ctx, uri);
                stream->initialize();
                writer = new WriterStream(ctx, alias + "-writer", replicator2->database, replicator2->builder, replicator2->metadata, stream);
#else
                throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: " + writerType +
                                             ", expected: not \"zeromq\" since the code is not compiled");
#endif /* defined(LINK_LIBRARY_PROTOBUF) && defined(LINK_LIBRARY_ZEROMQ) */
            } else if (writerType == "network") {
#ifdef LINK_LIBRARY_PROTOBUF
                const std::string uri = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, writerJson, "uri");

                auto* stream = new StreamNetwork(ctx, uri);
                stream->initialize();
                writer = new WriterStream(ctx, alias + "-writer", replicator2->database, replicator2->builder, replicator2->metadata, stream);
#else
                throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: " + writerType +
                                             ", expected: not \"network\" since the code is not compiled");
#endif /* LINK_LIBRARY_PROTOBUF */
            } else
                throw ConfigurationException(30001, "bad JSON, invalid \"type\" value: " + writerType +
                                                    R"(, expected: one of {"file", "kafka", "zeromq", "network", "discard"})");

            writers.push_back(writer);
            writer->initialize();
            ctx->spawnThread(writer);
        }

        ctx->mainLoop();

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "main (" + ss.str() + ") stop");
        }

        return 0;
    }

    void OpenLogReplicator::mainProcessMapping(const rapidjson::Value& readerJson) {
        if (readerJson.HasMember("path-mapping")) {
            const rapidjson::Value& pathMappingArrayJson = Ctx::getJsonFieldA(configFileName, readerJson, "path-mapping");

            if ((pathMappingArrayJson.Size() % 2) != 0)
                throw ConfigurationException(30001, "bad JSON, invalid \"path-mapping\" value: " +
                                                    std::to_string(pathMappingArrayJson.Size()) + " elements, expected: even number of elements");

            for (rapidjson::SizeType k = 0; k < pathMappingArrayJson.Size() / 2; ++k) {
                const std::string sourceMapping = Ctx::getJsonFieldS(configFileName, Ctx::MAX_PATH_LENGTH, pathMappingArrayJson, "path-mapping", k * 2);
                const std::string targetMapping = Ctx::getJsonFieldS(configFileName, Ctx::MAX_PATH_LENGTH, pathMappingArrayJson, "path-mapping", (k * 2) + 1);
                replicator->addPathMapping(sourceMapping, targetMapping);
            }
        }
    }
}
