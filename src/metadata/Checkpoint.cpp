/* Base class for process to write checkpoint files
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

#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <thread>
#include <utility>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/exception/ConfigurationException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/DbTable.h"
#include "../common/table/SysObj.h"
#include "../common/table/SysUser.h"
#include "Checkpoint.h"
#include "Metadata.h"
#include "Schema.h"
#include "SchemaElement.h"

namespace OpenLogReplicator {
    Checkpoint::Checkpoint(Ctx* newCtx, Metadata* newMetadata, std::string newAlias, std::string newConfigFileName, time_t newConfigFileChange):
            Thread(newCtx, std::move(newAlias)),
            metadata(newMetadata),
            configFileName(std::move(newConfigFileName)),
            configFileChange(newConfigFileChange) {}

    Checkpoint::~Checkpoint() {
        delete[] configFileBuffer;
        configFileBuffer = nullptr;
    }

    void Checkpoint::wakeUp() {
        {
            contextSet(CONTEXT::MUTEX, REASON::CHECKPOINT_WAKEUP);
            std::unique_lock const lck(mtx);
            condLoop.notify_all();
        }
        contextSet(CONTEXT::CPU);
    }

    void Checkpoint::trackConfigFile() {
        struct stat configFileStat{};
        if (unlikely(stat(configFileName.c_str(), &configFileStat) != 0))
            throw RuntimeException(10003, "file: " + configFileName + " - get metadata returned: " + strerror(errno));

        if (configFileStat.st_mtime == configFileChange)
            return;

        ctx->info(0, "config file changed, reloading");

        try {
            const int fid = open(configFileName.c_str(), O_RDONLY);
            if (unlikely(fid == -1))
                throw ConfigurationException(10001, "file: " + configFileName + " - open for read returned: " + strerror(errno));

            if (unlikely(configFileStat.st_size > CONFIG_FILE_MAX_SIZE || configFileStat.st_size == 0))
                throw ConfigurationException(10004, "file: " + configFileName + " - wrong size: " +
                                             std::to_string(configFileStat.st_size));

            delete[] configFileBuffer;

            configFileBuffer = new char[configFileStat.st_size + 1];
            const uint64_t bytesRead = read(fid, configFileBuffer, configFileStat.st_size);
            if (unlikely(bytesRead != static_cast<uint64_t>(configFileStat.st_size)))
                throw ConfigurationException(10005, "file: " + configFileName + " - " + std::to_string(bytesRead) +
                                             " bytes read instead of " + std::to_string(configFileStat.st_size));
            configFileBuffer[configFileStat.st_size] = 0;

            updateConfigFile();

            delete[] configFileBuffer;
            configFileBuffer = nullptr;
        } catch (ConfigurationException& ex) {
            ctx->error(ex.code, ex.msg);
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
        }

        configFileChange = configFileStat.st_mtime;
    }

    void Checkpoint::updateConfigFile() {
        rapidjson::Document document;
        if (unlikely(document.Parse(configFileBuffer).HasParseError()))
            throw ConfigurationException(20001, "file: " + configFileName + " offset: " + std::to_string(document.GetErrorOffset()) +
                                         " - parse error: " + GetParseError_En(document.GetParseError()));

        if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
            static const std::vector<std::string> documentNames{
                "dump-path",
                "dump-raw-data",
                "dump-redo-log",
                "log-level",
                "memory",
                "metrics",
                "source",
                "state",
                "target",
                "trace",
                "version"
            };
            Ctx::checkJsonFields(configFileName, document, documentNames);
        }

        const std::string version = Ctx::getJsonFieldS(configFileName, Ctx::JSON_PARAMETER_LENGTH, document, "version");
        if (unlikely(version != OpenLogReplicator_SCHEMA_VERSION))
            throw ConfigurationException(30001, "bad JSON, invalid 'version' value: " + version + ", expected: " + OpenLogReplicator_SCHEMA_VERSION);

        // Iterate through sources
        const rapidjson::Value& sourceArrayJson = Ctx::getJsonFieldA(configFileName, document, "source");
        if (unlikely(sourceArrayJson.Size() != 1)) {
            throw ConfigurationException(30001, "bad JSON, invalid 'source' value: " + std::to_string(sourceArrayJson.Size()) +
                                         " elements, expected: 1 element");
        }

        for (rapidjson::SizeType j = 0; j < sourceArrayJson.Size(); ++j) {
            const rapidjson::Value& sourceJson = Ctx::getJsonFieldO(configFileName, sourceArrayJson, "source", j);

            if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                static const std::vector<std::string> sourceNames{
                    "alias",
                    "arch",
                    "arch-read-sleep-us",
                    "arch-read-tries",
                    "debug",
                    "filter",
                    "flags",
                    "format",
                    "name",
                    "reader",
                    "redo-read-sleep-us",
                    "redo-verify-delay-us",
                    "refresh-interval-us",
                    "transaction-max-mb"
                };
                Ctx::checkJsonFields(configFileName, sourceJson, sourceNames);
            }

            metadata->resetElements();

            std::string debugOwner;
            std::string debugTable;

            if (sourceJson.HasMember("debug")) {
                const rapidjson::Value& debugJson = Ctx::getJsonFieldO(configFileName, sourceJson, "debug");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> debugNames{
                        "owner",
                        "stop-checkpoints",
                        "stop-log-switches",
                        "stop-transactions",
                        "table"
                    };
                    Ctx::checkJsonFields(configFileName, debugJson, debugNames);
                }

                if (!ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS) && (debugJson.HasMember("owner") || debugJson.HasMember("table"))) {
                    debugOwner = Ctx::getJsonFieldS(configFileName, SysUser::NAME_LENGTH, debugJson, "owner");
                    debugTable = Ctx::getJsonFieldS(configFileName, SysObj::NAME_LENGTH, debugJson, "table");
                    ctx->info(0, "will shutdown after committed DML in " + debugOwner + "." + debugTable);
                }
            }

            std::set<std::string> users;
            if (!debugOwner.empty() && !debugTable.empty()) {
                metadata->addElement(debugOwner, debugTable, DbTable::OPTIONS::DEBUG_TABLE);
                users.insert(debugOwner);
            }
            if (ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA))
                metadata->addElement(".*", ".*", DbTable::OPTIONS::DEFAULT);

            if (sourceJson.HasMember("filter")) {
                const rapidjson::Value& filterJson = Ctx::getJsonFieldO(configFileName, sourceJson, "filter");

                if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
                    static const std::vector<std::string> filterNames{
                        "dump-xid",
                        "separator",
                        "skip-xid",
                        "table"
                    };
                    Ctx::checkJsonFields(configFileName, filterJson, filterNames);
                }

                if (filterJson.HasMember("table") && !ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
                    const rapidjson::Value& tableArrayJson = Ctx::getJsonFieldA(configFileName, filterJson, "table");

                    std::string separator{","};
                    if (filterJson.HasMember("separator"))
                        separator = Ctx::getJsonFieldS(configFileName, Ctx::JSON_FORMAT_SEPARATOR_LENGTH, filterJson, "separator");

                    for (rapidjson::SizeType k = 0; k < tableArrayJson.Size(); ++k) {
                        const rapidjson::Value& tableElementJson = Ctx::getJsonFieldO(configFileName, tableArrayJson, "table", k);

                        const std::string owner = Ctx::getJsonFieldS(configFileName, SysUser::NAME_LENGTH, tableElementJson, "owner");
                        const std::string table = Ctx::getJsonFieldS(configFileName, SysObj::NAME_LENGTH, tableElementJson, "table");
                        SchemaElement* element = metadata->addElement(owner, table, DbTable::OPTIONS::DEFAULT);

                        users.insert(owner);

                        if (tableElementJson.HasMember("key")) {
                            element->key = Ctx::getJsonFieldS(configFileName, Ctx::JSON_KEY_LENGTH, tableElementJson, "key");
                            element->parseKey(element->key, separator);
                        }

                        if (tableElementJson.HasMember("condition"))
                            element->condition = Ctx::getJsonFieldS(configFileName, Ctx::JSON_CONDITION_LENGTH, tableElementJson,
                                                                    "condition");

                        if (tableElementJson.HasMember("tag")) {
                            element->tag = Ctx::getJsonFieldS(configFileName, Ctx::JSON_TAG_LENGTH, tableElementJson, "tag");
                            element->parseTag(element->tag, separator);
                        }
                    }

                    for (const auto& user: metadata->users)
                        if (unlikely(users.find(user) == users.end()))
                            throw ConfigurationException(20007, "file: " + configFileName + " - " + user + " is missing");

                    for (const auto& user: users)
                        if (unlikely(metadata->users.find(user) == metadata->users.end()))
                            throw ConfigurationException(20007, "file: " + configFileName + " - " + user + " is redundant");

                    users.clear();
                }
            }
        }

        ctx->info(0, "scanning objects which match the configuration file");
        // Suspend transaction processing for the schema update
        {
            contextSet(CONTEXT::TRAN, REASON::TRAN);
            std::unique_lock const lckTransaction(metadata->mtxTransaction);
            metadata->commitElements();
            metadata->schema->purgeMetadata();

            // Mark all tables as touched to force a schema update
            for (const auto& [_, sysObj]: metadata->schema->sysObjPack.mapRowId)
                metadata->schema->touchTable(sysObj->obj);

            std::vector<std::string> msgs;
            std::unordered_map<typeObj, std::string> tablesUpdated;
            metadata->buildMaps(msgs, tablesUpdated);
            for (const auto& msg: msgs)
                ctx->info(0, msg);
            for (const auto& [_, tableName]: tablesUpdated)
                ctx->info(0, "- found: " + tableName);

            metadata->schema->resetTouched();
        }
        contextSet(CONTEXT::CPU);
    }

    void Checkpoint::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "checkpoint (" + ss.str() + ") start");
        }

        try {
            while (!ctx->hardShutdown) {
                metadata->writeCheckpoint(this, false);
                metadata->deleteOldCheckpoints(this);

                if (ctx->hardShutdown)
                    break;

                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;

                trackConfigFile();

                {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                        ctx->logTrace(Ctx::TRACE::SLEEP, "Checkpoint:run lastCheckpointScn: " + metadata->lastCheckpointScn.toString() +
                                      " checkpointScn: " + metadata->checkpointScn.toString());

                    contextSet(CONTEXT::MUTEX, REASON::CHECKPOINT_RUN);
                    std::unique_lock lck(mtx);
                    contextSet(CONTEXT::WAIT, REASON::CHECKPOINT_NO_WORK);
                    condLoop.wait_for(lck, std::chrono::milliseconds(100));
                }
                contextSet(CONTEXT::CPU);
            }

            if (ctx->softShutdown)
                metadata->writeCheckpoint(this, true);
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "checkpoint (" + ss.str() + ") stop");
        }
    }
}
