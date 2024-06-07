/* Metrics with Prometheus
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

#include "MetricsPrometheus.h"
#include "../Ctx.h"

namespace OpenLogReplicator {
    MetricsPrometheus::MetricsPrometheus(uint64_t newTagNames, const char* newBind) :
            Metrics(newTagNames),
            bind(newBind),
            exposer(nullptr),
            bytesConfirmed(nullptr),
            bytesConfirmedCounter(nullptr),
            bytesParsed(nullptr),
            bytesParsedCounter(nullptr),
            bytesRead(nullptr),
            bytesReadCounter(nullptr),
            bytesSent(nullptr),
            bytesSentCounter(nullptr),
            checkpoints(nullptr),
            checkpointsOutCounter(nullptr),
            checkpointsSkipCounter(nullptr),
            checkpointLag(nullptr),
            checkpointLagGauge(nullptr),
            ddlOps(nullptr),
            ddlOpsAlterCounter(nullptr),
            ddlOpsCreateCounter(nullptr),
            ddlOpsDropCounter(nullptr),
            ddlOpsOtherCounter(nullptr),
            ddlOpsPurgeCounter(nullptr),
            ddlOpsTruncateCounter(nullptr),
            dmlOps(nullptr),
            dmlOpsDeleteOutCounter(nullptr),
            dmlOpsInsertOutCounter(nullptr),
            dmlOpsUpdateOutCounter(nullptr),
            dmlOpsDeleteSkipCounter(nullptr),
            dmlOpsInsertSkipCounter(nullptr),
            dmlOpsUpdateSkipCounter(nullptr),
            logSwitches(nullptr),
            logSwitchesOnlineCounter(nullptr),
            logSwitchesArchivedCounter(nullptr),
            logSwitchesLag(nullptr),
            logSwitchesLagOnlineGauge(nullptr),
            logSwitchesLagArchivedGauge(nullptr),
            memoryAllocatedMb(nullptr),
            memoryAllocatedMbGauge(nullptr),
            memoryUsedTotalMb(nullptr),
            memoryUsedTotalMbGauge(nullptr),
            memoryUsedMb(nullptr),
            memoryUsedMbBuilderGauge(nullptr),
            memoryUsedMbParserGauge(nullptr),
            memoryUsedMbReaderGauge(nullptr),
            memoryUsedMbTransactionsGauge(nullptr),
            messagesConfirmed(nullptr),
            messagesConfirmedCounter(nullptr),
            messagesSent(nullptr),
            messagesSentCounter(nullptr),
            transactions(nullptr),
            transactionsCommitOutCounter(nullptr),
            transactionsRollbackOutCounter(nullptr),
            transactionsCommitPartialCounter(nullptr),
            transactionsRollbackPartialCounter(nullptr),
            transactionsCommitSkipCounter(nullptr),
            transactionsRollbackSkipCounter(nullptr) {
    }

    MetricsPrometheus::~MetricsPrometheus() {
        if (exposer != nullptr) {
            delete exposer;
            exposer = nullptr;
        }
    }

    void MetricsPrometheus::initialize(const Ctx* ctx) {
        ctx->info(0, "starting Prometheus metrics, listening on: " + bind);
        exposer = new prometheus::Exposer(bind);
        registry = std::make_shared<prometheus::Registry>();

        // bytes_confirmed
        bytesConfirmed = &prometheus::BuildCounter().Name("bytes_confirmed").Help("Number of bytes confirmed by output").Register(*registry);
        bytesConfirmedCounter = &bytesConfirmed->Add({});

        // bytes_parsed
        bytesParsed = &prometheus::BuildCounter().Name("bytes_parsed").Help("Number of bytes parsed containing redo log data").Register(*registry);
        bytesParsedCounter = &bytesParsed->Add({});

        // bytes_read
        bytesRead = &prometheus::BuildCounter().Name("bytes_read").Help("Number of bytes read from redo log files").Register(*registry);
        bytesReadCounter = &bytesRead->Add({});

        // bytes_sent
        bytesSent = &prometheus::BuildCounter().Name("bytes_sent").Help("Number of bytes sent to output (for example to Kafka or network writer)")
                .Register(*registry);
        bytesSentCounter = &bytesSent->Add({});

        // checkpoints
        checkpoints = &prometheus::BuildCounter().Name("checkpoints").Help("Number of checkpoint records").Register(*registry);
        checkpointsOutCounter = &checkpoints->Add({{"filter", "out"}});
        checkpointsSkipCounter = &checkpoints->Add({{"filter", "skip"}});

        // checkpoint_lag
        checkpointLag = &prometheus::BuildGauge().Name("checkpoint_lag").Help("Checkpoint processing lag in seconds").Register(*registry);
        checkpointLagGauge = &checkpointLag->Add({});

        // ddl_ops
        ddlOps = &prometheus::BuildCounter().Name("ddl_ops").Help("Number of DDL operations").Register(*registry);
        ddlOpsAlterCounter = &ddlOps->Add({{"type", "alter"}});
        ddlOpsCreateCounter = &ddlOps->Add({{"type", "create"}});
        ddlOpsDropCounter = &ddlOps->Add({{"type", "drop"}});
        ddlOpsOtherCounter = &ddlOps->Add({{"type", "other"}});
        ddlOpsPurgeCounter = &ddlOps->Add({{"type", "purge"}});
        ddlOpsTruncateCounter = &ddlOps->Add({{"type", "truncate"}});

        // dml_ops
        dmlOps = &prometheus::BuildCounter().Name("dml_ops").Help("Number of DML operations").Register(*registry);
        dmlOpsDeleteOutCounter = &dmlOps->Add({{"type",   "delete"},
                                               {"filter", "out"}});
        dmlOpsInsertOutCounter = &dmlOps->Add({{"type",   "insert"},
                                               {"filter", "out"}});
        dmlOpsUpdateOutCounter = &dmlOps->Add({{"type",   "update"},
                                               {"filter", "out"}});
        dmlOpsDeleteSkipCounter = &dmlOps->Add({{"type",   "delete"},
                                                {"filter", "skip"}});
        dmlOpsInsertSkipCounter = &dmlOps->Add({{"type",   "insert"},
                                                {"filter", "skip"}});
        dmlOpsUpdateSkipCounter = &dmlOps->Add({{"type",   "update"},
                                                {"filter", "skip"}});

        // log_switches
        logSwitches = &prometheus::BuildCounter().Name("log_switches").Help("Number of redo log switches").Register(*registry);
        logSwitchesOnlineCounter = &logSwitches->Add({{"type", "online"}});
        logSwitchesArchivedCounter = &logSwitches->Add({{"type", "archived"}});

        // log_switches_lag
        logSwitchesLag = &prometheus::BuildGauge().Name("log_switches_lag").Help("Redo log file processing lag in seconds").Register(*registry);
        logSwitchesLagOnlineGauge = &logSwitchesLag->Add({{"type", "online"}});
        logSwitchesLagArchivedGauge = &logSwitchesLag->Add({{"type", "archived"}});

        // messages_confirmed
        messagesConfirmed = &prometheus::BuildCounter().Name("messages_confirmed").Help("Number of messages confirmed by output").Register(*registry);
        messagesConfirmedCounter = &messagesConfirmed->Add({});

        // memory_allocated_mb
        memoryAllocatedMb = &prometheus::BuildGauge().Name("memory_allocated_mb").Help("Amount of allocated memory in MB").Register(*registry);
        memoryAllocatedMbGauge = &memoryAllocatedMb->Add({});

        // memory_used_total_mb
        memoryUsedTotalMb = &prometheus::BuildGauge().Name("memory_used_total_mb").Help("Total used memory").Register(*registry);
        memoryUsedTotalMbGauge = &memoryUsedTotalMb->Add({});

        // memory_used_mb
        memoryUsedMb = &prometheus::BuildGauge().Name("memory_used_mb").Help("Memory used by module: builder").Register(*registry);
        memoryUsedMbBuilderGauge = &memoryUsedMb->Add({{"type", "builder"}});
        memoryUsedMbParserGauge = &memoryUsedMb->Add({{"type", "parser"}});
        memoryUsedMbReaderGauge = &memoryUsedMb->Add({{"type", "reader"}});
        memoryUsedMbTransactionsGauge = &memoryUsedMb->Add({{"type", "transactions"}});

        // messages_sent
        messagesSent = &prometheus::BuildCounter().Name("messages_sent").Help("Number of messages sent to output (for example to Kafka or network writer)")
                .Register(*registry);
        messagesSentCounter = &messagesSent->Add({});

        // transactions
        transactions = &prometheus::BuildCounter().Name("dml_ops").Help("Number of transactions").Register(*registry);
        transactionsCommitOutCounter = &transactions->Add({{"type",   "commit"},
                                                           {"filter", "out"}});
        transactionsRollbackOutCounter = &transactions->Add({{"type",   "rollback"},
                                                             {"filter", "out"}});
        transactionsCommitPartialCounter = &transactions->Add({{"type",   "commit"},
                                                               {"filter", "partial"}});
        transactionsRollbackPartialCounter = &transactions->Add({{"type",   "rollback"},
                                                                 {"filter", "partial"}});
        transactionsCommitSkipCounter = &transactions->Add({{"type",   "commit"},
                                                            {"filter", "skip"}});
        transactionsRollbackSkipCounter = &transactions->Add({{"type",   "rollback"},
                                                              {"filter", "skip"}});

        exposer->RegisterCollectable(registry);
    }

    void MetricsPrometheus::shutdown() {
    }

    // bytes_confirmed
    void MetricsPrometheus::emitBytesConfirmed(uint64_t counter) {
        bytesConfirmedCounter->Increment(counter);
    }

    // bytes_parsed
    void MetricsPrometheus::emitBytesParsed(uint64_t counter) {
        bytesParsedCounter->Increment(counter);
    }

    // bytes_read
    void MetricsPrometheus::emitBytesRead(uint64_t counter) {
        bytesReadCounter->Increment(counter);
    }

    // bytes_sent
    void MetricsPrometheus::emitBytesSent(uint64_t counter) {
        bytesSentCounter->Increment(counter);
    }

    // checkpoints
    void MetricsPrometheus::emitCheckpointsOut(uint64_t counter) {
        checkpointsOutCounter->Increment(counter);
    }

    void MetricsPrometheus::emitCheckpointsSkip(uint64_t counter) {
        checkpointsSkipCounter->Increment(counter);
    }

    // checkpoint_lag
    void MetricsPrometheus::emitCheckpointLag(int64_t gauge) {
        checkpointLagGauge->Set(gauge);
    }

    // ddl_ops
    void MetricsPrometheus::emitDdlOpsAlter(uint64_t counter) {
        ddlOpsAlterCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDdlOpsCreate(uint64_t counter) {
        ddlOpsCreateCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDdlOpsDrop(uint64_t counter) {
        ddlOpsDropCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDdlOpsOther(uint64_t counter) {
        ddlOpsOtherCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDdlOpsPurge(uint64_t counter) {
        ddlOpsPurgeCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDdlOpsTruncate(uint64_t counter) {
        ddlOpsTruncateCounter->Increment(counter);
    }

    // dml_ops
    void MetricsPrometheus::emitDmlOpsDeleteOut(uint64_t counter) {
        dmlOpsDeleteOutCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsInsertOut(uint64_t counter) {
        dmlOpsInsertOutCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsUpdateOut(uint64_t counter) {
        dmlOpsUpdateOutCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsDeleteSkip(uint64_t counter) {
        dmlOpsDeleteSkipCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsInsertSkip(uint64_t counter) {
        dmlOpsInsertSkipCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsUpdateSkip(uint64_t counter) {
        dmlOpsUpdateSkipCounter->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsDeleteOut(uint64_t counter, const std::string& owner, const std::string& table) {
        std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        auto iter = dmlOpsDeleteOutCounterMap.find(key);

        if (iter != dmlOpsDeleteOutCounterMap.end())
            cnt = iter->second;
        else
            cnt = &dmlOps->Add({{"type",   "delete"},
                                {"filter", "out"},
                                {"owner",  owner},
                                {"table",  table}});

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsInsertOut(uint64_t counter, const std::string& owner, const std::string& table) {
        std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        auto iter = dmlOpsInsertOutCounterMap.find(key);

        if (iter != dmlOpsInsertOutCounterMap.end())
            cnt = iter->second;
        else
            cnt = &dmlOps->Add({{"type",   "insert"},
                                {"filter", "out"},
                                {"owner",  owner},
                                {"table",  table}});

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsUpdateOut(uint64_t counter, const std::string& owner, const std::string& table) {
        std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        auto iter = dmlOpsUpdateOutCounterMap.find(key);

        if (iter != dmlOpsUpdateOutCounterMap.end())
            cnt = iter->second;
        else
            cnt = &dmlOps->Add({{"type",   "update"},
                                {"filter", "out"},
                                {"owner",  owner},
                                {"table",  table}});

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsDeleteSkip(uint64_t counter, const std::string& owner, const std::string& table) {
        std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        auto iter = dmlOpsDeleteSkipCounterMap.find(key);

        if (iter != dmlOpsDeleteSkipCounterMap.end())
            cnt = iter->second;
        else
            cnt = &dmlOps->Add({{"type",   "delete"},
                                {"filter", "skip"},
                                {"owner",  owner},
                                {"table",  table}});

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsInsertSkip(uint64_t counter, const std::string& owner, const std::string& table) {
        std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        auto iter = dmlOpsInsertSkipCounterMap.find(key);

        if (iter != dmlOpsInsertSkipCounterMap.end())
            cnt = iter->second;
        else
            cnt = &dmlOps->Add({{"type",   "insert"},
                                {"filter", "skip"},
                                {"owner",  owner},
                                {"table",  table}});

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsUpdateSkip(uint64_t counter, const std::string& owner, const std::string& table) {
        std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        auto iter = dmlOpsUpdateSkipCounterMap.find(key);

        if (iter != dmlOpsUpdateSkipCounterMap.end())
            cnt = iter->second;
        else
            cnt = &dmlOps->Add({{"type",   "update"},
                                {"filter", "skip"},
                                {"owner",  owner},
                                {"table",  table}});

        cnt->Increment(counter);
    }

    // log_switches
    void MetricsPrometheus::emitLogSwitchesArchived(uint64_t counter) {
        logSwitchesArchivedCounter->Increment(counter);
    }

    void MetricsPrometheus::emitLogSwitchesOnline(uint64_t counter) {
        logSwitchesOnlineCounter->Increment(counter);
    }

    // log_switches_lag
    void MetricsPrometheus::emitLogSwitchesLagArchived(int64_t gauge) {
        logSwitchesLagArchivedGauge->Set(gauge);
    }

    void MetricsPrometheus::emitLogSwitchesLagOnline(int64_t gauge) {
        logSwitchesLagOnlineGauge->Set(gauge);
    }

    // memory_allocated_mb
    void MetricsPrometheus::emitMemoryAllocatedMb(int64_t gauge) {
        memoryAllocatedMbGauge->Set(gauge);
    }

    // memory_used_total_mb
    void MetricsPrometheus::emitMemoryUsedTotalMb(int64_t gauge) {
        memoryUsedTotalMbGauge->Set(gauge);
    }

    // memory_used_mb
    void MetricsPrometheus::emitMemoryUsedMbBuilder(int64_t gauge) {
        memoryUsedMbBuilderGauge->Set(gauge);
    }

    void MetricsPrometheus::emitMemoryUsedMbParser(int64_t gauge) {
        memoryUsedMbParserGauge->Set(gauge);
    }

    void MetricsPrometheus::emitMemoryUsedMbReader(int64_t gauge) {
        memoryUsedMbReaderGauge->Set(gauge);
    }

    void MetricsPrometheus::emitMemoryUsedMbTransactions(int64_t gauge) {
        memoryUsedMbTransactionsGauge->Set(gauge);
    }

    // messages_confirmed
    void MetricsPrometheus::emitMessagesConfirmed(uint64_t counter) {
        messagesConfirmedCounter->Increment(counter);
    }

    // messages_sent
    void MetricsPrometheus::emitMessagesSent(uint64_t counter) {
        messagesSentCounter->Increment(counter);
    }

    // transactions
    void MetricsPrometheus::emitTransactionsCommitOut(uint64_t counter) {
        transactionsCommitOutCounter->Increment(counter);
    }

    void MetricsPrometheus::emitTransactionsRollbackOut(uint64_t counter) {
        transactionsRollbackOutCounter->Increment(counter);
    }

    void MetricsPrometheus::emitTransactionsCommitPartial(uint64_t counter) {
        transactionsCommitPartialCounter->Increment(counter);
    }

    void MetricsPrometheus::emitTransactionsRollbackPartial(uint64_t counter) {
        transactionsRollbackPartialCounter->Increment(counter);
    }

    void MetricsPrometheus::emitTransactionsCommitSkip(uint64_t counter) {
        transactionsCommitSkipCounter->Increment(counter);
    }

    void MetricsPrometheus::emitTransactionsRollbackSkip(uint64_t counter) {
        transactionsRollbackSkipCounter->Increment(counter);
    }
}
