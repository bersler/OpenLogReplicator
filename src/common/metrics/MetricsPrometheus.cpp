/* Metrics with Prometheus
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

#include "MetricsPrometheus.h"
#include "../Ctx.h"

namespace OpenLogReplicator {
    MetricsPrometheus::MetricsPrometheus(TAG_NAMES newTagNames, std::string newBind):
            Metrics(newTagNames),
            bind(std::move(newBind)) {}

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
        bytesConfirmed = &prometheus::BuildCounter().Name("bytes_confirmed")
                                                    .Help("Number of bytes confirmed by output")
                                                    .Register(*registry);
        bytesConfirmedCounter = &bytesConfirmed->Add({});

        // bytes_parsed
        bytesParsed = &prometheus::BuildCounter().Name("bytes_parsed")
                                                 .Help("Number of bytes parsed containing redo log data")
                                                 .Register(*registry);
        bytesParsedCounter = &bytesParsed->Add({});

        // bytes_read
        bytesRead = &prometheus::BuildCounter().Name("bytes_read")
                                               .Help("Number of bytes read from redo log files")
                                               .Register(*registry);
        bytesReadCounter = &bytesRead->Add({});

        // bytes_sent
        bytesSent = &prometheus::BuildCounter().Name("bytes_sent")
                                               .Help("Number of bytes sent to output (for example to Kafka or network writer)")
                                               .Register(*registry);
        bytesSentCounter = &bytesSent->Add({});

        // checkpoints
        checkpoints = &prometheus::BuildCounter().Name("checkpoints")
                                                 .Help("Number of checkpoint records")
                                                 .Register(*registry);
        checkpointsOutCounter = &checkpoints->Add({
            {"filter", "out"}
        });
        checkpointsSkipCounter = &checkpoints->Add({
            {"filter", "skip"}
        });

        // checkpoint_lag
        checkpointLag = &prometheus::BuildGauge().Name("checkpoint_lag")
                                                 .Help("Checkpoint processing lag in seconds")
                                                 .Register(*registry);
        checkpointLagGauge = &checkpointLag->Add({});

        // ddl_ops
        ddlOps = &prometheus::BuildCounter().Name("ddl_ops")
                                            .Help("Number of DDL operations")
                                            .Register(*registry);
        ddlOpsAlterCounter = &ddlOps->Add({
            {"type", "alter"}
        });
        ddlOpsCreateCounter = &ddlOps->Add({
            {"type", "create"}
        });
        ddlOpsDropCounter = &ddlOps->Add({
            {"type", "drop"}
        });
        ddlOpsOtherCounter = &ddlOps->Add({
            {"type", "other"}
        });
        ddlOpsPurgeCounter = &ddlOps->Add({
            {"type", "purge"}
        });
        ddlOpsTruncateCounter = &ddlOps->Add({
            {"type", "truncate"}
        });

        // dml_ops
        dmlOps = &prometheus::BuildCounter().Name("dml_ops")
                                            .Help("Number of DML operations")
                                            .Register(*registry);
        dmlOpsDeleteOutCounter = &dmlOps->Add({
            {"type", "delete"},
            {"filter", "out"}
        });
        dmlOpsInsertOutCounter = &dmlOps->Add({
            {"type", "insert"},
            {"filter", "out"}
        });
        dmlOpsUpdateOutCounter = &dmlOps->Add({
            {"type", "update"},
            {"filter", "out"}
        });
        dmlOpsDeleteSkipCounter = &dmlOps->Add({
            {"type", "delete"},
            {"filter", "skip"}
        });
        dmlOpsInsertSkipCounter = &dmlOps->Add({
            {"type", "insert"},
            {"filter", "skip"}
        });
        dmlOpsUpdateSkipCounter = &dmlOps->Add({
            {"type", "update"},
            {"filter", "skip"}
        });

        // log_switches
        logSwitches = &prometheus::BuildCounter().Name("log_switches")
                                                 .Help("Number of redo log switches")
                                                 .Register(*registry);
        logSwitchesOnlineCounter = &logSwitches->Add({
            {"type", "online"}
        });
        logSwitchesArchivedCounter = &logSwitches->Add({
            {"type", "archived"}
        });

        // log_switches_lag
        logSwitchesLag = &prometheus::BuildGauge().Name("log_switches_lag")
                                                  .Help("Redo log file processing lag in seconds")
                                                  .Register(*registry);
        logSwitchesLagOnlineGauge = &logSwitchesLag->Add({
            {"type", "online"}
        });
        logSwitchesLagArchivedGauge = &logSwitchesLag->Add({
            {"type", "archived"}
        });

        // messages_confirmed
        messagesConfirmed = &prometheus::BuildCounter().Name("messages_confirmed")
                                                       .Help("Number of messages confirmed by output")
                                                       .Register(*registry);
        messagesConfirmedCounter = &messagesConfirmed->Add({});

        // memory_allocated_mb
        memoryAllocatedMb = &prometheus::BuildGauge().Name("memory_allocated_mb")
                                                     .Help("Amount of allocated memory in MB")
                                                     .Register(*registry);
        memoryAllocatedMbGauge = &memoryAllocatedMb->Add({});

        // memory_used_total_mb
        memoryUsedTotalMb = &prometheus::BuildGauge().Name("memory_used_total_mb")
                                                     .Help("Total used memory")
                                                     .Register(*registry);
        memoryUsedTotalMbGauge = &memoryUsedTotalMb->Add({});

        // memory_used_mb
        memoryUsedMb = &prometheus::BuildGauge().Name("memory_used_mb")
                                                 .Help("Memory used by module: builder")
                                                 .Register(*registry);
        memoryUsedMbBuilderGauge = &memoryUsedMb->Add({
            {"type", "builder"}
        });
        memoryUsedMbMiscGauge = &memoryUsedMb->Add({
            {"type", "misc"}
        });
        memoryUsedMbParserGauge = &memoryUsedMb->Add({
            {"type", "parser"}
        });
        memoryUsedMbReaderGauge = &memoryUsedMb->Add({
            {"type", "reader"}
        });
        memoryUsedMbTransactionsGauge = &memoryUsedMb->Add({
            {"type", "transactions"}
        });
        memoryUsedMbWriterGauge = &memoryUsedMb->Add({
            {"type", "writer"}
        });

        // messages_sent
        messagesSent = &prometheus::BuildCounter().Name("messages_sent")
                                                  .Help("Number of messages sent to output (for example to Kafka or network writer)")
                                                  .Register(*registry);
        messagesSentCounter = &messagesSent->Add({});

        // service_state
        serviceState = &prometheus::BuildGauge().Name("service_state")
                                                  .Help("Service state")
                                                  .Register(*registry);
        serviceStateInitializingGauge = &serviceState->Add({
            {"state", "initializing"}
        });
        serviceStateStartingGauge = &serviceState->Add({
            {"state", "starting"}
        });
        serviceStateReadyGauge = &serviceState->Add({
            {"state", "ready"}
        });
        serviceStateReplicatingGauge = &serviceState->Add({
            {"state", "replicating"}
        });
        serviceStateFinishingGauge = &serviceState->Add({
            {"state", "finishing"}
        });
        serviceStateAbortingGauge = &serviceState->Add({
            {"state", "aborting"}
        });

        // swap_operations_mb
        swapOperationsMb = &prometheus::BuildCounter().Name("swap_operations_mb")
                                                      .Help("Operations on swap space in MB")
                                                      .Register(*registry);
        swapOperationsMbDiscardCounter = &swapOperationsMb->Add({
            {"type", "discard"}
        });
        swapOperationsMbReadCounter = &swapOperationsMb->Add({
            {"type", "read"}
        });
        swapOperationsMbWriteCounter = &swapOperationsMb->Add({
            {"type", "write"}
        });

        // swap_usage_mb
        swapUsageMb = &prometheus::BuildGauge().Name("swap_usage_mb")
                                               .Help("Swap usage in MB")
                                               .Register(*registry);
        swapUsageMbGauge = &swapUsageMb->Add({});

        memoryUsedTotalMb = &prometheus::BuildGauge().Name("memory_used_total_mb")
                                                     .Help("Total used memory")
                                                     .Register(*registry);
        memoryUsedTotalMbGauge = &memoryUsedTotalMb->Add({});

        // transactions
        transactions = &prometheus::BuildCounter().Name("dml_ops")
                                                  .Help("Number of transactions")
                                                  .Register(*registry);
        transactionsCommitOutCounter = &transactions->Add({
            {"type", "commit"},
            {"filter", "out"}
        });
        transactionsRollbackOutCounter = &transactions->Add({
            {"type", "rollback"},
            {"filter", "out"}
        });
        transactionsCommitPartialCounter = &transactions->Add({
            {"type", "commit"},
            {"filter", "partial"}
        });
        transactionsRollbackPartialCounter = &transactions->Add({
            {"type", "rollback"},
            {"filter", "partial"}
        });
        transactionsCommitSkipCounter = &transactions->Add({
            {"type", "commit"},
            {"filter", "skip"}
        });
        transactionsRollbackSkipCounter = &transactions->Add({
            {"type", "rollback"},
            {"filter", "skip"}
        });

        exposer->RegisterCollectable(registry);
    }

    void MetricsPrometheus::shutdown() {}

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
        const std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        const auto& it = dmlOpsDeleteOutCounterMap.find(key);

        if (it != dmlOpsDeleteOutCounterMap.end())
            cnt = it->second;
        else
            cnt = &dmlOps->Add({
                {"type", "delete"},
                {"filter", "out"},
                {"owner", owner},
                {"table", table}
            });

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsInsertOut(uint64_t counter, const std::string& owner, const std::string& table) {
        const std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        const auto& it = dmlOpsInsertOutCounterMap.find(key);

        if (it != dmlOpsInsertOutCounterMap.end())
            cnt = it->second;
        else
            cnt = &dmlOps->Add({
                {"type", "insert"},
                {"filter", "out"},
                {"owner", owner},
                {"table", table}
            });

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsUpdateOut(uint64_t counter, const std::string& owner, const std::string& table) {
        const std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        const auto& it = dmlOpsUpdateOutCounterMap.find(key);

        if (it != dmlOpsUpdateOutCounterMap.end())
            cnt = it->second;
        else
            cnt = &dmlOps->Add({
                {"type", "update"},
                {"filter", "out"},
                {"owner", owner},
                {"table", table}
            });

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsDeleteSkip(uint64_t counter, const std::string& owner, const std::string& table) {
        const std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        const auto& it = dmlOpsDeleteSkipCounterMap.find(key);

        if (it != dmlOpsDeleteSkipCounterMap.end())
            cnt = it->second;
        else
            cnt = &dmlOps->Add({
                {"type", "delete"},
                {"filter", "skip"},
                {"owner", owner},
                {"table", table}
            });

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsInsertSkip(uint64_t counter, const std::string& owner, const std::string& table) {
        const std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        const auto& it = dmlOpsInsertSkipCounterMap.find(key);

        if (it != dmlOpsInsertSkipCounterMap.end())
            cnt = it->second;
        else
            cnt = &dmlOps->Add({
                {"type", "insert"},
                {"filter", "skip"},
                {"owner", owner},
                {"table", table}
            });

        cnt->Increment(counter);
    }

    void MetricsPrometheus::emitDmlOpsUpdateSkip(uint64_t counter, const std::string& owner, const std::string& table) {
        const std::string key(owner + "." + table);
        prometheus::Counter* cnt;
        const auto& it = dmlOpsUpdateSkipCounterMap.find(key);

        if (it != dmlOpsUpdateSkipCounterMap.end())
            cnt = it->second;
        else
            cnt = &dmlOps->Add({
                {"type", "update"},
                {"filter", "skip"},
                {"owner", owner},
                {"table", table}
            });

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

    void MetricsPrometheus::emitMemoryUsedMbMisc(int64_t gauge) {
        memoryUsedMbMiscGauge->Set(gauge);
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

    void MetricsPrometheus::emitMemoryUsedMbWriter(int64_t gauge) {
        memoryUsedMbWriterGauge->Set(gauge);
    }

    // messages_confirmed
    void MetricsPrometheus::emitMessagesConfirmed(uint64_t counter) {
        messagesConfirmedCounter->Increment(counter);
    }

    // messages_sent
    void MetricsPrometheus::emitMessagesSent(uint64_t counter) {
        messagesSentCounter->Increment(counter);
    }

    // service_state
    void MetricsPrometheus::emitServiceStateInitializing(int64_t gauge) {
        serviceStateInitializingGauge->Set(gauge);
    }

    void MetricsPrometheus::emitServiceStateStarting(int64_t gauge) {
        serviceStateStartingGauge->Set(gauge);
    }

    void MetricsPrometheus::emitServiceStateReady(int64_t gauge) {
        serviceStateReadyGauge->Set(gauge);
    }

    void MetricsPrometheus::emitServiceStateReplicating(int64_t gauge) {
        serviceStateReplicatingGauge->Set(gauge);
    }

    void MetricsPrometheus::emitServiceStateFinishing(int64_t gauge) {
        serviceStateFinishingGauge->Set(gauge);
    }

    void MetricsPrometheus::emitServiceStateAborting(int64_t gauge) {
        serviceStateAbortingGauge->Set(gauge);
    }

    // swap_operations_mb
    void MetricsPrometheus::emitSwapOperationsMbDiscard(uint64_t counter) {
        swapOperationsMbDiscardCounter->Increment(counter);
    }

    void MetricsPrometheus::emitSwapOperationsMbRead(uint64_t counter) {
        swapOperationsMbReadCounter->Increment(counter);
    }

    void MetricsPrometheus::emitSwapOperationsMbWrite(uint64_t counter) {
        swapOperationsMbWriteCounter->Increment(counter);
    }

    // swap_usage_mb
    void MetricsPrometheus::emitSwapUsageMb(int64_t gauge) {
        swapUsageMbGauge->Set(gauge);
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
