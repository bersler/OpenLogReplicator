/* Header for MetricsPrometheus class
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

#ifndef METRICS_PROMETHEUS_H_
#define METRICS_PROMETHEUS_H_

#include <map>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "Metrics.h"

namespace OpenLogReplicator {
    class MetricsPrometheus final : public Metrics {
    protected:
        std::string bind;
        prometheus::Exposer* exposer{nullptr};
        std::shared_ptr<prometheus::Registry> registry;

        // bytes_confirmed
        prometheus::Family<prometheus::Counter>* bytesConfirmed{nullptr};
        prometheus::Counter* bytesConfirmedCounter{nullptr};

        // bytes_parsed
        prometheus::Family<prometheus::Counter>* bytesParsed{nullptr};
        prometheus::Counter* bytesParsedCounter{nullptr};

        // bytes_read
        prometheus::Family<prometheus::Counter>* bytesRead{nullptr};
        prometheus::Counter* bytesReadCounter{nullptr};

        // bytes_sent
        prometheus::Family<prometheus::Counter>* bytesSent{nullptr};
        prometheus::Counter* bytesSentCounter{nullptr};

        // checkpoints
        prometheus::Family<prometheus::Counter>* checkpoints{nullptr};
        prometheus::Counter* checkpointsOutCounter{nullptr};
        prometheus::Counter* checkpointsSkipCounter{nullptr};

        // checkpoint_lag
        prometheus::Family<prometheus::Gauge>* checkpointLag{nullptr};
        prometheus::Gauge* checkpointLagGauge{nullptr};

        // ddl_ops
        prometheus::Family<prometheus::Counter>* ddlOps{nullptr};
        prometheus::Counter* ddlOpsAlterCounter{nullptr};
        prometheus::Counter* ddlOpsCreateCounter{nullptr};
        prometheus::Counter* ddlOpsDropCounter{nullptr};
        prometheus::Counter* ddlOpsOtherCounter{nullptr};
        prometheus::Counter* ddlOpsPurgeCounter{nullptr};
        prometheus::Counter* ddlOpsTruncateCounter{nullptr};

        // dml_ops
        prometheus::Family<prometheus::Counter>* dmlOps{nullptr};
        prometheus::Counter* dmlOpsDeleteOutCounter{nullptr};
        prometheus::Counter* dmlOpsInsertOutCounter{nullptr};
        prometheus::Counter* dmlOpsUpdateOutCounter{nullptr};
        prometheus::Counter* dmlOpsDeleteSkipCounter{nullptr};
        prometheus::Counter* dmlOpsInsertSkipCounter{nullptr};
        prometheus::Counter* dmlOpsUpdateSkipCounter{nullptr};
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsDeleteOutCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsInsertOutCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsUpdateOutCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsDeleteSkipCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsInsertSkipCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsUpdateSkipCounterMap;

        // log_switches
        prometheus::Family<prometheus::Counter>* logSwitches{nullptr};
        prometheus::Counter* logSwitchesOnlineCounter{nullptr};
        prometheus::Counter* logSwitchesArchivedCounter{nullptr};

        // log_switches_lag
        prometheus::Family<prometheus::Gauge>* logSwitchesLag{nullptr};
        prometheus::Gauge* logSwitchesLagOnlineGauge{nullptr};
        prometheus::Gauge* logSwitchesLagArchivedGauge{nullptr};

        // memory_allocated_mb
        prometheus::Family<prometheus::Gauge>* memoryAllocatedMb{nullptr};
        prometheus::Gauge* memoryAllocatedMbGauge{nullptr};

        // memory_used_total_mb
        prometheus::Family<prometheus::Gauge>* memoryUsedTotalMb{nullptr};
        prometheus::Gauge* memoryUsedTotalMbGauge{nullptr};

        // memory_used_mb
        prometheus::Family<prometheus::Gauge>* memoryUsedMb{nullptr};
        prometheus::Gauge* memoryUsedMbBuilderGauge{nullptr};
        prometheus::Gauge* memoryUsedMbMiscGauge{nullptr};
        prometheus::Gauge* memoryUsedMbParserGauge{nullptr};
        prometheus::Gauge* memoryUsedMbReaderGauge{nullptr};
        prometheus::Gauge* memoryUsedMbTransactionsGauge{nullptr};
        prometheus::Gauge* memoryUsedMbWriterGauge{nullptr};

        // messages_confirmed
        prometheus::Family<prometheus::Counter>* messagesConfirmed{nullptr};
        prometheus::Counter* messagesConfirmedCounter{nullptr};

        // messages_sent
        prometheus::Family<prometheus::Counter>* messagesSent{nullptr};
        prometheus::Counter* messagesSentCounter{nullptr};

        // swap_operations
        prometheus::Family<prometheus::Counter>* swapOperationsMb{nullptr};
        prometheus::Counter* swapOperationsMbDiscardCounter{nullptr};
        prometheus::Counter* swapOperationsMbReadCounter{nullptr};
        prometheus::Counter* swapOperationsMbWriteCounter{nullptr};

        // swap_usage_mb
        prometheus::Family<prometheus::Gauge>* swapUsageMb{nullptr};
        prometheus::Gauge* swapUsageMbGauge{nullptr};

        // transactions
        prometheus::Family<prometheus::Counter>* transactions{nullptr};
        prometheus::Counter* transactionsCommitOutCounter{nullptr};
        prometheus::Counter* transactionsRollbackOutCounter{nullptr};
        prometheus::Counter* transactionsCommitPartialCounter{nullptr};
        prometheus::Counter* transactionsRollbackPartialCounter{nullptr};
        prometheus::Counter* transactionsCommitSkipCounter{nullptr};
        prometheus::Counter* transactionsRollbackSkipCounter{nullptr};

        // service_satate
        prometheus::Family<prometheus::Gauge>* serviceState{nullptr};
        prometheus::Gauge* serviceStateInitializingGauge{nullptr};
        prometheus::Gauge* serviceStateStartingGauge{nullptr};
        prometheus::Gauge* serviceStateReadyGauge{nullptr};
        prometheus::Gauge* serviceStateReplicatingGauge{nullptr};
        prometheus::Gauge* serviceStateFinishingGauge{nullptr};
        prometheus::Gauge* serviceStateAbortingGauge{nullptr};

    public:
        MetricsPrometheus(TAG_NAMES newTagNames, std::string newBind);
        ~MetricsPrometheus() override;

        void initialize(const Ctx* ctx) override;
        void shutdown() override;

        // bytes_confirmed
        void emitBytesConfirmed(uint64_t counter) override;

        // bytes parsed
        void emitBytesParsed(uint64_t counter) override;

        // bytes read
        void emitBytesRead(uint64_t counter) override;

        // bytes sent
        void emitBytesSent(uint64_t counter) override;

        // checkpoints
        void emitCheckpointsOut(uint64_t counter) override;
        void emitCheckpointsSkip(uint64_t counter) override;

        // checkpoint_lag
        void emitCheckpointLag(int64_t gauge) override;

        // ddl_ops
        void emitDdlOpsAlter(uint64_t counter) override;
        void emitDdlOpsCreate(uint64_t counter) override;
        void emitDdlOpsDrop(uint64_t counter) override;
        void emitDdlOpsOther(uint64_t counter) override;
        void emitDdlOpsPurge(uint64_t counter) override;
        void emitDdlOpsTruncate(uint64_t counter) override;

        // dml_ops
        void emitDmlOpsDeleteOut(uint64_t counter) override;
        void emitDmlOpsInsertOut(uint64_t counter) override;
        void emitDmlOpsUpdateOut(uint64_t counter) override;
        void emitDmlOpsDeleteSkip(uint64_t counter) override;
        void emitDmlOpsInsertSkip(uint64_t counter) override;
        void emitDmlOpsUpdateSkip(uint64_t counter) override;
        void emitDmlOpsDeleteOut(uint64_t counter, const std::string& owner, const std::string& table) override;
        void emitDmlOpsInsertOut(uint64_t counter, const std::string& owner, const std::string& table) override;
        void emitDmlOpsUpdateOut(uint64_t counter, const std::string& owner, const std::string& table) override;
        void emitDmlOpsDeleteSkip(uint64_t counter, const std::string& owner, const std::string& table) override;
        void emitDmlOpsInsertSkip(uint64_t counter, const std::string& owner, const std::string& table) override;
        void emitDmlOpsUpdateSkip(uint64_t counter, const std::string& owner, const std::string& table) override;

        // log_switches
        void emitLogSwitchesArchived(uint64_t counter) override;
        void emitLogSwitchesOnline(uint64_t counter) override;

        // log_switches_lag
        void emitLogSwitchesLagArchived(int64_t gauge) override;
        void emitLogSwitchesLagOnline(int64_t gauge) override;

        // memory_allocated_mb
        void emitMemoryAllocatedMb(int64_t gauge) override;

        // memory_used_total_mb
        void emitMemoryUsedTotalMb(int64_t gauge) override;

        // memory_used_mb
        void emitMemoryUsedMbBuilder(int64_t gauge) override;
        void emitMemoryUsedMbMisc(int64_t gauge) override;
        void emitMemoryUsedMbParser(int64_t gauge) override;
        void emitMemoryUsedMbReader(int64_t gauge) override;
        void emitMemoryUsedMbTransactions(int64_t gauge) override;
        void emitMemoryUsedMbWriter(int64_t gauge) override;

        // messages_confirmed
        void emitMessagesConfirmed(uint64_t counter) override;

        // messages sent
        void emitMessagesSent(uint64_t counter) override;

        // swap_operations
        void emitSwapOperationsMbDiscard(uint64_t counter) override;
        void emitSwapOperationsMbRead(uint64_t counter) override;
        void emitSwapOperationsMbWrite(uint64_t counter) override;

        // swap_usage_mb
        void emitSwapUsageMb(int64_t gauge) override;

        // transactions
        void emitTransactionsCommitOut(uint64_t counter) override;
        void emitTransactionsRollbackOut(uint64_t counter) override;
        void emitTransactionsCommitPartial(uint64_t counter) override;
        void emitTransactionsRollbackPartial(uint64_t counter) override;
        void emitTransactionsCommitSkip(uint64_t counter) override;
        void emitTransactionsRollbackSkip(uint64_t counter) override;

        // service_state
        void emitServiceStateInitializing(int64_t gauge) override;
        void emitServiceStateStarting(int64_t gauge) override;
        void emitServiceStateReady(int64_t gauge) override;
        void emitServiceStateReplicating(int64_t gauge) override;
        void emitServiceStateFinishing(int64_t gauge) override;
        void emitServiceStateAborting(int64_t gauge) override;
    };
}

#endif
