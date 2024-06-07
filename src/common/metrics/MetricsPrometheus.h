/* Header for MetricsPrometheus class
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

#include <map>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "Metrics.h"

#ifndef METRICS_PROMETHEUS_H_
#define METRICS_PROMETHEUS_H_

namespace OpenLogReplicator {
    class MetricsPrometheus final : public Metrics {
    protected:
        std::string bind;
        prometheus::Exposer* exposer;
        std::shared_ptr<prometheus::Registry> registry;

        // bytes_confirmed
        prometheus::Family<prometheus::Counter>* bytesConfirmed;
        prometheus::Counter* bytesConfirmedCounter;

        // bytes_parsed
        prometheus::Family<prometheus::Counter>* bytesParsed;
        prometheus::Counter* bytesParsedCounter;

        // bytes_read
        prometheus::Family<prometheus::Counter>* bytesRead;
        prometheus::Counter* bytesReadCounter;

        // bytes_sent
        prometheus::Family<prometheus::Counter>* bytesSent;
        prometheus::Counter* bytesSentCounter;

        // checkpoints
        prometheus::Family<prometheus::Counter>* checkpoints;
        prometheus::Counter* checkpointsOutCounter;
        prometheus::Counter* checkpointsSkipCounter;

        // checkpoint_lag
        prometheus::Family<prometheus::Gauge>* checkpointLag;
        prometheus::Gauge* checkpointLagGauge;

        // ddl_ops
        prometheus::Family<prometheus::Counter>* ddlOps;
        prometheus::Counter* ddlOpsAlterCounter;
        prometheus::Counter* ddlOpsCreateCounter;
        prometheus::Counter* ddlOpsDropCounter;
        prometheus::Counter* ddlOpsOtherCounter;
        prometheus::Counter* ddlOpsPurgeCounter;
        prometheus::Counter* ddlOpsTruncateCounter;

        // dml_ops
        prometheus::Family<prometheus::Counter>* dmlOps;
        prometheus::Counter* dmlOpsDeleteOutCounter;
        prometheus::Counter* dmlOpsInsertOutCounter;
        prometheus::Counter* dmlOpsUpdateOutCounter;
        prometheus::Counter* dmlOpsDeleteSkipCounter;
        prometheus::Counter* dmlOpsInsertSkipCounter;
        prometheus::Counter* dmlOpsUpdateSkipCounter;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsDeleteOutCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsInsertOutCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsUpdateOutCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsDeleteSkipCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsInsertSkipCounterMap;
        std::unordered_map<std::string, prometheus::Counter*> dmlOpsUpdateSkipCounterMap;

        // log_switches
        prometheus::Family<prometheus::Counter>* logSwitches;
        prometheus::Counter* logSwitchesOnlineCounter;
        prometheus::Counter* logSwitchesArchivedCounter;

        // log_switches_lag
        prometheus::Family<prometheus::Gauge>* logSwitchesLag;
        prometheus::Gauge* logSwitchesLagOnlineGauge;
        prometheus::Gauge* logSwitchesLagArchivedGauge;

        // memory_allocated_mb
        prometheus::Family<prometheus::Gauge>* memoryAllocatedMb;
        prometheus::Gauge* memoryAllocatedMbGauge;

        // memory_used_total_mb
        prometheus::Family<prometheus::Gauge>* memoryUsedTotalMb;
        prometheus::Gauge* memoryUsedTotalMbGauge;

        // memory_used_mb
        prometheus::Family<prometheus::Gauge>* memoryUsedMb;
        prometheus::Gauge* memoryUsedMbBuilderGauge;
        prometheus::Gauge* memoryUsedMbParserGauge;
        prometheus::Gauge* memoryUsedMbReaderGauge;
        prometheus::Gauge* memoryUsedMbTransactionsGauge;

        // messages_confirmed
        prometheus::Family<prometheus::Counter>* messagesConfirmed;
        prometheus::Counter* messagesConfirmedCounter;

        // messages_sent
        prometheus::Family<prometheus::Counter>* messagesSent;
        prometheus::Counter* messagesSentCounter;

        // transactions
        prometheus::Family<prometheus::Counter>* transactions;
        prometheus::Counter* transactionsCommitOutCounter;
        prometheus::Counter* transactionsRollbackOutCounter;
        prometheus::Counter* transactionsCommitPartialCounter;
        prometheus::Counter* transactionsRollbackPartialCounter;
        prometheus::Counter* transactionsCommitSkipCounter;
        prometheus::Counter* transactionsRollbackSkipCounter;

    public:
        MetricsPrometheus(uint64_t newTagNames, const char* newBind);
        virtual ~MetricsPrometheus() override;

        virtual void initialize(const Ctx* ctx) override;
        virtual void shutdown() override;

        // bytes_confirmed
        virtual void emitBytesConfirmed(uint64_t counter) override;

        // bytes parsed
        virtual void emitBytesParsed(uint64_t counter) override;

        // bytes read
        virtual void emitBytesRead(uint64_t counter) override;

        // bytes sent
        virtual void emitBytesSent(uint64_t counter) override;

        // checkpoints
        virtual void emitCheckpointsOut(uint64_t counter) override;
        virtual void emitCheckpointsSkip(uint64_t counter) override;

        // checkpoint_lag
        virtual void emitCheckpointLag(int64_t gauge) override;

        // ddl_ops
        virtual void emitDdlOpsAlter(uint64_t counter) override;
        virtual void emitDdlOpsCreate(uint64_t counter) override;
        virtual void emitDdlOpsDrop(uint64_t counter) override;
        virtual void emitDdlOpsOther(uint64_t counter) override;
        virtual void emitDdlOpsPurge(uint64_t counter) override;
        virtual void emitDdlOpsTruncate(uint64_t counter) override;

        // dml_ops
        virtual void emitDmlOpsDeleteOut(uint64_t counter) override;
        virtual void emitDmlOpsInsertOut(uint64_t counter) override;
        virtual void emitDmlOpsUpdateOut(uint64_t counter) override;
        virtual void emitDmlOpsDeleteSkip(uint64_t counter) override;
        virtual void emitDmlOpsInsertSkip(uint64_t counter) override;
        virtual void emitDmlOpsUpdateSkip(uint64_t counter) override;
        virtual void emitDmlOpsDeleteOut(uint64_t counter, const std::string& owner, const std::string& table) override;
        virtual void emitDmlOpsInsertOut(uint64_t counter, const std::string& owner, const std::string& table) override;
        virtual void emitDmlOpsUpdateOut(uint64_t counter, const std::string& owner, const std::string& table) override;
        virtual void emitDmlOpsDeleteSkip(uint64_t counter, const std::string& owner, const std::string& table) override;
        virtual void emitDmlOpsInsertSkip(uint64_t counter, const std::string& owner, const std::string& table) override;
        virtual void emitDmlOpsUpdateSkip(uint64_t counter, const std::string& owner, const std::string& table) override;

        // log_switches
        virtual void emitLogSwitchesArchived(uint64_t counter) override;
        virtual void emitLogSwitchesOnline(uint64_t counter) override;

        // log_switches_lag
        virtual void emitLogSwitchesLagArchived(int64_t gauge) override;
        virtual void emitLogSwitchesLagOnline(int64_t gauge) override;

        // memory_allocated_mb
        virtual void emitMemoryAllocatedMb(int64_t gauge) override;

        // memory_used_total_mb
        virtual void emitMemoryUsedTotalMb(int64_t gauge) override;

        // memory_used_mb
        virtual void emitMemoryUsedMbBuilder(int64_t gauge) override;
        virtual void emitMemoryUsedMbParser(int64_t gauge) override;
        virtual void emitMemoryUsedMbReader(int64_t gauge) override;
        virtual void emitMemoryUsedMbTransactions(int64_t gauge) override;

        // messages_confirmed
        virtual void emitMessagesConfirmed(uint64_t counter) override;

        // messages sent
        virtual void emitMessagesSent(uint64_t counter) override;

        // transactions
        virtual void emitTransactionsCommitOut(uint64_t counter) override;
        virtual void emitTransactionsRollbackOut(uint64_t counter) override;
        virtual void emitTransactionsCommitPartial(uint64_t counter) override;
        virtual void emitTransactionsRollbackPartial(uint64_t counter) override;
        virtual void emitTransactionsCommitSkip(uint64_t counter) override;
        virtual void emitTransactionsRollbackSkip(uint64_t counter) override;
    };
}

#endif
