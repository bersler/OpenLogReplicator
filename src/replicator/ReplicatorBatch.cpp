/* Thread reading Oracle Redo Logs using batch mode
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

#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "ReplicatorBatch.h"

namespace OpenLogReplicator {
    ReplicatorBatch::ReplicatorBatch(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                                     TransactionBuffer* newTransactionBuffer, const std::string& newAlias, const char* newDatabase) :
            Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, newAlias, newDatabase) {
    }

    ReplicatorBatch::~ReplicatorBatch() = default;

    void ReplicatorBatch::positionReader() {
        if (metadata->startSequence != Ctx::ZERO_SEQ)
            metadata->setSeqOffset(metadata->startSequence, 0);
        else
            metadata->setSeqOffset(0, 0);
        metadata->sequence = 0;
    }

    void ReplicatorBatch::createSchema() {
        if (ctx->flagsSet(Ctx::REDO_FLAGS_SCHEMALESS))
            return;

        ctx->hint("if you don't have earlier schema, try with schemaless mode ('flags': 2)");
        if (metadata->schema->scn != Ctx::ZERO_SCN)
            ctx->hint("you can also set start SCN for writer: 'start-scn': " + std::to_string(metadata->schema->scn));

        throw RuntimeException(10052, "schema file missing");
    }

    void ReplicatorBatch::updateOnlineRedoLogData() {
        // No need to update online redo log data in batch mode
    }

    const char* ReplicatorBatch::getModeName() const {
        return "batch";
    }

    bool ReplicatorBatch::continueWithOnline() {
        ctx->info(0, "finished batch processing, exiting");
        ctx->stopSoft();
        return false;
    }
}
