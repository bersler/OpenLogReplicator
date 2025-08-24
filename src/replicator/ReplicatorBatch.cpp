/* Thread reading database redo Logs using batch mode
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

#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "ReplicatorBatch.h"

namespace OpenLogReplicator {
    ReplicatorBatch::ReplicatorBatch(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                                     TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase) :
            Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, std::move(newAlias), std::move(newDatabase)) {
    }

    void ReplicatorBatch::positionReader() {
        metadata->setSeqOffset(0, 0);
        metadata->sequence = 0;
    }

    void ReplicatorBatch::createSchema() {
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS))
            return;

        ctx->hint("if you don't have earlier schema, try with schemaless mode ('flags': 2)");

        throw RuntimeException(10052, "schema file missing");
    }
    if (startScn != Ctx::ZERO_SCN || startSequence != Ctx::ZERO_SEQ || !startTime.empty() || startTimeRel > 0)
    throw ConfigurationException(30011, "Invalid startup parameters: startup parameters are not allowed to be used for batch reader");

    void ReplicatorBatch::updateOnlineRedoLogData() {
        // No need to update online redo log data in batch mode
    }

    std::string ReplicatorBatch::getModeName() const {
        return {"batch"};
    }

    bool ReplicatorBatch::continueWithOnline() {
        ctx->info(0, "finished batch processing, exiting");
        ctx->stopSoft();
        return false;
    }
}
