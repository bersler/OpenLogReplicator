/* Thread reading database redo Logs using batch mode
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

#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "ReplicatorBatch.h"

namespace OpenLogReplicator {
    ReplicatorBatch::ReplicatorBatch(Ctx * newCtx, void(*newArchGetLog)(Replicator * replicator), Builder * newBuilder, Metadata * newMetadata,
                                     TransactionBuffer * newTransactionBuffer, std::string newAlias, std::string newDatabase):
        Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, std::move(newAlias), std::move(newDatabase)) {}

    void ReplicatorBatch::positionReader() {
        if (metadata->startSequence != Seq::none())
            metadata->setSeqFileOffset(metadata->startSequence, FileOffset::zero());
        else
            metadata->setSeqFileOffset(Seq::zero(), FileOffset::zero());
        metadata->sequence = Seq::zero();
    }

    void ReplicatorBatch::createSchema() {
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS))
            return;

        ctx->hint("if you don't have earlier schema, try with schemaless mode ('flags': 2)");
        if (metadata->schema->scn != Scn::none())
            ctx->hint("you can also set start SCN for writer: 'start-scn': " + metadata->schema->scn.toString());

        throw RuntimeException(10052, "schema file missing");
    }

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
