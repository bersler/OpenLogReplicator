/* Thread reading Oracle Redo Logs using batch mode
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

#include "../common/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "ReplicatorBatch.h"

namespace OpenLogReplicator {
    ReplicatorBatch::ReplicatorBatch(Ctx* ctx, void (*archGetLog)(Replicator* replicator), Builder* builder, Metadata* metadata,
                                     TransactionBuffer* transactionBuffer, std::string alias, const char* database) :
            Replicator(ctx, archGetLog, builder, metadata, transactionBuffer, alias, database) {
    }

    ReplicatorBatch::~ReplicatorBatch() = default;

    void ReplicatorBatch::positionReader() {
        if (metadata->startSequence != ZERO_SEQ)
            metadata->setSeqOffset(metadata->startSequence, 0);
        else
            metadata->setSeqOffset(0, 0);
        metadata->sequence = 0;
    }

    void ReplicatorBatch::createSchema() {
        if (FLAG(REDO_FLAGS_SCHEMALESS))
            return;

        ERROR("HINT: if you don't have earlier schema, try with schema-less mode ('flags': 2)")
        if (metadata->schema->scn != ZERO_SCN) {
            ERROR("HINT: you can also set start SCN for writer: 'start-scn': " << std::dec << metadata->schema->scn)
        }

        throw RuntimeException("schema file missing");
    }

    const char* ReplicatorBatch::getModeName() const {
        return "batch";
    }

    bool ReplicatorBatch::continueWithOnline() {
        INFO("finished batch processing, exiting")
        ctx->stopSoft();
        return false;
    }
}
