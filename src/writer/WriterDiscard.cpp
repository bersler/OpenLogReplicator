/* Thread no writing, just discarding messages
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

#include "../metadata/Metadata.h"
#include "WriterDiscard.h"

namespace OpenLogReplicator {
    WriterDiscard::WriterDiscard(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata):
            Writer(newCtx, std::move(newAlias), std::move(newDatabase), newBuilder, newMetadata) {}

    void WriterDiscard::initialize() {
        Writer::initialize();
        streaming = true;
    }

    void WriterDiscard::sendMessage(BuilderMsg* msg) {
        confirmMessage(msg);
    }

    std::string WriterDiscard::getType() const {
        return "discard";
    }

    void WriterDiscard::pollQueue() {
        if (metadata->status == Metadata::STATUS::READY)
            metadata->setStatusStarting(this);
    }
}
