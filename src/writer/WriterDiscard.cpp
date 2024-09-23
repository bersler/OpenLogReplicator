/* Thread no writing, just discarding messages
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

#include "WriterDiscard.h"

namespace OpenLogReplicator {
    WriterDiscard::WriterDiscard(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata) :
            Writer(newCtx, newAlias, newDatabase, newBuilder, newMetadata) {
    }

    WriterDiscard::~WriterDiscard() {
    }

    void WriterDiscard::initialize() {
    }

    void WriterDiscard::sendMessage(BuilderMsg* msg) {
        confirmMessage(msg);
    }

    std::string WriterDiscard::getType() const {
        return "discard";
    }

    void WriterDiscard::pollQueue() {
    }
}
