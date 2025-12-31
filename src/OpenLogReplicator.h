/* Header for OpenLogReplicator
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

#ifndef OPEN_LOG_REPLICATOR_H_
#define OPEN_LOG_REPLICATOR_H_

#include <list>
#include <rapidjson/document.h>
#include <string>

#include "common/types/Types.h"

namespace OpenLogReplicator {
    class Builder;
    class Ctx;
    class Checkpoint;
    class Locales;
    class MemoryManager;
    class Metadata;
    class Replicator;
    class TransactionBuffer;
    class Writer;

    class OpenLogReplicator final {
    protected:
        std::vector<Replicator*> replicators;
        std::vector<Checkpoint*> checkpoints;
        std::vector<Locales*> localess;
        std::vector<Builder*> builders;
        std::vector<Metadata*> metadatas;
        std::vector<MemoryManager*> memoryManagers;
        std::vector<TransactionBuffer*> transactionBuffers;
        std::vector<Writer*> writers;
        Replicator* replicator{nullptr};
        int fid{-1};
        char* configFileBuffer{nullptr};
        std::string configFileName;
        Ctx* ctx;

        void mainProcessMapping(const rapidjson::Value& readerJson) const;

    public:
        OpenLogReplicator(std::string newConfigFileName, Ctx* newCtx);
        ~OpenLogReplicator();

        int run();
    };
}

#endif
