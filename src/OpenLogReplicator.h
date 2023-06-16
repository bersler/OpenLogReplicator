/* Header for OpenLogReplicator
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <list>
#include <rapidjson/document.h>
#include <string>

#ifndef OPENLOGREPLICATOR_H_
#define OPENLOGREPLICATOR_H_

namespace OpenLogReplicator {
    class Ctx;
    class Replicator;
    class Checkpoint;
    class Locales;
    class Builder;
    class Metadata;
    class TransactionBuffer;
    class Writer;

    class OpenLogReplicator {
    protected:
        std::list<Replicator*> replicators;
        std::list<Checkpoint*> checkpoints;
        std::list<Locales*> localess;
        std::list<Builder*> builders;
        std::list<Metadata*> metadatas;
        std::list<TransactionBuffer*> transactionBuffers;
        std::list<Writer*> writers;
        Replicator* replicator;
        int fid;
        char* configFileBuffer;
        std::string configFileName;
        Ctx* ctx;

        void mainProcessMapping(const rapidjson::Value& readerJson);

    public:
        OpenLogReplicator(const std::string& newConfigFileName, Ctx* newCtx);
        virtual ~OpenLogReplicator();
        int run();
    };
}

#endif
