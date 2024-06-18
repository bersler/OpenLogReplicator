/* Header for Writer class
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

#include <mutex>
#include "../common/Thread.h"

#ifndef WRITER_H_
#define WRITER_H_

namespace OpenLogReplicator {
    class Builder;
    struct BuilderMsg;
    struct BuilderQueue;
    class Metadata;

    class Writer : public Thread {
    protected:
        static constexpr uint64_t CHECKPOINT_FILE_MAX_SIZE = 1024;

        std::string database;
        Builder* builder;
        Metadata* metadata;
        // Information about local checkpoint
        BuilderQueue* builderQueue;
        typeScn checkpointScn;
        typeIdx checkpointIdx;
        time_t checkpointTime;
        uint64_t sentMessages;
        uint64_t oldSize;
        uint64_t currentQueueSize;
        uint64_t maxQueueSize;
        bool streaming;

        std::mutex mtx;
        // scn,idx confirmed by client
        typeScn confirmedScn;
        typeIdx confirmedIdx;
        BuilderMsg** queue;

        void createMessage(BuilderMsg* msg);
        virtual void sendMessage(BuilderMsg* msg) = 0;
        virtual std::string getName() const = 0;
        virtual void pollQueue() = 0;
        void run() override;
        void mainLoop();
        virtual void writeCheckpoint(bool force);
        void readCheckpoint();
        void sortQueue();
        void resetMessageQueue();

    public:
        Writer(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata);
        ~Writer() override;

        virtual void initialize();
        void confirmMessage(BuilderMsg* msg);
        void wakeUp() override;
    };
}

#endif
