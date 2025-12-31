/* Header for Writer class
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

#ifndef WRITER_H_
#define WRITER_H_

#include <mutex>
#include "../common/Thread.h"

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
        BuilderQueue* builderQueue{nullptr};
        Scn checkpointScn{Scn::none()};
        typeIdx checkpointIdx{0};
        time_t checkpointTime;
        uint64_t sentMessages{0};
        uint64_t oldSize{0};
        uint64_t currentQueueSize{0};
        uint64_t hwmQueueSize{0};
        bool streaming{false};
        bool redo{false};

        std::mutex mtx;
        // scn,idx confirmed by client
        Scn confirmedScn{Scn::none()};
        typeIdx confirmedIdx{0};
        BuilderMsg** queue{nullptr};

        void createMessage(BuilderMsg* msg);
        virtual void sendMessage(BuilderMsg* msg) = 0;
        virtual std::string getType() const = 0;
        virtual void pollQueue() = 0;
        void run() override;
        void mainLoop();
        virtual void writeCheckpoint(bool force);
        void readCheckpoint();
        void sortQueue();
        void resetMessageQueue();

    public:
        Writer(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata);
        ~Writer() override;

        virtual void initialize();
        void confirmMessage(BuilderMsg* msg);
        void wakeUp() override;
        virtual void flush() {}

        std::string getName() const override {
            return {"Writer: " + getType()};
        }
    };
}

#endif
