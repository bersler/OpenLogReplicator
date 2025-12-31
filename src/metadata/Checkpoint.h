/* Header for Checkpoint class
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

#ifndef CHECKPOINT_H_
#define CHECKPOINT_H_

#include <condition_variable>
#include <mutex>
#include <set>
#include <vector>
#include <unordered_map>

#include "../common/Thread.h"
#include "../common/types/Types.h"
#include "../common/types/Xid.h"

namespace OpenLogReplicator {
    class Metadata;
    class DbIncarnation;
    class TransactionBuffer;

    class Checkpoint final : public Thread {
    public:
        static constexpr off_t CONFIG_FILE_MAX_SIZE = 1048576;

    protected:
        Metadata* metadata;
        std::mutex mtx;
        std::condition_variable condLoop;
        char* configFileBuffer{nullptr};
        std::string configFileName;
        time_t configFileChange;

        void trackConfigFile();
        void updateConfigFile();

    public:
        Checkpoint(Ctx* newCtx, Metadata* newMetadata, std::string newAlias, std::string newConfigFileName, time_t newConfigFileChange);
        ~Checkpoint() override;

        void wakeUp() override;
        void run() override;

        std::string getName() const override {
            return {"Checkpoint"};
        }
    };
}

#endif
