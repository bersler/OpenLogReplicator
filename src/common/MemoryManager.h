/* Header for MemoryManager class
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

#ifndef MEMORY_MANAGER_H_
#define MEMORY_MANAGER_H_

#include "../common/Thread.h"

namespace OpenLogReplicator {

    class MemoryManager final : public Thread {
    protected:
        std::string swapPath;

    public:
        MemoryManager(Ctx* newCtx, std::string newAlias, std::string newSwapPath);
        ~MemoryManager() override;

        void wakeUp() override;
        void run() override;
        void initialize();

    private:
        uint64_t cleanOldTransactions();
        void cleanup(bool silent = false);
        void getChunkToUnswap(Xid& xid, int64_t& index) const;
        void getChunkToSwap(Xid& xid, int64_t& index);
        bool unswap(Xid xid, int64_t index);
        bool swap(Xid xid, int64_t index);

        std::string getName() const override {
            return {"MemoryManager"};
        }
    };
}

#endif
