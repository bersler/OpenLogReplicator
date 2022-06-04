/* Header for Checkpoint class
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

#include <condition_variable>
#include <mutex>
#include <set>
#include <vector>
#include <unordered_map>

#include "../common/Thread.h"
#include "../common/types.h"
#include "../common/typeTime.h"
#include "../common/typeXid.h"

#ifndef CHECKPOINT_H_
#define CHECKPOINT_H_

namespace OpenLogReplicator {
    class Metadata;
    class OracleIncarnation;
    class TransactionBuffer;

    class Checkpoint : public Thread {
    protected:
        Metadata* metadata;
        std::mutex mtx;
        std::condition_variable condLoop;

        void checkpoint();

    public:
        Checkpoint(Ctx* ctx, Metadata* metadata, std::string alias);
        virtual ~Checkpoint();

        virtual void wakeUp();
        virtual void run();

        friend std::ostream& operator<<(std::ostream& os, const Checkpoint& checkpoint);
    };
}

#endif
