/* Base class for source and target thread
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

#include "Ctx.h"
#include "Thread.h"
#include "exception/RuntimeException.h"

namespace OpenLogReplicator {
    Thread::Thread(Ctx* newCtx, const std::string& newAlias) :
            ctx(newCtx),
            pthread(0),
            alias(newAlias),
            finished(false) {
    }

    Thread::~Thread() = default;

    void Thread::wakeUp() {
        if (unlikely(ctx->trace & Ctx::TRACE_THREADS)) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE_THREADS, "thread (" + ss.str() + ") wake up");
        }
    }

    void* Thread::runStatic(void* voidThread) {
        Thread* thread = reinterpret_cast<Thread*>(voidThread);
        thread->run();
        thread->finished = true;
        return nullptr;
    }
}
