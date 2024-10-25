/* Header for Thread class
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

#include <atomic>
#include <sys/time.h>

#include "types.h"
#include "Clock.h"
#include "Ctx.h"

#ifndef THREAD_H_
#define THREAD_H_

namespace OpenLogReplicator {
    class Ctx;

    class Thread {
    protected:
        virtual void run() = 0;

    public:
        enum CONTEXT {
            CONTEXT_NONE, CONTEXT_CPU, CONTEXT_OS, CONTEXT_MUTEX, CONTEXT_WAIT, CONTEXT_SLEEP, CONTEXT_MEM, CONTEXT_TRAN, CONTEXT_CHKPT,
            CONTEXT_NUM
        };
        enum REASON {
            REASON_NONE,
            // MUTEX
            BUILDER_RELEASE, BUILDER_ROTATE, BUILDER_COMMIT, CHECKPOINT_RUN, // 1
            CHECKPOINT_WAKEUP, CTX_NOTHING_TO_SWAP, CTX_FREE_MEMORY, CTX_GET_SWAP, CTX_GET_USED, // 5
            CTX_MEMORY_INIT, CTX_SWAPPED_FLUSH1, CTX_SWAPPED_FLUSH2, CTX_SWAPPED_GET, CTX_SWAPPED_GROW1, // 10
            CTX_SWAPPED_GROW2, CTX_SWAPPED_RELEASE, CTX_SWAPPED_SIZE, CTX_SWAPPED_SHRINK1, CTX_SWAPPED_SHRINK2, // 15
            CTX_SWAPPED_WONT, MEMORY_CLEAN, MEMORY_RUN1, MEMORY_RUN2, MEMORY_SWAP1, // 20
            MEMORY_SWAP2, MEMORY_UNSWAP, READER_ALLOCATE1, READER_ALLOCATE2, READER_CHECK_FINISHED, // 25
            READER_CHECK_STATUS, READER_CONFIRM, READER_CHECK_FREE, READER_CHECK_REDO, READER_FREE, // 30
            READER_FULL, READER_MAIN1, READER_MAIN2, READER_READ1, READER_READ2, // 35
            READER_SET_READ, READER_SLEEP1, READER_SLEEP2, READER_UPDATE_REDO1, READER_UPDATE_REDO2, // 40
            READER_UPDATE_REDO3, READER_WAKE_UP, REPLICATOR_ARCH, REPLICATOR_SCHEMA, REPLICATOR_UPDATE, // 45
            TRANSACTION_DROP, TRANSACTION_FIND, TRANSACTION_SYSTEM, WRITER_CONFIRM, WRITER_DONE, // 50
            // SLEEP
            CHECKPOINT_NO_WORK, MEMORY_EXHAUSTED, METADATA_WAIT_WRITER, METADATA_WAIT_FOR_REPLICATOR, READER_CHECK, // 55
            READER_EMPTY, READER_BUFFER_FULL, READER_FINISHED, READER_NO_WORK, MEMORY_NO_WORK, // 60
            WRITER_NO_WORK, // 65
            // OTHER
            REASON_OS, REASON_MEM, REASON_TRAN, REASON_CHKPT, // 66
            // END
            REASON_NUM = 1000
        };
        Ctx* ctx;
        pthread_t pthread;
        std::string alias;
        std::atomic<bool> finished;

        explicit Thread(Ctx* newCtx, const std::string& newAlias);
        virtual ~Thread();
        virtual void wakeUp();
        static void* runStatic(void* thread);

#ifdef THREAD_INFO
        static constexpr bool contextCompiled = true;
#else
        static constexpr bool contextCompiled = false;
#endif
        time_ut contextTimeLast{0};
        time_ut contextTime[CONTEXT_NUM]{0};
        time_ut contextCnt[CONTEXT_NUM]{0};
        uint64_t reasonCnt[REASON_NUM]{0};
        REASON curReason{REASON_NONE};
        CONTEXT curContext{CONTEXT_NONE};
        uint64_t contextSwitches{0};

        virtual const std::string getName() const = 0;

        void contextRun() {
            contextStart();
            run();
            contextStop();
        }

        void contextStart() {
            if (!contextCompiled)
                return;

            contextTimeLast = ctx->clock->getTimeUt();
        }

        void contextSet(CONTEXT context, REASON reason = REASON_NONE) {
            if (!contextCompiled)
                return;

            ++contextSwitches;
            time_ut contextTimeNow = ctx->clock->getTimeUt();
            contextTime[curContext] += contextTimeNow - contextTimeLast;
            ++contextCnt[curContext];
            ++reasonCnt[reason];
            curReason = reason;
            curContext = context;
            contextTimeLast = contextTimeNow;
        }

        void contextStop() {
            if (!contextCompiled)
                return;

            ++contextSwitches;
            time_ut contextTimeNow = ctx->clock->getTimeUt();
            contextTime[curContext] += contextTimeNow - contextTimeLast;
            ++contextCnt[curContext];

            std::string msg =
                    "thread: " + alias +
                    " cpu: " + std::to_string(contextTime[CONTEXT_CPU]) + "/" + std::to_string(contextCnt[CONTEXT_CPU]) +
                    " os: " + std::to_string(contextTime[CONTEXT_OS]) + "/" + std::to_string(contextCnt[CONTEXT_OS]) +
                    " mtx: " + std::to_string(contextTime[CONTEXT_MUTEX]) + "/" + std::to_string(contextCnt[CONTEXT_MUTEX]) +
                    " wait: " + std::to_string(contextTime[CONTEXT_WAIT]) + "/" + std::to_string(contextCnt[CONTEXT_WAIT]) +
                    " sleep: " + std::to_string(contextTime[CONTEXT_SLEEP]) + "/" + std::to_string(contextCnt[CONTEXT_SLEEP]) +
                    " mem: " + std::to_string(contextTime[CONTEXT_MEM]) + "/" + std::to_string(contextCnt[CONTEXT_MEM]) +
                    " tran: " + std::to_string(contextTime[CONTEXT_TRAN]) + "/" + std::to_string(contextCnt[CONTEXT_TRAN]) +
                    " chkpt: " + std::to_string(contextTime[CONTEXT_CHKPT]) + "/" + std::to_string(contextCnt[CONTEXT_CHKPT]) +
                    " switches: " + std::to_string(contextSwitches) + " reasons:";
            for (int reason = REASON_NONE; reason < REASON_NUM; ++reason) {
                if (reasonCnt[reason] == 0)
                    continue;

                msg += " " + std::to_string(reason) + "/" + std::to_string(reasonCnt[reason]);
            }
            ctx->info(0, msg);
        }
    };
}

#endif
