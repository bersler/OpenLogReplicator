/* Header for Thread class
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

#ifndef THREAD_H_
#define THREAD_H_

#include <atomic>
#include <sys/time.h>

#include "Clock.h"
#include "Ctx.h"
#include "types/Types.h"

namespace OpenLogReplicator {
    class Ctx;

    class Thread {
    protected:
        virtual void run() = 0;

    public:
        enum class CONTEXT : unsigned char {
            NONE,
            CPU,
            OS,
            MUTEX,
            WAIT,
            SLEEP,
            MEM,
            TRAN,
            CHKPT,
            NUM
        };

        enum class REASON : unsigned char {
            NONE,
            // MUTEX
            BUILDER_RELEASE,
            BUILDER_ROTATE,
            BUILDER_COMMIT,
            CHECKPOINT_RUN,
            // 1
            CHECKPOINT_WAKEUP,
            CTX_NOTHING_TO_SWAP,
            CTX_FREE_MEMORY,
            CTX_GET_SWAP,
            CTX_GET_USED,
            // 5
            CTX_MEMORY_INIT,
            CTX_SWAPPED_FLUSH1,
            CTX_SWAPPED_FLUSH2,
            CTX_SWAPPED_GET,
            CTX_SWAPPED_GROW1,
            // 10
            CTX_SWAPPED_GROW2,
            CTX_SWAPPED_RELEASE,
            CTX_SWAPPED_SIZE,
            CTX_SWAPPED_SHRINK1,
            CTX_SWAPPED_SHRINK2,
            // 15
            CTX_SWAPPED_WONT,
            MEMORY_CLEAN,
            MEMORY_RUN1,
            MEMORY_RUN2,
            MEMORY_SWAP1,
            // 20
            MEMORY_SWAP2,
            MEMORY_UNSWAP,
            READER_ALLOCATE1,
            READER_ALLOCATE2,
            READER_CHECK_FINISHED,
            // 25
            READER_CHECK_STATUS,
            READER_CONFIRM,
            READER_CHECK_FREE,
            READER_CHECK_REDO,
            READER_FREE,
            // 30
            READER_FULL,
            READER_MAIN1,
            READER_MAIN2,
            READER_READ1,
            READER_READ2,
            // 35
            READER_SET_READ,
            READER_SLEEP1,
            READER_SLEEP2,
            READER_UPDATE_REDO1,
            READER_UPDATE_REDO2,
            // 40
            READER_UPDATE_REDO3,
            READER_WAKE_UP,
            REPLICATOR_ARCH,
            REPLICATOR_SCHEMA,
            REPLICATOR_UPDATE,
            // 45
            TRANSACTION_DROP,
            TRANSACTION_FIND,
            TRANSACTION_SYSTEM,
            WRITER_CONFIRM,
            WRITER_DONE,
            // 50
            // SLEEP
            CHECKPOINT_NO_WORK,
            MEMORY_EXHAUSTED,
            METADATA_WAIT_WRITER,
            METADATA_WAIT_FOR_REPLICATOR,
            READER_CHECK,
            // 55
            READER_EMPTY,
            READER_BUFFER_FULL,
            READER_FINISHED,
            READER_NO_WORK,
            MEMORY_NO_WORK,
            // 60
            WRITER_NO_WORK,
            MEMORY_BLOCKED,
            // 65
            // OTHER
            OS,
            MEM,
            TRAN,
            CHKPT,
            // 67
            // END
            NUM = 255
        };

        Ctx* ctx;
        pthread_t pthread{0};
        std::string alias;
        std::atomic<bool> finished{false};

        explicit Thread(Ctx* newCtx, std::string newAlias);
        virtual ~Thread() = default;

        virtual void wakeUp();
        static void* runStatic(void* thread);

#ifdef THREAD_INFO
        static constexpr bool contextCompiled = true;
#else
        static constexpr bool contextCompiled = false;
#endif
        time_ut contextTimeLast{0};
        time_ut contextTime[static_cast<uint>(CONTEXT::NUM)]{};
        time_ut contextCnt[static_cast<uint>(CONTEXT::NUM)]{};
        uint64_t reasonCnt[static_cast<uint>(REASON::NUM)]{};
        REASON curReason{REASON::NONE};
        CONTEXT curContext{CONTEXT::NONE};
        uint64_t contextSwitches{0};

        virtual std::string getName() const = 0;

        void contextRun() {
            contextStart();
            run();
            contextStop();
        }

        void contextStart() {
            if constexpr (contextCompiled) {
                contextTimeLast = ctx->clock->getTimeUt();
            }
        }

        void contextSet(CONTEXT context, REASON reason = REASON::NONE) {
            if constexpr (contextCompiled) {
                ++contextSwitches;
                const time_ut contextTimeNow = ctx->clock->getTimeUt();
                contextTime[static_cast<uint>(curContext)] += contextTimeNow - contextTimeLast;
                ++contextCnt[static_cast<uint>(curContext)];
                ++reasonCnt[static_cast<uint>(reason)];
                curReason = reason;
                curContext = context;
                contextTimeLast = contextTimeNow;
            }
        }

        void contextStop() {
            if constexpr (contextCompiled) {
                ++contextSwitches;
                const time_ut contextTimeNow = ctx->clock->getTimeUt();
                contextTime[static_cast<uint>(curContext)] += contextTimeNow - contextTimeLast;
                ++contextCnt[static_cast<uint>(curContext)];

                std::string msg =
                        "thread: " + alias +
                        " cpu: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::CPU)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::CPU)]) +
                        " os: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::OS)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::OS)]) +
                        " mtx: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::MUTEX)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::MUTEX)]) +
                        " wait: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::WAIT)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::WAIT)]) +
                        " sleep: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::SLEEP)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::SLEEP)]) +
                        " mem: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::MEM)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::MEM)]) +
                        " tran: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::TRAN)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::TRAN)]) +
                        " chkpt: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::CHKPT)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::CHKPT)]) +
                        " switches: " + std::to_string(contextSwitches) + " reasons:";
                for (uint reason = static_cast<uint>(REASON::NONE); reason < static_cast<int>(REASON::NUM); ++reason) {
                    if (reasonCnt[reason] == 0)
                        continue;

                    msg += " " + std::to_string(reason) + "/" + std::to_string(reasonCnt[reason]);
                }
                ctx->info(0, msg);
            }
        }
    };
}

#endif
