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
        static constexpr uint64_t PERF_NONE = 0;
        static constexpr uint64_t PERF_CPU = 1;
        static constexpr uint64_t PERF_OS = 2;
        static constexpr uint64_t PERF_MUTEX = 3;
        static constexpr uint64_t PERF_WAIT = 4;
        static constexpr uint64_t PERF_SLEEP = 5;
        static constexpr uint64_t PERF_MEM = 6;
        static constexpr uint64_t PERF_TRAN = 7;
        static constexpr uint64_t PERF_CHKPT = 8;
        static constexpr uint64_t PERF_LEN = 9;

        Ctx* ctx;
        pthread_t pthread;
        std::string alias;
        std::atomic<bool> finished;

        explicit Thread(Ctx* newCtx, const std::string& newAlias);
        virtual ~Thread();
        virtual void wakeUp();
        static void* runStatic(void* thread);

#ifdef THREAD_INFO
        static constexpr bool perfCompiled = true;
#else
        static constexpr bool perfCompiled = false;
#endif
        time_ut perfLast {0};
        time_ut perfSum[PERF_LEN] {0, 0, 0};
        uint64_t perfStatus {PERF_NONE};
        uint64_t perfSwitches {0};

        void perfRun() {
            perfStart();
            run();
            perfStop();
        }

        void perfStart() {
            if (!perfCompiled)
                return;

            perfLast = ctx->clock->getTimeUt();
        }

        void perfSet(uint64_t status) {
            if (!perfCompiled )
                return;

            ++perfSwitches;
            time_ut perfNow = ctx->clock->getTimeUt();
            perfSum[perfStatus] += perfNow - perfLast;
            perfStatus = status;
            perfLast = perfNow;
        }

        void perfStop() {
            if (!perfCompiled)
                return;

            time_ut perfNow = ctx->clock->getTimeUt();
            perfSum[perfStatus] += perfNow - perfLast;

            ctx->info(0, "perf: " + alias +
                         " cpu: " + std::to_string(perfSum[PERF_CPU]) + " us"
                         " os: " + std::to_string(perfSum[PERF_OS]) + " us"
                         " mtx: " + std::to_string(perfSum[PERF_MUTEX]) + " us"
                         " wait: " + std::to_string(perfSum[PERF_WAIT]) + " us"
                         " sleep: " + std::to_string(perfSum[PERF_SLEEP]) + " us"
                         " mem: " + std::to_string(perfSum[PERF_MEM]) + " us"
                         " tran: " + std::to_string(perfSum[PERF_TRAN]) + " us"
                         " chkpt: " + std::to_string(perfSum[PERF_CHKPT]) + " us"
                         " switches: " + std::to_string(perfSwitches));
        }
    };
}

#endif
