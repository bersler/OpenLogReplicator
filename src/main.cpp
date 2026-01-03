/* Main program
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

#include <algorithm>
#include <csignal>
#include <pthread.h>
#include <regex>
#include <sys/utsname.h>
#include <thread>
#include <unistd.h>

#include "OpenLogReplicator.h"
#include "common/ClockHW.h"
#include "common/Ctx.h"
#include "common/Thread.h"
#include "common/exception/ConfigurationException.h"
#include "common/exception/DataException.h"
#include "common/exception/RuntimeException.h"
#include "common/types/Data.h"
#include "metadata/Metadata.h"

#ifdef LINK_LIBRARY_OCI
#define HAS_OCI " OCI"
#else
#define HAS_OCI ""
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#define HAS_PROTOBUF " Protobuf"
#ifdef LINK_LIBRARY_ZEROMQ
#define HAS_ZEROMQ " ZeroMQ"
#else
#define HAS_ZEROMQ ""
#endif /* LINK_LIBRARY_ZEROMQ */
#else
#define HAS_PROTOBUF ""
#define HAS_ZEROMQ ""
#endif /* LINK_LIBRARY_PROTOBUF */

#ifdef LINK_LIBRARY_RDKAFKA
#define HAS_KAFKA " Kafka"
#else
#define HAS_KAFKA ""
#endif /* LINK_LIBRARY_RDKAFKA */

#ifdef LINK_LIBRARY_PROMETHEUS
#define HAS_PROMETHEUS " Prometheus"
#else
#define HAS_PROMETHEUS ""
#endif /* LINK_LIBRARY_PROMETHEUS */

#ifdef LINK_STATIC
#define HAS_STATIC " static"
#else
#define HAS_STATIC ""
#endif /* LINK_STATIC */

#ifdef THREAD_INFO
#define HAS_THREAD_INFO " thread-info"
#else
#define HAS_THREAD_INFO ""
#endif /* THREAD_INFO */

namespace {
    OpenLogReplicator::Ctx* mainCtx = nullptr;

    void printStacktrace() {
        mainCtx->printStacktrace();
    }

    void signalHandler(int s) {
        mainCtx->signalHandler(s);
    }

    void signalCrash(int sig __attribute__((unused))) {
        printStacktrace();
        exit(1);
    }

    void signalDump(int sig __attribute__((unused))) {
        printStacktrace();
        mainCtx->signalDump();
    }

    int mainFunction(int argc, char** argv) {
        int ret = 1;
        utsname name{};
        if (uname(&name) != 0) exit(-1);
        std::string buildArch;
        if (strlen(OpenLogReplicator_CMAKE_BUILD_TIMESTAMP) > 0)
            buildArch = ", build-arch: " OpenLogReplicator_CPU_ARCH;

        mainCtx->welcome("OpenLogReplicator v" + std::to_string(OpenLogReplicator_VERSION_MAJOR) + "." +
                std::to_string(OpenLogReplicator_VERSION_MINOR) + "." + std::to_string(OpenLogReplicator_VERSION_PATCH) +
                " (C) 2018-2026 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");
        mainCtx->welcome("arch: " + std::string(name.machine) + buildArch + ", system: " + name.sysname +
                ", release: " + name.release + ", build: " +
                OpenLogReplicator_CMAKE_BUILD_TYPE + ", compiled: " + OpenLogReplicator_CMAKE_BUILD_TIMESTAMP + ", modules:"
                HAS_KAFKA HAS_OCI HAS_PROMETHEUS HAS_PROTOBUF HAS_ZEROMQ HAS_STATIC HAS_THREAD_INFO);

        const char* fileName = "scripts/OpenLogReplicator.json";
        try {
            bool forceRoot = false;
            const std::regex regexTest(".*");
            const std::string regexString("check if matches!");
            const bool regexWorks = regex_search(regexString, regexTest);
            if (!regexWorks)
                throw OpenLogReplicator::RuntimeException(10019, "binaries are build with no regex implementation, check if you have gcc version >= 4.9");

            for (int i = 1; i < argc; ++i) {
                const std::string arg = argv[i];

                if (arg == "")
                    continue;

                if (arg == "-v" || arg == "--version") {
                    // Print banner and exit
                    return 0;
                }

                if (arg == "-r" || arg == "--root") {
                    // Allow bad practice to run as root
                    forceRoot = true;
                    continue;
                }

                if (i + 1 < argc && (arg == "-f" || arg == "--file")) {
                    // Custom config path
                    fileName = argv[i + 1];
                    ++i;
                    continue;
                }

                if (i + 1 < argc && (arg == "-p" || arg == "--process")) {
                    // Custom process name
#if __linux__
                    pthread_setname_np(pthread_self(), argv[i + 1]);
#endif
#if __APPLE__
                    pthread_setname_np(argv[i + 1]);
#endif
                    ++i;
                    continue;
                }

                if (geteuid() == 0) {
                    if (!forceRoot)
                        throw OpenLogReplicator::RuntimeException(10020, "program is run as root, you should never do that");
                    mainCtx->warning(10020, "program is run as root, you should never do that");
                }

                throw OpenLogReplicator::ConfigurationException(30002, "invalid arguments, run: " + std::string(argv[0]) + " [-v|--version] [-f|--file CONFIG] "
                                                                "[-p|--process PROCESSNAME] [-r|--root]");
            }
        } catch (OpenLogReplicator::ConfigurationException& ex) {
            mainCtx->error(ex.code, ex.msg);
            return 1;
        } catch (OpenLogReplicator::RuntimeException& ex) {
            mainCtx->error(ex.code, ex.msg);
            return 1;
        }

        OpenLogReplicator::OpenLogReplicator openLogReplicator(fileName, mainCtx);
        try {
            ret = openLogReplicator.run();
        } catch (OpenLogReplicator::ConfigurationException& ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (OpenLogReplicator::DataException& ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (OpenLogReplicator::RuntimeException& ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (std::bad_alloc& ex) {
            mainCtx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            mainCtx->stopHard();
        }

        return ret;
    }
}

int main(int argc, char** argv) {
    OpenLogReplicator::Ctx ctx;
    mainCtx = &ctx;
    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
    signal(SIGSEGV, signalCrash);
    signal(SIGUSR1, signalDump);

    const char* logTimezone = std::getenv("OLR_LOG_TIMEZONE");
    if (logTimezone != nullptr)
        if (!OpenLogReplicator::Data::parseTimezone(logTimezone, ctx.logTimezone))
            ctx.warning(10070, "invalid environment variable OLR_LOG_TIMEZONE value: " + std::string(logTimezone));

    std::string olrLocales;
    const char* olrLocalesStr = getenv("OLR_LOCALES");
    if (olrLocalesStr != nullptr)
        olrLocales = olrLocalesStr;
    if (olrLocales == "MOCK")
        OLR_LOCALES = OpenLogReplicator::Ctx::LOCALES::MOCK;

    const int ret = mainFunction(argc, argv);

    signal(SIGINT, nullptr);
    signal(SIGPIPE, nullptr);
    signal(SIGSEGV, nullptr);
    signal(SIGUSR1, nullptr);
    mainCtx = nullptr;

    return ret;
}
