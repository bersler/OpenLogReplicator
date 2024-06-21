/* Main program
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
#endif /* LINK_LIBRARY_OCI */

namespace OpenLogReplicator {
    Ctx* mainCtx = nullptr;

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
        struct utsname name;
        if (uname(&name)) exit(-1);
        std::string buildArch;
        if (strlen(OpenLogReplicator_CMAKE_BUILD_TIMESTAMP) > 0)
            buildArch = ", build-arch: " OpenLogReplicator_CPU_ARCH;

        mainCtx->welcome("OpenLogReplicator v" + std::to_string(OpenLogReplicator_VERSION_MAJOR) + "." +
                         std::to_string(OpenLogReplicator_VERSION_MINOR) + "." + std::to_string(OpenLogReplicator_VERSION_PATCH) +
                         " (C) 2018-2024 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");
        mainCtx->welcome("arch: " + std::string(name.machine) + buildArch + ", system: " + name.sysname +
                         ", release: " + name.release + ", build: " +
                         OpenLogReplicator_CMAKE_BUILD_TYPE + ", compiled: " + OpenLogReplicator_CMAKE_BUILD_TIMESTAMP + ", modules:"
                         HAS_KAFKA HAS_OCI HAS_PROMETHEUS HAS_PROTOBUF HAS_ZEROMQ HAS_STATIC);

        const char* fileName = "scripts/OpenLogReplicator.json";
        try {
            bool forceRoot = false;
            std::regex regexTest(".*");
            std::string regexString("check if matches!");
            bool regexWorks = regex_search(regexString, regexTest);
            if (!regexWorks)
                throw RuntimeException(10019, "binaries are build with no regex implementation, check if you have gcc version >= 4.9");

            for (int i = 1; i < argc; i++) {
                if ((strncmp(argv[i], "-v", 2) == 0 || strncmp(argv[i], "--version", 9) == 0)) {
                    // Print banner and exit
                    return 0;
                }

                if ((strncmp(argv[i], "-r", 2) == 0 || strncmp(argv[i], "--root", 6) == 0)) {
                    // Allow bad practice to run as root
                    forceRoot = true;
                    continue;
                }

                if (i + 1 < argc && (strncmp(argv[i], "-f", 2) == 0 || strncmp(argv[i], "--file", 6) == 0)) {
                    // Custom config path
                    fileName = argv[i + 1];
                    ++i;
                    continue;
                }

                if (i + 1 < argc && (strncmp(argv[i], "-p", 2) == 0 || strncmp(argv[i], "--process", 9) == 0)) {
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

                if (getuid() == 0) {
                    if (!forceRoot)
                        throw RuntimeException(10020, "program is run as root, you should never do that");
                    mainCtx->warning(10020, "program is run as root, you should never do that");
                }

                throw ConfigurationException(30002, "invalid arguments, run: " + std::string(argv[0]) +
                                                             " [-v|--version] [-f|--file CONFIG] [-p|--process PROCESSNAME]");
            }
        } catch (ConfigurationException& ex) {
            mainCtx->error(ex.code, ex.msg);
            return 1;
        } catch (RuntimeException& ex) {
            mainCtx->error(ex.code, ex.msg);
            return 1;
        }

        OpenLogReplicator openLogReplicator(fileName, mainCtx);
        try {
            ret = openLogReplicator.run();
        } catch (ConfigurationException& ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (DataException& ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (RuntimeException& ex) {
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
    OpenLogReplicator::mainCtx = &ctx;
    signal(SIGINT, OpenLogReplicator::signalHandler);
    signal(SIGPIPE, OpenLogReplicator::signalHandler);
    signal(SIGSEGV, OpenLogReplicator::signalCrash);
    signal(SIGUSR1, OpenLogReplicator::signalDump);

    const char* logTimezone = std::getenv("OLR_LOG_TIMEZONE");
    if (logTimezone != nullptr)
        if (!ctx.parseTimezone(logTimezone, ctx.logTimezone))
            ctx.warning(10070, "invalid environment variable OLR_LOG_TIMEZONE value: " + std::string(logTimezone));

    std::string olrLocales;
    const char* olrLocalesStr = getenv("OLR_LOCALES");
    if (olrLocalesStr != nullptr)
        olrLocales = olrLocalesStr;
    if (olrLocales == "MOCK")
        OLR_LOCALES = OpenLogReplicator::Ctx::OLR_LOCALES_MOCK;

    int ret = OpenLogReplicator::mainFunction(argc, argv);

    signal(SIGINT, nullptr);
    signal(SIGPIPE, nullptr);
    signal(SIGSEGV, nullptr);
    signal(SIGUSR1, nullptr);
    OpenLogReplicator::mainCtx = nullptr;

    return ret;
}
