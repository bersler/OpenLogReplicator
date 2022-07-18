/* Main program
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

#define GLOBALS 1

#include <algorithm>
#include <csignal>
#include <regex>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <thread>
#include <unistd.h>
#include <execinfo.h>

#include "OpenLogReplicator.h"
#include "common/Ctx.h"
#include "common/ConfigurationException.h"
#include "common/DataException.h"
#include "common/RuntimeException.h"
#include "common/Thread.h"

#ifdef LINK_LIBRARY_OCI
#define HAS_OCI " OCI"
#else
#define HAS_OCI ""
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#define HAS_PROTOBUF " Probobuf"
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

uint64_t OLR_LOCALES = OLR_LOCALES_TIMESTAMP;

namespace OpenLogReplicator {
    Ctx *mainCtx = nullptr;

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
        ALL("OpenLogReplicator v." << std::dec << OpenLogReplicator_VERSION_MAJOR << "." << OpenLogReplicator_VERSION_MINOR <<  "." << OpenLogReplicator_VERSION_PATCH <<
                                   " (C) 2018-2022 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information" <<
                                   ", arch: " << name.machine <<
                                   ", system: " << name.sysname <<
                                   ", release: " << name.release <<
                                   ", build: " << OpenLogReplicator_CMAKE_BUILD_TYPE <<
                                   ", modules:" HAS_KAFKA HAS_OCI HAS_PROTOBUF HAS_ZEROMQ)

        const char* fileName = "scripts/OpenLogReplicator.json";
        try {
            std::regex regexTest(".*");
            std::string regexString("check if matches!");
            bool regexWorks = regex_search(regexString, regexTest);
            if (!regexWorks)
                throw ConfigurationException("binaries are build with no regex implementation, check if you have gcc version >= 4.9");

            if (getuid() == 0)
                throw ConfigurationException("program is run as root, you should never do that");

            if (argc == 2 && (strncmp(argv[1], "-v", 2) == 0 || strncmp(argv[1], "--version", 9) == 0)) {
                // Print banner and exit
                return 0;
            } else if (argc == 3 && (strncmp(argv[1], "-f", 2) == 0 || strncmp(argv[1], "--file", 6) == 0)) {
                // Custom config path
                fileName = argv[2];
            } else if (argc > 1)
                throw ConfigurationException(std::string("invalid arguments, run: ") + argv[0] +
                                                        " [-v|--version] or [-f|--file CONFIG] default path for CONFIG file is " + fileName);

            OpenLogReplicator openLogReplicator(fileName, mainCtx);
            ret = openLogReplicator.run();
        } catch (ConfigurationException& ex) {
            ERROR(ex.msg)
        } catch (RuntimeException& ex) {
            ERROR(ex.msg)
        } catch (std::bad_alloc& ex) {
            ERROR("memory allocation failed: " << ex.what())
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

    std::string olrLocales;
    const char* olrLocalesStr = getenv("OLR_LOCALES");
    if (olrLocalesStr != nullptr)
        olrLocales = olrLocalesStr;
    if (olrLocales == "MOCK")
        OLR_LOCALES = OLR_LOCALES_MOCK;

    int ret = OpenLogReplicator::mainFunction(argc, argv);

    signal(SIGINT, nullptr);
    signal(SIGPIPE, nullptr);
    signal(SIGSEGV, nullptr);
    signal(SIGUSR1, nullptr);

    return ret;
}
