/* Global functions & variables
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <execinfo.h>
#include <set>
#include <signal.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <sstream>

#define GLOBAL_H_ 0
#include "ConfigurationException.h"
#include "types.h"

namespace OpenLogReplicator {

    set<pthread_t> threads;
    mutex threadMtx;
    mutex mainMtx;
    pthread_t mainThread;
    condition_variable mainCV;
    bool exitOnSignal = false;
    bool mainShutdown = false;
    uint64_t trace = 3;
    uint64_t trace2 = 0;

    void printStacktrace(void) {
        unique_lock<mutex> lck(threadMtx);
        cerr << "stacktrace for thread: " << dec << pthread_self() << endl;
        void* array[128];
        size_t size = backtrace(array, 128);
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        cerr << endl;
    }

    void stopMain(void) {
        unique_lock<mutex> lck(mainMtx);

        mainShutdown = true;
        TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") STOP ALL");
        mainCV.notify_all();
    }

    void signalHandler(int s) {
        if (!exitOnSignal) {
            WARNING("caught signal " << s << ", exiting");
            exitOnSignal = true;
            stopMain();
        }
    }

    void signalCrash(int sig) {
        printStacktrace();
        exit(1);
    }

    void signalDump(int sig) {
        printStacktrace();
        if (mainThread == pthread_self()) {
            for (pthread_t thread : threads) {
                pthread_kill(thread, SIGUSR1);
            }
        }
    }

    void unRegisterThread(pthread_t pthread) {
        unique_lock<mutex> lck(threadMtx);
        threads.erase(pthread);
    }

    void registerThread(pthread_t pthread) {
        unique_lock<mutex> lck(threadMtx);
        threads.insert(pthread);
    }

    const rapidjson::Value& getJSONfieldA(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsArray()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not an array");
        }
        return ret;
    }

    const uint16_t getJSONfieldU16(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a non negative number");
        }
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFF) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const int16_t getJSONfieldI16(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a number");
        }
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFF) || (val < -(int64_t)0x8000)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const uint32_t getJSONfieldU32(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a non negative number");
        }
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFFFFFF) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const int32_t getJSONfieldI32(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a number");
        }
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFFFFFF) || (val < -(int64_t)0x80000000)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const uint64_t getJSONfieldU64(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a non negative number");
        }
        return ret.GetUint64();
    }

    const int64_t getJSONfieldI64(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a number");
        }
        return ret.GetInt64();
    }

    const rapidjson::Value& getJSONfieldO(string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsObject()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not an object");
        }
        return ret;
    }

    const char* getJSONfieldS(string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
        }
        const rapidjson::Value& ret = value[field];
        if (!ret.IsString()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a string");
        }
        if (ret.GetStringLength() > maxLength) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too long (" << dec <<
                    ret.GetStringLength() << ", max: " << maxLength);
        }
        return ret.GetString();
    }

    const rapidjson::Value& getJSONfieldA(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsArray()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not an array");
        }
        return ret;
    }

    const uint16_t getJSONfieldU16(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a non negative number");
        }
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFF) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const int16_t getJSONfieldI16(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a number");
        }
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFF) || (val < -(int64_t)0x8000)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const uint32_t getJSONfieldU32(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a non negative number");
        }
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFFFFFF) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const int32_t getJSONfieldI32(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a number");
        }
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFFFFFF) || (val < -(int64_t)0x80000000)) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too big");
        }
        return val;
    }

    const uint64_t getJSONfieldU64(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a non negative number");
        }
        return ret.GetUint64();
    }

    const int64_t getJSONfieldI64(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a number");
        }
        return ret.GetInt64();
    }

    const rapidjson::Value& getJSONfieldO(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsObject()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not an object");
        }
        return ret;
    }

    const char* getJSONfieldS(string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsString()) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a string");
        }
        if (ret.GetStringLength() > maxLength) {
            CONFIG_FAIL("parsing " << fileName << ", field " << field << " is too long (" << dec <<
                    ret.GetStringLength() << ", max: " << maxLength);
        }
        return ret.GetString();
    }
}
