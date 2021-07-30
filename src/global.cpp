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
#include <thread>
#include <unistd.h>

#define GLOBAL_H_ 0
#include "ConfigurationException.h"
#include "types.h"

namespace OpenLogReplicator {

uint64_t trace = 3;
uint64_t trace2 = 0;
mutex mainMtx;
condition_variable mainThread;
bool exitOnSignal = false;
bool mainShutdown = false;

void stopMain(void) {
    unique_lock<mutex> lck(mainMtx);

    mainShutdown = true;
    TRACE(TRACE2_THREADS, "THREADS: MAIN (" << hex << this_thread::get_id() << ") STOP ALL");
    mainThread.notify_all();
}

void signalHandler(int s) {
    if (!exitOnSignal) {
        WARNING("caught signal " << s << ", exiting");
        exitOnSignal = true;
        stopMain();
    }
}

void signalCrash(int sig) {
    void* array[32];
    size_t size = backtrace(array, 32);
    ERROR("signal " << dec << sig);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
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

const uint64_t getJSONfieldU(string& fileName, const rapidjson::Value& value, const char* field) {
    if (!value.HasMember(field)) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
    }
    const rapidjson::Value& ret = value[field];
    if (!ret.IsUint64()) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a non negative number");
    }
    return ret.GetUint64();
}

const int64_t getJSONfieldI(string& fileName, const rapidjson::Value& value, const char* field) {
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

const char* getJSONfieldS(string& fileName, const rapidjson::Value& value, const char* field) {
    if (!value.HasMember(field)) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
    }
    const rapidjson::Value& ret = value[field];
    if (!ret.IsString()) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " is not a string");
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

const uint64_t getJSONfieldU(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
    const rapidjson::Value& ret = value[num];
    if (!ret.IsUint64()) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a non negative number");
    }
    return ret.GetUint64();
}

const int64_t getJSONfieldI(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
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

const char* getJSONfieldS(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
    const rapidjson::Value& ret = value[num];
    if (!ret.IsString()) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << "[" << dec << num << "] is not a string");
    }
    return ret.GetString();
}

}
