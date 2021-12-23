/* Header for Global functions
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

#include <condition_variable>
#include <mutex>
#include <thread>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#ifndef GLOBAL_H_
#define GLOBAL_H_

namespace OpenLogReplicator {

extern std::mutex threadMtx;
extern std::mutex mainMtx;
extern pthread_t mainThread;
extern std::condition_variable mainCV;
extern bool exitOnSignal;
extern bool mainShutdown;
extern uint64_t trace;
extern uint64_t trace2;

void printStacktrace(void);
void stopMain(void);
void signalHandler(int s);
void signalCrash(int sig);
void signalCrash(int sig);
void signalDump(int sig);
void unRegisterThread(pthread_t pthread);
void registerThread(pthread_t pthread);

const rapidjson::Value& getJSONfieldA(std::string& fileName, const rapidjson::Value& value, const char* field);
const uint16_t getJSONfieldU16(std::string& fileName, const rapidjson::Value& value, const char* field);
const int16_t getJSONfieldI16(std::string& fileName, const rapidjson::Value& value, const char* field);
const uint32_t getJSONfieldU32(std::string& fileName, const rapidjson::Value& value, const char* field);
const int32_t getJSONfieldI32(std::string& fileName, const rapidjson::Value& value, const char* field);
const uint64_t getJSONfieldU64(std::string& fileName, const rapidjson::Value& value, const char* field);
const int64_t getJSONfieldI64(std::string& fileName, const rapidjson::Value& value, const char* field);
const rapidjson::Value& getJSONfieldO(std::string& fileName, const rapidjson::Value& value, const char* field);
const char* getJSONfieldS(std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field);

const rapidjson::Value& getJSONfieldA(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const uint16_t getJSONfieldU16(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const int16_t getJSONfieldI16(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const uint32_t getJSONfieldU32(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const int32_t getJSONfieldI32(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const uint64_t getJSONfieldU64(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const int64_t getJSONfieldI64(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const rapidjson::Value& getJSONfieldO(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const char* getJSONfieldS(std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field, uint64_t num);

}

#endif
