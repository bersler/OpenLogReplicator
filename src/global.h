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
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#ifndef GLOBAL_H_
#define GLOBAL_H_

using namespace std;

namespace OpenLogReplicator {

extern mutex mainMtx;
extern condition_variable mainThread;
extern bool exitOnSignal;
extern bool mainShutdown;
extern uint64_t trace;
extern uint64_t trace2;

void stopMain(void);
void signalHandler(int s);
void signalCrash(int sig);

const rapidjson::Value& getJSONfieldA(string& fileName, const rapidjson::Value& value, const char* field);
const uint64_t getJSONfieldU(string& fileName, const rapidjson::Value& value, const char* field);
const int64_t getJSONfieldI(string& fileName, const rapidjson::Value& value, const char* field);
const rapidjson::Value& getJSONfieldO(string& fileName, const rapidjson::Value& value, const char* field);
const char* getJSONfieldS(string& fileName, const rapidjson::Value& value, const char* field);

const rapidjson::Value& getJSONfieldA(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const uint64_t getJSONfieldU(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const int64_t getJSONfieldI(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const rapidjson::Value& getJSONfieldO(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
const char* getJSONfieldS(string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);

}

#endif
