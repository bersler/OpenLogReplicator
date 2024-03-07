/* Context of program
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

#define GLOBALS 1

#include <cstdlib>
#include <csignal>
#include <execinfo.h>
#include <iostream>
#include <set>
#include <string>
#include <unistd.h>

#include "ClockHW.h"
#include "Ctx.h"
#include "Thread.h"
#include "typeIntX.h"
#include "exception/DataException.h"
#include "exception/RuntimeException.h"
#include "metrics/Metrics.h"

uint64_t OLR_LOCALES = OLR_LOCALES_TIMESTAMP;

namespace OpenLogReplicator {
    const char Ctx::map64[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    const char Ctx::map64R[256] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 0, 0, 0, 63,
            52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0, 0, 0, 0, 0, 0,
            0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 0, 0, 0, 0, 0,
            0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    const std::string Ctx::memoryModules[MEMORY_MODULES_NUM] = {"builder", "parser", "reader", "transaction"};

    const int64_t Ctx::cumDays[12] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    const int64_t Ctx::cumDaysLeap[12] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

    typeIntX typeIntX::BASE10[TYPE_INTX_DIGITS][10];

    Ctx::Ctx() :
            bigEndian(false),
            memoryMinMb(0),
            memoryMaxMb(0),
            memoryChunks(nullptr),
            memoryChunksMin(0),
            memoryChunksAllocated(0),
            memoryChunksFree(0),
            memoryChunksMax(0),
            memoryChunksHWM(0),
            memoryChunksReusable(0),
            mainThread(pthread_self()),
            metrics(nullptr),
            clock(nullptr),
            version12(false),
            version(0),
            columnLimit(COLUMN_LIMIT),
            dumpRedoLog(0),
            dumpRawData(0),
            readBufferMax(0),
            buffersFree(0),
            bufferSizeMax(0),
            buffersMaxUsed(0),
            suppLogSize(0),
            checkpointIntervalS(600),
            checkpointIntervalMb(500),
            checkpointKeep(100),
            schemaForceInterval(20),
            redoReadSleepUs(50000),
            redoVerifyDelayUs(0),
            archReadSleepUs(10000000),
            archReadTries(10),
            refreshIntervalUs(10000000),
            pollIntervalUs(100000),
            queueSize(65536),
            dumpPath("."),
            stopLogSwitches(0),
            stopCheckpoints(0),
            stopTransactions(0),
            transactionSizeMax(0),
            logLevel(3),
            trace(0),
            flags(0),
            disableChecks(0),
            hardShutdown(false),
            softShutdown(false),
            replicatorFinished(false) {
        memoryModulesAllocated[0] = 0;
        memoryModulesAllocated[1] = 0;
        memoryModulesAllocated[2] = 0;
        memoryModulesAllocated[3] = 0;
        clock = new ClockHW();
        tzset();
        dbTimezone = BAD_TIMEZONE;
        logTimezone = -timezone;
        hostTimezone = -timezone;
    }

    Ctx::~Ctx() {
        lobIdToXidMap.clear();

        while (memoryChunksAllocated > 0) {
            --memoryChunksAllocated;
            free(memoryChunks[memoryChunksAllocated]);
            memoryChunks[memoryChunksAllocated] = nullptr;
        }

        if (memoryChunks != nullptr) {
            delete[] memoryChunks;
            memoryChunks = nullptr;
        }

        if (metrics != nullptr) {
            metrics->shutdown();
            delete metrics;
            metrics = nullptr;
        }

        if (clock != nullptr) {
            delete clock;
            clock = nullptr;
        }
    }

    const rapidjson::Value& Ctx::getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsArray())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an array");
        return ret;
    }

    uint16_t Ctx::getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFF)
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return val;
    }

    int16_t Ctx::getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > static_cast<int64_t>(0x7FFF)) || (val < -static_cast<int64_t>(0x8000)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return static_cast<int16_t>(val);
    }

    uint32_t Ctx::getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFFFFFF)
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return static_cast<uint32_t>(val);
    }

    int32_t Ctx::getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > static_cast<int64_t>(0x7FFFFFFF)) || (val < -static_cast<int64_t>(0x80000000)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return static_cast<int32_t>(val);
    }

    uint64_t Ctx::getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned 64-bit number");
        return ret.GetUint64();
    }

    int64_t Ctx::getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed 64-bit number");
        return ret.GetInt64();
    }

    const rapidjson::Value& Ctx::getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsObject())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an object");
        return ret;
    }

    const char* Ctx::getJsonFieldS(const std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsString())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a string");
        if (ret.GetStringLength() > maxLength)
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too long (" +
                                       std::to_string(ret.GetStringLength()) + ", max: " + std::to_string(maxLength) + ")");
        return ret.GetString();
    }

    const rapidjson::Value& Ctx::getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsArray())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not an array");
        return ret;
    }

    uint16_t Ctx::getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFF)
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is too big (" + std::to_string(val) + ")");
        return val;
    }

    int16_t Ctx::getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > static_cast<int64_t>(0x7FFF)) || (val < -static_cast<int64_t>(0x8000)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is too big (" + std::to_string(val) + ")");
        return static_cast<int16_t>(val);
    }

    uint32_t Ctx::getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFFFFFF)
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is too big (" + std::to_string(val) + ")");
        return static_cast<uint32_t>(val);
    }

    int32_t Ctx::getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > static_cast<int64_t>(0x7FFFFFFF)) || (val < -static_cast<int64_t>(0x80000000)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is too big (" + std::to_string(val) + ")");
        return static_cast<int32_t>(val);
    }

    uint64_t Ctx::getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not an unsigned 64-bit number");
        return ret.GetUint64();
    }

    int64_t Ctx::getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not a signed 64-bit number");
        return ret.GetInt64();
    }

    const rapidjson::Value& Ctx::getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsObject())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not an object");
        return ret;
    }

    const char* Ctx::getJsonFieldS(const std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsString())
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is not a string");
        if (ret.GetStringLength() > maxLength)
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                       "] is too long (" + std::to_string(ret.GetStringLength()) + ", max: " + std::to_string(maxLength) + ")");
        return ret.GetString();
    }

    bool Ctx::parseTimezone(const char* str, int64_t& out) {
        uint64_t len = strlen(str);

        if (len == 5) {
            if (str[1] >= '0' && str[1] <= '9' &&
                str[2] == ':' &&
                str[3] >= '0' && str[3] <= '9' &&
                str[4] >= '0' && str[4] <= '9') {
                out = -(str[1] - '0') * 3600 + (str[3] - '0') * 60 + (str[4] - '0');
            } else
                return false;
        } else if (len == 6) {
            if (str[1] >= '0' && str[1] <= '9' &&
                str[2] >= '0' && str[2] <= '9' &&
                str[3] == ':' &&
                str[4] >= '0' && str[4] <= '9' &&
                str[5] >= '0' && str[5] <= '9') {
                out = -(str[1] - '0') * 36000 + (str[2] - '0') * 3600 + (str[4] - '0') * 60 + (str[5] - '0');
            } else
                return false;
        } else
            return false;

        if (str[0] == '-')
            out = -out;
        else if (str[0] != '+')
            return false;

        return true;
    }

    std::string Ctx::timezoneToString(int64_t tz) {
        char result[7];

        if (tz < 0) {
            result[0] = '-';
            tz = -tz;
        } else
            result[0] = '+';

        tz /= 60;

        result[6] = 0;
        result[5] = map10(tz % 10);
        tz /= 10;
        result[4] = map10(tz % 6);
        tz /= 6;
        result[3] = ':';
        result[2] = map10(tz % 10);
        tz /= 10;
        result[1] = map10(tz % 10);

        return result;
    }

    time_t Ctx::valuesToEpoch(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second, int64_t tz) {
        time_t result;

        if (year > 0) {
            result = yearToDays(year, month) + cumDays[month % 12] + day;
            result *= 24;
            result += hour;
            result *= 60;
            result += minute;
            result *= 60;
            result += second;
            return result - UNIX_AD1970_01_01 - tz; // adjust to 1970 epoch, 719527 days
        } else {
            // treat dates BC with the exact rules as AD for leap years
            result = -yearToDaysBC(-year, month) + cumDays[month % 12] + day;
            result *= 24;
            result += hour;
            result *= 60;
            result += minute;
            result *= 60;
            result += second;
            return result - UNIX_BC1970_01_01 - tz; // adjust to 1970 epoch, 718798 days (year 0 does not exist)
        }
    }

    uint64_t Ctx::epochToIso8601(time_t timestamp, char* buffer, bool addT, bool addZ) {
        // (-)YYYY-MM-DD hh:mm:ss or (-)YYYY-MM-DDThh:mm:ssZ

        if (timestamp < UNIX_BC4712_01_01 || timestamp > UNIX_AD9999_12_31)
            throw RuntimeException(10069, "invalid timestamp value: " + std::to_string(timestamp));

        timestamp += UNIX_AD1970_01_01;
        if (timestamp >= 365 * 60 * 60 * 24) {
            // AD
            int64_t second = (timestamp % 60);
            timestamp /= 60;
            int64_t minute = (timestamp % 60);
            timestamp /= 60;
            int64_t hour = (timestamp % 24);
            timestamp /= 24;

            int64_t year = timestamp / 365 + 1;
            int64_t day = yearToDays(year, 0);

            while (day > timestamp) {
                --year;
                day = yearToDays(year, 0);
            }
            day = timestamp - day;

            int64_t month = day / 27;
            if (month > 11) month = 11;

            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)) {
                // leap year
                while (cumDaysLeap[month] > day)
                    --month;
                day -= cumDaysLeap[month];
            } else {
                while (cumDays[month] > day)
                    --month;
                day -= cumDays[month];
            }
            ++month;
            ++day;

            buffer[3] = map10(year % 10);
            year /= 10;
            buffer[2] = map10(year % 10);
            year /= 10;
            buffer[1] = map10(year % 10);
            year /= 10;
            buffer[0] = map10(year);
            buffer[4] = '-';
            buffer[6] = map10(month % 10);
            month /= 10;
            buffer[5] = map10(month);
            buffer[7] = '-';
            buffer[9] = map10(day % 10);
            day /= 10;
            buffer[8] = map10(day);
            if (addT)
                buffer[10] = 'T';
            else
                buffer[10] = ' ';
            buffer[12] = map10(hour % 10);
            hour /= 10;
            buffer[11] = map10(hour);
            buffer[13] = ':';
            buffer[15] = map10(minute % 10);
            minute /= 10;
            buffer[14] = map10(minute);
            buffer[16] = ':';
            buffer[18] = map10(second % 10);
            second /= 10;
            buffer[17] = map10(second);
            if (addZ) {
                buffer[19] = 'Z';
                buffer[20] = 0;
                return 20;
            } else {
                buffer[19] = 0;
                return 19;
            }
        } else {
            // BC
            timestamp = 365 * 24 * 60 * 60 - timestamp;

            int64_t second = (timestamp % 60);
            timestamp /= 60;
            int64_t minute = (timestamp % 60);
            timestamp /= 60;
            int64_t hour = (timestamp % 24);
            timestamp /= 24;

            int64_t year = timestamp / 366 - 1;
            if (year < 0) year = 0;
            int64_t day = yearToDaysBC(year, 0);

            while (day < timestamp) {
                ++year;
                day = yearToDaysBC(year, 0);
            }
            if (year == 3013 || year == 3009) {
                std::cerr << "year: " << year << ", day: " << day << ", timestamp: " << timestamp << std::endl;
            }
            day -= timestamp;

            int64_t month = day / 27;
            if (month > 11) month = 11;

            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)) {
                // leap year
                while (cumDaysLeap[month] > day)
                    --month;
                day -= cumDaysLeap[month];
            } else {
                while (cumDays[month] > day)
                    --month;
                day -= cumDays[month];
            }
            ++month;
            ++day;
            buffer[0] = '-';
            buffer[4] = map10(year % 10);
            year /= 10;
            buffer[3] = map10(year % 10);
            year /= 10;
            buffer[2] = map10(year % 10);
            year /= 10;
            buffer[1] = map10(year);
            buffer[5] = '-';
            buffer[7] = map10(month % 10);
            month /= 10;
            buffer[6] = map10(month);
            buffer[8] = '-';
            buffer[10] = map10(day % 10);
            day /= 10;
            buffer[9] = map10(day);
            if (addT)
                buffer[11] = 'T';
            else
                buffer[11] = ' ';
            buffer[13] = map10(hour % 10);
            hour /= 10;
            buffer[12] = map10(hour);
            buffer[14] = ':';
            buffer[16] = map10(minute % 10);
            minute /= 10;
            buffer[15] = map10(minute);
            buffer[17] = ':';
            buffer[19] = map10(second % 10);
            second /= 10;
            buffer[18] = map10(second);
            if (addZ) {
                buffer[20] = 'Z';
                buffer[21] = 0;
                return 21;
            } else {
                buffer[20] = 0;
                return 20;
            }
        };
    }

    void Ctx::initialize(uint64_t newMemoryMinMb, uint64_t newMemoryMaxMb, uint64_t newReadBufferMax) {
        memoryMinMb = newMemoryMinMb;
        memoryMaxMb = newMemoryMaxMb;
        memoryChunksMin = (memoryMinMb / MEMORY_CHUNK_SIZE_MB);
        memoryChunksMax = memoryMaxMb / MEMORY_CHUNK_SIZE_MB;
        readBufferMax = newReadBufferMax;
        buffersFree = newReadBufferMax;
        bufferSizeMax = readBufferMax * MEMORY_CHUNK_SIZE;

        memoryChunks = new uint8_t* [memoryMaxMb / MEMORY_CHUNK_SIZE_MB];
        for (uint64_t i = 0; i < memoryChunksMin; ++i) {
            memoryChunks[i] = reinterpret_cast<uint8_t*>(aligned_alloc(MEMORY_ALIGNMENT, MEMORY_CHUNK_SIZE));
            if (memoryChunks[i] == nullptr)
                throw RuntimeException(10016, "couldn't allocate " + std::to_string(MEMORY_CHUNK_SIZE_MB) +
                                              " bytes memory for: memory chunks#2");
            ++memoryChunksAllocated;
            ++memoryChunksFree;
        }
        memoryChunksHWM = static_cast<uint64_t>(memoryChunksMin);

        if (metrics) {
            metrics->emitMemoryAllocatedMb(memoryChunksAllocated);
            metrics->emitMemoryUsedTotalMb(0);
        }
    }

    void Ctx::wakeAllOutOfMemory() {
        std::unique_lock<std::mutex> lck(memoryMtx);
        condOutOfMemory.notify_all();
    }

    uint64_t Ctx::getMaxUsedMemory() const {
        return memoryChunksHWM * MEMORY_CHUNK_SIZE_MB;
    }

    uint64_t Ctx::getFreeMemory() {
        return memoryChunksFree * MEMORY_CHUNK_SIZE_MB;
    }

    uint64_t Ctx::getAllocatedMemory() const {
        return memoryChunksAllocated * MEMORY_CHUNK_SIZE_MB;
    }

    uint8_t* Ctx::getMemoryChunk(uint64_t module, bool reusable) {
        std::unique_lock<std::mutex> lck(memoryMtx);

        if (memoryChunksFree == 0) {
            while (memoryChunksAllocated == memoryChunksMax && !softShutdown) {
                if (memoryChunksReusable > 1) {
                    warning(10067, "out of memory, but there are reusable memory chunks, trying to reuse some memory");

                    if (trace & TRACE_SLEEP)
                        logTrace(TRACE_SLEEP, "Ctx:getMemoryChunk");
                    condOutOfMemory.wait(lck);
                } else {
                    hint("try to restart with higher value of 'memory-max-mb' parameter or if big transaction - add to 'skip-xid' list; "
                         "transaction would be skipped");
                    throw RuntimeException(10017, "out of memory");
                }
            }

            if (memoryChunksFree == 0) {
                memoryChunks[0] = reinterpret_cast<uint8_t*>(aligned_alloc(MEMORY_ALIGNMENT, MEMORY_CHUNK_SIZE));
                if (memoryChunks[0] == nullptr) {
                    throw RuntimeException(10016, "couldn't allocate " + std::to_string(MEMORY_CHUNK_SIZE_MB) +
                                                  " bytes memory for: " + memoryModules[module]);
                }
                ++memoryChunksFree;
                ++memoryChunksAllocated;

                if (metrics)
                    metrics->emitMemoryAllocatedMb(memoryChunksAllocated);
            }

            if (memoryChunksAllocated > memoryChunksHWM)
                memoryChunksHWM = static_cast<uint64_t>(memoryChunksAllocated);
        }

        --memoryChunksFree;
        if (reusable)
            ++memoryChunksReusable;
        ++memoryModulesAllocated[module];

        if (metrics) {
            metrics->emitMemoryUsedTotalMb(memoryChunksAllocated - memoryChunksFree);

            switch (module) {
                case MEMORY_MODULE_BUILDER:
                    metrics->emitMemoryUsedMbBuilder(memoryModulesAllocated[module]);
                    break;

                case MEMORY_MODULE_PARSER:
                    metrics->emitMemoryUsedMbParser(memoryModulesAllocated[module]);
                    break;

                case MEMORY_MODULE_READER:
                    metrics->emitMemoryUsedMbReader(memoryModulesAllocated[module]);
                    break;

                case MEMORY_MODULE_TRANSACTIONS:
                    metrics->emitMemoryUsedMbTransactions(memoryModulesAllocated[module]);
            }
        }

        return memoryChunks[memoryChunksFree];
    }

    void Ctx::freeMemoryChunk(uint64_t module, uint8_t* chunk, bool reusable) {
        std::unique_lock<std::mutex> lck(memoryMtx);

        if (memoryChunksFree == memoryChunksAllocated)
            throw RuntimeException(50001, "trying to free unknown memory block for: " + memoryModules[module]);

        // Keep memoryChunksMin reserved
        if (memoryChunksFree >= memoryChunksMin) {
            free(chunk);
            --memoryChunksAllocated;
            if (metrics)
                metrics->emitMemoryAllocatedMb(memoryChunksAllocated);
        } else {
            memoryChunks[memoryChunksFree] = chunk;
            ++memoryChunksFree;
        }
        if (reusable)
            --memoryChunksReusable;

        condOutOfMemory.notify_all();

        --memoryModulesAllocated[module];

        if (metrics) {
            metrics->emitMemoryUsedTotalMb(memoryChunksAllocated - memoryChunksFree);

            switch (module) {
                case MEMORY_MODULE_BUILDER:
                    metrics->emitMemoryUsedMbBuilder(memoryModulesAllocated[module]);
                    break;

                case MEMORY_MODULE_PARSER:
                    metrics->emitMemoryUsedMbParser(memoryModulesAllocated[module]);
                    break;

                case MEMORY_MODULE_READER:
                    metrics->emitMemoryUsedMbReader(memoryModulesAllocated[module]);
                    break;

                case MEMORY_MODULE_TRANSACTIONS:
                    metrics->emitMemoryUsedMbTransactions(memoryModulesAllocated[module]);
            }
        }
    }

    void Ctx::stopHard() {
        logTrace(TRACE_THREADS, "stop hard");

        {
            std::unique_lock<std::mutex> lck(mtx);

            if (hardShutdown)
                return;
            hardShutdown = true;
            softShutdown = true;

            condMainLoop.notify_all();
        }
        {
            std::unique_lock<std::mutex> lck(memoryMtx);
            condOutOfMemory.notify_all();
        }
    }

    void Ctx::stopSoft() {
        logTrace(TRACE_THREADS, "stop soft");

        std::unique_lock<std::mutex> lck(mtx);
        if (softShutdown)
            return;

        softShutdown = true;
        condMainLoop.notify_all();
    }

    void Ctx::mainFinish() {
        logTrace(TRACE_THREADS, "main finish start");

        while (wakeThreads()) {
            usleep(10000);
            wakeAllOutOfMemory();
        }

        while (!threads.empty()) {
            Thread* thread;
            {
                std::unique_lock<std::mutex> lck(mtx);
                thread = *(threads.begin());
            }
            finishThread(thread);
        }

        logTrace(TRACE_THREADS, "main finish end");
    }

    void Ctx::mainLoop() {
        logTrace(TRACE_THREADS, "main loop start");

        {
            std::unique_lock<std::mutex> lck(mtx);
            if (!hardShutdown) {
                if (trace & TRACE_SLEEP)
                    logTrace(TRACE_SLEEP, "Ctx:mainLoop");
                condMainLoop.wait(lck);
            }
        }

        logTrace(TRACE_THREADS, "main loop end");
    }

    void Ctx::printStacktrace() {
        void* array[128];
        int size;
        error(10014, "stacktrace for thread: " + std::to_string(reinterpret_cast<uint64_t>(pthread_self())));
        {
            std::unique_lock<std::mutex> lck(mtx);
            size = backtrace(array, 128);
        }
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        error(10014, "stacktrace for thread: completed");
    }

    void Ctx::signalHandler(int s) {
        if (!hardShutdown) {
            error(10015, "caught signal: " + std::to_string(s));
            stopHard();
        }
    }

    bool Ctx::wakeThreads() {
        logTrace(TRACE_THREADS, "wake threads");

        bool wakingUp = false;
        {
            std::unique_lock<std::mutex> lck(mtx);
            for (Thread* thread: threads) {
                if (!thread->finished) {
                    logTrace(TRACE_THREADS, "waking up thread: " + thread->alias);
                    thread->wakeUp();
                    wakingUp = true;
                }
            }
        }
        wakeAllOutOfMemory();

        return wakingUp;
    }

    void Ctx::spawnThread(Thread* thread) {
        logTrace(TRACE_THREADS, "spawn: " + thread->alias);

        if (pthread_create(&thread->pthread, nullptr, &Thread::runStatic, reinterpret_cast<void*>(thread)))
            throw RuntimeException(10013, "spawning thread: " + thread->alias);
        {
            std::unique_lock<std::mutex> lck(mtx);
            threads.insert(thread);
        }
    }

    void Ctx::finishThread(Thread* thread) {
        logTrace(TRACE_THREADS, "finish: " + thread->alias);

        std::unique_lock<std::mutex> lck(mtx);
        if (threads.find(thread) == threads.end())
            return;
        threads.erase(thread);
        pthread_join(thread->pthread, nullptr);
    }

    std::ostringstream& Ctx::writeEscapeValue(std::ostringstream& ss, const std::string& str) {
        const char* c_str = str.c_str();
        for (uint64_t i = 0; i < str.length(); ++i) {
            if (*c_str == '\t') {
                ss << "\\t";
            } else if (*c_str == '\r') {
                ss << "\\r";
            } else if (*c_str == '\n') {
                ss << "\\n";
            } else if (*c_str == '\b') {
                ss << "\\b";
            } else if (*c_str == '\f') {
                ss << "\\f";
            } else if (*c_str == '"' || *c_str == '\\') {
                ss << '\\' << *c_str;
            } else if (*c_str < 32) {
                ss << "\\u00" << Ctx::map16((*c_str >> 4) & 0x0F) << Ctx::map16(*c_str & 0x0F);
            } else {
                ss << *c_str;
            }
            ++c_str;
        }
        return ss;
    }

    bool Ctx::checkNameCase(const char* name) {
        uint64_t num = 0;
        while (*(name + num) != 0) {
            if (islower(static_cast<unsigned char>(*(name + num))))
                return false;

            if (num == 1024)
                throw DataException(20004, "identifier '" + std::string(name) + "' is too long");
            ++num;
        }

        return true;
    }

    void Ctx::releaseBuffer() {
        std::unique_lock<std::mutex> lck(memoryMtx);
        ++buffersFree;
    }

    void Ctx::allocateBuffer() {
        std::unique_lock<std::mutex> lck(memoryMtx);
        --buffersFree;
        if (readBufferMax - buffersFree > buffersMaxUsed)
            buffersMaxUsed = readBufferMax - buffersFree;
    }

    void Ctx::signalDump() {
        if (mainThread == pthread_self()) {
            std::unique_lock<std::mutex> lck(mtx);
            for (Thread* thread: threads)
                pthread_kill(thread->pthread, SIGUSR1);
        }
    }

    void Ctx::welcome(const std::string& message) {
        int code = 0;
        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << " INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::hint(const std::string& message) {
        if (logLevel < LOG_LEVEL_ERROR)
            return;

        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " HINT  " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "HINT  " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::error(int code, const std::string& message) {
        if (logLevel < LOG_LEVEL_ERROR)
            return;

        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " ERROR " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "ERROR " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::warning(int code, const std::string& message) {
        if (logLevel < LOG_LEVEL_WARNING)
            return;

        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " WARN  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "WARN  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::info(int code, const std::string& message) {
        if (logLevel < LOG_LEVEL_INFO)
            return;

        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::debug(int code, const std::string& message) {
        if (logLevel < LOG_LEVEL_DEBUG)
            return;

        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " DEBUG " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "DEBUG " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::logTrace(int mask, const std::string& message) {
        const char* code = "XXXXX";
        if ((trace & mask) == 0)
            return;

        switch (mask) {
            case TRACE_DML:
                code = "DML  ";
                break;

            case TRACE_DUMP:
                code = "DUMP ";
                break;

            case TRACE_LOB:
                code = "LOB  ";
                break;

            case TRACE_LWN:
                code = "LWN  ";
                break;

            case TRACE_THREADS:
                code = "THRD ";
                break;

            case TRACE_SQL:
                code = "SQL  ";
                break;

            case TRACE_FILE:
                code = "FILE ";
                break;

            case TRACE_DISK:
                code = "DISK ";
                break;

            case TRACE_PERFORMANCE:
                code = "PERFM";
                break;

            case TRACE_TRANSACTION:
                code = "TRANX";
                break;

            case TRACE_REDO:
                code = "REDO ";
                break;

            case TRACE_ARCHIVE_LIST:
                code = "ARCHL";
                break;

            case TRACE_SCHEMA_LIST:
                code = "SCHEM";
                break;

            case TRACE_WRITER:
                code = "WRITR";
                break;

            case TRACE_CHECKPOINT:
                code = "CHKPT";
                break;

            case TRACE_SYSTEM:
                code = "SYSTM";
                break;

            case TRACE_LOB_DATA:
                code = "LOBDT";
                break;

            case TRACE_SLEEP:
                code = "SLEEP";
                break;
        }

        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " TRACE " << code << " " << message << '\n';
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "TRACE " << code << " " << message << '\n';
            std::cerr << s.str();
        }
    }
}
