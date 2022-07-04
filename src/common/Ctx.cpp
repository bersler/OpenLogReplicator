/* Context of little/big-endian data
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

#include <cstdlib>
#include <csignal>
#include <execinfo.h>
#include <iostream>
#include <set>
#include <string>
#include <unistd.h>

#include "Ctx.h"
#include "DataException.h"
#include "RuntimeException.h"
#include "Thread.h"

namespace OpenLogReplicator {
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
            version12(false),
            version(0),
            dumpRedoLog(0),
            dumpRawData(0),
            //
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
            trace(3),
            trace2(0),
            flags(0),
            disableChecks(0),
            hardShutdown(false),
            softShutdown(false),
            replicatorFinished(false),
            read16(read16Little),
            read32(read32Little),
            read56(read56Little),
            read64(read64Little),
            readScn(readScnLittle),
            readScnR(readScnRLittle),
            write16(write16Little),
            write32(write32Little),
            write56(write56Little),
            write64(write64Little),
            writeScn(writeScnLittle) {
        mainThread = pthread_self();
    }

    Ctx::~Ctx() {
        while (memoryChunksAllocated > 0) {
            --memoryChunksAllocated;
            free(memoryChunks[memoryChunksAllocated]);
            memoryChunks[memoryChunksAllocated] = nullptr;
        }

        if (memoryChunks != nullptr) {
            delete[] memoryChunks;
            memoryChunks = nullptr;
        }
    }

    void Ctx::setBigEndian() {
        bigEndian = true;
        read16 = read16Big;
        read32 = read32Big;
        read56 = read56Big;
        read64 = read64Big;
        readScn = readScnBig;
        readScnR = readScnRBig;
        write16 = write16Big;
        write32 = write32Big;
        write56 = write56Big;
        write64 = write64Big;
        writeScn = writeScnBig;
    }

    bool Ctx::isBigEndian() const {
        return bigEndian;
    }

    uint16_t Ctx::read16Little(const uint8_t* buf) {
        return (uint16_t)buf[0] | ((uint16_t)buf[1] << 8);
    }

    uint16_t Ctx::read16Big(const uint8_t* buf) {
        return ((uint16_t)buf[0] << 8) | (uint16_t)buf[1];
    }

    uint32_t Ctx::read32Little(const uint8_t* buf) {
        return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
               ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
    }

    uint32_t Ctx::read32Big(const uint8_t* buf) {
        return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
               ((uint32_t)buf[2] << 8) | (uint32_t)buf[3];
    }

    uint64_t Ctx::read56Little(const uint8_t* buf) {
        return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
               ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
               ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
               ((uint64_t)buf[6] << 48);
    }

    uint64_t Ctx::read56Big(const uint8_t* buf) {
        return (((uint64_t)buf[0] << 24) | ((uint64_t)buf[1] << 16) |
                ((uint64_t)buf[2] << 8) | ((uint64_t)buf[3]) |
                ((uint64_t)buf[4] << 40) | ((uint64_t)buf[5] << 32) |
                ((uint64_t)buf[6] << 48));
    }

    uint64_t Ctx::read64Little(const uint8_t* buf) {
        return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
               ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
               ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
               ((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
    }

    uint64_t Ctx::read64Big(const uint8_t* buf) {
        return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
               ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
               ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
               ((uint64_t)buf[6] << 8) | (uint64_t)buf[7];
    }

    typeScn Ctx::readScnLittle(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[5] & 0x80) == 0x80)
            return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                   ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                   ((uint64_t)buf[6] << 32) | ((uint64_t)buf[7] << 40) |
                   ((uint64_t)buf[4] << 48) | ((uint64_t)(buf[5] & 0x7F) << 56);
        else
            return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                   ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                   ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40);
    }

    typeScn Ctx::readScnBig(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[4] & 0x80) == 0x80)
            return (uint64_t)buf[3] | ((uint64_t)buf[2] << 8) |
                   ((uint64_t)buf[1] << 16) | ((uint64_t)buf[0] << 24) |
                   ((uint64_t)buf[7] << 32) | ((uint64_t)buf[6] << 40) |
                   ((uint64_t)buf[5] << 48) | ((uint64_t)(buf[4] & 0x7F) << 56);
        else
            return (uint64_t)buf[3] | ((uint64_t)buf[2] << 8) |
                   ((uint64_t)buf[1] << 16) | ((uint64_t)buf[0] << 24) |
                   ((uint64_t)buf[5] << 32) | ((uint64_t)buf[4] << 40);
    }

    typeScn Ctx::readScnRLittle(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[1] & 0x80) == 0x80)
            return (uint64_t)buf[2] | ((uint64_t)buf[3] << 8) |
                   ((uint64_t)buf[4] << 16) | ((uint64_t)buf[5] << 24) |
                   //((uint64_t)buf[6] << 32) | ((uint64_t)buf[7] << 40) |
                   ((uint64_t)buf[0] << 48) | ((uint64_t)(buf[1] & 0x7F) << 56);
        else
            return (uint64_t)buf[2] | ((uint64_t)buf[3] << 8) |
                   ((uint64_t)buf[4] << 16) | ((uint64_t)buf[5] << 24) |
                   ((uint64_t)buf[0] << 32) | ((uint64_t)buf[1] << 40);
    }

    typeScn Ctx::readScnRBig(const uint8_t* buf) {
        if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
            return ZERO_SCN;
        if ((buf[0] & 0x80) == 0x80)
            return (uint64_t)buf[5] | ((uint64_t)buf[4] << 8) |
                   ((uint64_t)buf[3] << 16) | ((uint64_t)buf[2] << 24) |
                   //((uint64_t)buf[7] << 32) | ((uint64_t)buf[6] << 40) |
                   ((uint64_t)buf[1] << 48) | ((uint64_t)(buf[0] & 0x7F) << 56);
        else
            return (uint64_t)buf[5] | ((uint64_t)buf[4] << 8) |
                   ((uint64_t)buf[3] << 16) | ((uint64_t)buf[2] << 24) |
                   ((uint64_t)buf[1] << 32) | ((uint64_t)buf[0] << 40);
    }

    void Ctx::write16Little(uint8_t* buf, uint16_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
    }

    void Ctx::write16Big(uint8_t* buf, uint16_t val) {
        buf[0] = (val >> 8) & 0xFF;
        buf[1] = val & 0xFF;
    }

    void Ctx::write32Little(uint8_t* buf, uint32_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
    }

    void Ctx::write32Big(uint8_t* buf, uint32_t val) {
        buf[0] = (val >> 24) & 0xFF;
        buf[1] = (val >> 16) & 0xFF;
        buf[2] = (val >> 8) & 0xFF;
        buf[3] = val & 0xFF;
    }

    void Ctx::write56Little(uint8_t* buf, uint64_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 32) & 0xFF;
        buf[5] = (val >> 40) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
    }

    void Ctx::write56Big(uint8_t* buf, uint64_t val) {
        buf[0] = (val >> 24) & 0xFF;
        buf[1] = (val >> 16) & 0xFF;
        buf[2] = (val >> 8) & 0xFF;
        buf[3] = val & 0xFF;
        buf[4] = (val >> 40) & 0xFF;
        buf[5] = (val >> 32) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
    }

    void Ctx::write64Little(uint8_t* buf, uint64_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 32) & 0xFF;
        buf[5] = (val >> 40) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
        buf[7] = (val >> 56) & 0xFF;
    }

    void Ctx::write64Big(uint8_t* buf, uint64_t val) {
        buf[0] = (val >> 56) & 0xFF;
        buf[1] = (val >> 48) & 0xFF;
        buf[2] = (val >> 40) & 0xFF;
        buf[3] = (val >> 32) & 0xFF;
        buf[4] = (val >> 24) & 0xFF;
        buf[5] = (val >> 16) & 0xFF;
        buf[6] = (val >> 8) & 0xFF;
        buf[7] = val & 0xFF;
    }

    void Ctx::writeScnLittle(uint8_t* buf, typeScn val) {
        if (val < 0x800000000000) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
        } else {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 48) & 0xFF;
            buf[5] = ((val >> 56) & 0x7F) | 0x80;
            buf[6] = (val >> 32) & 0xFF;
            buf[7] = (val >> 40) & 0xFF;
        }
    }

    void Ctx::writeScnBig(uint8_t* buf, typeScn val) {
        if (val < 0x800000000000) {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
            buf[4] = (val >> 40) & 0xFF;
            buf[5] = (val >> 32) & 0xFF;
        } else {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
            buf[4] = ((val >> 56) & 0x7F) | 0x80;
            buf[5] = (val >> 48) & 0xFF;
            buf[6] = (val >> 40) & 0xFF;
            buf[7] = (val >> 32) & 0xFF;
        }
    }

    const rapidjson::Value& Ctx::getJsonFieldA(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsArray())
            throw DataException("parsing " + fileName + ", field " + field + " is not an array");
        return ret;
    }

    uint16_t Ctx::getJsonFieldU16(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64())
            throw DataException("parsing " + fileName + ", field " + field + " is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFF)
            throw DataException("parsing " + fileName + ", field " + field + " is too big (" + std::to_string(val) + ")");
        return val;
    }

    int16_t Ctx::getJsonFieldI16(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64())
            throw DataException("parsing " + fileName + ", field " + field + " is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFF) || (val < -(int64_t)0x8000))
            throw DataException("parsing " + fileName + ", field " + field + " is too big (" + std::to_string(val) + ")");
        return (int16_t)val;
    }

    uint32_t Ctx::getJsonFieldU32(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64())
            throw DataException("parsing " + fileName + ", field " + field + " is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFFFFFF)
            throw DataException("parsing " + fileName + ", field " + field + " is too big (" + std::to_string(val) + ")");
        return (uint32_t)val;
    }

    int32_t Ctx::getJsonFieldI32(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64())
            throw DataException("parsing " + fileName + ", field " + field + " is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFFFFFF) || (val < -(int64_t)0x80000000))
            throw DataException("parsing " + fileName + ", field " + field + " is too big (" + std::to_string(val) + ")");
        return (int32_t)val;
    }

    uint64_t Ctx::getJsonFieldU64(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsUint64())
            throw DataException("parsing " + fileName + ", field " + field + " is not an unsigned 64-bit number");
        return ret.GetUint64();
    }

    int64_t Ctx::getJsonFieldI64(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsInt64())
            throw DataException("parsing " + fileName + ", field " + field + " is not a signed 64-bit number");
        return ret.GetInt64();
    }

    const rapidjson::Value& Ctx::getJsonFieldO(std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsObject())
            throw DataException("parsing " + fileName + ", field " + field + " is not an object");
        return ret;
    }

    char* Ctx::getJsonFieldS(std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field) {
        if (!value.HasMember(field))
            throw DataException("parsing " + fileName + ", field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (!ret.IsString())
            throw DataException("parsing " + fileName + ", field " + field + " is not a string");
        if (ret.GetStringLength() > maxLength)
            throw DataException("parsing " + fileName + ", field " + field + " is too long (" +
                                std::to_string(ret.GetStringLength()) + ", max: " + std::to_string(maxLength) + ")");
        return (char*)ret.GetString();
    }

    const rapidjson::Value& Ctx::getJsonFieldA(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsArray())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not an array");
        return ret;
    }

    uint16_t Ctx::getJsonFieldU16(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFF)
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is too big (" + std::to_string(val) + ")");
        return val;
    }

    int16_t Ctx::getJsonFieldI16(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFF) || (val < -(int64_t)0x8000))
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is too big (" + std::to_string(val) + ")");
        return (int16_t)val;
    }

    uint32_t Ctx::getJsonFieldU32(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not an unsigned 64-bit number");
        uint64_t val = ret.GetUint64();
        if (val > 0xFFFFFFFF)
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is too big (" + std::to_string(val) + ")");
        return (uint32_t)val;
    }

    int32_t Ctx::getJsonFieldI32(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not a signed 64-bit number");
        int64_t val = ret.GetInt64();
        if ((val > (int64_t)0x7FFFFFFF) || (val < -(int64_t)0x80000000))
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is too big (" + std::to_string(val) + ")");
        return (int32_t)val;
    }

    uint64_t Ctx::getJsonFieldU64(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsUint64())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not an unsigned 64-bit number");
        return ret.GetUint64();
    }

    int64_t Ctx::getJsonFieldI64(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsInt64())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not a signed 64-bit number");
        return ret.GetInt64();
    }

    const rapidjson::Value& Ctx::getJsonFieldO(std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsObject())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not an object");
        return ret;
    }

    char* Ctx::getJsonFieldS(std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field, uint64_t num) {
        const rapidjson::Value& ret = value[num];
        if (!ret.IsString())
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is not a string");
        if (ret.GetStringLength() > maxLength)
            throw DataException("parsing " + fileName + ", field " + field + "[" + std::to_string(num) + "] is too long (" + std::to_string(ret.GetStringLength()) + ", max: " + std::to_string(maxLength) + ")");
        return (char*)ret.GetString();
    }

    void Ctx::initialize(uint64_t newMemoryMinMb, uint64_t newMemoryMaxMb, uint64_t newReadBufferMax) {
        memoryMinMb = newMemoryMinMb;
        memoryMaxMb = newMemoryMaxMb;
        memoryChunksMin = (memoryMinMb / MEMORY_CHUNK_SIZE_MB);
        memoryChunksMax = memoryMaxMb / MEMORY_CHUNK_SIZE_MB;
        readBufferMax = newReadBufferMax;
        buffersFree = newReadBufferMax;
        bufferSizeMax = readBufferMax * MEMORY_CHUNK_SIZE;

        memoryChunks = new uint8_t*[memoryMaxMb / MEMORY_CHUNK_SIZE_MB];
        for (uint64_t i = 0; i < memoryChunksMin; ++i) {
            memoryChunks[i] = (uint8_t*) aligned_alloc(MEMORY_ALIGNMENT, MEMORY_CHUNK_SIZE);
            if (memoryChunks[i] == nullptr)
                throw RuntimeException("couldn't allocate " + std::to_string(MEMORY_CHUNK_SIZE_MB) + " bytes memory (for: memory chunks#2)");
            ++memoryChunksAllocated;
            ++memoryChunksFree;
        }
        memoryChunksHWM = (uint64_t)memoryChunksMin;
    }

    void Ctx::wakeAllOutOfMemory() {
        std::unique_lock<std::mutex> lck(mtx);
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

    uint8_t* Ctx::getMemoryChunk(const char* module, bool reusable) {
        std::unique_lock<std::mutex> lck(mtx);

        if (memoryChunksFree == 0) {
            while (memoryChunksAllocated == memoryChunksMax && !softShutdown) {
                if (memoryChunksReusable > 1) {
                    condOutOfMemory.wait(lck);
                }
                if (memoryChunksAllocated == memoryChunksMax && memoryChunksReusable == 0) {
                    throw RuntimeException(
                            "out of memory, HINT: try to restart with higher value of 'memory-max-mb' parameter or if big transaction - add to 'skip-xid' list; transaction would be skipped");
                }
            }

            if (memoryChunksFree == 0) {
                memoryChunks[0] = (uint8_t *) aligned_alloc(MEMORY_ALIGNMENT, MEMORY_CHUNK_SIZE);
                if (memoryChunks[0] == nullptr) {
                    throw RuntimeException(
                            "couldn't allocate " + std::to_string(MEMORY_CHUNK_SIZE_MB) + " bytes memory for: " +
                            module);
                }
                ++memoryChunksFree;
                ++memoryChunksAllocated;
            }

            if (memoryChunksAllocated > memoryChunksHWM)
                memoryChunksHWM = (uint64_t)memoryChunksAllocated;
        }

        --memoryChunksFree;
        if (reusable)
            ++memoryChunksReusable;
        return memoryChunks[memoryChunksFree];
    }

    void Ctx::freeMemoryChunk(const char* module, uint8_t* chunk, bool reusable) {
        std::unique_lock<std::mutex> lck(mtx);

        if (memoryChunksFree == memoryChunksAllocated)
            throw RuntimeException(std::string("trying to free unknown memory block for: ") + module);

        //keep memoryChunksMin reserved
        if (memoryChunksFree >= memoryChunksMin) {
            free(chunk);
            --memoryChunksAllocated;
        } else {
            memoryChunks[memoryChunksFree] = chunk;
            ++memoryChunksFree;
        }
        if (reusable)
            --memoryChunksReusable;

        condOutOfMemory.notify_all();
    }

    void Ctx::stopHard() {
        std::unique_lock<std::mutex> lck(mtx);
        if (hardShutdown)
            return;

        hardShutdown = true;
        softShutdown = true;
        condMainLoop.notify_all();
    }

    void Ctx::stopSoft() {
        std::unique_lock<std::mutex> lck(mtx);
        if (softShutdown)
            return;

        softShutdown = true;
        condMainLoop.notify_all();
    }

    void Ctx::mainFinish() {
        for (;;) {
            bool wakingUp = false;
            wakeAllOutOfMemory();
            for (Thread *thread: threads) {
                if (!thread->finished) {
                    thread->wakeUp();
                    wakingUp = true;
                }
            }
            if (!wakingUp)
                break;
            usleep(1000);
        }

        while (threads.size() > 0) {
            Thread* thread = *(threads.begin());
            Thread::finishThread(thread);
        }
    }

    void Ctx::mainLoop() {
        std::unique_lock<std::mutex> lck(mtx);
        if (!hardShutdown)
            condMainLoop.wait(lck);
    }

    void Ctx::printStacktrace() {
        std::unique_lock<std::mutex> lck(mtx);
        ERROR("stacktrace for thread: 0x" << std::hex << pthread_self());
        void* array[128];
        int size = backtrace(array, 128);
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        ERROR("stacktrace completed");
    }

    void Ctx::signalHandler(int s) {
        if (!hardShutdown) {
            ERROR("caught signal " << s << ", exiting")
            stopHard();
        }
    }

    void Ctx::unRegisterThread(Thread* t) {
        std::unique_lock<std::mutex> lck(mtx);
        threads.erase(t);
    }

    void Ctx::registerThread(Thread* t) {
        std::unique_lock<std::mutex> lck(mtx);
        threads.insert(t);
    }

    std::stringstream& Ctx::writeEscapeValue(std::stringstream& ss, std::string& str) {
        const char* c_str = str.c_str();
        for (uint64_t i = 0; i < str.length(); ++i) {
            if (*c_str == '\t' || *c_str == '\r' || *c_str == '\n' || *c_str == '\b') {
                //skip
            } else if (*c_str == '"' || *c_str == '\\' /* || *c_str == '/' */) {
                ss << '\\' << *c_str;
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
            if (islower((unsigned char)*(name + num)))
                return false;

            if (num == 1024)
                throw DataException("'" + std::string(name) + "' is too long");
            ++num;
        }

        return true;
    }

    void Ctx::releaseBuffer() {
        std::unique_lock<std::mutex> lck(mtx);
        ++buffersFree;
    }

    void Ctx::allocateBuffer() {
        std::unique_lock<std::mutex> lck(mtx);
        --buffersFree;
        if (readBufferMax - buffersFree > buffersMaxUsed)
            buffersMaxUsed = readBufferMax - buffersFree;
    }

    void Ctx::signalDump() {
        if (mainThread == pthread_self()) {
            for (Thread *thread : threads) {
                pthread_kill(thread->pthread, SIGUSR1);
            }
        }
    }
}
