/* Header for Ctx class
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
#include <condition_variable>
#include <fstream>
#include <memory>
#include <mutex>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <thread>
#include <unordered_map>

#include "typeLobId.h"
#include "typeXid.h"

#ifndef CTX_H_
#define CTX_H_

#ifndef GLOBALS
extern uint64_t OLR_LOCALES;
#endif

namespace OpenLogReplicator {
    class Clock;
    class Metrics;
    class Thread;

    class Ctx final {
    public:
        static constexpr uint64_t BAD_TIMEZONE = 0x7FFFFFFFFFFFFFFF;
        static constexpr size_t MEMORY_ALIGNMENT = 512;
        static constexpr uint64_t MAX_PATH_LENGTH = 2048;

        static constexpr uint64_t COLUMN_LIMIT = 1000;
        static constexpr uint64_t COLUMN_LIMIT_23_0 = 4096;

        static constexpr uint64_t DISABLE_CHECKS_GRANTS = 0x00000001;
        static constexpr uint64_t DISABLE_CHECKS_SUPPLEMENTAL_LOG = 0x00000002;
        static constexpr uint64_t DISABLE_CHECKS_BLOCK_SUM = 0x00000004;
        static constexpr uint64_t DISABLE_CHECKS_JSON_TAGS = 0x00000008;

        static constexpr uint64_t JSON_PARAMETER_LENGTH = 256;
        static constexpr uint64_t JSON_BROKERS_LENGTH = 4096;
        static constexpr uint64_t JSON_TOPIC_LENGTH = 256;

        static constexpr uint64_t JSON_USERNAME_LENGTH = 128;
        static constexpr uint64_t JSON_PASSWORD_LENGTH = 128;
        static constexpr uint64_t JSON_SERVER_LENGTH = 4096;
        static constexpr uint64_t JSON_KEY_LENGTH = 4096;
        static constexpr uint64_t JSON_CONDITION_LENGTH = 16384;
        static constexpr uint64_t JSON_XID_LENGTH = 32;

        static constexpr uint64_t LOG_LEVEL_SILENT = 0;
        static constexpr uint64_t LOG_LEVEL_ERROR = 1;
        static constexpr uint64_t LOG_LEVEL_WARNING = 2;
        static constexpr uint64_t LOG_LEVEL_INFO = 3;
        static constexpr uint64_t LOG_LEVEL_DEBUG = 4;

        static constexpr uint64_t MEMORY_MODULE_BUILDER = 0;
        static constexpr uint64_t MEMORY_MODULE_PARSER = 1;
        static constexpr uint64_t MEMORY_MODULE_READER = 2;
        static constexpr uint64_t MEMORY_MODULE_TRANSACTIONS = 3;
        static constexpr uint64_t MEMORY_MODULES_NUM = 4;

        static constexpr uint64_t MEMORY_CHUNK_SIZE_MB = 1;
        static constexpr uint64_t MEMORY_CHUNK_SIZE = MEMORY_CHUNK_SIZE_MB * 1024 * 1024;
        static constexpr uint64_t MEMORY_CHUNK_MIN_MB = 16;

        static constexpr uint64_t OLR_LOCALES_TIMESTAMP = 0;
        static constexpr uint64_t OLR_LOCALES_MOCK = 1;

        static constexpr uint64_t REDO_FLAGS_ARCH_ONLY = 0x00000001;
        static constexpr uint64_t REDO_FLAGS_SCHEMALESS = 0x00000002;
        static constexpr uint64_t REDO_FLAGS_ADAPTIVE_SCHEMA = 0x00000004;
        static constexpr uint64_t REDO_FLAGS_DIRECT_DISABLE = 0x00000008;
        static constexpr uint64_t REDO_FLAGS_IGNORE_DATA_ERRORS = 0x00000010;
        static constexpr uint64_t REDO_FLAGS_SHOW_DDL = 0x00000020;
        static constexpr uint64_t REDO_FLAGS_SHOW_HIDDEN_COLUMNS = 0x00000040;
        static constexpr uint64_t REDO_FLAGS_SHOW_GUARD_COLUMNS = 0x00000080;
        static constexpr uint64_t REDO_FLAGS_SHOW_NESTED_COLUMNS = 0x00000100;
        static constexpr uint64_t REDO_FLAGS_SHOW_UNUSED_COLUMNS = 0x00000200;
        static constexpr uint64_t REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS = 0x00000400;
        static constexpr uint64_t REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS = 0x00000800;
        static constexpr uint64_t REDO_FLAGS_SHOW_CHECKPOINT = 0x00001000;
        static constexpr uint64_t REDO_FLAGS_CHECKPOINT_KEEP = 0x00002000;
        static constexpr uint64_t REDO_FLAGS_VERIFY_SCHEMA = 0x00004000;
        static constexpr uint64_t REDO_FLAGS_RAW_COLUMN_DATA = 0x00008000;
        static constexpr uint64_t REDO_FLAGS_EXPERIMENTAL_XMLTYPE = 0x00010000;
        static constexpr uint64_t REDO_FLAGS_EXPERIMENTAL_JSON = 0x00020000;
        static constexpr uint64_t REDO_FLAGS_EXPERIMENTAL_NOT_NULL_MISSING = 0x00040000;

        static constexpr uint64_t TRACE_DML = 0x00000001;
        static constexpr uint64_t TRACE_DUMP = 0x00000002;
        static constexpr uint64_t TRACE_LOB = 0x00000004;
        static constexpr uint64_t TRACE_LWN = 0x00000008;
        static constexpr uint64_t TRACE_THREADS = 0x00000010;
        static constexpr uint64_t TRACE_SQL = 0x00000020;
        static constexpr uint64_t TRACE_FILE = 0x00000040;
        static constexpr uint64_t TRACE_DISK = 0x00000080;
        static constexpr uint64_t TRACE_PERFORMANCE = 0x00000100;
        static constexpr uint64_t TRACE_TRANSACTION = 0x00000200;
        static constexpr uint64_t TRACE_REDO = 0x00000400;
        static constexpr uint64_t TRACE_ARCHIVE_LIST = 0x00000800;
        static constexpr uint64_t TRACE_SCHEMA_LIST = 0x00001000;
        static constexpr uint64_t TRACE_WRITER = 0x00002000;
        static constexpr uint64_t TRACE_CHECKPOINT = 0x00004000;
        static constexpr uint64_t TRACE_SYSTEM = 0x00008000;
        static constexpr uint64_t TRACE_LOB_DATA = 0x00010000;
        static constexpr uint64_t TRACE_SLEEP = 0x00020000;
        static constexpr uint64_t TRACE_CONDITION = 0x00040000;

        static constexpr time_t UNIX_AD1970_01_01 = 62167132800L;
        static constexpr time_t UNIX_BC1970_01_01 = 62167132800L - 365 * 24 * 60 * 60;
        static constexpr time_t UNIX_BC4712_01_01 = -210831897600L;
        static constexpr time_t UNIX_AD9999_12_31 = 253402300799L;

        static constexpr typeSeq ZERO_SEQ = 0xFFFFFFFF;
        static constexpr typeScn ZERO_SCN = 0xFFFFFFFFFFFFFFFF;
        static constexpr typeBlk ZERO_BLK = 0xFFFFFFFF;

    protected:
        bool bigEndian;
        std::atomic<uint64_t> memoryMinMb;
        std::atomic<uint64_t> memoryMaxMb;

        std::atomic<uint8_t**> memoryChunks;
        std::atomic<uint64_t> memoryChunksMin;
        std::atomic<uint64_t> memoryChunksAllocated;
        std::atomic<uint64_t> memoryChunksFree;
        std::atomic<uint64_t> memoryChunksMax;
        std::atomic<uint64_t> memoryChunksHWM;
        std::atomic<uint64_t> memoryChunksReusable;
        uint64_t memoryModulesAllocated[MEMORY_MODULES_NUM];

        std::condition_variable condMainLoop;
        std::condition_variable condOutOfMemory;
        std::mutex mtx;
        std::mutex memoryMtx;
        std::set<Thread*> threads;
        pthread_t mainThread;

        inline int64_t yearToDays(int64_t year, int64_t month) const {
            int64_t result = year * 365 + year / 4 - year / 100 + year / 400;
            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && month < 2)
                --result;

            return result;
        }

        inline int64_t yearToDaysBC(int64_t year, int64_t month) const {
            int64_t result = (year * 365) + (year / 4) - (year / 100) + (year / 400);
            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && month >= 2)
                --result;

            return result;
        }

    public:

        bool flagsSet(uint64_t mask) const {
            return (flags & mask) != 0;
        }

        bool disableChecksSet(uint64_t mask) const {
            return (disableChecks & mask) != 0;
        }

        static const char map64[65];
        static const char map64R[256];
        static const std::string memoryModules[MEMORY_MODULES_NUM];
        static const int64_t cumDays[12];
        static const int64_t cumDaysLeap[12];

        Metrics* metrics;
        Clock* clock;
        bool version12;
        std::atomic<uint64_t> version;                   // Compatibility level of redo logs
        uint64_t columnLimit;
        std::string versionStr;
        std::atomic<uint64_t> dumpRedoLog;
        std::atomic<uint64_t> dumpRawData;
        std::unique_ptr<std::ofstream> dumpStream;
        int64_t dbTimezone;
        int64_t hostTimezone;
        int64_t logTimezone;

        inline void setBigEndian() {
            bigEndian = true;
        }

        [[nodiscard]] inline bool isBigEndian() const {
            return bigEndian;
        }

        // Disk read buffers
        std::atomic<uint64_t> readBufferMax;
        std::atomic<uint64_t> buffersFree;
        std::atomic<uint64_t> bufferSizeMax;
        std::atomic<uint64_t> buffersMaxUsed;
        std::atomic<uint64_t> suppLogSize;
        // Checkpoint
        uint64_t checkpointIntervalS;
        uint64_t checkpointIntervalMb;
        uint64_t checkpointKeep;
        uint64_t schemaForceInterval;
        // Reader
        uint64_t redoReadSleepUs;
        uint64_t redoVerifyDelayUs;
        uint64_t archReadSleepUs;
        uint64_t archReadTries;
        uint64_t refreshIntervalUs;
        // Writer
        uint64_t pollIntervalUs;
        uint64_t queueSize;
        // Transaction buffer
        std::string dumpPath;
        std::string redoCopyPath;
        uint64_t stopLogSwitches;
        uint64_t stopCheckpoints;
        uint64_t stopTransactions;
        uint64_t transactionSizeMax;
        std::atomic<uint64_t> logLevel;
        std::atomic<uint64_t> trace;
        std::atomic<uint64_t> flags;
        std::atomic<uint64_t> disableChecks;
        std::atomic<bool> hardShutdown;
        std::atomic<bool> softShutdown;
        std::atomic<bool> replicatorFinished;
        std::unordered_map<typeLobId, typeXid> lobIdToXidMap;

        Ctx();
        virtual ~Ctx();

        static inline char map10(uint64_t x) {
            return static_cast<char>('0' + x);
        }

        static inline char map16(uint64_t x) {
            if (x < 10)
                return static_cast<char>('0' + x);
            else
                return static_cast<char>('a' + (x - 10));
        }

        static inline char map16U(uint64_t x) {
            if (x < 10)
                return static_cast<char>('0' + x);
            else
                return static_cast<char>('A' + (x - 10));
        }

        inline uint16_t read16(const uint8_t* buf) const {
            if (bigEndian)
                return read16Big(buf);
            else
                return read16Little(buf);
        }

        inline uint32_t read32(const uint8_t* buf) const {
            if (bigEndian)
                return read32Big(buf);
            else
                return read32Little(buf);
        }

        inline uint64_t read56(const uint8_t* buf) const {
            if (bigEndian)
                return read56Big(buf);
            else
                return read56Little(buf);
        }

        inline uint64_t read64(const uint8_t* buf) const {
            if (bigEndian)
                return read64Big(buf);
            else
                return read64Little(buf);
        }

        inline typeScn readScn(const uint8_t* buf) const {
            if (bigEndian)
                return readScnBig(buf);
            else
                return readScnLittle(buf);
        }

        inline typeScn readScnR(const uint8_t* buf) const {
            if (bigEndian)
                return readScnRBig(buf);
            else
                return readScnRLittle(buf);
        }

        inline void write16(uint8_t* buf, uint16_t val) const {
            if (bigEndian)
                write16Big(buf, val);
            else
                write16Little(buf, val);
        }

        inline void write32(uint8_t* buf, uint32_t val) const {
            if (bigEndian)
                write32Big(buf, val);
            else
                write32Little(buf, val);
        }

        inline void write56(uint8_t* buf, uint64_t val) const {
            if (bigEndian)
                write56Big(buf, val);
            else
                write56Little(buf, val);
        }

        inline void write64(uint8_t* buf, uint64_t val) const {
            if (bigEndian)
                write64Big(buf, val);
            else
                write64Little(buf, val);
        }

        inline void writeScn(uint8_t* buf, typeScn val) const {
            if (bigEndian)
                writeScnBig(buf, val);
            else
                writeScnLittle(buf, val);
        }

        inline uint16_t read16Little(const uint8_t* buf) const {
            return static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
        }

        inline uint16_t read16Big(const uint8_t* buf) const {
            return (static_cast<uint16_t>(buf[0]) << 8) | static_cast<uint16_t>(buf[1]);
        }

        inline uint32_t read24Big(const uint8_t* buf) const {
            return (static_cast<uint32_t>(buf[0]) << 16) |
                   (static_cast<uint32_t>(buf[1]) << 8) | static_cast<uint32_t>(buf[2]);
        }

        inline uint32_t read32Little(const uint8_t* buf) const {
            return static_cast<uint32_t>(buf[0]) | (static_cast<uint32_t>(buf[1]) << 8) |
                   (static_cast<uint32_t>(buf[2]) << 16) | (static_cast<uint32_t>(buf[3]) << 24);
        }

        inline uint32_t read32Big(const uint8_t* buf) const {
            return (static_cast<uint32_t>(buf[0]) << 24) | (static_cast<uint32_t>(buf[1]) << 16) |
                   (static_cast<uint32_t>(buf[2]) << 8) | static_cast<uint32_t>(buf[3]);
        }

        inline uint64_t read56Little(const uint8_t* buf) const {
            return static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
                   (static_cast<uint64_t>(buf[2]) << 16) | (static_cast<uint64_t>(buf[3]) << 24) |
                   (static_cast<uint64_t>(buf[4]) << 32) | (static_cast<uint64_t>(buf[5]) << 40) |
                   (static_cast<uint64_t>(buf[6]) << 48);
        }

        inline uint64_t read56Big(const uint8_t* buf) const {
            return (static_cast<uint64_t>(buf[0]) << 24) | (static_cast<uint64_t>(buf[1]) << 16) |
                   (static_cast<uint64_t>(buf[2]) << 8) | (static_cast<uint64_t>(buf[3])) |
                   (static_cast<uint64_t>(buf[4]) << 40) | (static_cast<uint64_t>(buf[5]) << 32) |
                   (static_cast<uint64_t>(buf[6]) << 48);
        }

        inline uint64_t read64Little(const uint8_t* buf) const {
            return static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
                   (static_cast<uint64_t>(buf[2]) << 16) | (static_cast<uint64_t>(buf[3]) << 24) |
                   (static_cast<uint64_t>(buf[4]) << 32) | (static_cast<uint64_t>(buf[5]) << 40) |
                   (static_cast<uint64_t>(buf[6]) << 48) | (static_cast<uint64_t>(buf[7]) << 56);
        }

        inline uint64_t read64Big(const uint8_t* buf) const {
            return (static_cast<uint64_t>(buf[0]) << 56) | (static_cast<uint64_t>(buf[1]) << 48) |
                   (static_cast<uint64_t>(buf[2]) << 40) | (static_cast<uint64_t>(buf[3]) << 32) |
                   (static_cast<uint64_t>(buf[4]) << 24) | (static_cast<uint64_t>(buf[5]) << 16) |
                   (static_cast<uint64_t>(buf[6]) << 8) | static_cast<uint64_t>(buf[7]);
        }

        inline typeScn readScnLittle(const uint8_t* buf) const {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return ZERO_SCN;
            if ((buf[5] & 0x80) == 0x80)
                return static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
                       (static_cast<uint64_t>(buf[2]) << 16) | (static_cast<uint64_t>(buf[3]) << 24) |
                       (static_cast<uint64_t>(buf[6]) << 32) | (static_cast<uint64_t>(buf[7]) << 40) |
                       (static_cast<uint64_t>(buf[4]) << 48) | (static_cast<uint64_t>(buf[5] & 0x7F) << 56);
            else
                return static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
                       (static_cast<uint64_t>(buf[2]) << 16) | (static_cast<uint64_t>(buf[3]) << 24) |
                       (static_cast<uint64_t>(buf[4]) << 32) | (static_cast<uint64_t>(buf[5]) << 40);
        }

        inline typeScn readScnBig(const uint8_t* buf) const {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return ZERO_SCN;
            if ((buf[4] & 0x80) == 0x80)
                return static_cast<uint64_t>(buf[3]) | (static_cast<uint64_t>(buf[2]) << 8) |
                       (static_cast<uint64_t>(buf[1]) << 16) | (static_cast<uint64_t>(buf[0]) << 24) |
                       (static_cast<uint64_t>(buf[7]) << 32) | (static_cast<uint64_t>(buf[6]) << 40) |
                       (static_cast<uint64_t>(buf[5]) << 48) | (static_cast<uint64_t>(buf[4] & 0x7F) << 56);
            else
                return static_cast<uint64_t>(buf[3]) | (static_cast<uint64_t>(buf[2]) << 8) |
                       (static_cast<uint64_t>(buf[1]) << 16) | (static_cast<uint64_t>(buf[0]) << 24) |
                       (static_cast<uint64_t>(buf[5]) << 32) | (static_cast<uint64_t>(buf[4]) << 40);
        }

        inline typeScn readScnRLittle(const uint8_t* buf) const {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return ZERO_SCN;
            if ((buf[1] & 0x80) == 0x80)
                return static_cast<uint64_t>(buf[2]) | (static_cast<uint64_t>(buf[3]) << 8) |
                       (static_cast<uint64_t>(buf[4]) << 16) | (static_cast<uint64_t>(buf[5]) << 24) |
                       // (static_cast<uint64_t>(buf[6]) << 32) | (static_cast<uint64_t>(buf[7]) << 40) |
                       (static_cast<uint64_t>(buf[0]) << 48) | (static_cast<uint64_t>(buf[1] & 0x7F) << 56);
            else
                return static_cast<uint64_t>(buf[2]) | (static_cast<uint64_t>(buf[3]) << 8) |
                       (static_cast<uint64_t>(buf[4]) << 16) | (static_cast<uint64_t>(buf[5]) << 24) |
                       (static_cast<uint64_t>(buf[0]) << 32) | (static_cast<uint64_t>(buf[1]) << 40);
        }

        inline typeScn readScnRBig(const uint8_t* buf) const {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return ZERO_SCN;
            if ((buf[0] & 0x80) == 0x80)
                return static_cast<uint64_t>(buf[5]) | (static_cast<uint64_t>(buf[4]) << 8) |
                       (static_cast<uint64_t>(buf[3]) << 16) | (static_cast<uint64_t>(buf[2]) << 24) |
                       // (static_cast<uint64_t>(buf[7]) << 32) | (static_cast<uint64_t>(buf[6]) << 40) |
                       (static_cast<uint64_t>(buf[1]) << 48) | (static_cast<uint64_t>(buf[0] & 0x7F) << 56);
            else
                return static_cast<uint64_t>(buf[5]) | (static_cast<uint64_t>(buf[4]) << 8) |
                       (static_cast<uint64_t>(buf[3]) << 16) | (static_cast<uint64_t>(buf[2]) << 24) |
                       (static_cast<uint64_t>(buf[1]) << 32) | (static_cast<uint64_t>(buf[0]) << 40);
        }

        inline void write16Little(uint8_t* buf, uint16_t val) const {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
        }

        inline void write16Big(uint8_t* buf, uint16_t val) const {
            buf[0] = (val >> 8) & 0xFF;
            buf[1] = val & 0xFF;
        }

        inline void write32Little(uint8_t* buf, uint32_t val) const {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
        }

        inline void write32Big(uint8_t* buf, uint32_t val) const {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
        }

        inline void write56Little(uint8_t* buf, uint64_t val) const {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
        }

        inline void write56Big(uint8_t* buf, uint64_t val) const {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
            buf[4] = (val >> 40) & 0xFF;
            buf[5] = (val >> 32) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
        }

        inline void write64Little(uint8_t* buf, uint64_t val) const {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
            buf[7] = (val >> 56) & 0xFF;
        }

        inline void write64Big(uint8_t* buf, uint64_t val) const {
            buf[0] = (val >> 56) & 0xFF;
            buf[1] = (val >> 48) & 0xFF;
            buf[2] = (val >> 40) & 0xFF;
            buf[3] = (val >> 32) & 0xFF;
            buf[4] = (val >> 24) & 0xFF;
            buf[5] = (val >> 16) & 0xFF;
            buf[6] = (val >> 8) & 0xFF;
            buf[7] = val & 0xFF;
        }

        inline void writeScnLittle(uint8_t* buf, typeScn val) const {
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

        inline void writeScnBig(uint8_t* buf, typeScn val) const {
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

        static void checkJsonFields(const std::string& fileName, const rapidjson::Value& value, const char* names[]);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint16_t getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int16_t getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint32_t getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int32_t getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint64_t getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int64_t getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static const char* getJsonFieldS(const std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field);

        [[nodiscard]] static const rapidjson::Value& getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static uint16_t getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static int16_t getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static uint32_t getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static int32_t getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static uint64_t getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static int64_t getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field, uint64_t num);
        [[nodiscard]] static const char* getJsonFieldS(const std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field,
                                                       uint64_t num);

        bool parseTimezone(const char* str, int64_t& out) const;
        std::string timezoneToString(int64_t tz) const;
        time_t valuesToEpoch(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute, int64_t second, int64_t tz) const;
        uint64_t epochToIso8601(time_t timestamp, char* buffer, bool addT, bool addZ) const;

        void initialize(uint64_t newMemoryMinMb, uint64_t newMemoryMaxMb, uint64_t newReadBufferMax);
        void wakeAllOutOfMemory();
        [[nodiscard]] uint64_t getMaxUsedMemory() const;
        [[nodiscard]] uint64_t getAllocatedMemory() const;
        [[nodiscard]] uint64_t getFreeMemory() const;
        [[nodiscard]] uint8_t* getMemoryChunk(uint64_t module, bool reusable);
        void freeMemoryChunk(uint64_t module, uint8_t* chunk, bool reusable);
        void stopHard();
        void stopSoft();
        void mainLoop();
        void mainFinish();
        void printStacktrace();
        void signalHandler(int s);

        bool wakeThreads();
        void spawnThread(Thread* thread);
        void finishThread(Thread* thread);
        static std::ostringstream& writeEscapeValue(std::ostringstream& ss, const std::string& str);
        static bool checkNameCase(const char* name);
        void releaseBuffer();
        void allocateBuffer();
        void signalDump();

        void welcome(const std::string& message) const;
        void hint(const std::string& message) const;
        void error(int code, const std::string& message) const;
        void warning(int code, const std::string& message) const;
        void info(int code, const std::string& message) const;
        void debug(int code, const std::string& message) const;
        void logTrace(int mask, const std::string& message) const;
    };
}

#endif
