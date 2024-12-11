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
#include <vector>

#include "typeLobId.h"
#include "typeXid.h"

#ifndef CTX_H_
#define CTX_H_

namespace OpenLogReplicator {
    class Clock;
    class Metrics;
    class Thread;

    class SwapChunk final {
    public:
        std::vector<uint8_t*> chunks;
        int64_t swappedMin;
        int64_t swappedMax;
        bool release;

        SwapChunk() :
                swappedMin(-1), swappedMax(-1), release(false) {};
    };


    class Ctx final {
    public:
        static constexpr uint64_t BAD_TIMEZONE{0x7FFFFFFFFFFFFFFF};
        static constexpr size_t MEMORY_ALIGNMENT{512};
        static constexpr uint MAX_PATH_LENGTH{2048};

        static constexpr typeCol COLUMN_LIMIT{1000};
        static constexpr typeCol COLUMN_LIMIT_23_0{4096};
        static constexpr uint JSON_PARAMETER_LENGTH{256};
        static constexpr uint JSON_TOPIC_LENGTH{256};

        static constexpr uint JSON_USERNAME_LENGTH{128};
        static constexpr uint JSON_PASSWORD_LENGTH{128};
        static constexpr uint JSON_SERVER_LENGTH{4096};
        static constexpr uint JSON_KEY_LENGTH{4096};
        static constexpr uint JSON_CONDITION_LENGTH{16384};
        static constexpr uint JSON_XID_LENGTH{32};
        static constexpr uint JSON_FORMAT_SEPARATOR_LENGTH{128};
        static constexpr uint JSON_TAG_LENGTH{4096};

        enum class LOCALES {
            TIMESTAMP, MOCK
        };
        enum class LOG {
            SILENT, ERROR, WARNING, INFO, DEBUG
        };
        enum class MEMORY {
            BUILDER, PARSER, READER, TRANSACTIONS
        };
        static constexpr uint MEMORY_COUNT{4};
        enum class DISABLE_CHECKS {
            GRANTS = 1 << 0, SUPPLEMENTAL_LOG = 1 << 1, BLOCK_SUM = 1 << 2, JSON_TAGS = 1 << 3
        };
        enum class REDO_FLAGS {
            ARCH_ONLY = 1 << 0, SCHEMALESS = 1 << 1, ADAPTIVE_SCHEMA = 1 << 2, DIRECT_DISABLE = 1 << 3, IGNORE_DATA_ERRORS = 1 << 4,
            SHOW_DDL = 1 << 5, SHOW_HIDDEN_COLUMNS = 1 << 6, SHOW_GUARD_COLUMNS = 1 << 7, SHOW_NESTED_COLUMNS = 1 << 8, SHOW_UNUSED_COLUMNS = 1 << 9,
            SHOW_INCOMPLETE_TRANSACTIONS = 1 << 10, SHOW_SYSTEM_TRANSACTIONS = 1 << 11, SHOW_CHECKPOINT = 1 << 12, CHECKPOINT_KEEP = 1 << 13,
            VERIFY_SCHEMA = 1 << 14, RAW_COLUMN_DATA = 1 << 15, EXPERIMENTAL_XMLTYPE = 1 << 16, EXPERIMENTAL_JSON = 1 << 17,
            EXPERIMENTAL_NOT_NULL_MISSING = 1 << 18
        };
        enum class TRACE {
            DML = 1 << 0, DUMP = 1 << 1, LOB = 1 << 2, LWN = 1 << 3, THREADS = 1 << 4, SQL = 1 << 5, FILE = 1 << 6, DISK = 1 << 7, PERFORMANCE = 1 << 8,
            TRANSACTION = 1 << 9, REDO = 1 << 10, ARCHIVE_LIST = 1 << 11, SCHEMA_LIST = 1 << 12, WRITER = 1 << 13, CHECKPOINT = 1 << 14, SYSTEM = 1 << 15,
            LOB_DATA = 1 << 16, SLEEP = 1 << 17, CONDITION = 1 << 18
        };

        static constexpr uint64_t MEMORY_CHUNK_SIZE_MB{1};
        static constexpr uint64_t MEMORY_CHUNK_SIZE{MEMORY_CHUNK_SIZE_MB * 1024 * 1024};
        static constexpr uint64_t MEMORY_CHUNK_MIN_MB{32};

        static constexpr time_t UNIX_AD1970_01_01{62167132800L};
        static constexpr time_t UNIX_BC1970_01_01{62167132800L - 365 * 24 * 60 * 60};
        static constexpr time_t UNIX_BC4712_01_01{-210831897600L};
        static constexpr time_t UNIX_AD9999_12_31{253402300799L};

        static constexpr typeSeq ZERO_SEQ{0xFFFFFFFF};
        static constexpr typeScn ZERO_SCN{0xFFFFFFFFFFFFFFFF};
        static constexpr typeBlk ZERO_BLK{0xFFFFFFFF};

        uint trace;
        uint flags;
        uint disableChecks;
        LOG logLevel;

    protected:
        bool bigEndian;

        mutable std::mutex memoryMtx;
        std::condition_variable condOutOfMemory;
        uint8_t** memoryChunks;
        uint64_t memoryChunksMin;
        uint64_t memoryChunksMax;
        uint64_t memoryChunksSwap;
        uint64_t memoryChunksAllocated;
        uint64_t memoryChunksFree;
        uint64_t memoryChunksHWM;
        uint64_t memoryModulesAllocated[MEMORY_COUNT];
        bool outOfMemoryParser;

        std::mutex mtx;
        std::condition_variable condMainLoop;
        std::set<Thread*> threads;
        pthread_t mainThread;

    public:
        uint64_t memoryModulesHWM[MEMORY_COUNT];
        static const char map64[65];
        static const char map64R[256];
        static const std::string memoryModules[MEMORY_COUNT];
        static const int64_t cumDays[12];
        static const int64_t cumDaysLeap[12];

        Metrics* metrics;
        Clock* clock;
        bool version12;
        std::atomic<uint32_t> version;                   // Compatibility level of redo logs
        typeCol columnLimit;
        std::string versionStr;
        std::atomic<uint> dumpRedoLog;
        std::atomic<uint> dumpRawData;
        std::unique_ptr<std::ofstream> dumpStream;
        int64_t dbTimezone;
        int64_t hostTimezone;
        int64_t logTimezone;

        // Memory buffers
        uint64_t memoryChunksReadBufferMax;
        uint64_t memoryChunksReadBufferMin;
        uint64_t memoryChunksUnswapBufferMin;
        uint64_t memoryChunksWriteBufferMax;
        uint64_t memoryChunksWriteBufferMin;

        // Disk read buffers
        uint64_t bufferSizeMax;
        uint64_t bufferSizeFree;
        uint64_t bufferSizeHWM;
        uint64_t suppLogSize;
        // Checkpoint
        uint64_t checkpointIntervalS;
        uint64_t checkpointIntervalMb;
        uint64_t checkpointKeep;
        uint64_t schemaForceInterval;
        // Reader
        uint64_t redoReadSleepUs;
        uint64_t redoVerifyDelayUs;
        uint64_t archReadSleepUs;
        uint archReadTries;
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
        typeTransactionSize transactionSizeMax;
        bool hardShutdown;
        bool softShutdown;
        bool replicatorFinished;
        std::unordered_map<typeLobId, typeXid> lobIdToXidMap;
        Thread* parserThread;
        Thread* writerThread;

        std::unordered_map<typeXid, SwapChunk*> swapChunks;
        std::vector<typeXid> commitedXids;
        std::condition_variable chunksMemoryManager;
        std::condition_variable chunksTransaction;
        uint64_t swappedMB;
        typeXid swappedFlushXid;
        typeXid swappedShrinkXid;
        mutable std::mutex swapMtx;
        std::condition_variable reusedTransactions;

        Ctx();
        virtual ~Ctx();

    protected:
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
        bool isTraceSet(TRACE mask) const {
            return (trace & static_cast<uint>(mask)) != 0;
        }

        bool isFlagSet(REDO_FLAGS mask) const {
            return (flags & static_cast<uint>(mask)) != 0;
        }

        bool isLogLevelAt(LOG level) const {
            return logLevel >= level;
        }

        bool isDisableChecksSet(DISABLE_CHECKS mask) const {
            return (disableChecks & static_cast<uint>(mask)) != 0;
        }

        inline void setBigEndian() {
            bigEndian = true;
        }

        [[nodiscard]] inline bool isBigEndian() const {
            return bigEndian;
        }

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

        void inline logTrace(TRACE mask, const std::string& message) const {
            if (unlikely((trace & static_cast<uint>(mask)) != 0)) {
                logTraceInt(mask, message);
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
        [[nodiscard]] static uint getJsonFieldU(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int getJsonFieldI(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static const char* getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field);

        [[nodiscard]] static const rapidjson::Value& getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint16_t getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int16_t getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint32_t getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int32_t getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint64_t getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int64_t getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint getJsonFieldU(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int getJsonFieldI(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static const char* getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field,
                                                       uint num);

        bool parseTimezone(const char* str, int64_t& out) const;
        std::string timezoneToString(int64_t tz) const;
        time_t valuesToEpoch(int year, int month, int day, int hour, int minute, int second, int tz) const;
        uint64_t epochToIso8601(time_t timestamp, char* buffer, bool addT, bool addZ) const;

        void initialize(uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t memoryReadBufferMaxMb, uint64_t memoryReadBufferMinMb, uint64_t memorySwapMb,
                        uint64_t memoryUnswapBufferMinMb, uint64_t memoryWriteBufferMaxMb, uint64_t memoryWriteBufferMinMb);
        void wakeAllOutOfMemory();
        [[nodiscard]] bool nothingToSwap(Thread* t) const;
        [[nodiscard]] uint64_t getMemoryHWM() const;
        [[nodiscard]] uint64_t getAllocatedMemory() const;
        [[nodiscard]] uint64_t getSwapMemory(Thread* t) const;
        [[nodiscard]] uint64_t getFreeMemory(Thread* t) const;
        [[nodiscard]] uint8_t* getMemoryChunk(Thread* t, MEMORY module, bool swap = false);
        void freeMemoryChunk(Thread* t, MEMORY module, uint8_t* chunk);
        void swappedMemoryInit(Thread* t, typeXid xid);
        [[nodiscard]] uint64_t swappedMemorySize(Thread* t, typeXid xid) const;
        [[nodiscard]] uint8_t* swappedMemoryGet(Thread* t, typeXid xid, int64_t index);
        void swappedMemoryRelease(Thread* t, typeXid xid, int64_t index);
        [[nodiscard]] uint8_t* swappedMemoryGrow(Thread* t, typeXid xid);
        [[nodiscard]] uint8_t* swappedMemoryShrink(Thread* t, typeXid xid);
        void swappedMemoryFlush(Thread* t, typeXid xid);
        void swappedMemoryRemove(Thread* t, typeXid xid);
        void wontSwap(Thread* t);

        void stopHard();
        void stopSoft();
        void mainLoop();
        void mainFinish();
        void printStacktrace();
        void signalHandler(int s);

        bool wakeThreads();
        void spawnThread(Thread* t);
        void finishThread(Thread* t);
        static std::ostringstream& writeEscapeValue(std::ostringstream& ss, const std::string& str);
        static bool checkNameCase(const char* name);
        void signalDump();

        void welcome(const std::string& message) const;
        void hint(const std::string& message) const;
        void error(int code, const std::string& message) const;
        void warning(int code, const std::string& message) const;
        void info(int code, const std::string& message) const;
        void debug(int code, const std::string& message) const;
        void logTraceInt(TRACE mask, const std::string& message) const;
        void printMemoryUsageHWM() const;
        void printMemoryUsageCurrent() const;
    };
}

#ifndef GLOBALS
extern OpenLogReplicator::Ctx::LOCALES OLR_LOCALES;
#endif

#endif
