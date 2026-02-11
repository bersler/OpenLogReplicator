/* Header for Ctx class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef CTX_H_
#define CTX_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <fstream>
#include <memory>
#include <mutex>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "types/LobId.h"
#include "types/Scn.h"
#include "types/Xid.h"

namespace OpenLogReplicator {
    class Clock;
    class Metrics;
    class Thread;

    class SwapChunk final {
    public:
        std::vector<uint8_t*> chunks;
        int64_t swappedMin{-1};
        int64_t swappedMax{-1};
        bool release{false};
    };

    class Ctx final {
    public:
        enum class LOCALES : unsigned char {
            TIMESTAMP,
            MOCK
        };

        enum class LOG : unsigned char {
            SILENT,
            ERROR,
            WARNING,
            INFO,
            DEBUG
        };

        enum class MEMORY : unsigned char {
            BUILDER,
            MISC,
            PARSER,
            READER,
            TRANSACTIONS,
            WRITER
        };

        static constexpr uint MEMORY_COUNT{6};

        enum class DISABLE_CHECKS : unsigned char {
            GRANTS           = 1 << 0,
            SUPPLEMENTAL_LOG = 1 << 1,
            BLOCK_SUM        = 1 << 2,
            JSON_TAGS        = 1 << 3
        };

        enum class REDO_FLAGS : unsigned int {
            ARCH_ONLY                     = 1 << 0,
            SCHEMALESS                    = 1 << 1,
            ADAPTIVE_SCHEMA               = 1 << 2,
            DIRECT_DISABLE                = 1 << 3,
            IGNORE_DATA_ERRORS            = 1 << 4,
            SHOW_DDL                      = 1 << 5,
            SHOW_HIDDEN_COLUMNS           = 1 << 6,
            SHOW_GUARD_COLUMNS            = 1 << 7,
            SHOW_NESTED_COLUMNS           = 1 << 8,
            SHOW_UNUSED_COLUMNS           = 1 << 9,
            SHOW_INCOMPLETE_TRANSACTIONS  = 1 << 10,
            SHOW_SYSTEM_TRANSACTIONS      = 1 << 11,
            SHOW_CHECKPOINT               = 1 << 12,
            CHECKPOINT_KEEP               = 1 << 13,
            VERIFY_SCHEMA                 = 1 << 14,
            RAW_COLUMN_DATA               = 1 << 15,
            EXPERIMENTAL_XMLTYPE          = 1 << 16,
            EXPERIMENTAL_JSON             = 1 << 17,
            EXPERIMENTAL_NOT_NULL_MISSING = 1 << 18
        };

        enum class TRACE : unsigned int {
            DML          = 1 << 0,
            DUMP         = 1 << 1,
            LOB          = 1 << 2,
            LWN          = 1 << 3,
            THREADS      = 1 << 4,
            SQL          = 1 << 5,
            FILE         = 1 << 6,
            DISK         = 1 << 7,
            PERFORMANCE  = 1 << 8,
            TRANSACTION  = 1 << 9,
            REDO         = 1 << 10,
            ARCHIVE_LIST = 1 << 11,
            SCHEMA_LIST  = 1 << 12,
            WRITER       = 1 << 13,
            CHECKPOINT   = 1 << 14,
            SYSTEM       = 1 << 15,
            LOB_DATA     = 1 << 16,
            SLEEP        = 1 << 17,
            CONDITION    = 1 << 18,
            STREAM       = 1 << 19
        };

        static constexpr uint64_t MEMORY_CHUNK_SIZE_MB{1};
        static constexpr uint64_t MEMORY_CHUNK_SIZE{MEMORY_CHUNK_SIZE_MB * 1024 * 1024};
        static constexpr uint64_t MEMORY_CHUNK_MIN_MB{32};

        static constexpr typeBlk ZERO_BLK{0xFFFFFFFF};

        static constexpr uint64_t BAD_TIMEZONE{0x7FFFFFFFFFFFFFFF};
        static constexpr int MIN_BLOCK_SIZE{512};
        static constexpr uint MEMORY_ALIGNMENT{4096};
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

        static const std::string memoryModules[MEMORY_COUNT];

        uint trace{0};
        uint flags{0};
        uint disableChecks{0};
        LOG logLevel{LOG::INFO};

    protected:
        mutable std::mutex memoryMtx;
        std::condition_variable condOutOfMemory;
        uint8_t** memoryChunks{nullptr};
        uint64_t memoryChunksMin{0};
        uint64_t memoryChunksMax{0};
        uint64_t memoryChunksSwap{0};
        uint64_t memoryChunksAllocated{0};
        uint64_t memoryChunksFree{0};
        uint64_t memoryChunksHWM{0};
        uint64_t memoryModulesAllocated[MEMORY_COUNT]{0, 0, 0, 0, 0, 0};

        mutable std::mutex mtx;
        std::condition_variable condMainLoop;
        std::set<Thread*> threads;
        pthread_t mainThread;
        bool outOfMemoryParser{false};
        bool bigEndian{false};

    public:
        uint64_t memoryModulesHWM[MEMORY_COUNT]{0, 0, 0, 0, 0, 0};

        Metrics* metrics{nullptr};
        Clock* clock{nullptr};
        std::string versionStr;
        std::unique_ptr<std::ofstream> dumpStream;
        int64_t dbTimezone{BAD_TIMEZONE};
        int64_t hostTimezone;
        int64_t logTimezone;

        // Memory buffers
        uint64_t memoryChunksReadBufferMax{0};
        uint64_t memoryChunksReadBufferMin{0};
        uint64_t memoryChunksUnswapBufferMin{0};
        uint64_t memoryChunksWriteBufferMax{0};
        uint64_t memoryChunksWriteBufferMin{0};

        // Disk read buffers
        uint64_t bufferSizeMax{0};
        uint64_t bufferSizeFree{0};
        uint64_t bufferSizeHWM{0};
        uint64_t suppLogSize{0};
        // Checkpoint
        uint64_t checkpointIntervalS{600};
        uint64_t checkpointIntervalMb{500};
        uint64_t checkpointKeep{100};
        uint64_t schemaForceInterval{20};
        // Reader
        uint64_t redoReadSleepUs{50000};
        uint64_t redoVerifyDelayUs{0};
        uint64_t archReadSleepUs{10000000};
        uint64_t refreshIntervalUs{10000000};
        // Writer
        uint64_t pollIntervalUs{100000};
        uint64_t queueSize{65536};

        std::atomic<uint32_t> version{0}; // Compatibility level of redo logs
        std::atomic<uint> dumpRedoLog{0};
        std::atomic<uint> dumpRawData{0};
        uint archReadTries{10};

        typeCol columnLimit{COLUMN_LIMIT};

        // Transaction buffer
        std::string dumpPath{"."};
        std::string redoCopyPath;
        uint64_t stopLogSwitches{0};
        uint64_t stopCheckpoints{0};
        uint64_t stopTransactions{0};
        typeTransactionSize transactionSizeMax{0};
        std::unordered_map<LobId, Xid> lobIdToXidMap;
        Thread* parserThread{nullptr};
        Thread* writerThread{nullptr};

        std::unordered_map<Xid, SwapChunk*> swapChunks;
        std::vector<Xid> commitedXids;
        std::condition_variable chunksMemoryManager;
        std::condition_variable chunksTransaction;
        uint64_t swappedMB{0};
        Xid swappedFlushXid{0, 0, 0};
        Xid swappedShrinkXid{0, 0, 0};
        mutable std::mutex swapMtx;
        std::condition_variable reusedTransactions;
        bool version12{false};
        bool hardShutdown{false};
        bool softShutdown{false};
        bool replicatorFinished{false};

        Ctx();
        ~Ctx();

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

        void setBigEndian() {
            bigEndian = true;
        }

        [[nodiscard]] bool isBigEndian() const {
            return bigEndian;
        }

        uint16_t read16(const uint8_t* buf) const {
            if (bigEndian)
                return read16Big(buf);
            return read16Little(buf);
        }

        uint32_t read32(const uint8_t* buf) const {
            if (bigEndian)
                return read32Big(buf);
            return read32Little(buf);
        }

        uint64_t read56(const uint8_t* buf) const {
            if (bigEndian)
                return read56Big(buf);
            return read56Little(buf);
        }

        uint64_t read64(const uint8_t* buf) const {
            if (bigEndian)
                return read64Big(buf);
            return read64Little(buf);
        }

        Scn readScn(const uint8_t* buf) const {
            if (bigEndian)
                return readScnBig(buf);
            return readScnLittle(buf);
        }

        Scn readScnR(const uint8_t* buf) const {
            if (bigEndian)
                return readScnRBig(buf);
            return readScnRLittle(buf);
        }

        void write16(uint8_t* buf, uint16_t val) const {
            if (bigEndian)
                write16Big(buf, val);
            else
                write16Little(buf, val);
        }

        void write32(uint8_t* buf, uint32_t val) const {
            if (bigEndian)
                write32Big(buf, val);
            else
                write32Little(buf, val);
        }

        void write56(uint8_t* buf, uint64_t val) const {
            if (bigEndian)
                write56Big(buf, val);
            else
                write56Little(buf, val);
        }

        void write64(uint8_t* buf, uint64_t val) const {
            if (bigEndian)
                write64Big(buf, val);
            else
                write64Little(buf, val);
        }

        void writeScn(uint8_t* buf, Scn val) const {
            if (bigEndian)
                writeScnBig(buf, val);
            else
                writeScnLittle(buf, val);
        }

        static uint16_t read16Little(const uint8_t* buf) {
            return static_cast<uint16_t>(buf[0]) |
                    (static_cast<uint16_t>(buf[1]) << 8);
        }

        static uint16_t read16Big(const uint8_t* buf) {
            return (static_cast<uint16_t>(buf[0]) << 8) |
                    static_cast<uint16_t>(buf[1]);
        }

        static uint32_t read24Big(const uint8_t* buf) {
            return (static_cast<uint32_t>(buf[0]) << 16) |
                    (static_cast<uint32_t>(buf[1]) << 8) |
                    static_cast<uint32_t>(buf[2]);
        }

        static uint32_t read32Little(const uint8_t* buf) {
            return static_cast<uint32_t>(buf[0]) |
                    (static_cast<uint32_t>(buf[1]) << 8) |
                    (static_cast<uint32_t>(buf[2]) << 16) |
                    (static_cast<uint32_t>(buf[3]) << 24);
        }

        static uint32_t read32Big(const uint8_t* buf) {
            return (static_cast<uint32_t>(buf[0]) << 24) |
                    (static_cast<uint32_t>(buf[1]) << 16) |
                    (static_cast<uint32_t>(buf[2]) << 8) |
                    static_cast<uint32_t>(buf[3]);
        }

        static uint64_t read56Little(const uint8_t* buf) {
            return static_cast<uint64_t>(buf[0]) |
                    (static_cast<uint64_t>(buf[1]) << 8) |
                    (static_cast<uint64_t>(buf[2]) << 16) |
                    (static_cast<uint64_t>(buf[3]) << 24) |
                    (static_cast<uint64_t>(buf[4]) << 32) |
                    (static_cast<uint64_t>(buf[5]) << 40) |
                    (static_cast<uint64_t>(buf[6]) << 48);
        }

        static uint64_t read56Big(const uint8_t* buf) {
            return (static_cast<uint64_t>(buf[0]) << 24) |
                    (static_cast<uint64_t>(buf[1]) << 16) |
                    (static_cast<uint64_t>(buf[2]) << 8) |
                    (static_cast<uint64_t>(buf[3])) |
                    (static_cast<uint64_t>(buf[4]) << 40) |
                    (static_cast<uint64_t>(buf[5]) << 32) |
                    (static_cast<uint64_t>(buf[6]) << 48);
        }

        static uint64_t read64Little(const uint8_t* buf) {
            return static_cast<uint64_t>(buf[0]) |
                    (static_cast<uint64_t>(buf[1]) << 8) |
                    (static_cast<uint64_t>(buf[2]) << 16) |
                    (static_cast<uint64_t>(buf[3]) << 24) |
                    (static_cast<uint64_t>(buf[4]) << 32) |
                    (static_cast<uint64_t>(buf[5]) << 40) |
                    (static_cast<uint64_t>(buf[6]) << 48) |
                    (static_cast<uint64_t>(buf[7]) << 56);
        }

        static uint64_t read64Big(const uint8_t* buf) {
            return (static_cast<uint64_t>(buf[0]) << 56) |
                    (static_cast<uint64_t>(buf[1]) << 48) |
                    (static_cast<uint64_t>(buf[2]) << 40) |
                    (static_cast<uint64_t>(buf[3]) << 32) |
                    (static_cast<uint64_t>(buf[4]) << 24) |
                    (static_cast<uint64_t>(buf[5]) << 16) |
                    (static_cast<uint64_t>(buf[6]) << 8) |
                    static_cast<uint64_t>(buf[7]);
        }

        static Scn readScnLittle(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[5] & 0x80) == 0x80)
                return Scn(buf[0], buf[1], buf[2], buf[3], buf[6], buf[7], buf[4], buf[5] & 0x7F);
            return Scn(buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]);
        }

        static Scn readScnBig(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[4] & 0x80) == 0x80)
                return Scn(buf[3], buf[2], buf[1], buf[0], buf[7], buf[6], buf[5], buf[4] & 0x7F);
            return Scn(buf[3], buf[2], buf[1], buf[0], buf[5], buf[4]);
        }

        static Scn readScnRLittle(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[1] & 0x80) == 0x80)
                return Scn(buf[2], buf[3], buf[4], buf[5], 0, 0, buf[0], buf[1] & 0x7F);
            return Scn(buf[2], buf[3], buf[4], buf[5], buf[0], buf[1]);
        }

        static Scn readScnRBig(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[0] & 0x80) == 0x80)
                return Scn(buf[5], buf[4], buf[3], buf[2], 0, 0, buf[1], buf[0] & 0x7F);
            return Scn(buf[5], buf[4], buf[3], buf[2], buf[1], buf[0]);
        }

        static void write16Little(uint8_t* buf, uint16_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
        }

        static void write16Big(uint8_t* buf, uint16_t val) {
            buf[0] = (val >> 8) & 0xFF;
            buf[1] = val & 0xFF;
        }

        static void write32Little(uint8_t* buf, uint32_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
        }

        static void write32Big(uint8_t* buf, uint32_t val) {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
        }

        static void write56Little(uint8_t* buf, uint64_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
        }

        static void write56Big(uint8_t* buf, uint64_t val) {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
            buf[4] = (val >> 40) & 0xFF;
            buf[5] = (val >> 32) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
        }

        static void write64Little(uint8_t* buf, uint64_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
            buf[7] = (val >> 56) & 0xFF;
        }

        static void write64Big(uint8_t* buf, uint64_t val) {
            buf[0] = (val >> 56) & 0xFF;
            buf[1] = (val >> 48) & 0xFF;
            buf[2] = (val >> 40) & 0xFF;
            buf[3] = (val >> 32) & 0xFF;
            buf[4] = (val >> 24) & 0xFF;
            buf[5] = (val >> 16) & 0xFF;
            buf[6] = (val >> 8) & 0xFF;
            buf[7] = val & 0xFF;
        }

        static void writeScnLittle(uint8_t* buf, Scn val) {
            if (val.getData() < 0x800000000000) {
                buf[0] = val.getData() & 0xFF;
                buf[1] = (val.getData() >> 8) & 0xFF;
                buf[2] = (val.getData() >> 16) & 0xFF;
                buf[3] = (val.getData() >> 24) & 0xFF;
                buf[4] = (val.getData() >> 32) & 0xFF;
                buf[5] = (val.getData() >> 40) & 0xFF;
            } else {
                buf[0] = val.getData() & 0xFF;
                buf[1] = (val.getData() >> 8) & 0xFF;
                buf[2] = (val.getData() >> 16) & 0xFF;
                buf[3] = (val.getData() >> 24) & 0xFF;
                buf[4] = (val.getData() >> 48) & 0xFF;
                buf[5] = ((val.getData() >> 56) & 0x7F) | 0x80;
                buf[6] = (val.getData() >> 32) & 0xFF;
                buf[7] = (val.getData() >> 40) & 0xFF;
            }
        }

        static void writeScnBig(uint8_t* buf, Scn val) {
            if (val.getData() < 0x800000000000) {
                buf[0] = (val.getData() >> 24) & 0xFF;
                buf[1] = (val.getData() >> 16) & 0xFF;
                buf[2] = (val.getData() >> 8) & 0xFF;
                buf[3] = val.getData() & 0xFF;
                buf[4] = (val.getData() >> 40) & 0xFF;
                buf[5] = (val.getData() >> 32) & 0xFF;
            } else {
                buf[0] = (val.getData() >> 24) & 0xFF;
                buf[1] = (val.getData() >> 16) & 0xFF;
                buf[2] = (val.getData() >> 8) & 0xFF;
                buf[3] = val.getData() & 0xFF;
                buf[4] = ((val.getData() >> 56) & 0x7F) | 0x80;
                buf[5] = (val.getData() >> 48) & 0xFF;
                buf[6] = (val.getData() >> 40) & 0xFF;
                buf[7] = (val.getData() >> 32) & 0xFF;
            }
        }

        void logTrace(TRACE mask, const std::string& message) const {
            if (unlikely((trace & static_cast<uint>(mask)) != 0)) {
                logTraceInt(mask, message);
            }
        }

        void assertDebug(bool condition) {
            if constexpr (CTXASSERT == 1) {
                if (unlikely(!condition)) {
                    printStacktrace();
                    throw std::runtime_error("Assertion failed");
                }
            }
        }

        static void checkJsonFields(const std::string& fileName, const rapidjson::Value& value, const std::vector<std::string>& names);
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
        [[nodiscard]] static std::string getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field);

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
        [[nodiscard]] static std::string getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field,
                                                       uint num);

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
        void swappedMemoryInit(Thread* t, Xid xid);
        [[nodiscard]] uint64_t swappedMemorySize(Thread* t, Xid xid) const;
        [[nodiscard]] uint8_t* swappedMemoryGet(Thread* t, Xid xid, int64_t index);
        void swappedMemoryRelease(Thread* t, Xid xid, int64_t index);
        [[nodiscard]] uint8_t* swappedMemoryGrow(Thread* t, Xid xid);
        [[nodiscard]] uint8_t* swappedMemoryShrink(Thread* t, Xid xid);
        void swappedMemoryFlush(Thread* t, Xid xid);
        void swappedMemoryRemove(Thread* t, Xid xid);
        void wontSwap(Thread* t) const;

        void stopHard();
        void stopSoft();
        void mainLoop();
        void mainFinish();
        void printStacktrace();
        void signalHandler(int s);

        bool wakeThreads();
        void spawnThread(Thread* t);
        void finishThread(Thread* t);
        void signalDump() const;
        void usleepInt(uint64_t usec) const;

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
