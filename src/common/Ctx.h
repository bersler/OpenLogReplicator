/* Header for Ctx class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <mutex>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <thread>
#include <unordered_map>

#include "types.h"
#include "typeLobId.h"
#include "typeXid.h"

#ifndef CTX_H_
#define CTX_H_

#define REDO_VERSION_12_1       0x0C100000
#define REDO_VERSION_12_2       0x0C200000
#define REDO_VERSION_18_0       0x12000000
#define REDO_VERSION_19_0       0x13000000

#define MEMORY_CHUNK_SIZE_MB                    1
#define MEMORY_CHUNK_SIZE                       (MEMORY_CHUNK_SIZE_MB*1024*1024)
#define MEMORY_CHUNK_MIN_MB                     16

#define OLR_LOCALES_TIMESTAMP                   0
#define OLR_LOCALES_MOCK                        1

#define ALL(__x)                                {                                                                       \
                                                    if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {                         \
                                                        std::ostringstream __s;                                         \
                                                        time_t __now = time(nullptr);                                   \
                                                        tm __nowTm = *localtime(&__now);                                \
                                                        char __str[50];                                                 \
                                                        strftime(__str, sizeof(__str), "%F %T", &__nowTm);              \
                                                        __s << __str << " [INFO] " << __x << std::endl;                 \
                                                        std::cerr << __s.str();                                         \
                                                    } else {                                                            \
                                                        std::ostringstream __s;                                         \
                                                        __s << "[INFO] " << __x << std::endl;                           \
                                                        std::cerr << __s.str();                                         \
                                                    }                                                                   \
                                                }

#define ERROR(__x)                              {                                                                       \
                                                    if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {                         \
                                                        std::ostringstream __s;                                         \
                                                        time_t __now = time(nullptr);                                   \
                                                        tm __nowTm = *localtime(&__now);                                \
                                                        char __str[50];                                                 \
                                                        strftime(__str, sizeof(__str), "%F %T", &__nowTm);              \
                                                        __s << __str << " [ERROR] " << __x << std::endl;                \
                                                        std::cerr << __s.str();                                         \
                                                    } else {                                                            \
                                                        std::ostringstream __s;                                         \
                                                        __s << "[ERROR] " << __x << std::endl;                          \
                                                        std::cerr << __s.str();                                         \
                                                    }                                                                   \
                                                }

#define WARNING(__x)                            {                                                                       \
                                                    if (ctx->trace >= TRACE_WARNING) {                                  \
                                                        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {                     \
                                                            std::ostringstream __s;                                     \
                                                            time_t __now = time(nullptr);                               \
                                                            tm __nowTm = *localtime(&__now);                            \
                                                            char __str[50];                                             \
                                                            strftime(__str, sizeof(__str), "%F %T", &__nowTm);          \
                                                            __s << __str << " [WARNING] " << __x << std::endl;          \
                                                            std::cerr << __s.str();                                     \
                                                        } else {                                                        \
                                                            std::ostringstream __s;                                     \
                                                            __s << "[WARNING] " << __x << std::endl;                    \
                                                            std::cerr << __s.str();                                     \
                                                        }                                                               \
                                                    }                                                                   \
                                                }

#define INFO(__x)                               {                                                                       \
                                                    if (ctx->trace >= TRACE_INFO) {                                     \
                                                        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {                     \
                                                            std::ostringstream __s;                                     \
                                                            time_t __now = time(nullptr);                               \
                                                            tm __nowTm = *localtime(&__now);                            \
                                                            char __str[50];                                             \
                                                            strftime(__str, sizeof(__str), "%F %T", &__nowTm);          \
                                                            __s << __str << " [INFO] " << __x << std::endl;             \
                                                            std::cerr << __s.str();                                     \
                                                        } else {                                                        \
                                                            std::ostringstream __s;                                     \
                                                            __s << "[INFO] " << __x << std::endl;                       \
                                                            std::cerr << __s.str();                                     \
                                                        }                                                               \
                                                    }                                                                   \
                                                }

#define DEBUG(__x)                              {                                                                       \
                                                    if (ctx->trace >= TRACE_DEBUG) {                                    \
                                                        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {                     \
                                                            std::ostringstream __s;                                     \
                                                            time_t __now = time(nullptr);                               \
                                                            tm __nowTm = *localtime(&__now);                            \
                                                            char __str[50];                                             \
                                                            strftime(__str, sizeof(__str), "%F %T", &__nowTm);          \
                                                            __s << __str << " [DEBUG] " << __x << std::endl;            \
                                                            std::cerr << __s.str();                                     \
                                                        } else {                                                        \
                                                            std::ostringstream __s;                                     \
                                                            __s << "[DEBUG] " << __x << std::endl;                      \
                                                            std::cerr << __s.str();                                     \
                                                        }                                                               \
                                                    }                                                                   \
                                                }

#define TRACE(__t,__x)                          {                                                                       \
                                                    if ((ctx->trace2 & (__t)) != 0) {                                   \
                                                        if (OLR_LOCALES == OLR_LOCALES_TIMESTAMP) {                     \
                                                            std::ostringstream __s;                                     \
                                                            time_t __now = time(nullptr);                               \
                                                            tm __nowTm = *localtime(&__now);                            \
                                                            char __str[50];                                             \
                                                            strftime(__str, sizeof(__str), "%F %T", &__nowTm);          \
                                                            __s << __str << " [TRACE] " << __x << std::endl;            \
                                                            std::cerr << __s.str();                                     \
                                                        } else {                                                        \
                                                            std::ostringstream __s;                                     \
                                                            __s << "[TRACE] " << __x << std::endl;                      \
                                                            std::cerr << __s.str();                                     \
                                                        }                                                               \
                                                    }                                                                   \
                                                }

#define TRACE_SILENT                            0
#define TRACE_ERROR                             1
#define TRACE_WARNING                           2
#define TRACE_INFO                              3
#define TRACE_DEBUG                             4
#define TRACE_MOCK                              0x80000000

#define TRACE2_DML                              0x00000001
#define TRACE2_DUMP                             0x00000002
#define TRACE2_LOB                              0x00000004
#define TRACE2_LWN                              0x00000008
#define TRACE2_THREADS                          0x00000010
#define TRACE2_SQL                              0x00000020
#define TRACE2_FILE                             0x00000040
#define TRACE2_DISK                             0x00000080
#define TRACE2_PERFORMANCE                      0x00000100
#define TRACE2_TRANSACTION                      0x00000200
#define TRACE2_REDO                             0x00000400
#define TRACE2_ARCHIVE_LIST                     0x00000800
#define TRACE2_SCHEMA_LIST                      0x00001000
#define TRACE2_WRITER                           0x00002000
#define TRACE2_CHECKPOINT                       0x00004000
#define TRACE2_SYSTEM                           0x00008000

#define REDO_FLAGS_ARCH_ONLY                    0x00000001
#define REDO_FLAGS_SCHEMALESS                   0x00000002
#define REDO_FLAGS_ADAPTIVE_SCHEMA              0x00000004
#define REDO_FLAGS_DIRECT_DISABLE               0x00000008
#define REDO_FLAGS_IGNORE_DATA_ERRORS           0x00000010
#define REDO_FLAGS_TRACK_DDL                    0x00000020
#define REDO_FLAGS_SHOW_INVISIBLE_COLUMNS       0x00000040
#define REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS      0x00000080
#define REDO_FLAGS_SHOW_NESTED_COLUMNS          0x00000100
#define REDO_FLAGS_SHOW_UNUSED_COLUMNS          0x00000200
#define REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS 0x00000400
#define REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS     0x00000800
#define REDO_FLAGS_HIDE_CHECKPOINT              0x00001000
#define REDO_FLAGS_CHECKPOINT_KEEP              0x00002000
#define REDO_FLAGS_VERIFY_SCHEMA                0x00004000
#define REDO_FLAGS_RAW_COLUMN_DATA              0x00008000

#define FLAG(x)                                 ((ctx->flags&(x))!=0)

#define DISABLE_CHECKS_GRANTS                   0x00000001
#define DISABLE_CHECKS_SUPPLEMENTAL_LOG         0x00000002
#define DISABLE_CHECKS_BLOCK_SUM                0x00000004
#define DISABLE_CHECKS(x)                       ((ctx->disableChecks&(x))!=0)

#ifndef GLOBALS
extern uint64_t OLR_LOCALES;
#endif

namespace OpenLogReplicator {
    class Thread;

    class Ctx {
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

        std::condition_variable condMainLoop;
        std::condition_variable condOutOfMemory;
        std::mutex mtx;
        std::mutex memoryMtx;
        std::set<Thread*> threads;
        pthread_t mainThread;

    public:
        static const char map64[65];
        static const char map16[17];
        static const char map10[11];

        bool version12;
        std::atomic<uint64_t> version;                   // Compatibility level of redo logs
        std::string versionStr;
        std::atomic<uint64_t> dumpRedoLog;
        std::atomic<uint64_t> dumpRawData;
        std::ofstream dumpStream;

        void setBigEndian();
        [[nodiscard]] bool isBigEndian() const;

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
        std::atomic<uint64_t> trace;
        std::atomic<uint64_t> trace2;
        std::atomic<uint64_t> flags;
        std::atomic<uint64_t> disableChecks;
        std::atomic<bool> experimentalLobs;
        std::atomic<bool> hardShutdown;
        std::atomic<bool> softShutdown;
        std::atomic<bool> replicatorFinished;
        std::unordered_map<typeLobId, typeXid> lobIdToXidMap;

        Ctx();
        virtual ~Ctx();

        uint16_t (*read16)(const uint8_t* buf);
        uint32_t (*read32)(const uint8_t* buf);
        uint64_t (*read56)(const uint8_t* buf);
        uint64_t (*read64)(const uint8_t* buf);
        typeScn (*readScn)(const uint8_t* buf);
        typeScn (*readScnR)(const uint8_t* buf);
        void (*write16)(uint8_t* buf, uint16_t val);
        void (*write32)(uint8_t* buf, uint32_t val);
        void (*write56)(uint8_t* buf, uint64_t val);
        void (*write64)(uint8_t* buf, uint64_t val);
        void (*writeScn)(uint8_t* buf, typeScn val);

        static uint16_t read16Little(const uint8_t* buf);
        static uint16_t read16Big(const uint8_t* buf);
        static uint32_t read24Big(const uint8_t* buf);
        static uint32_t read32Little(const uint8_t* buf);
        static uint32_t read32Big(const uint8_t* buf);
        static uint64_t read56Little(const uint8_t* buf);
        static uint64_t read56Big(const uint8_t* buf);
        static uint64_t read64Little(const uint8_t* buf);
        static uint64_t read64Big(const uint8_t* buf);
        static typeScn readScnLittle(const uint8_t* buf);
        static typeScn readScnBig(const uint8_t* buf);
        static typeScn readScnRLittle(const uint8_t* buf);
        static typeScn readScnRBig(const uint8_t* buf);

        static void write16Little(uint8_t* buf, uint16_t val);
        static void write16Big(uint8_t* buf, uint16_t val);
        static void write32Little(uint8_t* buf, uint32_t val);
        static void write32Big(uint8_t* buf, uint32_t val);
        static void write56Little(uint8_t* buf, uint64_t val);
        static void write56Big(uint8_t* buf, uint64_t val);
        static void write64Little(uint8_t* buf, uint64_t val);
        static void write64Big(uint8_t* buf, uint64_t val);
        static void writeScnLittle(uint8_t* buf, typeScn val);
        static void writeScnBig(uint8_t* buf, typeScn val);

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
        [[nodiscard]] static const char* getJsonFieldS(const std::string& fileName, uint64_t maxLength, const rapidjson::Value& value, const char* field, uint64_t num);

        void initialize(uint64_t newMemoryMinMb, uint64_t newMemoryMaxMb, uint64_t newReadBufferMax);
        void wakeAllOutOfMemory();
        [[nodiscard]] uint64_t getMaxUsedMemory() const;
        [[nodiscard]] uint64_t getAllocatedMemory() const;
        [[nodiscard]] uint64_t getFreeMemory();
        [[nodiscard]] uint8_t* getMemoryChunk(const char* module, bool reusable);
        void freeMemoryChunk(const char* module, uint8_t* chunk, bool reusable);
        void stopHard();
        void stopSoft();
        void mainLoop();
        void mainFinish();
        void printStacktrace();
        void signalHandler(int s);

        bool wakeThreads();
        void spawnThread(Thread* thread);
        void finishThread(Thread* thread);
        static std::ostringstream& writeEscapeValue(std::ostringstream& ss, std::string& str);
        static bool checkNameCase(const char* name);
        void releaseBuffer();
        void allocateBuffer();
        void signalDump();
    };
}

#endif
