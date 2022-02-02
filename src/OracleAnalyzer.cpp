/* Thread reading Oracle Redo Logs using offline mode
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

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "global.h"
#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleIncarnation.h"
#include "OutputBuffer.h"
#include "ReaderFilesystem.h"
#include "RedoLog.h"
#include "RedoLogException.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "State.h"
#include "SystemTransaction.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    OracleAnalyzer::OracleAnalyzer(OutputBuffer* outputBuffer, uint64_t dumpRedoLog, uint64_t dumpRawData, const char* dumpPath,
            const char* alias, const char* database, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t readBufferMax,
            uint64_t disableChecks) :
        Thread(alias),
        sequence(ZERO_SEQ),
        offset(0),
        nextScn(ZERO_SCN),
        suppLogDbPrimary(0),
        suppLogDbAll(0),
        memoryMinMb(memoryMinMb),
        memoryMaxMb(memoryMaxMb),
        memoryChunks(nullptr),
        memoryChunksMin(memoryMinMb / MEMORY_CHUNK_SIZE_MB),
        memoryChunksAllocated(0),
        memoryChunksFree(0),
        memoryChunksMax(memoryMaxMb / MEMORY_CHUNK_SIZE_MB),
        memoryChunksHWM(0),
        memoryChunksSupplemental(0),
        database(database),
        dbBlockChecksum(""),
        logArchiveFormat("o1_mf_%t_%s_%h_.arc"),
        redoCopyPath(""),
        state(nullptr),
        checkpointIntervalS(600),
        checkpointIntervalMB(100),
        checkpointFirst(1),
        checkpointAll(false),
        checkpointOutputCheckpoint(true),
        checkpointOutputLogSwitch(true),
        checkpointLastTime(0),
        checkpointLastOffset(0),
        archReader(nullptr),
        waitingForWriter(false),
        context(""),
        firstScn(ZERO_SCN),
        checkpointScn(ZERO_SCN),
        schemaScn(ZERO_SCN),
        schemaFirstScn(ZERO_SCN),
        startScn(ZERO_SCN),
        startSequence(ZERO_SEQ),
        startTimeRel(0),
        readBufferMax(readBufferMax),
        transactionBuffer(nullptr),
        schema(nullptr),
        outputBuffer(outputBuffer),
        systemTransaction(nullptr),
        dumpRedoLog(dumpRedoLog),
        dumpRawData(dumpRawData),
        dumpPath(dumpPath),
        flags(0),
        disableChecks(disableChecks),
        redoReadSleepUs(50000),
        archReadSleepUs(10000000),
        archReadTries(10),
        redoVerifyDelayUs((flags & REDO_FLAGS_DIRECT_DISABLE) != 0 ? 500000 : 0),
        refreshIntervalUs(10000000),
        version(0),
        conId(-1),
        resetlogs(0),
        activation(0),
        stopLogSwitches(0),
        stopCheckpoints(0),
        stopTransactions(0),
        transactionMax(0),
        stopFlushBuffer(false),
        oiCurrent(nullptr),
        bigEndian(false),
        suppLogSize(0),
        version12(false),
        schemaChanged(false),
        activationChanged(false),
        archGetLog(archGetLogPath),
        read16(read16Little),
        read32(read32Little),
        read56(read56Little),
        read64(read64Little),
        readSCN(readSCNLittle),
        readSCNr(readSCNrLittle),
        write16(write16Little),
        write32(write32Little),
        write56(write56Little),
        write64(write64Little),
        writeSCN(writeSCNLittle) {
    }

    OracleAnalyzer::~OracleAnalyzer() {
        readerDropAll();

        if (systemTransaction != nullptr) {
            delete systemTransaction;
            systemTransaction = nullptr;
        }

        while (!archiveRedoQueue.empty()) {
            RedoLog* redoTmp = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete redoTmp;
        }

        for (RedoLog* onlineRedo : onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        for (auto it : xidTransactionMap) {
            Transaction* transaction = it.second;
            delete transaction;
        }
        xidTransactionMap.clear();

        if (transactionBuffer != nullptr) {
            delete transactionBuffer;
            transactionBuffer = nullptr;
        }

        while (memoryChunksAllocated > 0) {
            --memoryChunksAllocated;
            free(memoryChunks[memoryChunksAllocated]);
            memoryChunks[memoryChunksAllocated] = nullptr;
        }

        if (memoryChunks != nullptr) {
            delete[] memoryChunks;
            memoryChunks = nullptr;
        }

        if (schema != nullptr) {
            delete schema;
            schema = nullptr;
        }

        if (state != nullptr) {
            delete state;
            state = nullptr;
        }

        for (OracleIncarnation* oi : oiSet)
            delete oi;
        oiSet.clear();
        oiCurrent = nullptr;

        pathMapping.clear();
        redoLogsBatch.clear();
        checkpointScnList.clear();
        skipXidList.clear();
        brokenXidMapList.clear();
    }

    void OracleAnalyzer::initialize(void) {
        memoryChunks = new uint8_t*[memoryMaxMb / MEMORY_CHUNK_SIZE_MB];
        if (memoryChunks == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) << " bytes memory (for: memory chunks#1)");
        }

        for (uint64_t i = 0; i < memoryChunksMin; ++i) {
            memoryChunks[i] = (uint8_t*) aligned_alloc(MEMORY_ALIGNMENT, MEMORY_CHUNK_SIZE);

            if (memoryChunks[i] == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << MEMORY_CHUNK_SIZE_MB << " bytes memory (for: memory chunks#2)");
            }
            ++memoryChunksAllocated;
            ++memoryChunksFree;
        }
        memoryChunksHWM = memoryChunksMin;

        transactionBuffer = new TransactionBuffer(this);
        if (transactionBuffer == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(TransactionBuffer) << " bytes memory (for: memory chunks#5)");
        }

        schema = new Schema(this);
        if (schema == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(Schema) << " bytes memory (for: schema)");
        }
    }

    void OracleAnalyzer::updateOnlineLogs(void) {
        for (RedoLog* onlineRedo : onlineRedoSet) {
            if (!readerUpdateRedoLog(onlineRedo->reader)) {
                RUNTIME_FAIL("updating failed for " << std::dec << onlineRedo->path);
            } else {
                onlineRedo->sequence = onlineRedo->reader->sequence;
                onlineRedo->firstScn = onlineRedo->reader->firstScn;
                onlineRedo->nextScn = onlineRedo->reader->nextScn;
            }
        }
    }

    uint16_t OracleAnalyzer::read16Little(const uint8_t* buf) {
        return (uint16_t)buf[0] | ((uint16_t)buf[1] << 8);
    }

    uint16_t OracleAnalyzer::read16Big(const uint8_t* buf) {
        return ((uint16_t)buf[0] << 8) | (uint16_t)buf[1];
    }

    uint32_t OracleAnalyzer::read32Little(const uint8_t* buf) {
        return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
                ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
    }

    uint32_t OracleAnalyzer::read32Big(const uint8_t* buf) {
        return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
                ((uint32_t)buf[2] << 8) | (uint32_t)buf[3];
    }

    uint64_t OracleAnalyzer::read56Little(const uint8_t* buf) {
        return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
                ((uint64_t)buf[6] << 48);
    }

    uint64_t OracleAnalyzer::read56Big(const uint8_t* buf) {
        return (((uint64_t)buf[0] << 24) | ((uint64_t)buf[1] << 16) |
                ((uint64_t)buf[2] << 8) | ((uint64_t)buf[3]) |
                ((uint64_t)buf[4] << 40) | ((uint64_t)buf[5] << 32) |
                ((uint64_t)buf[6] << 48));
    }

    uint64_t OracleAnalyzer::read64Little(const uint8_t* buf) {
        return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
                ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
                ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
                ((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
    }

    uint64_t OracleAnalyzer::read64Big(const uint8_t* buf) {
        return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
                ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
                ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
                ((uint64_t)buf[6] << 8) | (uint64_t)buf[7];
    }

    typeSCN OracleAnalyzer::readSCNLittle(const uint8_t* buf) {
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

    typeSCN OracleAnalyzer::readSCNBig(const uint8_t* buf) {
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

    typeSCN OracleAnalyzer::readSCNrLittle(const uint8_t* buf) {
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

    typeSCN OracleAnalyzer::readSCNrBig(const uint8_t* buf) {
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

    void OracleAnalyzer::write16Little(uint8_t* buf, uint16_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
    }

    void OracleAnalyzer::write16Big(uint8_t* buf, uint16_t val) {
        buf[0] = (val >> 8) & 0xFF;
        buf[1] = val & 0xFF;
    }

    void OracleAnalyzer::write32Little(uint8_t* buf, uint32_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
    }

    void OracleAnalyzer::write32Big(uint8_t* buf, uint32_t val) {
        buf[0] = (val >> 24) & 0xFF;
        buf[1] = (val >> 16) & 0xFF;
        buf[2] = (val >> 8) & 0xFF;
        buf[3] = val & 0xFF;
    }

    void OracleAnalyzer::write56Little(uint8_t* buf, uint64_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 32) & 0xFF;
        buf[5] = (val >> 40) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
    }

    void OracleAnalyzer::write56Big(uint8_t* buf, uint64_t val) {
        buf[0] = (val >> 48) & 0xFF;
        buf[1] = (val >> 40) & 0xFF;
        buf[2] = (val >> 32) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 16) & 0xFF;
        buf[5] = (val >> 8) & 0xFF;
        buf[6] = val & 0xFF;
    }

    void OracleAnalyzer::write64Little(uint8_t* buf, uint64_t val) {
        buf[0] = val & 0xFF;
        buf[1] = (val >> 8) & 0xFF;
        buf[2] = (val >> 16) & 0xFF;
        buf[3] = (val >> 24) & 0xFF;
        buf[4] = (val >> 32) & 0xFF;
        buf[5] = (val >> 40) & 0xFF;
        buf[6] = (val >> 48) & 0xFF;
        buf[7] = (val >> 56) & 0xFF;
    }

    void OracleAnalyzer::write64Big(uint8_t* buf, uint64_t val) {
        buf[0] = (val >> 56) & 0xFF;
        buf[1] = (val >> 48) & 0xFF;
        buf[2] = (val >> 40) & 0xFF;
        buf[3] = (val >> 32) & 0xFF;
        buf[4] = (val >> 24) & 0xFF;
        buf[5] = (val >> 16) & 0xFF;
        buf[6] = (val >> 8) & 0xFF;
        buf[7] = val & 0xFF;
    }

    void OracleAnalyzer::writeSCNLittle(uint8_t* buf, typeSCN val) {
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
            buf[5] = ((val >> 56) & 0xFF) | 0x80;
            buf[6] = (val >> 32) & 0xFF;
            buf[7] = (val >> 40) & 0xFF;
        }
    }

    void OracleAnalyzer::writeSCNBig(uint8_t* buf, typeSCN val) {
        if (val < 0x800000000000) {
            buf[5] = val & 0xFF;
            buf[4] = (val >> 8) & 0xFF;
            buf[3] = (val >> 16) & 0xFF;
            buf[2] = (val >> 24) & 0xFF;
            buf[1] = (val >> 32) & 0xFF;
            buf[0] = (val >> 40) & 0xFF;
        } else {
            buf[5] = val & 0xFF;
            buf[4] = (val >> 8) & 0xFF;
            buf[3] = (val >> 16) & 0xFF;
            buf[2] = (val >> 24) & 0xFF;
            buf[1] = (val >> 48) & 0xFF;
            buf[0] = ((val >> 56) & 0xFF) | 0x80;
            buf[7] = (val >> 32) & 0xFF;
            buf[6] = (val >> 40) & 0xFF;
        }
    }

    void OracleAnalyzer::setBigEndian(void) {
        bigEndian = true;
        read16 = read16Big;
        read32 = read32Big;
        read56 = read56Big;
        read64 = read64Big;
        readSCN = readSCNBig;
        readSCNr = readSCNrBig;
        write16 = write16Big;
        write32 = write32Big;
        write56 = write56Big;
        write64 = write64Big;
        writeSCN = writeSCNBig;
    }

    bool OracleAnalyzer::isBigEndian(void) {
        return bigEndian;
    }

    void OracleAnalyzer::loadDatabaseMetadata(void) {
        archReader = readerCreate(0);
    }

    void OracleAnalyzer::positionReader(void) {
        if (startTime.length() > 0) {
            RUNTIME_FAIL("starting by time is not supported for offline mode");
        } else if (startTimeRel > 0) {
            RUNTIME_FAIL("starting by relative time is not supported for offline mode");
        }

        if (startSequence != ZERO_SEQ)
            sequence = startSequence;
        else
            sequence = 0;
        offset = 0;
    }

    void OracleAnalyzer::createSchema(void) {
        if ((flags & REDO_FLAGS_SCHEMALESS) != 0)
            return;

        RUNTIME_FAIL("schema file missing");
    }

    void OracleAnalyzer::updateOnlineRedoLogData(void) {
        //nothing here
    }

    void* OracleAnalyzer::run(void) {
        TRACE(TRACE2_THREADS, "THREADS: ANALYZER (" << std::hex << std::this_thread::get_id() << ") START");

        try {
            loadDatabaseMetadata();

            while (firstScn == ZERO_SCN) {
                {
                    std::unique_lock<std::mutex> lck(mtx);
                    if (startScn == ZERO_SCN && startSequence == ZERO_SEQ && startTime.length() == 0 && startTimeRel == 0)
                        writerCond.wait(lck);
                }

                if (shutdown)
                    return 0;

                std::string flagsStr;
                if (flags) {
                    flagsStr = " (flags: " + std::to_string(flags) + ")";
                }

                std::string starting;
                if (startTime.length() > 0)
                    starting = "time: " + startTime;
                else if (startTimeRel > 0)
                    starting = "time-rel: " + std::to_string(startTimeRel);
                else if (startScn != ZERO_SCN)
                    starting = "scn: " + std::to_string(startScn);
                else
                    starting = "now";

                std::string startingSeq;
                if (startSequence != ZERO_SEQ)
                    startingSeq = ", seq: " + std::to_string(startSequence);

                INFO("Oracle Analyzer for " << database << " in " << getModeName() << " mode is starting" << flagsStr << " from " << starting << startingSeq);

                if (shutdown)
                    return 0;

                readCheckpoints();
                if (firstScn == ZERO_SCN || sequence == ZERO_SEQ)
                    positionReader();

                INFO("current resetlogs is: " << std::dec << resetlogs);

                if (!schema->readSchema()) {
                    createSchema();
                    schema->writeSchema();
                }

                if (sequence == ZERO_SEQ) {
                    RUNTIME_FAIL("starting sequence is unknown, failing");
                }

                if (firstScn == ZERO_SCN) {
                    INFO("last confirmed scn: <none>, starting sequence: " << std::dec << sequence << ", offset: " << offset);
                } else {
                    INFO("last confirmed scn: " << std::dec << firstScn << ", starting sequence: " << std::dec << sequence << ", offset: " << offset);
                }

                if ((dbBlockChecksum.compare("OFF") == 0 || dbBlockChecksum.compare("FALSE") == 0) &&
                        (disableChecks & DISABLE_CHECK_BLOCK_SUM) == 0) {
                    WARNING("HINT: set DB_BLOCK_CHECKSUM = TYPICAL on the database"
                            " or turn off consistency checking in OpenLogReplicator setting parameter disable-checks: "
                            << std::dec << DISABLE_CHECK_BLOCK_SUM << " for the reader");
                }

                {
                    std::unique_lock<std::mutex> lck(mtx);
                    outputBuffer->writersCond.notify_all();
                }
            }

            uint64_t ret = REDO_OK;
            RedoLog* redo = nullptr;
            bool logsProcessed;

            while (!shutdown) {
                logsProcessed = false;

                //
                //ARCHIVED REDO LOGS READ
                //
                while (!shutdown) {
                    TRACE(TRACE2_REDO, "REDO: checking archived redo logs, seq: " << std::dec << sequence);
                    updateResetlogs();
                    archGetLog(this);

                    if (archiveRedoQueue.empty()) {
                        if ((flags & REDO_FLAGS_ARCH_ONLY) != 0) {
                            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: archived redo log missing for seq: " << std::dec << sequence << ", sleeping");
                            usleep(archReadSleepUs);
                        } else {
                            break;
                        }
                    }

                    TRACE(TRACE2_REDO, "REDO: searching archived redo log for seq: " << std::dec << sequence);
                    while (!archiveRedoQueue.empty() && !shutdown) {
                        RedoLog* redoPrev = redo;
                        redo = archiveRedoQueue.top();
                        TRACE(TRACE2_REDO, "REDO: " << redo->path << " is seq: " << std::dec << redo->sequence << ", scn: " << std::dec << redo->firstScn);

                        //when no checkpoint exists start processing from first file
                        if (sequence == 0)
                            sequence = redo->sequence;

                        //skip older archived redo logs
                        if (redo->sequence < sequence) {
                            archiveRedoQueue.pop();
                            delete redo;
                            continue;
                        } else if (redo->sequence > sequence) {
                            RUNTIME_FAIL("couldn't find archive log for seq: " << std::dec << sequence << ", found: " << redo->sequence << " instead");
                        }

                        logsProcessed = true;
                        redo->reader = archReader;

                        archReader->fileName = redo->path;
                        uint64_t retry = archReadTries;

                        while (true) {
                            if (readerCheckRedoLog(archReader) && readerUpdateRedoLog(archReader)) {
                                break;
                            }

                            if (retry == 0) {
                                RUNTIME_FAIL("opening archived redo log: " << redo->path);
                            }

                            INFO("archived redo log " << redo->path << " is not ready for read, sleeping " << std::dec << archReadSleepUs << " us");
                            usleep(archReadSleepUs);
                            --retry;
                        }

                        //new activation value after resetlogs operation
                        if (activationChanged) {
                            activationChanged = false;
                            schemaScn = nextScn;
                            schema->writeSchema();
                        }
                        ret = redo->processLog();

                        if (shutdown)
                            break;

                        if (ret != REDO_FINISHED) {
                            if  (ret == REDO_STOPPED) {
                                archiveRedoQueue.pop();
                                delete redo;
                                redo = nullptr;
                                break;
                            }
                            RUNTIME_FAIL("archive log processing returned: " << Reader::REDO_CODE[ret] << " (code: " << std::dec << ret << ")");
                        }

                        ++sequence;
                        archiveRedoQueue.pop();
                        delete redo;
                        redo = nullptr;

                        if (stopLogSwitches > 0) {
                            --stopLogSwitches;
                            if (stopLogSwitches == 0) {
                                INFO("shutdown started - exhausted number of log switches");
                                stopMain();
                                shutdown = true;
                            }
                        }
                    }

                    if (!logsProcessed)
                        break;
                }

                if (!continueWithOnline())
                    break;

                if (shutdown)
                    break;

                //
                //ONLINE REDO LOGS READ
                //
                if ((flags & REDO_FLAGS_ARCH_ONLY) == 0) {
                    TRACE(TRACE2_REDO, "REDO: checking online redo logs, seq: " << std::dec << sequence);
                    updateResetlogs();
                    updateOnlineLogs();

                    while (!shutdown) {
                        redo = nullptr;
                        TRACE(TRACE2_REDO, "REDO: searching online redo log for seq: " << std::dec << sequence);

                        //keep reading online redo logs while it is possible
                        if (redo == nullptr) {
                            bool higher = false;
                            clock_t startTime = getTime();

                            while (!shutdown) {
                                for (RedoLog* onlineRedo : onlineRedoSet) {
                                    if (onlineRedo->reader->sequence > sequence)
                                        higher = true;

                                    if (onlineRedo->reader->sequence == sequence &&
                                            (onlineRedo->reader->numBlocksHeader == ZERO_BLK ||
                                                    offset < onlineRedo->reader->numBlocksHeader * onlineRedo->reader->blockSize)) {
                                        redo = onlineRedo;
                                    }

                                    TRACE(TRACE2_REDO, "REDO: " << onlineRedo->path << " is seq: " << std::dec << onlineRedo->sequence <<
                                            ", scn: " << std::dec << onlineRedo->firstScn << ", blocks: " << std::dec << onlineRedo->reader->numBlocksHeader);
                                }

                                //all so far read, waiting for switch
                                if (redo == nullptr && !higher) {
                                    usleep(redoReadSleepUs);
                                } else
                                    break;

                                if (shutdown)
                                    break;

                                clock_t loopTime = getTime();
                                if (startTime + refreshIntervalUs < loopTime) {
                                    updateOnlineRedoLogData();
                                    updateOnlineLogs();
                                    goStandby();
                                    break;
                                }

                                updateOnlineLogs();
                            }
                        }

                        if (redo == nullptr)
                            break;

                        //if online redo log is overwritten - then switch to reading archive logs
                        if (shutdown)
                            break;
                        logsProcessed = true;

                        ret = redo->processLog();

                        if (shutdown)
                            break;

                        if (ret == REDO_FINISHED) {
                            ++sequence;
                        } else if (ret == REDO_STOPPED) {
                            //nothing here
                        } else if (ret == REDO_OVERWRITTEN) {
                            INFO("online redo log has been overwritten by new data, continuing reading from archived redo log");
                            break;
                        } else {
                            if (redo->group == 0) {
                                RUNTIME_FAIL("read archived redo log");
                            } else {
                                RUNTIME_FAIL("read online redo log");
                            }
                        }

                        if (stopLogSwitches > 0) {
                            --stopLogSwitches;
                            if (stopLogSwitches == 0) {
                                INFO("shutdown initiated by number of log switches");
                                stopMain();
                                shutdown = true;
                            }
                        }
                    }
                }

                if (shutdown)
                    break;

                if (!logsProcessed)
                    usleep(redoReadSleepUs);
            }
        } catch (ConfigurationException& ex) {
            stopMain();
        } catch (RuntimeException& ex) {
            stopMain();
        }

        INFO("Oracle analyzer for: " << database << " is shutting down");

        DEBUG("state at stop: " << *this);
        uint64_t buffersMax = readerDropAll();

        INFO("Oracle analyzer for: " << database << " is shut down, allocated at most " << std::dec <<
                (memoryChunksHWM * MEMORY_CHUNK_SIZE_MB) << "MB memory, max disk read buffer: " << (buffersMax * MEMORY_CHUNK_SIZE_MB) << "MB");

        TRACE(TRACE2_THREADS, "THREADS: ANALYZER (" << std::hex << std::this_thread::get_id() << ") STOP");
        return 0;
    }

    bool OracleAnalyzer::readerCheckRedoLog(Reader* reader) {
        std::unique_lock<std::mutex> lck(mtx);
        reader->status = READER_STATUS_CHECK;
        reader->sequence = 0;
        reader->firstScn = ZERO_SCN;
        reader->nextScn = ZERO_SCN;

        readerCond.notify_all();
        sleepingCond.notify_all();

        while (reader->status == READER_STATUS_CHECK) {
            if (shutdown)
                break;
            analyzerCond.wait(lck);
        }
        if (reader->ret == REDO_OK)
            return true;
        else
            return false;
    }

    uint64_t OracleAnalyzer::readerDropAll(void) {
        uint64_t buffersMaxUsed = 0;
        {
            std::unique_lock<std::mutex> lck(mtx);
            for (Reader* reader : readers)
                reader->shutdown = true;
            readerCond.notify_all();
            sleepingCond.notify_all();
        }
        for (Reader* reader : readers) {
            if (reader->started)
                pthread_join(reader->pthread, nullptr);
            if (reader->buffersMaxUsed > buffersMaxUsed)
                buffersMaxUsed = reader->buffersMaxUsed;
            delete reader;
        }
        archReader = nullptr;
        readers.clear();
        return buffersMaxUsed;
    }

    void OracleAnalyzer::updateResetlogs(void) {
        if (nextScn == ZERO_SCN || offset != 0)
            return;

        OracleIncarnation* oiCurrent = nullptr;
        for (OracleIncarnation* oi : oiSet) {
            if (oi->resetlogs == resetlogs) {
                oiCurrent = oi;
                break;
            }
        }

        //resetlogs is changed
        for (OracleIncarnation* oi : oiSet) {
            if (oi->resetlogsScn == nextScn && oiCurrent->resetlogs == resetlogs && oi->priorIncarnation == oiCurrent->incarnation) {
                INFO("new resetlogs detected: " << std::dec << oi->resetlogs);
                sequence = 1;
                resetlogs = oi->resetlogs;
                activation = 0;
                return;
            }
        }

        if (oiSet.size() == 0)
            return;

        if (oiCurrent == nullptr)
            RUNTIME_FAIL("resetlogs (" << std::dec << resetlogs << ") not found in incarnation list");
    }

    Reader* OracleAnalyzer::readerCreate(int64_t group) {
        for (Reader* reader : readers)
            if (reader->group == group)
                return reader;

        ReaderFilesystem* readerFS = new ReaderFilesystem(alias.c_str(), this, group);
        if (readerFS == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(ReaderFilesystem) << " bytes memory (for: disk reader creation)");
        }
        readers.insert(readerFS);
        readerFS->initialize();

        if (pthread_create(&readerFS->pthread, nullptr, &Reader::runStatic, (void*)readerFS)) {
            CONFIG_FAIL("spawning thread");
        }
        return readerFS;
    }

    void OracleAnalyzer::checkOnlineRedoLogs() {
        for (RedoLog* onlineRedo : onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        for (Reader* reader : readers) {
            if (reader->group == 0)
                continue;

            bool foundPath = false;
            for (std::string& path : reader->paths) {
                reader->fileName = path;
                applyMapping(reader->fileName);
                if (readerCheckRedoLog(reader)) {
                    foundPath = true;
                    RedoLog* redo = new RedoLog(this, reader->group, reader->fileName);
                    if (redo == nullptr) {
                        readerDropAll();
                        RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(RedoLog) << " bytes memory (for: online redo logs)");
                    }

                    redo->reader = reader;
                    INFO("online redo log: " << reader->fileName);
                    onlineRedoSet.insert(redo);
                    break;
                }
            }

            if (!foundPath) {
                uint64_t badGroup = reader->group;
                for (std::string& path : reader->paths) {
                    std::string pathMapped(path);
                    applyMapping(pathMapped);
                    ERROR("can't read: " << pathMapped);
                }
                readerDropAll();
                RUNTIME_FAIL("can't read any member of group " << std::dec << badGroup);
            }
        }
    }

    //format uses wildcards:
    //%s - sequence number
    //%S - sequence number zero filled
    //%t - thread id
    //%T - thread id zero filled
    //%r - resetlogs id
    //%a - activation id
    //%d - database id
    //%h - some hash
    uint64_t OracleAnalyzer::getSequenceFromFileName(OracleAnalyzer* oracleAnalyzer, const std::string& file) {
        uint64_t sequence = 0;
        uint64_t i = 0;
        uint64_t j = 0;

        while (i < oracleAnalyzer->logArchiveFormat.length() && j < file.length()) {
            if (oracleAnalyzer->logArchiveFormat[i] == '%') {
                if (i + 1 >= oracleAnalyzer->logArchiveFormat.length()) {
                    WARNING("Error getting sequence from file: " << file << " log_archive_format: " << oracleAnalyzer->logArchiveFormat <<
                            " at position " << j << " format position " << i << ", found end after %");
                    return 0;
                }
                uint64_t digits = 0;
                if (oracleAnalyzer->logArchiveFormat[i + 1] == 's' || oracleAnalyzer->logArchiveFormat[i + 1] == 'S' ||
                        oracleAnalyzer->logArchiveFormat[i + 1] == 't' || oracleAnalyzer->logArchiveFormat[i + 1] == 'T' ||
                        oracleAnalyzer->logArchiveFormat[i + 1] == 'r' || oracleAnalyzer->logArchiveFormat[i + 1] == 'a' ||
                        oracleAnalyzer->logArchiveFormat[i + 1] == 'd') {
                    //some [0-9]*
                    uint64_t number = 0;
                    while (j < file.length() && file[j] >= '0' && file[j] <= '9') {
                        number = number * 10 + (file[j] - '0');
                        ++j;
                        ++digits;
                    }

                    if (oracleAnalyzer->logArchiveFormat[i + 1] == 's' || oracleAnalyzer->logArchiveFormat[i + 1] == 'S')
                        sequence = number;
                    i += 2;
                } else if (oracleAnalyzer->logArchiveFormat[i + 1] == 'h') {
                    //some [0-9a-z]*
                    while (j < file.length() && ((file[j] >= '0' && file[j] <= '9') || (file[j] >= 'a' && file[j] <= 'z'))) {
                        ++j;
                        ++digits;
                    }
                    i += 2;
                }

                if (digits == 0) {
                    WARNING("Error getting sequence from file: " << file << " log_archive_format: " << oracleAnalyzer->logArchiveFormat <<
                            " at position " << j << " format position " << i << ", found no number/hash");
                    return 0;
                }
            } else
            if (file[j] == oracleAnalyzer->logArchiveFormat[i]) {
                ++i;
                ++j;
            } else {
                WARNING("Error getting sequence from file: " << file << " log_archive_format: " << oracleAnalyzer->logArchiveFormat <<
                        " at position " << j << " format position " << i << ", found different values");
                return 0;
            }
        }

        if (i == oracleAnalyzer->logArchiveFormat.length() && j == file.length())
            return sequence;

        WARNING("Error getting sequence from file: " << file << " log_archive_format: " << oracleAnalyzer->logArchiveFormat <<
                " at position " << j << " format position " << i << ", found no sequence");
        return 0;
    }

    bool OracleAnalyzer::readerUpdateRedoLog(Reader* reader) {
        std::unique_lock<std::mutex> lck(mtx);
        reader->status = READER_STATUS_UPDATE;
        readerCond.notify_all();
        sleepingCond.notify_all();

        while (reader->status == READER_STATUS_UPDATE) {
            if (shutdown)
                break;
            analyzerCond.wait(lck);
        }

        if (reader->ret == REDO_OK)
            return true;
        else
            return false;
    }

    void OracleAnalyzer::doShutdown(void) {
        shutdown = true;
        {
            std::unique_lock<std::mutex> lck(mtx);
            readerCond.notify_all();
            sleepingCond.notify_all();
            analyzerCond.notify_all();
            memoryCond.notify_all();
            writerCond.notify_all();
        }
    }

    void OracleAnalyzer::addPathMapping(const char* source, const char* target) {
        TRACE(TRACE2_FILE, "FILE: added mapping [" << source << "] -> [" << target << "]");
        std::string sourceMaping(source);
        std::string targetMapping(target);
        pathMapping.push_back(sourceMaping);
        pathMapping.push_back(targetMapping);
    }

    void OracleAnalyzer::skipEmptyFields(RedoLogRecord* redoLogRecord, typeFIELD& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength) {
        uint16_t nextFieldLength;
        while (fieldNum + 1 <= redoLogRecord->fieldCnt) {
            nextFieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (((uint64_t)fieldNum) + 1) * 2);
            if (nextFieldLength != 0)
                return;
            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = nextFieldLength;

            if (fieldPos + fieldLength > redoLogRecord->length) {
                REDOLOG_FAIL("field length out of vector: field: " << std::dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                ", pos: " << std::dec << fieldPos << ", length:" << fieldLength << ", max: " << redoLogRecord->length);
            }
        }
    }

    void OracleAnalyzer::addRedoLogsBatch(const char* path) {
        redoLogsBatch.push_back(path);
    }

    void OracleAnalyzer::applyMapping(std::string& path) {
        uint64_t sourceLength;
        uint64_t targetLength;
        uint64_t newPathLength = path.length();
        char pathBuffer[MAX_PATH_LENGTH];

        for (uint64_t i = 0; i < pathMapping.size() / 2; ++i) {
            sourceLength = pathMapping[i * 2].length();
            targetLength = pathMapping[i * 2 + 1].length();

            if (sourceLength <= newPathLength &&
                    newPathLength - sourceLength + targetLength < MAX_PATH_LENGTH - 1 &&
                    memcmp(path.c_str(), pathMapping[i * 2].c_str(), sourceLength) == 0) {

                memcpy(pathBuffer, pathMapping[i * 2 + 1].c_str(), targetLength);
                memcpy(pathBuffer + targetLength, path.c_str() + sourceLength, newPathLength - sourceLength);
                pathBuffer[newPathLength - sourceLength + targetLength] = 0;
                if (newPathLength - sourceLength + targetLength >= MAX_PATH_LENGTH) {
                    RUNTIME_FAIL("After mapping path length (" << std::dec << (newPathLength - sourceLength + targetLength) << ") is too long for: " << pathBuffer);
                }
                path.assign(pathBuffer);
                break;
            }
        }
    }

    bool OracleAnalyzer::checkpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool switchRedo) {
        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << std::dec << scn
                << " time: " << std::dec << time_.getVal()
                << " seq: " << sequence
                << " offset: " << offset
                << " switch: " << switchRedo
                << " checkpointLastTime: " << checkpointLastTime.getVal()
                << " checkpointLastOffset: " << checkpointLastOffset);

        if (!checkpointAll && !switchRedo && !checkpointFirst) {
            if (checkpointLastTime.getVal() >= 0 && !schemaChanged &&
                    (offset - checkpointLastOffset < checkpointIntervalMB * 1024 * 1024 || checkpointIntervalMB == 0)) {
                if ((time_.getVal() - checkpointLastTime.getVal() >= checkpointIntervalS && checkpointIntervalS == 0)) {
                    checkpointLastTime = time_;
                    return true;
                }

                return false;
            }
        }
        checkpointFirst = 0;

        std::string jsonName(database + "-chkpt-" + std::to_string(scn));
        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: writing scn: " << std::dec << scn << " time: " << time_.getVal() << " seq: " <<
                sequence << " offset: " << offset << " switch: " << switchRedo);

        typeSEQ minSequence = ZERO_SEQ;
        uint64_t minOffset = 0;
        typeXID minXid;

        for (auto it : xidTransactionMap) {
            Transaction* transaction = it.second;
            if (transaction->firstSequence < minSequence) {
                minSequence = transaction->firstSequence;
                minOffset = transaction->firstOffset;
                minXid = transaction->xid;
            } else if (transaction->firstSequence == minSequence && transaction->firstOffset < minOffset) {
                minOffset = transaction->firstOffset;
                minXid = transaction->xid;
            }
        }

        std::stringstream ss;
        ss << "{\"database\":\"" << database
                << "\",\"scn\":" << std::dec << scn
                << ",\"resetlogs\":" << std::dec << resetlogs
                << ",\"activation\":" << std::dec << activation
                << ",\"time\":" << std::dec << time_.getVal()
                << ",\"seq\":" << std::dec << sequence
                << ",\"offset\":" << std::dec << offset
                << ",\"switch\":" << std::dec << switchRedo;

        if (minSequence != ZERO_SEQ) {
            ss << ",\"min-tran\":{"
                    << "\"seq\":" << std::dec << minSequence
                    << ",\"offset\":" << std::dec << minOffset
                    << ",\"xid:\":\"" << PRINTXID(minXid) << "\"}";
        }

        ss << "}";
        state->write(jsonName, ss);

        checkpointScnList.insert(scn);
        if (checkpointScn != ZERO_SCN) {
            bool unlinkFile = false;
            bool firstFound = false;
            std::set<typeSCN>::iterator it = checkpointScnList.end();

            while (it != checkpointScnList.begin()) {
                --it;
                std::string jsonName(database + "-chkpt-" + std::to_string(*it));

                unlinkFile = false;
                if (*it > checkpointScn) {
                    continue;
                } else {
                    if (!firstFound)
                        firstFound = true;
                    else
                        unlinkFile = true;
                }

                if (unlinkFile) {
                    if ((flags & REDO_FLAGS_CHECKPOINT_KEEP) == 0) {
                        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: delete: " << jsonName << " checkpoint scn: " << std::dec << checkpointScn);
                        state->drop(jsonName);
                    }
                    it = checkpointScnList.erase(it);
                }
            }
        }

        checkpointLastTime = time_;
        checkpointLastOffset = offset;
        if (schemaChanged) {
            schemaChanged = false;
            return true;
        }

        if (switchRedo) {
            if (checkpointOutputLogSwitch)
                return true;
        } else {
            return true;
        }

        return false;
    }

    void OracleAnalyzer::readCheckpoints(void) {
        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: searching for previous checkpoint information");

        std::set<std::string> namesList;
        state->list(namesList);

        typeSCN fileScnMax = 0;
        for (std::string jsonName : namesList) {
            std::string prefix(database + "-chkpt-");
            if (jsonName.length() < prefix.length() || jsonName.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            std::string fileScnStr(jsonName.substr(prefix.length(), jsonName.length()));
            typeSCN fileScn;
            try {
                fileScn = strtoull(fileScnStr.c_str(), nullptr, 10);
            } catch (std::exception& e) {
                //ignore other files
                continue;
            }

            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: found: " << jsonName << " scn: " << std::dec << fileScn);
            checkpointScnList.insert(fileScn);
        }

        if (startScn != ZERO_SCN)
            firstScn = startScn;
        else
            firstScn = 0;

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: firstScn: " << std::dec << firstScn);
        if (firstScn != ZERO_SCN && firstScn != 0) {
            bool toDrop = false;
            bool finish;
            std::set<typeSCN>::iterator it = checkpointScnList.end();

            while (it != checkpointScnList.begin()) {
                --it;
                std::string jsonName(database + "-chkpt-" + std::to_string(*it));

                toDrop = false;
                if (*it > firstScn) {
                    toDrop = true;
                } else {
                    if (readCheckpoint(jsonName, *it))
                        toDrop = true;
                }

                if (toDrop) {
                    if ((flags & REDO_FLAGS_CHECKPOINT_KEEP) == 0) {
                        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: delete: " << jsonName << " scn: " << std::dec << *it);
                        state->drop(jsonName);
                    }
                    it = checkpointScnList.erase(it);
                }
            }
        }
    }

    bool OracleAnalyzer::readCheckpoint(std::string& jsonName, typeSCN fileScn) {
        //checkpoint file is read, can delete rest
        if (sequence != ZERO_SEQ && sequence > 0)
            return true;

        std::string checkpointJSON;
        rapidjson::Document document;
        state->read(jsonName, CHECKPOINT_FILE_MAX_SIZE, checkpointJSON, false);

        if (checkpointJSON.length() == 0 || document.Parse(checkpointJSON.c_str()).HasParseError()) {
            WARNING("parsing: " << jsonName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()) << " - skipping file");
            return false;
        }

        const char* databaseRead = getJSONfieldS(jsonName, JSON_PARAMETER_LENGTH, document, "database");
        if (database.compare(databaseRead) != 0) {
            WARNING("invalid database for: " << jsonName << " - " << databaseRead << " instead of " << database << " - skipping file");
            return false;
        }

        resetlogs = getJSONfieldU32(jsonName, document, "resetlogs");
        activation = getJSONfieldU32(jsonName, document, "activation");

        typeSCN scnRead = getJSONfieldU64(jsonName, document, "scn");
        if (fileScn != scnRead) {
            WARNING("invalid scn for: " << jsonName << " - " << std::dec << scnRead << " instead of " << fileScn << " - skipping file");
            return false;
        }

        typeSEQ seqRead = getJSONfieldU32(jsonName, document, "seq");
        uint64_t offsetRead = getJSONfieldU64(jsonName, document, "offset");
        if ((offsetRead & 511) != 0) {
            WARNING("invalid offset for: " << jsonName << " - " << std::dec << scnRead << " value " << offsetRead << " is not a multiplication of 512 - skipping file");
            return false;
        }

        typeSEQ minTranSeq = 0;
        uint64_t minTranOffset = 0;

        if (document.HasMember("min-tran")) {
            const rapidjson::Value& minTranJSON = getJSONfieldO(jsonName, document, "min-tran");
            minTranSeq = getJSONfieldU32(jsonName, minTranJSON, "seq");
            minTranOffset = getJSONfieldU64(jsonName, minTranJSON, "offset");
            if ((minTranOffset & 511) != 0) {
                WARNING("invalid offset for: " << jsonName << " - " << std::dec << scnRead << " value " << minTranOffset << " is not a multiplication of 512 - skipping file");
                return false;
            }
        }

        if (minTranSeq > 0) {
            sequence = minTranSeq;
            offset = minTranOffset;
        } else {
            sequence = seqRead;
            offset = offsetRead;
        }

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: found: " << jsonName << " scn: " << std::dec << fileScn << " seq: " << sequence <<
                " offset: " << offset);
        return false;
    }

    uint8_t* OracleAnalyzer::getMemoryChunk(const char* module, bool supp) {
        TRACE(TRACE2_MEMORY, "MEMORY: " << module << " - get at: " << std::dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            std::unique_lock<std::mutex> lck(mtx);

            if (memoryChunksFree == 0) {
                if (memoryChunksAllocated == memoryChunksMax) {
                    if (memoryChunksSupplemental > 0 && waitingForWriter) {
                        WARNING("out of memory, sleeping until writer buffers are flushed and memory is released");
                        memoryCond.wait(lck);
                    }
                    if (memoryChunksAllocated == memoryChunksMax) {
                        ERROR("HINT: try to restart with higher value of \"memory-max-mb\" parameter or if big transaction - add to \"skip-xid\" list; transaction would be skipped");
                        shutdown = true;
                        readerCond.notify_all();
                        sleepingCond.notify_all();
                        analyzerCond.notify_all();
                        memoryCond.notify_all();
                        writerCond.notify_all();
                        RUNTIME_FAIL("memory exhausted when needed for: " << module);
                    }
                }

                memoryChunks[0] = (uint8_t*) aligned_alloc(MEMORY_ALIGNMENT, MEMORY_CHUNK_SIZE);
                if (memoryChunks[0] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << (MEMORY_CHUNK_SIZE_MB) << " bytes memory (for: memory chunks#6)");
                }
                ++memoryChunksFree;
                ++memoryChunksAllocated;

                if (memoryChunksAllocated > memoryChunksHWM)
                    memoryChunksHWM = memoryChunksAllocated;
            }

            --memoryChunksFree;
            if (supp)
                ++memoryChunksSupplemental;
            return memoryChunks[memoryChunksFree];
        }
    }

    void OracleAnalyzer::freeMemoryChunk(const char* module, uint8_t* chunk, bool supp) {
        TRACE(TRACE2_MEMORY, "MEMORY: " << module << " - free at: " << std::dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            std::unique_lock<std::mutex> lck(mtx);

            if (memoryChunksFree == memoryChunksAllocated) {
                RUNTIME_FAIL("trying to free unknown memory block for: " << module);
            }

            //keep 25% reserved
            if (memoryChunksAllocated > memoryChunksMin && memoryChunksFree > memoryChunksAllocated / 4) {
                free(chunk);
                --memoryChunksAllocated;
            } else {
                memoryChunks[memoryChunksFree] = chunk;
                ++memoryChunksFree;
            }
            if (supp)
                --memoryChunksSupplemental;
        }
    }

    bool OracleAnalyzer::checkConnection(void) {
        return true;
    }

    void OracleAnalyzer::goStandby(void) {
    }

    bool OracleAnalyzer::continueWithOnline(void) {
        return true;
    }

    const char* OracleAnalyzer::getModeName(void) const {
        return "offline";
    }

    void OracleAnalyzer::archGetLogPath(OracleAnalyzer* oracleAnalyzer) {
        if (oracleAnalyzer->logArchiveFormat.length() == 0) {
            RUNTIME_FAIL("missing location of archived redo logs for offline mode");
        }

        std::string mappedPath(oracleAnalyzer->dbRecoveryFileDest + "/" + oracleAnalyzer->context + "/archivelog");
        oracleAnalyzer->applyMapping(mappedPath);
        TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath);

        DIR* dir;
        if ((dir = opendir(mappedPath.c_str())) == nullptr) {
            RUNTIME_FAIL("can't access directory: " << mappedPath);
        }

        std::string newLastCheckedDay;
        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            std::string mappedSubPath(mappedPath + "/" + ent->d_name);
            if (stat(mappedSubPath.c_str(), &fileStat)) {
                WARNING("reading information for file: " << mappedSubPath << " - " << strerror(errno));
                continue;
            }

            if (!S_ISDIR(fileStat.st_mode))
                continue;

            //skip earlier days
            if (oracleAnalyzer->lastCheckedDay.length() > 0 && oracleAnalyzer->lastCheckedDay.compare(ent->d_name) > 0)
                continue;

            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath << "/" << ent->d_name);

            std::string mappedPathWithFile(mappedPath + "/" + ent->d_name);
            DIR* dir2;
            if ((dir2 = opendir(mappedPathWithFile.c_str())) == nullptr) {
                closedir(dir);
                RUNTIME_FAIL("can't access directory: " << mappedPathWithFile);
            }

            struct dirent* ent2;
            while ((ent2 = readdir(dir2)) != nullptr) {
                if (strcmp(ent2->d_name, ".") == 0 || strcmp(ent2->d_name, "..") == 0)
                    continue;

                std::string fileName(mappedPath + "/" + ent->d_name + "/" + ent2->d_name);
                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << fileName);

                uint64_t sequence = getSequenceFromFileName(oracleAnalyzer, ent2->d_name);

                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence);

                if (sequence == 0 || sequence < oracleAnalyzer->sequence)
                    continue;

                RedoLog* redo = new RedoLog(oracleAnalyzer, 0, fileName);
                if (redo == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(RedoLog) << " bytes memory (arch log list#2)");
                }

                redo->firstScn = ZERO_SCN;
                redo->nextScn = ZERO_SCN;
                redo->sequence = sequence;
                oracleAnalyzer->archiveRedoQueue.push(redo);
            }
            closedir(dir2);

            if (newLastCheckedDay.length() == 0 ||
                (newLastCheckedDay.length() > 0 && newLastCheckedDay.compare(ent->d_name) < 0))
                newLastCheckedDay = ent->d_name;
        }
        closedir(dir);

        if (newLastCheckedDay.length() != 0 &&
                (oracleAnalyzer->lastCheckedDay.length() == 0 ||
                        (oracleAnalyzer->lastCheckedDay.length() > 0 && oracleAnalyzer->lastCheckedDay.compare(newLastCheckedDay) < 0))) {
            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: updating last checked day to: " << newLastCheckedDay);
            oracleAnalyzer->lastCheckedDay = newLastCheckedDay;
        }
    }

    void OracleAnalyzer::archGetLogList(OracleAnalyzer* oracleAnalyzer) {
        uint64_t sequenceStart = ZERO_SEQ;
        for (std::string& mappedPath : oracleAnalyzer->redoLogsBatch) {
            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath);

            struct stat fileStat;
            if (stat(mappedPath.c_str(), &fileStat)) {
                WARNING("reading information for file: " << mappedPath << " - " << strerror(errno));
                continue;
            }

            //single file
            if (!S_ISDIR(fileStat.st_mode)) {
                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath);

                //getting file name from path
                const char* fileName = mappedPath.c_str();
                uint64_t j = mappedPath.length();
                while (j > 0) {
                    if (fileName[j - 1] == '/')
                        break;
                    --j;
                }
                uint64_t sequence = getSequenceFromFileName(oracleAnalyzer, fileName + j);

                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence);

                if (sequence == 0 || sequence < oracleAnalyzer->sequence)
                    continue;

                RedoLog* redo = new RedoLog(oracleAnalyzer, 0, mappedPath);
                if (redo == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(RedoLog) << " bytes memory (arch log list#3)");
                }

                redo->firstScn = ZERO_SCN;
                redo->nextScn = ZERO_SCN;
                redo->sequence = sequence;
                oracleAnalyzer->archiveRedoQueue.push(redo);
                if (sequenceStart == ZERO_SEQ || sequenceStart > sequence)
                    sequenceStart = sequence;
            //dir, check all files
            } else {
                DIR* dir;
                if ((dir = opendir(mappedPath.c_str())) == nullptr) {
                    RUNTIME_FAIL("can't access directory: " << mappedPath);
                }

                struct dirent* ent;
                while ((ent = readdir(dir)) != nullptr) {
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                        continue;

                    std::string fileName(mappedPath + "/" + ent->d_name);
                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << fileName);

                    uint64_t sequence = getSequenceFromFileName(oracleAnalyzer, ent->d_name);

                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence);

                    if (sequence == 0 || sequence < oracleAnalyzer->sequence)
                        continue;

                    RedoLog* redo = new RedoLog(oracleAnalyzer, 0, fileName);
                    if (redo == nullptr) {
                        RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(RedoLog) << " bytes memory (arch log list#4)");
                    }

                    redo->firstScn = ZERO_SCN;
                    redo->nextScn = ZERO_SCN;
                    redo->sequence = sequence;
                    oracleAnalyzer->archiveRedoQueue.push(redo);
                }
                closedir(dir);
            }
        }

        if (sequenceStart != ZERO_SEQ && oracleAnalyzer->sequence == 0) {
            oracleAnalyzer->sequence = sequenceStart;
            oracleAnalyzer->offset = 0;
        }
        oracleAnalyzer->redoLogsBatch.clear();
    }

    bool redoLogCompare::operator()(RedoLog* const& p1, RedoLog* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool redoLogCompareReverse::operator()(RedoLog* const& p1, RedoLog* const& p2) {
        return p1->sequence < p2->sequence;
    }

    std::ostream& operator<<(std::ostream& os, const OracleAnalyzer& oracleAnalyzer) {
        if (oracleAnalyzer.xidTransactionMap.size() > 0)
            os << "Transactions open: " << std::dec << oracleAnalyzer.xidTransactionMap.size() << std::endl;
        for (auto it : oracleAnalyzer.xidTransactionMap) {
            os << "transaction: " << *it.second << std::endl;
        }
        return os;
    }
}
