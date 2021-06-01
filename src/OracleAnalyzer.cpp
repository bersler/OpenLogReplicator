/* Thread reading Oracle Redo Logs using offline mode
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

#include <dirent.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>

#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OutputBuffer.h"
#include "ReaderFilesystem.h"
#include "RedoLog.h"
#include "RedoLogException.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SystemTransaction.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

using namespace rapidjson;
using namespace std;

extern void stopMain();
extern const Value& getJSONfieldV(string &fileName, const Value& value, const char* field);
extern const Value& getJSONfieldD(string &fileName, const Document& document, const char* field);

namespace OpenLogReplicator {
    OracleAnalyzer::OracleAnalyzer(OutputBuffer *outputBuffer, uint64_t dumpRedoLog, uint64_t dumpRawData, const char *alias,
            const char *database, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t readBufferMax, uint64_t disableChecks) :
        Thread(alias),
        sequence(ZERO_SEQ),
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
        checkpointPath("checkpoint"),
        checkpointIntervalS(600),
        checkpointIntervalMB(100),
        checkpointFirst(1),
        checkpointAll(0),
        checkpointOutputCheckpoint(1),
        checkpointOutputLogSwitch(1),
        checkpointLastTime(0),
        checkpointLastOffset(0),
        archReader(nullptr),
        waitingForWriter(false),
        context(""),
        firstScn(ZERO_SCN),
        checkpointScn(ZERO_SCN),
        schemaScn(ZERO_SCN),
        startScn(ZERO_SCN),
        startSequence(0),
        startTimeRel(0),
        readStartOffset(0),
        readBufferMax(readBufferMax),
        transactionBuffer(nullptr),
        schema(nullptr),
        outputBuffer(outputBuffer),
        systemTransaction(nullptr),
        dumpRedoLog(dumpRedoLog),
        dumpRawData(dumpRawData),
        flags(0),
        disableChecks(disableChecks),
        redoReadSleepUS(10000),
        archReadSleepUS(10000000),
        archReadRetry(3),
        redoVerifyDelayUS(50000),
        version(0),
        conId(-1),
        resetlogs(0),
        activation(0),
        bigEndian(false),
        suppLogSize(0),
        version12(false),
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

        memoryChunks = new uint8_t*[memoryMaxMb / MEMORY_CHUNK_SIZE_MB];
        if (memoryChunks == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) << " bytes memory (for: memory chunks#1)");
        }

        for (uint64_t i = 0; i < memoryChunksMin; ++i) {
            memoryChunks[i] = new uint8_t[MEMORY_CHUNK_SIZE];

            if (memoryChunks[i] == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << MEMORY_CHUNK_SIZE_MB << " bytes memory (for: memory chunks#2)");
            }
            ++memoryChunksAllocated;
            ++memoryChunksFree;
        }
        memoryChunksHWM = memoryChunksMin;

        transactionBuffer = new TransactionBuffer(this);
        if (transactionBuffer == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(TransactionBuffer) << " bytes memory (for: memory chunks#5)");
        }

        schema = new Schema(this);
        if (schema == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Schema) << " bytes memory (for: schema)");
        }
    }

    OracleAnalyzer::~OracleAnalyzer() {
        readerDropAll();

        while (!archiveRedoQueue.empty()) {
            RedoLog *redoTmp = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete redoTmp;
        }

        for (RedoLog *redoLog : onlineRedoSet)
            delete redoLog;
        onlineRedoSet.clear();

        for (auto it : xidTransactionMap) {
            Transaction *transaction = it.second;
            delete transaction;
        }
        xidTransactionMap.clear();

        if (transactionBuffer != nullptr) {
            delete transactionBuffer;
            transactionBuffer = nullptr;
        }

        while (memoryChunksAllocated > 0) {
            --memoryChunksAllocated;
            delete[] memoryChunks[memoryChunksAllocated];
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
    }

    void OracleAnalyzer::updateOnlineLogs(void) {
        for (RedoLog *redoLog : onlineRedoSet) {
            redoLog->resetRedo();
            if (!readerUpdateRedoLog(redoLog->reader)) {
                RUNTIME_FAIL("updating failed for " << dec << redoLog->path);
            } else {
                redoLog->sequence = redoLog->reader->sequence;
                redoLog->firstScn = redoLog->reader->firstScn;
                redoLog->nextScn = redoLog->reader->nextScn;
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

    void OracleAnalyzer::initialize(void) {
        archReader = readerCreate(0);
    }

    void OracleAnalyzer::positionReader(void) {
        if (startSequence > 0) {
            sequence = startSequence;
            firstScn = 0;
        } else if (startTime.length() > 0) {
            RUNTIME_FAIL("starting by time is not supported for offline mode");
        } else if (startTimeRel > 0) {
            RUNTIME_FAIL("starting by relative time is not supported for offline mode");
        } else if (startScn != ZERO_SCN) {
            sequence = 0;
            firstScn = startScn;
        }
    }

    void OracleAnalyzer::createSchema(void) {
        if ((flags & REDO_FLAGS_SCHEMALESS) == 0) {
            RUNTIME_FAIL("schema file missing");
        }
    }

    void *OracleAnalyzer::run(void) {
        TRACE(TRACE2_THREADS, "THREADS: ANALYZER (" << hex << this_thread::get_id() << ") START");

        try {
            initialize();
            while (firstScn == ZERO_SCN) {
                {
                    unique_lock<mutex> lck(mtx);
                    if (startScn == ZERO_SCN)
                        writerCond.wait(lck);
                }

                if (shutdown)
                    return 0;

                string flagsStr;
                if (flags) {
                    flagsStr = " (flags: " + to_string(flags) + ")";
                }

                string starting;
                if (startSequence > 0)
                    starting = "seq: " + to_string(startSequence);
                else if (startTime.length() > 0)
                    starting = "time: " + startTime;
                else if (startTimeRel > 0)
                    starting = "time-rel: " + to_string(startTimeRel);
                else if (startScn > 0)
                    starting = "scn: " + to_string(startScn);
                else
                    starting = "now";

                INFO("Oracle Analyzer for " << database << " in " << getModeName() << " mode is starting" << flagsStr << " from " << starting);

                if (shutdown)
                    return 0;

                positionReader();
                readCheckpoints();
                if (!schema->readSchema()) {
                    createSchema();
                    schema->writeSchema();
                }
                goStandby();

                if (firstScn == ZERO_SCN) {
                    INFO("last confirmed scn: <none>, starting sequence: " << dec << sequence << ", offset: " << readStartOffset);
                } else {
                    INFO("last confirmed scn: " << dec << firstScn << ", starting sequence: " << dec << sequence << ", offset: " << readStartOffset);
                }

                if ((dbBlockChecksum.compare("OFF") == 0 || dbBlockChecksum.compare("FALSE") == 0) &&
                        (disableChecks & DISABLE_CHECK_BLOCK_SUM) == 0) {
                    WARNING("HINT please set DB_BLOCK_CHECKSUM = TYPICAL on the database"
                            " or turn off consistency checking in OpenLogReplicator setting parameter disable-checks: "
                            << dec << DISABLE_CHECK_BLOCK_SUM << " for the reader");
                }

                {
                    unique_lock<mutex> lck(mtx);
                    outputBuffer->writersCond.notify_all();
                }
            }

            uint64_t ret = REDO_OK;
            RedoLog *redo = nullptr;
            bool logsProcessed;

            while (!shutdown) {
                logsProcessed = false;

                //
                //ONLINE REDO LOGS READ
                //
                if ((flags & REDO_FLAGS_ARCH_ONLY) == 0) {
                    TRACE(TRACE2_REDO, "REDO: checking online redo logs");
                    updateOnlineLogs();

                    while (!shutdown) {
                        redo = nullptr;
                        TRACE(TRACE2_REDO, "REDO: searching online redo log for seq: " << dec << sequence);

                        //find the candidate to read
                        for (RedoLog *redoLog : onlineRedoSet) {
                            if (redoLog->sequence == sequence)
                                redo = redoLog;
                            TRACE(TRACE2_REDO, "REDO: " << redoLog->path << " is " << dec << redoLog->sequence);
                        }

                        //keep reading online redo logs while it is possible
                        if (redo == nullptr) {
                            bool higher = false;
                            while (!shutdown) {
                                for (RedoLog *redoTmp : onlineRedoSet) {
                                    if (redoTmp->reader->sequence > sequence)
                                        higher = true;
                                    if (redoTmp->reader->sequence == sequence)
                                        redo = redoTmp;
                                }

                                //all so far read, waiting for switch
                                if (redo == nullptr && !higher) {
                                    usleep(redoReadSleepUS);
                                } else
                                    break;

                                if (shutdown)
                                    break;

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

                        if (ret != REDO_FINISHED) {
                            if (ret == REDO_OVERWRITTEN) {
                                INFO("online redo log has been overwritten by new data, continuing reading from archived redo log");
                                break;
                            }
                            if (redo->group == 0) {
                                RUNTIME_FAIL("read archived redo log");
                            } else {
                                RUNTIME_FAIL("read online redo log");
                            }
                        }
                        ++sequence;
                    }
                }

                //
                //ARCHIVED REDO LOGS READ
                //
                if (shutdown)
                    break;
                TRACE(TRACE2_REDO, "REDO: checking archived redo logs");
                archGetLog(this);

                if (archiveRedoQueue.empty()) {
                    if ((flags & REDO_FLAGS_ARCH_ONLY) != 0) {
                        TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: archived redo log missing for seq: " << dec << sequence << ", sleeping");
                        usleep(archReadSleepUS);
                    } else {
                        RUNTIME_FAIL("couldn't find archive log for seq: " << dec << sequence);
                    }
                }

                while (!archiveRedoQueue.empty() && !shutdown) {
                    RedoLog *redoPrev = redo;
                    redo = archiveRedoQueue.top();
                    TRACE(TRACE2_REDO, "REDO: searching archived redo log for seq: " << dec << sequence);

                    //when no checkpoint exists start processing from first file
                    if (sequence == 0)
                        sequence = redo->sequence;

                    //skip older archived redo logs
                    if (redo->sequence < sequence) {
                        archiveRedoQueue.pop();
                        delete redo;
                        continue;
                    } else if (redo->sequence > sequence) {
                        RUNTIME_FAIL("couldn't find archive log for seq: " << dec << sequence << ", found: " << redo->sequence << " instead");
                    }

                    logsProcessed = true;
                    redo->reader = archReader;

                    archReader->pathMapped = redo->path;
                    uint64_t retry = archReadRetry;

                    while (true) {
                        if (readerCheckRedoLog(archReader) && readerUpdateRedoLog(archReader))
                            break;

                        if (retry == 0) {
                            RUNTIME_FAIL("opening archived redo log: " << redo->path);
                        }

                        INFO("archived redo log " << redo->path << " is not ready for read, sleeping " << dec << archReadSleepUS << " us");
                        usleep(archReadSleepUS);
                        --retry;
                    }

                    if (ret == REDO_OVERWRITTEN && redoPrev != nullptr && redoPrev->sequence == redo->sequence) {
                        redo->continueRedo(redoPrev);
                    } else {
                        redo->resetRedo();
                    }

                    ret = redo->processLog();

                    if (shutdown)
                        break;

                    if (ret != REDO_FINISHED) {
                        RUNTIME_FAIL("archive log processing returned: " << Reader::REDO_CODE[ret] << " (code: " << dec << ret << ")");
                    }

                    ++sequence;
                    archiveRedoQueue.pop();
                    delete redo;
                    redo = nullptr;
                }

                if (shutdown)
                    break;

                if (!continueWithOnline())
                    break;

                if (!logsProcessed)
                    usleep(redoReadSleepUS);
            }
        } catch (ConfigurationException &ex) {
            stopMain();
        } catch (RuntimeException &ex) {
            stopMain();
        }

        INFO("Oracle analyzer for: " << database << " is shutting down");

        DEBUG(*this);
        uint64_t buffersMax = readerDropAll();

        INFO("Oracle analyzer for: " << database << " is shut down, allocated at most " << dec <<
                (memoryChunksHWM * MEMORY_CHUNK_SIZE_MB) << "MB memory, max disk read buffer: " << (buffersMax * MEMORY_CHUNK_SIZE_MB) << "MB");

        TRACE(TRACE2_THREADS, "THREADS: ANALYZER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }

    bool OracleAnalyzer::readerCheckRedoLog(Reader *reader) {
        unique_lock<mutex> lck(mtx);
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
        uint64_t buffersMax = 0;
        {
            unique_lock<mutex> lck(mtx);
            for (Reader *reader : readers)
                reader->shutdown = true;
            readerCond.notify_all();
            sleepingCond.notify_all();
        }
        for (Reader *reader : readers) {
            if (reader->started)
                pthread_join(reader->pthread, nullptr);
            if (reader->buffersMax > buffersMax)
                buffersMax = reader->buffersMax;
            delete reader;
        }
        archReader = nullptr;
        readers.clear();
        return buffersMax;
    }

    Reader *OracleAnalyzer::readerCreate(int64_t group) {
        ReaderFilesystem *readerFS = new ReaderFilesystem(alias.c_str(), this, group);
        if (readerFS == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(ReaderFilesystem) << " bytes memory (for: disk reader creation)");
        }

        readers.insert(readerFS);
        if (pthread_create(&readerFS->pthread, nullptr, &Reader::runStatic, (void*)readerFS)) {
            CONFIG_FAIL("spawning thread");
        }
        return readerFS;
    }

    void OracleAnalyzer::checkOnlineRedoLogs() {
        for (Reader *reader : readers) {
            if (reader->group == 0)
                continue;

            bool foundPath = false;
            for (string &path : reader->paths) {
                reader->pathMapped = applyMapping(path);
                if (readerCheckRedoLog(reader)) {
                    foundPath = true;
                    RedoLog* redo = new RedoLog(this, reader->group, reader->pathMapped.c_str());
                    if (redo == nullptr) {
                        readerDropAll();
                        RUNTIME_FAIL("couldn't allocate " << dec << sizeof(RedoLog) << " bytes memory (for: online redo logs)");
                    }

                    redo->reader = reader;
                    onlineRedoSet.insert(redo);
                    break;
                }
            }

            if (!foundPath) {
                uint64_t badGroup = reader->group;
                for (string &path : reader->paths) {
                    string pathMapped = applyMapping(path);
                    ERROR("can't read: " << pathMapped);
                }
                readerDropAll();
                RUNTIME_FAIL("can't read any member of group " << dec << badGroup);
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
    uint64_t OracleAnalyzer::getSequenceFromFileName(OracleAnalyzer *oracleAnalyzer, const string &file) {
        uint64_t sequence = 0, i = 0, j = 0;

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

        if  (i == oracleAnalyzer->logArchiveFormat.length() && j == file.length())
            return sequence;

        WARNING("Error getting sequence from file: " << file << " log_archive_format: " << oracleAnalyzer->logArchiveFormat <<
                " at position " << j << " format position " << i << ", found no sequence");
        return 0;
    }

    bool OracleAnalyzer::readerUpdateRedoLog(Reader *reader) {
        unique_lock<mutex> lck(mtx);
        reader->status = READER_STATUS_UPDATE;
        readerCond.notify_all();
        sleepingCond.notify_all();

        while (reader->status == READER_STATUS_UPDATE) {
            if (shutdown)
                break;
            analyzerCond.wait(lck);
        }

        if (reader->ret == REDO_OK) {

            return true;
        } else
            return false;
    }

    void OracleAnalyzer::doShutdown(void) {
        shutdown = true;
        {
            unique_lock<mutex> lck(mtx);
            readerCond.notify_all();
            sleepingCond.notify_all();
            analyzerCond.notify_all();
            memoryCond.notify_all();
            writerCond.notify_all();
        }
    }

    void OracleAnalyzer::addPathMapping(const char* source, const char* target) {
        TRACE(TRACE2_FILE, "FILE: added mapping [" << source << "] -> [" << target << "]");
        string sourceMaping = source, targetMapping = target;
        pathMapping.push_back(sourceMaping);
        pathMapping.push_back(targetMapping);
    }

    void OracleAnalyzer::skipEmptyFields(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        uint16_t nextFieldLength;
        while (fieldNum + 1 <= redoLogRecord->fieldCnt) {
            nextFieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (fieldNum + 1) * 2);
            if (nextFieldLength != 0)
                return;
            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = nextFieldLength;

            if (fieldPos + fieldLength > redoLogRecord->length) {
                REDOLOG_FAIL("field length out of vector: field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                ", pos: " << dec << fieldPos << ", length:" << fieldLength << ", max: " << redoLogRecord->length);
            }
        }
    }

    void OracleAnalyzer::addRedoLogsBatch(string path) {
        redoLogsBatch.push_back(path);
    }

    void OracleAnalyzer::nextField(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        ++fieldNum;
        if (fieldNum > redoLogRecord->fieldCnt) {
            REDOLOG_FAIL("field missing in vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", data: " << dec << redoLogRecord->rowData <<
                    ", obj: " << dec << redoLogRecord->obj <<
                    ", dataObj: " << dec << redoLogRecord->dataObj <<
                    ", op: " << hex << redoLogRecord->opCode <<
                    ", cc: " << dec << (uint64_t)redoLogRecord->cc <<
                    ", suppCC: " << dec << redoLogRecord->suppLogCC);
        }

        if (fieldNum == 1)
            fieldPos = redoLogRecord->fieldPos;
        else
            fieldPos += (fieldLength + 3) & 0xFFFC;
        fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + fieldNum * 2);

        if (fieldPos + fieldLength > redoLogRecord->length) {
            REDOLOG_FAIL("field length out of vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", pos: " << dec << fieldPos << ", length:" << fieldLength << " max: " << redoLogRecord->length);
        }
    }

    bool OracleAnalyzer::nextFieldOpt(RedoLogRecord *redoLogRecord, uint64_t &fieldNum, uint64_t &fieldPos, uint16_t &fieldLength) {
        if (fieldNum >= redoLogRecord->fieldCnt)
            return false;

        ++fieldNum;

        if (fieldNum == 1)
            fieldPos = redoLogRecord->fieldPos;
        else
            fieldPos += (fieldLength + 3) & 0xFFFC;
        fieldLength = read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + fieldNum * 2);

        if (fieldPos + fieldLength > redoLogRecord->length) {
            REDOLOG_FAIL("field length out of vector, field: " << dec << fieldNum << "/" << redoLogRecord->fieldCnt <<
                    ", pos: " << dec << fieldPos << ", length:" << fieldLength << " max: " << redoLogRecord->length);
        }
        return true;
    }

    string OracleAnalyzer::applyMapping(string path) {
        uint64_t sourceLength, targetLength, newPathLength = path.length();
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
                path = pathBuffer;
                break;
            }
        }

        return path;
    }

    bool OracleAnalyzer::checkpoint(typeSCN scn, typetime time_, typeSEQ sequence, uint64_t offset, bool switchRedo) {
        if (trace >= TRACE_DEBUG) {
            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << dec << scn
                    << " time: " << dec << time_.getVal()
                    << " seq: " << sequence
                    << " offset: " << offset
                    << " switch: " << switchRedo
                    << " checkpointLastTime: " << checkpointLastTime.getVal()
                    << " checkpointLastOffset: " << checkpointLastOffset);
        }

        if (!checkpointAll &&
                checkpointLastTime.getVal() >= 0 &&
                !switchRedo &&
                !checkpointFirst &&
                (offset - checkpointLastOffset < checkpointIntervalMB * 1024 * 1024 || checkpointIntervalMB == 0)) {
            if (time_.getVal() - checkpointLastTime.getVal() >= checkpointIntervalS && checkpointIntervalS == 0) {
                checkpointLastTime = time_;
                return true;
            }

            return false;
        }
        checkpointFirst = 0;

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: writing scn: " << dec << scn << " time: " << time_.getVal() << " seq: " <<
                sequence << " offset: " << offset << " switch: " << switchRedo);
        string fileName = checkpointPath + "/" + database + "-chkpt-" + to_string(scn) + ".json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            RUNTIME_FAIL("writing checkpoint data to " << fileName);
        }

        typeSEQ minSequence = ZERO_SEQ;
        uint64_t minOffset = 0;
        typeXID minXid;

        for (auto it : xidTransactionMap) {
            Transaction *transaction = it.second;
            if (transaction->firstSequence < minSequence) {
                minSequence = transaction->firstSequence;
                minOffset = transaction->firstOffset;
                minXid = transaction->xid;
            } else if (transaction->firstSequence == minSequence && transaction->firstOffset < minOffset) {
                minOffset = transaction->firstOffset;
                minXid = transaction->xid;
            }
        }

        stringstream ss;
        ss << "{\"database\":\"" << database
                << "\",\"scn\":" << dec << scn
                << ",\"resetlogs\":" << dec << resetlogs
                << ",\"activation\":" << dec << activation
                << ",\"time\":" << dec << time_.getVal()
                << ",\"sequence\":" << dec << sequence
                << ",\"offset\":" << dec << offset
                << ",\"switch\":" << dec << switchRedo;

        if (minSequence != ZERO_SEQ) {
            ss << ",\"min-tran\":{"
                    << "\"seq\":" << dec << minSequence
                    << ",\"offset\":" << dec << minOffset
                    << ",\"xid:\":\"" << hex << setfill('0') << setw(16) << minXid << "\"}";
        }

        ss << "}";

        outfile << ss.rdbuf();
        outfile.close();

        checkpointScnList.insert(scn);
        if (checkpointScn != ZERO_SCN) {
            bool unlinkFile = false, firstFound = false;
            set<typeSCN>::iterator it = checkpointScnList.end();

            while (it != checkpointScnList.begin()) {
                --it;
                string fileName = checkpointPath + "/" + database + "-chkpt-" + to_string(*it) + ".json";

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
                        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: delete file: " << fileName << " checkpoint scn: " << dec << checkpointScn);
                        unlink(fileName.c_str());
                    }
                    it = checkpointScnList.erase(it);
                }
            }
        }

        checkpointLastTime = time_;
        checkpointLastOffset = offset;

        if (switchRedo) {
            if (checkpointOutputLogSwitch)
                return true;
        } else {
            return true;
        }

        return false;
    }

    void OracleAnalyzer::readCheckpoints(void) {
        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: searching for previous checkpoint information on: " << checkpointPath);
        DIR *dir;
        if ((dir = opendir(checkpointPath.c_str())) == nullptr) {
            RUNTIME_FAIL("can't access directory: " << checkpointPath);
        }

        string newLastCheckedDay;
        struct dirent *ent;
        typeSCN fileScnMax = 0;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            string fileName = ent->d_name;

            string fullName = checkpointPath + "/" + ent->d_name;
            if (stat(fullName.c_str(), &fileStat)) {
                WARNING("can't read file information for: " << fullName);
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            string prefix = database + "-chkpt-";
            if (fileName.length() < prefix.length() || fileName.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            string suffix = ".json";
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()).compare(suffix) != 0)
                continue;

            string fileScnStr = fileName.substr(prefix.length(), fileName.length() - suffix.length());
            typeSCN fileScn;
            try {
                fileScn = strtoull(fileScnStr.c_str(), nullptr, 10);
            } catch (exception &e) {
                //ignore other files
                continue;
            }

            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: found: " << checkpointPath << "/" << fileName << " scn: " << dec << fileScn);
            checkpointScnList.insert(fileScn);
        }
        closedir(dir);

        if (firstScn != ZERO_SCN && firstScn != 0) {
            bool unlinkFile = false, finish;
            set<typeSCN>::iterator it = checkpointScnList.end();

            while (it != checkpointScnList.begin()) {
                --it;
                string fileName = checkpointPath + "/" + database + "-chkpt-" + to_string(*it) + ".json";

                unlinkFile = false;
                if (*it > firstScn) {
                    unlinkFile = true;
                } else {
                    if (readCheckpointFile(fileName, *it))
                        unlinkFile = true;
                }

                if (unlinkFile) {
                    if ((flags & REDO_FLAGS_CHECKPOINT_KEEP) == 0) {
                        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: delete file: " << fileName << " scn: " << dec << *it);
                        unlink(fileName.c_str());
                    }
                    it = checkpointScnList.erase(it);
                }
            }
        }
    }

    bool OracleAnalyzer::readCheckpointFile(string &fileName, typeSCN fileScn) {
        //checkpoint file is read, can delete rest
        if (sequence != ZERO_SEQ && sequence > 0)
            return true;

        ifstream infile;
        infile.open(fileName.c_str(), ios::in);

        if (!infile.is_open()) {
            INFO("read error for " << fileName);
            return false;
        }

        string checkpointJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (checkpointJSON.length() == 0 || document.Parse(checkpointJSON.c_str()).HasParseError()) {
            WARNING("parsing " << fileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()) << " - skipping file");
            return false;
        }

        const Value& databaseJSON = getJSONfieldD(fileName, document, "database");
        const char* databaseRead = databaseJSON.GetString();
        if (database.compare(databaseRead) != 0) {
            WARNING("invalid database for " << fileName << " - " << databaseRead << " instead of " << database << " - skipping file");
            return false;
        }

        const Value& resetlogsJSON = getJSONfieldD(fileName, document, "resetlogs");
        typeresetlogs resetlogsRead = resetlogsJSON.GetUint64();
        if (resetlogs != 0) {
            if (resetlogs != resetlogsRead) {
                WARNING("invalid resetlogs for " << fileName << " - " << dec << resetlogsRead << " instead of " << resetlogs << " - skipping file");
                return false;
            }
        } else
            resetlogs = resetlogsRead;

        const Value& activationJSON = getJSONfieldD(fileName, document, "activation");
        typeactivation activationRead = activationJSON.GetUint64();
        if (activation != 0) {
            if (activation != activationRead) {
                WARNING("invalid activation for " << fileName << " - " << dec << activationRead << " instead of " << activation << " - skipping file");
                return false;
            }
        } else
            activation = activationRead;

        const Value& scnJSON = getJSONfieldD(fileName, document, "scn");
        typeSCN scnRead = scnJSON.GetUint64();
        if (fileScn != scnRead) {
            WARNING("invalid scn for " << fileName << " - " << dec << scnRead << " instead of " << fileScn << " - skipping file");
            return false;
        }

        const Value& seqJSON = getJSONfieldD(fileName, document, "sequence");
        typeSEQ seqRead = seqJSON.GetUint64();

        const Value& offsetJSON = getJSONfieldD(fileName, document, "offset");
        uint64_t offsetRead = offsetJSON.GetUint64();

        typeSEQ minTranSeq = 0;
        uint64_t minTranOffset = 0;

        if (document.HasMember("min-tran")) {
            const Value& minTranJSON = getJSONfieldD(fileName, document, "min-tran");

            const Value& minTranSeqJSON = getJSONfieldV(fileName, minTranJSON, "seq");
            minTranSeq = minTranSeqJSON.GetUint64();

            const Value& minTranOffsetJSON = getJSONfieldV(fileName, minTranJSON, "offset");
            minTranOffset = minTranOffsetJSON.GetUint64();
        }

        infile.close();

        if (minTranSeq > 0) {
            sequence = minTranSeq;
            readStartOffset = minTranOffset;
        } else {
            sequence = seqRead;
            readStartOffset = offsetRead;
        }

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: found: " << fileName << " scn: " << dec << fileScn << " seq: " << sequence <<
                " offset: " << readStartOffset);
        return false;
    }

    uint8_t *OracleAnalyzer::getMemoryChunk(const char *module, bool supp) {
        TRACE(TRACE2_MEMORY, "MEMORY: " << module << " - get at: " << dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            unique_lock<mutex> lck(mtx);

            if (memoryChunksFree == 0) {
                if (memoryChunksAllocated == memoryChunksMax) {
                    if (memoryChunksSupplemental > 0 && waitingForWriter) {
                        WARNING("out of memory, sleeping until writer buffers are free and release some");
                        memoryCond.wait(lck);
                    }
                    if (memoryChunksAllocated == memoryChunksMax) {
                        RUNTIME_FAIL("used all memory up to memory-max-mb parameter, restart with higher value, module: " << module);
                    }
                }

                memoryChunks[0] = new uint8_t[MEMORY_CHUNK_SIZE];
                if (memoryChunks[0] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << (MEMORY_CHUNK_SIZE_MB) << " bytes memory (for: memory chunks#6)");
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

    void OracleAnalyzer::freeMemoryChunk(const char *module, uint8_t *chunk, bool supp) {
        TRACE(TRACE2_MEMORY, "MEMORY: " << module << " - free at: " << dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            unique_lock<mutex> lck(mtx);

            if (memoryChunksFree == memoryChunksAllocated) {
                RUNTIME_FAIL("trying to free unknown memory block for module: " << module);
            }

            //keep 25% reserved
            if (memoryChunksAllocated > memoryChunksMin && memoryChunksFree > memoryChunksAllocated / 4) {
                delete[] chunk;
                --memoryChunksAllocated;
            } else {
                memoryChunks[memoryChunksFree] = chunk;
                ++memoryChunksFree;
            }
            if (supp)
                --memoryChunksSupplemental;
        }
    }

    void OracleAnalyzer::checkConnection(void) {
    }

    void OracleAnalyzer::goStandby(void) {
    }

    bool OracleAnalyzer::continueWithOnline(void) {
        return true;
    }

    const char* OracleAnalyzer::getModeName(void) const {
        return "offline";
    }

    void OracleAnalyzer::archGetLogPath(OracleAnalyzer *oracleAnalyzer) {
        if (oracleAnalyzer->logArchiveFormat.length() == 0) {
            RUNTIME_FAIL("missing location of archived redo logs for offline mode");
        }

        string mappedPath = oracleAnalyzer->applyMapping(oracleAnalyzer->dbRecoveryFileDest + "/" + oracleAnalyzer->database + "/archivelog");
        TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath);

        DIR *dir;
        if ((dir = opendir(mappedPath.c_str())) == nullptr) {
            RUNTIME_FAIL("can't access directory: " << mappedPath);
        }

        string newLastCheckedDay;
        struct dirent *ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            string mappedSubPath = mappedPath + "/" + ent->d_name;
            if (stat(mappedSubPath.c_str(), &fileStat)) {
                WARNING("can't read file information for: " << mappedSubPath);
                continue;
            }

            if (!S_ISDIR(fileStat.st_mode))
                continue;

            //skip earlier days
            if (oracleAnalyzer->lastCheckedDay.length() > 0 && oracleAnalyzer->lastCheckedDay.compare(ent->d_name) > 0)
                continue;

            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath << "/" << ent->d_name);

            string mappedPathWithFile = mappedPath + "/" + ent->d_name;
            DIR *dir2;
            if ((dir2 = opendir(mappedPathWithFile.c_str())) == nullptr) {
                closedir(dir);
                RUNTIME_FAIL("can't access directory: " << mappedPathWithFile);
            }

            struct dirent *ent2;
            while ((ent2 = readdir(dir2)) != nullptr) {
                if (strcmp(ent2->d_name, ".") == 0 || strcmp(ent2->d_name, "..") == 0)
                    continue;

                string fileName = mappedPath + "/" + ent->d_name + "/" + ent2->d_name;
                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << fileName);

                uint64_t sequence = getSequenceFromFileName(oracleAnalyzer, ent2->d_name);

                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence);

                if (sequence == 0 || sequence < oracleAnalyzer->sequence)
                    continue;

                RedoLog* redo = new RedoLog(oracleAnalyzer, 0, fileName.c_str());
                if (redo == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(RedoLog) << " bytes memory (arch log list#2)");
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

    void OracleAnalyzer::archGetLogList(OracleAnalyzer *oracleAnalyzer) {
        for (string &mappedPath : oracleAnalyzer->redoLogsBatch) {
            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath);

            struct stat fileStat;
            if (stat(mappedPath.c_str(), &fileStat)) {
                WARNING("can't read file information for: " << mappedPath);
                continue;
            }

            //single file
            if (!S_ISDIR(fileStat.st_mode)) {
                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath);

                //getting file name from path
                const char *fileName = mappedPath.c_str();
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

                RedoLog* redo = new RedoLog(oracleAnalyzer, 0, mappedPath.c_str());
                if (redo == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(RedoLog) << " bytes memory (arch log list#3)");
                }

                redo->firstScn = ZERO_SCN;
                redo->nextScn = ZERO_SCN;
                redo->sequence = sequence;
                oracleAnalyzer->archiveRedoQueue.push(redo);
            //dir, check all files
            } else {
                DIR *dir;
                if ((dir = opendir(mappedPath.c_str())) == nullptr) {
                    RUNTIME_FAIL("can't access directory: " << mappedPath);
                }

                struct dirent *ent;
                while ((ent = readdir(dir)) != nullptr) {
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                        continue;

                    string fileName = mappedPath + "/" + ent->d_name;
                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << fileName);

                    uint64_t sequence = getSequenceFromFileName(oracleAnalyzer, ent->d_name);

                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence);

                    if (sequence == 0 || sequence < oracleAnalyzer->sequence)
                        continue;

                    RedoLog* redo = new RedoLog(oracleAnalyzer, 0, fileName.c_str());
                    if (redo == nullptr) {
                        RUNTIME_FAIL("couldn't allocate " << dec << sizeof(RedoLog) << " bytes memory (arch log list#4)");
                    }

                    redo->firstScn = ZERO_SCN;
                    redo->nextScn = ZERO_SCN;
                    redo->sequence = sequence;
                    oracleAnalyzer->archiveRedoQueue.push(redo);
                }
                closedir(dir);
            }
        }
    }

    bool redoLogCompare::operator()(RedoLog* const& p1, RedoLog* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool redoLogCompareReverse::operator()(RedoLog* const& p1, RedoLog* const& p2) {
        return p1->sequence < p2->sequence;
    }

    ostream& operator<<(ostream& os, const OracleAnalyzer& oracleAnalyzer) {
        if (oracleAnalyzer.xidTransactionMap.size() > 0)
            os << "Transactions open: " << dec << oracleAnalyzer.xidTransactionMap.size() << endl;
        for (auto it : oracleAnalyzer.xidTransactionMap) {
            os << "transaction: " << *it.second << endl;
        }
        return os;
    }
}
