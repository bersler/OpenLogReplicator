/* Thread reading Oracle Redo Logs using offline mode
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <thread>
#include <dirent.h>
#include <unistd.h>
#include <RedoLog.h>
#include <sys/stat.h>

#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBuffer.h"
#include "Reader.h"
#include "ReaderFilesystem.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

using namespace std;

extern void stopMain();

namespace OpenLogReplicator {

    OracleAnalyzer::OracleAnalyzer(OutputBuffer *outputBuffer, const char *alias, const char *database, uint64_t trace,
            uint64_t trace2, uint64_t dumpRedoLog, uint64_t dumpRawData, uint64_t flags, uint64_t disableChecks,
            uint64_t redoReadSleep, uint64_t archReadSleep, uint64_t memoryMinMb, uint64_t memoryMaxMb) :
        Thread(alias),
        sequence(0),
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
        archReader(nullptr),
        waitingForWriter(false),
        context(""),
        scn(ZERO_SCN),
        startScn(ZERO_SCN),
        startSequence(0),
        startTimeRel(0),
        transactionBuffer(nullptr),
        schema(nullptr),
        outputBuffer(outputBuffer),
        dumpRedoLog(dumpRedoLog),
        dumpRawData(dumpRawData),
        flags(flags),
        disableChecks(disableChecks),
        redoReadSleep(redoReadSleep),
        archReadSleep(archReadSleep),
        trace(trace),
        trace2(trace2),
        version(0),
        conId(0),
        resetlogs(0),
        activation(0),
        isBigEndian(false),
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

        schema = new Schema();
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

    typescn OracleAnalyzer::readSCNLittle(const uint8_t* buf) {
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

    typescn OracleAnalyzer::readSCNBig(const uint8_t* buf) {
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

    typescn OracleAnalyzer::readSCNrLittle(const uint8_t* buf) {
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

    typescn OracleAnalyzer::readSCNrBig(const uint8_t* buf) {
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

    void OracleAnalyzer::writeSCNLittle(uint8_t* buf, typescn val) {
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

    void OracleAnalyzer::writeSCNBig(uint8_t* buf, typescn val) {
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
        isBigEndian = true;
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
        if (startSequence > 0) {
            RUNTIME_FAIL("starting by SCN is not supported for offline mode");
        } else if (startTime.length() > 0) {
            RUNTIME_FAIL("starting by time is not supported for offline mode");
        } else if (startTimeRel > 0) {
            RUNTIME_FAIL("starting by relative time is not supported for offline mode");
        } else if (startScn != ZERO_SCN) {
            RUNTIME_FAIL("startup SCN is not provided");
        }

        if (scn == ZERO_SCN) {
            RUNTIME_FAIL("getting database SCN");
        }
        initializeSchema();
    }

    void OracleAnalyzer::initializeSchema(void) {
        INFO_("last confirmed SCN: " << dec << scn);
        if (!schema->readSchema(this)) {
            refreshSchema();
            schema->writeSchema(this);
        }
    }

    void OracleAnalyzer::refreshSchema(void) {
        RUNTIME_FAIL("schema file missing - required for offline mode");
    }

    void *OracleAnalyzer::run(void) {
        TRACE_(TRACE2_THREADS, "ANALYZER (" << hex << this_thread::get_id() << ") START");

        try {
            while (scn == ZERO_SCN) {
                {
                    unique_lock<mutex> lck(mtx);
                    if (startScn == ZERO_SCN)
                        analyzerCond.wait(lck);
                }

                if (shutdown)
                    return 0;

                string flagsStr;
                if (flags) {
                    flagsStr = " (flags: " + to_string(flags) + ")";
                }

                string starting;
                if (startSequence > 0)
                    starting = "SEQ:" + to_string(startSequence);
                else if (startTime.length() > 0)
                    starting = "TIME:" + startTime;
                else if (startTimeRel > 0)
                    starting = "TIME-REL:" + to_string(startTimeRel);
                else if (startScn != ZERO_SCN)
                    starting = "SCN:" + to_string(startScn);
                else
                    starting = "NOW";

                INFO_("Oracle Analyzer for " << database << " in " << getModeName() << " mode is starting" << flagsStr << " from " << starting);

                if (shutdown)
                    throw RuntimeException("shut down on request");
                initialize();

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
                    TRACE_(TRACE2_REDO, "checking online redo logs");
                    updateOnlineLogs();

                    while (!shutdown) {
                        redo = nullptr;
                        TRACE_(TRACE2_REDO, "searching online redo log for sequence: " << dec << sequence);

                        //find the candidate to read
                        for (RedoLog *redoLog : onlineRedoSet) {
                            if (redoLog->sequence == sequence)
                                redo = redoLog;
                            TRACE_(TRACE2_REDO, redoLog->path << " is " << dec << redoLog->sequence);
                        }

                        //keep reading online redo logs while it is possible
                        if (redo == nullptr) {
                            bool isHigher = false;
                            while (!shutdown) {
                                for (RedoLog *redoTmp : onlineRedoSet) {
                                    if (redoTmp->reader->sequence > sequence)
                                        isHigher = true;
                                    if (redoTmp->reader->sequence == sequence)
                                        redo = redoTmp;
                                }

                                //all so far read, waiting for switch
                                if (redo == nullptr && !isHigher) {
                                    usleep(redoReadSleep);
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
                                INFO_("online redo log has been overwritten by new data, continuing reading from archived redo log");
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
                TRACE_(TRACE2_REDO, "checking archive redo logs");
                archGetLog(this);

                if (archiveRedoQueue.empty()) {
                    if ((flags & REDO_FLAGS_ARCH_ONLY) != 0) {
                        TRACE_(TRACE2_ARCHIVE_LIST, "archived redo log missing for sequence: " << dec << sequence << ", sleeping");
                        usleep(archReadSleep);
                    } else {
                        RUNTIME_FAIL("couldn't find archive log for sequence: " << dec << sequence);
                    }
                }

                while (!archiveRedoQueue.empty() && !shutdown) {
                    RedoLog *redoPrev = redo;
                    redo = archiveRedoQueue.top();
                    TRACE_(TRACE2_REDO, "searching archived redo log for sequence: " << dec << sequence);

                    //when no checkpoint exists start processing from first file
                    if (sequence == 0)
                        sequence = redo->sequence;

                    //skip older archived redo logs
                    if (redo->sequence < sequence) {
                        archiveRedoQueue.pop();
                        delete redo;
                        continue;
                    } else if (redo->sequence > sequence) {
                        RUNTIME_FAIL("couldn't find archive log for sequence: " << dec << sequence << ", found: " << redo->sequence << " instead");
                    }

                    logsProcessed = true;
                    redo->reader = archReader;

                    archReader->pathMapped = redo->path;
                    if (!readerCheckRedoLog(archReader)) {
                        RUNTIME_FAIL("opening archive log: " << redo->path);
                    }

                    if (!readerUpdateRedoLog(archReader)) {
                        RUNTIME_FAIL("reading archive log: " << redo->path);
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
                        RUNTIME_FAIL("archive log processing returned: " << dec << ret);
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
                    usleep(redoReadSleep);
            }
        } catch(ConfigurationException &ex) {
            stopMain();
        } catch(RuntimeException &ex) {
            stopMain();
        }

        INFO_("Oracle analyzer for: " << database << " is shutting down");

        FULL_(*this);
        readerDropAll();

        INFO_("Oracle analyzer for: " << database << " is shut down, allocated at most " << dec <<
                (memoryChunksHWM * MEMORY_CHUNK_SIZE_MB) << "MB memory");

        TRACE_(TRACE2_THREADS, "ANALYZER (" << hex << this_thread::get_id() << ") STOP");
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

    void OracleAnalyzer::readerDropAll(void) {
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
            delete reader;
        }
        archReader = nullptr;
        readers.clear();
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

    //checking if file name looks something like o1_mf_1_SSSS_XXXXXXXX_.arc
    //SS - sequence number
    uint64_t OracleAnalyzer::getSequenceFromFileName(const char *file) {
        uint64_t sequence = 0, i, j, iMax = strnlen(file, 256);
        for (i = 0; i < iMax; ++i)
            if (file[i] == '_')
                break;

        //first '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] == '_')
                break;

        //second '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] == '_')
                break;

        //third '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] >= '0' && file[i] <= '9')
                sequence = sequence * 10 + (file[i] - '0');
            else
                break;

        //forth '_'
        if (i >= iMax || file[i] != '_')
            return 0;

        for (++i; i < iMax; ++i)
            if (file[i] == '_')
                break;

        if (i >= iMax || file[i] != '_')
            return 0;

        //fifth '_'
        if (strncmp(file + i, "_.arc", 5) != 0)
            return 0;

        return sequence;
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

    void OracleAnalyzer::stop(void) {
        shutdown = true;
        {
            unique_lock<mutex> lck(mtx);
            readerCond.notify_all();
            sleepingCond.notify_all();
            analyzerCond.notify_all();
            memoryCond.notify_all();
        }
    }

    void OracleAnalyzer::addPathMapping(const char* source, const char* target) {
        TRACE_(TRACE2_FILE, "added mapping [" << source << "] -> [" << target << "]");
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
                    ", objn: " << dec << redoLogRecord->objn <<
                    ", objd: " << dec << redoLogRecord->objd <<
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

    uint8_t *OracleAnalyzer::getMemoryChunk(const char *module, bool supp) {
        TRACE_(TRACE2_MEMORY, module << " - get at: " << dec << memoryChunksFree << "/" << memoryChunksAllocated);

        {
            unique_lock<mutex> lck(mtx);

            if (memoryChunksFree == 0) {
                if (memoryChunksAllocated == memoryChunksMax) {
                    if (memoryChunksSupplemental > 0 && waitingForWriter) {
                        WARNING_("out of memory, sleeping until writer buffers are free and release some");
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
        TRACE_(TRACE2_MEMORY, module << " - free at: " << dec << memoryChunksFree << "/" << memoryChunksAllocated);

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

    bool OracleAnalyzer::continueWithOnline(void) {
        return true;
    }

    const char* OracleAnalyzer::getModeName(void) {
        return "offline";
    }

    void OracleAnalyzer::archGetLogPath(OracleAnalyzer *oracleAnalyzer) {
        if (oracleAnalyzer->dbRecoveryFileDest.length() == 0) {
            if (oracleAnalyzer->logArchiveDest.length() > 0 && oracleAnalyzer->logArchiveFormat.length() > 0) {
                RUNTIME_FAIL("only db_recovery_file_dest location of archived redo logs is supported for offline mode");
            } else {
                RUNTIME_FAIL("missing location of archived redo logs for offline mode");
            }
        }

        string mappedPath = oracleAnalyzer->applyMapping(oracleAnalyzer->dbRecoveryFileDest + "/" + oracleAnalyzer->database + "/archivelog");
        TRACE(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath);

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

            TRACE(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath << "/" << ent->d_name);

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
                TRACE(TRACE2_ARCHIVE_LIST, "checking path: " << fileName);

                uint64_t sequence = getSequenceFromFileName(ent2->d_name);

                TRACE(TRACE2_ARCHIVE_LIST, "found sequence: " << sequence);

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
            TRACE(TRACE2_ARCHIVE_LIST, "updating last checked day to: " << newLastCheckedDay);
            oracleAnalyzer->lastCheckedDay = newLastCheckedDay;
        }
    }

    void OracleAnalyzer::archGetLogList(OracleAnalyzer *oracleAnalyzer) {
        for (string &mappedPath : oracleAnalyzer->redoLogsBatch) {
            TRACE(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath);

            struct stat fileStat;
            if (stat(mappedPath.c_str(), &fileStat)) {
                WARNING("can't read file information for: " << mappedPath);
                continue;
            }

            //single file
            if (!S_ISDIR(fileStat.st_mode)) {
                TRACE(TRACE2_ARCHIVE_LIST, "checking path: " << mappedPath);

                //getting file name from path
                const char *fileName = mappedPath.c_str();
                uint64_t j = mappedPath.length();
                while (j > 0) {
                    if (fileName[j - 1] == '/')
                        break;
                    --j;
                }
                uint64_t sequence = getSequenceFromFileName(fileName + j);

                TRACE(TRACE2_ARCHIVE_LIST, "found sequence: " << sequence);

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
                    TRACE(TRACE2_ARCHIVE_LIST, "checking path: " << fileName);

                    uint64_t sequence = getSequenceFromFileName(ent->d_name);

                    TRACE(TRACE2_ARCHIVE_LIST, "found sequence: " << sequence);

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
