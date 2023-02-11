/* Base class for process which is reading from redo log files
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

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/RuntimeException.h"
#include "../common/Timer.h"
#include "Reader.h"

namespace OpenLogReplicator {
    const char* Reader::REDO_CODE[] = {"OK", "OVERWRITTEN", "FINISHED", "STOPPED", "EMPTY", "READ ERROR", "WRITE ERROR",
                                       "SEQUENCE ERROR", "CRC ERROR", "BLOCK ERROR", "BAD DATA ERROR", "OTHER ERROR"};

    Reader::Reader(Ctx* newCtx, const std::string newAlias, const std::string& newDatabase, int64_t newGroup, bool newConfiguredBlockSum) :
        Thread(newCtx, newAlias),
        ctx(newCtx),
        database(newDatabase),
        fileCopyDes(-1),
        fileSize(0),
        fileCopySequence(0),
        hintDisplayed(false),
        configuredBlockSum(newConfiguredBlockSum),
        readBlocks(false),
        reachedZero(false),
        group(newGroup),
        sequence(0),
        numBlocksHeader(ZERO_BLK),
        resetlogs(0),
        activation(0),
        headerBuffer(nullptr),
        compatVsn(0),
        firstTimeHeader(0),
        firstScn(ZERO_SCN),
        firstScnHeader(ZERO_SCN),
        nextScn(ZERO_SCN),
        nextScnHeader(ZERO_SCN),
        blockSize(0),
        sumRead(0),
        sumTime(0),
        bufferScan(0),
        lastRead(0),
        lastReadTime(0),
        readTime(0),
        loopTime(0),
        bufferStart(0),
        bufferEnd(0),
        status(READER_STATUS_SLEEPING),
        ret(REDO_OK),
        redoBufferList(nullptr) {
    }

    void Reader::initialize() {
        if (redoBufferList == nullptr) {
            redoBufferList = new uint8_t*[ctx->readBufferMax];
            memset(reinterpret_cast<void*>(redoBufferList), 0, ctx->readBufferMax * sizeof(uint8_t*));
        }

        if (headerBuffer == nullptr) {
            headerBuffer = reinterpret_cast<uint8_t*>(aligned_alloc(MEMORY_ALIGNMENT, REDO_PAGE_SIZE_MAX * 2));
            if (headerBuffer == nullptr)
                throw RuntimeException("couldn't allocate " + std::to_string(REDO_PAGE_SIZE_MAX * 2) + " bytes memory (for: read header)");
        }

        if (ctx->redoCopyPath.length() > 0) {
            if ((opendir(ctx->redoCopyPath.c_str())) == nullptr)
                throw RuntimeException("can't access directory: " + ctx->redoCopyPath);
        }
    }

    void Reader::wakeUp() {
        std::unique_lock<std::mutex> lck(mtx);
        condBufferFull.notify_all();
        condReaderSleeping.notify_all();
        condParserSleeping.notify_all();
    }

    Reader::~Reader() {
        for (uint64_t num = 0; num < ctx->readBufferMax; ++num)
            bufferFree(num);

        if (redoBufferList != nullptr) {
            delete[] redoBufferList;
            redoBufferList = nullptr;
        }

        if (headerBuffer != nullptr) {
            free(headerBuffer);
            headerBuffer = nullptr;
        }

        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }
    }

    uint64_t Reader::checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint) {
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_EMPTY;

        if ((blockSize == 512 && buffer[1] != 0x22) ||
                (blockSize == 1024 && buffer[1] != 0x22) ||
                (blockSize == 4096 && buffer[1] != 0x82)) {
            ERROR("invalid block size (found: " << std::dec << blockSize << ", block: " << std::dec << blockNumber <<
                    ", header[1]: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(buffer[1]) << "): " << fileName)
            return REDO_ERROR_BAD_DATA;
        }

        typeBlk blockNumberHeader = ctx->read32(buffer + 4);
        typeSeq sequenceHeader = ctx->read32(buffer + 8);

        if (sequence == 0 || status == READER_STATUS_UPDATE) {
            sequence = sequenceHeader;
        } else {
            if (group == 0) {
                if (sequence != sequenceHeader) {
                    WARNING("invalid header sequence (" << std::dec << sequenceHeader << ", expected: " << sequence << "): " << fileName)
                    return REDO_ERROR_SEQUENCE;
                }
            } else {
                if (sequence > sequenceHeader)
                    return REDO_EMPTY;
                if (sequence < sequenceHeader)
                    return REDO_OVERWRITTEN;
            }
        }

        if (blockNumberHeader != blockNumber) {
            ERROR("invalid header block number (" << std::dec << blockNumberHeader << ", expected: " << blockNumber << "): " << fileName)
            return REDO_ERROR_BLOCK;
        }

        if (!DISABLE_CHECKS(DISABLE_CHECKS_BLOCK_SUM)) {
            typeSum chSum = ctx->read16(buffer + 14);
            typeSum chSum2 = calcChSum(buffer, blockSize);
            if (chSum != chSum2) {
                if (showHint) {
                    WARNING("header sum for block number: " << std::dec << blockNumber <<
                            ", should be: 0x" << std::setfill('0') << std::setw(4) << std::hex << chSum <<
                            ", calculated: 0x" << std::setfill('0') << std::setw(4) << std::hex << chSum2)
                    if (!hintDisplayed) {
                        if (!configuredBlockSum) {
                            WARNING("HINT: set DB_BLOCK_CHECKSUM = TYPICAL on the database"
                                    " or turn off consistency checking in OpenLogReplicator setting parameter disable-checks: " << std::dec <<
                                    DISABLE_CHECKS_BLOCK_SUM << " for the reader")
                        }
                        hintDisplayed = true;
                    }
                }
                return REDO_ERROR_CRC;
            }
        }

        return REDO_OK;
    }

    uint64_t Reader::readSize(uint64_t prevRead) {
        if (prevRead < blockSize)
            return blockSize;

        prevRead *= 2;
        if (prevRead > MEMORY_CHUNK_SIZE)
            prevRead = MEMORY_CHUNK_SIZE;

        return prevRead;
    }

    uint64_t Reader::reloadHeaderRead() {
        if (ctx->softShutdown)
            return REDO_ERROR;

        int64_t bytes = redoRead(headerBuffer, 0, blockSize > 0 ? blockSize * 2: REDO_PAGE_SIZE_MAX * 2);
        if (bytes < 512) {
            ERROR("reading file: " << fileName << " - " << strerror(errno))
            return REDO_ERROR_READ;
        }

        // Check file header
        if (headerBuffer[0] != 0) {
            ERROR("invalid header (header[0]: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[0]) <<
                    "): " << fileName)
            return REDO_ERROR_BAD_DATA;
        }

        if (headerBuffer[28] == 0x7A && headerBuffer[29] == 0x7B && headerBuffer[30] == 0x7C && headerBuffer[31] == 0x7D) {
            if (!ctx->isBigEndian())
                ctx->setBigEndian();
        } else if (headerBuffer[28] != 0x7D || headerBuffer[29] != 0x7C || headerBuffer[30] != 0x7B || headerBuffer[31] != 0x7A || ctx->isBigEndian()) {
            ERROR("invalid header (header[28-31]: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[28]) <<
                    ", 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[29]) <<
                    ", 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[30]) <<
                    ", 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[31]) << "): " << fileName)
            return REDO_ERROR_BAD_DATA;
        }

        bool blockSizeOK = false;
        blockSize = ctx->read32(headerBuffer + 20);
        if ((blockSize == 512 && headerBuffer[1] == 0x22) || (blockSize == 1024 && headerBuffer[1] == 0x22) || (blockSize == 4096 && headerBuffer[1] == 0x82))
            blockSizeOK = true;

        if (!blockSizeOK) {
            ERROR("invalid block size (found: " << std::dec << blockSize <<
                    ", header[1]: 0x" << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[1]) << "): " << fileName)
            blockSize = 0;
            return REDO_ERROR_BAD_DATA;
        }

        if (bytes < static_cast<int64_t>(blockSize * 2)) {
            ERROR("reading file: " << fileName << " - " << strerror(errno))
            return REDO_ERROR_READ;
        }

        if (bytes > 0 && ctx->redoCopyPath.length() > 0) {
            if (static_cast<uint64_t>(bytes) > blockSize * 2)
                bytes = static_cast<int64_t>(blockSize * 2);

            typeSeq sequenceHeader = ctx->read32(headerBuffer + blockSize + 8);
            if (fileCopySequence != sequenceHeader) {
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }
            }

            if (fileCopyDes == -1) {
                fileNameWrite = ctx->redoCopyPath + "/" + database + "_" + std::to_string(sequenceHeader) + ".arc";
                fileCopyDes = open(fileNameWrite.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
                if (fileCopyDes == -1)
                    throw RuntimeException("opening in write mode file: " + fileNameWrite + " - " + strerror(errno));
                INFO("writing redo log copy to: " << fileNameWrite)
                fileCopySequence = sequenceHeader;
            }

            int64_t bytesWritten = pwrite(fileCopyDes, headerBuffer, bytes, 0);
            if (bytesWritten != bytes) {
                ERROR("writing file: " << fileNameWrite << " - " << strerror(errno))
                return REDO_ERROR_WRITE;
            }
        }

        return REDO_OK;
    }

    uint64_t Reader::reloadHeader() {
        uint64_t retReload = reloadHeaderRead();
        if (retReload != REDO_OK)
            return retReload;

        uint64_t version;
        compatVsn = ctx->read32(headerBuffer + blockSize + 20);
        if (compatVsn == 0)
            return REDO_EMPTY;

        if ((compatVsn >= 0x0B200000 && compatVsn <= 0x0B200400)     // 11.2.0.0 - 11.2.0.4
            || (compatVsn >= 0x0C100000 && compatVsn <= 0x0C100200)  // 12.1.0.0 - 12.1.0.2
            || (compatVsn >= 0x0C200000 && compatVsn <= 0x0C200100)  // 12.2.0.0 - 12.2.0.1
            || (compatVsn >= 0x12000000 && compatVsn <= 0x120E0000)  // 18.0.0.0 - 18.14.0.0
            || (compatVsn >= 0x13000000 && compatVsn <= 0x13110000)  // 19.0.0.0 - 19.17.0.0
            || (compatVsn >= 0x15000000 && compatVsn <= 0x15080000)) // 21.0.0.0 - 21.8.0.0
            version = compatVsn;
        else {
            ERROR("invalid database version (found: 0x" << std::setfill('0') << std::setw(8) << std::hex << compatVsn << "): " << fileName)
            return REDO_ERROR_BAD_DATA;
        }

        activation = ctx->read32(headerBuffer + blockSize + 52);
        numBlocksHeader = ctx->read32(headerBuffer + blockSize + 156);
        resetlogs = ctx->read32(headerBuffer + blockSize + 160);
        firstScnHeader = ctx->readScn(headerBuffer + blockSize + 180);
        firstTimeHeader = ctx->read32(headerBuffer + blockSize + 188);
        nextScnHeader = ctx->readScn(headerBuffer + blockSize + 192);

        if (numBlocksHeader != ZERO_BLK && fileSize > static_cast<uint64_t>(numBlocksHeader) * blockSize && group == 0) {
            fileSize = static_cast<uint64_t>(numBlocksHeader) * blockSize;
            INFO("updating redo log size to: " << std::dec << fileSize << " for: " << fileName)
        }

        if (ctx->version == 0) {
            char SID[9];
            memcpy(reinterpret_cast<void*>(SID),
                   reinterpret_cast<const void*>(headerBuffer + blockSize + 28), 8);
            SID[8] = 0;
            ctx->version = version;
            typeSeq sequenceHeader = ctx->read32(headerBuffer + blockSize + 8);

            if (compatVsn < REDO_VERSION_18_0) {
                ctx->versionStr = std::to_string(compatVsn >> 24) + "." + std::to_string((compatVsn >> 20) & 0xF) + "." +
                        std::to_string((compatVsn >> 16) & 0xF) + "." + std::to_string((compatVsn >> 8) & 0xFF);
            } else {
                ctx->versionStr = std::to_string(compatVsn >> 24) + "." + std::to_string((compatVsn >> 16) & 0xFF) + "." +
                        std::to_string((compatVsn >> 8) & 0xFF);
            }
            INFO("found redo log version: " << ctx->versionStr << ", activation: " << std::dec << activation << ", resetlogs: " << std::dec << resetlogs <<
                    ", page: " << std::dec << blockSize << ", sequence: " << std::dec << sequenceHeader << ", SID: " << SID << ", endian: " <<
                    (ctx->isBigEndian() ? "BIG" : "LITTLE"))
        }

        if (version != ctx->version) {
            ERROR("invalid database version (found: 0x" << std::setfill('0') << std::setw(8) << std::hex << compatVsn <<
                    ", expected: 0x" << std::setfill('0') << std::setw(8) << std::hex << ctx->version << "): " << fileName)
            return REDO_ERROR_BAD_DATA;
        }

        uint64_t badBlockCrcCount = 0;
        retReload = checkBlockHeader(headerBuffer + blockSize, 1, false);
        TRACE(TRACE2_DISK, "DISK: block: 1 check: " << retReload)

        while (retReload == REDO_ERROR_CRC) {
            ++badBlockCrcCount;
            if (badBlockCrcCount == REDO_BAD_CDC_MAX_CNT)
                return REDO_ERROR_BAD_DATA;

            usleep(ctx->redoReadSleepUs);
            retReload = checkBlockHeader(headerBuffer + blockSize, 1, false);
            TRACE(TRACE2_DISK, "DISK: block: 1 check: " << retReload)
        }

        if (retReload != REDO_OK)
            return retReload;

        if (firstScn == ZERO_SCN || status == READER_STATUS_UPDATE) {
            firstScn = firstScnHeader;
            nextScn = nextScnHeader;
        } else {
            if (firstScnHeader != firstScn) {
                ERROR("invalid first scn value (found: " << std::dec << firstScnHeader << ", expected: " << std::dec << firstScn << "): " << fileName)
                return REDO_ERROR_BAD_DATA;
            }
        }

        // Updating nextScn if changed
        if (nextScn == ZERO_SCN && nextScnHeader != ZERO_SCN) {
            DEBUG("updating next scn to: " << std::dec << nextScnHeader)
            nextScn = nextScnHeader;
        } else if (nextScn != ZERO_SCN && nextScnHeader != ZERO_SCN && nextScn != nextScnHeader) {
            ERROR("invalid next scn value (found: " << std::dec << nextScnHeader << ", expected: " << std::dec << nextScn << "): " << fileName)
            return REDO_ERROR_BAD_DATA;
        }

        return retReload;
    }

    bool Reader::read1() {
        uint64_t toRead = readSize(lastRead);

        if (bufferScan + toRead > fileSize)
            toRead = fileSize - bufferScan;

        uint64_t redoBufferPos = bufferScan % MEMORY_CHUNK_SIZE;
        uint64_t redoBufferNum = (bufferScan / MEMORY_CHUNK_SIZE) % ctx->readBufferMax;
        if (redoBufferPos + toRead > MEMORY_CHUNK_SIZE)
            toRead = MEMORY_CHUNK_SIZE - redoBufferPos;

        if (toRead == 0) {
            ERROR("zero to read (start: " << std::dec << bufferStart << ", end: " << bufferEnd << ", scan: " << bufferScan << "): " << fileName)
            ret = REDO_ERROR;
            return false;
        }

        bufferAllocate(redoBufferNum);
        TRACE(TRACE2_DISK, "DISK: reading#1 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" <<
                " bytes: " << std::dec << toRead)
        int64_t actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferScan, toRead);

        TRACE(TRACE2_DISK, "DISK: reading#1 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " got: " <<
                std::dec << actualRead)
        if (actualRead < 0) {
            ret = REDO_ERROR_READ;
            return false;
        }

        if (actualRead > 0 && fileCopyDes != -1 && (ctx->redoVerifyDelayUs == 0 || group == 0)) {
            int64_t bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead,
                                          static_cast<int64_t>(bufferEnd));
            if (bytesWritten != actualRead) {
                ERROR("writing file: " << fileNameWrite << " - " << strerror(errno))
                ret = REDO_ERROR_WRITE;
                return false;
            }
        }

        typeBlk maxNumBlock = actualRead / blockSize;
        typeBlk bufferScanBlock = bufferScan / blockSize;
        uint64_t goodBlocks = 0;
        uint64_t currentRet = REDO_OK;

        // Check which blocks are good
        for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
            currentRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + numBlock * blockSize, bufferScanBlock + numBlock,
                                      ctx->redoVerifyDelayUs == 0 || group == 0);
            TRACE(TRACE2_DISK, "DISK: block: " << std::dec << (bufferScanBlock + numBlock) << " check: " << currentRet)

            if (currentRet != REDO_OK)
                break;
            ++goodBlocks;
        }

        // Partial online redo log file
        if (goodBlocks == 0 && group == 0) {
            if (nextScnHeader != ZERO_SCN) {
                ret = REDO_FINISHED;
                nextScn = nextScnHeader;
            } else {
                WARNING("end of online redo log file at position " << std::dec << bufferScan)
                ret = REDO_STOPPED;
            }
            return false;
        }

        // Treat bad blocks as empty
        if (currentRet == REDO_ERROR_CRC && ctx->redoVerifyDelayUs > 0 && group != 0)
            currentRet = REDO_EMPTY;

        if (goodBlocks == 0 && currentRet != REDO_OK && (currentRet != REDO_EMPTY || group == 0)) {
            ret = currentRet;
            return false;
        }

        // Check for log switch
        if (goodBlocks == 0 && currentRet == REDO_EMPTY) {
            currentRet = reloadHeader();
            if (currentRet != REDO_OK) {
                ret = currentRet;
                return false;
            }
            reachedZero = true;
        } else {
            readBlocks = true;
            reachedZero = false;
        }

        lastRead = goodBlocks * blockSize;
        lastReadTime = Timer::getTime();
        if (goodBlocks > 0) {
            if (ctx->redoVerifyDelayUs > 0 && group != 0) {
                bufferScan += goodBlocks * blockSize;

                for (uint64_t numBlock = 0; numBlock < goodBlocks; ++numBlock) {
                    auto readTimeP = reinterpret_cast<time_t*>(redoBufferList[redoBufferNum] + redoBufferPos + numBlock * blockSize);
                    *readTimeP = lastReadTime;
                }
            } else {
                std::unique_lock<std::mutex> lck(mtx);
                bufferEnd += goodBlocks * blockSize;
                bufferScan = bufferEnd;
                condParserSleeping.notify_all();
            }
        }

        // Batch mode with partial online redo log file
        if (currentRet == REDO_ERROR_SEQUENCE && group == 0) {
            if (nextScnHeader != ZERO_SCN) {
                ret = REDO_FINISHED;
                nextScn = nextScnHeader;
            } else {
                WARNING("end of online redo log file at position " << std::dec << bufferScan)
                ret = REDO_STOPPED;
            }
            return false;
        }

        return true;
    }

    bool Reader::read2() {
        uint64_t maxNumBlock = (bufferScan - bufferEnd) / blockSize;
        uint64_t goodBlocks = 0;
        if (maxNumBlock > REDO_READ_VERIFY_MAX_BLOCKS)
            maxNumBlock = REDO_READ_VERIFY_MAX_BLOCKS;

        for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
            uint64_t redoBufferPos = (bufferEnd + numBlock * blockSize) % MEMORY_CHUNK_SIZE;
            uint64_t redoBufferNum = ((bufferEnd + numBlock * blockSize) / MEMORY_CHUNK_SIZE) % ctx->readBufferMax;

            auto readTimeP = reinterpret_cast<time_t*>(redoBufferList[redoBufferNum] + redoBufferPos);
            if (*readTimeP + static_cast<time_t>(ctx->redoVerifyDelayUs) < loopTime) {
                ++goodBlocks;
            } else {
                readTime = *readTimeP + static_cast<time_t>(ctx->redoVerifyDelayUs);
                break;
            }
        }

        if (goodBlocks > 0) {
            uint64_t toRead = readSize(goodBlocks * blockSize);
            if (toRead > goodBlocks * blockSize)
                toRead = goodBlocks * blockSize;

            uint64_t redoBufferPos = bufferEnd % MEMORY_CHUNK_SIZE;
            uint64_t redoBufferNum = (bufferEnd / MEMORY_CHUNK_SIZE) % ctx->readBufferMax;

            if (redoBufferPos + toRead > MEMORY_CHUNK_SIZE)
                toRead = MEMORY_CHUNK_SIZE - redoBufferPos;

            if (toRead == 0) {
                ERROR("zero to read (start: " << std::dec << bufferStart << ", end: " << bufferEnd << ", scan: " << bufferScan << "): " << fileName)
                ret = REDO_ERROR;
                return false;
            }

            TRACE(TRACE2_DISK, "DISK: reading#2 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" <<
                    " bytes: " << std::dec << toRead)
            int64_t actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferEnd, toRead);

            TRACE(TRACE2_DISK, "DISK: reading#2 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" <<
                    " got: " << std::dec << actualRead)
            if (actualRead < 0) {
                ERROR("reading file: " << fileName << " - " << strerror(errno))
                ret = REDO_ERROR_READ;
                return false;
            }
            if (actualRead > 0 && fileCopyDes != -1) {
                int64_t bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead,
                                              static_cast<int64_t>(bufferEnd));
                if (bytesWritten != actualRead) {
                    ERROR("writing file: " << fileNameWrite << " - " << strerror(errno))
                    ret = REDO_ERROR_WRITE;
                    return false;
                }
            }

            readBlocks = true;
            uint64_t currentRet = REDO_OK;
            maxNumBlock = actualRead / blockSize;
            typeBlk bufferEndBlock = bufferEnd / blockSize;

            // Check which blocks are good
            for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                currentRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + numBlock * blockSize, bufferEndBlock + numBlock,
                                              true);
                TRACE(TRACE2_DISK, "DISK: block: " << std::dec << (bufferEndBlock + numBlock) << " check: " << currentRet)

                if (currentRet != REDO_OK)
                    break;
                ++goodBlocks;
            }

            // Verify header for online redo logs after every successful read
            if (currentRet == REDO_OK && group > 0)
                currentRet = reloadHeader();

            if (currentRet != REDO_OK) {
                ret = currentRet;
                return false;
            }

            {
                std::unique_lock<std::mutex> lck(mtx);
                bufferEnd += actualRead;
                condParserSleeping.notify_all();
            }
        }

        return true;
    }

    void Reader::mainLoop() {
        while (!ctx->softShutdown) {
            {
                std::unique_lock<std::mutex> lck(mtx);
                condParserSleeping.notify_all();

                if (status == READER_STATUS_SLEEPING && !ctx->softShutdown) {
                    condReaderSleeping.wait(lck);
                } else if (status == READER_STATUS_READ && !ctx->softShutdown && ctx->buffersFree == 0 && (bufferEnd % MEMORY_CHUNK_SIZE) == 0) {
                    // Buffer full
                    condBufferFull.wait(lck);
                }
            }

            if (ctx->softShutdown)
                break;

            if (status == READER_STATUS_CHECK) {
                TRACE(TRACE2_FILE, "FILE: trying to open: " << fileName)
                redoClose();
                uint64_t currentRet = redoOpen();
                {
                    std::unique_lock<std::mutex> lck(mtx);
                    ret = currentRet;
                    status = READER_STATUS_SLEEPING;
                    condParserSleeping.notify_all();
                }
                continue;

            } else if (status == READER_STATUS_UPDATE) {
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }

                sumRead = 0;
                sumTime = 0;
                uint64_t currentRet = reloadHeader();
                if (currentRet == REDO_OK) {
                    bufferStart = blockSize * 2;
                    bufferEnd = blockSize * 2;
                }

                for (uint64_t num = 0; num < ctx->readBufferMax; ++num)
                    bufferFree(num);

                {
                    std::unique_lock<std::mutex> lck(mtx);
                    ret = currentRet;
                    status = READER_STATUS_SLEEPING;
                    condParserSleeping.notify_all();
                }
            } else if (status == READER_STATUS_READ) {
                TRACE(TRACE2_DISK, "DISK: reading " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << ") at size: " << fileSize)
                lastRead = blockSize;
                lastReadTime = 0;
                readTime = 0;
                bufferScan = bufferEnd;
                reachedZero = false;

                while (!ctx->softShutdown && status == READER_STATUS_READ) {
                    loopTime = Timer::getTime();
                    readBlocks = false;
                    readTime = 0;

                    if (bufferEnd == fileSize) {
                        if (nextScnHeader != ZERO_SCN) {
                            ret = REDO_FINISHED;
                            nextScn = nextScnHeader;
                        } else {
                            WARNING("end of online redo log file at position " << std::dec << bufferScan)
                            ret = REDO_STOPPED;
                        }
                        break;
                    }

                    // Buffer full?
                    if (bufferStart + ctx->bufferSizeMax == bufferEnd) {
                        std::unique_lock<std::mutex> lck(mtx);
                        if (!ctx->softShutdown && bufferStart + ctx->bufferSizeMax == bufferEnd) {
                            condBufferFull.wait(lck);
                            continue;
                        }
                    }

                    if (bufferEnd < bufferScan)
                        if (!read2())
                            break;

                    // #1 read
                    if (bufferScan < fileSize && (ctx->buffersFree > 0 || (bufferScan % MEMORY_CHUNK_SIZE) > 0)
                        && (!reachedZero || lastReadTime + static_cast<time_t>(ctx->redoReadSleepUs) < loopTime))
                        if (!read1())
                            break;

                    if (numBlocksHeader != ZERO_BLK && bufferEnd == static_cast<uint64_t>(numBlocksHeader) * blockSize) {
                        if (nextScnHeader != ZERO_SCN) {
                            ret = REDO_FINISHED;
                            nextScn = nextScnHeader;
                        } else {
                            WARNING("end of online redo log file at position " << std::dec << bufferScan)
                            ret = REDO_STOPPED;
                        }
                        break;
                    }

                    // Sleep some time
                    if (!readBlocks) {
                        if (readTime == 0) {
                            usleep(ctx->redoReadSleepUs);
                        } else {
                            time_t nowTime = Timer::getTime();
                            if (readTime > nowTime) {
                                if (static_cast<time_t>(ctx->redoReadSleepUs) < readTime - nowTime)
                                    usleep(ctx->redoReadSleepUs);
                                else
                                    usleep(readTime - nowTime);
                            }
                        }
                    }
                }

                {
                    std::unique_lock<std::mutex> lck(mtx);
                    status = READER_STATUS_SLEEPING;
                    condParserSleeping.notify_all();
                }
            }
        }
    }

    typeSum Reader::calcChSum(uint8_t* buffer, uint64_t size) const {
        typeSum oldChSum = ctx->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint64_t i = 0; i < size / 8; ++i, buffer += 8)
            sum ^= *(reinterpret_cast<uint64_t*>(buffer));
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }

    void Reader::run() {
        TRACE(TRACE2_THREADS, "THREADS: reader (" << std::hex << std::this_thread::get_id() << ") start")

        try {
            mainLoop();
        } catch (RuntimeException& ex) {
        } catch (std::bad_alloc& ex) {
            ERROR("memory allocation failed: " << ex.what())
        }

        redoClose();
        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }

        TRACE(TRACE2_THREADS, "THREADS: reader (" << std::hex << std::this_thread::get_id() << ") stop")
    }

    void Reader::bufferAllocate(uint64_t num) {
        if (redoBufferList[num] == nullptr) {
            redoBufferList[num] = ctx->getMemoryChunk("reader", false);
            if (ctx->buffersFree == 0)
                throw RuntimeException("couldn't allocate " + std::to_string(MEMORY_CHUNK_SIZE) + " bytes memory (for: read buffer)");

            ctx->allocateBuffer();
        }
    }

    void Reader::bufferFree(uint64_t num) {
        if (redoBufferList[num] != nullptr) {
            ctx->freeMemoryChunk("reader", redoBufferList[num], false);
            redoBufferList[num] = nullptr;
            ctx->releaseBuffer();
        }
    }

    void Reader::printHeaderInfo(std::ostringstream& ss, const std::string& path) const {
        char SID[9];
        memcpy(reinterpret_cast<void*>(SID),
               reinterpret_cast<const void*>(headerBuffer + blockSize + 28), 8);
        SID[8] = 0;

        ss << "DUMP OF REDO FROM FILE '" << path << "'" << std::endl;
        if (ctx->version >= REDO_VERSION_12_2)
            ss << " Container ID: 0" << std::endl << " Container UID: 0" << std::endl;
        ss << " Opcodes *.*" << std::endl;
        if (ctx->version >= REDO_VERSION_12_2)
            ss << " Container ID: 0" << std::endl << " Container UID: 0" << std::endl;
        ss << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff" << std::endl;
        if (ctx->version < REDO_VERSION_12_2)
            ss << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff" << std::endl;
        else
            ss << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff" << std::endl;
        ss << " Times: creation thru eternity" << std::endl;

        uint32_t dbid = ctx->read32(headerBuffer + blockSize + 24);
        uint32_t controlSeq = ctx->read32(headerBuffer + blockSize + 36);
        uint32_t fileSizeHeader = ctx->read32(headerBuffer + blockSize + 40);
        uint16_t fileNumber = ctx->read16(headerBuffer + blockSize + 48);

        ss << " FILE HEADER:" << std::endl <<
                "\tCompatibility Vsn = " << std::dec << compatVsn << "=0x" << std::hex << compatVsn << std::endl <<
                "\tDb ID=" << std::dec << dbid << "=0x" << std::hex << dbid << ", Db Name='" << SID << "'" << std::endl <<
                "\tActivation ID=" << std::dec << activation << "=0x" << std::hex << activation << std::endl <<
                "\tControl Seq=" << std::dec << controlSeq << "=0x" << std::hex << controlSeq << ", File size=" << std::dec << fileSizeHeader << "=0x" <<
                std::hex << fileSizeHeader << std::endl <<
                "\tFile Number=" << std::dec << fileNumber << ", Blksiz=" << std::dec << blockSize << ", File Type=2 LOG" << std::endl;

        typeSeq seq = ctx->read32(headerBuffer + blockSize + 8);
        uint8_t descrip[65];
        memcpy (reinterpret_cast<void*>(descrip),
                reinterpret_cast<const void*>(headerBuffer + blockSize + 92), 64);
        descrip[64] = 0;
        uint16_t thread = ctx->read16(headerBuffer + blockSize + 176);
        uint32_t hws = ctx->read32(headerBuffer + blockSize + 172);
        uint8_t eot = headerBuffer[blockSize + 204];
        uint8_t dis = headerBuffer[blockSize + 205];

        ss << R"( descrip:")" << descrip << R"(")" << std::endl <<
           " thread: " << std::dec << thread <<
           " nab: 0x" << std::hex << numBlocksHeader <<
           " seq: 0x" << std::setfill('0') << std::setw(8) << std::hex << seq <<
           " hws: 0x" << std::hex << hws <<
                " eot: " << std::dec << static_cast<uint64_t>(eot) <<
                " dis: " << std::dec << static_cast<uint64_t>(dis) << std::endl;

        typeScn resetlogsScn = ctx->readScn(headerBuffer + blockSize + 164);
        typeResetlogs prevResetlogsCnt = ctx->read32(headerBuffer + blockSize + 292);
        typeScn prevResetlogsScn = ctx->readScn(headerBuffer + blockSize + 284);
        typeTime nextTime(ctx->read32(headerBuffer + blockSize + 200));
        typeScn enabledScn = ctx->readScn(headerBuffer + blockSize + 208);
        typeTime enabledTime(ctx->read32(headerBuffer + blockSize + 216));
        typeScn threadClosedScn = ctx->readScn(headerBuffer + blockSize + 220);
        typeTime threadClosedTime(ctx->read32(headerBuffer + blockSize + 228));
        typeScn termialRecScn = ctx->readScn(headerBuffer + blockSize + 240);
        typeTime termialRecTime(ctx->read32(headerBuffer + blockSize + 248));
        typeScn mostRecentScn = ctx->readScn(headerBuffer + blockSize + 260);
        typeSum chSum = ctx->read16(headerBuffer + blockSize + 14);
        typeSum chSum2 = calcChSum(headerBuffer + blockSize, blockSize);

        if (ctx->version < REDO_VERSION_12_2) {
            ss << " resetlogs count: 0x" << std::hex << resetlogs << " scn: " << PRINTSCN48(resetlogsScn) << " (" << std::dec << resetlogsScn << ")" << std::endl <<
                    " prev resetlogs count: 0x" << std::hex << prevResetlogsCnt << " scn: " << PRINTSCN48(prevResetlogsScn) << " (" << std::dec << prevResetlogsScn << ")" << std::endl <<
                    " Low  scn: " << PRINTSCN48(firstScnHeader) << " (" << std::dec << firstScnHeader << ")" << " " << firstTimeHeader << std::endl <<
                    " Next scn: " << PRINTSCN48(nextScnHeader) << " (" << std::dec << nextScn << ")" << " " << nextTime << std::endl <<
                    " Enabled scn: " << PRINTSCN48(enabledScn) << " (" << std::dec << enabledScn << ")" << " " << enabledTime << std::endl <<
                    " Thread closed scn: " << PRINTSCN48(threadClosedScn) << " (" << std::dec << threadClosedScn << ")" << " " << threadClosedTime << std::endl <<
                    " Disk cksum: 0x" << std::hex << chSum << " Calc cksum: 0x" << std::hex << chSum2 << std::endl <<
                    " Terminal recovery stop scn: " << PRINTSCN48(termialRecScn) << std::endl <<
                    " Terminal recovery  " << termialRecTime << std::endl <<
                    " Most recent redo scn: " << PRINTSCN48(mostRecentScn) << std::endl;
        } else {
            typeScn realNextScn = ctx->readScn(headerBuffer + blockSize + 272);

            ss << " resetlogs count: 0x" << std::hex << resetlogs << " scn: " << PRINTSCN64(resetlogsScn) << std::endl <<
                    " prev resetlogs count: 0x" << std::hex << prevResetlogsCnt << " scn: " << PRINTSCN64(prevResetlogsScn) << std::endl <<
                    " Low  scn: " << PRINTSCN64(firstScnHeader) << " " << firstTimeHeader << std::endl <<
                    " Next scn: " << PRINTSCN64(nextScnHeader) << " " << nextTime << std::endl <<
                    " Enabled scn: " << PRINTSCN64(enabledScn) << " " << enabledTime << std::endl <<
                    " Thread closed scn: " << PRINTSCN64(threadClosedScn) << " " << threadClosedTime << std::endl <<
                    " Real next scn: " << PRINTSCN64(realNextScn) << std::endl <<
                    " Disk cksum: 0x" << std::hex << chSum << " Calc cksum: 0x" << std::hex << chSum2 << std::endl <<
                    " Terminal recovery stop scn: " << PRINTSCN64(termialRecScn) << std::endl <<
                    " Terminal recovery  " << termialRecTime << std::endl <<
                    " Most recent redo scn: " << PRINTSCN64(mostRecentScn) << std::endl;
        }

        uint32_t largestLwn = ctx->read32(headerBuffer + blockSize + 268);
        ss <<
                        " Largest LWN: " << std::dec << largestLwn << " blocks" << std::endl;

        uint32_t miscFlags = ctx->read32(headerBuffer + blockSize + 236);
        const char* endOfRedo;
        if ((miscFlags & REDO_END) != 0)
            endOfRedo = "Yes";
        else
            endOfRedo = "No";
        if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
            ss << " FailOver End-of-redo stream : " << endOfRedo << std::endl;
        else
            ss << " End-of-redo stream : " << endOfRedo << std::endl;

        if ((miscFlags & REDO_ASYNC) != 0)
            ss << " Archivelog created using asynchronous network transmittal" << std::endl;

        if ((miscFlags & REDO_NODATALOSS) != 0)
            ss << " No ctx-loss mode" << std::endl;

        if ((miscFlags & REDO_RESYNC) != 0)
            ss << " Resynchronization mode" << std::endl;
        else
            ss << " Unprotected mode" << std::endl;

        if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
            ss << " Closed thread archival" << std::endl;

        if ((miscFlags & REDO_MAXPERFORMANCE) != 0)
            ss << " Maximize performance mode" << std::endl;

        ss << " Miscellaneous flags: 0x" << std::hex << miscFlags << std::endl;

        if (ctx->version >= REDO_VERSION_12_2) {
            uint32_t miscFlags2 = ctx->read32(headerBuffer + blockSize + 296);
            ss << " Miscellaneous second flags: 0x" << std::hex << miscFlags2 << std::endl;
        }

        auto thr = static_cast<int32_t>(ctx->read32(headerBuffer + blockSize + 432));
        auto seq2 = static_cast<int32_t>(ctx->read32(headerBuffer + blockSize + 436));
        typeScn scn2 = ctx->readScn(headerBuffer + blockSize + 440);
        uint8_t zeroBlocks = headerBuffer[blockSize + 206];
        uint8_t formatId = headerBuffer[blockSize + 207];
        if (ctx->version < REDO_VERSION_12_2)
            ss << " Thread internal enable indicator: thr: " << std::dec << thr << "," <<
                    " seq: " << std::dec << seq2 <<
                    " scn: " << PRINTSCN48(scn2) << std::endl <<
                    " Zero blocks: " << std::dec << static_cast<uint64_t>(zeroBlocks) << std::endl <<
                    " Format ID is " << std::dec << static_cast<uint64_t>(formatId) << std::endl;
        else
            ss << " Thread internal enable indicator: thr: " << std::dec << thr << "," <<
                    " seq: " << std::dec << seq2 <<
                    " scn: " << PRINTSCN64(scn2) << std::endl <<
                    " Zero blocks: " << std::dec << static_cast<uint64_t>(zeroBlocks) << std::endl <<
                    " Format ID is " << std::dec << static_cast<uint64_t>(formatId) << std::endl;

        uint32_t standbyApplyDelay = ctx->read32(headerBuffer + blockSize + 280);
        if (standbyApplyDelay > 0)
            ss << " Standby Apply Delay: " << std::dec << standbyApplyDelay << " minute(s) " << std::endl;

        typeTime standbyLogCloseTime(ctx->read32(headerBuffer + blockSize + 304));
        if (standbyLogCloseTime.getVal() > 0)
            ss << " Standby Log Close Time:  " << standbyLogCloseTime << std::endl;

        ss << " redo log key is ";
        for (uint64_t i = 448; i < 448 + 16; ++i)
            ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint64_t>(headerBuffer[blockSize + i]);
        ss << std::endl;

        uint16_t redoKeyFlag = ctx->read16(headerBuffer + blockSize + 480);
        ss << " redo log key flag is " << std::dec << redoKeyFlag << std::endl;
        uint16_t enabledRedoThreads = 1; // TODO: find field position/size
        ss << " Enabled redo threads: " << std::dec << enabledRedoThreads << " " << std::endl;
    }

    uint64_t Reader::getBlockSize() {
        return blockSize;
    }

    uint64_t Reader::getBufferStart() {
        return bufferStart;
    }

    uint64_t Reader::getBufferEnd() {
        return bufferEnd;
    }

    uint64_t Reader::getRet() {
        return ret;
    }

    typeScn Reader::getFirstScn() {
        return firstScn;
    }

    typeScn Reader::getFirstScnHeader() {
        return firstScnHeader;
    }

    typeScn Reader::getNextScn() {
        return nextScn;
    }

    typeBlk Reader::getNumBlocks() {
        return numBlocksHeader;
    }

    int64_t Reader::getGroup() {
        return group;
    }

    typeSeq Reader::getSequence() {
        return sequence;
    }

    typeResetlogs Reader::getResetlogs() {
        return resetlogs;
    }

    typeActivation Reader::getActivation() {
        return activation;
    }

    uint64_t Reader::getSumRead() {
        return sumRead;
    }

    uint64_t Reader::getSumTime() {
        return sumTime;
    }

    void Reader::setRet(uint64_t newRet) {
        ret = newRet;
    }

    void Reader::setBufferStartEnd(uint64_t newBufferStart, uint64_t newBufferEnd) {
        bufferStart = newBufferStart;
        bufferEnd = newBufferEnd;
    }

    bool Reader::checkRedoLog() {
        std::unique_lock<std::mutex> lck(mtx);
        status = READER_STATUS_CHECK;
        sequence = 0;
        firstScn = ZERO_SCN;
        nextScn = ZERO_SCN;
        condBufferFull.notify_all();
        condReaderSleeping.notify_all();

        while (status == READER_STATUS_CHECK) {
            if (ctx->softShutdown)
                break;
            condParserSleeping.wait(lck);
        }
        if (ret == REDO_OK)
            return true;
        else
            return false;
    }

    bool Reader::updateRedoLog() {
        for(;;) {
            std::unique_lock<std::mutex> lck(mtx);
            status = READER_STATUS_UPDATE;
            condBufferFull.notify_all();
            condReaderSleeping.notify_all();

            while (status == READER_STATUS_UPDATE) {
                if (ctx->softShutdown)
                    break;
                condParserSleeping.wait(lck);
            }

            if (ret == REDO_EMPTY) {
                usleep(ctx->redoReadSleepUs);
                continue;
            }

            if (ret == REDO_OK)
                return true;
            else
                return false;
        }
    }

    void Reader::setStatusRead() {
        std::unique_lock<std::mutex> lck(mtx);
        status = READER_STATUS_READ;
        condBufferFull.notify_all();
        condReaderSleeping.notify_all();
    }

    void Reader::confirmReadData(uint64_t confirmedBufferStart) {
        std::unique_lock<std::mutex> lck(mtx);
        bufferStart = confirmedBufferStart;
        if (status == READER_STATUS_READ) {
            condBufferFull.notify_all();
        }
    }

    bool Reader::checkFinished(uint64_t confirmedBufferStart) {
        std::unique_lock<std::mutex> lck(mtx);
        if (bufferStart < confirmedBufferStart)
            bufferStart = confirmedBufferStart;

        // All work done
        if (confirmedBufferStart == bufferEnd) {
            if (ret == REDO_STOPPED || ret == REDO_OVERWRITTEN || ret == REDO_FINISHED || status == READER_STATUS_SLEEPING)
                return true;
            condParserSleeping.wait(lck);
        }
        return false;
    }
}
