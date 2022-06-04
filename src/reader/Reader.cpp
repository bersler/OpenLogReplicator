/* Base class for process which is reading from redo log files
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

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64
#include <dirent.h>
#include <fcntl.h>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>

#include "OracleAnalyzer.h"
#include "Reader.h"
#include "RuntimeException.h"

namespace OpenLogReplicator {
    char* Reader::REDO_CODE[] = {"OK", "OVERWRITTEN", "FINISHED", "STOPPED", "EMPTY", "READ ERROR", "WRITE ERROR", "SEQUENCE ERROR",
            "CRC ERROR", "BLOCK ERROR", "BAD DATA ERROR", "OTHER ERROR"};

    Reader::Reader(const char* alias, OracleAnalyzer* oracleAnalyzer, int64_t group) :
        Thread(alias),
        oracleAnalyzer(oracleAnalyzer),
        hintDisplayed(false),
        fileCopyDes(-1),
        fileCopySequence(0),
        redoBufferList(nullptr),
        headerBuffer(nullptr),
        group(group),
        sequence(0),
        blockSize(0),
        numBlocksHeader(ZERO_BLK),
        numBlocks(0),
        firstScn(ZERO_SCN),
        nextScn(ZERO_SCN),
        sumRead(0),
        sumTime(0),
        compatVsn(0),
        resetlogsHeader(0),
        activationHeader(0),
        firstScnHeader(0),
        nextScnHeader(ZERO_SCN),
        fileSize(0),
        status(READER_STATUS_SLEEPING),
        bufferStart(0),
        bufferEnd(0),
        bufferSizeMax(oracleAnalyzer->readBufferMax * MEMORY_CHUNK_SIZE),
        buffersFree(oracleAnalyzer->readBufferMax),
        buffersMaxUsed(0) {
    }

    void Reader::initialize(void) {
        if (redoBufferList == nullptr) {
            redoBufferList = new uint8_t*[oracleAnalyzer->readBufferMax];
            if (redoBufferList == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << (oracleAnalyzer->readBufferMax * sizeof(uint8_t*)) << " bytes memory (for: read buffer list)");
            }
            memset(redoBufferList, 0, oracleAnalyzer->readBufferMax * sizeof(uint8_t*));
        }

        if (headerBuffer == nullptr) {
            headerBuffer = (uint8_t*) aligned_alloc(MEMORY_ALIGNMENT, REDO_PAGE_SIZE_MAX * 2);
            if (headerBuffer == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << (REDO_PAGE_SIZE_MAX * 2) << " bytes memory (for: read header)");
            }
        }

        if (oracleAnalyzer->redoCopyPath.length() > 0) {
            DIR* dir;
            if ((dir = opendir(oracleAnalyzer->redoCopyPath.c_str())) == nullptr) {
                RUNTIME_FAIL("can't access directory: " << oracleAnalyzer->redoCopyPath);
            }
        }
    }

    Reader::~Reader() {
        for (uint64_t num = 0; num < oracleAnalyzer->readBufferMax; ++num)
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

    uint64_t Reader::checkBlockHeader(uint8_t* buffer, typeBLK blockNumber, bool checkSum, bool showHint) {
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_EMPTY;
        if (shutdown)
            return REDO_ERROR;

        if ((blockSize == 512 && buffer[1] != 0x22) ||
                (blockSize == 1024 && buffer[1] != 0x22) ||
                (blockSize == 4096 && buffer[1] != 0x82)) {
            ERROR("invalid block size (found: " << std::dec << blockSize << ", block: " << std::dec << blockNumber <<
                    ", header[1]: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)buffer[1] << "): " << fileName);
            return REDO_ERROR_BAD_DATA;
        }

        typeBLK blockNumberHeader = oracleAnalyzer->read32(buffer + 4);
        typeSEQ sequenceHeader = oracleAnalyzer->read32(buffer + 8);

        if (sequence == 0 || status == READER_STATUS_UPDATE) {
            sequence = sequenceHeader;
        } else {
            if (group == 0) {
                if (sequence != sequenceHeader) {
                    WARNING("invalid header sequence (" << std::dec << sequenceHeader << ", expected: " << sequence << "): " << fileName);
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
            ERROR("invalid header block number (" << std::dec << blockNumberHeader << ", expected: " << blockNumber << "): " << fileName);
            return REDO_ERROR_BLOCK;
        }

        if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_BLOCK_SUM) == 0) {
            typeSUM chSum = oracleAnalyzer->read16(buffer + 14);
            typeSUM chSum2 = calcChSum(buffer, blockSize);
            if (chSum != chSum2) {
                if (showHint) {
                    WARNING("header sum for block number: " << std::dec << blockNumber <<
                            ", should be: 0x" << std::setfill('0') << std::setw(4) << std::hex << chSum <<
                            ", calculated: 0x" << std::setfill('0') << std::setw(4) << std::hex << chSum2);
                    if (!hintDisplayed) {
                        if (oracleAnalyzer->dbBlockChecksum.compare("OFF") == 0 || oracleAnalyzer->dbBlockChecksum.compare("FALSE") == 0) {
                            WARNING("HINT: set DB_BLOCK_CHECKSUM = TYPICAL on the database"
                                    " or turn off consistency checking in OpenLogReplicator setting parameter disable-checks: "
                                    << std::dec << DISABLE_CHECK_BLOCK_SUM << " for the reader");
                        }
                        hintDisplayed = true;
                    }
                }
                return REDO_ERROR_CRC;
            }
        }

        return REDO_OK;
    }

    uint64_t Reader::readSize(uint64_t lastRead) {
        if (lastRead < blockSize)
            return blockSize;

        lastRead *= 2;
        if (lastRead > MEMORY_CHUNK_SIZE)
            lastRead = MEMORY_CHUNK_SIZE;

        return lastRead;
    }

    uint64_t Reader::reloadHeaderRead(void) {
        if (shutdown)
            return REDO_ERROR;

        int64_t bytes = redoRead(headerBuffer, 0, blockSize > 0 ? blockSize * 2: REDO_PAGE_SIZE_MAX * 2);
        if (bytes < 512) {
            ERROR("reading file: " << fileName << " - " << strerror(errno));
            return REDO_ERROR_READ;
        }

        //check file header
        if (headerBuffer[0] != 0) {
            ERROR("invalid header (header[0]: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)headerBuffer[0] << "): " << fileName);
            return REDO_ERROR_BAD_DATA;
        }

        if (headerBuffer[28] == 0x7A && headerBuffer[29] == 0x7B && headerBuffer[30] == 0x7C && headerBuffer[31] == 0x7D) {
            if (!oracleAnalyzer->bigEndian)
                oracleAnalyzer->setBigEndian();
        } else
        if (headerBuffer[28] != 0x7D || headerBuffer[29] != 0x7C || headerBuffer[30] != 0x7B || headerBuffer[31] != 0x7A ||
                oracleAnalyzer->bigEndian) {
            ERROR("invalid header (header[28-31]: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)headerBuffer[28] <<
                    ", 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)headerBuffer[29] <<
                    ", 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)headerBuffer[30] <<
                    ", 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)headerBuffer[31] << "): " << fileName);
            return REDO_ERROR_BAD_DATA;
        }

        bool blockSizeOK = false;
        blockSize = oracleAnalyzer->read32(headerBuffer + 20);
        if (blockSize == 512 && headerBuffer[1] == 0x22)
            blockSizeOK = true;
        else
        if (blockSize == 1024 && headerBuffer[1] == 0x22)
            blockSizeOK = true;
        else
        if (blockSize == 4096 && headerBuffer[1] == 0x82)
            blockSizeOK = true;

        if (!blockSizeOK) {
            ERROR("invalid block size (found: " << std::dec << blockSize <<
                    ", header[1]: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)headerBuffer[1] << "): " << fileName);
            blockSize = 0;
            return REDO_ERROR_BAD_DATA;
        }

        if (bytes < ((int64_t)blockSize * 2)) {
            ERROR("reading file: " << fileName << " - " << strerror(errno));
            return REDO_ERROR_READ;
        }

        if (bytes > 0 && oracleAnalyzer->redoCopyPath.length() > 0) {
            if (bytes > blockSize * 2)
                bytes = blockSize * 2;

            typeSEQ sequenceHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 8);
            if (fileCopySequence != sequenceHeader) {
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }
            }

            if (fileCopyDes == -1) {
                fileNameWrite = oracleAnalyzer->redoCopyPath + "/" + oracleAnalyzer->database + "_" + std::to_string(sequenceHeader) + ".arc";
                fileCopyDes = open(fileNameWrite.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
                if (fileCopyDes == -1) {
                    RUNTIME_FAIL("opening in write mode file: " << std::dec << fileNameWrite << " - " << strerror(errno));
                }
                INFO("writing redo log copy to: " << fileNameWrite);
                fileCopySequence = sequenceHeader;
            }

            int64_t bytesWritten = pwrite(fileCopyDes, headerBuffer, bytes, 0);
            if (bytesWritten != bytes) {
                ERROR("writing file: " << fileNameWrite << " - " << strerror(errno));
                return REDO_ERROR_WRITE;
            }
        }

        numBlocks = oracleAnalyzer->read32(headerBuffer + 24);
        return REDO_OK;
    }

    uint64_t Reader::reloadHeader(void) {
        uint64_t ret = reloadHeaderRead();
        if (ret != REDO_OK)
            return ret;

        uint64_t version = 0;
        compatVsn = oracleAnalyzer->read32(headerBuffer + blockSize + 20);
        if (compatVsn == 0)
            return REDO_EMPTY;

        if ((compatVsn >= 0x0B200000 && compatVsn <= 0x0B200400) //11.2.0.0 - 11.2.0.4
            || (compatVsn >= 0x0C100000 && compatVsn <= 0x0C100200) //12.1.0.0 - 12.1.0.2
            || (compatVsn >= 0x0C200000 && compatVsn <= 0x0C200100) //12.2.0.0 - 12.2.0.1
            || (compatVsn >= 0x12000000 && compatVsn <= 0x120E0000) //18.0.0.0 - 18.14.0.0
            || (compatVsn >= 0x13000000 && compatVsn <= 0x130E0000) //19.0.0.0 - 19.14.0.0
            || (compatVsn >= 0x15000000 && compatVsn <= 0x15050000)) //21.0.0.0 - 21.5.0.0
            version = compatVsn;
        else {
            ERROR("invalid database version (found: 0x" << std::setfill('0') << std::setw(8) << std::hex << compatVsn << "): " << fileName);
            return REDO_ERROR_BAD_DATA;
        }

        activationHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 52);
        numBlocksHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 156);
        resetlogsHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 160);
        firstScnHeader = oracleAnalyzer->readSCN(headerBuffer + blockSize + 180);
        firstTimeHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 188);
        nextScnHeader = oracleAnalyzer->readSCN(headerBuffer + blockSize + 192);

        if (numBlocksHeader != ZERO_BLK && fileSize > ((uint64_t)numBlocksHeader) * blockSize && group == 0) {
            fileSize = ((uint64_t)numBlocksHeader) * blockSize;
            INFO("updating redo log size to: " << std::dec << fileSize << " for: " << fileName);
        }

        if (oracleAnalyzer->version == 0) {
            char SID[9];
            memcpy(SID, headerBuffer + blockSize + 28, 8); SID[8] = 0;
            oracleAnalyzer->version = version;
            typeSEQ sequenceHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 8);

            if (compatVsn < REDO_VERSION_18_0) {
                INFO("found redo log version: " << std::dec << (compatVsn >> 24) << "." << ((compatVsn >> 20) & 0xF) << "."
                        << ((compatVsn >> 16) & 0xF) << "." << ((compatVsn >> 8) & 0xFF)
                        << ", activation: " << std::dec << activationHeader
                        << ", resetlogs: " << std::dec << resetlogsHeader
                        << ", page: " << std::dec << blockSize
                        << ", sequence: " << std::dec << sequenceHeader
                        << ", SID: " << SID
                        << ", endian: " << (oracleAnalyzer->bigEndian ? "BIG" : "LITTLE"));
            } else {
                INFO("found redo log version: " << std::dec << (compatVsn >> 24) << "." << ((compatVsn >> 16) & 0xFF) << "."
                        << ((compatVsn >> 8) & 0xFF)
                        << ", activation: " << std::dec << activationHeader
                        << ", resetlogs: " << std::dec << resetlogsHeader
                        << ", page: " << std::dec << blockSize
                        << ", sequence: " << std::dec << sequenceHeader
                        << ", SID: " << SID
                        << ", endian: " << (oracleAnalyzer->bigEndian ? "BIG" : "LITTLE"));
            }
        }

        if (version != oracleAnalyzer->version) {
            ERROR("invalid database version (found: 0x" << std::setfill('0') << std::setw(8) << std::hex << compatVsn <<
                    ", expected: 0x" << std::setfill('0') << std::setw(8) << std::hex << oracleAnalyzer->version << "): " << fileName);
            return REDO_ERROR_BAD_DATA;
        }

        uint64_t badBlockCrcCount = 0;
        ret = checkBlockHeader(headerBuffer + blockSize, 1, true, false);
        TRACE(TRACE2_DISK, "DISK: block: 1 check: " << ret);

        while (ret == REDO_ERROR_CRC) {
            ++badBlockCrcCount;
            if (badBlockCrcCount == REDO_BAD_CDC_MAX_CNT)
                return REDO_ERROR_BAD_DATA;

            usleep(oracleAnalyzer->redoReadSleepUs);
            ret = checkBlockHeader(headerBuffer + blockSize, 1, true, false);
            TRACE(TRACE2_DISK, "DISK: block: 1 check: " << ret);
        }

        if (ret != REDO_OK)
            return ret;

        if (firstScn == ZERO_SCN || status == READER_STATUS_UPDATE) {
            firstScn = firstScnHeader;
            nextScn = nextScnHeader;
        } else {
            if (firstScnHeader != firstScn) {
                ERROR("invalid first scn value (found: " << std::dec << firstScnHeader << ", expected: " << std::dec << firstScn << "): " << fileName);
                return REDO_ERROR_BAD_DATA;
            }
        }

        //updating nextScn if changed
        if (nextScn == ZERO_SCN && nextScnHeader != ZERO_SCN) {
            DEBUG("updating next scn to: " << std::dec << nextScnHeader);
            nextScn = nextScnHeader;
        } else
        if (nextScn != ZERO_SCN && nextScnHeader != ZERO_SCN && nextScn != nextScnHeader) {
            ERROR("invalid next scn value (found: " << std::dec << nextScnHeader << ", expected: " << std::dec << nextScn << "): " << fileName);
            return REDO_ERROR_BAD_DATA;
        }

        return ret;
    }

    typeSUM Reader::calcChSum(uint8_t* buffer, uint64_t size) const {
        typeSUM oldChSum = oracleAnalyzer->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint64_t i = 0; i < size / 8; ++i, buffer += 8)
            sum ^= *((uint64_t*)buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }

    void* Reader::run(void) {
        TRACE(TRACE2_THREADS, "THREADS: READER (" << std::hex << std::this_thread::get_id() << ") START");

        try {
            while (!shutdown) {
                {
                    std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                    oracleAnalyzer->analyzerCond.notify_all();

                    if (status == READER_STATUS_SLEEPING && !shutdown) {
                        oracleAnalyzer->sleepingCond.wait(lck);
                    } else if (status == READER_STATUS_READ && !shutdown && buffersFree == 0 && (bufferEnd % MEMORY_CHUNK_SIZE) == 0) {
                        //buffer full
                        oracleAnalyzer->readerCond.wait(lck);
                    }
                }

                if (shutdown)
                    break;

                if (status == READER_STATUS_CHECK) {
                    TRACE(TRACE2_FILE, "FILE: trying to open: " << fileName);
                    redoClose();
                    uint64_t tmpRet = redoOpen();
                    {
                        std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                        ret = tmpRet;
                        status = READER_STATUS_SLEEPING;
                        oracleAnalyzer->analyzerCond.notify_all();
                    }
                    continue;

                } else if (status == READER_STATUS_UPDATE) {
                    if (fileCopyDes != -1) {
                        close(fileCopyDes);
                        fileCopyDes = -1;
                    }

                    sumRead = 0;
                    sumTime = 0;
                    uint64_t tmpRet = reloadHeader();
                    if (tmpRet == REDO_OK) {
                        bufferStart = blockSize * 2;
                        bufferEnd = blockSize * 2;
                    }

                    for (uint64_t num = 0; num < oracleAnalyzer->readBufferMax; ++num)
                        bufferFree(num);

                    {
                        std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                        ret = tmpRet;
                        status = READER_STATUS_SLEEPING;
                        oracleAnalyzer->analyzerCond.notify_all();
                    }
                } else if (status == READER_STATUS_READ) {
                    TRACE(TRACE2_DISK, "DISK: reading " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << ") at size: " << fileSize);
                    uint64_t lastRead = blockSize;
                    clock_t lastReadTime = 0;
                    clock_t readTime = 0;
                    uint64_t bufferScan = bufferEnd;
                    bool readBlocks = false;
                    bool reachedZero = false;

                    while (!shutdown && status == READER_STATUS_READ) {
                        clock_t loopTime = getTime();
                        readBlocks = false;
                        readTime = 0;

                        if (bufferEnd == fileSize) {
                            if (nextScnHeader != ZERO_SCN) {
                                ret = REDO_FINISHED;
                                oracleAnalyzer->nextScn = nextScnHeader;
                            } else {
                                WARNING("end of online redo log file at position " << std::dec << bufferScan);
                                ret = REDO_STOPPED;
                            } break;
                        }

                        //buffer full?
                        if (bufferStart + bufferSizeMax == bufferEnd) {
                            std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                            if (!shutdown && bufferStart + bufferSizeMax == bufferEnd) {
                                oracleAnalyzer->readerCond.wait(lck);
                                continue;
                            }
                        }

                        //#2 read
                        if (bufferEnd < bufferScan) {
                            uint64_t maxNumBlock = (bufferScan - bufferEnd) / blockSize;
                            uint64_t goodBlocks = 0;
                            if (maxNumBlock > REDO_READ_VERIFY_MAX_BLOCKS)
                                maxNumBlock = REDO_READ_VERIFY_MAX_BLOCKS;

                            for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                                uint64_t redoBufferPos = (bufferEnd + numBlock * blockSize) % MEMORY_CHUNK_SIZE;
                                uint64_t redoBufferNum = ((bufferEnd + numBlock * blockSize) / MEMORY_CHUNK_SIZE) % oracleAnalyzer->readBufferMax;

                                time_t* readTimeP = (time_t*) (redoBufferList[redoBufferNum] + redoBufferPos);
                                if (*readTimeP + oracleAnalyzer->redoVerifyDelayUs < loopTime)
                                    ++goodBlocks;
                                else {
                                    readTime = *readTimeP + oracleAnalyzer->redoVerifyDelayUs;
                                    break;
                                }
                            }

                            if (goodBlocks > 0) {
                                uint64_t toRead = readSize(goodBlocks * blockSize);
                                if (toRead > goodBlocks * blockSize)
                                    toRead = goodBlocks * blockSize;

                                uint64_t redoBufferPos = bufferEnd % MEMORY_CHUNK_SIZE;
                                uint64_t redoBufferNum = (bufferEnd / MEMORY_CHUNK_SIZE) % oracleAnalyzer->readBufferMax;

                                if (redoBufferPos + toRead > MEMORY_CHUNK_SIZE)
                                    toRead = MEMORY_CHUNK_SIZE - redoBufferPos;

                                if (toRead == 0) {
                                    ERROR("zero to read (start: " << std::dec << bufferStart << ", end: " << bufferEnd << ", scan: " << bufferScan << "): " << fileName);
                                    ret = REDO_ERROR;
                                    break;
                                }

                                TRACE(TRACE2_DISK, "DISK: reading#2 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " bytes: " << std::dec << toRead);
                                int64_t actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferEnd, toRead);

                                TRACE(TRACE2_DISK, "DISK: reading#2 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " got: " << std::dec << actualRead);
                                if (actualRead < 0) {
                                    ERROR("reading file: " << fileName << " - " << strerror(errno));
                                    ret = REDO_ERROR_READ;
                                    break;
                                }
                                if (actualRead > 0 && fileCopyDes != -1) {
                                    int64_t bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead, bufferEnd);
                                    if (bytesWritten != actualRead) {
                                        ERROR("writing file: " << fileNameWrite << " - " << strerror(errno));
                                        ret = REDO_ERROR_WRITE;
                                        break;
                                    }
                                }

                                readBlocks = true;
                                uint64_t tmpRet = REDO_OK;
                                typeBLK maxNumBlock = actualRead / blockSize;
                                typeBLK bufferEndBlock = bufferEnd / blockSize;

                                //check which blocks are good
                                for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                                    tmpRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + numBlock * blockSize, bufferEndBlock + numBlock,
                                            false, true);
                                    TRACE(TRACE2_DISK, "DISK: block: " << std::dec << (bufferEndBlock + numBlock) << " check: " << tmpRet);

                                    if (tmpRet != REDO_OK)
                                        break;
                                    ++goodBlocks;
                                }

                                //verify header for online redo logs after every successful read
                                if (tmpRet == REDO_OK && group > 0)
                                    tmpRet = reloadHeader();

                                if (tmpRet != REDO_OK) {
                                    ret = tmpRet;
                                    break;
                                }

                                {
                                    std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                                    bufferEnd += actualRead;
                                    oracleAnalyzer->analyzerCond.notify_all();
                                }
                            }
                        }

                        //#1 read
                        if (bufferScan < fileSize && (buffersFree > 0 || (bufferScan % MEMORY_CHUNK_SIZE) > 0)
                                && (!reachedZero || lastReadTime + oracleAnalyzer->redoReadSleepUs < loopTime)) {
                            uint64_t toRead = readSize(lastRead);

                            if (bufferScan + toRead > fileSize)
                                toRead = fileSize - bufferScan;

                            uint64_t redoBufferPos = bufferScan % MEMORY_CHUNK_SIZE;
                            uint64_t redoBufferNum = (bufferScan / MEMORY_CHUNK_SIZE) % oracleAnalyzer->readBufferMax;
                            if (redoBufferPos + toRead > MEMORY_CHUNK_SIZE)
                                toRead = MEMORY_CHUNK_SIZE - redoBufferPos;

                            if (toRead == 0) {
                                ERROR("zero to read (start: " << std::dec << bufferStart << ", end: " << bufferEnd << ", scan: " << bufferScan << "): " << fileName);
                                ret = REDO_ERROR;
                                break;
                            }

                            bufferAllocate(redoBufferNum);
                            TRACE(TRACE2_DISK, "DISK: reading#1 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " bytes: " << std::dec << toRead);
                            int64_t actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferScan, toRead);

                            TRACE(TRACE2_DISK, "DISK: reading#1 " << fileName << " at (" << std::dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " got: " << std::dec << actualRead);
                            if (actualRead < 0) {
                                ret = REDO_ERROR_READ;
                                break;
                            }

                            if (actualRead > 0 && fileCopyDes != -1 && (oracleAnalyzer->redoVerifyDelayUs == 0 || group == 0)) {
                                int64_t bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead, bufferEnd);
                                if (bytesWritten != actualRead) {
                                    ERROR("writing file: " << fileNameWrite << " - " << strerror(errno));
                                    ret = REDO_ERROR_WRITE;
                                    break;
                                }
                            }

                            typeBLK maxNumBlock = actualRead / blockSize;
                            typeBLK bufferScanBlock = bufferScan / blockSize;
                            uint64_t goodBlocks = 0;
                            uint64_t tmpRet = REDO_OK;

                            //check which blocks are good
                            for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                                tmpRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + numBlock * blockSize, bufferScanBlock + numBlock,
                                        false, oracleAnalyzer->redoVerifyDelayUs == 0 || group == 0);
                                TRACE(TRACE2_DISK, "DISK: block: " << std::dec << (bufferScanBlock + numBlock) << " check: " << tmpRet);

                                if (tmpRet != REDO_OK)
                                    break;
                                ++goodBlocks;
                            }

                            //partial online redo log file
                            if (goodBlocks == 0 && group == 0) {
                                if (nextScnHeader != ZERO_SCN) {
                                    ret = REDO_FINISHED;
                                    oracleAnalyzer->nextScn = nextScnHeader;
                                } else {
                                    WARNING("end of online redo log file at position " << std::dec << bufferScan);
                                    ret = REDO_STOPPED;
                                }
                                break;
                            }

                            //treat bad blocks as empty
                            if (tmpRet == REDO_ERROR_CRC && oracleAnalyzer->redoVerifyDelayUs > 0 && group != 0)
                                tmpRet = REDO_EMPTY;

                            if (goodBlocks == 0 && tmpRet != REDO_OK && (tmpRet != REDO_EMPTY || group == 0)) {
                                ret = tmpRet;
                                break;
                            }

                            //check for log switch
                            if (goodBlocks == 0 && tmpRet == REDO_EMPTY) {
                                tmpRet = reloadHeader();
                                if (tmpRet != REDO_OK) {
                                    ret = tmpRet;
                                    break;
                                }
                                reachedZero = true;
                            } else {
                                readBlocks = true;
                                reachedZero = false;
                            }

                            lastRead = goodBlocks * blockSize;
                            lastReadTime = getTime();
                            if (goodBlocks > 0) {
                                if (oracleAnalyzer->redoVerifyDelayUs > 0 && group != 0) {
                                    bufferScan += goodBlocks * blockSize;

                                    for (uint64_t numBlock = 0; numBlock < goodBlocks; ++numBlock) {
                                        time_t* readTimeP = (time_t*) (redoBufferList[redoBufferNum] + redoBufferPos + numBlock * blockSize);
                                        *readTimeP = lastReadTime;
                                    }
                                } else {
                                    std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                                    bufferEnd += goodBlocks * blockSize;
                                    bufferScan = bufferEnd;
                                    oracleAnalyzer->analyzerCond.notify_all();
                                }
                            }

                            //batch mode with partial online redo log file
                            if (tmpRet == REDO_ERROR_SEQUENCE && group == 0) {
                                if (nextScnHeader != ZERO_SCN) {
                                    ret = REDO_FINISHED;
                                    oracleAnalyzer->nextScn = nextScnHeader;
                                } else {
                                    WARNING("end of online redo log file at position " << std::dec << bufferScan);
                                    ret = REDO_STOPPED;
                                }
                                break;
                            }
                        }

                        if (numBlocksHeader != ZERO_BLK && bufferEnd == ((uint64_t)numBlocksHeader) * blockSize) {
                            if (nextScnHeader != ZERO_SCN) {
                                ret = REDO_FINISHED;
                                oracleAnalyzer->nextScn = nextScnHeader;
                            } else {
                                WARNING("end of online redo log file at position " << std::dec << bufferScan);
                                ret = REDO_STOPPED;
                            }
                            break;
                        }

                        //sleep some time
                        if (!readBlocks) {
                            if (readTime == 0) {
                                usleep(oracleAnalyzer->redoReadSleepUs);
                            } else {
                                clock_t nowTime = getTime();
                                if (readTime > nowTime) {
                                    if (oracleAnalyzer->redoReadSleepUs < readTime - nowTime)
                                        usleep(oracleAnalyzer->redoReadSleepUs);
                                    else
                                        usleep(readTime - nowTime);
                                }
                            }
                        }
                    }

                    {
                        std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                        status = READER_STATUS_SLEEPING;
                        oracleAnalyzer->analyzerCond.notify_all();
                    }
                }
            }
        } catch (RuntimeException& ex) {
        }

        redoClose();
        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }

        TRACE(TRACE2_THREADS, "THREADS: READER (" << std::hex << std::this_thread::get_id() << ") STOP");
        return 0;
    }

    void Reader::bufferAllocate(uint64_t num) {
        if (redoBufferList[num] == nullptr) {
            redoBufferList[num] = oracleAnalyzer->getMemoryChunk("disk read buffer", false);
            if (redoBufferList[num] == nullptr || buffersFree == 0) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << MEMORY_CHUNK_SIZE << " bytes memory (for: read buffer)");
            }

            {
                std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                --buffersFree;
                if (oracleAnalyzer->readBufferMax - buffersFree > buffersMaxUsed)
                    buffersMaxUsed = oracleAnalyzer->readBufferMax - buffersFree;
            }
        }
    }

    void Reader::bufferFree(uint64_t num) {
        if (redoBufferList[num] != nullptr) {
            oracleAnalyzer->freeMemoryChunk("disk read buffer", redoBufferList[num], false);
            redoBufferList[num] = nullptr;
            {
                std::unique_lock<std::mutex> lck(oracleAnalyzer->mtx);
                ++buffersFree;
            }
        }
    }
}
