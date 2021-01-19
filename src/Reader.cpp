/* Base class for process which is reading from redo log files
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

#include <thread>
#include <sys/time.h>
#include <unistd.h>

#include "OracleAnalyzer.h"
#include "Reader.h"
#include "RuntimeException.h"

using namespace std;

namespace OpenLogReplicator {

    Reader::Reader(const char *alias, OracleAnalyzer *oracleAnalyzer, int64_t group) :
        Thread(alias),
        oracleAnalyzer(oracleAnalyzer),
        hintDisplayed(false),
        redoBuffer(nullptr),
        headerBuffer(new uint8_t[REDO_PAGE_SIZE_MAX * 2]),
        group(group),
        sequence(0),
        blockSize(0),
        numBlocksHeader(0xFFFFFFFF),
        numBlocks(0),
        firstScn(ZERO_SCN),
        nextScn(ZERO_SCN),
        compatVsn(0),
        resetlogsRead(0),
        activationRead(0),
        firstScnHeader(0),
        nextScnHeader(ZERO_SCN),
        fileSize(0),
        status(READER_STATUS_SLEEPING),
        bufferStart(0),
        bufferEnd(0) {
        redoBuffer = oracleAnalyzer->getMemoryChunk("DISK", false);
        if (headerBuffer == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << (REDO_PAGE_SIZE_MAX * 2) << " bytes memory (for: read buffer)");
        }
    }

    Reader::~Reader() {
        if (redoBuffer != nullptr) {
            oracleAnalyzer->freeMemoryChunk("DISK", redoBuffer, false);
            redoBuffer = nullptr;
        }

        if (headerBuffer != nullptr) {
            delete[] headerBuffer;
            headerBuffer = nullptr;
        }
    }

    uint64_t Reader::checkBlockHeader(uint8_t *buffer, typeblk blockNumber, bool checkSum) {
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_EMPTY;

        if ((blockSize == 512 && buffer[1] != 0x22) ||
                (blockSize == 1024 && buffer[1] != 0x22) ||
                (blockSize == 4096 && buffer[1] != 0x82)) {
            ERROR("unsupported block size: " << dec << blockSize << ", magic field[1]: [0x" <<
                    setfill('0') << setw(2) << hex << (uint64_t)buffer[1] << "]");
            return REDO_ERROR;
        }

        typeblk blockNumberHeader = oracleAnalyzer->read32(buffer + 4);
        typeSEQ sequenceHeader = oracleAnalyzer->read32(buffer + 8);

        if (sequence == 0 || status == READER_STATUS_UPDATE) {
            sequence = sequenceHeader;
        } else {
            if (group == 0) {
                if (sequence != sequenceHeader)
                    return REDO_ERROR;
            } else {
                if (sequence > sequenceHeader)
                    return REDO_EMPTY;
                if (sequence < sequenceHeader)
                    return REDO_OVERWRITTEN;
            }
        }

        if (blockNumberHeader != blockNumber) {
            ERROR("header bad block number for " << dec << blockNumber << ", found: " << blockNumberHeader);
            return REDO_ERROR;
        }

        if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_BLOCK_SUM) == 0) {
            typesum chSum = oracleAnalyzer->read16(buffer + 14);
            typesum chSum2 = calcChSum(buffer, blockSize);
            if (chSum != chSum2) {
                WARNING("header sum for block number for block " << dec << blockNumber <<
                        ", should be: 0x" << setfill('0') << setw(4) << hex << chSum <<
                        ", calculated: 0x" << setfill('0') << setw(4) << hex << chSum2);
                if (!hintDisplayed) {
                    if (oracleAnalyzer->dbBlockChecksum.compare("OFF") == 0 || oracleAnalyzer->dbBlockChecksum.compare("FALSE") == 0) {
                        WARNING("HINT please set DB_BLOCK_CHECKSUM = TYPICAL on the database"
                                " or turn off consistency checking in OpenLogReplicator setting parameter disable-checks: "
                                << dec << DISABLE_CHECK_BLOCK_SUM << " for the reader");
                    }
                    hintDisplayed = true;
                }
                return REDO_BAD_CRC;
            }
        }

        return REDO_OK;
    }

    uint64_t Reader::readSize(uint64_t lastRead) {
        if (lastRead < blockSize)
            return blockSize;

        lastRead *= 2;
        if (lastRead > DISK_BUFFER_SIZE / 8)
            lastRead = DISK_BUFFER_SIZE / 8;

        return lastRead;
    }

    uint64_t Reader::reloadHeaderRead(void) {
        int64_t bytes = redoRead(headerBuffer, 0, blockSize > 0 ? blockSize * 2: REDO_PAGE_SIZE_MAX * 2);
        if (bytes < 512) {
            ERROR("unable to read file " << pathMapped);
            return REDO_ERROR;
        }

        //check file header
        if (headerBuffer[0] != 0) {
            ERROR("block header bad magic field[0]: [0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[0] << "]");
            return REDO_ERROR;
        }

        if (headerBuffer[28] == 0x7A && headerBuffer[29] == 0x7B && headerBuffer[30] == 0x7C && headerBuffer[31] == 0x7D) {
            if (!oracleAnalyzer->isBigEndian) {
                INFO("File: " << pathMapped << " has BIG ENDIAN data, changing configuration");
                oracleAnalyzer->setBigEndian();
            }
        } else
        if (headerBuffer[28] != 0x7D || headerBuffer[29] != 0x7C || headerBuffer[30] != 0x7B || headerBuffer[31] != 0x7A ||
                oracleAnalyzer->isBigEndian) {
            ERROR("block header bad magic fields[28-31]: [0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[28] <<
                    ", 0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[29] <<
                    ", 0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[30] <<
                    ", 0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[31] << "]");
            return REDO_ERROR;
        }

        blockSize = oracleAnalyzer->read32(headerBuffer + 20);
        if ((blockSize == 512 && headerBuffer[1] != 0x22) ||
                (blockSize == 1024 && headerBuffer[1] != 0x22) ||
                (blockSize == 4096 && headerBuffer[1] != 0x82)) {
            ERROR("unsupported block size: " << blockSize << ", magic field[1]: [0x" <<
                    setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[1] << "]");
            return REDO_ERROR;
        }

        if (bytes < blockSize * 2) {
            ERROR("unable read file " << pathMapped);
            return REDO_ERROR;
        }

        //check first block
        if (bytes < ((int64_t)blockSize * 2)) {
            ERROR("unable to read redo header for " << pathMapped);
            return REDO_ERROR;
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

        if ((compatVsn >= 0x0B200000 && compatVsn <= 0x0B200400) //11.2.0.0 - 11.2.0.4
            || (compatVsn >= 0x0C100000 && compatVsn <= 0x0C100200) //12.1.0.0 - 12.1.0.2
            || (compatVsn >= 0x0C200000 && compatVsn <= 0x0C200100) //12.2.0.0 - 12.2.0.1
            || (compatVsn >= 0x12000000 && compatVsn <= 0x120C0000) //18.0.0.0 - 18.12.0.0
            || (compatVsn >= 0x13000000 && compatVsn <= 0x13090000)) //19.0.0.0 - 19.9.0.0
            version = compatVsn;

        if (oracleAnalyzer->version == 0) {
            oracleAnalyzer->version = version;
            INFO("found redo log version: 0x" << setfill('0') << setw(8) << hex << compatVsn);
        }

        if (version == 0 || version != oracleAnalyzer->version) {
            ERROR("unsupported database version: 0x" << setfill('0') << setw(8) << hex << compatVsn);
            return REDO_ERROR;
        }

        activationRead = oracleAnalyzer->read32(headerBuffer + blockSize + 52);
        numBlocksHeader = oracleAnalyzer->read32(headerBuffer + blockSize + 156);
        resetlogsRead = oracleAnalyzer->read32(headerBuffer + blockSize + 160);
        firstScnHeader = oracleAnalyzer->readSCN(headerBuffer + blockSize + 180);
        nextScnHeader = oracleAnalyzer->readSCN(headerBuffer + blockSize + 192);

        uint64_t badBlockCrcCount = 0;
        ret = checkBlockHeader(headerBuffer + blockSize, 1, true);
        TRACE(TRACE2_DISK, "DISK: block: 1 check: " << ret);

        while (ret == REDO_BAD_CRC) {
            WARNING("CRC error during header check");
            ++badBlockCrcCount;
            if (badBlockCrcCount == REDO_BAD_CDC_MAX_CNT)
                return REDO_ERROR;

            usleep(oracleAnalyzer->redoReadSleep);
            ret = checkBlockHeader(headerBuffer + blockSize, 1, true);
            TRACE(TRACE2_DISK, "DISK: block: 1 check: " << ret);
        }

        if (ret != REDO_OK)
            return ret;

        if (oracleAnalyzer->resetlogs == 0 && (oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) != 0)
            oracleAnalyzer->resetlogs = resetlogsRead;

        if (resetlogsRead != oracleAnalyzer->resetlogs) {
            if (group == 0) {
                ERROR("resetlogs id (" << dec << resetlogsRead << ") for archived redo log does not match database information (" <<
                        oracleAnalyzer->resetlogs << "): " << pathMapped);
            } else {
                ERROR("resetlogs id (" << dec << resetlogsRead << ") for online redo log does not match database information (" <<
                        oracleAnalyzer->resetlogs << "): " << pathMapped);
            } return REDO_ERROR;
        }

        if (oracleAnalyzer->activation == 0 && (oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) != 0)
            oracleAnalyzer->activation = activationRead;

        if (activationRead != 0 && activationRead != oracleAnalyzer->activation) {
            if (group == 0) {
                ERROR("activation id (" << dec << activationRead << ") for archived redo log does not match database information (" <<
                        oracleAnalyzer->activation << "): " << pathMapped);
            } else {
                ERROR("activation id (" << dec << activationRead << ") for online redo log does not match database information (" <<
                        oracleAnalyzer->activation << "): " << pathMapped);
            }
            return REDO_ERROR;
        }

        if (firstScn == ZERO_SCN || status == READER_STATUS_UPDATE) {
            firstScn = firstScnHeader;
            nextScn = nextScnHeader;
        } else {
            if (firstScnHeader != firstScn) {
                ERROR("first SCN (" << dec << firstScnHeader << ") for redo log does not match database information (" <<
                        firstScn << "): " << pathMapped);
                return REDO_ERROR;
            }
        }

        //updating nextScn if changed
        if (nextScn == ZERO_SCN && nextScnHeader != ZERO_SCN) {
            TRACE(TRACE_FULL, "updating next SCN to: " << dec << nextScnHeader);
            nextScn = nextScnHeader;
        } else
        if (nextScn != ZERO_SCN && nextScnHeader != ZERO_SCN && nextScn != nextScnHeader) {
            ERROR("next SCN (" << nextScn << ") does not match database information (" << nextScnHeader << "): " << pathMapped);
            return REDO_ERROR;
        }

        return ret;
    }

    time_t Reader::getTime(void) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return (1000000 * tv.tv_sec) + tv.tv_usec;
    }

    typesum Reader::calcChSum(uint8_t *buffer, uint64_t size) const {
        typesum oldChSum = oracleAnalyzer->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint64_t i = 0; i < size / 8; ++i, buffer += 8)
            sum ^= *((uint64_t*)buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }

    void *Reader::run(void) {
        TRACE(TRACE2_THREADS, "READER (" << hex << this_thread::get_id() << ") START");

        while (!shutdown) {
            {
                unique_lock<mutex> lck(oracleAnalyzer->mtx);
                oracleAnalyzer->analyzerCond.notify_all();

                if (status == READER_STATUS_SLEEPING && !shutdown) {
                    oracleAnalyzer->sleepingCond.wait(lck);
                } else if (status == READER_STATUS_READ && bufferStart + DISK_BUFFER_SIZE == bufferEnd && !shutdown) {
                    oracleAnalyzer->readerCond.wait(lck);
                }
            }

            if (shutdown)
                break;

            if (status == READER_STATUS_CHECK) {
                TRACE(TRACE2_FILE, "trying to open: " << pathMapped);
                redoClose();
                uint64_t tmpRet = redoOpen();
                {
                    unique_lock<mutex> lck(oracleAnalyzer->mtx);
                    ret = tmpRet;
                    status = READER_STATUS_SLEEPING;
                    oracleAnalyzer->analyzerCond.notify_all();
                }
                continue;

            } else if (status == READER_STATUS_UPDATE) {
                uint64_t tmpRet = reloadHeader();
                if (tmpRet == REDO_OK) {
                    bufferStart = blockSize * 2;
                    bufferEnd = blockSize * 2;
                }

                {
                    unique_lock<mutex> lck(oracleAnalyzer->mtx);
                    ret = tmpRet;
                    status = READER_STATUS_SLEEPING;
                    oracleAnalyzer->analyzerCond.notify_all();
                }
            } else if (status == READER_STATUS_READ) {
                TRACE(TRACE2_DISK, "reading " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << ") at size: " << fileSize);
                uint64_t lastRead = blockSize;
                clock_t lastReadTime = 0, read1Time = 0, read2Time = 0;
                uint64_t bufferScan = bufferEnd;
                bool reachedZero = false;

                while (!shutdown && status == READER_STATUS_READ) {
                    clock_t loopTime = getTime();
                    read1Time = 0;
                    read2Time = 0;

                    //buffer full?
                    {
                        unique_lock<mutex> lck(oracleAnalyzer->mtx);
                        if (bufferStart + DISK_BUFFER_SIZE <= bufferEnd) {
                            oracleAnalyzer->readerCond.wait(lck);
                            continue;
                        }
                    }

                    if (bufferEnd == fileSize) {
                        ret = REDO_FINISHED;
                        break;
                    }

                    //#2 read
                    if (bufferEnd < bufferScan) {
                        uint64_t maxNumBlock = (bufferScan - bufferEnd) / blockSize;
                        uint64_t goodBlocks = 0;
                        if (maxNumBlock > REDO_READ_VERIFY_MAX_BLOCKS)
                            maxNumBlock = REDO_READ_VERIFY_MAX_BLOCKS;

                        for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                            time_t *readTime = (time_t*)(redoBuffer + ((bufferEnd + numBlock * blockSize) % DISK_BUFFER_SIZE));
                            if (*readTime + oracleAnalyzer->redoVerifyDelay < loopTime)
                                ++goodBlocks;
                            else {
                                read2Time = *readTime + oracleAnalyzer->redoVerifyDelay;
                                break;
                            }
                        }

                        if (goodBlocks > 0) {
                            uint64_t toRead = readSize(goodBlocks * blockSize);
                            if (toRead > goodBlocks * blockSize)
                                toRead = goodBlocks * blockSize;

                            uint64_t bufferPos = bufferEnd % DISK_BUFFER_SIZE;
                            if (bufferPos + toRead > DISK_BUFFER_SIZE)
                                toRead = DISK_BUFFER_SIZE - bufferPos;

                            if (toRead == 0) {
                                WARNING("zero bytes to read #2, start: " << dec << bufferStart << ", end: " << bufferEnd << ", scan: " << bufferScan);
                                ret = REDO_ERROR;
                                break;
                            }

                            TRACE(TRACE2_DISK, "reading#2 " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " bytes: " << dec << toRead);
                            int64_t actualRead = redoRead(redoBuffer + bufferPos, bufferEnd, toRead);

                            TRACE(TRACE2_DISK, "reading#2 " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " got: " << dec << actualRead);
                            if (actualRead < 0) {
                                ret = REDO_ERROR;
                                break;
                            }

                            uint64_t tmpRet = REDO_OK;
                            typeblk maxNumBlock = actualRead / blockSize;
                            typeblk bufferEndBlock = bufferEnd / blockSize;

                            //check which blocks are good
                            for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                                tmpRet = checkBlockHeader(redoBuffer + bufferPos + numBlock * blockSize, bufferEndBlock + numBlock, false);
                                TRACE(TRACE2_DISK, "block: " << dec << (bufferEndBlock + numBlock) << " check: " << tmpRet);

                                if (tmpRet != REDO_OK)
                                    break;
                                ++goodBlocks;
                            }

                            //verify header for online redo logs after every successfull read
                            if (tmpRet == REDO_OK && group > 0)
                                tmpRet = reloadHeader();

                            if (tmpRet != REDO_OK) {
                                ret = tmpRet;
                                break;
                            }

                            {
                                unique_lock<mutex> lck(oracleAnalyzer->mtx);
                                bufferEnd += actualRead;
                                oracleAnalyzer->analyzerCond.notify_all();
                            }
                        }
                    }

                    //#1 read
                    if (bufferScan < fileSize && bufferStart + DISK_BUFFER_SIZE > bufferScan
                            && (!reachedZero || lastReadTime + oracleAnalyzer->redoReadSleep < loopTime)) {
                        uint64_t toRead = readSize(lastRead);
                        if (bufferScan + toRead - bufferStart > DISK_BUFFER_SIZE)
                            toRead = DISK_BUFFER_SIZE - bufferScan + bufferStart;

                        if (bufferScan + toRead > fileSize)
                            toRead = fileSize - bufferScan;

                        uint64_t bufferPos = bufferScan % DISK_BUFFER_SIZE;
                        if (bufferPos + toRead > DISK_BUFFER_SIZE)
                            toRead = DISK_BUFFER_SIZE - bufferPos;

                        if (toRead == 0) {
                            WARNING("zero bytes to read #1, start: " << dec << bufferStart << ", end: " << bufferEnd << ", scan: " << bufferScan);
                            ret = REDO_ERROR;
                            break;
                        }

                        TRACE(TRACE2_DISK, "reading#1 " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " bytes: " << dec << toRead);
                        int64_t actualRead = redoRead(redoBuffer + bufferPos, bufferScan, toRead);

                        TRACE(TRACE2_DISK, "reading#1 " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << "/" << bufferScan << ")" << " got: " << dec << actualRead);
                        if (actualRead < 0) {
                            ret = REDO_ERROR;
                            break;
                        }

                        typeblk maxNumBlock = actualRead / blockSize;
                        typeblk bufferScanBlock = bufferScan / blockSize;
                        uint64_t goodBlocks = 0;
                        uint64_t tmpRet = REDO_OK;

                        //check which blocks are good
                        for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                            tmpRet = checkBlockHeader(redoBuffer + bufferPos + numBlock * blockSize, bufferScanBlock + numBlock, false);
                            TRACE(TRACE2_DISK, "block: " << dec << (bufferScanBlock + numBlock) << " check: " << tmpRet);

                            if (tmpRet != REDO_OK)
                                break;
                            ++goodBlocks;
                        }

                        //treat bad blocks as empty
                        if (tmpRet == REDO_BAD_CRC && oracleAnalyzer->redoVerifyDelay > 0 && group != 0)
                            tmpRet = REDO_EMPTY;

                        if (tmpRet != REDO_OK && (tmpRet != REDO_EMPTY || group == 0)) {
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
                        } else
                            reachedZero = false;

                        lastRead = goodBlocks * blockSize;
                        lastReadTime = getTime();
                        if (goodBlocks > 0) {
                            if (oracleAnalyzer->redoVerifyDelay > 0 && group != 0) {
                                bufferScan += goodBlocks * blockSize;

                                for (uint64_t numBlock = 0; numBlock < goodBlocks; ++numBlock) {
                                    time_t *readTime = (time_t*)(redoBuffer + bufferPos + numBlock * blockSize);
                                    *readTime = lastReadTime;
                                }
                            } else {
                                unique_lock<mutex> lck(oracleAnalyzer->mtx);
                                bufferEnd += goodBlocks * blockSize;
                                bufferScan = bufferEnd;
                                oracleAnalyzer->analyzerCond.notify_all();
                            }
                        } else {
                            tmpRet = reloadHeader();

                            if (tmpRet != REDO_OK) {
                                ret = tmpRet;
                                break;
                            }
                            read1Time = lastReadTime + oracleAnalyzer->redoReadSleep;
                        }
                    }

                    if (numBlocksHeader != 0xFFFFFFFF && bufferEnd == ((uint64_t)numBlocksHeader) * blockSize) {
                        ret = REDO_FINISHED;
                        break;
                    }

                    //sleep some time
                    if (read1Time > 0) {
                        clock_t nowTime = getTime();
                        if (read1Time < read2Time || read2Time == 0) {
                            if (read1Time > nowTime) {
                                usleep(read1Time - nowTime);
                            }
                        } else {
                            if (read2Time > nowTime) {
                                usleep(read2Time - nowTime);
                            }
                        }
                    }
                }

                {
                    unique_lock<mutex> lck(oracleAnalyzer->mtx);
                    status = READER_STATUS_SLEEPING;
                    oracleAnalyzer->analyzerCond.notify_all();
                }
            }
        }

        redoClose();
        TRACE(TRACE2_THREADS, "READER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }
}
