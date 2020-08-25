/* Base class for process which is reading from redo log
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
#include <unistd.h>

#include "OracleAnalyser.h"
#include "Reader.h"
#include "RuntimeException.h"

using namespace std;

namespace OpenLogReplicator {

    Reader::Reader(const string alias, OracleAnalyser *oracleAnalyser, int64_t group, bool singleBlockRead) :
        Thread(alias),
        oracleAnalyser(oracleAnalyser),
        singleBlockRead(singleBlockRead),
        redoBuffer(nullptr),
        headerBuffer(new uint8_t[REDO_PAGE_SIZE_MAX * 2]),
        group(group),
        sequence(0),
        blockSize(0),
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
        redoBuffer = oracleAnalyser->getMemoryChunk("DISK", false);
        if (headerBuffer == nullptr) {
            RUNTIME_FAIL("could not allocate " << dec << (REDO_PAGE_SIZE_MAX * 2) << " bytes memory for (reason: read buffer)");
        }
    }

    Reader::~Reader() {
        if (redoBuffer != nullptr) {
            oracleAnalyser->freeMemoryChunk("DISK", redoBuffer, false);
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

        typeblk blockNumberHeader = oracleAnalyser->read32(buffer + 4);
        typeseq sequenceHeader = oracleAnalyser->read32(buffer + 8);

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

        if ((oracleAnalyser->flags & REDO_FLAGS_BLOCK_CHECK_SUM) != 0 &&
                (checkSum || group == 0 || (oracleAnalyser->flags & REDO_FLAGS_DISABLE_READ_VERIFICATION) != 0)) {
            typesum chSum = oracleAnalyser->read16(buffer + 14);
            typesum chSum2 = calcChSum(buffer, blockSize);
            if (chSum != chSum2) {
                ERROR("header sum for block number for block " << dec << blockNumber <<
                        ", should be: 0x" << setfill('0') << setw(4) << hex << chSum <<
                        ", calculated: 0x" << setfill('0') << setw(4) << hex << chSum2);
                return REDO_ERROR;
            }
        }

        return REDO_OK;
    }

    uint64_t Reader::reloadHeader(void) {
        int64_t bytes = 0;
        if (singleBlockRead) {
            bytes = redoRead(headerBuffer + blockSize, blockSize, blockSize);
            if (bytes != blockSize) {
                ERROR("unable to read file " << pathMapped);
                return REDO_ERROR;
            }
        } else {
            bytes = redoRead(headerBuffer, 0, blockSize > 0 ? blockSize * 2: REDO_PAGE_SIZE_MAX * 2);
            if (bytes < 512) {
                ERROR("unable to read file " << pathMapped);
                return REDO_ERROR;
            }

            //check file header
            if (headerBuffer[0] != 0) {
                ERROR("block header bad magic field[0]: [0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[0] << "]");
                return REDO_ERROR;
            }

            if ((oracleAnalyser->isBigEndian && (headerBuffer[28] != 0x7A || headerBuffer[29] != 0x7B || headerBuffer[30] != 0x7C || headerBuffer[31] != 0x7D))
                    || (!oracleAnalyser->isBigEndian && (headerBuffer[28] != 0x7D || headerBuffer[29] != 0x7C || headerBuffer[30] != 0x7B || headerBuffer[31] != 0x7A))) {
                ERROR("block header bad magic fields[28-31]: [0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[28] <<
                        ", 0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[29] <<
                        ", 0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[30] <<
                        ", 0x" << setfill('0') << setw(2) << hex << (uint64_t)headerBuffer[31] << "]");
                return REDO_ERROR;
            }

            blockSize = oracleAnalyser->read32(headerBuffer + 20);
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

            numBlocks = oracleAnalyser->read32(headerBuffer + 24);
        }

        uint64_t version = 0;
        compatVsn = oracleAnalyser->read32(headerBuffer + blockSize + 20);
        if (compatVsn == 0x0B200000) //11.2.0.0
            version = 0x11200;
        else
        if (compatVsn == 0x0B200100) //11.2.0.1
            version = 0x11201;
        else
        if (compatVsn == 0x0B200200) //11.2.0.2
            version = 0x11202;
        else
        if (compatVsn == 0x0B200300) //11.2.0.3
            version = 0x11203;
        else
        if (compatVsn == 0x0B200400) //11.2.0.4
            version = 0x11204;
        else
        if (compatVsn == 0x0C100000) //12.1.0.0
            version = 0x12100;
        else
        if (compatVsn == 0x0C100100) //12.1.0.1
            version = 0x12101;
        else
        if (compatVsn == 0x0C100200) //12.1.0.2
            version = 0x12102;
        else
        if (compatVsn == 0x0C200000) //12.2.0.0
            version = 0x12200;
        else
        if (compatVsn == 0x0C200100) //12.2.0.1
            version = 0x12201;
        else
        if (compatVsn == 0x12000000) //18.0.0.0
            version = 0x18000;
        else
        if (compatVsn == 0x12030000) //18.3.0.0
            version = 0x18300;
        else
        if (compatVsn == 0x12040000) //18.4.0.0
            version = 0x18400;
        else
        if (compatVsn == 0x12050000) //18.5.0.0
            version = 0x18500;
        else
        if (compatVsn == 0x12060000) //18.6.0.0
            version = 0x18600;
        else
        if (compatVsn == 0x12070000) //18.7.0.0
            version = 0x18700;
        else
        if (compatVsn == 0x12080000) //18.8.0.0
            version = 0x18800;
        else
        if (compatVsn == 0x12090000) //18.9.0.0
            version = 0x18900;
        else
        if (compatVsn == 0x120A0000) //18.10.0.0
            version = 0x18A00;
        else
        if (compatVsn == 0x120B0000) //18.11.0.0
            version = 0x18B00;
        else
        if (compatVsn == 0x13000000) //19.0.0.0
            version = 0x19000;
        else
        if (compatVsn == 0x13030000) //19.3.0.0
            version = 0x19300;
        else
        if (compatVsn == 0x13040000) //19.4.0.0
            version = 0x19400;
        else
        if (compatVsn == 0x13050000) //19.5.0.0
            version = 0x19500;
        else
        if (compatVsn == 0x13060000) //19.6.0.0
            version = 0x19600;
        else
        if (compatVsn == 0x13070000) //19.7.0.0
            version = 0x19700;
        else
        if (compatVsn == 0x13080000) //19.8.0.0
            version = 0x19800;

        if (oracleAnalyser->version == 0)
            oracleAnalyser->version = version;

        if (version == 0 || version != oracleAnalyser->version) {
            ERROR("unsupported database version: 0x" << setfill('0') << setw(8) << hex << compatVsn);
            return REDO_ERROR;
        }

        activationRead = oracleAnalyser->read32(headerBuffer + blockSize + 52);
        resetlogsRead = oracleAnalyser->read32(headerBuffer + blockSize + 160);
        firstScnHeader = oracleAnalyser->readSCN(headerBuffer + blockSize + 180);
        nextScnHeader = oracleAnalyser->readSCN(headerBuffer + blockSize + 192);

        uint64_t ret = checkBlockHeader(headerBuffer + blockSize, 1, true);
        TRACE(TRACE2_DISK, "DISK: block: 1 check: " << ret);
        if (ret != REDO_OK)
            return ret;

        if (resetlogsRead != oracleAnalyser->resetlogs) {
            if (group == 0) {
                ERROR("resetlogs id (" << dec << resetlogsRead << ") for archived redo log does not match database information (" <<
                        oracleAnalyser->resetlogs << "): " << pathMapped);
            } else {
                ERROR("resetlogs id (" << dec << resetlogsRead << ") for online redo log does not match database information (" <<
                        oracleAnalyser->resetlogs << "): " << pathMapped);
            } return REDO_ERROR;
        }

        if (activationRead != oracleAnalyser->activation) {
            if (group == 0) {
                ERROR("activation id (" << dec << activationRead << ") for archived redo log does not match database information (" <<
                        oracleAnalyser->activation << "): " << pathMapped);
            } else {
                ERROR("activation id (" << dec << activationRead << ") for online redo log does not match database information (" <<
                        oracleAnalyser->activation << "): " << pathMapped);
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

    typesum Reader::calcChSum(uint8_t *buffer, uint64_t size) {
        typesum oldChSum = oracleAnalyser->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint64_t i = 0; i < size / 8; ++i, buffer += 8)
            sum ^= *((uint64_t*)buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }

    void *Reader::run(void) {
        uint64_t curStatus;
        TRACE(TRACE2_THREADS, "READER (" << hex << this_thread::get_id() << ") START");

        while (!shutdown) {
            {
                unique_lock<mutex> lck(oracleAnalyser->mtx);
                oracleAnalyser->analyserCond.notify_all();

                if (status == READER_STATUS_SLEEPING && !shutdown) {
                    oracleAnalyser->sleepingCond.wait(lck);
                } else if (status == READER_STATUS_READ && bufferStart + DISK_BUFFER_SIZE == bufferEnd && !shutdown) {
                    oracleAnalyser->readerCond.wait(lck);
                }
                curStatus = status;
            }

            if (shutdown)
                break;

            if (curStatus == READER_STATUS_CHECK) {
                TRACE(TRACE2_FILE, "trying to open: " << pathMapped);
                redoClose();
                uint64_t curRet = redoOpen();
                {
                    unique_lock<mutex> lck(oracleAnalyser->mtx);
                    ret = curRet;
                    status = READER_STATUS_SLEEPING;
                    oracleAnalyser->analyserCond.notify_all();
                }
                continue;

            } else if (status == READER_STATUS_UPDATE) {
                uint64_t curRet = reloadHeader();
                if (curRet == REDO_OK) {
                    bufferStart = blockSize * 2;
                    bufferEnd = blockSize * 2;
                }

                {
                    unique_lock<mutex> lck(oracleAnalyser->mtx);
                    ret = curRet;
                    status = READER_STATUS_SLEEPING;
                    oracleAnalyser->analyserCond.notify_all();
                }
            } else if (status == READER_STATUS_READ) {
                uint64_t curBufferStart = 0;
                {
                    unique_lock<mutex> lck(oracleAnalyser->mtx);
                    curBufferStart = bufferStart;
                }

                TRACE(TRACE2_DISK, "reading " << pathMapped << " at (" << dec << curBufferStart << "/" << bufferEnd << ") at size: " << fileSize);
                uint64_t lastRead = blockSize;
                while (!shutdown && status == READER_STATUS_READ && curBufferStart + DISK_BUFFER_SIZE > bufferEnd) {
                    uint64_t toRead = 0;

                    if (singleBlockRead) {
                        toRead = blockSize;
                    } else {
                        toRead = lastRead;
                        if (bufferEnd + toRead - bufferStart > DISK_BUFFER_SIZE)
                            toRead = DISK_BUFFER_SIZE - bufferEnd + bufferStart;
                    }

                    if (bufferEnd + toRead > fileSize)
                        toRead = fileSize - bufferEnd;

                    if (toRead == 0) {
                        unique_lock<mutex> lck(oracleAnalyser->mtx);
                        status = READER_STATUS_SLEEPING;
                        ret = REDO_FINISHED;
                        oracleAnalyser->analyserCond.notify_all();
                        break;
                    }

                    uint64_t bufferPos = bufferEnd % DISK_BUFFER_SIZE;
                    if (bufferPos + toRead > DISK_BUFFER_SIZE)
                        toRead = DISK_BUFFER_SIZE - bufferPos;

                    TRACE(TRACE2_DISK, "reading " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << ")" << " bytes: " << dec << toRead);
                    int64_t actualRead = redoRead(redoBuffer + bufferPos, bufferEnd, toRead);

                    TRACE(TRACE2_DISK, "reading " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << ")" << " got: " << dec << actualRead);
                    if (actualRead < 0) {
                        unique_lock<mutex> lck(oracleAnalyser->mtx);
                        status = READER_STATUS_SLEEPING;
                        ret = REDO_ERROR;
                        oracleAnalyser->analyserCond.notify_all();
                        break;
                    }

                    typeblk maxNumBlock = actualRead / blockSize;
                    typeblk bufferEndBlock = bufferEnd / blockSize;
                    uint64_t curBufferEnd = bufferEnd;

                    uint64_t goodBlocks = 0, curRet = REDO_OK;
                    bool reachedZero = false;

                    for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                        curRet = checkBlockHeader(redoBuffer + bufferPos + numBlock * blockSize, bufferEndBlock + numBlock, false);
                        TRACE(TRACE2_DISK, "block: " << dec << (bufferEndBlock + numBlock) << " check: " << curRet);

                        if (curRet == REDO_OVERWRITTEN) {
                            unique_lock<mutex> lck(oracleAnalyser->mtx);
                            status = READER_STATUS_SLEEPING;
                            ret = curRet;
                            break;
                        } else if (curRet == REDO_ERROR) {
                            unique_lock<mutex> lck(oracleAnalyser->mtx);
                            status = READER_STATUS_SLEEPING;
                            ret = curRet;
                            break;
                        } else if (curRet == REDO_EMPTY) {
                            reachedZero = true;
                            break;
                        }

                        ++goodBlocks;
                    }

                    //read verification to prevent buffer overwrite
                    if (goodBlocks > 0 && group != 0 && (oracleAnalyser->flags & REDO_FLAGS_DISABLE_READ_VERIFICATION) == 0) {
                        actualRead = redoRead(redoBuffer + bufferPos, bufferEnd, goodBlocks * blockSize);
                        reachedZero = false;

                        TRACE(TRACE2_DISK, "second reading " << pathMapped << " at (" << dec << bufferStart << "/" << bufferEnd << ")" << " got: " << dec << actualRead);

                        if (actualRead < 0) {
                            unique_lock<mutex> lck(oracleAnalyser->mtx);
                            status = READER_STATUS_SLEEPING;
                            ret = REDO_ERROR;
                            oracleAnalyser->analyserCond.notify_all();
                            break;
                        }

                        maxNumBlock = actualRead / blockSize;
                        goodBlocks = 0;
                        curRet = REDO_OK;

                        for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                            curRet = checkBlockHeader(redoBuffer + bufferPos + numBlock * blockSize, bufferEndBlock + numBlock, true);
                            TRACE(TRACE2_DISK, "block: " << dec << (bufferEndBlock + numBlock) << " check: " << curRet);

                            if (curRet == REDO_OVERWRITTEN) {
                                unique_lock<mutex> lck(oracleAnalyser->mtx);
                                status = READER_STATUS_SLEEPING;
                                ret = curRet;
                                break;
                            } else if (curRet == REDO_ERROR) {
                                unique_lock<mutex> lck(oracleAnalyser->mtx);
                                status = READER_STATUS_SLEEPING;
                                ret = curRet;
                                break;
                            } else if (curRet == REDO_EMPTY) {
                                reachedZero = true;
                                break;
                            }

                            ++goodBlocks;
                        }
                    }

                    curBufferEnd += goodBlocks * blockSize;

                    if (goodBlocks == maxNumBlock) {
                        lastRead = lastRead * 2;
                        if (lastRead > DISK_BUFFER_SIZE / 8)
                            lastRead = DISK_BUFFER_SIZE / 8;
                    } else if (goodBlocks < maxNumBlock / 4) {
                        lastRead /= 4;
                        if (lastRead < blockSize)
                            lastRead = blockSize;
                    } else if (goodBlocks < maxNumBlock / 2) {
                        lastRead /= 2;
                        if (lastRead < blockSize)
                            lastRead = blockSize;
                    }

                    //reached EOF
                    if ((reachedZero && nextScnHeader != ZERO_SCN) || curBufferEnd == fileSize) {
                        unique_lock<mutex> lck(oracleAnalyser->mtx);
                        bufferEnd = curBufferEnd;
                        status = READER_STATUS_SLEEPING;
                        ret = REDO_FINISHED;
                        oracleAnalyser->analyserCond.notify_all();
                        break;
                    }

                    //some data has been read, try to process it first
                    if (curBufferEnd > bufferEnd) {
                        unique_lock<mutex> lck(oracleAnalyser->mtx);
                        bufferEnd = curBufferEnd;
                        curBufferStart = bufferStart;
                        oracleAnalyser->analyserCond.notify_all();
                    } else {
                        //nothing new read, check if header has changed
                        usleep(oracleAnalyser->redoReadSleep);
                        curRet = reloadHeader();
                    }

                    if (curRet == REDO_OVERWRITTEN || curRet == REDO_ERROR) {
                        unique_lock<mutex> lck(oracleAnalyser->mtx);
                        status = READER_STATUS_SLEEPING;
                        ret = curRet;
                        break;
                    }
                }
            }
        }

        redoClose();
        TRACE(TRACE2_THREADS, "READER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }
}
