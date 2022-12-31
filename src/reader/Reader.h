/* Header for Reader class
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
#include <vector>

#include "../common/Thread.h"
#include "../common/types.h"
#include "../common/typeTime.h"

#ifndef READER_H_
#define READER_H_

#define READER_STATUS_SLEEPING  0
#define READER_STATUS_CHECK     1
#define READER_STATUS_UPDATE    2
#define READER_STATUS_READ      3

#define REDO_END                0x0008
#define REDO_ASYNC              0x0100
#define REDO_NODATALOSS         0x0200
#define REDO_RESYNC             0x0800
#define REDO_CLOSEDTHREAD       0x1000
#define REDO_MAXPERFORMANCE     0x2000

#define REDO_OK                 0
#define REDO_OVERWRITTEN        1
#define REDO_FINISHED           2
#define REDO_STOPPED            3
#define REDO_SHUTDOWN           4
#define REDO_EMPTY              5
#define REDO_ERROR_READ         6
#define REDO_ERROR_WRITE        7
#define REDO_ERROR_SEQUENCE     8
#define REDO_ERROR_CRC          9
#define REDO_ERROR_BLOCK       10
#define REDO_ERROR_BAD_DATA    11
#define REDO_ERROR             12

#define REDO_PAGE_SIZE_MAX      4096
#define REDO_BAD_CDC_MAX_CNT    20
#define REDO_READ_VERIFY_MAX_BLOCKS (MEMORY_CHUNK_SIZE/blockSize)

namespace OpenLogReplicator {
    class Reader : public Thread {
    protected:
        Ctx* ctx;
        std::string database;
        int fileCopyDes;
        uint64_t fileSize;
        typeSeq fileCopySequence;
        bool hintDisplayed;
        bool configuredBlockSum;
        bool readBlocks;
        bool reachedZero;
        std::string fileNameWrite;
        int64_t group;
        typeSeq sequence;
        typeBlk numBlocksHeader;
        typeResetlogs resetlogs;
        typeActivation activation;
        uint8_t* headerBuffer;
        uint32_t compatVsn;
        typeTime firstTimeHeader;
        typeScn firstScn;
        typeScn firstScnHeader;
        typeScn nextScn;
        typeScn nextScnHeader;
        uint64_t blockSize;
        uint64_t sumRead;
        uint64_t sumTime;
        uint64_t bufferScan;
        uint64_t lastRead;
        time_t lastReadTime;
        time_t readTime;
        time_t loopTime;

        std::mutex mtx;
        std::atomic<uint64_t> bufferStart;
        std::atomic<uint64_t> bufferEnd;
        std::atomic<uint64_t> status;
        std::atomic<uint64_t> ret;
        std::condition_variable condBufferFull;
        std::condition_variable condReaderSleeping;
        std::condition_variable condParserSleeping;

        virtual void redoClose() = 0;
        virtual uint64_t redoOpen() = 0;
        virtual int64_t redoRead(uint8_t* buf, uint64_t offset, uint64_t size) = 0;
        virtual uint64_t readSize(uint64_t lastRead);
        virtual uint64_t reloadHeaderRead();
        uint64_t checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint);
        uint64_t reloadHeader();
        bool read1();
        bool read2();
        void mainLoop();

    public:
        const static char* REDO_CODE[13];
        uint8_t** redoBufferList;
        std::vector<std::string> paths;
        std::string fileName;

        Reader(Ctx* newCtx, const std::string newAlias, const std::string& newDatabase, int64_t newGroup, bool newConfiguredBlockSum);
        ~Reader() override;

        void initialize();
        void wakeUp() override;
        void run() override;
        void bufferAllocate(uint64_t num);
        void bufferFree(uint64_t num);
        typeSum calcChSum(uint8_t* buffer, uint64_t size) const;
        void printHeaderInfo(std::ostringstream& ss, const std::string& path) const;
        [[nodiscard]] uint64_t getBlockSize();
        [[nodiscard]] uint64_t getBufferStart();
        [[nodiscard]] uint64_t getBufferEnd();
        [[nodiscard]] uint64_t getRet();
        [[nodiscard]] typeScn getFirstScn();
        [[nodiscard]] typeScn getFirstScnHeader();
        [[nodiscard]] typeScn getNextScn();
        [[nodiscard]] typeBlk getNumBlocks();
        [[nodiscard]] int64_t getGroup();
        [[nodiscard]] typeSeq getSequence();
        [[nodiscard]] typeResetlogs getResetlogs();
        [[nodiscard]] typeActivation getActivation();
        [[nodiscard]] uint64_t getSumRead();
        [[nodiscard]] uint64_t getSumTime();

        void setRet(uint64_t newRet);
        void setBufferStartEnd(uint64_t newBufferStart, uint64_t newBufferEnd);
        bool checkRedoLog();
        bool updateRedoLog();
        void setStatusRead();
        void confirmReadData(uint64_t confirmedBufferStart);
        [[nodiscard]] bool checkFinished(uint64_t confirmedBufferStart);
    };
}

#endif
