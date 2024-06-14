/* Header for Reader class
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

namespace OpenLogReplicator {
    class Reader : public Thread {
    protected:
        static constexpr uint64_t FLAGS_END = 0x0008;
        static constexpr uint64_t FLAGS_ASYNC = 0x0100;
        static constexpr uint64_t FLAGS_NODATALOSS = 0x0200;
        static constexpr uint64_t FLAGS_RESYNC = 0x0800;
        static constexpr uint64_t FLAGS_CLOSEDTHREAD = 0x1000;
        static constexpr uint64_t FLAGS_MAXPERFORMANCE = 0x2000;

        static constexpr uint64_t STATUS_SLEEPING = 0;
        static constexpr uint64_t STATUS_CHECK = 1;
        static constexpr uint64_t STATUS_UPDATE = 2;
        static constexpr uint64_t STATUS_READ = 3;

        static constexpr uint64_t PAGE_SIZE_MAX = 4096;
        static constexpr uint64_t BAD_CDC_MAX_CNT = 20;

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
        typeTime nextTime;
        uint64_t blockSize;
        uint64_t sumRead;
        uint64_t sumTime;
        uint64_t bufferScan;
        uint64_t lastRead;
        time_ut lastReadTime;
        time_ut readTime;
        time_ut loopTime;

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
        static constexpr uint64_t REDO_OK = 0;
        static constexpr uint64_t REDO_OVERWRITTEN = 1;
        static constexpr uint64_t REDO_FINISHED = 2;
        static constexpr uint64_t REDO_STOPPED = 3;
        static constexpr uint64_t REDO_SHUTDOWN = 4;
        static constexpr uint64_t REDO_EMPTY = 5;
        static constexpr uint64_t REDO_ERROR_READ = 6;
        static constexpr uint64_t REDO_ERROR_WRITE = 7;
        static constexpr uint64_t REDO_ERROR_SEQUENCE = 8;
        static constexpr uint64_t REDO_ERROR_CRC = 9;
        static constexpr uint64_t REDO_ERROR_BLOCK = 10;
        static constexpr uint64_t REDO_ERROR_BAD_DATA = 11;
        static constexpr uint64_t REDO_ERROR = 12;

        const static char* REDO_CODE[13];
        uint8_t** redoBufferList;
        std::vector<std::string> paths;
        std::string fileName;

        Reader(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, int64_t newGroup, bool newConfiguredBlockSum);
        ~Reader() override;

        void initialize();
        void wakeUp() override;
        void run() override;
        void bufferAllocate(uint64_t num);
        void bufferFree(uint64_t num);
        typeSum calcChSum(uint8_t* buffer, uint64_t size) const;
        void printHeaderInfo(std::ostringstream& ss, const std::string& path) const;
        [[nodiscard]] uint64_t getBlockSize() const;
        [[nodiscard]] uint64_t getBufferStart() const;
        [[nodiscard]] uint64_t getBufferEnd() const;
        [[nodiscard]] uint64_t getRet() const;
        [[nodiscard]] typeScn getFirstScn() const;
        [[nodiscard]] typeScn getFirstScnHeader() const;
        [[nodiscard]] typeScn getNextScn() const;
        [[nodiscard]] typeTime getNextTime() const;
        [[nodiscard]] typeBlk getNumBlocks() const;
        [[nodiscard]] int64_t getGroup() const;
        [[nodiscard]] typeSeq getSequence() const;
        [[nodiscard]] typeResetlogs getResetlogs() const;
        [[nodiscard]] typeActivation getActivation() const;
        [[nodiscard]] uint64_t getSumRead() const;
        [[nodiscard]] uint64_t getSumTime() const;

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
