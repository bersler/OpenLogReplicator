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
    public:
        enum class REDO_CODE {
            OK, OVERWRITTEN, FINISHED, STOPPED, SHUTDOWN, EMPTY, ERROR_READ, ERROR_WRITE, ERROR_SEQUENCE, ERROR_CRC, ERROR_BLOCK, ERROR_BAD_DATA,
            ERROR, CNT
        };

    protected:
        static constexpr uint64_t FLAGS_END{0x0008};
        static constexpr uint64_t FLAGS_ASYNC{0x0100};
        static constexpr uint64_t FLAGS_NODATALOSS{0x0200};
        static constexpr uint64_t FLAGS_RESYNC{0x0800};
        static constexpr uint64_t FLAGS_CLOSEDTHREAD{0x1000};
        static constexpr uint64_t FLAGS_MAXPERFORMANCE{0x2000};

        enum class STATUS {
            SLEEPING, CHECK, UPDATE, READ
        };

        static constexpr uint PAGE_SIZE_MAX{4096};
        static constexpr uint BAD_CDC_MAX_CNT{20};

        std::string database;
        int fileCopyDes;
        uint64_t fileSize;
        typeSeq fileCopySequence;
        bool hintDisplayed;
        bool configuredBlockSum;
        bool readBlocks;
        bool reachedZero;
        std::string fileNameWrite;
        int group;
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
        uint blockSize;
        uint64_t sumRead;
        uint64_t sumTime;
        uint64_t bufferScan;
        uint lastRead;
        time_ut lastReadTime;
        time_ut readTime;
        time_ut loopTime;

        std::mutex mtx;
        std::atomic<uint64_t> bufferStart;
        std::atomic<uint64_t> bufferEnd;
        std::atomic<STATUS> status;
        std::atomic<REDO_CODE> ret;
        std::condition_variable condBufferFull;
        std::condition_variable condReaderSleeping;
        std::condition_variable condParserSleeping;

        virtual void redoClose() = 0;
        virtual REDO_CODE redoOpen() = 0;
        virtual int redoRead(uint8_t* buf, uint64_t offset, uint size) = 0;
        virtual uint readSize(uint lastRead);
        virtual REDO_CODE reloadHeaderRead();
        REDO_CODE checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint);
        REDO_CODE reloadHeader();
        bool read1();
        bool read2();
        void mainLoop();

    public:
        const static char* REDO_MSG[static_cast<uint>(REDO_CODE::CNT)];
        uint8_t** redoBufferList;
        std::vector<std::string> paths;
        std::string fileName;

        Reader(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, int newGroup, bool newConfiguredBlockSum);
        ~Reader() override;

        void initialize();
        void wakeUp() override;
        void run() override;
        void bufferAllocate(uint num);
        void bufferFree(Thread* t, uint num);
        bool bufferIsFree();
        typeSum calcChSum(uint8_t* buffer, uint size) const;
        void printHeaderInfo(std::ostringstream& ss, const std::string& path) const;
        [[nodiscard]] uint getBlockSize() const;
        [[nodiscard]] uint64_t getBufferStart() const;
        [[nodiscard]] uint64_t getBufferEnd() const;
        [[nodiscard]] REDO_CODE getRet() const;
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

        void setRet(REDO_CODE newRet);
        void setBufferStartEnd(uint64_t newBufferStart, uint64_t newBufferEnd);
        bool checkRedoLog();
        bool updateRedoLog();
        void setStatusRead();
        void confirmReadData(uint64_t confirmedBufferStart);
        [[nodiscard]] bool checkFinished(Thread* t, uint64_t confirmedBufferStart);

        const std::string getName() const override {
            return std::string{"Reader: " + fileName};
        }
    };
}

#endif
