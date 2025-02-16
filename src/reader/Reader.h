/* Header for Reader class
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/types/FileOffset.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"

#ifndef READER_H_
#define READER_H_

namespace OpenLogReplicator {
    class Reader : public Thread {
    public:
        enum class REDO_CODE : unsigned char {
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

        enum class STATUS : unsigned char {
            SLEEPING, CHECK, UPDATE, READ
        };

        static constexpr uint PAGE_SIZE_MAX{4096};
        static constexpr uint BAD_CDC_MAX_CNT{20};

        std::string database;
        int fileCopyDes{-1};
        uint64_t fileSize{0};
        Seq fileCopySequence;
        bool hintDisplayed{false};
        bool configuredBlockSum;
        bool readBlocks{false};
        bool reachedZero{false};
        std::string fileNameWrite;
        int group;
        Seq sequence;
        typeBlk numBlocksHeader{Ctx::ZERO_BLK};
        typeResetlogs resetlogs{0};
        typeActivation activation{0};
        uint8_t* headerBuffer{nullptr};
        uint32_t compatVsn{0};
        Time firstTimeHeader{0};
        Scn firstScn{Scn::none()};
        Scn firstScnHeader{Scn::none()};
        Scn nextScn{Scn::none()};
        Scn nextScnHeader{Scn::none()};
        Time nextTime{0};
        uint blockSize{0};
        uint64_t sumRead{0};
        uint64_t sumTime{0};
        uint64_t bufferScan{0};
        uint lastRead{0};
        time_ut lastReadTime{0};
        time_ut readTime{0};
        time_ut loopTime{0};

        std::mutex mtx;
        std::atomic<uint64_t> bufferStart{0};
        std::atomic<uint64_t> bufferEnd{0};
        std::atomic<STATUS> status{STATUS::SLEEPING};
        std::atomic<REDO_CODE> ret{REDO_CODE::OK};
        std::condition_variable condBufferFull;
        std::condition_variable condReaderSleeping;
        std::condition_variable condParserSleeping;

        virtual void redoClose() = 0;
        virtual REDO_CODE redoOpen() = 0;
        virtual int redoRead(uint8_t* buf, uint64_t offset, uint size) = 0;
        virtual uint readSize(uint prevRead);
        virtual REDO_CODE reloadHeaderRead();
        REDO_CODE checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint);
        REDO_CODE reloadHeader();
        bool read1();
        bool read2();
        void mainLoop();

    public:
        const static char* REDO_MSG[static_cast<uint>(REDO_CODE::CNT)];
        uint8_t** redoBufferList{nullptr};
        std::vector<std::string> paths;
        std::string fileName;

        Reader(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum);
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
        [[nodiscard]] FileOffset getBufferStart() const;
        [[nodiscard]] FileOffset getBufferEnd() const;
        [[nodiscard]] REDO_CODE getRet() const;
        [[nodiscard]] Scn getFirstScn() const;
        [[nodiscard]] Scn getFirstScnHeader() const;
        [[nodiscard]] Scn getNextScn() const;
        [[nodiscard]] Time getNextTime() const;
        [[nodiscard]] typeBlk getNumBlocks() const;
        [[nodiscard]] int getGroup() const;
        [[nodiscard]] Seq getSequence() const;
        [[nodiscard]] typeResetlogs getResetlogs() const;
        [[nodiscard]] typeActivation getActivation() const;
        [[nodiscard]] uint64_t getSumRead() const;
        [[nodiscard]] uint64_t getSumTime() const;

        void setRet(REDO_CODE newRet);
        void setBufferStartEnd(FileOffset newBufferStart, FileOffset newBufferEnd);
        bool checkRedoLog();
        bool updateRedoLog();
        void setStatusRead();
        void confirmReadData(FileOffset confirmedBufferStart);
        [[nodiscard]] bool checkFinished(Thread* t, FileOffset confirmedBufferStart);
        virtual void showHint(Thread* t, std::string origPath, std::string mappedPath) const = 0;

        std::string getName() const override {
            return {"Reader: " + fileName};
        }
    };
}

#endif
