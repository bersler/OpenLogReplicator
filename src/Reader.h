/* Header for Reader class
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

#include <vector>
#include "Thread.h"

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
#define REDO_ERROR              2
#define REDO_FINISHED           3
#define REDO_EMPTY              4
#define REDO_BAD_CRC            5

#define DISK_BUFFER_SIZE        MEMORY_CHUNK_SIZE
#define REDO_PAGE_SIZE_MAX      4096
#define REDO_BAD_CDC_MAX_CNT    20

using namespace std;

namespace OpenLogReplicator {

    class OracleAnalyzer;

    class Reader : public Thread {
    protected:
        OracleAnalyzer *oracleAnalyzer;
        bool singleBlockRead;
        bool hintDisplayed;

        virtual void redoClose(void) = 0;
        virtual uint64_t redoOpen(void) = 0;
        virtual int64_t redoRead(uint8_t *buf, uint64_t pos, uint64_t size) = 0;

        uint64_t checkBlockHeader(uint8_t *buffer, typeblk blockNumber, bool checkSum);
        uint64_t reloadHeader(void);

    public:
        uint8_t *redoBuffer;
        uint8_t *headerBuffer;
        int64_t group;
        typeSEQ sequence;
        vector<string> paths;
        string pathMapped;
        uint64_t blockSize;
        typeblk numBlocks;
        typeSCN firstScn;
        typeSCN nextScn;
        uint32_t compatVsn;
        typeresetlogs resetlogsRead;
        typeactivation activationRead;
        typeSCN firstScnHeader;
        typeSCN nextScnHeader;

        uint64_t fileSize;
        volatile uint64_t status;
        volatile uint64_t ret;
        volatile uint64_t bufferStart;
        volatile uint64_t bufferEnd;

        Reader(const char *alias, OracleAnalyzer *oracleAnalyzer, int64_t group, bool singleBlockRead);
        virtual ~Reader();

        void *run(void);
        typesum calcChSum(uint8_t *buffer, uint64_t size) const;
    };
}

#endif
