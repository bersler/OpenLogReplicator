/* Header for WriterFile class
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

#include "Writer.h"

#ifndef WRITERFILE_H_
#define WRITERFILE_H_

#define WRITERFILE_MODE_STDOUT              0
#define WRITERFILE_MODE_NOROTATE            1
#define WRITERFILE_MODE_NUM                 2
#define WRITERFILE_MODE_TIMETAMP            3
#define WRITERFILE_MODE_SEQUENCE            4

using namespace std;

namespace OpenLogReplicator {
    class RedoLogRecord;
    class OracleAnalyzer;

    class WriterFile : public Writer {
    protected:
        size_t prefixPos;
        size_t suffixPos;
        uint64_t mode;
        uint64_t fill;
        string output;
        string outputPath;
        string outputFile;
        string outputFileMask;
        string format;
        uint64_t outputFileNum;
        uint64_t outputSize;
        uint64_t maxSize;
        int64_t outputDes;
        uint64_t newLine;
        uint64_t append;
        typeSEQ lastSequence;
        char *newLineMsg;
        bool warningDisplayed;
        void closeFile(void);
        void checkFile(typeSCN scn, typeSEQ sequence, uint64_t length);
        virtual void sendMessage(OutputBufferMsg *msg);
        virtual string getName() const;
        virtual void pollQueue(void);

    public:
        WriterFile(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *output, const char *format, uint64_t maxSize, uint64_t newLine,
                uint64_t append, uint64_t pollIntervalUS, uint64_t checkpointIntervalS, uint64_t queueSize, typeSCN startScn, typeSEQ startSequence,
                const char* startTime, uint64_t startTimeRel);
        virtual ~WriterFile();
    };
}

#endif
