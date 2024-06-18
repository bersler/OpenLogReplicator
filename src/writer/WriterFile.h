/* Header for WriterFile class
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

#include "Writer.h"

#ifndef WRITER_FILE_H_
#define WRITER_FILE_H_

namespace OpenLogReplicator {
    class WriterFile final : public Writer {
    protected:
        static constexpr uint64_t MODE_STDOUT = 0;
        static constexpr uint64_t MODE_NO_ROTATE = 1;
        static constexpr uint64_t MODE_NUM = 2;
        static constexpr uint64_t MODE_TIMESTAMP = 3;
        static constexpr uint64_t MODE_SEQUENCE = 4;

        size_t prefixPos;
        size_t suffixPos;
        uint64_t mode;
        uint64_t fill;
        std::string output;
        std::string pathName;
        std::string fullFileName;
        std::string fileNameMask;
        std::string timestampFormat;
        uint64_t fileNameNum;
        uint64_t fileSize;
        uint64_t maxFileSize;
        int outputDes;
        uint64_t newLine;
        uint64_t append;
        typeSeq lastSequence;
        const char* newLineMsg;
        bool warningDisplayed;
        void closeFile();
        void checkFile(typeScn scn, typeSeq sequence, uint64_t size);
        void sendMessage(BuilderMsg* msg) override;
        std::string getName() const override;
        void pollQueue() override;

    public:
        WriterFile(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata, const char* newOutput,
                   const char* newTimestampFormat, uint64_t newMaxFileSize, uint64_t newNewLine, uint64_t newAppend);
        ~WriterFile() override;

        void initialize() override;
    };
}

#endif
