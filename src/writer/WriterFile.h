/* Header for WriterFile class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef WRITER_FILE_H_
#define WRITER_FILE_H_

#include "Writer.h"

namespace OpenLogReplicator {
    class WriterFile final : public Writer {
    protected:
        enum class MODE : unsigned char {
            STDOUT,
            NO_ROTATE,
            NUM,
            TIMESTAMP,
            SEQUENCE
        };

        size_t prefixPos{0};
        size_t suffixPos{0};
        MODE mode{MODE::STDOUT};
        uint64_t fill{0};
        std::string output;
        std::string pathName;
        std::string fullFileName;
        std::string fileNameMask;
        std::string fileTimestampFormat;
        uint64_t fileNameNum{0};
        uint64_t fileSize{0};
        uint64_t maxFileSize;
        int outputDes{-1};
        uint64_t newLine;
        uint64_t append;
        Seq lastSequence{Seq::none()};
        const uint8_t* newLineMsg{nullptr};
        bool warningDisplayed{false};
        uint8_t* buffer{nullptr};
        uint bufferFill{0};
        uint writeBufferFlushSize;

        void closeFile();
        void checkFile(Scn scn, Seq sequence, uint64_t size);
        void sendMessage(BuilderMsg* msg) override;
        std::string getType() const override;
        void pollQueue() override;
        void unbufferedWrite(const uint8_t* data, uint64_t size);
        void bufferedWrite(const uint8_t* data, uint64_t size);

    public:
        WriterFile(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, std::string newOutput,
                   std::string newFileTimestampFormat, uint64_t newMaxFileSize, uint64_t newNewLine, uint64_t newAppend, uint newWiteBufferFlushSize);
        ~WriterFile() override;

        void initialize() override;
        void flush() override;
    };
}

#endif
