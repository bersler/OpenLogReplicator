/* Thread writing to file (or stdout)
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

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <algorithm>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/exception/ConfigurationException.h"
#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "WriterFile.h"

namespace OpenLogReplicator {
    WriterFile::WriterFile(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata,
                           std::string newOutput, std::string newFileTimestampFormat, uint64_t newMaxFileSize, uint64_t newNewLine, uint64_t newAppend,
                           uint newWriteBufferFlushSize):
            Writer(newCtx, std::move(newAlias), std::move(newDatabase), newBuilder, newMetadata),
            output(std::move(newOutput)),
            fileTimestampFormat(std::move(newFileTimestampFormat)),
            maxFileSize(newMaxFileSize),
            newLine(newNewLine),
            append(newAppend),
            writeBufferFlushSize(newWriteBufferFlushSize) {}

    WriterFile::~WriterFile() {
        closeFile();
        if (buffer == nullptr)
            return;
        ctx->freeMemoryChunk(this, Ctx::MEMORY::WRITER, buffer);
        buffer = nullptr;
    }

    void WriterFile::initialize() {
        Writer::initialize();
        buffer = ctx->getMemoryChunk(this, Ctx::MEMORY::WRITER);

        if (newLine == 1) {
            newLineMsg = reinterpret_cast<const uint8_t*>("\n");
        } else if (newLine == 2) {
            newLineMsg = reinterpret_cast<const uint8_t*>("\r\n");
        }

        if (this->output.empty()) {
            outputDes = STDOUT_FILENO;
            return;
        }

        auto outputIt = this->output.find_last_of('/');
        if (outputIt != std::string::npos) {
            pathName = this->output.substr(0, outputIt);
            fileNameMask = this->output.substr(outputIt + 1);
        } else {
            pathName = ".";
            fileNameMask = this->output;
        }

        if ((prefixPos = fileNameMask.find("%i")) != std::string::npos) {
            mode = MODE::NUM;
            suffixPos = prefixPos + 2;
        } else if ((prefixPos = fileNameMask.find("%2i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 2;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%3i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 3;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%4i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 4;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%5i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 5;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%6i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 6;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%7i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 7;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%8i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 8;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%9i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 9;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%10i")) != std::string::npos) {
            mode = MODE::NUM;
            fill = 10;
            suffixPos = prefixPos + 4;
        } else if ((prefixPos = fileNameMask.find("%t")) != std::string::npos) {
            mode = MODE::TIMESTAMP;
            suffixPos = prefixPos + 2;
        } else if ((prefixPos = fileNameMask.find("%s")) != std::string::npos) {
            mode = MODE::SEQUENCE;
            suffixPos = prefixPos + 2;
        } else {
            if ((prefixPos = fileNameMask.find('%')) != std::string::npos)
                throw ConfigurationException(30005, "invalid value for 'output': " + this->output);
            if (append == 0)
                throw ConfigurationException(30006, "output file is with no rotation: " + this->output + " - 'append' must be set to 1");
            mode = MODE::NO_ROTATE;
        }

        if ((mode == MODE::TIMESTAMP || mode == MODE::NUM) && maxFileSize == 0)
            throw ConfigurationException(30007, "output file is with no max file size: " + this->output +
                                         " - 'max-file-size' must be defined for output with rotation");

        // Search for last used number
        if (mode == MODE::NUM) {
            DIR* dir = opendir(pathName.c_str());
            if (dir == nullptr)
                throw RuntimeException(10012, "directory: " + pathName + " - can't read");

            dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                const std::string dName(ent->d_name);
                if (dName == "." || dName == "..")
                    continue;

                struct stat fileStat{};
                const std::string fileName(ent->d_name);

                const std::string fileNameFull(pathName + "/" + ent->d_name);
                if (stat(fileNameFull.c_str(), &fileStat) != 0) {
                    ctx->warning(10003, "file: " + fileNameFull + " - get metadata returned: " + strerror(errno));
                    continue;
                }

                if (S_ISDIR(fileStat.st_mode))
                    continue;

                const std::string prefix(fileNameMask.substr(0, prefixPos));
                if (fileName.length() < prefix.length() || fileName.substr(0, prefix.length()) != prefix)
                    continue;

                const std::string suffix(fileNameMask.substr(suffixPos));
                if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length()) != suffix)
                    continue;

                if (unlikely(ctx->isTraceSet(Ctx::TRACE::WRITER)))
                    ctx->logTrace(Ctx::TRACE::WRITER, "found previous output file: " + pathName + "/" + fileName);
                const std::string fileNameFoundNum(fileName.substr(prefix.length(), fileName.length() - suffix.length() - prefix.length()));
                uint64_t fileNum;
                try {
                    fileNum = strtoull(fileNameFoundNum.c_str(), nullptr, 10);
                } catch ([[maybe_unused]] const std::exception& e) {
                    // Ignore other files
                    continue;
                }
                if (append > 0) {
                    fileNameNum = std::max(fileNameNum, fileNum);
                } else {
                    if (fileNameNum <= fileNum)
                        fileNameNum = fileNum + 1;
                }
            }
            closedir(dir);
            ctx->info(0, "next number for " + this->output + " is: " + std::to_string(fileNameNum));
        }

        streaming = true;
    }

    void WriterFile::closeFile() {
        if (outputDes == -1)
            return;

        contextSet(CONTEXT::OS, REASON::OS);
        close(outputDes);
        contextSet(CONTEXT::CPU);
        outputDes = -1;
    }

    void WriterFile::checkFile(Scn scn __attribute__((unused)), Seq sequence, uint64_t size) {
        if (mode == MODE::STDOUT)
            return;

        if (mode == MODE::NO_ROTATE) {
            fullFileName = pathName + "/" + fileNameMask;
        } else if (mode == MODE::NUM) {
            if (fileSize + size > maxFileSize) {
                flush();
                closeFile();
                ++fileNameNum;
                fileSize = 0;
            }
            if (size > maxFileSize)
                ctx->warning(60029, "message size (" + std::to_string(size) + ") will exceed 'max-file' size (" +
                                    std::to_string(maxFileSize) + ")");

            if (outputDes == -1) {
                const std::string outputFileNumStr(std::to_string(fileNameNum));
                uint64_t zeros = 0;
                if (fill > outputFileNumStr.length())
                    zeros = fill - outputFileNumStr.length();
                fullFileName = pathName + "/" + fileNameMask.substr(0, prefixPos) + std::string(zeros, '0') + outputFileNumStr +
                               fileNameMask.substr(suffixPos);
            }
        } else if (mode == MODE::TIMESTAMP) {
            bool shouldSwitch = false;
            if (fileSize + size > maxFileSize)
                shouldSwitch = true;

            if (size > maxFileSize)
                ctx->warning(60029, "message size (" + std::to_string(size) + ") will exceed 'max-file' size (" +
                                    std::to_string(maxFileSize) + ")");

            if (outputDes == -1 || shouldSwitch) {
                const time_t now = time(nullptr);
                const tm nowTm = *localtime(&now);
                char str[50];
                strftime(str, sizeof(str), fileTimestampFormat.c_str(), &nowTm);
                const std::string newOutputFile = pathName + "/" + fileNameMask.substr(0, prefixPos) + str + fileNameMask.substr(suffixPos);
                if (fullFileName == newOutputFile) {
                    if (!warningDisplayed) {
                        ctx->warning(60030, "rotation size is set too low (" + std::to_string(maxFileSize) +
                                            "), increase it, should rotate but too early (" + fullFileName + ")");
                        warningDisplayed = true;
                    }
                    shouldSwitch = false;
                } else
                    fullFileName = newOutputFile;
            }

            if (shouldSwitch) {
                flush();
                closeFile();
                fileSize = 0;
            }
        } else if (mode == MODE::SEQUENCE) {
            if (sequence != lastSequence) {
                flush();
                closeFile();
            }

            lastSequence = sequence;
            if (outputDes == -1)
                fullFileName = pathName + "/" + fileNameMask.substr(0, prefixPos) + sequence.toString() +
                               fileNameMask.substr(suffixPos);
        }

        // File is closed, open it
        if (outputDes == -1) {
            struct stat fileStat{};
            contextSet(CONTEXT::OS, REASON::OS);
            const int statRet = stat(fullFileName.c_str(), &fileStat);
            contextSet(CONTEXT::CPU);
            if (statRet == 0) {
                // File already exists, append?
                if (append == 0)
                    throw RuntimeException(10003, "file: " + fullFileName + " - get metadata returned: " + strerror(errno));

                fileSize = fileStat.st_size;
            } else
                fileSize = 0;

            ctx->info(0, "opening output file: " + fullFileName);
            contextSet(CONTEXT::OS, REASON::OS);
            outputDes = open(fullFileName.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
            contextSet(CONTEXT::CPU);

            if (outputDes == -1)
                throw RuntimeException(10006, "file: " + fullFileName + " - open for writing returned: " + strerror(errno));

            contextSet(CONTEXT::OS, REASON::OS);
            const int lseekRet = lseek(outputDes, 0, SEEK_END);
            contextSet(CONTEXT::CPU);
            if (lseekRet == -1)
                throw RuntimeException(10011, "file: " + fullFileName + " - seek returned: " + strerror(errno));
        }
    }

    void WriterFile::sendMessage(BuilderMsg* msg) {
        checkFile(msg->scn, msg->sequence, msg->size + newLine);

        bufferedWrite(msg->data + msg->tagSize, msg->size - msg->tagSize);
        fileSize += msg->size - msg->tagSize;

        if (newLine > 0) {
            bufferedWrite(newLineMsg, newLine);
            fileSize += newLine;
        }

        confirmMessage(msg);
    }

    std::string WriterFile::getType() const {
        if (outputDes == STDOUT_FILENO)
            return "stdout";
        return "file:" + pathName + "/" + fileNameMask;
    }

    void WriterFile::pollQueue() {
        if (metadata->status == Metadata::STATUS::READY)
            metadata->setStatusStarting(this);

        flush();
    }

    void WriterFile::flush() {
        if (bufferFill == 0)
            return;

        unbufferedWrite(buffer, bufferFill);
        bufferFill = 0;
    }

    void WriterFile::unbufferedWrite(const uint8_t* data, uint64_t size) {
        contextSet(CONTEXT::OS, REASON::OS);
        const int64_t bytesWritten = write(outputDes, data, size);
        contextSet(CONTEXT::CPU);
        if (bytesWritten <= 0 || static_cast<uint64_t>(bytesWritten) != size)
            throw RuntimeException(10007, "file: " + fullFileName + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                                   std::to_string(size) + ", code returned: " + strerror(errno));
    }

    void WriterFile::bufferedWrite(const uint8_t* data, uint64_t size) {
        if (bufferFill + size > Ctx::MEMORY_CHUNK_SIZE)
            flush();

        if (size > Ctx::MEMORY_CHUNK_SIZE) {
            unbufferedWrite(data, size);
            return;
        }

        memcpy(buffer + bufferFill, data, size);
        bufferFill += size;

        if (bufferFill > writeBufferFlushSize)
            flush();
    }
}
