/* Thread writing to file (or stdout)
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

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

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
    WriterFile::WriterFile(Ctx* newCtx, const std::string& newAlias, const std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata,
                           const char* newOutput, const char* newTimestampFormat, uint64_t newMaxFileSize, uint64_t newNewLine, uint64_t newAppend) :
            Writer(newCtx, newAlias, newDatabase, newBuilder, newMetadata),
            prefixPos(0),
            suffixPos(0),
            mode(MODE_STDOUT),
            fill(0),
            output(newOutput),
            timestampFormat(newTimestampFormat),
            fileNameNum(0),
            fileSize(0),
            maxFileSize(newMaxFileSize),
            outputDes(-1),
            newLine(newNewLine),
            append(newAppend),
            lastSequence(Ctx::ZERO_SEQ),
            newLineMsg(nullptr),
            warningDisplayed(false) {
    }

    WriterFile::~WriterFile() {
        closeFile();
    }

    void WriterFile::initialize() {
        Writer::initialize();

        if (newLine == 1) {
            newLineMsg = "\n";
        } else if (newLine == 2) {
            newLineMsg = "\r\n";
        }

        if (this->output.length() == 0) {
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
            mode = MODE_NUM;
            suffixPos = prefixPos + 2;
        } else if ((prefixPos = fileNameMask.find("%2i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 2;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%3i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 3;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%4i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 4;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%5i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 5;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%6i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 6;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%7i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 7;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%8i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 8;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%9i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 9;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = fileNameMask.find("%10i")) != std::string::npos) {
            mode = MODE_NUM;
            fill = 10;
            suffixPos = prefixPos + 4;
        } else if ((prefixPos = fileNameMask.find("%t")) != std::string::npos) {
            mode = MODE_TIMESTAMP;
            suffixPos = prefixPos + 2;
        } else if ((prefixPos = fileNameMask.find("%s")) != std::string::npos) {
            mode = MODE_SEQUENCE;
            suffixPos = prefixPos + 2;
        } else {
            if ((prefixPos = fileNameMask.find('%')) != std::string::npos)
                throw ConfigurationException(30005, "invalid value for 'output': " + this->output);
            if (append == 0)
                throw ConfigurationException(30006, "output file is with no rotation: " + this->output + " - 'append' must be set to 1");
            mode = MODE_NO_ROTATE;
        }

        if ((mode == MODE_TIMESTAMP || mode == MODE_NUM) && maxFileSize == 0)
            throw ConfigurationException(30007, "output file is with no max file size: " + this->output +
                                                " - 'max-file-size' must be defined for output with rotation");

        // Search for last used number
        if (mode == MODE_NUM) {
            DIR* dir;
            if ((dir = opendir(pathName.c_str())) == nullptr)
                throw RuntimeException(10012, "directory: " + pathName + " - can't read");

            struct dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                    continue;

                struct stat fileStat;
                std::string fileName(ent->d_name);

                std::string fileNameFull(pathName + "/" + ent->d_name);
                if (stat(fileNameFull.c_str(), &fileStat) != 0) {
                    ctx->warning(60034, "file: " + fileNameFull + " - stat returned: " + strerror(errno));
                    continue;
                }

                if (S_ISDIR(fileStat.st_mode))
                    continue;

                std::string prefix(fileNameMask.substr(0, prefixPos));
                if (fileName.length() < prefix.length() || fileName.substr(0, prefix.length()) != prefix)
                    continue;

                std::string suffix(fileNameMask.substr(suffixPos));
                if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length()) != suffix)
                    continue;

                if (unlikely(ctx->trace & Ctx::TRACE_WRITER))
                    ctx->logTrace(Ctx::TRACE_WRITER, "found previous output file: " + pathName + "/" + fileName);
                std::string fileNameFoundNum(fileName.substr(prefix.length(), fileName.length() - suffix.length() - prefix.length()));
                typeScn fileNum;
                try {
                    fileNum = strtoull(fileNameFoundNum.c_str(), nullptr, 10);
                } catch (const std::exception& e) {
                    // Ignore other files
                    continue;
                }
                if (append > 0) {
                    if (fileNameNum < fileNum)
                        fileNameNum = fileNum;
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
        if (outputDes != -1) {
            close(outputDes);
            outputDes = -1;
        }
    }

    void WriterFile::checkFile(typeScn scn __attribute__((unused)), typeSeq sequence, uint64_t size) {
        if (mode == MODE_STDOUT) {
            return;
        } else if (mode == MODE_NO_ROTATE) {
            fullFileName = pathName + "/" + fileNameMask;
        } else if (mode == MODE_NUM) {
            if (fileSize + size > maxFileSize) {
                closeFile();
                ++fileNameNum;
                fileSize = 0;
            }
            if (size > maxFileSize)
                ctx->warning(60029, "message size (" + std::to_string(size) + ") will exceed 'max-file' size (" +
                                    std::to_string(maxFileSize) + ")");

            if (outputDes == -1) {
                std::string outputFileNumStr(std::to_string(fileNameNum));
                uint64_t zeros = 0;
                if (fill > outputFileNumStr.length())
                    zeros = fill - outputFileNumStr.length();
                fullFileName = pathName + "/" + fileNameMask.substr(0, prefixPos) + std::string(zeros, '0') + outputFileNumStr +
                               fileNameMask.substr(suffixPos);
            }
        } else if (mode == MODE_TIMESTAMP) {
            bool shouldSwitch = false;
            if (fileSize + size > maxFileSize)
                shouldSwitch = true;

            if (size > maxFileSize)
                ctx->warning(60029, "message size (" + std::to_string(size) + ") will exceed 'max-file' size (" +
                                    std::to_string(maxFileSize) + ")");

            if (outputDes == -1 || shouldSwitch) {
                time_t now = time(nullptr);
                tm nowTm = *localtime(&now);
                char str[50];
                strftime(str, sizeof(str), timestampFormat.c_str(), &nowTm);
                std::string newOutputFile = pathName + "/" + fileNameMask.substr(0, prefixPos) + str + fileNameMask.substr(suffixPos);
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
                closeFile();
                fileSize = 0;
            }
        } else if (mode == MODE_SEQUENCE) {
            if (sequence != lastSequence) {
                closeFile();
            }

            lastSequence = sequence;
            if (outputDes == -1)
                fullFileName = pathName + "/" + fileNameMask.substr(0, prefixPos) + std::to_string(sequence) +
                               fileNameMask.substr(suffixPos);
        }

        // File is closed, open it
        if (outputDes == -1) {
            struct stat fileStat;
            if (stat(fullFileName.c_str(), &fileStat) == 0) {
                // File already exists, append?
                if (append == 0)
                    throw RuntimeException(10003, "file: " + fullFileName + " - stat returned: " + strerror(errno));

                fileSize = fileStat.st_size;
            } else
                fileSize = 0;

            ctx->info(0, "opening output file: " + fullFileName);
            outputDes = open(fullFileName.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

            if (outputDes == -1)
                throw RuntimeException(10006, "file: " + fullFileName + " - open for write returned: " + strerror(errno));

            if (lseek(outputDes, 0, SEEK_END) == -1)
                throw RuntimeException(10011, "file: " + fullFileName + " - seek returned: " + strerror(errno));
        }
    }

    void WriterFile::sendMessage(BuilderMsg* msg) {
        if (newLine > 0)
            checkFile(msg->scn, msg->sequence, msg->size + 1);
        else
            checkFile(msg->scn, msg->sequence, msg->size);

        int64_t bytesWritten = write(outputDes, reinterpret_cast<const char*>(msg->data), msg->size);
        if (static_cast<uint64_t>(bytesWritten) != msg->size)
            throw RuntimeException(10007, "file: " + fullFileName + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                                          std::to_string(msg->size) + ", code returned: " + strerror(errno));

        fileSize += bytesWritten;

        if (newLine > 0) {
            bytesWritten = write(outputDes, newLineMsg, newLine);
            if (static_cast<uint64_t>(bytesWritten) != newLine)
                throw RuntimeException(10007, "file: " + fullFileName + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                                              std::to_string(msg->size) + ", code returned: " + strerror(errno));
            fileSize += bytesWritten;
        }

        confirmMessage(msg);
    }

    std::string WriterFile::getName() const {
        if (outputDes == STDOUT_FILENO)
            return "stdout";
        else
            return "file:" + pathName + "/" + fileNameMask;
    }

    void WriterFile::pollQueue() {
        if (metadata->status == Metadata::STATUS_READY)
            metadata->setStatusStart();
    }
}
