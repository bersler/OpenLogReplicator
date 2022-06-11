/* Thread writing to file (or stdout)
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/RuntimeException.h"
#include "WriterFile.h"

namespace OpenLogReplicator {
    WriterFile::WriterFile(Ctx* newCtx, std::string newAlias, std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata, const char* newOutput,
                           const char* newFormat, uint64_t newMaxSize, uint64_t newNewLine, uint64_t newAppend) :
        Writer(newCtx, newAlias, newDatabase, newBuilder, newMetadata),
        prefixPos(0),
        suffixPos(0),
        mode(WRITER_FILE_MODE_STDOUT),
        fill(0),
        output(newOutput),
        format(newFormat),
        outputFileNum(0),
        outputSize(0),
        maxSize(newMaxSize),
        outputDes(-1),
        newLine(newNewLine),
        append(newAppend),
        lastSequence(ZERO_SEQ),
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

        size_t pathPos = this->output.find_last_of('/');
        if (pathPos != std::string::npos) {
            outputPath =  this->output.substr(0, pathPos);
            outputFileMask = this->output.substr(pathPos + 1);
        } else {
            outputPath = ".";
            outputFileMask = this->output;
        }

        if ((prefixPos = this->output.find("%i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            suffixPos = prefixPos + 2;
        } else if ((prefixPos = this->output.find("%2i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 2;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%3i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 3;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%4i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 4;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%5i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 5;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%6i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 6;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%7i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 7;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%8i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 8;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%9i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 9;
            suffixPos = prefixPos + 3;
        } else if ((prefixPos = this->output.find("%10i")) != std::string::npos) {
            mode = WRITER_FILE_MODE_NUM;
            fill = 10;
            suffixPos = prefixPos + 4;
        } else if ((prefixPos = this->output.find("%t")) != std::string::npos) {
            mode = WRITER_FILE_MODE_TIMESTAMP;
            suffixPos = prefixPos + 2;
        } else if ((prefixPos = this->output.find("%s")) != std::string::npos) {
            mode = WRITER_FILE_MODE_SEQUENCE;
            suffixPos = prefixPos + 2;
        } else {
            if ((prefixPos = this->output.find('%')) != std::string::npos)
                throw RuntimeException("invalid value for 'output': " + this->output);
            if (append == 0)
                throw RuntimeException("output file is with no rotation: " + this->output + " - 'append' must be set to 1");
            mode = WRITER_FILE_MODE_NO_ROTATE;
        }

        if ((mode == WRITER_FILE_MODE_TIMESTAMP || mode == WRITER_FILE_MODE_NUM) && maxSize == 0)
            throw RuntimeException("output file is with no max size: " + this->output + " - 'max-size' must be defined for output with rotation");

        //search for last used number
        if (mode == WRITER_FILE_MODE_NUM) {
            DIR* dir;
            if ((dir = opendir(outputPath.c_str())) == nullptr)
                throw RuntimeException("can't access directory: " + outputPath + " to create output files defined with: " + this->output);

            struct dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                    continue;

                struct stat fileStat;
                std::string fileNameFound(ent->d_name);

                std::string fullName(outputPath + "/" + ent->d_name);
                if (stat(fullName.c_str(), &fileStat)) {
                    WARNING("reading information for file: " << fullName << " - " << strerror(errno))
                    continue;
                }

                if (S_ISDIR(fileStat.st_mode))
                    continue;

                std::string prefix(outputFileMask.substr(0, prefixPos));
                if (fileNameFound.length() < prefix.length() || fileNameFound.substr(0, prefix.length()) != prefix)
                    continue;

                std::string suffix(outputFileMask.substr(suffixPos));
                if (fileNameFound.length() < suffix.length() || fileNameFound.substr(fileNameFound.length() - suffix.length()) != suffix)
                    continue;

                TRACE(TRACE2_WRITER, "WRITER: found previous output file: " << outputPath << "/" << fileNameFound)
                std::string fileNameFoundNum(fileNameFound.substr(prefix.length(), fileNameFound.length() - suffix.length() - prefix.length()));
                typeScn fileNum;
                try {
                    fileNum = strtoull(fileNameFoundNum.c_str(), nullptr, 10);
                } catch (std::exception& e) {
                    //ignore other files
                    continue;
                }
                if (append > 0) {
                    if (outputFileNum < fileNum)
                        outputFileNum = fileNum;
                } else {
                    if (outputFileNum <= fileNum)
                        outputFileNum = fileNum + 1;
                }
            }
            closedir(dir);
            INFO("next number for " << this->output << " is: " << std::dec << outputFileNum)
        }
    }

    void WriterFile::closeFile() {
        if (outputDes != -1) {
            close(outputDes);
            outputDes = -1;
        }
    }

    void WriterFile::checkFile(typeScn scn __attribute__((unused)), typeSeq sequence, uint64_t length) {
        if (mode == WRITER_FILE_MODE_STDOUT) {
            return;
        } else if (mode == WRITER_FILE_MODE_NO_ROTATE) {
            outputFile = outputPath + "/" + outputFileMask;
        } else if (mode == WRITER_FILE_MODE_NUM) {
            if (outputSize + length > maxSize) {
                closeFile();
                ++outputFileNum;
                outputSize = 0;
            }
            if (length > maxSize) {
                WARNING("message size (" << std::dec << length << ") will exceed 'max-file' size (" << maxSize << ")")
            }

            if (outputDes == -1) {
                std::string outputFileNumStr(std::to_string(outputFileNum));
                uint64_t zeros = 0;
                if (fill > outputFileNumStr.length())
                    zeros = fill - outputFileNumStr.length();
                outputFile = outputPath + "/" + outputFileMask.substr(0, prefixPos) + std::string(zeros, '0') + outputFileNumStr + outputFileMask.substr(suffixPos);
            }
        } else if (mode == WRITER_FILE_MODE_TIMESTAMP) {
            bool shouldSwitch = false;
            if (outputSize + length > maxSize)
                shouldSwitch = true;

            if (length > maxSize) {
                WARNING("message size (" << std::dec << length << ") will exceed 'max-file' size (" << maxSize << ")")
            }

            if (outputDes == -1 || shouldSwitch) {
                time_t now = time(nullptr);
                tm nowTm = *localtime(&now);
                char str[50];
                strftime(str, sizeof(str), format.c_str(), &nowTm);
                std::string newOutputFile = outputPath + "/" + outputFileMask.substr(0, prefixPos) + str + outputFileMask.substr(suffixPos);
                if (outputFile == newOutputFile) {
                    if (!warningDisplayed) {
                        WARNING("rotation size is set too low (" << std::dec << maxSize << "), increase it, should rotate but too early (" << outputFile << ")")
                        warningDisplayed = true;
                    }
                    shouldSwitch = false;
                } else
                    outputFile = newOutputFile;
            }

            if (shouldSwitch) {
                closeFile();
                outputSize = 0;
            }
        } else if (mode == WRITER_FILE_MODE_SEQUENCE) {
            if (sequence != lastSequence) {
                closeFile();
            }

            lastSequence = sequence;
            if (outputDes == -1)
                outputFile = outputPath + "/" + outputFileMask.substr(0, prefixPos) + std::to_string(sequence) + outputFileMask.substr(suffixPos);
        }

        //file is closed, open it
        if (outputDes == -1) {
            struct stat fileStat;
            int statRet = stat(outputFile.c_str(), &fileStat);
            TRACE(TRACE2_WRITER, "WRITER: stat for " << outputFile << " returns " << std::dec << statRet << ", errno = " << errno)

            //file already exists, append?
            if (append == 0 && statRet == 0)
                throw RuntimeException("output file already exists but append mode is not used: " + outputFile);

            INFO("opening output file: " << outputFile)
            outputDes = open(outputFile.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
            if (outputDes == -1)
                throw RuntimeException("opening in write mode file: " + outputFile + " - " + strerror(errno));
            if (lseek(outputDes, 0, SEEK_END) == -1)
                throw RuntimeException("seeking to end of file: " + outputFile + " - " + strerror(errno));

            if (statRet == 0)
                outputSize = fileStat.st_size;
            else
                outputSize = 0;
        }
    }

    void WriterFile::sendMessage(BuilderMsg* msg) {
        if (newLine > 0)
            checkFile(msg->scn, msg->sequence, msg->length + 1);
        else
            checkFile(msg->scn, msg->sequence, msg->length);

        int64_t bytesWritten = write(outputDes, (const char*)msg->data, msg->length);
        if ((uint64_t)bytesWritten != msg->length)
            throw RuntimeException("writing file: " + outputFile + " - " + strerror(errno));
        outputSize += bytesWritten;

        if (newLine > 0) {
            bytesWritten = write(outputDes, newLineMsg, newLine);
            if ((uint64_t)bytesWritten != newLine)
                throw RuntimeException("writing file: " + outputFile + " - " + strerror(errno));
            outputSize += bytesWritten;
        }

        confirmMessage(msg);
    }

    std::string WriterFile::getName() const {
        if (outputDes == STDOUT_FILENO)
            return "stdout";
        else
            return "file:" + outputPath + "/" + outputFileMask;
    }

    void WriterFile::pollQueue() {
    }
}
