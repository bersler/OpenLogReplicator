/* Thread writing to file (or stdout)
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

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "OracleAnalyzer.h"
#include "OutputBuffer.h"
#include "RuntimeException.h"
#include "WriterFile.h"

using namespace std;

namespace OpenLogReplicator {
    WriterFile::WriterFile(const char* alias, OracleAnalyzer* oracleAnalyzer, uint64_t pollIntervalUs, uint64_t checkpointIntervalS,
            uint64_t queueSize, typeSCN startScn, typeSEQ startSequence, const char* startTime, uint64_t startTimeRel,
            const char* output, const char* format, uint64_t maxSize, uint64_t newLine, uint64_t append) :
        Writer(alias, oracleAnalyzer, 0, pollIntervalUs, checkpointIntervalS, queueSize, startScn, startSequence, startTime, startTimeRel),
        prefixPos(0),
        suffixPos(0),
        mode(WRITERFILE_MODE_STDOUT),
        fill(0),
        output(output),
        format(format),
        outputFileNum(0),
        outputSize(0),
        maxSize(maxSize),
        outputDes(-1),
        newLine(newLine),
        append(append),
        lastSequence(ZERO_SEQ),
        newLineMsg(nullptr),
        warningDisplayed(false) {
    }

    WriterFile::~WriterFile() {
        closeFile();
    }

    void WriterFile::initialize(void) {
        Writer::initialize();

        if (newLine == 1) {
            newLineMsg = "\n";
        } else
        if (newLine == 2) {
            newLineMsg = "\r\n";
        }

        if (this->output.length() == 0) {
            outputDes = STDOUT_FILENO;
            return;
        }

        size_t pathPos = this->output.find_last_of("/");
        if (pathPos != string::npos) {
            outputPath =  this->output.substr(0, pathPos);
            outputFileMask = this->output.substr(pathPos + 1);
        } else {
            outputPath = ".";
            outputFileMask = this->output;
        }

        if ((prefixPos = this->output.find("%i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            suffixPos = prefixPos + 2;
        } else
        if ((prefixPos = this->output.find("%2i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 2;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%3i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 3;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%4i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 4;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%5i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 5;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%6i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 6;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%7i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 7;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%8i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 8;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%9i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 9;
            suffixPos = prefixPos + 3;
        } else
        if ((prefixPos = this->output.find("%10i")) != string::npos) {
            mode = WRITERFILE_MODE_NUM;
            fill = 10;
            suffixPos = prefixPos + 4;
        } else
        if ((prefixPos = this->output.find("%t")) != string::npos) {
            mode = WRITERFILE_MODE_TIMETAMP;
            suffixPos = prefixPos + 2;
        } else
        if ((prefixPos = this->output.find("%s")) != string::npos) {
            mode = WRITERFILE_MODE_SEQUENCE;
            suffixPos = prefixPos + 2;
        } else {
            if ((prefixPos = this->output.find("%")) != string::npos) {
                RUNTIME_FAIL("invalid value for \"output\": " << this->output);
            }
            if (append == 0) {
                RUNTIME_FAIL("output file is with no rotation: " << this->output << " - \"append\" must be set to 1");
            }
            mode = WRITERFILE_MODE_NOROTATE;
        }

        if ((mode == WRITERFILE_MODE_TIMETAMP || mode == WRITERFILE_MODE_NUM) && maxSize == 0) {
            RUNTIME_FAIL("output file is with no max size: " << this->output << " - \"max-size\" must be defined for output with rotation");
        }

        //search for last used number
        if (mode == WRITERFILE_MODE_NUM) {
            DIR* dir;
            if ((dir = opendir(outputPath.c_str())) == nullptr) {
                RUNTIME_FAIL("can't access directory: " << outputPath << " to create output files defined with: " << this->output);
            }

            struct dirent* ent;
            typeSCN fileScnMax = ZERO_SCN;
            while ((ent = readdir(dir)) != nullptr) {
                if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                    continue;

                struct stat fileStat;
                string fileNameFound(ent->d_name);

                string fullName(outputPath + "/" + ent->d_name);
                if (stat(fullName.c_str(), &fileStat)) {
                    WARNING("reading information for file: " << fullName << " - " << strerror(errno));
                    continue;
                }

                if (S_ISDIR(fileStat.st_mode))
                    continue;

                string prefix(outputFileMask.substr(0, prefixPos));
                if (fileNameFound.length() < prefix.length() || fileNameFound.substr(0, prefix.length()).compare(prefix) != 0)
                    continue;

                string suffix(outputFileMask.substr(suffixPos));
                if (fileNameFound.length() < suffix.length() || fileNameFound.substr(fileNameFound.length() - suffix.length()).compare(suffix) != 0)
                    continue;

                TRACE(TRACE2_WRITER, "WRITER: found previous output file: " << outputPath << "/" << fileNameFound);
                string fileNameFoundNum(fileNameFound.substr(prefix.length(), fileNameFound.length() - suffix.length() - prefix.length()));
                typeSCN fileNum;
                try {
                    fileNum = strtoull(fileNameFoundNum.c_str(), nullptr, 10);
                } catch (exception& e) {
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
            INFO("next number for " << this->output << " is: " << dec << outputFileNum);
        }
    }

    void WriterFile::closeFile(void) {
        if (outputDes != -1) {
            close(outputDes);
            outputDes = -1;
        }
    }

    void WriterFile::checkFile(typeSCN scn, typeSEQ sequence, uint64_t length) {
        if (mode == WRITERFILE_MODE_STDOUT) {
            return;
        } else
        if (mode == WRITERFILE_MODE_NOROTATE) {
            outputFile = outputPath + "/" + outputFileMask;
        } else
        if (mode == WRITERFILE_MODE_NUM) {
            if (outputSize + length > maxSize) {
                closeFile();
                ++outputFileNum;
                outputSize = 0;
            }
            if (length > maxSize) {
                WARNING("message size (" << dec << length << ") will exceed \"max-file\" size (" << maxSize << ")");
            }

            if (outputDes == -1) {
                string outputFileNumStr(to_string(outputFileNum));
                uint64_t zeros = 0;
                if (fill > outputFileNumStr.length())
                    zeros = fill - outputFileNumStr.length();
                outputFile = outputPath + "/" + outputFileMask.substr(0, prefixPos) + string(zeros, '0') + outputFileNumStr + outputFileMask.substr(suffixPos);
            }
        } else
        if (mode == WRITERFILE_MODE_TIMETAMP) {
            bool shouldSwitch = false;
            if (outputSize + length > maxSize)
                shouldSwitch = true;

            if (length > maxSize) {
                WARNING("message size (" << dec << length << ") will exceed \"max-file\" size (" << maxSize << ")");
            }

            if (outputDes == -1 || shouldSwitch) {
                stringstream __s;
                time_t now = time(nullptr);
                tm nowTm = *localtime(&now);
                char str[50];
                strftime(str, sizeof(str), format.c_str(), &nowTm);
                string newOutputFile = outputPath + "/" + outputFileMask.substr(0, prefixPos) + str + outputFileMask.substr(suffixPos);
                if (outputFile.compare(newOutputFile) == 0) {
                    if (!warningDisplayed) {
                        WARNING("rotation size is set too low (" << dec << maxSize << "), increase it, should rotate but too early (" << outputFile << ")");
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
        } else
        if (mode == WRITERFILE_MODE_SEQUENCE) {
            if (sequence != lastSequence) {
                closeFile();
            }

            lastSequence = sequence;
            if (outputDes == -1)
                outputFile = outputPath + "/" + outputFileMask.substr(0, prefixPos) + to_string(sequence) + outputFileMask.substr(suffixPos);
        }

        //file is closed, open it
        if (outputDes == -1) {
            struct stat fileStat;
            int statRet = stat(outputFile.c_str(), &fileStat);
            TRACE(TRACE2_WRITER, "WRITER: stat for " << outputFile << " returns " << dec << statRet << ", errno = " << errno);

            //file already exists, append?
            if (append == 0 && statRet == 0) {
                RUNTIME_FAIL("output file already exists but append mode is not used: " << dec << outputFile);
            }

            INFO("opening output file: " << outputFile);
            outputDes = open(outputFile.c_str(), O_CREAT | O_WRONLY | O_LARGEFILE, S_IRUSR | S_IWUSR);
            if (outputDes == -1) {
                RUNTIME_FAIL("opening in write mode file: " << outputFile << " - " << strerror(errno));
            }
            if (lseek(outputDes, 0, SEEK_END) == -1) {
                RUNTIME_FAIL("seeking to end of file: " << outputFile << " - " << strerror(errno));
            }

            if (statRet == 0)
                outputSize = fileStat.st_size;
            else
                outputSize = 0;
        }
    }

    void WriterFile::sendMessage(OutputBufferMsg* msg) {
        if (newLine > 0)
            checkFile(msg->scn, msg->sequence, msg->length + 1);
        else
            checkFile(msg->scn, msg->sequence, msg->length);

        int64_t bytesWritten = write(outputDes, (const char*)msg->data, msg->length);
        if (bytesWritten != msg->length) {
            RUNTIME_FAIL("writing file: " << outputFile << " - " << strerror(errno));
        }
        outputSize += bytesWritten;

        if (newLine > 0) {
            bytesWritten = write(outputDes, newLineMsg, newLine);
            if (bytesWritten != newLine) {
                RUNTIME_FAIL("writing file: " << outputFile << " - " << strerror(errno));
            }
            outputSize += bytesWritten;
        }

        confirmMessage(msg);
    }

    string WriterFile::getName() const {
        if (outputDes == STDOUT_FILENO)
            return "stdout";
        else
            return "file:" + outputPath + "/" + outputFileMask;
    }

    void WriterFile::pollQueue(void) {
    }
}
