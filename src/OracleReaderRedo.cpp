/* Class reading a redo log file
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <iomanip>
#include <list>
#include <ctime>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include "OracleReader.h"
#include "OracleReaderRedo.h"
#include "OracleObject.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "Transaction.h"
#include "TransactionMap.h"
#include "OpCode0501.h"
#include "OpCode0502.h"
#include "OpCode0504.h"
#include "OpCode0506.h"
#include "OpCode050B.h"
#include "OpCode0513.h"
#include "OpCode0514.h"
#include "OpCode0B02.h"
#include "OpCode0B03.h"
#include "OpCode0B04.h"
#include "OpCode0B05.h"
#include "OpCode0B06.h"
#include "OpCode0B08.h"
#include "OpCode0B0B.h"
#include "OpCode0B0C.h"
#include "OpCode1801.h"

using namespace std;

void stopMain();

namespace OpenLogReplicator {

    OracleReaderRedo::OracleReaderRedo(OracleReader *oracleReader, int64_t group, const char* path) :
            oracleReader(oracleReader),
            group(group),
            blockSize(0),
            blockNumber(0),
            numBlocks(0),
            lastBytesRead(READ_CHUNK_MIN_SIZE),
            lastReadSuccessfull(false),
            headerInfoPrinted(false),
            fileDes(0),
            lastCheckpointScn(0),
            extScn(0),
            curScn(ZERO_SCN),
            curScnPrev(0),
            curSubScn(0),
            recordBeginPos(0),
            recordBeginBlock(0),
            recordTimestmap(0),
            recordPos(0),
            recordLeftToCopy(0),
            redoBufferPos(0),
            redoBufferFileStart(0),
            redoBufferFileEnd(0),
            path(path),
            firstScn(ZERO_SCN),
            nextScn(ZERO_SCN),
            sequence(0) {
    }

    uint64_t OracleReaderRedo::checkBlockHeader(uint8_t *buffer, typeblk blockNumber) {
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_EMPTY;

        if (buffer[0] != 1 || buffer[1] != 0x22) {
            cerr << "ERROR: header bad magic number for block " << dec << blockNumber << endl;
            return REDO_ERROR;
        }

        typeblk blockNumberHeader = oracleReader->read32(buffer + 4);
        typeseq sequenceHeader = oracleReader->read32(buffer + 8);

        if (sequence == 0) {
            sequence = sequenceHeader;
        } else {
            if (group == 0) {
                if (sequence != sequenceHeader)
                    return REDO_WRONG_SEQUENCE;
            } else {
                if (sequence > sequenceHeader)
                    return REDO_EMPTY;
                if (sequence < sequenceHeader)
                    return REDO_WRONG_SEQUENCE_SWITCHED;
            }
        }

        if (blockNumberHeader != blockNumber) {
            cerr << "ERROR: header bad block number for " << dec << blockNumber << ", found: " << blockNumberHeader << endl;
            return REDO_ERROR;
        }

        return REDO_OK;
    }

    uint64_t OracleReaderRedo::checkRedoHeader() {
        int64_t bytes = pread(fileDes, oracleReader->headerBuffer, REDO_PAGE_SIZE_MAX * 2, 0);
        if (bytes < REDO_PAGE_SIZE_MAX * 2) {
            cerr << "ERROR: unable to read redo header for " << path << " bytes read: " << dec << bytes << endl;
            return REDO_ERROR;
        }

        //check file header
        if (oracleReader->headerBuffer[0] != 0 ||
                oracleReader->headerBuffer[1] != 0x22 ||
                oracleReader->headerBuffer[28] != 0x7D ||
                oracleReader->headerBuffer[29] != 0x7C ||
                oracleReader->headerBuffer[30] != 0x7B ||
                oracleReader->headerBuffer[31] != 0x7A) {
            cerr << "[0]: " << hex << (uint64_t)oracleReader->headerBuffer[0] << endl;
            cerr << "[1]: " << hex << (uint64_t)oracleReader->headerBuffer[1] << endl;
            cerr << "[28]: " << hex << (uint64_t)oracleReader->headerBuffer[28] << endl;
            cerr << "[29]: " << hex << (uint64_t)oracleReader->headerBuffer[29] << endl;
            cerr << "[30]: " << hex << (uint64_t)oracleReader->headerBuffer[30] << endl;
            cerr << "[31]: " << hex << (uint64_t)oracleReader->headerBuffer[31] << endl;
            cerr << "ERROR: block header bad magic fields" << endl;
            return REDO_ERROR;
        }

        blockSize = oracleReader->read16(oracleReader->headerBuffer + 20);
        if (blockSize != 512 && blockSize != 1024) {
            cerr << "ERROR: unsupported block size: " << blockSize << endl;
            return REDO_ERROR;
        }

        //check first block
        if (bytes < ((int64_t)blockSize * 2)) {
            cerr << "ERROR: unable to read redo header for " << path << endl;
            return REDO_ERROR;
        }

        numBlocks = oracleReader->read32(oracleReader->headerBuffer + 24);
        uint32_t compatVsn = oracleReader->read32(oracleReader->headerBuffer + blockSize + 20);

        if (compatVsn == 0x0B200000) //11.2.0.0
            oracleReader->version = 0x11200;
        else
        if (compatVsn == 0x0B200100) //11.2.0.1
            oracleReader->version = 0x11201;
        else
        if (compatVsn == 0x0B200200) //11.2.0.2
            oracleReader->version = 0x11202;
        else
        if (compatVsn == 0x0B200300) //11.2.0.3
            oracleReader->version = 0x11203;
        else
        if (compatVsn == 0x0B200400) //11.2.0.4
            oracleReader->version = 0x11204;
        else
        if (compatVsn == 0x0C100000) //12.1.0.0
            oracleReader->version = 0x12100;
        else
        if (compatVsn == 0x0C100100) //12.1.0.1
            oracleReader->version = 0x12101;
        else
        if (compatVsn == 0x0C100200) //12.1.0.2
            oracleReader->version = 0x12102;
        else
        if (compatVsn == 0x0C200100) //12.2.0.1
            oracleReader->version = 0x12201;
        else
        if (compatVsn == 0x12000000) //18.0.0.0
            oracleReader->version = 0x18000;
        else
        if (compatVsn == 0x12030000) //18.3.0.0
            oracleReader->version = 0x18300;
        else
        if (compatVsn == 0x12040000) //18.4.0.0
            oracleReader->version = 0x18400;
        else
        if (compatVsn == 0x12050000) //18.5.0.0
            oracleReader->version = 0x18500;
        else
        if (compatVsn == 0x12060000) //18.6.0.0
            oracleReader->version = 0x18600;
        else
        if (compatVsn == 0x12070000) //18.7.0.0
            oracleReader->version = 0x18700;
        else
        if (compatVsn == 0x12080000) //18.8.0.0
            oracleReader->version = 0x18800;
        else
        if (compatVsn == 0x12090000) //18.9.0.0
            oracleReader->version = 0x18900;
        else
        if (compatVsn == 0x120A0000) //18.10.0.0
            oracleReader->version = 0x18A00;
        else
        if (compatVsn == 0x13000000) //19.0.0.0
            oracleReader->version = 0x19000;
        else
        if (compatVsn == 0x13030000) //19.3.0.0
            oracleReader->version = 0x19300;
        else
        if (compatVsn == 0x13040000) //19.4.0.0
            oracleReader->version = 0x19400;
        else
        if (compatVsn == 0x13050000) //19.5.0.0
            oracleReader->version = 0x19500;
        else
        if (compatVsn == 0x13060000) //19.6.0.0
            oracleReader->version = 0x19600;
        else
        if (compatVsn == 0x13070000) //19.7.0.0
            oracleReader->version = 0x19700;
        else {
            cerr << "ERROR: Unsupported database version: " << hex << compatVsn << endl;
            return REDO_ERROR;
        }

        typeresetlogs resetlogsCnt = oracleReader->read32(oracleReader->headerBuffer + blockSize + 160);
        typescn firstScnHeader = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 180);
        typescn nextScnHeader = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 192);

        uint64_t ret = checkBlockHeader(oracleReader->headerBuffer + blockSize, 1);
        if (ret != REDO_OK)
            return ret;

        if (resetlogsCnt != oracleReader->resetlogs) {
            cerr << "ERROR: resetlogs id (" << dec << resetlogsCnt << ") for archived redo log does not match database information (" <<
                    oracleReader->resetlogs << "): " << path << endl;
            return REDO_ERROR;
        }

        if (firstScn == ZERO_SCN) {
            firstScn = firstScnHeader;
            nextScn = nextScnHeader;
        } else {
            if (firstScnHeader != firstScn) {
                //archive log incorrect sequence
                if (group == 0) {
                    cerr << "ERROR: first SCN (" << dec << firstScnHeader << ") for archived redo log does not match database information (" <<
                            firstScn << "): " << path << endl;
                    return REDO_ERROR;
                //redo log switch appeared and header is now overwritten
                } else {
                    if (oracleReader->trace >= TRACE_WARN)
                        cerr << "WARNING: first SCN (" << dec << firstScnHeader << ") for online redo log does not match database information (" <<
                                firstScn << "): " << path << endl;
                    return REDO_WRONG_SEQUENCE_SWITCHED;
                }
            }
        }

        //updating nextScn if changed
        if (nextScn == ZERO_SCN && nextScnHeader != ZERO_SCN) {
            if (oracleReader->trace >= TRACE_FULL)
                cerr << "FULL: updating next SCN to: " << dec << nextScnHeader << endl;
            nextScn = nextScnHeader;
        } else
        if (nextScn != ZERO_SCN && nextScnHeader != ZERO_SCN && nextScn != nextScnHeader) {
            cerr << "ERROR: next SCN (" << nextScn << ") does not match database information (" <<
                    nextScnHeader << "): " << path << endl;
            return REDO_ERROR;
        }

        char SID[9];
        memcpy(SID, oracleReader->headerBuffer + blockSize + 28, 8); SID[8] = 0;

        if (oracleReader->dumpRedoLog >= 1 && !headerInfoPrinted) {
            oracleReader->dumpStream << "DUMP OF REDO FROM FILE '" << path << "'" << endl;
            if (oracleReader->version >= 0x12200)
                oracleReader->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleReader->dumpStream << " Opcodes *.*" << endl;
            if (oracleReader->version >= 0x12200)
                oracleReader->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleReader->dumpStream << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff" << endl;
            if (oracleReader->version < 0x12200)
                oracleReader->dumpStream << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff" << endl;
            else
                oracleReader->dumpStream << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff" << endl;
            oracleReader->dumpStream << " Times: creation thru eternity" << endl;

            uint32_t dbid = oracleReader->read32(oracleReader->headerBuffer + blockSize + 24);
            uint32_t controlSeq = oracleReader->read32(oracleReader->headerBuffer + blockSize + 36);
            uint32_t fileSize = oracleReader->read32(oracleReader->headerBuffer + blockSize + 40);
            uint16_t fileNumber = oracleReader->read16(oracleReader->headerBuffer + blockSize + 48);
            uint32_t activationId = oracleReader->read32(oracleReader->headerBuffer + blockSize + 52);

            oracleReader->dumpStream << " FILE HEADER:" << endl <<
                    "\tCompatibility Vsn = " << dec << compatVsn << "=0x" << hex << compatVsn << endl <<
                    "\tDb ID=" << dec << dbid << "=0x" << hex << dbid << ", Db Name='" << SID << "'" << endl <<
                    "\tActivation ID=" << dec << activationId << "=0x" << hex << activationId << endl <<
                    "\tControl Seq=" << dec << controlSeq << "=0x" << hex << controlSeq << ", File size=" << dec << fileSize << "=0x" << hex << fileSize << endl <<
                    "\tFile Number=" << dec << fileNumber << ", Blksiz=" << dec << blockSize << ", File Type=2 LOG" << endl;

            typeseq seq = oracleReader->read32(oracleReader->headerBuffer + blockSize + 8);
            uint8_t descrip[65];
            memcpy (descrip, oracleReader->headerBuffer + blockSize + 92, 64); descrip[64] = 0;
            uint16_t thread = oracleReader->read16(oracleReader->headerBuffer + blockSize + 176);
            uint32_t nab = oracleReader->read32(oracleReader->headerBuffer + blockSize + 156);
            uint32_t hws = oracleReader->read32(oracleReader->headerBuffer + blockSize + 172);
            uint8_t eot = oracleReader->headerBuffer[blockSize + 204];
            uint8_t dis = oracleReader->headerBuffer[blockSize + 205];

            oracleReader->dumpStream << " descrip:\"" << descrip << "\"" << endl <<
                    " thread: " << dec << thread <<
                    " nab: 0x" << hex << nab <<
                    " seq: 0x" << setfill('0') << setw(8) << hex << (typeseq)seq <<
                    " hws: 0x" << hex << hws <<
                    " eot: " << dec << (uint64_t)eot <<
                    " dis: " << dec << (uint64_t)dis << endl;

            typescn resetlogsScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 164);
            typeresetlogs prevResetlogsCnt = oracleReader->read32(oracleReader->headerBuffer + blockSize + 292);
            typescn prevResetlogsScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 284);
            typetime firstTime(oracleReader->read32(oracleReader->headerBuffer + blockSize + 188));
            typetime nextTime(oracleReader->read32(oracleReader->headerBuffer + blockSize + 200));
            typescn enabledScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 208);
            typetime enabledTime(oracleReader->read32(oracleReader->headerBuffer + blockSize + 216));
            typescn threadClosedScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 220);
            typetime threadClosedTime(oracleReader->read32(oracleReader->headerBuffer + blockSize + 228));
            typescn termialRecScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 240);
            typetime termialRecTime(oracleReader->read32(oracleReader->headerBuffer + blockSize + 248));
            typescn mostRecentScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 260);
            typesum chSum = oracleReader->read16(oracleReader->headerBuffer + blockSize + 14);
            typesum chSum2 = calcChSum(oracleReader->headerBuffer + blockSize, blockSize);

            if (oracleReader->version < 0x12200) {
                oracleReader->dumpStream <<
                        " resetlogs count: 0x" << hex << resetlogsCnt << " scn: " << PRINTSCN48(resetlogsScn) << " (" << dec << resetlogsScn << ")" << endl <<
                        " prev resetlogs count: 0x" << hex << prevResetlogsCnt << " scn: " << PRINTSCN48(prevResetlogsScn) << " (" << dec << prevResetlogsScn << ")" << endl <<
                        " Low  scn: " << PRINTSCN48(firstScnHeader) << " (" << dec << firstScnHeader << ")" << " " << firstTime << endl <<
                        " Next scn: " << PRINTSCN48(nextScnHeader) << " (" << dec << nextScn << ")" << " " << nextTime << endl <<
                        " Enabled scn: " << PRINTSCN48(enabledScn) << " (" << dec << enabledScn << ")" << " " << enabledTime << endl <<
                        " Thread closed scn: " << PRINTSCN48(threadClosedScn) << " (" << dec << threadClosedScn << ")" << " " << threadClosedTime << endl <<
                        " Disk cksum: 0x" << hex << chSum << " Calc cksum: 0x" << hex << chSum2 << endl <<
                        " Terminal recovery stop scn: " << PRINTSCN48(termialRecScn) << endl <<
                        " Terminal recovery  " << termialRecTime << endl <<
                        " Most recent redo scn: " << PRINTSCN48(mostRecentScn) << endl;
            } else {
                typescn realNextScn = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 272);

                oracleReader->dumpStream <<
                        " resetlogs count: 0x" << hex << resetlogsCnt << " scn: " << PRINTSCN64(resetlogsScn) << endl <<
                        " prev resetlogs count: 0x" << hex << prevResetlogsCnt << " scn: " << PRINTSCN64(prevResetlogsScn) << endl <<
                        " Low  scn: " << PRINTSCN64(firstScnHeader) << " " << firstTime << endl <<
                        " Next scn: " << PRINTSCN64(nextScnHeader) << " " << nextTime << endl <<
                        " Enabled scn: " << PRINTSCN64(enabledScn) << " " << enabledTime << endl <<
                        " Thread closed scn: " << PRINTSCN64(threadClosedScn) << " " << threadClosedTime << endl <<
                        " Real next scn: " << PRINTSCN64(realNextScn) << endl <<
                        " Disk cksum: 0x" << hex << chSum << " Calc cksum: 0x" << hex << chSum2 << endl <<
                        " Terminal recovery stop scn: " << PRINTSCN64(termialRecScn) << endl <<
                        " Terminal recovery  " << termialRecTime << endl <<
                        " Most recent redo scn: " << PRINTSCN64(mostRecentScn) << endl;
            }

            uint32_t largestLwn = oracleReader->read32(oracleReader->headerBuffer + blockSize + 268);
            oracleReader->dumpStream <<
                    " Largest LWN: " << dec << largestLwn << " blocks" << endl;

            uint32_t miscFlags = oracleReader->read32(oracleReader->headerBuffer + blockSize + 236);
            string endOfRedo;
            if ((miscFlags & REDO_END) != 0)
                endOfRedo = "Yes";
            else
                endOfRedo = "No";
            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleReader->dumpStream << " FailOver End-of-redo stream : " << endOfRedo << endl;
            else
                oracleReader->dumpStream << " End-of-redo stream : " << endOfRedo << endl;

            if ((miscFlags & REDO_ASYNC) != 0)
                oracleReader->dumpStream << " Archivelog created using asynchronous network transmittal" << endl;

            if ((miscFlags & REDO_NODATALOSS) != 0)
                oracleReader->dumpStream << " No data-loss mode" << endl;

            if ((miscFlags & REDO_RESYNC) != 0)
                oracleReader->dumpStream << " Resynchronization mode" << endl;
            else
                oracleReader->dumpStream << " Unprotected mode" << endl;

            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleReader->dumpStream << " Closed thread archival" << endl;

            if ((miscFlags & REDO_MAXPERFORMANCE) != 0)
                oracleReader->dumpStream << " Maximize performance mode" << endl;

            oracleReader->dumpStream << " Miscellaneous flags: 0x" << hex << miscFlags << endl;

            if (oracleReader->version >= 0x12200) {
                uint32_t miscFlags2 = oracleReader->read32(oracleReader->headerBuffer + blockSize + 296);
                oracleReader->dumpStream << " Miscellaneous second flags: 0x" << hex << miscFlags2 << endl;
            }

            int32_t thr = (int32_t)oracleReader->read32(oracleReader->headerBuffer + blockSize + 432);
            int32_t seq2 = (int32_t)oracleReader->read32(oracleReader->headerBuffer + blockSize + 436);
            typescn scn2 = oracleReader->readSCN(oracleReader->headerBuffer + blockSize + 440);
            uint8_t zeroBlocks = oracleReader->headerBuffer[blockSize + 206];
            uint8_t formatId = oracleReader->headerBuffer[blockSize + 207];
            if (oracleReader->version < 0x12200)
                oracleReader->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN48(scn2) << endl <<
                        " Zero blocks: " << dec << (uint64_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint64_t)formatId << endl;
            else
                oracleReader->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN64(scn2) << endl <<
                        " Zero blocks: " << dec << (uint64_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint64_t)formatId << endl;

            uint32_t standbyApplyDelay = oracleReader->read32(oracleReader->headerBuffer + blockSize + 280);
            if (standbyApplyDelay > 0)
                oracleReader->dumpStream << " Standby Apply Delay: " << dec << standbyApplyDelay << " minute(s) " << endl;

            typetime standbyLogCloseTime(oracleReader->read32(oracleReader->headerBuffer + blockSize + 304));
            if (standbyLogCloseTime.getVal() > 0)
                oracleReader->dumpStream << " Standby Log Close Time:  " << standbyLogCloseTime << endl;

            oracleReader->dumpStream << " redo log key is ";
            for (uint64_t i = 448; i < 448 + 16; ++i)
                oracleReader->dumpStream << setfill('0') << setw(2) << hex << (uint64_t)oracleReader->headerBuffer[blockSize + i];
            oracleReader->dumpStream << endl;

            uint16_t redoKeyFlag = oracleReader->read16(oracleReader->headerBuffer + blockSize + 480);
            oracleReader->dumpStream << " redo log key flag is " << dec << redoKeyFlag << endl;
            uint16_t enabledRedoThreads = 1; //FIXME
            oracleReader->dumpStream << " Enabled redo threads: " << dec << enabledRedoThreads << " " << endl;

            headerInfoPrinted = true;
        }

        return ret;
    }

    void OracleReaderRedo::initFile() {
        if (fileDes > 0)
            return;

        fileDes = open(path.c_str(), O_RDONLY | O_LARGEFILE | ((oracleReader->directRead > 0) ? O_DIRECT : 0));
        if (fileDes == -1) {
            cerr << "ERROR: can not open: " << path << endl;
            throw RedoLogException("eror reading file", nullptr, 0);
        }
    }

    uint64_t OracleReaderRedo::readFile() {
        int64_t curBytesRead;
        if (redoBufferPos == DISK_BUFFER_SIZE)
            redoBufferPos = 0;

        if (lastReadSuccessfull && lastBytesRead * 2 < DISK_BUFFER_SIZE)
            lastBytesRead *= 2;
        curBytesRead = lastBytesRead;
        if (redoBufferPos == DISK_BUFFER_SIZE)
            redoBufferPos = 0;

        if (redoBufferPos + curBytesRead > DISK_BUFFER_SIZE)
            curBytesRead = DISK_BUFFER_SIZE - redoBufferPos;

        int64_t bytes = pread(fileDes, oracleReader->redoBuffer + redoBufferPos, curBytesRead, redoBufferFileStart);

        if (bytes < ((int64_t)curBytesRead)) {
            lastReadSuccessfull = false;
            lastBytesRead = READ_CHUNK_MIN_SIZE;
        } else
            lastReadSuccessfull = true;

        if ((oracleReader->trace2 & TRACE2_DISK) != 0)
            cerr << "DISK: read file: " << dec << fileDes << ", pos: " << redoBufferPos << ", seek: " << redoBufferFileStart << ", bytes: " << curBytesRead << ", got:" << bytes << endl;

        if (bytes > 0) {
            typeblk maxNumBlock = bytes / blockSize;

            for (uint64_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                uint64_t ret = checkBlockHeader(oracleReader->redoBuffer + redoBufferPos + numBlock * blockSize, blockNumber + numBlock);

                if (redoBufferFileStart < redoBufferFileEnd && (ret == REDO_WRONG_SEQUENCE_SWITCHED || ret == REDO_EMPTY)) {
                    lastReadSuccessfull = false;
                    lastBytesRead = READ_CHUNK_MIN_SIZE;
                    return REDO_OK;
                }

                if (ret != REDO_OK) {
                    lastReadSuccessfull = false;
                    lastBytesRead = READ_CHUNK_MIN_SIZE;
                    return ret;
                }

                redoBufferFileEnd += blockSize;
            }
        }
        return REDO_OK;
    }

    void OracleReaderRedo::analyzeRecord() {
        RedoLogRecord redoLogRecord[VECTOR_MAX_LENGTH];
        OpCode *opCodes[VECTOR_MAX_LENGTH];
        uint64_t isUndoRedo[VECTOR_MAX_LENGTH];
        uint64_t vectors = 0;
        uint64_t opCodesUndo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsUndo = 0;
        uint64_t opCodesRedo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsRedo = 0;

        uint64_t recordLength = oracleReader->read32(oracleReader->recordBuffer);
        uint8_t vld = oracleReader->recordBuffer[4];
        curScnPrev = curScn;
        curScn = oracleReader->read32(oracleReader->recordBuffer + 8) |
                ((uint64_t)(oracleReader->read16(oracleReader->recordBuffer + 6)) << 32);
        curSubScn = oracleReader->read16(oracleReader->recordBuffer + 12);
        uint64_t headerLength;
        uint16_t numChk = 0, numChkMax = 0;

        if (extScn > lastCheckpointScn && curScnPrev != curScn && curScnPrev != ZERO_SCN)
            flushTransactions(extScn);

        if ((vld & 0x04) != 0) {
            headerLength = 68;
            numChk = oracleReader->read32(oracleReader->recordBuffer + 24);
            numChkMax = oracleReader->read32(oracleReader->recordBuffer + 26);
            recordTimestmap = oracleReader->read32(oracleReader->recordBuffer + 64);
            if (numChk + 1 == numChkMax) {
                extScn = oracleReader->readSCN(oracleReader->recordBuffer + 40);
            }
            if (oracleReader->trace >= TRACE_FULL) {
                if (oracleReader->version < 0x12200)
                    cerr << "FULL: C scn: " << PRINTSCN48(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << " CHECKPOINT at " <<
                    PRINTSCN48(extScn) << endl;
                else
                    cerr << "FULL: C scn: " << PRINTSCN64(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << " CHECKPOINT at " <<
                    PRINTSCN64(extScn) << endl;
            }
        } else {
            headerLength = 24;
            if (oracleReader->trace >= TRACE_FULL) {
                if (oracleReader->version < 0x12200)
                    cerr << "FULL:   scn: " << PRINTSCN48(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << endl;
                else
                    cerr << "FULL:   scn: " << PRINTSCN64(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << endl;
            }
        }

        if (oracleReader->dumpRedoLog >= 1) {
            uint16_t thread = 1; //FIXME
            oracleReader->dumpStream << " " << endl;

            if (oracleReader->version < 0x12100)
                oracleReader->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld << endl;
            else {
                uint32_t conUid = oracleReader->read32(oracleReader->recordBuffer + 16);
                oracleReader->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld <<
                        " CON_UID: " << dec << conUid << endl;
            }

            if (oracleReader->dumpRawData > 0) {
                oracleReader->dumpStream << "##: " << dec << recordLength;
                for (uint64_t j = 0; j < headerLength; ++j) {
                    if ((j & 0x0F) == 0)
                        oracleReader->dumpStream << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
                    if ((j & 0x07) == 0)
                        oracleReader->dumpStream << " ";
                    oracleReader->dumpStream << setfill('0') << setw(2) << hex << (uint64_t)oracleReader->recordBuffer[j] << " ";
                }
                oracleReader->dumpStream << endl;
            }

            if (headerLength == 68) {
                if (oracleReader->version < 0x12200)
                    oracleReader->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
                else
                    oracleReader->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
                uint32_t nst = 1; //FIXME
                uint32_t lwnLen = oracleReader->read32(oracleReader->recordBuffer + 28);

                if (oracleReader->version < 0x12200)
                    oracleReader->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: " << setfill('0') << setw(4) << dec << lwnLen <<
                        " NST: " << setfill('0') << setw(4) << dec << nst <<
                        " SCN: " << PRINTSCN48(extScn) << ")" << endl;
                else
                    oracleReader->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(8) << hex << lwnLen <<
                        " NST: 0x" << setfill('0') << setw(4) << hex << nst <<
                        " SCN: " << PRINTSCN64(extScn) << ")" << endl;
            } else {
                if (oracleReader->version < 0x12200)
                    oracleReader->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
                else
                    oracleReader->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
            }
        }

        if (headerLength > recordLength)
            throw RedoLogException("too small log record: ", path.c_str(), recordLength);

        uint64_t pos = headerLength;
        while (pos < recordLength) {
            memset(&redoLogRecord[vectors], 0, sizeof(struct RedoLogRecord));
            redoLogRecord[vectors].vectorNo = vectors + 1;
            redoLogRecord[vectors].cls = oracleReader->read16(oracleReader->recordBuffer + pos + 2);
            redoLogRecord[vectors].afn = oracleReader->read16(oracleReader->recordBuffer + pos + 4);
            redoLogRecord[vectors].dba = oracleReader->read32(oracleReader->recordBuffer + pos + 8);
            redoLogRecord[vectors].scnRecord = oracleReader->readSCN(oracleReader->recordBuffer + pos + 12);
            redoLogRecord[vectors].rbl = 0; //FIXME
            redoLogRecord[vectors].seq = oracleReader->recordBuffer[pos + 20];
            redoLogRecord[vectors].typ = oracleReader->recordBuffer[pos + 21];
            int16_t usn = (redoLogRecord[vectors].cls >= 15) ? (redoLogRecord[vectors].cls - 15) / 2 : -1;

            uint64_t fieldOffset;
            if (oracleReader->version >= 0x12100) {
                fieldOffset = 32;
                redoLogRecord[vectors].flgRecord = oracleReader->read16(oracleReader->recordBuffer + pos + 28);
                redoLogRecord[vectors].conId = oracleReader->read32(oracleReader->recordBuffer + pos + 24);
            } else {
                fieldOffset = 24;
                redoLogRecord[vectors].flgRecord = 0;
                redoLogRecord[vectors].conId = 0;
            }

            if (pos + fieldOffset + 1 >= recordLength)
                throw RedoLogException("position of field list outside of record: ", nullptr, pos + fieldOffset);

            uint8_t *fieldList = oracleReader->recordBuffer + pos + fieldOffset;

            redoLogRecord[vectors].opCode = (((typeop1)oracleReader->recordBuffer[pos + 0]) << 8) |
                    oracleReader->recordBuffer[pos + 1];
            redoLogRecord[vectors].length = fieldOffset + ((oracleReader->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectors].scn = curScn;
            redoLogRecord[vectors].subScn = curSubScn;
            redoLogRecord[vectors].usn = usn;
            redoLogRecord[vectors].data = oracleReader->recordBuffer + pos;
            redoLogRecord[vectors].fieldLengthsDelta = fieldOffset;
            redoLogRecord[vectors].fieldCnt = (oracleReader->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) - 2) / 2;
            redoLogRecord[vectors].fieldPos = fieldOffset + ((oracleReader->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) + 2) & 0xFFFC);

            uint64_t fieldPos = redoLogRecord[vectors].fieldPos;
            for (uint64_t i = 1; i <= redoLogRecord[vectors].fieldCnt; ++i) {
                redoLogRecord[vectors].length += (oracleReader->read16(fieldList + i * 2) + 3) & 0xFFFC;
                fieldPos += (oracleReader->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta + i * 2) + 3) & 0xFFFC;
                if (pos + redoLogRecord[vectors].length > recordLength)
                    throw RedoLogException("position of field list outside of record: ", nullptr, pos + redoLogRecord[vectors].length);
            }

            if (redoLogRecord[vectors].fieldPos > redoLogRecord[vectors].length)
                throw RedoLogException("incomplete record", nullptr, 0);

            redoLogRecord[vectors].recordObjn = 0xFFFFFFFF;
            redoLogRecord[vectors].recordObjd = 0xFFFFFFFF;

            pos += redoLogRecord[vectors].length;

            switch (redoLogRecord[vectors].opCode) {
            case 0x0501: //Undo
                opCodes[vectors] = new OpCode0501(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0502: //Begin transaction
                opCodes[vectors] = new OpCode0502(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0504: //Commit/rollback transaction
                opCodes[vectors] = new OpCode0504(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0506: //Partial rollback
                opCodes[vectors] = new OpCode0506(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x050B:
                opCodes[vectors] = new OpCode050B(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0513: //Session information
                opCodes[vectors] = new OpCode0513(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0514: //Session information
                opCodes[vectors] = new OpCode0514(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B02: //REDO: Insert row piece
                opCodes[vectors] = new OpCode0B02(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B03: //REDO: Delete row piece
                opCodes[vectors] = new OpCode0B03(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B04: //REDO: Lock row piece
                opCodes[vectors] = new OpCode0B04(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B05: //REDO: Update row piece
                opCodes[vectors] = new OpCode0B05(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B06: //REDO: Overwrite row piece
                opCodes[vectors] = new OpCode0B06(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B08: //REDO: Change forwarding address
                opCodes[vectors] = new OpCode0B08(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B0B: //REDO: Insert multiple rows
                opCodes[vectors] = new OpCode0B0B(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x0B0C: //REDO: Delete multiple rows
                opCodes[vectors] = new OpCode0B0C(oracleReader, &redoLogRecord[vectors]);
                break;
            case 0x1801: //DDL
                opCodes[vectors] = new OpCode1801(oracleReader, &redoLogRecord[vectors]);
                break;
            default:
                opCodes[vectors] = new OpCode(oracleReader, &redoLogRecord[vectors]);
                break;
            }

            isUndoRedo[vectors] = 0;
            //UNDO
            if (redoLogRecord[vectors].opCode == 0x0501
                    || redoLogRecord[vectors].opCode == 0x0506
                    || redoLogRecord[vectors].opCode == 0x050B) {
                opCodesUndo[vectorsUndo++] = vectors;
                isUndoRedo[vectors] = 1;
                if (vectorsUndo <= vectorsRedo) {
                    redoLogRecord[opCodesRedo[vectorsUndo - 1]].recordObjd = redoLogRecord[opCodesUndo[vectorsUndo - 1]].objd;
                    redoLogRecord[opCodesRedo[vectorsUndo - 1]].recordObjn = redoLogRecord[opCodesUndo[vectorsUndo - 1]].objn;
                }
            //REDO
            } else if ((redoLogRecord[vectors].opCode & 0xFF00) == 0x0A00 ||
                    (redoLogRecord[vectors].opCode & 0xFF00) == 0x0B00) {
                opCodesRedo[vectorsRedo++] = vectors;
                isUndoRedo[vectors] = 2;
                if (vectorsRedo <= vectorsUndo) {
                    redoLogRecord[opCodesRedo[vectorsRedo - 1]].recordObjd = redoLogRecord[opCodesUndo[vectorsRedo - 1]].objd;
                    redoLogRecord[opCodesRedo[vectorsRedo - 1]].recordObjn = redoLogRecord[opCodesUndo[vectorsRedo - 1]].objn;
                }
            }

            ++vectors;
        }

        for (uint64_t i = 0; i < vectors; ++i) {
            opCodes[i]->process();
            delete opCodes[i];
            opCodes[i] = nullptr;
        }

        for (uint64_t i = 0; i < vectors; ++i) {
            //begin transaction
            if (redoLogRecord[i].opCode == 0x0502) {
            }
        }

        uint64_t iPair = 0;
        for (uint64_t i = 0; i < vectors; ++i) {
            //begin transaction
            if (redoLogRecord[i].opCode == 0x0502) {
                if (SQN(redoLogRecord[i].xid) > 0)
                    appendToTransaction(&redoLogRecord[i]);

                //commit/rollback transaction
            } else if (redoLogRecord[i].opCode == 0x0504) {
                appendToTransaction(&redoLogRecord[i]);

            //ddl, etc.
            } else if (isUndoRedo[i] == 0) {
                appendToTransaction(&redoLogRecord[i]);
            } else if (iPair < vectorsUndo) {
                if (opCodesUndo[iPair] == i) {
                    if (iPair < vectorsRedo)
                        appendToTransaction(&redoLogRecord[opCodesUndo[iPair]], &redoLogRecord[opCodesRedo[iPair]]);
                    else
                        appendToTransaction(&redoLogRecord[opCodesUndo[iPair]]);
                    ++iPair;
                } else if (opCodesRedo[iPair] == i) {
                    if (iPair < vectorsUndo)
                        appendToTransaction(&redoLogRecord[opCodesRedo[iPair]], &redoLogRecord[opCodesUndo[iPair]]);
                    else
                        appendToTransaction(&redoLogRecord[opCodesRedo[iPair]]);
                    ++iPair;
                }
            }
        }

        for (uint64_t i = 0; i < vectors; ++i) {
            //commit transaction
            if (redoLogRecord[i].opCode == 0x0504) {
            }
        }
    }

    void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord) {
        if (oracleReader->trace >= TRACE_FULL) {
            cerr << "FULL: ";
            redoLogRecord->dump(oracleReader);
            cerr << endl;
        }

        //skip other PDB vectors
        if (redoLogRecord->conId > 1 && redoLogRecord->conId != oracleReader->conId)
            return;

        //DDL or part of multi-block UNDO
        if (redoLogRecord->opCode == 0x1801 || redoLogRecord->opCode == 0x0501) {
            if (redoLogRecord->opCode == 0x0501) {
                if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0) {
                    return;
                }
                if ((oracleReader->trace2 & TRACE2_DUMP) != 0)
                    cerr << "DUMP: merging Multi-block" << endl;
            }

            RedoLogRecord zero;
            memset(&zero, 0, sizeof(struct RedoLogRecord));

            redoLogRecord->object = oracleReader->checkDict(redoLogRecord->objn, redoLogRecord->objd);
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0 || (redoLogRecord->object->altered && redoLogRecord->opCode != 0x01801))
                return;

            Transaction *transaction = oracleReader->xidTransactionMap[redoLogRecord->xid];
            if (transaction == nullptr) {
                if (oracleReader->trace >= TRACE_DETAIL)
                    cerr << "ERROR: transaction missing" << endl;

                transaction = new Transaction(oracleReader, redoLogRecord->xid, oracleReader->transactionBuffer);
                transaction->add(oracleReader, redoLogRecord->objn, redoLogRecord->objd, redoLogRecord->uba, redoLogRecord->dba, redoLogRecord->slt,
                        redoLogRecord->rci, redoLogRecord, &zero, oracleReader->transactionBuffer, sequence);
                oracleReader->xidTransactionMap[redoLogRecord->xid] = transaction;
                oracleReader->transactionHeap.add(transaction);
            } else {
                if (transaction->opCodes > 0)
                    oracleReader->lastOpTransactionMap.erase(transaction);
                transaction->add(oracleReader, redoLogRecord->objn, redoLogRecord->objd, redoLogRecord->uba, redoLogRecord->dba, redoLogRecord->slt,
                        redoLogRecord->rci, redoLogRecord, &zero, oracleReader->transactionBuffer, sequence);
                oracleReader->transactionHeap.update(transaction->pos);
            }
            transaction->lastUba = redoLogRecord->uba;
            transaction->lastDba = redoLogRecord->dba;
            transaction->lastSlt = redoLogRecord->slt;
            transaction->lastRci = redoLogRecord->rci;

            oracleReader->lastOpTransactionMap.set(transaction);
            oracleReader->transactionHeap.update(transaction->pos);

            return;
        } else
        if (redoLogRecord->opCode != 0x0502 && redoLogRecord->opCode != 0x0504)
            return;

        Transaction *transaction = oracleReader->xidTransactionMap[redoLogRecord->xid];
        if (transaction == nullptr) {
            transaction = new Transaction(oracleReader, redoLogRecord->xid, oracleReader->transactionBuffer);
            transaction->touch(curScn, sequence);
            oracleReader->xidTransactionMap[redoLogRecord->xid] = transaction;
            oracleReader->transactionHeap.add(transaction);
        } else
            transaction->touch(curScn, sequence);

        if (redoLogRecord->opCode == 0x0502) {
            transaction->isBegin = true;
        }

        if (redoLogRecord->opCode == 0x0504) {
            transaction->isCommit = true;
            transaction->commitTime = recordTimestmap;
            if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
                transaction->isRollback = true;
            oracleReader->transactionHeap.update(transaction->pos);
        }
    }

    void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        bool isShutdown = false;
        if (oracleReader->trace >= TRACE_FULL) {
            cerr << "FULL: ";
            redoLogRecord1->dump(oracleReader);
            cerr << " (1)" << endl;
            cerr << "FULL: ";
            redoLogRecord2->dump(oracleReader);
            cerr << " (2)" << endl;
        }

        //skip other PDB vectors
        if (redoLogRecord1->conId > 1 && redoLogRecord1->conId != oracleReader->conId)
            return;
        if (redoLogRecord2->conId > 1 && redoLogRecord2->conId != oracleReader->conId)
            return;

        typeobj objn, objd;
        if (redoLogRecord1->objd != 0) {
            objn = redoLogRecord1->objn;
            objd = redoLogRecord1->objd;
        } else {
            objn = redoLogRecord2->objn;
            objd = redoLogRecord2->objd;
        }

        if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0) {
            cerr << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            if (oracleReader->dumpRedoLog >= 1)
                oracleReader->dumpStream << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            return;
        }

        redoLogRecord1->object = oracleReader->checkDict(objn, objd);
        if (redoLogRecord1->object == nullptr || redoLogRecord1->object->altered)
            return;

        redoLogRecord2->object = redoLogRecord1->object;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        if (redoLogRecord1->object->options == 1 && opCodeLong == 0x05010B02) {
            if (oracleReader->trace >= TRACE_DETAIL)
                cerr << "INFO: Exiting on user request" << endl;
            isShutdown = true;
        }

        switch (opCodeLong) {
        //insert row piece
        case 0x05010B02:
        //delete row piece
        case 0x05010B03:
        //update row piece
        case 0x05010B05:
        //overwrite row piece
        case 0x05010B06:
        //change forwarding address
        case 0x05010B08:
        //insert multiple rows
        case 0x05010B0B:
        //delete multiple rows
        case 0x05010B0C:
            {
                Transaction *transaction = oracleReader->xidTransactionMap[redoLogRecord1->xid];
                if (transaction == nullptr) {
                    transaction = new Transaction(oracleReader, redoLogRecord1->xid, oracleReader->transactionBuffer);
                    transaction->add(oracleReader, objn, objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, oracleReader->transactionBuffer, sequence);
                    oracleReader->xidTransactionMap[redoLogRecord1->xid] = transaction;
                    oracleReader->transactionHeap.add(transaction);
                } else {
                    if (transaction->opCodes > 0)
                        oracleReader->lastOpTransactionMap.erase(transaction);

                    transaction->add(oracleReader, objn, objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, oracleReader->transactionBuffer, sequence);
                    oracleReader->transactionHeap.update(transaction->pos);
                }
                transaction->lastUba = redoLogRecord1->uba;
                transaction->lastDba = redoLogRecord1->dba;
                transaction->lastSlt = redoLogRecord1->slt;
                transaction->lastRci = redoLogRecord1->rci;
                transaction->isShutdown = isShutdown;

                oracleReader->lastOpTransactionMap.set(transaction);
                oracleReader->transactionHeap.update(transaction->pos);
            }
            break;

        //rollback: delete row piece
        case 0x0B030506:
        case 0x0B03050B:
        //rollback: delete multiple rows
        case 0x0B0C0506:
        case 0x0B0C050B:
        //rollback: insert row piece
        case 0x0B020506:
        case 0x0B02050B:
        //rollback: insert multiple row
        case 0x0B0B0506:
        case 0x0B0B050B:
        //rollback: update row piece
        case 0x0B050506:
        case 0x0B05050B:
        //rollback: overwrite row piece
        case 0x0B060506:
        case 0x0B06050B:
            {
                Transaction *transaction = oracleReader->lastOpTransactionMap.getMatch(redoLogRecord1->uba,
                        redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci);

                //match
                if (transaction != nullptr) {
                    oracleReader->lastOpTransactionMap.erase(transaction);
                    transaction->rollbackLastOp(oracleReader, curScn, oracleReader->transactionBuffer);
                    oracleReader->transactionHeap.update(transaction->pos);
                    oracleReader->lastOpTransactionMap.set(transaction);

                } else {
                    //check all previous transactions - not yet implemented
                    bool foundPrevious = false;

                    for (uint64_t i = 1; i <= oracleReader->transactionHeap.heapSize; ++i) {
                        transaction = oracleReader->transactionHeap.heap[i];

                        if (transaction->opCodes > 0 &&
                                transaction->rollbackPreviousOp(oracleReader, curScn, oracleReader->transactionBuffer, redoLogRecord1->uba,
                                redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci)) {
                            oracleReader->transactionHeap.update(transaction->pos);
                            foundPrevious = true;
                            break;
                        }
                    }
                    if (!foundPrevious) {
                        if (oracleReader->trace >= TRACE_WARN)
                            cerr << "WARNING: can't rollback transaction part, UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " DBA: " << hex << redoLogRecord2->dba <<
                                    " SLT: " << dec << (uint64_t)redoLogRecord2->slt <<
                                    " RCI: " << dec << (uint64_t)redoLogRecord2->rci << endl;
                    } else {
                        if (oracleReader->trace >= TRACE_WARN)
                            cerr << "WARNING: would like to rollback transaction part, UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " DBA: " << hex << redoLogRecord2->dba <<
                                    " SLT: " << dec << (uint64_t)redoLogRecord2->slt <<
                                    " RCI: " << dec << (uint64_t)redoLogRecord2->rci << endl;
                    }
                }
            }

            break;
        }
    }

    void OracleReaderRedo::flushTransactions(typescn checkpointScn) {
        bool isShutdown = false;
        Transaction *transaction = oracleReader->transactionHeap.top();
        if ((oracleReader->trace2 & TRACE2_CHECKPOINT_FLUSH) != 0) {
            cerr << "FLUSH" << endl;
            oracleReader->dumpTransactions();
        }

        while (transaction != nullptr) {
            if (oracleReader->trace >= TRACE_FULL)
                cerr << "FULL: " << *transaction << endl;

            if (transaction->lastScn <= checkpointScn && transaction->isCommit) {
                if (transaction->lastScn > oracleReader->databaseScn) {
                    if (transaction->isBegin)  {
                        if (transaction->isShutdown)
                            isShutdown = true;
                        else
                            transaction->flush(oracleReader);
                    } else {
                        if (oracleReader->trace >= TRACE_WARN) {
                            cerr << "WARNING: skipping transaction with no begin: " << *transaction << endl;
                            oracleReader->dumpTransactions();
                        }
                    }
                } else {
                    if (oracleReader->trace >= TRACE_DETAIL) {
                        cerr << "INFO: skipping transaction already committed: " << *transaction << endl;
                    }
                }

                oracleReader->transactionHeap.pop();
                if (transaction->opCodes > 0)
                    oracleReader->lastOpTransactionMap.erase(transaction);

                oracleReader->xidTransactionMap.erase(transaction->xid);
                if (oracleReader->trace >= TRACE_FULL)
                    cerr << "FULL: dropping" << endl;
                oracleReader->transactionBuffer->deleteTransactionChunks(transaction->tc, transaction->tcLast);
                delete transaction;

                transaction = oracleReader->transactionHeap.top();
            } else
                break;
        }

        if ((oracleReader->trace2 & TRACE2_DUMP) != 0) {
            for (auto const& xid : oracleReader->xidTransactionMap) {
                Transaction *transaction = oracleReader->xidTransactionMap[xid.first];
                cerr << "DUMP: " << *transaction << endl;
            }
        }

        if (checkpointScn > oracleReader->databaseScn) {
            if (oracleReader->trace >= TRACE_FULL) {
                if (oracleReader->version >= 0x12200)
                    cerr << "INFO: Updating checkpoint SCN to: " << PRINTSCN64(checkpointScn) << endl;
                else
                    cerr << "INFO: Updating checkpoint SCN to: " << PRINTSCN48(checkpointScn) << endl;
            }
            oracleReader->databaseScn = checkpointScn;
        }
        lastCheckpointScn = checkpointScn;

        if (isShutdown)
            stopMain();
    }

    uint64_t OracleReaderRedo::processBuffer(void) {
        while (redoBufferFileStart < redoBufferFileEnd) {
            uint64_t curBlockPos = 16;
            while (curBlockPos < blockSize) {
                //next record
                if (recordLeftToCopy == 0) {
                    if (curBlockPos + 20 >= blockSize)
                        break;

                    recordLeftToCopy = (oracleReader->read32(oracleReader->redoBuffer + redoBufferPos + curBlockPos) + 3) & 0xFFFFFFFC;
                    if (recordLeftToCopy > REDO_RECORD_MAX_SIZE) {
                        cerr << "WARNING: too big log record: " << dec << recordLeftToCopy << " bytes" << endl;
                        throw RedoLogException("too big log record: ", path.c_str(), recordLeftToCopy);
                    }

                    recordPos = 0;
                    recordBeginPos = curBlockPos;
                    recordBeginBlock = blockNumber;
                }

                //nothing more
                if (recordLeftToCopy == 0)
                    break;

                uint64_t toCopy;
                if (curBlockPos + recordLeftToCopy > blockSize)
                    toCopy = blockSize - curBlockPos;
                else
                    toCopy = recordLeftToCopy;

                memcpy(oracleReader->recordBuffer + recordPos, oracleReader->redoBuffer + redoBufferPos + curBlockPos, toCopy);
                recordLeftToCopy -= toCopy;
                curBlockPos += toCopy;
                recordPos += toCopy;

                if ((oracleReader->trace2 & TRACE2_DISK) != 0)
                    cerr << "DISK: block: " << dec << redoBufferFileStart << " pos: " << dec << recordPos << endl;

                if (recordLeftToCopy == 0)
                    analyzeRecord();
            }

            ++blockNumber;
            redoBufferPos += blockSize;
            redoBufferFileStart += blockSize;
        }
        return REDO_OK;
    }

    void OracleReaderRedo::reload() {
        firstScn = ZERO_SCN;
        nextScn = ZERO_SCN;
        sequence = 0;
        blockNumber = 0;
        lastCheckpointScn = 0;
        curScn = ZERO_SCN;
        recordTimestmap = 0;
        recordBeginPos = 0;
        recordBeginBlock = 0;
        recordPos = 0;
        recordLeftToCopy = 0;
        redoBufferPos = 0;
        redoBufferFileStart = 0;
        redoBufferFileEnd = 0;
        lastReadSuccessfull = false;
        lastBytesRead = READ_CHUNK_MIN_SIZE;

        initFile();
        checkRedoHeader();
    }

    void OracleReaderRedo::clone(OracleReaderRedo *redo) {
        blockNumber = redo->blockNumber;
        lastCheckpointScn = redo->lastCheckpointScn;
        curScn = redo->curScn;
        recordBeginPos = redo->recordBeginPos;
        recordBeginBlock = redo->recordBeginBlock;
        recordTimestmap = redo->recordTimestmap;
        recordPos = redo->recordPos;
        recordLeftToCopy = redo->recordLeftToCopy;
        redoBufferPos = redo->redoBufferPos;
        redoBufferFileStart = redo->redoBufferFileStart;
        redoBufferFileEnd = redo->redoBufferFileEnd;
        headerInfoPrinted = true;
    }

    uint64_t OracleReaderRedo::processLog() {
        cerr << "Processing log: " << *this;
        if (oracleReader->trace < TRACE_INFO)
            cerr << endl;

        if (oracleReader->dumpRedoLog >= 1 && redoBufferFileStart == 0) {
            stringstream name;
            name << "DUMP-" << sequence << ".trace";
            oracleReader->dumpStream.open(name.str());
            if (!oracleReader->dumpStream.is_open()) {
                cerr << "ERORR: can't open " << name.str() << " for write. Aborting log dump." << endl;
                oracleReader->dumpRedoLog = 0;
            }
        }
        clock_t cStart = clock();

        initFile();
        bool reachedEndOfOnlineRedo = false;
        uint64_t ret = checkRedoHeader();
        if (oracleReader->trace >= TRACE_INFO)
            cerr <<
                " err: " << dec << ret <<
                " block: " << dec << blockSize <<
                " version: " << hex << oracleReader->version <<
                " firstScn: " << PRINTSCN64(firstScn) <<
                " nextScn: " << PRINTSCN64(nextScn) << endl;

        if (ret != REDO_OK) {
            if (oracleReader->dumpRedoLog >= 1 && oracleReader->dumpStream.is_open())
                oracleReader->dumpStream.close();
            return ret;
        }

        if (redoBufferFileStart == 0) {
            redoBufferFileStart = blockSize * 2;
            redoBufferFileEnd = blockSize * 2;
            blockNumber = 2;
        }

        while (blockNumber <= numBlocks && !reachedEndOfOnlineRedo && !oracleReader->shutdown) {
            processBuffer();
            oracleReader->checkForCheckpoint();

            while (redoBufferFileStart == redoBufferFileEnd && blockNumber <= numBlocks && !reachedEndOfOnlineRedo
                    && !oracleReader->shutdown) {
                uint64_t ret = readFile();

                if (ret == REDO_OK && redoBufferFileStart < redoBufferFileEnd)
                    break;

                //for archive redo log break on all errors
                if (group == 0) {
                    if (oracleReader->dumpRedoLog >= 1 && oracleReader->dumpStream.is_open())
                        oracleReader->dumpStream.close();
                    return ret;

                //for online redo log
                } else {
                    if (ret == REDO_WRONG_SEQUENCE_SWITCHED) {
                        return ret;
                    }

                    if (ret != REDO_OK && ret != REDO_EMPTY) {
                        if (oracleReader->dumpRedoLog >= 1 && oracleReader->dumpStream.is_open())
                            oracleReader->dumpStream.close();
                        return ret;
                    }

                    //check if sequence has changed
                    uint64_t ret = checkRedoHeader();
                    if (ret != REDO_OK) {
                        if (oracleReader->dumpRedoLog >= 1 && oracleReader->dumpStream.is_open())
                            oracleReader->dumpStream.close();
                        return ret;
                    }

                    if (nextScn != ZERO_SCN) {
                        reachedEndOfOnlineRedo = true;
                        break;
                    }

                    if (curScn != ZERO_SCN)
                        flushTransactions(curScn);

                    if (oracleReader->shutdown)
                        break;

                    usleep(oracleReader->redoReadSleep);
                }
            }

            if (redoBufferFileStart == redoBufferFileEnd) {
                break;
            }
        }

        if (reachedEndOfOnlineRedo) {
            if (curScn != ZERO_SCN)
                flushTransactions(curScn);
        }

        if (fileDes > 0) {
            close(fileDes);
            fileDes = -1;
        }

        if ((oracleReader->trace2 & TRACE2_PERFORMANCE) != 0) {
            clock_t cEnd = clock();
            double mySpeed = 0, myTime = 1000.0 * (cEnd-cStart) / CLOCKS_PER_SEC;
            if (myTime > 0)
                mySpeed = (uint64_t)blockNumber * blockSize / 1024 / 1024 / myTime * 1000;
            cerr << "PERFORMANCE: Redo processing time: " << myTime << " ms Speed: " << fixed << setprecision(2) << mySpeed << " MB/s" << endl;
        }

        if (oracleReader->dumpRedoLog >= 1 && oracleReader->dumpStream.is_open())
            oracleReader->dumpStream.close();
        return REDO_OK;
    }

    typesum OracleReaderRedo::calcChSum(uint8_t *buffer, uint64_t size) {
        typesum oldChSum = oracleReader->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint64_t i = 0; i < size / 8; ++i, buffer += 8)
            sum ^= *((uint64_t*)buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }


    OracleReaderRedo::~OracleReaderRedo() {
    }

    ostream& operator<<(ostream& os, const OracleReaderRedo& ors) {
        os << "(" << dec << ors.group << ", " << ors.firstScn << ", " << ors.sequence << ", \"" << ors.path << "\")";
        return os;
    }

}
