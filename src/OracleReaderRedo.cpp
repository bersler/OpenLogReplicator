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
#include "OracleEnvironment.h"
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

namespace OpenLogReplicator {

    OracleReaderRedo::OracleReaderRedo(OracleEnvironment *oracleEnvironment, int group, typescn firstScn,
                typescn nextScn, typeseq sequence, const char* path) :
            oracleEnvironment(oracleEnvironment),
            group(group),
            lastCheckpointScn(0),
            curScn(ZERO_SCN),
            firstScn(firstScn),
            nextScn(nextScn),
            recordBeginPos(0),
            recordBeginBlock(0),
            recordTimestmap(0),
            recordObjn(0),
            recordObjd(0),
            blockSize(0),
            blockNumber(0),
            numBlocks(0),
            redoBufferPos(0),
            redoBufferFileStart(0),
            redoBufferFileEnd(0),
            recordPos(0),
            recordLeftToCopy(0),
            lastRead(READ_CHUNK_MIN_SIZE),
            headerBufferFileEnd(0),
            lastReadSuccessfull(false),
            redoOverwritten(false),
            lastCheckpointInfo(false),
            fileDes(-1),
            path(path),
            sequence(sequence) {
    }

    int OracleReaderRedo::checkBlockHeader(uint8_t *buffer, uint32_t blockNumberExpected) {
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_EMPTY;

        if (buffer[0] != 1 || buffer[1] != 0x22) {
            cerr << "ERROR: header bad magic number for block " << blockNumberExpected << endl;
            return REDO_ERROR;
        }

        uint32_t sequenceCheck = oracleEnvironment->read32(buffer + 8);

        if (sequence != sequenceCheck)
            return REDO_WRONG_SEQUENCE;

        uint32_t blockNumberCheck = oracleEnvironment->read32(buffer + 4);
        if (blockNumberCheck != blockNumberExpected) {
            cerr << "ERROR: header bad block number for " << blockNumberExpected << ", found: " << blockNumberCheck << endl;
            return REDO_ERROR;
        }

        return REDO_OK;
    }

    int OracleReaderRedo::checkRedoHeader(bool first) {
        headerBufferFileEnd = pread(fileDes, oracleEnvironment->headerBuffer, REDO_PAGE_SIZE_MAX * 2, 0);
        if (headerBufferFileEnd < REDO_PAGE_SIZE_MIN * 2) {
            cerr << "ERROR: unable to read redo header for " << path.c_str() << endl;
            return REDO_ERROR;
        }

        //check file header
        if (oracleEnvironment->headerBuffer[0] != 0 ||
                oracleEnvironment->headerBuffer[1] != 0x22 ||
                oracleEnvironment->headerBuffer[28] != 0x7D ||
                oracleEnvironment->headerBuffer[29] != 0x7C ||
                oracleEnvironment->headerBuffer[30] != 0x7B ||
                oracleEnvironment->headerBuffer[31] != 0x7A) {
            cerr << "[0]: " << hex << (uint32_t)oracleEnvironment->headerBuffer[0] << endl;
            cerr << "[1]: " << hex << (uint32_t)oracleEnvironment->headerBuffer[1] << endl;
            cerr << "[28]: " << hex << (uint32_t)oracleEnvironment->headerBuffer[28] << endl;
            cerr << "[29]: " << hex << (uint32_t)oracleEnvironment->headerBuffer[29] << endl;
            cerr << "[30]: " << hex << (uint32_t)oracleEnvironment->headerBuffer[30] << endl;
            cerr << "[31]: " << hex << (uint32_t)oracleEnvironment->headerBuffer[31] << endl;
            cerr << "ERROR: block header bad magic fields" << endl;
            return REDO_ERROR;
        }

        blockSize = oracleEnvironment->read16(oracleEnvironment->headerBuffer + 20);
        if (blockSize != 512 && blockSize != 1024) {
            cerr << "ERROR: unsupported block size: " << blockSize << endl;
            return REDO_ERROR;
        }

        //check first block
        if (headerBufferFileEnd < blockSize * 2) {
            cerr << "ERROR: unable to read redo header for " << path.c_str() << endl;
            return REDO_ERROR;
        }

        numBlocks = oracleEnvironment->read32(oracleEnvironment->headerBuffer + 24);
        uint32_t compatVsn = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 20);

        if (compatVsn == 0x0B200200) //11.2.0.2
            oracleEnvironment->version = 11202;
        else
        if (compatVsn == 0x0B200300) //11.2.0.3
            oracleEnvironment->version = 11203;
        else
        if (compatVsn == 0x0B200400) //11.2.0.4
            oracleEnvironment->version = 11204;
        else
        if (compatVsn == 0x0C100200) //12.1.0.2
            oracleEnvironment->version = 12102;
        else
        if (compatVsn == 0x0C200100) //12.2.0.1
            oracleEnvironment->version = 12201;
        else
        if (compatVsn == 0x12030000) //18.3.0.0
            oracleEnvironment->version = 18300;
        else
        if (compatVsn == 0x12040000) //18.4.0.0
            oracleEnvironment->version = 18400;
        else
        if (compatVsn == 0x12050000) //18.5.0.0
            oracleEnvironment->version = 18500;
        else
        if (compatVsn == 0x12060000) //18.6.0.0
            oracleEnvironment->version = 18600;
        else
        if (compatVsn == 0x12070000) //18.7.0.0
            oracleEnvironment->version = 18700;
        else
        if (compatVsn == 0x12080000) //18.8.0.0
            oracleEnvironment->version = 18800;
        else
        if (compatVsn == 0x12090000) //18.9.0.0
            oracleEnvironment->version = 18900;
        else
        if (compatVsn == 0x13030000) //19.3.0.0
            oracleEnvironment->version = 19300;
        else
        if (compatVsn == 0x13040000) //19.4.0.0
            oracleEnvironment->version = 19400;
        else
        if (compatVsn == 0x13050000) //19.5.0.0
            oracleEnvironment->version = 19500;
        else
        if (compatVsn == 0x13060000) //19.6.0.0
            oracleEnvironment->version = 19600;
        else {
            cerr << "ERROR: Unsupported database version: " << hex << compatVsn << endl;
            return REDO_ERROR;
        }

        typescn firstScnHeader = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 180);
        typescn nextScnHeader = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 192);

        int ret = checkBlockHeader(oracleEnvironment->headerBuffer + blockSize, 1);
        if (ret == REDO_ERROR) {
            cerr << "ERROR: bad header" << endl;
            return ret;
        }

        if (firstScnHeader != firstScn) {
            //archive log incorrect sequence
            if (group == 0) {
                cerr << "ERROR: first SCN (" << dec << firstScnHeader << ") does not match database information (" <<
                        firstScn << "): " << path.c_str() << endl;
                return REDO_ERROR;
            //redo log switch appeared and header is now overwritten
            } else {
                if (oracleEnvironment->trace >= TRACE_WARN)
                    cerr << "WARNING: first SCN (" << firstScnHeader << ") does not match database information (" <<
                            firstScn << "): " << path.c_str() << endl;
                return REDO_WRONG_SEQUENCE_SWITCHED;
            }
        }

        //updating nextScn if changed
        if (nextScn == ZERO_SCN && nextScnHeader != ZERO_SCN) {
            if (oracleEnvironment->trace >= TRACE_INFO)
                cerr << "Log switch to " << dec << nextScnHeader << endl;
            nextScn = nextScnHeader;
        } else
        if (nextScn != ZERO_SCN && nextScnHeader != ZERO_SCN && nextScn != nextScnHeader) {
            cerr << "ERROR: next SCN (" << nextScn << ") does not match database information (" <<
                    nextScnHeader << "): " << path.c_str() << endl;
            return REDO_ERROR;
        }

        memcpy(SID, oracleEnvironment->headerBuffer + blockSize + 28, 8); SID[8] = 0;

        if (oracleEnvironment->dumpLogFile >= 1 && first) {
            oracleEnvironment->dumpStream << "DUMP OF REDO FROM FILE '" << path << "'" << endl;
            if (oracleEnvironment->version >= 12200)
                oracleEnvironment->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleEnvironment->dumpStream << " Opcodes *.*" << endl;
            if (oracleEnvironment->version >= 12200)
                oracleEnvironment->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleEnvironment->dumpStream << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff" << endl;
            if (oracleEnvironment->version < 12200)
                oracleEnvironment->dumpStream << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff" << endl;
            else
                oracleEnvironment->dumpStream << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff" << endl;
            oracleEnvironment->dumpStream << " Times: creation thru eternity" << endl;

            uint32_t dbid = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 24);
            uint32_t controlSeq = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 36);
            uint32_t fileSize = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 40);
            uint16_t fileNumber = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 48);
            uint32_t activationId = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 52);

            oracleEnvironment->dumpStream << " FILE HEADER:" << endl <<
                    "\tCompatibility Vsn = " << dec << compatVsn << "=0x" << hex << compatVsn << endl <<
                    "\tDb ID=" << dec << dbid << "=0x" << hex << dbid << ", Db Name='" << SID << "'" << endl <<
                    "\tActivation ID=" << dec << activationId << "=0x" << hex << activationId << endl <<
                    "\tControl Seq=" << dec << controlSeq << "=0x" << hex << controlSeq << ", File size=" << dec << fileSize << "=0x" << hex << fileSize << endl <<
                    "\tFile Number=" << dec << fileNumber << ", Blksiz=" << dec << blockSize << ", File Type=2 LOG" << endl;

            uint32_t seq = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 8);
            uint8_t descrip[65];
            memcpy (descrip, oracleEnvironment->headerBuffer + blockSize + 92, 64); descrip[64] = 0;
            uint16_t thread = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 176);
            uint32_t nab = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 156);
            uint32_t hws = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 172);
            uint8_t eot = oracleEnvironment->headerBuffer[blockSize + 204];
            uint8_t dis = oracleEnvironment->headerBuffer[blockSize + 205];

            oracleEnvironment->dumpStream << " descrip:\"" << descrip << "\"" << endl <<
                    " thread: " << dec << thread <<
                    " nab: 0x" << hex << nab <<
                    " seq: 0x" << setfill('0') << setw(8) << hex << (uint32_t)seq <<
                    " hws: 0x" << hex << (uint32_t)hws <<
                    " eot: " << dec << (uint32_t)eot <<
                    " dis: " << dec << (uint32_t)dis << endl;

            uint32_t resetlogsCnt = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 160);
            typescn resetlogsScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 164);
            uint32_t prevResetlogsCnt = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 292);
            typescn prevResetlogsScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 284);
            typetime firstTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 188));
            typetime nextTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 200));
            typescn enabledScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 208);
            typetime enabledTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 216));
            typescn threadClosedScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 220);
            typetime threadClosedTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 228));
            typescn termialRecScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 240);
            typetime termialRecTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 248));
            typescn mostRecentScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 260);
            uint16_t chSum = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 14);
            uint16_t chSum2 = calcChSum(oracleEnvironment->headerBuffer + blockSize, blockSize);

            if (oracleEnvironment->version < 12200) {

                oracleEnvironment->dumpStream <<
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
                typescn realNextScn = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 272);

                oracleEnvironment->dumpStream <<
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

            uint32_t largestLwn = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 268);
            oracleEnvironment->dumpStream <<
                    " Largest LWN: " << dec << largestLwn << " blocks" << endl;

            uint32_t miscFlags = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 236);
            string endOfRedo;
            if ((miscFlags & REDO_END) != 0)
                endOfRedo = "Yes";
            else
                endOfRedo = "No";
            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleEnvironment->dumpStream << " FailOver End-of-redo stream : " << endOfRedo << endl;
            else
                oracleEnvironment->dumpStream << " End-of-redo stream : " << endOfRedo << endl;

            if ((miscFlags & REDO_ASYNC) != 0)
                oracleEnvironment->dumpStream << " Archivelog created using asynchronous network transmittal" << endl;

            if ((miscFlags & REDO_NODATALOSS) != 0)
                oracleEnvironment->dumpStream << " No data-loss mode" << endl;

            if ((miscFlags & REDO_RESYNC) != 0)
                oracleEnvironment->dumpStream << " Resynchronization mode" << endl;
            else
                oracleEnvironment->dumpStream << " Unprotected mode" << endl;

            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleEnvironment->dumpStream << " Closed thread archival" << endl;

            if ((miscFlags & REDO_MAXPERFORMANCE) != 0)
                oracleEnvironment->dumpStream << " Maximize performance mode" << endl;

            oracleEnvironment->dumpStream << " Miscellaneous flags: 0x" << hex << miscFlags << endl;

            if (oracleEnvironment->version >= 12201) {
                uint32_t miscFlags2 = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 296);
                oracleEnvironment->dumpStream << " Miscellaneous second flags: 0x" << hex << miscFlags2 << endl;
            }

            int32_t thr = (int32_t)oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 432);
            int32_t seq2 = (int32_t)oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 436);
            typescn scn2 = oracleEnvironment->readSCN(oracleEnvironment->headerBuffer + blockSize + 440);
            uint8_t zeroBlocks = oracleEnvironment->headerBuffer[blockSize + 206];
            uint8_t formatId = oracleEnvironment->headerBuffer[blockSize + 207];
            if (oracleEnvironment->version < 12200)
                oracleEnvironment->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN48(scn2) << endl <<
                        " Zero blocks: " << dec << (uint32_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint32_t)formatId << endl;
            else
                oracleEnvironment->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN64(scn2) << endl <<
                        " Zero blocks: " << dec << (uint32_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint32_t)formatId << endl;

            uint32_t standbyApplyDelay = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 280);
            if (standbyApplyDelay > 0)
                oracleEnvironment->dumpStream << " Standby Apply Delay: " << dec << standbyApplyDelay << " minute(s) " << endl;

            typetime standbyLogCloseTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 304));
            if (standbyLogCloseTime.getVal() > 0)
                oracleEnvironment->dumpStream << " Standby Log Close Time:  " << standbyLogCloseTime << endl;

            oracleEnvironment->dumpStream << " redo log key is ";
            for (uint32_t i = 448; i < 448 + 16; ++i)
                oracleEnvironment->dumpStream << setfill('0') << setw(2) << hex << (uint32_t)oracleEnvironment->headerBuffer[blockSize + i];
            oracleEnvironment->dumpStream << endl;

            uint16_t redoKeyFlag = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 480);
            oracleEnvironment->dumpStream << " redo log key flag is " << dec << redoKeyFlag << endl;
            uint16_t enabledRedoThreads = 1; //FIXME
            oracleEnvironment->dumpStream << " Enabled redo threads: " << dec << enabledRedoThreads << " " << endl;
        }

        return ret;
    }

    int OracleReaderRedo::initFile() {
        if (fileDes != -1)
            return REDO_OK;

        fileDes = open(path.c_str(), O_RDONLY | O_LARGEFILE | (oracleEnvironment->directRead ? O_DIRECT : 0));
        if (fileDes <= 0) {
            cerr << "ERROR: can not open: " << path.c_str() << endl;
            return REDO_ERROR;
        }
        return REDO_OK;
    }

    int OracleReaderRedo::readFileMore() {
        uint32_t curRead;
        if (redoBufferPos == REDO_LOG_BUFFER_SIZE)
            redoBufferPos = 0;

        if (lastReadSuccessfull && lastRead * 2 < REDO_LOG_BUFFER_SIZE)
            lastRead *= 2;
        curRead = lastRead;
        if (redoBufferPos == REDO_LOG_BUFFER_SIZE)
            redoBufferPos = 0;

        if (redoBufferPos + curRead > REDO_LOG_BUFFER_SIZE)
            curRead = REDO_LOG_BUFFER_SIZE - redoBufferPos;

        uint32_t bytes = pread(fileDes, oracleEnvironment->redoBuffer + redoBufferPos, curRead, redoBufferFileStart);

        if (bytes < curRead) {
            lastReadSuccessfull = false;
            lastRead = READ_CHUNK_MIN_SIZE;
        } else
            lastReadSuccessfull = true;

        if (bytes > 0) {
            uint32_t maxNumBlock = bytes / blockSize;

            for (uint32_t numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                int ret = checkBlockHeader(oracleEnvironment->redoBuffer + redoBufferPos + numBlock * blockSize, blockNumber + numBlock);
                if (ret != REDO_OK) {
                    lastReadSuccessfull = false;
                    lastRead = READ_CHUNK_MIN_SIZE;

                    if (redoBufferFileStart < redoBufferFileEnd)
                        return REDO_OK;

                    return ret;
                }

                redoBufferFileEnd += blockSize;
            }
        }
        return REDO_OK;
    }

    void OracleReaderRedo::analyzeRecord() {
        bool checkpoint = false;
        RedoLogRecord redoLogRecord[VECTOR_MAX_LENGTH];
        OpCode *opCodes[VECTOR_MAX_LENGTH];
        uint32_t isUndoRedo[VECTOR_MAX_LENGTH];
        uint32_t vectors = 0;
        uint32_t opCodesUndo[VECTOR_MAX_LENGTH / 2];
        uint32_t vectorsUndo = 0;
        uint32_t opCodesRedo[VECTOR_MAX_LENGTH / 2];
        uint32_t vectorsRedo = 0;

        uint32_t recordLength = oracleEnvironment->read32(oracleEnvironment->recordBuffer);
        uint8_t vld = oracleEnvironment->recordBuffer[4];
        curScn = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 8) |
                ((uint64_t)(oracleEnvironment->read16(oracleEnvironment->recordBuffer + 6)) << 32);
        uint32_t headerLength;

        if ((vld & 0x04) != 0) {
            checkpoint = true;
            headerLength = 68;
            if (oracleEnvironment->trace >= TRACE_FULL) {
                if (oracleEnvironment->version < 12200)
                    cerr << endl << "Checkpoint SCN: " << PRINTSCN48(curScn) << endl;
                else
                    cerr << endl << "Checkpoint SCN: " << PRINTSCN64(curScn) << endl;
            }
        } else
            headerLength = 24;

        if (oracleEnvironment->dumpLogFile >= 1) {
            uint16_t subScn = oracleEnvironment->read16(oracleEnvironment->recordBuffer + 12);
            uint16_t thread = 1; //FIXME
            oracleEnvironment->dumpStream << " " << endl;

            if (oracleEnvironment->version < 12100)
                oracleEnvironment->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint32_t)vld << endl;
            else {
                uint32_t conUid = 0; //FIXME
                oracleEnvironment->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint32_t)vld <<
                        " CON_UID: " << dec << conUid << endl;
            }

            if (oracleEnvironment->dumpData) {
                oracleEnvironment->dumpStream << "##: " << dec << recordLength;
                for (uint32_t j = 0; j < headerLength; ++j) {
                    if ((j & 0x0F) == 0)
                        oracleEnvironment->dumpStream << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
                    if ((j & 0x07) == 0)
                        oracleEnvironment->dumpStream << " ";
                    oracleEnvironment->dumpStream << setfill('0') << setw(2) << hex << (uint32_t)oracleEnvironment->recordBuffer[j] << " ";
                }
                oracleEnvironment->dumpStream << endl;
            }

            if (headerLength == 68) {
                recordTimestmap = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 64);
                if (oracleEnvironment->version < 12200)
                    oracleEnvironment->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << subScn << " " << recordTimestmap << endl;
                else
                    oracleEnvironment->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << subScn << " " << recordTimestmap << endl;
                uint32_t nst = 1; //FIXME
                uint32_t lwnLen = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 28); //28 or 32

                typescn extScn = oracleEnvironment->readSCN(oracleEnvironment->recordBuffer + 40);
                if (oracleEnvironment->version < 12200)
                    oracleEnvironment->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: " << setfill('0') << setw(4) << dec << lwnLen <<
                        " NST: " << setfill('0') << setw(4) << dec << nst <<
                        " SCN: " << PRINTSCN48(extScn) << ")" << endl;
                else
                    oracleEnvironment->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(8) << hex << lwnLen <<
                        " NST: 0x" << setfill('0') << setw(4) << hex << nst <<
                        " SCN: " << PRINTSCN64(extScn) << ")" << endl;
            } else {
                if (oracleEnvironment->version < 12200)
                    oracleEnvironment->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << subScn << " " << recordTimestmap << endl;
                else
                    oracleEnvironment->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << subScn << " " << recordTimestmap << endl;
            }
        }

        if (headerLength > recordLength)
            throw RedoLogException("too small log record: ", path.c_str(), recordLength);

        uint32_t pos = headerLength;
        while (pos < recordLength) {
            memset(&redoLogRecord[vectors], 0, sizeof(struct RedoLogRecord));
            redoLogRecord[vectors].vectorNo = vectors + 1;
            //uint16_t opc = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos);
            //uint32_t recordObjd = (oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 6) << 16) |
            //                 oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 20);
            redoLogRecord[vectors].cls = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 2);
            redoLogRecord[vectors].afn = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 4);
            redoLogRecord[vectors].dba = oracleEnvironment->read32(oracleEnvironment->recordBuffer + pos + 8);
            redoLogRecord[vectors].scnRecord = oracleEnvironment->readSCN(oracleEnvironment->recordBuffer + pos + 12);
            redoLogRecord[vectors].rbl = 0; //FIXME
            redoLogRecord[vectors].seq = oracleEnvironment->recordBuffer[pos + 20];
            redoLogRecord[vectors].typ = oracleEnvironment->recordBuffer[pos + 21];
            redoLogRecord[vectors].conId = 0; //FIXME oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 22);
            redoLogRecord[vectors].flgRecord = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 24);
            int16_t usn = (redoLogRecord[vectors].cls >= 15) ? (redoLogRecord[vectors].cls - 15) / 2 : -1;

            uint32_t fieldOffset = 24;
            if (oracleEnvironment->version >= 12102) fieldOffset = 32;
            if (pos + fieldOffset + 1 >= recordLength)
                throw RedoLogException("position of field list outside of record: ", nullptr, pos + fieldOffset);

            uint8_t *fieldList = oracleEnvironment->recordBuffer + pos + fieldOffset;

            redoLogRecord[vectors].opCode = (((uint16_t)oracleEnvironment->recordBuffer[pos + 0]) << 8) |
                    oracleEnvironment->recordBuffer[pos + 1];
            redoLogRecord[vectors].length = fieldOffset + ((oracleEnvironment->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectors].scn = curScn;
            redoLogRecord[vectors].usn = usn;
            redoLogRecord[vectors].data = oracleEnvironment->recordBuffer + pos;
            redoLogRecord[vectors].fieldLengthsDelta = fieldOffset;
            redoLogRecord[vectors].fieldCnt = (oracleEnvironment->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) - 2) / 2;
            redoLogRecord[vectors].fieldPos = fieldOffset + ((oracleEnvironment->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) + 2) & 0xFFFC);

            uint32_t fieldPos = redoLogRecord[vectors].fieldPos;
            for (uint32_t i = 1; i <= redoLogRecord[vectors].fieldCnt; ++i) {
                redoLogRecord[vectors].length += (oracleEnvironment->read16(fieldList + i * 2) + 3) & 0xFFFC;
                fieldPos += (oracleEnvironment->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta + i * 2) + 3) & 0xFFFC;
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
                opCodes[vectors] = new OpCode0501(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0502: //Begin transaction
                opCodes[vectors] = new OpCode0502(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0504: //Commit/rollback transaction
                opCodes[vectors] = new OpCode0504(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0506: //Partial rollback
                opCodes[vectors] = new OpCode0506(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x050B:
                opCodes[vectors] = new OpCode050B(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0513: //Session information
                opCodes[vectors] = new OpCode0513(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0514: //Session information
                opCodes[vectors] = new OpCode0514(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B02: //REDO: Insert row piece
                opCodes[vectors] = new OpCode0B02(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B03: //REDO: Delete row piece
                opCodes[vectors] = new OpCode0B03(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B04: //REDO: Lock row piece
                opCodes[vectors] = new OpCode0B04(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B05: //REDO: Update row piece
                opCodes[vectors] = new OpCode0B05(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B06: //REDO: Overwrite row piece
                opCodes[vectors] = new OpCode0B06(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B08: //REDO: Change forwarding address
                opCodes[vectors] = new OpCode0B08(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B0B: //REDO: Insert multiple rows
                opCodes[vectors] = new OpCode0B0B(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x0B0C: //REDO: Delete multiple rows
                opCodes[vectors] = new OpCode0B0C(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            case 0x1801: //DDL
                opCodes[vectors] = new OpCode1801(oracleEnvironment, &redoLogRecord[vectors]);
                break;
            default:
                opCodes[vectors] = new OpCode(oracleEnvironment, &redoLogRecord[vectors]);
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

        for (uint32_t i = 0; i < vectors; ++i) {
            opCodes[i]->process();
            delete opCodes[i];
            opCodes[i] = nullptr;
        }

        for (uint32_t i = 0; i < vectors; ++i) {
            //begin transaction
            if (redoLogRecord[i].opCode == 0x0502) {
            }
        }

        uint32_t iPair = 0;
        for (uint32_t i = 0; i < vectors; ++i) {
            if (oracleEnvironment->trace >= TRACE_FULL) {
                cerr << "** " << setfill('0') << setw(4) << hex << redoLogRecord[i].opCode <<
                        " OBJD: " << dec << redoLogRecord[i].recordObjd <<
                        " OBJN: " << redoLogRecord[i].recordObjn <<
                        " XID: " << PRINTXID(redoLogRecord[i].xid) << endl;
            }

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

        for (uint32_t i = 0; i < vectors; ++i) {
            //commit transaction
            if (redoLogRecord[i].opCode == 0x0504) {
            }
        }

        flushTransactions(checkpoint);
    }

    void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord) {
        if (oracleEnvironment->trace >= TRACE_FULL) {
            cerr << "** Append: " <<
                    setfill('0') << setw(4) << hex << redoLogRecord->opCode << endl;
            redoLogRecord->dump();
        }

        //DDL or part of multi-block UNDO
        if (redoLogRecord->opCode == 0x1801 || redoLogRecord->opCode == 0x0501) {
            if (redoLogRecord->opCode == 0x0501) {
                if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0) {
                    return;
                }
                if (oracleEnvironment->trace >= TRACE_FULL)
                    cerr << "merging Multi-block" << endl;
            }

            RedoLogRecord zero;
            memset(&zero, 0, sizeof(struct RedoLogRecord));

            redoLogRecord->object = oracleEnvironment->checkDict(redoLogRecord->objn, redoLogRecord->objd);
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0)
                return;

            Transaction *transaction = oracleEnvironment->xidTransactionMap[redoLogRecord->xid];
            if (transaction == nullptr) {
                if (oracleEnvironment->trace >= TRACE_DETAIL)
                    cerr << "ERROR: transaction missing" << endl;

                transaction = new Transaction(redoLogRecord->xid, &oracleEnvironment->transactionBuffer);
                transaction->add(oracleEnvironment, redoLogRecord->objn, redoLogRecord->objd, redoLogRecord->uba, redoLogRecord->dba, redoLogRecord->slt,
                        redoLogRecord->rci, redoLogRecord, &zero, &oracleEnvironment->transactionBuffer);
                oracleEnvironment->xidTransactionMap[redoLogRecord->xid] = transaction;
                oracleEnvironment->transactionHeap.add(transaction);
            } else {
                if (transaction->opCodes > 0)
                    oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci);
                transaction->add(oracleEnvironment, redoLogRecord->objn, redoLogRecord->objd, redoLogRecord->uba, redoLogRecord->dba, redoLogRecord->slt,
                        redoLogRecord->rci, redoLogRecord, &zero, &oracleEnvironment->transactionBuffer);
                oracleEnvironment->transactionHeap.update(transaction->pos);
            }
            transaction->lastUba = redoLogRecord->uba;
            transaction->lastDba = redoLogRecord->dba;
            transaction->lastSlt = redoLogRecord->slt;
            transaction->lastRci = redoLogRecord->rci;

            if (oracleEnvironment->lastOpTransactionMap.get(redoLogRecord->uba, redoLogRecord->dba,
                    redoLogRecord->slt, redoLogRecord->rci) != nullptr) {
                cerr << "ERROR: last UBA already occupied!" << endl;
            } else {
                oracleEnvironment->lastOpTransactionMap.set(redoLogRecord->uba, redoLogRecord->dba,
                        redoLogRecord->slt, redoLogRecord->rci, transaction);
            }
            oracleEnvironment->transactionHeap.update(transaction->pos);

            return;
        } else
        if (redoLogRecord->opCode != 0x0502 && redoLogRecord->opCode != 0x0504)
            return;

        Transaction *transaction = oracleEnvironment->xidTransactionMap[redoLogRecord->xid];
        if (transaction == nullptr) {
            transaction = new Transaction(redoLogRecord->xid, &oracleEnvironment->transactionBuffer);
            transaction->touch(curScn);
            oracleEnvironment->xidTransactionMap[redoLogRecord->xid] = transaction;
            oracleEnvironment->transactionHeap.add(transaction);
        } else
            transaction->touch(curScn);

        if (redoLogRecord->opCode == 0x0502) {
            transaction->isBegin = true;
        }

        if (redoLogRecord->opCode == 0x0504) {
            transaction->isCommit = true;
            if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
                transaction->isRollback = true;
            oracleEnvironment->transactionHeap.update(transaction->pos);
        }
    }

    void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        if (oracleEnvironment->trace >= TRACE_FULL) {
            cerr << "** Append: " <<
                    setfill('0') << setw(4) << hex << redoLogRecord1->opCode << " + " <<
                    setfill('0') << setw(4) << hex << redoLogRecord2->opCode << endl;
            cerr << "SCN: " << PRINTSCN64(redoLogRecord1->scn) << endl;
            redoLogRecord1->dump();
            redoLogRecord2->dump();
        }

        uint32_t objn, objd;
        if (redoLogRecord1->objd != 0) {
            objn = redoLogRecord1->objn;
            objd = redoLogRecord1->objd;
        } else {
            objn = redoLogRecord2->objn;
            objd = redoLogRecord2->objd;
        }

        if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0) {
            cerr << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            if (oracleEnvironment->dumpLogFile >= 1)
                oracleEnvironment->dumpStream << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            return;
        }

        redoLogRecord1->object = oracleEnvironment->checkDict(objn, objd);
        if (redoLogRecord1->object == nullptr)
            return;

        redoLogRecord2->object = redoLogRecord1->object;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        if (redoLogRecord1->object->options == 1 && opCodeLong == 0x05010B02) {
            cout << "Exiting on user request" << endl;
            kill(getpid(), SIGINT);
            return;
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
                Transaction *transaction = oracleEnvironment->xidTransactionMap[redoLogRecord1->xid];
                if (transaction == nullptr) {
                    transaction = new Transaction(redoLogRecord1->xid, &oracleEnvironment->transactionBuffer);
                    transaction->add(oracleEnvironment, objn, objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, &oracleEnvironment->transactionBuffer);
                    oracleEnvironment->xidTransactionMap[redoLogRecord1->xid] = transaction;
                    oracleEnvironment->transactionHeap.add(transaction);
                } else {
                    if (transaction->opCodes > 0)
                        oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                                transaction->lastSlt, transaction->lastRci);

                    transaction->add(oracleEnvironment, objn, objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, &oracleEnvironment->transactionBuffer);
                    oracleEnvironment->transactionHeap.update(transaction->pos);
                }
                transaction->lastUba = redoLogRecord1->uba;
                transaction->lastDba = redoLogRecord1->dba;
                transaction->lastSlt = redoLogRecord1->slt;
                transaction->lastRci = redoLogRecord1->rci;

                if (oracleEnvironment->lastOpTransactionMap.get(redoLogRecord1->uba, redoLogRecord1->dba,
                        redoLogRecord1->slt, redoLogRecord1->rci) != nullptr) {
                    cerr << "ERROR: last UBA already occupied!" << endl;
                } else {
                    oracleEnvironment->lastOpTransactionMap.set(redoLogRecord1->uba, redoLogRecord1->dba,
                            redoLogRecord1->slt, redoLogRecord1->rci, transaction);
                }
                oracleEnvironment->transactionHeap.update(transaction->pos);
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
                Transaction *transaction = oracleEnvironment->lastOpTransactionMap.getMatch(redoLogRecord1->uba,
                        redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci);

                //match
                if (transaction != nullptr) {
                    oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci);
                    transaction->rollbackLastOp(oracleEnvironment, curScn, &oracleEnvironment->transactionBuffer);
                    oracleEnvironment->transactionHeap.update(transaction->pos);
                    oracleEnvironment->lastOpTransactionMap.set(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci, transaction);

                } else {
                    //check all previous transactions - not yet implemented
                    bool foundPrevious = false;

                    for (uint32_t i = 0; i < oracleEnvironment->transactionHeap.heapSize; ++i) {
                        transaction = oracleEnvironment->transactionHeap.heap[i];

                        if (transaction != nullptr &&
                                transaction->opCodes > 0 &&
                                transaction->rollbackPreviousOp(oracleEnvironment, curScn, &oracleEnvironment->transactionBuffer, redoLogRecord1->uba,
                                redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci)) {
                            oracleEnvironment->transactionHeap.update(transaction->pos);
                            foundPrevious = true;
                            break;
                        }
                    }
                    if (!foundPrevious) {
                        if (oracleEnvironment->trace >= TRACE_WARN)
                            cerr << "WARNING: can't rollback transaction part, UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " DBA: " << hex << redoLogRecord2->dba <<
                                    " SLT: " << dec << (uint32_t)redoLogRecord2->slt <<
                                    " RCI: " << dec << (uint32_t)redoLogRecord2->rci << endl;
                    } else {
                        if (oracleEnvironment->trace >= TRACE_WARN)
                            cerr << "WARNING: would like to rollback transaction part, UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " DBA: " << hex << redoLogRecord2->dba <<
                                    " SLT: " << dec << (uint32_t)redoLogRecord2->slt <<
                                    " RCI: " << dec << (uint32_t)redoLogRecord2->rci << endl;
                    }
                }
            }

            break;
        }
    }


    void OracleReaderRedo::flushTransactions(bool checkpoint) {
        Transaction *transaction = oracleEnvironment->transactionHeap.top();
        typescn checkpointScn;
        if (checkpoint) {
            checkpointScn = curScn;
            lastCheckpointScn = curScn;
            lastCheckpointInfo = false;
        } else if (curScn > 200) { //TODO: parametrize
            checkpointScn = curScn - 200;
            if (checkpointScn < lastCheckpointScn)
                return;

            if (!lastCheckpointInfo) {
                if (oracleEnvironment->trace >= TRACE_INFO) {
                    if (oracleEnvironment->version >= 12200)
                        cerr << "INFO: current SCN: " << PRINTSCN64(curScn) << ", checkpoint at SCN: " << PRINTSCN64(checkpointScn) << endl;
                    else
                        cerr << "INFO: current SCN: " << PRINTSCN48(curScn) << ", checkpoint at SCN: " << PRINTSCN48(checkpointScn) << endl;
                }
                lastCheckpointInfo = true;
            }
        } else
            return;

        while (transaction != nullptr) {
            if (oracleEnvironment->trace >= TRACE_FULL) {
                cerr << "FirstScn: " << PRINTSCN64(transaction->firstScn) <<
                        " lastScn: " << PRINTSCN64(transaction->lastScn) <<
                        " xid: " << PRINTXID(transaction->xid) <<
                        " pos: " << dec << transaction->pos <<
                        " opCodes: " << transaction->opCodes <<
                        " commit: " << transaction->isCommit <<
                        " uba: " << PRINTUBA(transaction->lastUba) <<
                        " dba: " << transaction->lastDba <<
                        " slt: " << hex << (uint32_t)transaction->lastSlt <<
                        endl;
            }

            if (transaction->lastScn <= checkpointScn && transaction->isCommit) {
                if (transaction->isBegin)
                    //FIXME: it should be checked if transaction begin SCN is within captured range of SCNs
                    transaction->flush(oracleEnvironment);
                else {
                    if (curScn + 100 > transaction->lastScn) //TODO: parametrize
                        return;

                    if (oracleEnvironment->trace >= TRACE_WARN) {
                        cerr << "WARNING: skipping transaction with no begin, XID: " << PRINTXID(transaction->xid) << endl;

                        for (uint32_t i = 1; i <= oracleEnvironment->transactionHeap.heapSize; ++i) {
                            Transaction *transactionI = oracleEnvironment->transactionHeap.heap[i];
                            cerr << "WARNING: heap dump[" << i << "] XID: " << PRINTXID(transactionI->xid) <<
                                    ", begin: " << transactionI->isBegin <<
                                    ", commit: " << transactionI->isCommit <<
                                    ", rollback: " << transactionI->isRollback << endl;

                        }
                    }
                }

                oracleEnvironment->transactionHeap.pop();
                if (transaction->opCodes > 0)
                    oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci);
                oracleEnvironment->xidTransactionMap.erase(transaction->xid);

                //oracleEnvironment->xidTransactionMap[transaction->xid]->free(&oracleEnvironment->transactionBuffer);
                delete oracleEnvironment->xidTransactionMap[transaction->xid];
                transaction = oracleEnvironment->transactionHeap.top();
            } else
                break;
        }

        if (oracleEnvironment->trace >= TRACE_FULL) {
            for (auto const& xid : oracleEnvironment->xidTransactionMap) {
                Transaction *transaction = oracleEnvironment->xidTransactionMap[xid.first];
                if (transaction != nullptr) {
                    cerr << "Queue: " << PRINTSCN64(transaction->firstScn) <<
                            " lastScn: " << PRINTSCN64(transaction->lastScn) <<
                            " xid: " << PRINTXID(transaction->xid) <<
                            " pos: " << dec << transaction->pos <<
                            " opCodes: " << transaction->opCodes <<
                            " commit: " << transaction->isCommit << endl;
                }
            }
        }
    }

    int OracleReaderRedo::processBuffer(void) {
        while (redoBufferFileStart < redoBufferFileEnd) {
            int ret = checkBlockHeader(oracleEnvironment->redoBuffer + redoBufferPos, blockNumber);
            if (ret != 0)
                return ret;

            uint32_t curBlockPos = 16;
            while (curBlockPos < blockSize) {
                //next record
                if (recordLeftToCopy == 0) {
                    if (curBlockPos + 20 >= blockSize)
                        break;

                    recordLeftToCopy = (oracleEnvironment->read32(oracleEnvironment->redoBuffer + redoBufferPos + curBlockPos) + 3) & 0xFFFFFFFC;
                    if (recordLeftToCopy > REDO_RECORD_MAX_SIZE)
                        throw RedoLogException("too big log record: ", path.c_str(), recordLeftToCopy);

                    recordPos = 0;
                    recordBeginPos = curBlockPos;
                    recordBeginBlock = blockNumber;
                }

                //nothing more
                if (recordLeftToCopy == 0)
                    break;

                uint32_t toCopy;
                if (curBlockPos + recordLeftToCopy > blockSize)
                    toCopy = blockSize - curBlockPos;
                else
                    toCopy = recordLeftToCopy;

                memcpy(oracleEnvironment->recordBuffer + recordPos, oracleEnvironment->redoBuffer + redoBufferPos + curBlockPos, toCopy);
                recordLeftToCopy -= toCopy;
                curBlockPos += toCopy;
                recordPos += toCopy;

                if (oracleEnvironment->trace >= TRACE_FULL)
                    cerr << "Block: " << dec << redoBufferFileStart << " pos: " << dec << recordPos << endl;

                if (recordLeftToCopy == 0)
                    analyzeRecord();
            }

            ++blockNumber;
            redoBufferPos += blockSize;
            redoBufferFileStart += blockSize;
        }
        return REDO_OK;
    }

    int OracleReaderRedo::processLog(OracleReader *oracleReader) {
        if (oracleEnvironment->trace >= TRACE_INFO)
            cerr << "Processing log: " << *this << endl;
        if (oracleEnvironment->dumpLogFile >= 1) {
            stringstream name;
            name << "DUMP-" << sequence << ".trace";
            oracleEnvironment->dumpStream.open(name.str());
            //TODO: add file creation error handling
        }
        clock_t cStart = clock();

        int ret = initFile();
        if (ret != REDO_OK)
            return ret;

        bool reachedEndOfOnlineRedo = false;
        ret = checkRedoHeader(true);
        if (ret != REDO_OK) {
            oracleEnvironment->dumpStream.close();
            return ret;
        }

        redoBufferFileStart = blockSize * 2;
        redoBufferFileEnd = blockSize * 2;
        blockNumber = 2;
        recordObjn = 0xFFFFFFFF;
        recordObjd = 0xFFFFFFFF;

        while (blockNumber <= numBlocks && !reachedEndOfOnlineRedo && !oracleReader->shutdown) {
            processBuffer();

            while (redoBufferFileStart == redoBufferFileEnd && blockNumber <= numBlocks && !reachedEndOfOnlineRedo
                    && !oracleReader->shutdown) {
                int ret = readFileMore();

                if (redoBufferFileStart < redoBufferFileEnd)
                    break;

                //for archive redo log break on all errors
                if (group == 0) {
                    oracleEnvironment->dumpStream.close();
                    return ret;
                //for online redo log
                } else {
                    if (ret == REDO_ERROR || ret == REDO_WRONG_SEQUENCE_SWITCHED) {
                        oracleEnvironment->dumpStream.close();
                        return ret;
                    }

                    //check if sequence has changed
                    int ret = checkRedoHeader(false);
                    if (ret != REDO_OK) {
                        oracleEnvironment->dumpStream.close();
                        return ret;
                    }

                    if (nextScn != ZERO_SCN) {
                        reachedEndOfOnlineRedo = true;
                        break;
                    }

                    flushTransactions(true);
                    //online redo log problem
                    if (oracleReader->shutdown)
                        break;

                    usleep(REDO_SLEEP_RETRY);
                }
            }

            if (redoBufferFileStart == redoBufferFileEnd) {
                break;
            }
        }

        if (fileDes > 0) {
            close(fileDes);
            fileDes = 0;
        }

        if (oracleEnvironment->trace >= TRACE_INFO) {
            clock_t cEnd = clock();
            double mySpeed = 0, myTime = 1000.0 * (cEnd-cStart) / CLOCKS_PER_SEC;
            if (myTime > 0)
                mySpeed = blockNumber * blockSize / 1024 / 1024 / myTime * 1000;
            cerr << "processLog: " << myTime << "ms (" << fixed << setprecision(2) << mySpeed << "MB/s)" << endl;
        }

        oracleEnvironment->dumpStream.close();
        return REDO_OK;
    }

    uint16_t OracleReaderRedo::calcChSum(uint8_t *buffer, uint32_t size) {
        uint16_t oldChSum = oracleEnvironment->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint32_t i = 0; i < size / 8; ++i, buffer += 8)
            sum ^= *((uint64_t*)buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }


    OracleReaderRedo::~OracleReaderRedo() {
    }

    ostream& operator<<(ostream& os, const OracleReaderRedo& ors) {
        os << "(" << ors.group << ", " << ors.firstScn << ", " << ors.sequence << ", \"" << ors.path << "\")";
        return os;
    }

}
