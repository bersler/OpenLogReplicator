/* Class reading a redo log file
   Copyright (C) 2018-2019 Adam Leszczynski.

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
#include "OracleReader.h"
#include "OracleReaderRedo.h"
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
#include "OpCode0B02.h"
#include "OpCode0B03.h"
#include "OpCode0B04.h"
#include "OpCode0B05.h"
#include "OpCode0B06.h"
#include "OpCode0B0B.h"
#include "OpCode0B0C.h"

using namespace std;
using namespace OpenLogReplicator;

namespace OpenLogReplicatorOracle {

    OracleReaderRedo::OracleReaderRedo(OracleEnvironment *oracleEnvironment, int group, typescn firstScn,
                typescn nextScn, typeseq sequence, const char* path) :
            oracleEnvironment(oracleEnvironment),
            group(group),
            curScn(ZERO_SCN),
            firstScn(firstScn),
            nextScn(nextScn),
            recordBeginPos(0),
            recordBeginBlock(0),
            recordTimestmap(0),
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
        typescn firstScnHeader = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 180);
        typescn nextScnHeader = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 192);

        if (compatVsn == 0x0B200400) //11.2.0.4
            oracleEnvironment->version = 11;
        //else
        //if (compatVsn == 0x0C100200) //12.1.0.2
        //    oracleEnvironment->version = 12;
        else {
            cerr << "ERROR: Unsupported database version: " << compatVsn << endl;
            return REDO_ERROR;
        }

        int ret = checkBlockHeader(oracleEnvironment->headerBuffer + blockSize, 1);
        if (ret == REDO_ERROR) {
            cerr << "ERROR: bad header" << endl;
            return ret;
        }

        if (firstScnHeader != firstScn) {
            //archive log incorrect sequence
            if (group == 0) {
                cerr << "ERROR: first SCN (" << firstScnHeader << ") does not match database information (" <<
                        firstScn << "): " << path.c_str() << endl;
                return REDO_ERROR;
            //redo log switch appeared and header is now overwritten
            } else {
                cerr << "WARNING: first SCN (" << firstScnHeader << ") does not match database information (" <<
                        firstScn << "): " << path.c_str() << endl;
                return REDO_WRONG_SEQUENCE_SWITCHED;
            }
        }

        //updating nextScn if changed
        if (nextScn == ZERO_SCN && nextScnHeader != ZERO_SCN) {
            cerr << "WARNING: log switch to " << nextScnHeader << endl;
            nextScn = nextScnHeader;
        } else
        if (nextScn != ZERO_SCN && nextScnHeader != ZERO_SCN && nextScn != nextScnHeader) {
            cerr << "ERROR: next SCN (" << firstScnHeader << ") does not match database information (" <<
                    firstScn << "): " << path.c_str() << endl;
            return REDO_ERROR;
        }

        //typescn threadClosedScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 220);
        memcpy(SID, oracleEnvironment->headerBuffer + blockSize + 28, 8); SID[8] = 0;

        if (oracleEnvironment->dumpLogFile && first) {
            oracleEnvironment->dumpStream << "DUMP OF REDO FROM FILE '" << path << "'" << endl <<
                    " Opcodes *.*" << endl <<
                    " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff" << endl <<
                    " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff" << endl <<
                    " Times: creation thru eternity" << endl;

            uint32_t dbid = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 24);
            uint16_t controlSeq = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 36);
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
            uint32_t thread = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 176);
            uint32_t nab = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 156);
            uint8_t hws = oracleEnvironment->headerBuffer[blockSize + 172];
            uint8_t eot = 0; //FIXME
            uint8_t dis = 0; //FIXME

            oracleEnvironment->dumpStream << " descrip:\"" << descrip << "\"" << endl <<
                    " thread: " << dec << thread <<
                    " nab: 0x" << hex << nab <<
                    " seq: 0x" << setfill('0') << setw(8) << hex << (uint32_t) seq <<
                    " hws: 0x" << hex << (uint32_t) hws <<
                    " eot: " << dec << (uint32_t) eot <<
                    " dis: " << dec << (uint32_t) dis << endl;

            uint32_t resetlogsCnt = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 160);
            typescn resetlogsScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 164); //FIXME: 164 or 208
            oracleEnvironment->dumpStream << " resetlogs count: 0x" << hex << resetlogsCnt <<
                    " scn: " << PRINTSCN(resetlogsScn) << " (" << dec << resetlogsScn << ")" << endl;

            uint32_t prevResetlogsCnt = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 292);
            typescn prevResetlogsScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 284);
            oracleEnvironment->dumpStream << " prev resetlogs count: 0x" << hex << prevResetlogsCnt <<
                    " scn: " << PRINTSCN(prevResetlogsScn) << " (" << dec << prevResetlogsScn << ")" << endl;

            typetime firstTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 188));
            oracleEnvironment->dumpStream << " Low  scn: " << PRINTSCN(firstScnHeader) <<
                    " (" << dec << firstScnHeader << ")" <<
                    " " << firstTime << endl;

            typetime nextTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 200));
            oracleEnvironment->dumpStream << " Next scn: " << PRINTSCN(nextScnHeader) <<
                    " (" << dec << nextScn << ")" <<
                    " " << nextTime << endl;

            typescn enabledScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 208);
            typetime enabledTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 216));
            oracleEnvironment->dumpStream << " Enabled scn: " << PRINTSCN(enabledScn) <<
                    " (" << dec << enabledScn << ")" <<
                    " " << enabledTime << endl;

            typescn threadClosedScn = oracleEnvironment->read48(oracleEnvironment->headerBuffer + blockSize + 220);
            typetime threadClosedTime(oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 228));
            oracleEnvironment->dumpStream <<
                    " Thread closed scn: " << PRINTSCN(threadClosedScn) << " (" << dec << threadClosedScn << ")" <<
                    " " << threadClosedTime << endl;

            uint16_t chSum = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 14);
            uint16_t chSum2 = calcChSum(oracleEnvironment->headerBuffer + blockSize, blockSize);
            oracleEnvironment->dumpStream <<
                    " Disk cksum: 0x" << hex << chSum << " Calc cksum: 0x" << hex << chSum2 << endl;

            typescn termialRecScn = 0; //FIXME
            typetime termialRecTime(0); //FIXME
            oracleEnvironment->dumpStream << " Terminal recovery stop scn: " << PRINTSCN(termialRecScn) << endl <<
                    " Terminal recovery  " << termialRecTime << endl;

            typescn mostRecentScn = 0;
            oracleEnvironment->dumpStream << " Most recent redo scn: " << PRINTSCN(mostRecentScn) << endl;

            uint32_t largestLwn = oracleEnvironment->read16(oracleEnvironment->headerBuffer + blockSize + 268);
            oracleEnvironment->dumpStream <<
                    " Largest LWN: " << dec << largestLwn << " blocks" << endl;

            string endOfRedo = "No"; //FIXME
            oracleEnvironment->dumpStream << " End-of-redo stream : " << endOfRedo << endl <<
                    " Unprotected mode" << endl;

            uint32_t miscFlags = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 236);
            oracleEnvironment->dumpStream << " Miscellaneous flags: 0x" << hex << miscFlags << endl;

            uint32_t thr = 0; //FIXME
            uint32_t seq2 = 0; //FIXME
            typescn scn2 = 0; //FIXME
            uint32_t zeroBlocks = 8; //FIXME
            uint32_t formatId = 2; //FIXME
            oracleEnvironment->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                    " seq: " << dec << seq2 <<
                    " scn: " << PRINTSCN(scn2) << endl <<
                    " Zero blocks: " << dec << zeroBlocks << endl <<
                    " Format ID is " << dec << formatId << endl;

            oracleEnvironment->dumpStream << " redo log key is " << hex;
            for (uint32_t i = 448; i < 448 + 16; ++i)
                oracleEnvironment->dumpStream << (uint32_t) oracleEnvironment->headerBuffer[blockSize + i];
            oracleEnvironment->dumpStream << endl;

            uint32_t redoKeyFlag = oracleEnvironment->read32(oracleEnvironment->headerBuffer + blockSize + 480);
            uint32_t enabledRedoThreads = 1; //FIXME
            oracleEnvironment->dumpStream << " redo log key flag is " << dec << redoKeyFlag << endl <<
                    " Enabled redo threads: " << dec << enabledRedoThreads << " " << endl;
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
        bool encrypted = false;
        bool checkpoint = false;
        RedoLogRecord redoLogRecord[2], *redoLogRecordCur, *redoLogRecordPrev;
        uint32_t redoLogCur = 0;
        redoLogRecordCur = &redoLogRecord[redoLogCur];
        redoLogRecordPrev = nullptr;

        uint32_t recordLength = oracleEnvironment->read32(oracleEnvironment->recordBuffer);
        uint8_t vld = oracleEnvironment->recordBuffer[4];
        curScn = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 8) |
                ((uint64_t)(oracleEnvironment->read16(oracleEnvironment->recordBuffer + 6)) << 32);
        uint32_t vectorNo = 1;

        uint16_t headerLength;
        if ((vld & 4) == 4) {
            checkpoint = true;
            headerLength = 68;
        } else
            headerLength = 24;

        if (oracleEnvironment->dumpLogFile) {
            uint16_t subScn = oracleEnvironment->read16(oracleEnvironment->recordBuffer + 12); //12 or 26 or 52
            uint16_t thread = 1; //FIXME
            oracleEnvironment->dumpStream << " " << endl;

            oracleEnvironment->dumpStream << "REDO RECORD - Thread:" << thread <<
                    " RBA: 0x" << hex << setfill('0') << setw(6) << sequence << "." <<
                                hex << setfill('0') << setw(8) << recordBeginBlock << "." <<
                                hex << setfill('0') << setw(4) << recordBeginPos <<
                    " LEN: 0x" << hex << setfill('0') << setw(4) << recordLength <<
                    " VLD: 0x" << hex << setfill('0') << setw(2) << (uint32_t) vld << endl;

            if (oracleEnvironment->dumpData) {
                oracleEnvironment->dumpStream << "##: " << dec << recordLength;
                for (uint32_t j = 0; j < headerLength; ++j) {
                    if ((j & 0xF) == 0)
                        oracleEnvironment->dumpStream << endl << "##  " << hex << setfill(' ') << setw(2) <<  j << ": ";
                    if ((j & 0x7) == 0)
                        oracleEnvironment->dumpStream << " ";
                    oracleEnvironment->dumpStream << hex << setfill('0') << setw(2) << (uint32_t) oracleEnvironment->recordBuffer[j] << " ";
                }
                oracleEnvironment->dumpStream << endl;
            }

            if (headerLength == 68) {
                recordTimestmap = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 64);
                oracleEnvironment->dumpStream << "SCN: " << PRINTSCN(curScn) << " SUBSCN:  " << dec << subScn << " " << recordTimestmap << endl;
                uint32_t nst = 1; //FIXME
                uint32_t lwnLen = oracleEnvironment->read32(oracleEnvironment->recordBuffer + 28); //28 or 32

                typescn extScn = oracleEnvironment->read48(oracleEnvironment->recordBuffer + 40);
                oracleEnvironment->dumpStream << "(LWN RBA: 0x" << hex << setfill('0') << setw(6) << sequence << "." <<
                                hex << setfill('0') << setw(8) << recordBeginBlock << "." <<
                                hex << setfill('0') << setw(4) << recordBeginPos <<
                    " LEN: " << dec << setfill('0') << setw(4) << lwnLen <<
                    " NST: " << dec << setfill('0') << setw(4) << nst <<
                    " SCN: " << PRINTSCN(extScn) << ")" << endl;
            } else {
                oracleEnvironment->dumpStream << "SCN: " << PRINTSCN(curScn) << " SUBSCN:  " << dec << subScn << " " << recordTimestmap << endl;
            }
        }

        if (headerLength > recordLength)
            throw RedoLogException("too small log record: ", path.c_str(), recordLength);

        uint32_t pos = headerLength;
        while (pos < recordLength) {
            //uint16_t opc = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos);
            uint16_t cls = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 2);
            uint16_t afn = oracleEnvironment->read16(oracleEnvironment->recordBuffer + pos + 4);
            uint32_t dba = oracleEnvironment->read32(oracleEnvironment->recordBuffer + pos + 8);
            int16_t usn = (cls >= 15) ? (cls - 15) / 2 : -1;
            uint8_t typ = oracleEnvironment->recordBuffer[pos + 21];
            if ((typ & 0x80) == 0x80)
                encrypted = true;

            uint32_t fieldOffset = 24;
            if (oracleEnvironment->version == 11) fieldOffset = 24;
            else if (oracleEnvironment->version == 12) fieldOffset = 32;

            if (pos + fieldOffset + 1 >= recordLength)
                throw RedoLogException("position of field list outside of record: ", nullptr, pos + fieldOffset);

            uint16_t *fieldList = (uint16_t*)(oracleEnvironment->recordBuffer + pos + fieldOffset);

            memset(redoLogRecordCur, 0, sizeof(struct RedoLogRecord));
            redoLogRecordCur->opCode = (((uint16_t)oracleEnvironment->recordBuffer[pos + 0]) << 8) |
                    oracleEnvironment->recordBuffer[pos + 1];
            redoLogRecordCur->length = fieldOffset + ((fieldList[0] + 2) & 0xFFFC);
            redoLogRecordCur->afn = afn;
            redoLogRecordCur->dba = dba;
            redoLogRecordCur->scn = curScn;
            redoLogRecordCur->usn = usn;
            redoLogRecordCur->data = oracleEnvironment->recordBuffer + pos;
            redoLogRecordCur->fieldLengthsDelta = fieldOffset;
            redoLogRecordCur->fieldNum = (*((uint16_t*)(redoLogRecordCur->data + redoLogRecordCur->fieldLengthsDelta)) - 2) / 2;
            redoLogRecordCur->fieldPos = fieldOffset + ((*((uint16_t*)(redoLogRecordCur->data + redoLogRecordCur->fieldLengthsDelta)) + 2) & 0xFFFC);

            if (redoLogRecordCur->fieldPos > redoLogRecordCur->length)
                throw RedoLogException("incomplete record", nullptr, 0);

            if (redoLogRecordCur->opCode == 0x0501 ||
                    redoLogRecordCur->opCode == 0x0502 ||
                    redoLogRecordCur->opCode == 0x0504 ||
                    redoLogRecordCur->opCode == 0x0506 ||
                    redoLogRecordCur->opCode == 0x0508 ||
                    redoLogRecordCur->opCode == 0x050B)
                recordObjd = 4294967295;

            if (oracleEnvironment->dumpLogFile) {
                typescn scn2 = oracleEnvironment->read48(oracleEnvironment->recordBuffer + pos + 12);
                uint8_t seq = oracleEnvironment->recordBuffer[pos + 20];
                uint32_t rbl = 0; //FIXME

                if (oracleEnvironment->version == 11) {
                    if (typ == 6)
                        oracleEnvironment->dumpStream << "CHANGE #" << dec << vectorNo <<
                            " MEDIA RECOVERY MARKER" <<
                            " SCN:" << PRINTSCN(scn2) <<
                            " SEQ:" << dec << (uint32_t)seq <<
                            " OP:" << (uint32_t)(redoLogRecordCur->opCode >> 8) << "." << (uint32_t)(redoLogRecordCur->opCode & 0xFF) <<
                            " ENC:" << dec << (uint32_t)encrypted << endl;
                    else
                        oracleEnvironment->dumpStream << "CHANGE #" << dec << vectorNo <<
                            " TYP:" << (uint32_t)typ <<
                            " CLS:" << cls <<
                            " AFN:" << afn <<
                            " DBA:0x" << setfill('0') << setw(8) << hex << dba <<
                            " OBJ:" << dec << recordObjd <<
                            " SCN:" << PRINTSCN(scn2) <<
                            " SEQ:" << dec << (uint32_t)seq <<
                            " OP:" << (uint32_t)(redoLogRecordCur->opCode >> 8) << "." << (uint32_t)(redoLogRecordCur->opCode & 0xFF) <<
                            " ENC:" << dec << (uint32_t)encrypted <<
                            " RBL:" << dec << rbl << endl;
                } else if (oracleEnvironment->version == 12) {
                    uint32_t conId = 0; //FIXME
                    uint32_t flg = 0; //FIXME
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << vectorNo <<
                        " CON_ID:" << conId <<
                        " TYP:" << (uint32_t)typ <<
                        " CLS:" << cls <<
                        " AFN:" << afn <<
                        " DBA:0x" << setfill('0') << setw(8) << hex << dba <<
                        " OBJ:" << dec << recordObjd <<
                        " SCN:" << PRINTSCN(scn2) <<
                        " SEQ:" << dec << (uint32_t)seq <<
                        " OP:" << (uint32_t)(redoLogRecordCur->opCode >> 8) << "." << (uint32_t)(redoLogRecordCur->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted <<
                        " RBL:" << dec << rbl <<
                        " FLG:0x" << setw(4) << hex << flg << endl;
                }
            }

            if (oracleEnvironment->dumpData) {
                oracleEnvironment->dumpStream << "##: " << dec << fieldOffset;
                for (uint32_t j = 0; j < fieldOffset; ++j) {
                    if ((j & 0xF) == 0)
                        oracleEnvironment->dumpStream << endl << "##  " << hex << setfill(' ') << setw(2) <<  j << ": ";
                    if ((j & 0x7) == 0)
                        oracleEnvironment->dumpStream << " ";
                    oracleEnvironment->dumpStream << hex << setfill('0') << setw(2) << (uint32_t) oracleEnvironment->recordBuffer[j] << " ";
                }
                oracleEnvironment->dumpStream << endl;
            }

            uint32_t fieldPosTmp = redoLogRecordCur->fieldPos;
            for (uint32_t i = 1; i <= redoLogRecordCur->fieldNum; ++i) {
                if (oracleEnvironment->dumpData) {
                    oracleEnvironment->dumpStream << "##: " << dec << ((uint16_t*)(redoLogRecordCur->data + redoLogRecordCur->fieldLengthsDelta))[i] << " (" << i << ")";
                    for (uint32_t j = 0; j < ((uint16_t*)(redoLogRecordCur->data + redoLogRecordCur->fieldLengthsDelta))[i]; ++j) {
                        if ((j & 0xF) == 0)
                            oracleEnvironment->dumpStream << endl << "##  " << hex << setfill(' ') << setw(2) <<  j << ": ";
                        if ((j & 0x7) == 0)
                            oracleEnvironment->dumpStream << " ";
                        oracleEnvironment->dumpStream << hex << setfill('0') << setw(2) << (uint32_t) redoLogRecordCur->data[fieldPosTmp + j] << " ";
                    }
                    oracleEnvironment->dumpStream << endl;
                }

                redoLogRecordCur->length += (fieldList[i] + 3) & 0xFFFC;
                fieldPosTmp += (((uint16_t*)(redoLogRecordCur->data + redoLogRecordCur->fieldLengthsDelta))[i] + 3) & 0xFFFC;
                if (pos + redoLogRecordCur->length > recordLength)
                    throw RedoLogException("position of field list outside of record: ", nullptr, pos + redoLogRecordCur->length);
            }

            OpCode *opCode = nullptr;
            pos += redoLogRecordCur->length;

            //begin transaction
            if (redoLogRecordCur->opCode == 0x0502) {
                opCode = new OpCode0502(oracleEnvironment, redoLogRecordCur);
                opCode->process();
                if (SQN(redoLogRecordCur->xid) > 0)
                    appendToTransaction(redoLogRecordCur);
            } else
            //commit transaction (or rollback)
            if (redoLogRecordCur->opCode == 0x0504) {
                opCode = new OpCode0504(oracleEnvironment, redoLogRecordCur);
                opCode->process();
                appendToTransaction(redoLogRecordCur);
            } else {
                switch (redoLogRecordCur->opCode) {
                //Undo
                case 0x0501:
                    opCode = new OpCode0501(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //Partial rollback
                case 0x0506:
                    opCode = new OpCode0506(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                case 0x050B:
                    opCode = new OpCode050B(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Insert row piece
                case 0x0B02:
                    opCode = new OpCode0B02(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Delete row piece
                case 0x0B03:
                    opCode = new OpCode0B03(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Lock row piece
                case 0x0B04:
                    opCode = new OpCode0B04(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Update row piece
                case 0x0B05:
                    opCode = new OpCode0B05(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Overwrite row piece
                case 0x0B06:
                    opCode = new OpCode0B06(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Insert multiple rows
                case 0x0B0B:
                    opCode = new OpCode0B0B(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;

                //REDO: Delete multiple rows
                case 0x0B0C:
                    opCode = new OpCode0B0C(oracleEnvironment, redoLogRecordCur);
                    opCode->process();
                    break;
                }

                if (redoLogRecordCur->objd != 0)
                    recordObjd = redoLogRecordCur->objd;

                if (redoLogRecordCur->opCode != 0) {
                    if (redoLogRecordPrev == nullptr) {
                        redoLogRecordPrev = redoLogRecordCur;
                        redoLogCur = 1 - redoLogCur;
                        redoLogRecordCur = &redoLogRecord[redoLogCur];
                    } else {
                        appendToTransaction(redoLogRecordPrev, redoLogRecordCur);
                        redoLogRecordPrev = nullptr;
                    }
                }
            }

            if (opCode != nullptr) {
                delete opCode;
                opCode = nullptr;
            }
            ++vectorNo;
        }

        if (redoLogRecordPrev != nullptr) {
            appendToTransaction(redoLogRecordPrev);
            redoLogRecordPrev = nullptr;
        }

        if (checkpoint)
            flushTransactions();
    }

    void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord) {
        if (oracleEnvironment->trace >= 1) {
            cout << "** Append: " <<
                    setfill('0') << setw(4) << hex << redoLogRecord->opCode << endl;
            redoLogRecord->dump();
        }

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
            if ((redoLogRecord->flg & OPCODE0504_ROLLBACK) != 0)
                transaction->isRollback = true;
            oracleEnvironment->transactionHeap.update(transaction->pos);
        }
    }

    void OracleReaderRedo::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        if (oracleEnvironment->trace >= 1) {
            cout << "** Append: " <<
                    setfill('0') << setw(4) << hex << redoLogRecord1->opCode << " + " <<
                    setfill('0') << setw(4) << hex << redoLogRecord2->opCode << endl;
            cout << "SCN: " << PRINTSCN(redoLogRecord1->scn) << endl;
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

        if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord2->bdba != 0) {
            cerr << "ERROR: BDBA does not match!" << endl;
            return;
        }

        redoLogRecord1->object = oracleEnvironment->checkDict(objn, objd);
        if (redoLogRecord1->object == nullptr)
            return;
        redoLogRecord2->object = redoLogRecord1->object;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;

        switch (opCodeLong) {
        //insert single row
        case 0x05010B02:
        //delete single row
        case 0x05010B03:
        //insert multiple rows
        case 0x05010B0B:
        //delete multiple row
        //case 0x05010B0C:
            {
                Transaction *transaction = oracleEnvironment->xidTransactionMap[redoLogRecord1->xid];
                if (transaction == nullptr) {
                    transaction = new Transaction(redoLogRecord1->xid, &oracleEnvironment->transactionBuffer);
                    transaction->add(objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, &oracleEnvironment->transactionBuffer);
                    oracleEnvironment->xidTransactionMap[redoLogRecord1->xid] = transaction;
                    oracleEnvironment->transactionHeap.add(transaction);
                } else {
                    if (transaction->opCodes > 0)
                        oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                                transaction->lastSlt, transaction->lastRci);
                    transaction->add(objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
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

        //rollback: delete single row
        case 0x0B030506:
        case 0x0B03050B:
        //rollback: delete multiple rows
        case 0x0B0C0506:
        case 0x0B0C050B:
        //rollback: insert single row
        //case 0x0B020506:
        //case 0x0B02050B:
        //rollback: insert multiple row
        //case 0x0B0B0506:
        //case 0x0B0B050B:
            {
                Transaction *transaction = oracleEnvironment->lastOpTransactionMap.getMatch(redoLogRecord1->uba,
                        redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci);
                //match
                if (transaction != nullptr) {
                    oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci);
                    transaction->rollbackLastOp(curScn, &oracleEnvironment->transactionBuffer);
                    oracleEnvironment->transactionHeap.update(transaction->pos);
                    oracleEnvironment->lastOpTransactionMap.set(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci, transaction);
                } else {
                    //check all previous transactions
                    bool foundPrevious = false;

                    for (uint32_t i = 0; i < oracleEnvironment->transactionHeap.heapSize; ++i) {
                        transaction = oracleEnvironment->transactionHeap.heap[i];

                        if (transaction->opCodes > 0 &&
                                transaction->rollbackPreviousOp(curScn, &oracleEnvironment->transactionBuffer, redoLogRecord1->uba,
                                redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci)) {
                            oracleEnvironment->transactionHeap.update(transaction->pos);
                            foundPrevious = true;
                            break;
                        }
                    }

                    if (!foundPrevious)
                        cerr << "WARNING: can't rollback transaction part, UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                " DBA: " << hex << redoLogRecord2->dba <<
                                " SLT: " << dec << (uint32_t)redoLogRecord2->slt <<
                                " RCI: " << dec << (uint32_t)redoLogRecord2->rci << endl;
                }
            }

            break;
        }
    }


    void OracleReaderRedo::flushTransactions() {
        Transaction *transaction = oracleEnvironment->transactionHeap.top();

        while (transaction != nullptr) {
            if (oracleEnvironment->trace >= 1) {
                cout << "FirstScn: " << PRINTSCN(transaction->firstScn) <<
                        " lastScn: " << PRINTSCN(transaction->lastScn) <<
                        " xid: " << PRINTXID(transaction->xid) <<
                        " pos: " << dec << transaction->pos <<
                        " opCodes: " << transaction->opCodes <<
                        " commit: " << transaction->isCommit <<
                        " uba: " << PRINTUBA(transaction->lastUba) <<
                        " dba: " << transaction->lastDba <<
                        " slt: " << hex << (uint32_t)transaction->lastSlt <<
                        endl;
            }

            if (transaction->lastScn <= curScn && transaction->isCommit) {
                if (transaction->isBegin)
                    //FIXME: it should be checked if transaction begin SCN is within captured range of SCNs
                    transaction->flush(oracleEnvironment);
                else
                    cerr << "WARNING: skipping transaction with no begin, XID: " << PRINTXID(transaction->xid) << endl;

                oracleEnvironment->transactionHeap.pop();
                if (transaction->opCodes > 0)
                    oracleEnvironment->lastOpTransactionMap.erase(transaction->lastUba, transaction->lastDba,
                            transaction->lastSlt, transaction->lastRci);
                oracleEnvironment->xidTransactionMap.erase(transaction->xid);

                delete oracleEnvironment->xidTransactionMap[transaction->xid];
                transaction = oracleEnvironment->transactionHeap.top();
            } else
                break;
        }

        if (oracleEnvironment->trace >= 1) {
            for (auto const& xid : oracleEnvironment->xidTransactionMap) {
                Transaction *transaction = oracleEnvironment->xidTransactionMap[xid.first];
                if (transaction != nullptr) {
                    cout << "Queue: " << PRINTSCN(transaction->firstScn) <<
                            " lastScn: " << PRINTSCN(transaction->lastScn) <<
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

                if (recordLeftToCopy == 0) {
                    analyzeRecord();
                }
            }

            ++blockNumber;
            redoBufferPos += blockSize;
            redoBufferFileStart += blockSize;
        }
        return REDO_OK;
    }

    int OracleReaderRedo::processLog(OracleReader *oracleReader) {
        cout << "processLog: " << *this << endl;
        if (oracleEnvironment->dumpLogFile) {
            stringstream name;
            name << "DUMP-" << sequence << ".trace";
            oracleEnvironment->dumpStream.open(name.str());
            //TODO: add file creation error handling
        }
        clock_t cStart = clock();

        initFile();
        bool reachedEndOfOnlineRedo = false;
        int ret = checkRedoHeader(true);
        if (ret != REDO_OK) {
            oracleEnvironment->dumpStream.close();
            return ret;
        }

        redoBufferFileStart = blockSize * 2;
        redoBufferFileEnd = blockSize * 2;
        blockNumber = 2;
        recordObjd = 4294967295;

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

                    flushTransactions();
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

        if (oracleEnvironment->trace >= 1) {
            clock_t cEnd = clock();
            double mySpeed = 0, myTime = 1000.0 * (cEnd-cStart) / CLOCKS_PER_SEC;
            if (myTime > 0)
                mySpeed = blockNumber * blockSize / 1024 / 1024 / myTime * 1000;
            cout << "processLog: " << myTime << "ms (" << fixed << setprecision(2) << mySpeed << "MB/s)" << endl;
        }

        oracleEnvironment->dumpStream.close();
        return REDO_OK;
    }

    uint16_t OracleReaderRedo::calcChSum(uint8_t *buffer, uint32_t size) {
        uint16_t oldChSum = *((uint16_t*)(buffer + 14));
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
