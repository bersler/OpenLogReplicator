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

#include <cstdio>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <signal.h>
#include <string.h>

#include "MemoryException.h"
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
#include "OpCode0B10.h"
#include "OpCode1801.h"
#include "OracleAnalyser.h"
#include "OracleAnalyserRedoLog.h"
#include "OracleObject.h"
#include "Reader.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Transaction.h"
#include "TransactionMap.h"

using namespace std;

void stopMain();

namespace OpenLogReplicator {

    OracleAnalyserRedoLog::OracleAnalyserRedoLog(OracleAnalyser *oracleAnalyser, int64_t group, const string path) :
            oracleAnalyser(oracleAnalyser),
            group(group),
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
            recordLength4(0),
            blockNumber(0),
            path(path),
            sequence(0),
            firstScn(firstScn),
            nextScn(nextScn),
            reader(nullptr) {
    }

    void OracleAnalyserRedoLog::printHeaderInfo() {

        if (oracleAnalyser->dumpRedoLog >= 1) {
            char SID[9];
            memcpy(SID, reader->headerBuffer + reader->blockSize + 28, 8); SID[8] = 0;

            oracleAnalyser->dumpStream << "DUMP OF REDO FROM FILE '" << path << "'" << endl;
            if (oracleAnalyser->version >= 0x12200)
                oracleAnalyser->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleAnalyser->dumpStream << " Opcodes *.*" << endl;
            if (oracleAnalyser->version >= 0x12200)
                oracleAnalyser->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleAnalyser->dumpStream << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff" << endl;
            if (oracleAnalyser->version < 0x12200)
                oracleAnalyser->dumpStream << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff" << endl;
            else
                oracleAnalyser->dumpStream << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff" << endl;
            oracleAnalyser->dumpStream << " Times: creation thru eternity" << endl;

            uint32_t dbid = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 24);
            uint32_t controlSeq = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 36);
            uint32_t fileSize = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 40);
            uint16_t fileNumber = oracleAnalyser->read16(reader->headerBuffer + reader->blockSize + 48);
            uint32_t activationId = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 52);

            oracleAnalyser->dumpStream << " FILE HEADER:" << endl <<
                    "\tCompatibility Vsn = " << dec << reader->compatVsn << "=0x" << hex << reader->compatVsn << endl <<
                    "\tDb ID=" << dec << dbid << "=0x" << hex << dbid << ", Db Name='" << SID << "'" << endl <<
                    "\tActivation ID=" << dec << activationId << "=0x" << hex << activationId << endl <<
                    "\tControl Seq=" << dec << controlSeq << "=0x" << hex << controlSeq << ", File size=" << dec << fileSize << "=0x" << hex << fileSize << endl <<
                    "\tFile Number=" << dec << fileNumber << ", Blksiz=" << dec << reader->blockSize << ", File Type=2 LOG" << endl;

            typeseq seq = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 8);
            uint8_t descrip[65];
            memcpy (descrip, reader->headerBuffer + reader->blockSize + 92, 64); descrip[64] = 0;
            uint16_t thread = oracleAnalyser->read16(reader->headerBuffer + reader->blockSize + 176);
            uint32_t nab = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 156);
            uint32_t hws = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 172);
            uint8_t eot = reader->headerBuffer[reader->blockSize + 204];
            uint8_t dis = reader->headerBuffer[reader->blockSize + 205];

            oracleAnalyser->dumpStream << " descrip:\"" << descrip << "\"" << endl <<
                    " thread: " << dec << thread <<
                    " nab: 0x" << hex << nab <<
                    " seq: 0x" << setfill('0') << setw(8) << hex << (typeseq)seq <<
                    " hws: 0x" << hex << hws <<
                    " eot: " << dec << (uint64_t)eot <<
                    " dis: " << dec << (uint64_t)dis << endl;

            typescn resetlogsScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 164);
            typeresetlogs prevResetlogsCnt = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 292);
            typescn prevResetlogsScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 284);
            typetime firstTime(oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 188));
            typetime nextTime(oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 200));
            typescn enabledScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 208);
            typetime enabledTime(oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 216));
            typescn threadClosedScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 220);
            typetime threadClosedTime(oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 228));
            typescn termialRecScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 240);
            typetime termialRecTime(oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 248));
            typescn mostRecentScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 260);
            typesum chSum = oracleAnalyser->read16(reader->headerBuffer + reader->blockSize + 14);
            typesum chSum2 = reader->calcChSum(reader->headerBuffer + reader->blockSize, reader->blockSize);

            if (oracleAnalyser->version < 0x12200) {
                oracleAnalyser->dumpStream <<
                        " resetlogs count: 0x" << hex << reader->resetlogsCnt << " scn: " << PRINTSCN48(resetlogsScn) << " (" << dec << resetlogsScn << ")" << endl <<
                        " prev resetlogs count: 0x" << hex << prevResetlogsCnt << " scn: " << PRINTSCN48(prevResetlogsScn) << " (" << dec << prevResetlogsScn << ")" << endl <<
                        " Low  scn: " << PRINTSCN48(reader->firstScnHeader) << " (" << dec << reader->firstScnHeader << ")" << " " << firstTime << endl <<
                        " Next scn: " << PRINTSCN48(reader->nextScnHeader) << " (" << dec << reader->nextScn << ")" << " " << nextTime << endl <<
                        " Enabled scn: " << PRINTSCN48(enabledScn) << " (" << dec << enabledScn << ")" << " " << enabledTime << endl <<
                        " Thread closed scn: " << PRINTSCN48(threadClosedScn) << " (" << dec << threadClosedScn << ")" << " " << threadClosedTime << endl <<
                        " Disk cksum: 0x" << hex << chSum << " Calc cksum: 0x" << hex << chSum2 << endl <<
                        " Terminal recovery stop scn: " << PRINTSCN48(termialRecScn) << endl <<
                        " Terminal recovery  " << termialRecTime << endl <<
                        " Most recent redo scn: " << PRINTSCN48(mostRecentScn) << endl;
            } else {
                typescn realNextScn = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 272);

                oracleAnalyser->dumpStream <<
                        " resetlogs count: 0x" << hex << reader->resetlogsCnt << " scn: " << PRINTSCN64(resetlogsScn) << endl <<
                        " prev resetlogs count: 0x" << hex << prevResetlogsCnt << " scn: " << PRINTSCN64(prevResetlogsScn) << endl <<
                        " Low  scn: " << PRINTSCN64(reader->firstScnHeader) << " " << firstTime << endl <<
                        " Next scn: " << PRINTSCN64(reader->nextScnHeader) << " " << nextTime << endl <<
                        " Enabled scn: " << PRINTSCN64(enabledScn) << " " << enabledTime << endl <<
                        " Thread closed scn: " << PRINTSCN64(threadClosedScn) << " " << threadClosedTime << endl <<
                        " Real next scn: " << PRINTSCN64(realNextScn) << endl <<
                        " Disk cksum: 0x" << hex << chSum << " Calc cksum: 0x" << hex << chSum2 << endl <<
                        " Terminal recovery stop scn: " << PRINTSCN64(termialRecScn) << endl <<
                        " Terminal recovery  " << termialRecTime << endl <<
                        " Most recent redo scn: " << PRINTSCN64(mostRecentScn) << endl;
            }

            uint32_t largestLwn = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 268);
            oracleAnalyser->dumpStream <<
                    " Largest LWN: " << dec << largestLwn << " blocks" << endl;

            uint32_t miscFlags = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 236);
            string endOfRedo;
            if ((miscFlags & REDO_END) != 0)
                endOfRedo = "Yes";
            else
                endOfRedo = "No";
            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleAnalyser->dumpStream << " FailOver End-of-redo stream : " << endOfRedo << endl;
            else
                oracleAnalyser->dumpStream << " End-of-redo stream : " << endOfRedo << endl;

            if ((miscFlags & REDO_ASYNC) != 0)
                oracleAnalyser->dumpStream << " Archivelog created using asynchronous network transmittal" << endl;

            if ((miscFlags & REDO_NODATALOSS) != 0)
                oracleAnalyser->dumpStream << " No data-loss mode" << endl;

            if ((miscFlags & REDO_RESYNC) != 0)
                oracleAnalyser->dumpStream << " Resynchronization mode" << endl;
            else
                oracleAnalyser->dumpStream << " Unprotected mode" << endl;

            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleAnalyser->dumpStream << " Closed thread archival" << endl;

            if ((miscFlags & REDO_MAXPERFORMANCE) != 0)
                oracleAnalyser->dumpStream << " Maximize performance mode" << endl;

            oracleAnalyser->dumpStream << " Miscellaneous flags: 0x" << hex << miscFlags << endl;

            if (oracleAnalyser->version >= 0x12200) {
                uint32_t miscFlags2 = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 296);
                oracleAnalyser->dumpStream << " Miscellaneous second flags: 0x" << hex << miscFlags2 << endl;
            }

            int32_t thr = (int32_t)oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 432);
            int32_t seq2 = (int32_t)oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 436);
            typescn scn2 = oracleAnalyser->readSCN(reader->headerBuffer + reader->blockSize + 440);
            uint8_t zeroBlocks = reader->headerBuffer[reader->blockSize + 206];
            uint8_t formatId = reader->headerBuffer[reader->blockSize + 207];
            if (oracleAnalyser->version < 0x12200)
                oracleAnalyser->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN48(scn2) << endl <<
                        " Zero blocks: " << dec << (uint64_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint64_t)formatId << endl;
            else
                oracleAnalyser->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN64(scn2) << endl <<
                        " Zero blocks: " << dec << (uint64_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint64_t)formatId << endl;

            uint32_t standbyApplyDelay = oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 280);
            if (standbyApplyDelay > 0)
                oracleAnalyser->dumpStream << " Standby Apply Delay: " << dec << standbyApplyDelay << " minute(s) " << endl;

            typetime standbyLogCloseTime(oracleAnalyser->read32(reader->headerBuffer + reader->blockSize + 304));
            if (standbyLogCloseTime.getVal() > 0)
                oracleAnalyser->dumpStream << " Standby Log Close Time:  " << standbyLogCloseTime << endl;

            oracleAnalyser->dumpStream << " redo log key is ";
            for (uint64_t i = 448; i < 448 + 16; ++i)
                oracleAnalyser->dumpStream << setfill('0') << setw(2) << hex << (uint64_t)reader->headerBuffer[reader->blockSize + i];
            oracleAnalyser->dumpStream << endl;

            uint16_t redoKeyFlag = oracleAnalyser->read16(reader->headerBuffer + reader->blockSize + 480);
            oracleAnalyser->dumpStream << " redo log key flag is " << dec << redoKeyFlag << endl;
            uint16_t enabledRedoThreads = 1; //FIXME
            oracleAnalyser->dumpStream << " Enabled redo threads: " << dec << enabledRedoThreads << " " << endl;
        }
    }

    void OracleAnalyserRedoLog::analyzeRecord() {
        RedoLogRecord redoLogRecord[VECTOR_MAX_LENGTH];
        OpCode *opCodes[VECTOR_MAX_LENGTH];
        uint64_t isUndoRedo[VECTOR_MAX_LENGTH];
        uint64_t vectors = 0;
        uint64_t opCodesUndo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsUndo = 0;
        uint64_t opCodesRedo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsRedo = 0;

        uint64_t recordLength = oracleAnalyser->read32(oracleAnalyser->recordBuffer);
        uint8_t vld = oracleAnalyser->recordBuffer[4];
        curScnPrev = curScn;
        curScn = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 8) |
                ((uint64_t)(oracleAnalyser->read16(oracleAnalyser->recordBuffer + 6)) << 32);
        curSubScn = oracleAnalyser->read16(oracleAnalyser->recordBuffer + 12);
        uint64_t headerLength;
        uint16_t numChk = 0, numChkMax = 0;

        if (extScn > lastCheckpointScn && curScnPrev != curScn && curScnPrev != ZERO_SCN)
            flushTransactions(extScn);

        if ((vld & 0x04) != 0) {
            headerLength = 68;
            numChk = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 24);
            numChkMax = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 26);
            recordTimestmap = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 64);
            if (numChk + 1 == numChkMax) {
                extScn = oracleAnalyser->readSCN(oracleAnalyser->recordBuffer + 40);
            }
            if (oracleAnalyser->trace >= TRACE_FULL) {
                if (oracleAnalyser->version < 0x12200)
                    cerr << "FULL: C scn: " << PRINTSCN48(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << " CHECKPOINT at " <<
                    PRINTSCN48(extScn) << endl;
                else
                    cerr << "FULL: C scn: " << PRINTSCN64(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << " CHECKPOINT at " <<
                    PRINTSCN64(extScn) << endl;
            }
        } else {
            headerLength = 24;
            if (oracleAnalyser->trace >= TRACE_FULL) {
                if (oracleAnalyser->version < 0x12200)
                    cerr << "FULL:   scn: " << PRINTSCN48(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << endl;
                else
                    cerr << "FULL:   scn: " << PRINTSCN64(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << endl;
            }
        }

        if (oracleAnalyser->dumpRedoLog >= 1) {
            uint16_t thread = 1; //FIXME
            oracleAnalyser->dumpStream << " " << endl;

            if (oracleAnalyser->version < 0x12100)
                oracleAnalyser->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld << endl;
            else {
                uint32_t conUid = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 16);
                oracleAnalyser->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld <<
                        " CON_UID: " << dec << conUid << endl;
            }

            if (oracleAnalyser->dumpRawData > 0) {
                oracleAnalyser->dumpStream << "##: " << dec << recordLength;
                for (uint64_t j = 0; j < headerLength; ++j) {
                    if ((j & 0x0F) == 0)
                        oracleAnalyser->dumpStream << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
                    if ((j & 0x07) == 0)
                        oracleAnalyser->dumpStream << " ";
                    oracleAnalyser->dumpStream << setfill('0') << setw(2) << hex << (uint64_t)oracleAnalyser->recordBuffer[j] << " ";
                }
                oracleAnalyser->dumpStream << endl;
            }

            if (headerLength == 68) {
                if (oracleAnalyser->version < 0x12200)
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
                else
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
                uint32_t nst = 1; //FIXME
                uint32_t lwnLen = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 28);

                if (oracleAnalyser->version < 0x12200)
                    oracleAnalyser->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: " << setfill('0') << setw(4) << dec << lwnLen <<
                        " NST: " << setfill('0') << setw(4) << dec << nst <<
                        " SCN: " << PRINTSCN48(extScn) << ")" << endl;
                else
                    oracleAnalyser->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << recordBeginBlock << "." <<
                                    setfill('0') << setw(4) << hex << recordBeginPos <<
                        " LEN: 0x" << setfill('0') << setw(8) << hex << lwnLen <<
                        " NST: 0x" << setfill('0') << setw(4) << hex << nst <<
                        " SCN: " << PRINTSCN64(extScn) << ")" << endl;
            } else {
                if (oracleAnalyser->version < 0x12200)
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
                else
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN: " << setfill(' ') << setw(2) << dec << curSubScn << " " << recordTimestmap << endl;
            }
        }

        if (headerLength > recordLength) {
            dumpRedoVector();
            throw RedoLogException("too small log record");
        }

        uint64_t pos = headerLength;
        while (pos < recordLength) {
            memset(&redoLogRecord[vectors], 0, sizeof(struct RedoLogRecord));
            redoLogRecord[vectors].vectorNo = vectors + 1;
            redoLogRecord[vectors].cls = oracleAnalyser->read16(oracleAnalyser->recordBuffer + pos + 2);
            redoLogRecord[vectors].afn = oracleAnalyser->read16(oracleAnalyser->recordBuffer + pos + 4);
            redoLogRecord[vectors].dba = oracleAnalyser->read32(oracleAnalyser->recordBuffer + pos + 8);
            redoLogRecord[vectors].scnRecord = oracleAnalyser->readSCN(oracleAnalyser->recordBuffer + pos + 12);
            redoLogRecord[vectors].rbl = 0; //FIXME
            redoLogRecord[vectors].seq = oracleAnalyser->recordBuffer[pos + 20];
            redoLogRecord[vectors].typ = oracleAnalyser->recordBuffer[pos + 21];
            int16_t usn = (redoLogRecord[vectors].cls >= 15) ? (redoLogRecord[vectors].cls - 15) / 2 : -1;

            uint64_t fieldOffset;
            if (oracleAnalyser->version >= 0x12100) {
                fieldOffset = 32;
                redoLogRecord[vectors].flgRecord = oracleAnalyser->read16(oracleAnalyser->recordBuffer + pos + 28);
                redoLogRecord[vectors].conId = oracleAnalyser->read32(oracleAnalyser->recordBuffer + pos + 24);
            } else {
                fieldOffset = 24;
                redoLogRecord[vectors].flgRecord = 0;
                redoLogRecord[vectors].conId = 0;
            }

            if (pos + fieldOffset + 1 >= recordLength) {
                dumpRedoVector();
                throw RedoLogException("position of field list outside of record");
            }

            uint8_t *fieldList = oracleAnalyser->recordBuffer + pos + fieldOffset;

            redoLogRecord[vectors].opCode = (((typeop1)oracleAnalyser->recordBuffer[pos + 0]) << 8) |
                    oracleAnalyser->recordBuffer[pos + 1];
            redoLogRecord[vectors].length = fieldOffset + ((oracleAnalyser->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectors].scn = curScn;
            redoLogRecord[vectors].subScn = curSubScn;
            redoLogRecord[vectors].usn = usn;
            redoLogRecord[vectors].data = oracleAnalyser->recordBuffer + pos;
            redoLogRecord[vectors].fieldLengthsDelta = fieldOffset;
            redoLogRecord[vectors].fieldCnt = (oracleAnalyser->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) - 2) / 2;
            redoLogRecord[vectors].fieldPos = fieldOffset + ((oracleAnalyser->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) + 2) & 0xFFFC);

            uint64_t fieldPos = redoLogRecord[vectors].fieldPos;
            for (uint64_t i = 1; i <= redoLogRecord[vectors].fieldCnt; ++i) {
                redoLogRecord[vectors].length += (oracleAnalyser->read16(fieldList + i * 2) + 3) & 0xFFFC;
                fieldPos += (oracleAnalyser->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta + i * 2) + 3) & 0xFFFC;

                if (pos + redoLogRecord[vectors].length > recordLength) {
                    cerr << "ERROR: position of field list outside of record (" <<
                            "i: " << dec << i <<
                            " c: " << dec << redoLogRecord[vectors].fieldCnt << " " <<
                            " o: " << dec << fieldOffset <<
                            " p: " << dec << pos <<
                            " l: " << dec << redoLogRecord[vectors].length <<
                            " r: " << dec << recordLength << ")" << endl;
                    dumpRedoVector();
                    throw RedoLogException("position of field list outside of record");
                }
            }

            if (redoLogRecord[vectors].fieldPos > redoLogRecord[vectors].length) {
                dumpRedoVector();
                throw RedoLogException("incomplete record");
            }

            redoLogRecord[vectors].recordObjn = 0xFFFFFFFF;
            redoLogRecord[vectors].recordObjd = 0xFFFFFFFF;

            pos += redoLogRecord[vectors].length;

            switch (redoLogRecord[vectors].opCode) {
            case 0x0501: //Undo
                opCodes[vectors] = new OpCode0501(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.1", sizeof(OpCode0501));
                break;

            case 0x0502: //Begin transaction
                opCodes[vectors] = new OpCode0502(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.2", sizeof(OpCode0502));
                break;

            case 0x0504: //Commit/rollback transaction
                opCodes[vectors] = new OpCode0504(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.3", sizeof(OpCode0504));
                break;

            case 0x0506: //Partial rollback
                opCodes[vectors] = new OpCode0506(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.4", sizeof(OpCode0506));
                break;

            case 0x050B:
                opCodes[vectors] = new OpCode050B(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.5", sizeof(OpCode050B));
                break;

            case 0x0513: //Session information
                opCodes[vectors] = new OpCode0513(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.6", sizeof(OpCode0513));
                break;

            case 0x0514: //Session information
                opCodes[vectors] = new OpCode0514(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.7", sizeof(OpCode0514));
                break;

            case 0x0B02: //REDO: Insert row piece
                opCodes[vectors] = new OpCode0B02(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.8", sizeof(OpCode0B02));
                break;

            case 0x0B03: //REDO: Delete row piece
                opCodes[vectors] = new OpCode0B03(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.9", sizeof(OpCode0B03));
                break;

            case 0x0B04: //REDO: Lock row piece
                opCodes[vectors] = new OpCode0B04(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.10", sizeof(OpCode0B04));
                break;

            case 0x0B05: //REDO: Update row piece
                opCodes[vectors] = new OpCode0B05(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.11", sizeof(OpCode0B05));
                break;

            case 0x0B06: //REDO: Overwrite row piece
                opCodes[vectors] = new OpCode0B06(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.12", sizeof(OpCode0B06));
                break;

            case 0x0B08: //REDO: Change forwarding address
                opCodes[vectors] = new OpCode0B08(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.13", sizeof(OpCode0B08));
                break;

            case 0x0B0B: //REDO: Insert multiple rows
                opCodes[vectors] = new OpCode0B0B(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.14", sizeof(OpCode0B0B));
                break;

            case 0x0B0C: //REDO: Delete multiple rows
                opCodes[vectors] = new OpCode0B0C(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.15", sizeof(OpCode0B0C));
                break;

            case 0x0B10: //REDO: Supp log for update
                opCodes[vectors] = new OpCode0B10(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.16", sizeof(OpCode0B10));
                break;

            case 0x1801: //DDL
                opCodes[vectors] = new OpCode1801(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.17", sizeof(OpCode1801));
                break;

            default:
                opCodes[vectors] = new OpCode(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::analyzeRecord.18", sizeof(OpCode));
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

    void OracleAnalyserRedoLog::appendToTransaction(RedoLogRecord *redoLogRecord) {
        if (oracleAnalyser->trace >= TRACE_FULL) {
            cerr << "FULL: ";
            redoLogRecord->dump(oracleAnalyser);
            cerr << endl;
        }

        //skip other PDB vectors
        if (redoLogRecord->conId > 1 && redoLogRecord->conId != oracleAnalyser->conId)
            return;

        //DDL or part of multi-block UNDO
        if (redoLogRecord->opCode == 0x1801 || redoLogRecord->opCode == 0x0501) {
            if (redoLogRecord->opCode == 0x0501) {
                if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0) {
                    return;
                }
                if ((oracleAnalyser->trace2 & TRACE2_DUMP) != 0)
                    cerr << "DUMP: merging Multi-block" << endl;
            }

            RedoLogRecord zero;
            memset(&zero, 0, sizeof(struct RedoLogRecord));

            redoLogRecord->object = oracleAnalyser->checkDict(redoLogRecord->objn, redoLogRecord->objd);
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0)
                return;

            Transaction *transaction = oracleAnalyser->xidTransactionMap[redoLogRecord->xid];
            if (transaction == nullptr) {
                if (oracleAnalyser->trace >= TRACE_DETAIL)
                    cerr << "ERROR: transaction missing" << endl;

                transaction = new Transaction(oracleAnalyser, redoLogRecord->xid, oracleAnalyser->transactionBuffer);
                if (transaction == nullptr)
                    throw MemoryException("OracleAnalyserRedoLog::appendToTransaction.1", sizeof(Transaction));

                transaction->add(oracleAnalyser, redoLogRecord->objn, redoLogRecord->objd, redoLogRecord->uba, redoLogRecord->dba, redoLogRecord->slt,
                        redoLogRecord->rci, redoLogRecord, &zero, oracleAnalyser->transactionBuffer, sequence);
                oracleAnalyser->xidTransactionMap[redoLogRecord->xid] = transaction;
                oracleAnalyser->transactionHeap.add(transaction);
            } else {
                if (transaction->opCodes > 0)
                    oracleAnalyser->lastOpTransactionMap.erase(transaction);
                transaction->add(oracleAnalyser, redoLogRecord->objn, redoLogRecord->objd, redoLogRecord->uba, redoLogRecord->dba, redoLogRecord->slt,
                        redoLogRecord->rci, redoLogRecord, &zero, oracleAnalyser->transactionBuffer, sequence);
                oracleAnalyser->transactionHeap.update(transaction->pos);
            }

            if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                cerr << "redo, now last: UBA: " << PRINTUBA(transaction->lastUba) <<
                        " DBA: 0x" << hex << transaction->lastDba <<
                        " SLT: " << dec << (uint64_t)transaction->lastSlt <<
                        " RCI: " << dec << (uint64_t)transaction->lastRci << endl;
            }
            oracleAnalyser->lastOpTransactionMap.set(transaction);
            oracleAnalyser->transactionHeap.update(transaction->pos);

            return;
        } else
        if (redoLogRecord->opCode != 0x0502 && redoLogRecord->opCode != 0x0504)
            return;

        Transaction *transaction = oracleAnalyser->xidTransactionMap[redoLogRecord->xid];
        if (transaction == nullptr) {
            transaction = new Transaction(oracleAnalyser, redoLogRecord->xid, oracleAnalyser->transactionBuffer);
            if (transaction == nullptr)
                throw MemoryException("OracleAnalyserRedoLog::appendToTransaction.2", sizeof(Transaction));

            transaction->touch(curScn, sequence);
            oracleAnalyser->xidTransactionMap[redoLogRecord->xid] = transaction;
            oracleAnalyser->transactionHeap.add(transaction);
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
            oracleAnalyser->transactionHeap.update(transaction->pos);
        }
    }

    void OracleAnalyserRedoLog::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        bool shutdown = false;
        if (oracleAnalyser->trace >= TRACE_FULL) {
            cerr << "FULL: ";
            redoLogRecord1->dump(oracleAnalyser);
            cerr << " (1)" << endl;
            cerr << "FULL: ";
            redoLogRecord2->dump(oracleAnalyser);
            cerr << " (2)" << endl;
        }

        //skip other PDB vectors
        if (redoLogRecord1->conId > 1 && redoLogRecord1->conId != oracleAnalyser->conId)
            return;
        if (redoLogRecord2->conId > 1 && redoLogRecord2->conId != oracleAnalyser->conId)
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
            if (oracleAnalyser->dumpRedoLog >= 1)
                oracleAnalyser->dumpStream << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            return;
        }

        redoLogRecord1->object = oracleAnalyser->checkDict(objn, objd);
        if (redoLogRecord1->object == nullptr)
            return;

        //cluster key
        if ((redoLogRecord2->fb & FB_K) != 0)
            return;

        redoLogRecord2->object = redoLogRecord1->object;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        if (redoLogRecord1->object->options == 1 && opCodeLong == 0x05010B02) {
            if (oracleAnalyser->trace >= TRACE_DETAIL)
                cerr << "INFO: Exiting on user request" << endl;
            shutdown = true;
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
        //supp log for update
        case 0x05010B10:
            {
                if (oracleAnalyser->onRollbackList(redoLogRecord1, redoLogRecord2)) {
                    if (oracleAnalyser->trace >= TRACE_WARN)
                        cerr << "INFO: rolling transaction part UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                " DBA: 0x" << hex << redoLogRecord1->dba <<
                                " SLT: " << dec << (uint64_t)redoLogRecord1->slt <<
                                " RCI: " << dec << (uint64_t)redoLogRecord1->rci <<
                                " OPFLAGS: " << hex << redoLogRecord2->opFlags << endl;
                    break;
                }

                Transaction *transaction = oracleAnalyser->xidTransactionMap[redoLogRecord1->xid];
                if (transaction == nullptr) {
                    transaction = new Transaction(oracleAnalyser, redoLogRecord1->xid, oracleAnalyser->transactionBuffer);
                    if (transaction == nullptr)
                        throw MemoryException("OracleAnalyserRedoLog::appendToTransaction.3", sizeof(Transaction));

                    transaction->add(oracleAnalyser, objn, objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, oracleAnalyser->transactionBuffer, sequence);
                    oracleAnalyser->xidTransactionMap[redoLogRecord1->xid] = transaction;
                    oracleAnalyser->transactionHeap.add(transaction);
                } else {
                    if (transaction->opCodes > 0)
                        oracleAnalyser->lastOpTransactionMap.erase(transaction);

                    transaction->add(oracleAnalyser, objn, objd, redoLogRecord1->uba, redoLogRecord1->dba, redoLogRecord1->slt, redoLogRecord1->rci,
                            redoLogRecord1, redoLogRecord2, oracleAnalyser->transactionBuffer, sequence);
                    oracleAnalyser->transactionHeap.update(transaction->pos);
                }
                transaction->shutdown = shutdown;

                if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                    cerr << "redo, now last: UBA: " << PRINTUBA(transaction->lastUba) <<
                            " DBA: 0x" << hex << transaction->lastDba <<
                            " SLT: " << dec << (uint64_t)transaction->lastSlt <<
                            " RCI: " << dec << (uint64_t)transaction->lastRci << endl;
                }
                oracleAnalyser->lastOpTransactionMap.set(transaction);
                oracleAnalyser->transactionHeap.update(transaction->pos);
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
        //rollback: supp log for update
        case 0x0B100506:
        case 0x0B10050B:
            {
                if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                    cerr << "rollback, searching for UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                            " DBA: 0x" << hex << redoLogRecord2->dba <<
                            " SLT: " << dec << (uint64_t)redoLogRecord2->slt <<
                            " RCI: " << dec << (uint64_t)redoLogRecord2->rci <<
                            " OPFLAGS: " << hex << redoLogRecord2->opFlags << endl;
                }
                Transaction *transaction = oracleAnalyser->lastOpTransactionMap.getMatch(redoLogRecord1->uba,
                        redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci, redoLogRecord2->opFlags);

                //match
                if (transaction != nullptr) {
                    if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                        cerr << "match, rolled back" << endl;
                    }

                    oracleAnalyser->lastOpTransactionMap.erase(transaction);
                    transaction->rollbackLastOp(oracleAnalyser, curScn, oracleAnalyser->transactionBuffer);
                    oracleAnalyser->transactionHeap.update(transaction->pos);

                    if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                        cerr << "rollback, now last: UBA: " << PRINTUBA(transaction->lastUba) <<
                                " DBA: 0x" << hex << transaction->lastDba <<
                                " SLT: " << dec << (uint64_t)transaction->lastSlt <<
                                " RCI: " << dec << (uint64_t)transaction->lastRci << endl;
                    }
                    oracleAnalyser->lastOpTransactionMap.set(transaction);
                } else {
                    if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                        cerr << "no match, searching" << endl;
                    }

                    //check all previous transactions
                    bool foundPrevious = false;

                    for (uint64_t i = 1; i <= oracleAnalyser->transactionHeap.heapSize; ++i) {
                        transaction = oracleAnalyser->transactionHeap.heap[i];

                        if (transaction->opCodes > 0 &&
                                transaction->rollbackPartOp(oracleAnalyser, curScn, oracleAnalyser->transactionBuffer, redoLogRecord1->uba,
                                redoLogRecord2->dba, redoLogRecord2->slt, redoLogRecord2->rci, redoLogRecord2->opFlags)) {
                            oracleAnalyser->transactionHeap.update(transaction->pos);
                            foundPrevious = true;
                            break;
                        }
                    }

                    if (!foundPrevious) {
                        if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                            cerr << "still no match, failing" << endl;
                        }

                        oracleAnalyser->addToRollbackList(redoLogRecord1, redoLogRecord2);

                        if (oracleAnalyser->trace >= TRACE_WARN)
                            cerr << "INFO: can't rollback transaction part UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                    " DBA: 0x" << hex << redoLogRecord2->dba <<
                                    " SLT: " << dec << (uint64_t)redoLogRecord2->slt <<
                                    " RCI: " << dec << (uint64_t)redoLogRecord2->rci <<
                                    " OPFLAGS: " << hex << redoLogRecord2->opFlags << endl;
                    } else {
                        if ((oracleAnalyser->trace2 & TRACE2_ROLLBACK) != 0) {
                            cerr << "match part, rolled back" << endl;
                        }
                    }
                }
            }

            break;
        }
    }

    void OracleAnalyserRedoLog::dumpRedoVector() {
        if (oracleAnalyser->trace >= TRACE_WARN) {
            cerr << "WARNING: Dumping redo Vector" << endl;
            cerr << "WARNING: ##: " << dec << recordLength4;
            for (uint64_t j = 0; j < recordLength4; ++j) {
                if ((j & 0x0F) == 0)
                    cerr << endl << "WARNING: ##  " << setfill(' ') << setw(2) << hex << j << ": ";
                if ((j & 0x07) == 0)
                    cerr << " ";
                cerr << setfill('0') << setw(2) << hex << (uint64_t)oracleAnalyser->recordBuffer[j] << " ";
            }
            cerr << endl;
        }
    }

    void OracleAnalyserRedoLog::flushTransactions(typescn checkpointScn) {
        bool shutdown = false;
        Transaction *transaction = oracleAnalyser->transactionHeap.top();
        if ((oracleAnalyser->trace2 & TRACE2_CHECKPOINT_FLUSH) != 0) {
            cerr << "FLUSH" << endl;
            oracleAnalyser->dumpTransactions();
        }

        while (transaction != nullptr) {
            if (oracleAnalyser->trace >= TRACE_FULL)
                cerr << "FULL: " << *transaction << endl;

            if (transaction->lastScn <= checkpointScn && transaction->isCommit) {
                if (transaction->lastScn > oracleAnalyser->databaseScn) {
                    if (transaction->isBegin)  {
                        if (transaction->shutdown)
                            shutdown = true;
                        else
                            transaction->flush(oracleAnalyser);
                    } else {
                        if (oracleAnalyser->trace >= TRACE_WARN) {
                            cerr << "WARNING: skipping transaction with no begin: " << *transaction << endl;
                            oracleAnalyser->dumpTransactions();
                        }
                    }
                } else {
                    if (oracleAnalyser->trace >= TRACE_DETAIL) {
                        cerr << "INFO: skipping transaction already committed: " << *transaction << endl;
                    }
                }

                oracleAnalyser->transactionHeap.pop();
                if (transaction->opCodes > 0)
                    oracleAnalyser->lastOpTransactionMap.erase(transaction);

                oracleAnalyser->xidTransactionMap.erase(transaction->xid);
                if (oracleAnalyser->trace >= TRACE_FULL)
                    cerr << "FULL: dropping" << endl;
                oracleAnalyser->transactionBuffer->deleteTransactionChunks(transaction->firstTc, transaction->lastTc);
                delete transaction;

                transaction = oracleAnalyser->transactionHeap.top();
            } else
                break;
        }

        if ((oracleAnalyser->trace2 & TRACE2_DUMP) != 0) {
            for (auto const& xid : oracleAnalyser->xidTransactionMap) {
                Transaction *transaction = oracleAnalyser->xidTransactionMap[xid.first];
                cerr << "DUMP: " << *transaction << endl;
            }
        }

        if (checkpointScn > oracleAnalyser->databaseScn) {
            if (oracleAnalyser->trace >= TRACE_FULL) {
                if (oracleAnalyser->version >= 0x12200)
                    cerr << "INFO: Updating checkpoint SCN to: " << PRINTSCN64(checkpointScn) << endl;
                else
                    cerr << "INFO: Updating checkpoint SCN to: " << PRINTSCN48(checkpointScn) << endl;
            }
            oracleAnalyser->databaseScn = checkpointScn;
        }
        lastCheckpointScn = checkpointScn;

        if (shutdown)
            stopMain();
    }

    void OracleAnalyserRedoLog::resetRedo() {
        lastCheckpointScn = 0;
        extScn = 0;
        curScn = ZERO_SCN;
        curScnPrev = ZERO_SCN;
        curSubScn = 0;
        recordBeginPos = 0;
        recordBeginBlock = 0;
        recordTimestmap = 0;
        recordPos = 0;
        recordLeftToCopy = 0;
        recordLength4 = 0;
        blockNumber = 2;
    }

    void OracleAnalyserRedoLog::continueRedo(OracleAnalyserRedoLog *prev) {
        lastCheckpointScn = prev->lastCheckpointScn;
        extScn = prev->extScn;
        curScn = prev->curScn;
        curScnPrev = prev->curScnPrev;
        curSubScn = prev->curSubScn;
        recordBeginPos = prev->recordBeginPos;
        recordBeginBlock = prev->recordBeginBlock;
        recordTimestmap = prev->recordTimestmap;
        recordPos = prev->recordPos;
        recordLeftToCopy = prev->recordLeftToCopy;
        recordLength4 = prev->recordLength4;
        blockNumber = prev->blockNumber;

        reader->bufferStart = prev->reader->bufferStart;
        reader->bufferEnd = prev->reader->bufferEnd;
    }

    uint64_t OracleAnalyserRedoLog::processLog() {
        cerr << "Processing log: " << *this << endl;
        if (oracleAnalyser->trace < TRACE_INFO)
            cerr << endl;
        uint64_t blockPos = 16, bufferPos = 0;
        uint64_t curBufferStart = 0, curBufferEnd = 0, curRet, curStatus;

        if (reader->bufferStart == reader->blockSize * 2) {
            if (oracleAnalyser->dumpRedoLog >= 1) {
                stringstream name;
                name << "DUMP-" << sequence << ".trace";
                oracleAnalyser->dumpStream.open(name.str());
                if (!oracleAnalyser->dumpStream.is_open()) {
                    cerr << "ERORR: can't open " << name.str() << " for write. Aborting log dump." << endl;
                    oracleAnalyser->dumpRedoLog = 0;
                }
                printHeaderInfo();
            }
        }

        clock_t cStart = clock();
        {
            unique_lock<mutex> lck(oracleAnalyser->mtx);
            reader->status = READER_STATUS_READ;
            curBufferEnd = reader->bufferEnd;
            oracleAnalyser->readerCond.notify_all();
            oracleAnalyser->sleepingCond.notify_all();
        }
        curBufferStart = reader->bufferStart;
        bufferPos = (blockNumber * reader->blockSize) % DISK_BUFFER_SIZE;

        while (!oracleAnalyser->shutdown) {
            //there is some work to do
            while (curBufferStart < curBufferEnd) {
                if ((oracleAnalyser->trace2 & TRACE2_VECTOR) != 0)
                    cerr << "VECTOR: block " << dec << (curBufferStart / reader->blockSize) << " left: " << dec << recordLeftToCopy << ", last length: "
                            << recordLength4 << endl;

                blockPos = 16;
                while (blockPos < reader->blockSize) {
                    //next record
                    if (recordLeftToCopy == 0) {
                        if (blockPos + 20 >= reader->blockSize)
                            break;

                        recordLength4 = (oracleAnalyser->read32(reader->redoBuffer + bufferPos + blockPos) + 3) & 0xFFFFFFFC;
                        recordLeftToCopy = recordLength4;
                        if (recordLength4 > REDO_RECORD_MAX_SIZE) {
                            dumpRedoVector();
                            cerr << "WARNING: too big log record: " << dec << recordLeftToCopy << " bytes" << endl;
                            throw RedoLogException("too big log record");
                        }

                        recordPos = 0;
                        recordBeginPos = blockPos;
                        recordBeginBlock = blockNumber;
                    }

                    //nothing more
                    if (recordLeftToCopy == 0)
                        break;

                    uint64_t toCopy;
                    if (blockPos + recordLeftToCopy > reader->blockSize)
                        toCopy = reader->blockSize - blockPos;
                    else
                        toCopy = recordLeftToCopy;

                    memcpy(oracleAnalyser->recordBuffer + recordPos, reader->redoBuffer + bufferPos + blockPos, toCopy);
                    recordLeftToCopy -= toCopy;
                    blockPos += toCopy;
                    recordPos += toCopy;

                    if (recordLeftToCopy == 0) {
                        if ((oracleAnalyser->trace2 & TRACE2_VECTOR) != 0)
                            cerr << "VECTOR: * block: " << dec << recordBeginBlock << " pos: " << dec << recordBeginPos << ", length: " << recordLength4 << endl;

                        try {
                            analyzeRecord();
                        } catch(RedoLogException &ex) {
                            if (oracleAnalyser->trace >= TRACE_WARN)
                                cerr << "WARNING: " << ex.msg << " forced to continue working" << endl;
                            if ((oracleAnalyser->flags & REDO_FLAGS_ON_ERROR_CONTINUE) == 0)
                                throw new RuntimeException(ex.msg);
                        }
                    }
                }

                ++blockNumber;
                curBufferStart += reader->blockSize;
                bufferPos += reader->blockSize;
                if (bufferPos == DISK_BUFFER_SIZE)
                    bufferPos = 0;

                if (curBufferStart - reader->bufferStart > DISK_BUFFER_SIZE / 16) {
                    unique_lock<mutex> lck(oracleAnalyser->mtx);
                    reader->bufferStart = curBufferStart;
                    curBufferEnd = reader->bufferEnd;
                    if (reader->status == READER_STATUS_READ)
                        oracleAnalyser->readerCond.notify_all();
                }

                oracleAnalyser->checkForCheckpoint();
            }

            {
                unique_lock<mutex> lck(oracleAnalyser->mtx);
                curBufferEnd = reader->bufferEnd;
                curStatus = reader->status;
                curRet = reader->ret;
                if (reader->bufferStart < curBufferStart) {
                    reader->bufferStart = curBufferStart;
                    if (reader->status == READER_STATUS_READ)
                        oracleAnalyser->readerCond.notify_all();
                }

                //all work done
                if (curBufferStart == curBufferEnd) {
                    if (curRet == REDO_FINISHED || curRet == REDO_OVERWRITTEN || curStatus == READER_STATUS_SLEEPING)
                        break;

                    oracleAnalyser->readerCond.notify_all();
                    oracleAnalyser->analyserCond.wait(lck);
                }
            }
        }

        if (curRet == REDO_FINISHED && curScn != ZERO_SCN)
            flushTransactions(curScn);

        if ((oracleAnalyser->trace2 & TRACE2_PERFORMANCE) != 0) {
            clock_t cEnd = clock();
            double mySpeed = 0, myTime = 1000.0 * (cEnd-cStart) / CLOCKS_PER_SEC;
            if (myTime > 0)
                mySpeed = (uint64_t)blockNumber * reader->blockSize / 1024 / 1024 / myTime * 1000;
            cerr << "PERFORMANCE: Redo processing time: " << myTime << " ms Speed: " << fixed << setprecision(2) << mySpeed << " MB/s" << endl;
        }

        if (oracleAnalyser->dumpRedoLog >= 1 && oracleAnalyser->dumpStream.is_open())
            oracleAnalyser->dumpStream.close();

        return curRet;
    }

    OracleAnalyserRedoLog::~OracleAnalyserRedoLog() {
    }

    ostream& operator<<(ostream& os, const OracleAnalyserRedoLog& ors) {
        os << "(" << dec << ors.group << ", " << ors.firstScn << ", " << ors.sequence << ", \"" << ors.path << "\")";
        return os;
    }
}
