/* Class reading a redo log file
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <thread>
#include <signal.h>
#include <string.h>

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
            vectors(0),
            group(group),
            path(path),
            sequence(0),
            firstScn(firstScn),
            nextScn(nextScn),
            reader(nullptr) {
        memset(&zero, 0, sizeof(struct RedoLogRecord));
    }

    OracleAnalyserRedoLog::~OracleAnalyserRedoLog() {
        for (uint64_t i = 0; i < vectors; ++i) {
            if (opCodes[i] != nullptr) {
                delete opCodes[i];
                opCodes[i] = nullptr;
            }
        }
    }


    void OracleAnalyserRedoLog::printHeaderInfo(void) {

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

            oracleAnalyser->dumpStream << " FILE HEADER:" << endl <<
                    "\tCompatibility Vsn = " << dec << reader->compatVsn << "=0x" << hex << reader->compatVsn << endl <<
                    "\tDb ID=" << dec << dbid << "=0x" << hex << dbid << ", Db Name='" << SID << "'" << endl <<
                    "\tActivation ID=" << dec << reader->activationRead << "=0x" << hex << reader->activationRead << endl <<
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
                        " resetlogs count: 0x" << hex << reader->resetlogsRead << " scn: " << PRINTSCN48(resetlogsScn) << " (" << dec << resetlogsScn << ")" << endl <<
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
                        " resetlogs count: 0x" << hex << reader->resetlogsRead << " scn: " << PRINTSCN64(resetlogsScn) << endl <<
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

    void OracleAnalyserRedoLog::analyzeRecord(void) {
        RedoLogRecord redoLogRecord[VECTOR_MAX_LENGTH];
        uint64_t isUndoRedo[VECTOR_MAX_LENGTH];
        uint64_t opCodesUndo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsUndo = 0;
        uint64_t opCodesRedo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsRedo = 0;

        for (uint64_t i = 0; i < vectors; ++i) {
            if (opCodes[i] != nullptr) {
                delete opCodes[i];
                opCodes[i] = nullptr;
            }
        }

        vectors = 0;
        memset(opCodes, 0, sizeof(opCodes));
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
            numChk = oracleAnalyser->read16(oracleAnalyser->recordBuffer + 24);
            numChkMax = oracleAnalyser->read16(oracleAnalyser->recordBuffer + 26);
            recordTimestmap = oracleAnalyser->read32(oracleAnalyser->recordBuffer + 64);
            if (numChk + 1 == numChkMax) {
                extScn = oracleAnalyser->readSCN(oracleAnalyser->recordBuffer + 40);
            }
            TRACE(TRACE2_DUMP, "C scn: " << PRINTSCN64(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn << " CHECKPOINT at " <<
                    PRINTSCN64(extScn));
        } else {
            headerLength = 24;
            TRACE(TRACE2_DUMP, "  scn: " << PRINTSCN64(curScn) << "." << setfill('0') << setw(4) << hex << curSubScn);
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
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << curSubScn << " " << recordTimestmap << endl;
                else
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << curSubScn << " " << recordTimestmap << endl;
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
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN48(curScn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << curSubScn << " " << recordTimestmap << endl;
                else
                    oracleAnalyser->dumpStream << "SCN: " << PRINTSCN64(curScn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << curSubScn << " " << recordTimestmap << endl;
            }
        }

        if (headerLength > recordLength) {
            dumpRedoVector();
            REDOLOG_FAIL("too small log record, header length: " << dec << headerLength << ", field length: " << recordLength);
        }

        uint64_t pos = headerLength;
        while (pos < recordLength) {
            memset(&redoLogRecord[vectors], 0, sizeof(struct RedoLogRecord));
            redoLogRecord[vectors].vectorNo = vectors + 1;
            redoLogRecord[vectors].cls = oracleAnalyser->read16(oracleAnalyser->recordBuffer + pos + 2);
            redoLogRecord[vectors].afn = oracleAnalyser->read32(oracleAnalyser->recordBuffer + pos + 4) & 0xFFFF;
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
                redoLogRecord[vectors].conId = oracleAnalyser->read16(oracleAnalyser->recordBuffer + pos + 24);
            } else {
                fieldOffset = 24;
                redoLogRecord[vectors].flgRecord = 0;
                redoLogRecord[vectors].conId = 0;
            }

            if (pos + fieldOffset + 1 >= recordLength) {
                dumpRedoVector();
                REDOLOG_FAIL("position of field list (" << dec << (pos + fieldOffset + 1) << ") outside of record, length: " << recordLength);
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
                    dumpRedoVector();
                    REDOLOG_FAIL("position of field list outside of record (" <<
                            "i: " << dec << i <<
                            " c: " << dec << redoLogRecord[vectors].fieldCnt << " " <<
                            " o: " << dec << fieldOffset <<
                            " p: " << dec << pos <<
                            " l: " << dec << redoLogRecord[vectors].length <<
                            " r: " << dec << recordLength << ")");
                }
            }

            if (redoLogRecord[vectors].fieldPos > redoLogRecord[vectors].length) {
                dumpRedoVector();
                REDOLOG_FAIL("incomplete record, pos: " << dec << redoLogRecord[vectors].fieldPos << ", length: " << redoLogRecord[vectors].length);
            }

            redoLogRecord[vectors].recordObjn = 0xFFFFFFFF;
            redoLogRecord[vectors].recordObjd = 0xFFFFFFFF;

            pos += redoLogRecord[vectors].length;

            switch (redoLogRecord[vectors].opCode) {
            case 0x0501: //Undo
                opCodes[vectors] = new OpCode0501(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0501) << " bytes memory for (reason: OP 5.1)");
                }
                break;

            case 0x0502: //Begin transaction
                opCodes[vectors] = new OpCode0502(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0502) << " bytes memory for (reason: OP 5.2)");
                }
                break;

            case 0x0504: //Commit/rollback transaction
                opCodes[vectors] = new OpCode0504(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0504) << " bytes memory for (reason: OP 5.4)");
                }
                break;

            case 0x0506: //Partial rollback
                opCodes[vectors] = new OpCode0506(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0506) << " bytes memory for (reason: OP 5.6)");
                }
                break;

            case 0x050B:
                opCodes[vectors] = new OpCode050B(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode050B) << " bytes memory for (reason: OP 5.11)");
                }
                break;

            case 0x0513: //Session information
                opCodes[vectors] = new OpCode0513(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0513) << " bytes memory for (reason: OP 5.19)");
                }
                break;

            case 0x0514: //Session information
                opCodes[vectors] = new OpCode0514(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0514) << " bytes memory for (reason: OP 5.20)");
                }
                break;

            case 0x0B02: //REDO: Insert row piece
                opCodes[vectors] = new OpCode0B02(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B02) << " bytes memory for (reason: OP 11.2)");
                }
                break;

            case 0x0B03: //REDO: Delete row piece
                opCodes[vectors] = new OpCode0B03(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B03) << " bytes memory for (reason: OP 11.3)");
                }
                break;

            case 0x0B04: //REDO: Lock row piece
                opCodes[vectors] = new OpCode0B04(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B04) << " bytes memory for (reason: OP 11.4)");
                }
                break;

            case 0x0B05: //REDO: Update row piece
                opCodes[vectors] = new OpCode0B05(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B05) << " bytes memory for (reason: OP 11.5)");
                }
                break;

            case 0x0B06: //REDO: Overwrite row piece
                opCodes[vectors] = new OpCode0B06(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B06) << " bytes memory for (reason: OP 11.6)");
                }
                break;

            case 0x0B08: //REDO: Change forwarding address
                opCodes[vectors] = new OpCode0B08(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B08) << " bytes memory for (reason: OP 11.8)");
                }
                break;

            case 0x0B0B: //REDO: Insert multiple rows
                opCodes[vectors] = new OpCode0B0B(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B0B) << " bytes memory for (reason: OP 11.11)");
                }
                break;

            case 0x0B0C: //REDO: Delete multiple rows
                opCodes[vectors] = new OpCode0B0C(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B0C) << " bytes memory for (reason: OP 11.12)");
                }
                break;

            case 0x0B10: //REDO: Supplemental log for update
                opCodes[vectors] = new OpCode0B10(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode0B10) << " bytes memory for (reason: OP 11.16)");
                }
                break;

            case 0x1801: //DDL
                opCodes[vectors] = new OpCode1801(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode1801) << " bytes memory for (reason: OP 24.1)");
                }
                break;

            default:
                opCodes[vectors] = new OpCode(oracleAnalyser, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(OpCode) << " bytes memory for (reason: OP)");
                }
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
    }

    void OracleAnalyserRedoLog::appendToTransaction(RedoLogRecord *redoLogRecord) {
        TRACE(TRACE2_DUMP, *redoLogRecord);

        //DDL
        if (redoLogRecord->opCode == 0x1801) {
            //skip other PDB vectors
            if (oracleAnalyser->conId > 0 && redoLogRecord->conId != oracleAnalyser->conId)
                return;

            //track DDL
            if ((oracleAnalyser->flags & REDO_FLAGS_TRACK_DDL) == 0)
                return;

            redoLogRecord->object = oracleAnalyser->checkDict(redoLogRecord->objn, redoLogRecord->objd);
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0)
                return;

            Transaction *transaction = oracleAnalyser->xidTransactionMap[redoLogRecord->xid];
            if (transaction == nullptr) {
                transaction = new Transaction(oracleAnalyser, redoLogRecord->xid);
                if (transaction == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(Transaction) << " bytes memory for (reason: append to transaction#1)");
                }
                oracleAnalyser->xidTransactionMap[redoLogRecord->xid] = transaction;

                transaction->add(redoLogRecord, &zero, sequence, curScn);
                oracleAnalyser->transactionHeap->add(transaction);
            } else {
                if (transaction->opCodes > 0)
                    oracleAnalyser->lastOpTransactionMap->erase(transaction);

                transaction->add(redoLogRecord, &zero, sequence, curScn);
                oracleAnalyser->transactionHeap->update(transaction->pos);
            }
            oracleAnalyser->lastOpTransactionMap->set(transaction);

            return;
        } else
        if (redoLogRecord->opCode == 0x0501) {
            if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0)
                return;

            redoLogRecord->object = oracleAnalyser->checkDict(redoLogRecord->objn, redoLogRecord->objd);
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0)
                return;
        } else
        if (redoLogRecord->opCode != 0x0502 && redoLogRecord->opCode != 0x0504)
            return;

        Transaction *transaction = oracleAnalyser->xidTransactionMap[redoLogRecord->xid];
        if (transaction == nullptr) {
            transaction = new Transaction(oracleAnalyser, redoLogRecord->xid);
            if (transaction == nullptr) {
                RUNTIME_FAIL("could not allocate " << dec << sizeof(Transaction) << " bytes memory for (reason: append to transaction#2)");
            }
            oracleAnalyser->xidTransactionMap[redoLogRecord->xid] = transaction;

            transaction->touch(curScn, sequence);
            oracleAnalyser->transactionHeap->add(transaction);
        } else
            transaction->touch(curScn, sequence);

        if (redoLogRecord->opCode == 0x0501) {
            transaction->addSplitBlock(redoLogRecord);
        } else

        if (redoLogRecord->opCode == 0x0502) {
            transaction->isBegin = true;
        } else

        if (redoLogRecord->opCode == 0x0504) {
            transaction->isCommit = true;
            transaction->commitTime = recordTimestmap;
            if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
                transaction->isRollback = true;
            oracleAnalyser->transactionHeap->update(transaction->pos);
        }
    }

    void OracleAnalyserRedoLog::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        bool shutdown = false;
        TRACE(TRACE2_DUMP, *redoLogRecord1);
        TRACE(TRACE2_DUMP, *redoLogRecord2);

        //skip other PDB vectors
        if (oracleAnalyser->conId > 0 && redoLogRecord2->conId != oracleAnalyser->conId && redoLogRecord1->opCode == 0x0501)
            return;
        if (oracleAnalyser->conId > 0 && redoLogRecord1->conId != oracleAnalyser->conId &&
                (redoLogRecord2->opCode == 0x0506 || redoLogRecord2->opCode == 0x050B))
            return;

        typeobj objn, objd;
        if (redoLogRecord1->objd != 0) {
            objn = redoLogRecord1->objn;
            objd = redoLogRecord1->objd;
            redoLogRecord2->objn = redoLogRecord1->objn;
            redoLogRecord2->objd = redoLogRecord1->objd;
        } else {
            objn = redoLogRecord2->objn;
            objd = redoLogRecord2->objd;
            redoLogRecord1->objn = redoLogRecord2->objn;
            redoLogRecord1->objd = redoLogRecord2->objd;
        }

        if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0) {
            if (oracleAnalyser->dumpRedoLog >= 1)
                oracleAnalyser->dumpStream << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            REDOLOG_FAIL("BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")");
        }

        redoLogRecord1->object = oracleAnalyser->checkDict(objn, objd);
        if (redoLogRecord1->object == nullptr)
            return;

        //cluster key
        if ((redoLogRecord1->fb & FB_K) != 0 || (redoLogRecord2->fb & FB_K) != 0)
            return;

        //partition move
        if ((redoLogRecord1->suppLogFb & FB_K) != 0 || (redoLogRecord2->suppLogFb & FB_K) != 0)
            return;

        redoLogRecord2->object = redoLogRecord1->object;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        if (redoLogRecord1->object->options == 1 && opCodeLong == 0x05010B02) {
            INFO("found shutdown command in events table");
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
                    TRACE(TRACE2_ROLLBACK, "rolling transaction part UBA: " << PRINTUBA(redoLogRecord1->uba) <<
                                " DBA: 0x" << hex << redoLogRecord1->dba <<
                                " SLT: " << dec << (uint64_t)redoLogRecord1->slt <<
                                " RCI: " << dec << (uint64_t)redoLogRecord1->rci <<
                                " SCN: " << PRINTSCN64(redoLogRecord1->scnRecord) <<
                                " OPFLAGS: " << hex << redoLogRecord2->opFlags);
                    break;
                }

                Transaction *transaction = oracleAnalyser->xidTransactionMap[redoLogRecord1->xid];
                if (transaction == nullptr) {
                    transaction = new Transaction(oracleAnalyser, redoLogRecord1->xid);
                    if (transaction == nullptr) {
                        RUNTIME_FAIL("could not allocate " << dec << sizeof(Transaction) << " bytes memory for (reason: append to transaction#3)");
                    }
                    oracleAnalyser->xidTransactionMap[redoLogRecord1->xid] = transaction;

                    //process split block
                    if ((redoLogRecord1->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) != 0)
                        transaction->addSplitBlock(redoLogRecord1, redoLogRecord2);
                    else {
                        oracleAnalyser->printRollbackInfo(redoLogRecord1, redoLogRecord2, transaction, "new transaction");
                        transaction->add(redoLogRecord1, redoLogRecord2, sequence, curScn);
                        oracleAnalyser->lastOpTransactionMap->set(transaction);
                    }
                    oracleAnalyser->transactionHeap->add(transaction);
                } else {
                    if ((redoLogRecord1->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) != 0)
                        transaction->addSplitBlock(redoLogRecord1, redoLogRecord2);
                    else {
                        if (transaction->opCodes > 0)
                            oracleAnalyser->lastOpTransactionMap->erase(transaction);

                        oracleAnalyser->printRollbackInfo(redoLogRecord1, redoLogRecord2, transaction, "");
                        transaction->add(redoLogRecord1, redoLogRecord2, sequence, curScn);
                        oracleAnalyser->transactionHeap->update(transaction->pos);
                        oracleAnalyser->lastOpTransactionMap->set(transaction);
                    }
                }
                transaction->shutdown = shutdown;
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
                Transaction *transaction = oracleAnalyser->lastOpTransactionMap->getMatchForRollback(redoLogRecord1, redoLogRecord2);

                //match
                if (transaction != nullptr) {
                    oracleAnalyser->printRollbackInfo(redoLogRecord1, redoLogRecord2, transaction, "match, rolled back");

                    oracleAnalyser->lastOpTransactionMap->erase(transaction);
                    transaction->rollbackLastOp(curScn);
                    oracleAnalyser->transactionHeap->update(transaction->pos);

                    if (transaction->opCodes > 0)
                        oracleAnalyser->lastOpTransactionMap->set(transaction);
                } else {
                    //check all previous transactions
                    bool foundPrevious = false;

                    for (uint64_t i = 1; i <= oracleAnalyser->transactionHeap->size; ++i) {
                        transaction = oracleAnalyser->transactionHeap->at(i);

                        if (transaction->opCodes > 0 &&
                                transaction->rollbackPartOp(redoLogRecord1, redoLogRecord2, curScn)) {
                            oracleAnalyser->printRollbackInfo(redoLogRecord1, redoLogRecord2, transaction, "partial match, rolled back");
                            oracleAnalyser->transactionHeap->update(transaction->pos);
                            foundPrevious = true;
                            break;
                        }
                    }

                    if (!foundPrevious) {
                        oracleAnalyser->printRollbackInfo(redoLogRecord1, redoLogRecord2, nullptr, "no match");
                        oracleAnalyser->addToRollbackList(redoLogRecord1, redoLogRecord2);
                    }
                }
            }

            break;
        }
    }

    void OracleAnalyserRedoLog::dumpRedoVector(void) {

        if (oracleAnalyser->trace >= TRACE_WARNING) {
            stringstream ss;
            ss << "WARNING: Dumping redo Vector" << endl;
            ss << "WARNING: ##: " << dec << recordLength4;
            for (uint64_t j = 0; j < recordLength4; ++j) {
                if ((j & 0x0F) == 0)
                    ss << endl << "WARNING: ##  " << setfill(' ') << setw(2) << hex << j << ": ";
                if ((j & 0x07) == 0)
                    ss << " ";
                ss << setfill('0') << setw(2) << hex << (uint64_t)oracleAnalyser->recordBuffer[j] << " ";
            }
            ss << endl;
            OUT(ss.str());
        }
    }

    void OracleAnalyserRedoLog::flushTransactions(typescn checkpointScn) {
        bool shutdownInstructed = false;
        Transaction *transaction = oracleAnalyser->transactionHeap->top();
        TRACE(TRACE2_CHECKPOINT_FLUSH, "flush:" << endl << *oracleAnalyser);

        while (transaction != nullptr) {
            TRACE(TRACE2_DUMP, *transaction);

            if (transaction->lastScn <= checkpointScn && transaction->isCommit) {
                if (transaction->lastScn > oracleAnalyser->databaseScn) {
                    if (!transaction->isBegin)  {
                        INFO("skipping transaction with no begin: " << *transaction);
                        FULL(*oracleAnalyser);
                    }

                    if (transaction->isBegin || (oracleAnalyser->flags & REDO_FLAGS_INCOMPLETE_TRANSACTIONS) != 0) {
                        if (transaction->shutdown) {
                            shutdownInstructed = true;
                        } else {
                            transaction->flush();
                        }
                    }
                } else {
                    INFO("skipping transaction already committed: " << *transaction);
                }

                oracleAnalyser->transactionHeap->pop();
                if (transaction->opCodes > 0)
                    oracleAnalyser->lastOpTransactionMap->erase(transaction);

                oracleAnalyser->xidTransactionMap.erase(transaction->xid);
                delete transaction;

                transaction = oracleAnalyser->transactionHeap->top();
            } else
                break;
        }

        if (checkpointScn > oracleAnalyser->databaseScn) {
            FULL("updating checkpoint SCN to: " << PRINTSCN64(checkpointScn));
            oracleAnalyser->databaseScn = checkpointScn;
        }
        lastCheckpointScn = checkpointScn;

        if (shutdownInstructed)
            stopMain();
    }

    void OracleAnalyserRedoLog::resetRedo(void) {
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

    uint64_t OracleAnalyserRedoLog::processLog(void) {
        if (firstScn == ZERO_SCN && nextScn == ZERO_SCN && reader->firstScn != 0) {
            firstScn = reader->firstScn;
            nextScn = reader->nextScn;
        }
        INFO("processing redo log: " << *this);
        uint64_t blockPos = 16, bufferPos = 0, blockNumberStart = blockNumber;
        uint64_t curBufferStart = 0, curBufferEnd = 0, curRet, curStatus;
        oracleAnalyser->suppLogSize = 0;

        if (reader->bufferStart == reader->blockSize * 2) {
            if (oracleAnalyser->dumpRedoLog >= 1) {
                stringstream name;
                name << oracleAnalyser->databaseContext.c_str() << "-" << dec << sequence << ".logdump";
                oracleAnalyser->dumpStream.open(name.str());
                if (!oracleAnalyser->dumpStream.is_open()) {
                    WARNING("can't open " << name.str() << " for write. Aborting log dump.");
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
                TRACE(TRACE2_VECTOR, "block " << dec << (curBufferStart / reader->blockSize) << " left: " << dec << recordLeftToCopy << ", last length: "
                            << recordLength4);

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
                            REDOLOG_FAIL("too big log record: " << dec << recordLeftToCopy << " bytes");
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
                        TRACE(TRACE2_VECTOR, "* block: " << dec << recordBeginBlock << " pos: " << dec << recordBeginPos << ", length: " << recordLength4);

                        try {
                            analyzeRecord();
                        } catch(RedoLogException &ex) {
                            if ((oracleAnalyser->flags & REDO_FLAGS_ON_ERROR_CONTINUE) == 0) {
                                RUNTIME_FAIL("runtime error, aborting further redo log processing");
                            } else
                                WARNING("forced to continue working in spite of error");
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
                    if (reader->status == READER_STATUS_READ) {
                        oracleAnalyser->readerCond.notify_all();
                    }
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
                    if (reader->status == READER_STATUS_READ) {
                        oracleAnalyser->readerCond.notify_all();
                    }
                }

                //all work done
                if (curBufferStart == curBufferEnd) {
                    if (curRet == REDO_FINISHED || curRet == REDO_OVERWRITTEN || curStatus == READER_STATUS_SLEEPING)
                        break;
                    oracleAnalyser->analyserCond.wait(lck);
                }
            }
        }

        if (curRet == REDO_FINISHED && curScn != ZERO_SCN) {
            Transaction *transaction;
            for (uint64_t i = 1; i <= oracleAnalyser->transactionHeap->size; ++i) {
                transaction = oracleAnalyser->transactionHeap->at(i);
                transaction->flushSplitBlocks();
            }

            flushTransactions(curScn);
        }

        clock_t cEnd = clock();
        double mySpeed = 0, myTime = 1000.0 * (cEnd-cStart) / CLOCKS_PER_SEC, suppLogPercent = 0.0;
        if (blockNumber != blockNumberStart)
            suppLogPercent = 100.0 * oracleAnalyser->suppLogSize / ((blockNumber - blockNumberStart)* reader->blockSize);
        if (myTime > 0)
            mySpeed = (blockNumber - blockNumberStart) * reader->blockSize / 1024 / 1024 / myTime * 1000;

        TRACE(TRACE2_PERFORMANCE, "redo processing time: " << myTime << " ms, " <<
                "Speed: " << fixed << setprecision(2) << mySpeed << " MB/s, " <<
                "Redo log size: " << dec << ((blockNumber - blockNumberStart) * reader->blockSize / 1024) << " kB, " <<
                "Supplemental redo log size: " << dec << oracleAnalyser->suppLogSize << " bytes " <<
                "(" << fixed << setprecision(2) << suppLogPercent << " %)");

        if (oracleAnalyser->dumpRedoLog >= 1 && oracleAnalyser->dumpStream.is_open())
            oracleAnalyser->dumpStream.close();

        return curRet;
    }

    ostream& operator<<(ostream& os, const OracleAnalyserRedoLog& ors) {
        os << "group: " << dec << ors.group << " scn: " << ors.firstScn << " to " <<
                ((ors.nextScn != ZERO_SCN) ? ors.nextScn : 0) << " sequence: " << ors.sequence << " path: " << ors.path;
        return os;
    }
}
