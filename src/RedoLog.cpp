/* Class reading a redo log file
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
#include "OracleAnalyzer.h"
#include "OracleObject.h"
#include "Reader.h"
#include "RedoLog.h"
#include "RedoLogException.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "Transaction.h"

using namespace std;

extern void stopMain();

namespace OpenLogReplicator {

    RedoLog::RedoLog(OracleAnalyzer *oracleAnalyzer, int64_t group, const char *path) :
            oracleAnalyzer(oracleAnalyzer),
            vectors(0),
            lwnConfirmedBlock(2),
            lwnAllocated(0),
            lwnTimestamp(0),
            lwnScn(0),
            lwnScnMax(0),
            lwnRecords(0),
            lwnStartBlock(0),
            shutdown(false),
            group(group),
            path(path),
            sequence(0),
            firstScn(firstScn),
            nextScn(nextScn),
            reader(nullptr) {
        memset(&zero, 0, sizeof(struct RedoLogRecord));

        lwnChunks[0] = oracleAnalyzer->getMemoryChunk("LWN", false);
        uint64_t *length = (uint64_t *)lwnChunks[0];
        *length = sizeof(uint64_t);
        lwnAllocated = 1;
    }

    RedoLog::~RedoLog() {
        while (lwnAllocated > 0)
            oracleAnalyzer->freeMemoryChunk("LWN", lwnChunks[--lwnAllocated], false);

        for (uint64_t i = 0; i < vectors; ++i) {
            if (opCodes[i] != nullptr) {
                delete opCodes[i];
                opCodes[i] = nullptr;
            }
        }
    }

    void RedoLog::printHeaderInfo(void) const {

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            char SID[9];
            memcpy(SID, reader->headerBuffer + reader->blockSize + 28, 8); SID[8] = 0;

            oracleAnalyzer->dumpStream << "DUMP OF REDO FROM FILE '" << path << "'" << endl;
            if (oracleAnalyzer->version >= REDO_VERSION_12_2)
                oracleAnalyzer->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleAnalyzer->dumpStream << " Opcodes *.*" << endl;
            if (oracleAnalyzer->version >= REDO_VERSION_12_2)
                oracleAnalyzer->dumpStream << " Container ID: 0" << endl << " Container UID: 0" << endl;
            oracleAnalyzer->dumpStream << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff" << endl;
            if (oracleAnalyzer->version < REDO_VERSION_12_2)
                oracleAnalyzer->dumpStream << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff" << endl;
            else
                oracleAnalyzer->dumpStream << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff" << endl;
            oracleAnalyzer->dumpStream << " Times: creation thru eternity" << endl;

            uint32_t dbid = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 24);
            uint32_t controlSeq = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 36);
            uint32_t fileSize = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 40);
            uint16_t fileNumber = oracleAnalyzer->read16(reader->headerBuffer + reader->blockSize + 48);

            oracleAnalyzer->dumpStream << " FILE HEADER:" << endl <<
                    "\tCompatibility Vsn = " << dec << reader->compatVsn << "=0x" << hex << reader->compatVsn << endl <<
                    "\tDb ID=" << dec << dbid << "=0x" << hex << dbid << ", Db Name='" << SID << "'" << endl <<
                    "\tActivation ID=" << dec << reader->activationRead << "=0x" << hex << reader->activationRead << endl <<
                    "\tControl Seq=" << dec << controlSeq << "=0x" << hex << controlSeq << ", File size=" << dec << fileSize << "=0x" << hex << fileSize << endl <<
                    "\tFile Number=" << dec << fileNumber << ", Blksiz=" << dec << reader->blockSize << ", File Type=2 LOG" << endl;

            typeSEQ seq = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 8);
            uint8_t descrip[65];
            memcpy (descrip, reader->headerBuffer + reader->blockSize + 92, 64); descrip[64] = 0;
            uint16_t thread = oracleAnalyzer->read16(reader->headerBuffer + reader->blockSize + 176);
            uint32_t nab = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 156);
            uint32_t hws = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 172);
            uint8_t eot = reader->headerBuffer[reader->blockSize + 204];
            uint8_t dis = reader->headerBuffer[reader->blockSize + 205];

            oracleAnalyzer->dumpStream << " descrip:\"" << descrip << "\"" << endl <<
                    " thread: " << dec << thread <<
                    " nab: 0x" << hex << nab <<
                    " seq: 0x" << setfill('0') << setw(8) << hex << (typeSEQ)seq <<
                    " hws: 0x" << hex << hws <<
                    " eot: " << dec << (uint64_t)eot <<
                    " dis: " << dec << (uint64_t)dis << endl;

            typeSCN resetlogsScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 164);
            typeresetlogs prevResetlogsCnt = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 292);
            typeSCN prevResetlogsScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 284);
            typetime firstTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 188));
            typetime nextTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 200));
            typeSCN enabledScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 208);
            typetime enabledTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 216));
            typeSCN threadClosedScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 220);
            typetime threadClosedTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 228));
            typeSCN termialRecScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 240);
            typetime termialRecTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 248));
            typeSCN mostRecentScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 260);
            typesum chSum = oracleAnalyzer->read16(reader->headerBuffer + reader->blockSize + 14);
            typesum chSum2 = reader->calcChSum(reader->headerBuffer + reader->blockSize, reader->blockSize);

            if (oracleAnalyzer->version < REDO_VERSION_12_2) {
                oracleAnalyzer->dumpStream <<
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
                typeSCN realNextScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 272);

                oracleAnalyzer->dumpStream <<
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

            uint32_t largestLwn = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 268);
            oracleAnalyzer->dumpStream <<
                    " Largest LWN: " << dec << largestLwn << " blocks" << endl;

            uint32_t miscFlags = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 236);
            string endOfRedo;
            if ((miscFlags & REDO_END) != 0)
                endOfRedo = "Yes";
            else
                endOfRedo = "No";
            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleAnalyzer->dumpStream << " FailOver End-of-redo stream : " << endOfRedo << endl;
            else
                oracleAnalyzer->dumpStream << " End-of-redo stream : " << endOfRedo << endl;

            if ((miscFlags & REDO_ASYNC) != 0)
                oracleAnalyzer->dumpStream << " Archivelog created using asynchronous network transmittal" << endl;

            if ((miscFlags & REDO_NODATALOSS) != 0)
                oracleAnalyzer->dumpStream << " No data-loss mode" << endl;

            if ((miscFlags & REDO_RESYNC) != 0)
                oracleAnalyzer->dumpStream << " Resynchronization mode" << endl;
            else
                oracleAnalyzer->dumpStream << " Unprotected mode" << endl;

            if ((miscFlags & REDO_CLOSEDTHREAD) != 0)
                oracleAnalyzer->dumpStream << " Closed thread archival" << endl;

            if ((miscFlags & REDO_MAXPERFORMANCE) != 0)
                oracleAnalyzer->dumpStream << " Maximize performance mode" << endl;

            oracleAnalyzer->dumpStream << " Miscellaneous flags: 0x" << hex << miscFlags << endl;

            if (oracleAnalyzer->version >= REDO_VERSION_12_2) {
                uint32_t miscFlags2 = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 296);
                oracleAnalyzer->dumpStream << " Miscellaneous second flags: 0x" << hex << miscFlags2 << endl;
            }

            int32_t thr = (int32_t)oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 432);
            int32_t seq2 = (int32_t)oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 436);
            typeSCN scn2 = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 440);
            uint8_t zeroBlocks = reader->headerBuffer[reader->blockSize + 206];
            uint8_t formatId = reader->headerBuffer[reader->blockSize + 207];
            if (oracleAnalyzer->version < REDO_VERSION_12_2)
                oracleAnalyzer->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN48(scn2) << endl <<
                        " Zero blocks: " << dec << (uint64_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint64_t)formatId << endl;
            else
                oracleAnalyzer->dumpStream << " Thread internal enable indicator: thr: " << dec << thr << "," <<
                        " seq: " << dec << seq2 <<
                        " scn: " << PRINTSCN64(scn2) << endl <<
                        " Zero blocks: " << dec << (uint64_t)zeroBlocks << endl <<
                        " Format ID is " << dec << (uint64_t)formatId << endl;

            uint32_t standbyApplyDelay = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 280);
            if (standbyApplyDelay > 0)
                oracleAnalyzer->dumpStream << " Standby Apply Delay: " << dec << standbyApplyDelay << " minute(s) " << endl;

            typetime standbyLogCloseTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 304));
            if (standbyLogCloseTime.getVal() > 0)
                oracleAnalyzer->dumpStream << " Standby Log Close Time:  " << standbyLogCloseTime << endl;

            oracleAnalyzer->dumpStream << " redo log key is ";
            for (uint64_t i = 448; i < 448 + 16; ++i)
                oracleAnalyzer->dumpStream << setfill('0') << setw(2) << hex << (uint64_t)reader->headerBuffer[reader->blockSize + i];
            oracleAnalyzer->dumpStream << endl;

            uint16_t redoKeyFlag = oracleAnalyzer->read16(reader->headerBuffer + reader->blockSize + 480);
            oracleAnalyzer->dumpStream << " redo log key flag is " << dec << redoKeyFlag << endl;
            uint16_t enabledRedoThreads = 1; //FIXME
            oracleAnalyzer->dumpStream << " Enabled redo threads: " << dec << enabledRedoThreads << " " << endl;
        }
    }

    void RedoLog::analyzeLwn(LwnMember* lwnMember) {
        RedoLogRecord redoLogRecord[VECTOR_MAX_LENGTH];
        uint64_t isUndoRedo[VECTOR_MAX_LENGTH];
        uint64_t opCodesUndo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsUndo = 0;
        uint64_t opCodesRedo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsRedo = 0;
        uint8_t *data = ((uint8_t *)lwnMember) + sizeof(struct LwnMember);

        for (uint64_t i = 0; i < vectors; ++i) {
            if (opCodes[i] != nullptr) {
                delete opCodes[i];
                opCodes[i] = nullptr;
            }
        }

        vectors = 0;
        memset(opCodes, 0, sizeof(opCodes));
        uint32_t recordLength = oracleAnalyzer->read32(data);
        uint8_t vld = data[4];
        uint64_t headerLength;

        if ((vld & 0x04) != 0)
            headerLength = 68;
        else
            headerLength = 24;

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint16_t thread = 1; //FIXME
            oracleAnalyzer->dumpStream << " " << endl;

            if (oracleAnalyzer->version < REDO_VERSION_12_1)
                oracleAnalyzer->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << lwnMember->block << "." <<
                                    setfill('0') << setw(4) << hex << lwnMember->pos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld << endl;
            else {
                uint32_t conUid = oracleAnalyzer->read32(data + 16);
                oracleAnalyzer->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << lwnMember->block << "." <<
                                    setfill('0') << setw(4) << hex << lwnMember->pos <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld <<
                        " CON_UID: " << dec << conUid << endl;
            }

            if (oracleAnalyzer->dumpRawData > 0) {
                oracleAnalyzer->dumpStream << "##: " << dec << headerLength;
                for (uint64_t j = 0; j < headerLength; ++j) {
                    if ((j & 0x0F) == 0)
                        oracleAnalyzer->dumpStream << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
                    if ((j & 0x07) == 0)
                        oracleAnalyzer->dumpStream << " ";
                    oracleAnalyzer->dumpStream << setfill('0') << setw(2) << hex << (uint64_t)data[j] << " ";
                }
                oracleAnalyzer->dumpStream << endl;
            }

            if (headerLength == 68) {
                if (oracleAnalyzer->version < REDO_VERSION_12_2)
                    oracleAnalyzer->dumpStream << "SCN: " << PRINTSCN48(lwnMember->scn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << lwnMember->subScn << " " << lwnTimestamp << endl;
                else
                    oracleAnalyzer->dumpStream << "SCN: " << PRINTSCN64(lwnMember->scn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << lwnMember->subScn << " " << lwnTimestamp << endl;
                uint16_t lwnNst = oracleAnalyzer->read16(data + 26);
                uint32_t lwnLen = oracleAnalyzer->read32(data + 32);

                if (oracleAnalyzer->version < REDO_VERSION_12_2)
                    oracleAnalyzer->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << lwnMember->block << "." <<
                                    setfill('0') << setw(4) << hex << lwnMember->pos <<
                        " LEN: " << setfill('0') << setw(4) << dec << lwnLen <<
                        " NST: " << setfill('0') << setw(4) << dec << lwnNst <<
                        " SCN: " << PRINTSCN48(lwnScn) << ")" << endl;
                else
                    oracleAnalyzer->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << lwnMember->block << "." <<
                                    setfill('0') << setw(4) << hex << lwnMember->pos <<
                        " LEN: 0x" << setfill('0') << setw(8) << hex << lwnLen <<
                        " NST: 0x" << setfill('0') << setw(4) << hex << lwnNst <<
                        " SCN: " << PRINTSCN64(lwnScn) << ")" << endl;
            } else {
                if (oracleAnalyzer->version < REDO_VERSION_12_2)
                    oracleAnalyzer->dumpStream << "SCN: " << PRINTSCN48(lwnMember->scn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << lwnMember->subScn << " " << lwnTimestamp << endl;
                else
                    oracleAnalyzer->dumpStream << "SCN: " << PRINTSCN64(lwnMember->scn) << " SUBSCN:" << setfill(' ') << setw(3) << dec << lwnMember->subScn << " " << lwnTimestamp << endl;
            }
        }

        if (headerLength > recordLength) {
            dumpRedoVector(data, recordLength);
            REDOLOG_FAIL("block: " << dec << lwnMember->block << ", pos: " << lwnMember->pos <<
                    ": too small log record, header length: " << dec << headerLength << ", field length: " << recordLength);
        }

        uint64_t pos = headerLength;
        while (pos < recordLength) {
            memset(&redoLogRecord[vectors], 0, sizeof(struct RedoLogRecord));
            redoLogRecord[vectors].vectorNo = vectors + 1;
            redoLogRecord[vectors].cls = oracleAnalyzer->read16(data + pos + 2);
            redoLogRecord[vectors].afn = oracleAnalyzer->read32(data + pos + 4) & 0xFFFF;
            redoLogRecord[vectors].dba = oracleAnalyzer->read32(data + pos + 8);
            redoLogRecord[vectors].scnRecord = oracleAnalyzer->readSCN(data + pos + 12);
            redoLogRecord[vectors].rbl = 0; //FIXME
            redoLogRecord[vectors].seq = data[pos + 20];
            redoLogRecord[vectors].typ = data[pos + 21];
            int16_t usn = (redoLogRecord[vectors].cls >= 15) ? (redoLogRecord[vectors].cls - 15) / 2 : -1;

            uint64_t fieldOffset;
            if (oracleAnalyzer->version >= REDO_VERSION_12_1) {
                fieldOffset = 32;
                redoLogRecord[vectors].flgRecord = oracleAnalyzer->read16(data + pos + 28);
                redoLogRecord[vectors].conId = oracleAnalyzer->read16(data + pos + 24);
            } else {
                fieldOffset = 24;
                redoLogRecord[vectors].flgRecord = 0;
                redoLogRecord[vectors].conId = 0;
            }

            if (pos + fieldOffset + 1 >= recordLength) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", pos: " << lwnMember->pos <<
                                    ": position of field list (" << dec << (pos + fieldOffset + 1) << ") outside of record, length: " << recordLength);
            }

            uint8_t *fieldList = data + pos + fieldOffset;

            redoLogRecord[vectors].opCode = (((typeop1)data[pos + 0]) << 8) |
                    data[pos + 1];
            redoLogRecord[vectors].length = fieldOffset + ((oracleAnalyzer->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectors].sequence = sequence;
            redoLogRecord[vectors].scn = lwnMember->scn;
            redoLogRecord[vectors].subScn = lwnMember->subScn;
            redoLogRecord[vectors].usn = usn;
            redoLogRecord[vectors].data = data + pos;
            redoLogRecord[vectors].fieldLengthsDelta = fieldOffset;
            if (redoLogRecord[vectors].fieldLengthsDelta + 1 >= recordLength) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", pos: " << lwnMember->pos <<
                                    ": field length list (" << dec << (redoLogRecord[vectors].fieldLengthsDelta) << ") outside of record, length: " << recordLength);
            }
            redoLogRecord[vectors].fieldCnt = (oracleAnalyzer->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) - 2) / 2;
            redoLogRecord[vectors].fieldPos = fieldOffset + ((oracleAnalyzer->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) + 2) & 0xFFFC);
            if (redoLogRecord[vectors].fieldPos >= recordLength) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", pos: " << lwnMember->pos <<
                                    ": fields (" << dec << (redoLogRecord[vectors].fieldPos) << ") outside of record, length: " << recordLength);
            }

            uint64_t fieldPos = redoLogRecord[vectors].fieldPos;
            for (uint64_t i = 1; i <= redoLogRecord[vectors].fieldCnt; ++i) {
                redoLogRecord[vectors].length += (oracleAnalyzer->read16(fieldList + i * 2) + 3) & 0xFFFC;
                fieldPos += (oracleAnalyzer->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta + i * 2) + 3) & 0xFFFC;

                if (pos + redoLogRecord[vectors].length > recordLength) {
                    dumpRedoVector(data, recordLength);
                    REDOLOG_FAIL("block: " << dec << lwnMember->block << ", pos: " << lwnMember->pos <<
                                        ": position of field list outside of record (" <<
                            "i: " << dec << i <<
                            " c: " << dec << redoLogRecord[vectors].fieldCnt << " " <<
                            " o: " << dec << fieldOffset <<
                            " p: " << dec << pos <<
                            " l: " << dec << redoLogRecord[vectors].length <<
                            " r: " << dec << recordLength << ")");
                }
            }

            if (redoLogRecord[vectors].fieldPos > redoLogRecord[vectors].length) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", pos: " << lwnMember->pos <<
                                    ": incomplete record, pos: " << dec << redoLogRecord[vectors].fieldPos << ", length: " << redoLogRecord[vectors].length);
            }

            redoLogRecord[vectors].recordObj = 0xFFFFFFFF;
            redoLogRecord[vectors].recordDataObj = 0xFFFFFFFF;

            pos += redoLogRecord[vectors].length;

            switch (redoLogRecord[vectors].opCode) {
            case 0x0501: //Undo
                opCodes[vectors] = new OpCode0501(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0501) << " bytes memory (for: OP 5.1)");
                }
                break;

            case 0x0502: //Begin transaction
                opCodes[vectors] = new OpCode0502(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0502) << " bytes memory (for: OP 5.2)");
                }
                break;

            case 0x0504: //Commit/rollback transaction
                opCodes[vectors] = new OpCode0504(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0504) << " bytes memory (for: OP 5.4)");
                }
                break;

            case 0x0506: //Partial rollback
                opCodes[vectors] = new OpCode0506(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0506) << " bytes memory (for: OP 5.6)");
                }
                break;

            case 0x050B:
                opCodes[vectors] = new OpCode050B(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode050B) << " bytes memory (for: OP 5.11)");
                }
                break;

            case 0x0513: //Session information
                opCodes[vectors] = new OpCode0513(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0513) << " bytes memory (for: OP 5.19)");
                }
                break;

            case 0x0514: //Session information
                opCodes[vectors] = new OpCode0514(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0514) << " bytes memory (for: OP 5.20)");
                }
                break;

            case 0x0B02: //REDO: Insert row piece
                opCodes[vectors] = new OpCode0B02(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B02) << " bytes memory (for: OP 11.2)");
                }
                break;

            case 0x0B03: //REDO: Delete row piece
                opCodes[vectors] = new OpCode0B03(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B03) << " bytes memory (for: OP 11.3)");
                }
                break;

            case 0x0B04: //REDO: Lock row piece
                opCodes[vectors] = new OpCode0B04(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B04) << " bytes memory (for: OP 11.4)");
                }
                break;

            case 0x0B05: //REDO: Update row piece
                opCodes[vectors] = new OpCode0B05(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B05) << " bytes memory (for: OP 11.5)");
                }
                break;

            case 0x0B06: //REDO: Overwrite row piece
                opCodes[vectors] = new OpCode0B06(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B06) << " bytes memory (for: OP 11.6)");
                }
                break;

            case 0x0B08: //REDO: Change forwarding address
                opCodes[vectors] = new OpCode0B08(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B08) << " bytes memory (for: OP 11.8)");
                }
                break;

            case 0x0B0B: //REDO: Insert multiple rows
                opCodes[vectors] = new OpCode0B0B(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B0B) << " bytes memory (for: OP 11.11)");
                }
                break;

            case 0x0B0C: //REDO: Delete multiple rows
                opCodes[vectors] = new OpCode0B0C(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B0C) << " bytes memory (for: OP 11.12)");
                }
                break;

            case 0x0B10: //REDO: Supplemental log for update
                opCodes[vectors] = new OpCode0B10(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode0B10) << " bytes memory (for: OP 11.16)");
                }
                break;

            case 0x1801: //DDL
                opCodes[vectors] = new OpCode1801(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode1801) << " bytes memory (for: OP 24.1)");
                }
                break;

            default:
                opCodes[vectors] = new OpCode(oracleAnalyzer, &redoLogRecord[vectors]);
                if (opCodes[vectors] == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OpCode) << " bytes memory (for: OP)");
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
                    redoLogRecord[opCodesRedo[vectorsUndo - 1]].recordDataObj = redoLogRecord[opCodesUndo[vectorsUndo - 1]].dataObj;
                    redoLogRecord[opCodesRedo[vectorsUndo - 1]].recordObj = redoLogRecord[opCodesUndo[vectorsUndo - 1]].obj;
                }
            //REDO
            } else if ((redoLogRecord[vectors].opCode & 0xFF00) == 0x0A00 ||
                    (redoLogRecord[vectors].opCode & 0xFF00) == 0x0B00) {
                opCodesRedo[vectorsRedo++] = vectors;
                isUndoRedo[vectors] = 2;
                if (vectorsRedo <= vectorsUndo) {
                    redoLogRecord[opCodesRedo[vectorsRedo - 1]].recordDataObj = redoLogRecord[opCodesUndo[vectorsRedo - 1]].dataObj;
                    redoLogRecord[opCodesRedo[vectorsRedo - 1]].recordObj = redoLogRecord[opCodesUndo[vectorsRedo - 1]].obj;
                }
            }

            ++vectors;
            if (vectors >= VECTOR_MAX_LENGTH) {
                RUNTIME_FAIL("out of redo vectors(" << dec << vectors << "), at pos: " << dec << pos << " record length: " << dec << recordLength);
            }
        }

        for (uint64_t i = 0; i < vectors; ++i) {
            opCodes[i]->process();
            delete opCodes[i];
            opCodes[i] = nullptr;
        }

        uint64_t iPair = 0;
        for (uint64_t i = 0; i < vectors; ++i) {
            //begin transaction
            if (redoLogRecord[i].opCode == 0x0502)
                appendToTransactionBegin(&redoLogRecord[i]);

            //commit/rollback transaction
            else if (redoLogRecord[i].opCode == 0x0504)
                appendToTransactionCommit(&redoLogRecord[i]);

            //ddl
            else if (redoLogRecord[i].opCode == 0x1801 && isUndoRedo[i] == 0)
                appendToTransactionDDL(&redoLogRecord[i]);

            else if (iPair < vectorsUndo) {
                if (opCodesUndo[iPair] == i) {
                    if (iPair < vectorsRedo)
                        appendToTransaction(&redoLogRecord[opCodesUndo[iPair]], &redoLogRecord[opCodesRedo[iPair]]);
                    else
                        appendToTransactionUndo(&redoLogRecord[opCodesUndo[iPair]]);
                    ++iPair;
                } else if (opCodesRedo[iPair] == i) {
                    if (iPair < vectorsUndo)
                        appendToTransaction(&redoLogRecord[opCodesRedo[iPair]], &redoLogRecord[opCodesUndo[iPair]]);
                    ++iPair;
                }
            }
        }
    }

    void RedoLog::appendToTransactionDDL(RedoLogRecord *redoLogRecord) {
        TRACE(TRACE2_DUMP, *redoLogRecord);

        //track DDL
        if ((oracleAnalyzer->flags & REDO_FLAGS_TRACK_DDL) == 0)
            return;

        redoLogRecord->object = oracleAnalyzer->schema->checkDict(redoLogRecord->obj, redoLogRecord->dataObj);
        if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) == 0) {
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0)
                return;
        }

        Transaction *transaction = oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)];
        if (transaction == nullptr) {
            if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS) == 0) {
                oracleAnalyzer->xidTransactionMap.erase((redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32));
                return;
            }

            transaction = new Transaction(oracleAnalyzer, redoLogRecord->xid);
            if (transaction == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: append to transaction#1)");
            }
            oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)] = transaction;
        } else {
            if (transaction->xid != redoLogRecord->xid) {
                RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #ddl");
            }
        }
        transaction->add(redoLogRecord, &zero);
    }

    void RedoLog::appendToTransactionUndo(RedoLogRecord *redoLogRecord) {
        TRACE(TRACE2_DUMP, *redoLogRecord);

        if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0)
            return;

        redoLogRecord->object = oracleAnalyzer->schema->checkDict(redoLogRecord->obj, redoLogRecord->dataObj);
        if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) == 0) {
            if (redoLogRecord->object == nullptr || redoLogRecord->object->options != 0)
                return;
        }

        Transaction *transaction = oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)];
        if (transaction == nullptr) {
            if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS) == 0) {
                oracleAnalyzer->xidTransactionMap.erase((redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32));
                return;
            }

            transaction = new Transaction(oracleAnalyzer, redoLogRecord->xid);
            if (transaction == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: append to transaction#2)");
            }
            oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)] = transaction;
        } else {
            if (transaction->xid != redoLogRecord->xid) {
                RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #undo");
            }
        }

        //cluster key
        if ((redoLogRecord->fb & FB_K) != 0)
            return;

        //partition move
        if ((redoLogRecord->suppLogFb & FB_K) != 0)
            return;

        transaction->add(redoLogRecord);
    }

    void RedoLog::appendToTransactionBegin(RedoLogRecord *redoLogRecord) {
        TRACE(TRACE2_DUMP, *redoLogRecord);

        //skip SQN cleanup
        if (SQN(redoLogRecord->xid) == 0)
            return;

        Transaction *transaction = oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)];
        if (transaction != nullptr) {
            RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #begin");
        }

        transaction = new Transaction(oracleAnalyzer, redoLogRecord->xid);
        if (transaction == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: begin transaction)");
        }
        oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)] = transaction;

        transaction->isBegin = true;
        transaction->firstSequence = sequence;
        transaction->firstPos = lwnStartBlock * reader->blockSize;
    }

    void RedoLog::appendToTransactionCommit(RedoLogRecord *redoLogRecord) {
        TRACE(TRACE2_DUMP, *redoLogRecord);

        Transaction *transaction = oracleAnalyzer->xidTransactionMap[(redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32)];
        if (transaction == nullptr) {
            //unknown transaction
            oracleAnalyzer->xidTransactionMap.erase((redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32));
            return;
        }
        if (transaction->xid != redoLogRecord->xid) {
            RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #commit");
        }

        transaction->commitTimestamp = lwnTimestamp;
        transaction->commitScn = redoLogRecord->scnRecord;
        if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
            transaction->isRollback = true;

        if (transaction->commitScn > oracleAnalyzer->scn) {
            if (transaction->shutdown)
                shutdown = true;

            if (transaction->isBegin)
                transaction->flush();
            else {
                INFO("skipping transaction with no begin: " << *transaction);
            }
        } else {
            INFO("skipping transaction already committed: " << *transaction);
        }

        oracleAnalyzer->xidTransactionMap.erase((redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32));
        delete transaction;
    }

    void RedoLog::appendToTransaction(RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2) {
        bool shutdownFound = false;
        TRACE(TRACE2_DUMP, *redoLogRecord1);
        TRACE(TRACE2_DUMP, *redoLogRecord2);

        //skip other PDB vectors
        if (oracleAnalyzer->conId > 0 && redoLogRecord2->conId != oracleAnalyzer->conId &&
                redoLogRecord1->opCode == 0x0501)
            return;

        if (oracleAnalyzer->conId > 0 && redoLogRecord1->conId != oracleAnalyzer->conId &&
                (redoLogRecord2->opCode == 0x0506 || redoLogRecord2->opCode == 0x050B))
            return;

        typeOBJ obj;
        typeDATAOBJ dataObj;

        if (redoLogRecord1->dataObj != 0) {
            obj = redoLogRecord1->obj;
            dataObj = redoLogRecord1->dataObj;
            redoLogRecord2->obj = redoLogRecord1->obj;
            redoLogRecord2->dataObj = redoLogRecord1->dataObj;
        } else {
            obj = redoLogRecord2->obj;
            dataObj = redoLogRecord2->dataObj;
            redoLogRecord1->obj = redoLogRecord2->obj;
            redoLogRecord1->dataObj = redoLogRecord2->dataObj;
        }

        if (redoLogRecord1->bdba != redoLogRecord2->bdba && redoLogRecord1->bdba != 0 && redoLogRecord2->bdba != 0) {
            if (oracleAnalyzer->dumpRedoLog >= 1)
                oracleAnalyzer->dumpStream << "ERROR: BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")!" << endl;
            REDOLOG_FAIL("BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")");
        }

        redoLogRecord1->object = oracleAnalyzer->schema->checkDict(obj, dataObj);
        if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) == 0) {
            if (redoLogRecord1->object == nullptr)
                return;
        }

        //cluster key
        if ((redoLogRecord1->fb & FB_K) != 0 || (redoLogRecord2->fb & FB_K) != 0)
            return;

        //partition move
        if ((redoLogRecord1->suppLogFb & FB_K) != 0 || (redoLogRecord2->suppLogFb & FB_K) != 0)
            return;

        redoLogRecord2->object = redoLogRecord1->object;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        if (redoLogRecord1->object != nullptr && redoLogRecord1->object->options == 1 && opCodeLong == 0x05010B02) {
            INFO("found shutdown command in events table");
            shutdownFound = true;
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
                Transaction *transaction = oracleAnalyzer->xidTransactionMap[(redoLogRecord1->xid >> 32) | (((uint64_t)redoLogRecord1->conId) << 32)];
                if (transaction == nullptr) {
                    if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS) == 0) {
                        oracleAnalyzer->xidTransactionMap.erase((redoLogRecord1->xid >> 32) | (((uint64_t)redoLogRecord1->conId) << 32));
                        return;
                    }

                    transaction = new Transaction(oracleAnalyzer, redoLogRecord1->xid);
                    if (transaction == nullptr) {
                        RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: append to transaction#3)");
                    }
                    oracleAnalyzer->xidTransactionMap[(redoLogRecord1->xid >> 32) | (((uint64_t)redoLogRecord1->conId) << 32)] = transaction;
                } else {
                    if (transaction->xid != redoLogRecord1->xid) {
                        RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord1->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #append");
                    }
                }
                transaction->add(redoLogRecord1, redoLogRecord2);
                transaction->shutdown = shutdownFound;
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
                typeXIDMAP xidMap = (((uint32_t)redoLogRecord2->usn) << 16) | (redoLogRecord2->slt) | (((uint64_t)redoLogRecord2->conId) << 32);
                Transaction *transaction = oracleAnalyzer->xidTransactionMap[xidMap];

                //match
                if (transaction != nullptr) {
                    transaction->rollbackLastOp(redoLogRecord1->scn);
                } else {
                    oracleAnalyzer->xidTransactionMap.erase(xidMap);
                    WARNING("no match found for transaction rollback, skipping");
                }
            }

            break;
        }
    }

    void RedoLog::dumpRedoVector(uint8_t *data, uint64_t recordLength) const {
        if (oracleAnalyzer->trace >= TRACE_WARNING) {
            stringstream ss;
            ss << "WARNING: Dumping redo Vector" << endl;
            ss << "WARNING: ##: " << dec << recordLength;
            for (uint64_t j = 0; j < recordLength; ++j) {
                if ((j & 0x0F) == 0)
                    ss << endl << "WARNING: ##  " << setfill(' ') << setw(2) << hex << j << ": ";
                if ((j & 0x07) == 0)
                    ss << " ";
                ss << setfill('0') << setw(2) << hex << (uint64_t)data[j] << " ";
            }
            ss << endl;
            OUT(ss.str());
        }
    }

    void RedoLog::resetRedo(void) {
        lwnConfirmedBlock = 2;

        while (lwnAllocated > 1)
            oracleAnalyzer->freeMemoryChunk("LWN", lwnChunks[--lwnAllocated], false);
        uint64_t *length = (uint64_t *)lwnChunks[0];
        *length = sizeof(uint64_t);
    }

    void RedoLog::continueRedo(RedoLog *prev) {
        lwnConfirmedBlock = prev->lwnConfirmedBlock;
        reader->bufferStart = prev->lwnConfirmedBlock * prev->reader->blockSize;
        reader->bufferEnd = prev->lwnConfirmedBlock * prev->reader->blockSize;

        while (lwnAllocated > 1)
            oracleAnalyzer->freeMemoryChunk("LWN", lwnChunks[--lwnAllocated], false);
        uint64_t *length = (uint64_t *)lwnChunks[0];
        *length = sizeof(uint64_t);
    }

    uint64_t RedoLog::processLog(void) {
        if (firstScn == ZERO_SCN && nextScn == ZERO_SCN && reader->firstScn != 0) {
            firstScn = reader->firstScn;
            nextScn = reader->nextScn;
        }
        INFO("processing redo log: " << *this);
        uint64_t currentBlock = lwnConfirmedBlock, blockPos = 16, bufferPos = 0, startBlock = lwnConfirmedBlock;
        uint64_t tmpBufferStart = 0;
        LwnMember *lwnMember;

        oracleAnalyzer->suppLogSize = 0;

        if (reader->bufferStart == reader->blockSize * 2) {
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                stringstream name;
                name << oracleAnalyzer->context.c_str() << "-" << dec << sequence << ".logdump";
                oracleAnalyzer->dumpStream.open(name.str());
                if (!oracleAnalyzer->dumpStream.is_open()) {
                    WARNING("can't open " << name.str() << " for write. Aborting log dump.");
                    oracleAnalyzer->dumpRedoLog = 0;
                }
                printHeaderInfo();
            }
        }

        clock_t cStart = clock();
        {
            unique_lock<mutex> lck(oracleAnalyzer->mtx);
            reader->status = READER_STATUS_READ;
            oracleAnalyzer->readerCond.notify_all();
            oracleAnalyzer->sleepingCond.notify_all();
        }
        tmpBufferStart = reader->bufferStart;
        bufferPos = (currentBlock * reader->blockSize) % DISK_BUFFER_SIZE;
        uint64_t recordLength4 = 0, recordPos = 0, recordLeftToCopy = 0, lwnEndBlock = lwnConfirmedBlock;
        uint16_t lwnNum = 0, lwnNumMax = 0, lwnNumCnt = 0;
        lwnStartBlock = lwnConfirmedBlock;

        while (!oracleAnalyzer->shutdown) {
            //there is some work to do
            while (tmpBufferStart < reader->bufferEnd) {
                //TRACE(TRACE2_LWN, "LWN block: " << dec << (tmpBufferStart / reader->blockSize) << " left: " << dec << recordLeftToCopy << ", last length: "
                //            << recordLength4);

                blockPos = 16;
                //new LWN block
                if (currentBlock == lwnEndBlock) {
                    uint8_t vld = reader->redoBuffer[bufferPos + blockPos + 4];

                    if ((vld & 0x04) != 0) {
                        ++lwnNumCnt;
                        lwnNum = oracleAnalyzer->read16(reader->redoBuffer + bufferPos + blockPos + 24);
                        lwnNumMax = oracleAnalyzer->read16(reader->redoBuffer + bufferPos + blockPos + 26);
                        uint32_t lwnLength = oracleAnalyzer->read32(reader->redoBuffer + bufferPos + blockPos + 28);
                        lwnScn = oracleAnalyzer->readSCN(reader->redoBuffer + bufferPos + blockPos + 40);
                        lwnTimestamp = oracleAnalyzer->read32(reader->redoBuffer + bufferPos + blockPos + 64);
                        lwnStartBlock = currentBlock;
                        lwnEndBlock = lwnStartBlock + lwnLength;
                        TRACE(TRACE2_LWN, "LWN: at: " << dec << lwnStartBlock << " length: " << lwnLength << " chk: " << dec << lwnNum << " max: " << lwnNumMax);
                    } else {
                        RUNTIME_FAIL("did not find LWN at pos: " << dec << tmpBufferStart);
                    }
                }

                while (blockPos < reader->blockSize) {
                    //next record
                    if (recordLeftToCopy == 0) {
                        if (blockPos + 20 >= reader->blockSize)
                            break;

                        recordLength4 = (((uint64_t)oracleAnalyzer->read32(reader->redoBuffer + bufferPos + blockPos)) + 3) & 0xFFFFFFFC;
                        if (recordLength4 > 0) {
                            uint64_t *length = (uint64_t*)(lwnChunks[lwnAllocated - 1]);

                            if (*length + sizeof(LwnMember) + recordLength4 > MEMORY_CHUNK_SIZE_MB * 1024 * 1024) {
                                if (lwnAllocated == MAX_LWN_CHUNKS) {
                                    RUNTIME_FAIL("all " << dec << MAX_LWN_CHUNKS << " LWN buffers allocated");
                                }

                                lwnChunks[lwnAllocated++] = oracleAnalyzer->getMemoryChunk("LWN", false);
                                length = (uint64_t*)(lwnChunks[lwnAllocated - 1]);
                                *length = sizeof(uint64_t);
                            }

                            lwnMember = (struct LwnMember*)(lwnChunks[lwnAllocated - 1] + *length);
                            *length += sizeof(LwnMember) + recordLength4;
                            lwnMember->scn = oracleAnalyzer->read32(reader->redoBuffer + bufferPos + blockPos + 8) |
                                    ((uint64_t)(oracleAnalyzer->read16(reader->redoBuffer + bufferPos + blockPos + 6)) << 32);
                            lwnMember->subScn = oracleAnalyzer->read16(reader->redoBuffer + bufferPos + blockPos + 12);
                            lwnMember->block = currentBlock;
                            lwnMember->pos = blockPos;

                            TRACE(TRACE2_LWN, "LWN: length: " << dec << recordLength4 << " scn: " << lwnMember->scn << " subScn: " << lwnMember->subScn);

                            uint64_t lwnPos = lwnRecords++;
                            if (lwnPos == MAX_RECORDS_IN_LWN) {
                                RUNTIME_FAIL("all " << dec << lwnPos << " records in LWN were used");
                            }
                            while (lwnPos > 0 &&
                                    (lwnMembers[lwnPos - 1]->scn > lwnMember->scn ||
                                        (lwnMembers[lwnPos - 1]->scn == lwnMember->scn && lwnMembers[lwnPos - 1]->subScn > lwnMember->subScn))) {
                                lwnMembers[lwnPos] = lwnMembers[lwnPos - 1];
                                --lwnPos;
                            }
                            lwnMembers[lwnPos] = lwnMember;
                        }

                        if (recordLength4 > MEMORY_CHUNK_SIZE_MB * 1024 * 1024 - sizeof(LwnMember) - sizeof(uint64_t)) {
                            RUNTIME_FAIL("too big log record: " << dec << recordLength4 << " bytes");
                        }

                        recordLeftToCopy = recordLength4;
                        recordPos = 0;
                    }

                    //nothing more
                    if (recordLeftToCopy == 0)
                        break;

                    uint64_t toCopy;
                    if (blockPos + recordLeftToCopy > reader->blockSize)
                        toCopy = reader->blockSize - blockPos;
                    else
                        toCopy = recordLeftToCopy;

                    memcpy(((uint8_t*)lwnMember) + sizeof(struct LwnMember) + recordPos, reader->redoBuffer + bufferPos + blockPos, toCopy);
                    recordLeftToCopy -= toCopy;
                    blockPos += toCopy;
                    recordPos += toCopy;
                }

                ++currentBlock;

                //checkpoint
                if (currentBlock == lwnEndBlock && lwnNumCnt == lwnNumMax) {
                    try {
                        TRACE(TRACE2_LWN, "LWN: analyze");
                        for (uint64_t i = 0; i < lwnRecords; ++i) {
                            TRACE(TRACE2_LWN, "LWN: analyze blk: " << dec << lwnMembers[i]->block << " pos: " << lwnMembers[i]->pos <<
                                    " scn: " << lwnMembers[i]->scn << " subscn: " << lwnMembers[i]->subScn);
                            analyzeLwn(lwnMembers[i]);
                            if (lwnScnMax < lwnMembers[i]->scn)
                                lwnScnMax = lwnMembers[i]->scn;
                        }
                    } catch(RedoLogException &ex) {
                        if ((oracleAnalyzer->flags & REDO_FLAGS_ON_ERROR_CONTINUE) == 0) {
                            RUNTIME_FAIL("runtime error, aborting further redo log processing");
                        } else
                            WARNING("forced to continue working in spite of error");
                    }

                    TRACE(TRACE2_LWN, "LWN: scn: " << dec << lwnScnMax);
                    for (uint64_t i = 1; i < lwnAllocated; ++i)
                        oracleAnalyzer->freeMemoryChunk("LWN", lwnChunks[i], false);
                    lwnNumCnt = 0;
                    lwnAllocated = 1;
                    uint64_t *length = (uint64_t *)lwnChunks[0];
                    *length = sizeof(uint64_t);
                    lwnRecords = 0;
                    lwnConfirmedBlock = currentBlock;
                }

                tmpBufferStart += reader->blockSize;
                bufferPos += reader->blockSize;
                if (bufferPos == DISK_BUFFER_SIZE)
                    bufferPos = 0;

                if (shutdown)
                    stopMain();

                if (tmpBufferStart - reader->bufferStart > DISK_BUFFER_SIZE / 16) {
                    unique_lock<mutex> lck(oracleAnalyzer->mtx);
                    reader->bufferStart = tmpBufferStart;
                    if (reader->status == READER_STATUS_READ) {
                        oracleAnalyzer->readerCond.notify_all();
                    }
                }
            }

            {
                unique_lock<mutex> lck(oracleAnalyzer->mtx);
                if (reader->bufferStart < tmpBufferStart) {
                    reader->bufferStart = tmpBufferStart;
                    if (reader->status == READER_STATUS_READ) {
                        oracleAnalyzer->readerCond.notify_all();
                    }
                }

                //all work done
                if (tmpBufferStart == reader->bufferEnd) {
                    if (reader->ret == REDO_FINISHED && nextScn == ZERO_SCN && reader->nextScn != 0)
                        nextScn = reader->nextScn;

                    if (reader->ret == REDO_FINISHED || reader->ret == REDO_OVERWRITTEN || reader->status == READER_STATUS_SLEEPING)
                        break;
                    oracleAnalyzer->analyzerCond.wait(lck);
                }
            }
        }

        clock_t cEnd = clock();
        double mySpeed = 0, myTime = 1000.0 * (cEnd - cStart) / CLOCKS_PER_SEC, suppLogPercent = 0.0;
        if (currentBlock != startBlock)
            suppLogPercent = 100.0 * oracleAnalyzer->suppLogSize / ((currentBlock - startBlock) * reader->blockSize);
        if (myTime > 0)
            mySpeed = (currentBlock - startBlock) * reader->blockSize / 1024 / 1024 / myTime * 1000;

        TRACE(TRACE2_PERFORMANCE, "redo processing time: " << myTime << " ms, " <<
                "Speed: " << fixed << setprecision(2) << mySpeed << " MB/s, " <<
                "Redo log size: " << dec << ((currentBlock - startBlock) * reader->blockSize / 1024) << " kB, " <<
                "Supplemental redo log size: " << dec << oracleAnalyzer->suppLogSize << " bytes " <<
                "(" << fixed << setprecision(2) << suppLogPercent << " %)");

        if (oracleAnalyzer->dumpRedoLog >= 1 && oracleAnalyzer->dumpStream.is_open())
            oracleAnalyzer->dumpStream.close();

        return reader->ret;
    }

    ostream& operator<<(ostream& os, const RedoLog& ors) {
        os << "group: " << dec << ors.group << " scn: " << ors.firstScn << " to " <<
                ((ors.nextScn != ZERO_SCN) ? ors.nextScn : 0) << " sequence: " << ors.sequence << " path: " << ors.path;
        return os;
    }
}
