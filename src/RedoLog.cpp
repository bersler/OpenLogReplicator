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
#include "OutputBuffer.h"
#include "Reader.h"
#include "RedoLog.h"
#include "RedoLogException.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

using namespace std;

namespace OpenLogReplicator {
    RedoLog::RedoLog(OracleAnalyzer* oracleAnalyzer, int64_t group, string& path) :
        oracleAnalyzer(oracleAnalyzer),
        vectors(0),
        lwnConfirmedBlock(2),
        lwnAllocated(0),
        lwnAllocatedMax(0),
        lwnTimestamp(0),
        lwnScn(0),
        lwnScnMax(0),
        lwnRecords(0),
        lwnCheckpointBlock(0),
        instrumentedShutdown(false),
        group(group),
        path(path),
        sequence(0),
        firstScn(firstScn),
        nextScn(nextScn),
        reader(nullptr) {

        memset(&zero, 0, sizeof(struct RedoLogRecord));

        lwnChunks[0] = oracleAnalyzer->getMemoryChunk("LWN transaction chunk", false);
        uint64_t* length = (uint64_t*) lwnChunks[0];
        *length = sizeof(uint64_t);
        lwnAllocated = 1;
        lwnAllocatedMax = 1;
    }

    RedoLog::~RedoLog() {
        while (lwnAllocated > 0)
            oracleAnalyzer->freeMemoryChunk("LWN transaction chunk", lwnChunks[--lwnAllocated], false);

        for (uint64_t i = 0; i < vectors; ++i) {
            if (opCodes[i] != nullptr) {
                delete opCodes[i];
                opCodes[i] = nullptr;
            }
        }
        vectors = 0;
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
                    "\tActivation ID=" << dec << reader->activationHeader << "=0x" << hex << reader->activationHeader << endl <<
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
            typeRESETLOGS prevResetlogsCnt = oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 292);
            typeSCN prevResetlogsScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 284);
            typeTIME firstTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 188));
            typeTIME nextTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 200));
            typeSCN enabledScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 208);
            typeTIME enabledTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 216));
            typeSCN threadClosedScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 220);
            typeTIME threadClosedTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 228));
            typeSCN termialRecScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 240);
            typeTIME termialRecTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 248));
            typeSCN mostRecentScn = oracleAnalyzer->readSCN(reader->headerBuffer + reader->blockSize + 260);
            typeSUM chSum = oracleAnalyzer->read16(reader->headerBuffer + reader->blockSize + 14);
            typeSUM chSum2 = reader->calcChSum(reader->headerBuffer + reader->blockSize, reader->blockSize);

            if (oracleAnalyzer->version < REDO_VERSION_12_2) {
                oracleAnalyzer->dumpStream <<
                        " resetlogs count: 0x" << hex << reader->resetlogsHeader << " scn: " << PRINTSCN48(resetlogsScn) << " (" << dec << resetlogsScn << ")" << endl <<
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
                        " resetlogs count: 0x" << hex << reader->resetlogsHeader << " scn: " << PRINTSCN64(resetlogsScn) << endl <<
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
            const char* endOfRedo = "";
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

            typeTIME standbyLogCloseTime(oracleAnalyzer->read32(reader->headerBuffer + reader->blockSize + 304));
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

    void RedoLog::freeLwn(void) {
        while (lwnAllocated > 1)
            oracleAnalyzer->freeMemoryChunk("LWN transaction chunk", lwnChunks[--lwnAllocated], false);

        uint64_t* length = (uint64_t*) lwnChunks[0];
        *length = sizeof(uint64_t);
        lwnRecords = 0;
    }

    void RedoLog::analyzeLwn(LwnMember* lwnMember) {
        RedoLogRecord redoLogRecord[VECTOR_MAX_LENGTH];
        uint64_t isUndoRedo[VECTOR_MAX_LENGTH];
        uint64_t opCodesUndo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsUndo = 0;
        uint64_t opCodesRedo[VECTOR_MAX_LENGTH / 2];
        uint64_t vectorsRedo = 0;
        uint8_t* data = ((uint8_t*) lwnMember) + sizeof(struct LwnMember);

        TRACE(TRACE2_LWN, "LWN: analyze length: " << dec << lwnMember->length << " scn: " << lwnMember->scn << " subScn: " << lwnMember->subScn);
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

        if (recordLength != lwnMember->length) {
            REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                    ": too small log record, buffer length: " << dec << lwnMember->length << ", field length: " << recordLength);
        }

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
                                    setfill('0') << setw(4) << hex << lwnMember->offset <<
                        " LEN: 0x" << setfill('0') << setw(4) << hex << recordLength <<
                        " VLD: 0x" << setfill('0') << setw(2) << hex << (uint64_t)vld << endl;
            else {
                uint32_t conUid = oracleAnalyzer->read32(data + 16);
                oracleAnalyzer->dumpStream << "REDO RECORD - Thread:" << thread <<
                        " RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << lwnMember->block << "." <<
                                    setfill('0') << setw(4) << hex << lwnMember->offset <<
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
                                    setfill('0') << setw(4) << hex << lwnMember->offset <<
                        " LEN: " << setfill('0') << setw(4) << dec << lwnLen <<
                        " NST: " << setfill('0') << setw(4) << dec << lwnNst <<
                        " SCN: " << PRINTSCN48(lwnScn) << ")" << endl;
                else
                    oracleAnalyzer->dumpStream << "(LWN RBA: 0x" << setfill('0') << setw(6) << hex << sequence << "." <<
                                    setfill('0') << setw(8) << hex << lwnMember->block << "." <<
                                    setfill('0') << setw(4) << hex << lwnMember->offset <<
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
            REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                    ": too small log record, header length: " << dec << headerLength << ", field length: " << recordLength);
        }

        uint64_t offset = headerLength;
        while (offset < recordLength) {
            memset(&redoLogRecord[vectors], 0, sizeof(struct RedoLogRecord));
            redoLogRecord[vectors].vectorNo = vectors + 1;
            redoLogRecord[vectors].cls = oracleAnalyzer->read16(data + offset + 2);
            redoLogRecord[vectors].afn = oracleAnalyzer->read32(data + offset + 4) & 0xFFFF;
            redoLogRecord[vectors].dba = oracleAnalyzer->read32(data + offset + 8);
            redoLogRecord[vectors].scnRecord = oracleAnalyzer->readSCN(data + offset + 12);
            redoLogRecord[vectors].rbl = 0; //FIXME
            redoLogRecord[vectors].seq = data[offset + 20];
            redoLogRecord[vectors].typ = data[offset + 21];
            typeUSN usn = (redoLogRecord[vectors].cls >= 15) ? (redoLogRecord[vectors].cls - 15) / 2 : -1;

            uint64_t fieldOffset;
            if (oracleAnalyzer->version >= REDO_VERSION_12_1) {
                fieldOffset = 32;
                redoLogRecord[vectors].flgRecord = oracleAnalyzer->read16(data + offset + 28);
                redoLogRecord[vectors].conId = oracleAnalyzer->read16(data + offset + 24);
            } else {
                fieldOffset = 24;
                redoLogRecord[vectors].flgRecord = 0;
                redoLogRecord[vectors].conId = 0;
            }

            if (offset + fieldOffset + 1 >= recordLength) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                                    ": position of field list (" << dec << (offset + fieldOffset + 1) << ") outside of record, length: " << recordLength);
            }

            uint8_t* fieldList = data + offset + fieldOffset;

            redoLogRecord[vectors].opCode = (((typeOP1)data[offset + 0]) << 8) |
                    data[offset + 1];
            redoLogRecord[vectors].length = fieldOffset + ((oracleAnalyzer->read16(fieldList) + 2) & 0xFFFC);
            redoLogRecord[vectors].sequence = sequence;
            redoLogRecord[vectors].scn = lwnMember->scn;
            redoLogRecord[vectors].subScn = lwnMember->subScn;
            redoLogRecord[vectors].usn = usn;
            redoLogRecord[vectors].data = data + offset;
            redoLogRecord[vectors].fieldLengthsDelta = fieldOffset;
            if (redoLogRecord[vectors].fieldLengthsDelta + 1 >= recordLength) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                                    ": field length list (" << dec << (redoLogRecord[vectors].fieldLengthsDelta) << ") outside of record, length: " << recordLength);
            }
            redoLogRecord[vectors].fieldCnt = (oracleAnalyzer->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) - 2) / 2;
            redoLogRecord[vectors].fieldPos = fieldOffset + ((oracleAnalyzer->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta) + 2) & 0xFFFC);
            if (redoLogRecord[vectors].fieldPos >= recordLength) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                                    ": fields (" << dec << (redoLogRecord[vectors].fieldPos) << ") outside of record, length: " << recordLength);
            }

            uint64_t fieldPos = redoLogRecord[vectors].fieldPos;
            for (uint64_t i = 1; i <= redoLogRecord[vectors].fieldCnt; ++i) {
                redoLogRecord[vectors].length += (oracleAnalyzer->read16(fieldList + i * 2) + 3) & 0xFFFC;
                fieldPos += (oracleAnalyzer->read16(redoLogRecord[vectors].data + redoLogRecord[vectors].fieldLengthsDelta + i * 2) + 3) & 0xFFFC;

                if (offset + redoLogRecord[vectors].length > recordLength) {
                    dumpRedoVector(data, recordLength);
                    REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                                        ": position of field list outside of record (" <<
                            "i: " << dec << i <<
                            " c: " << dec << redoLogRecord[vectors].fieldCnt << " " <<
                            " o: " << dec << fieldOffset <<
                            " p: " << dec << offset <<
                            " l: " << dec << redoLogRecord[vectors].length <<
                            " r: " << dec << recordLength << ")");
                }
            }

            if (redoLogRecord[vectors].fieldPos > redoLogRecord[vectors].length) {
                dumpRedoVector(data, recordLength);
                REDOLOG_FAIL("block: " << dec << lwnMember->block << ", offset: " << lwnMember->offset <<
                                    ": incomplete record, offset: " << dec << redoLogRecord[vectors].fieldPos << ", length: " << redoLogRecord[vectors].length);
            }

            redoLogRecord[vectors].recordObj = 0xFFFFFFFF;
            redoLogRecord[vectors].recordDataObj = 0xFFFFFFFF;

            offset += redoLogRecord[vectors].length;

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
                RUNTIME_FAIL("out of redo vectors(" << dec << vectors << "), at offset: " << dec << offset << " record length: " << dec << recordLength);
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

    void RedoLog::appendToTransactionDDL(RedoLogRecord* redoLogRecord) {
        bool system = false;
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord);

        //track DDL
        if ((oracleAnalyzer->flags & REDO_FLAGS_TRACK_DDL) == 0)
            return;

        //skip list
        if (oracleAnalyzer->skipXidList.find(redoLogRecord->xid) != oracleAnalyzer->skipXidList.end())
            return;

        OracleObject* object = oracleAnalyzer->schema->checkDict(redoLogRecord->obj, redoLogRecord->dataObj);
        if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) == 0) {
            if (object == nullptr)
                return;
        }
        if (object != nullptr && (object->options & OPTIONS_SYSTEM_TABLE) != 0)
            system = true;

        Transaction* transaction = nullptr;
        typeXIDMAP xidMap = (redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32);

        auto transactionIter = oracleAnalyzer->xidTransactionMap.find(xidMap);
        if (transactionIter != oracleAnalyzer->xidTransactionMap.end()) {
            transaction = transactionIter->second;
            if (transaction->xid != redoLogRecord->xid) {
                RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #ddl");
            }
        } else {
            if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS) == 0)
                return;

            transaction = new Transaction(oracleAnalyzer, redoLogRecord->xid);
            if (transaction == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: append to transaction#1)");
            }
            oracleAnalyzer->xidTransactionMap[xidMap] = transaction;
        }

        if (system)
            transaction->system = true;

        //transaction size limit
        if (oracleAnalyzer->transactionMax > 0 &&
                transaction->size + redoLogRecord->length + ROW_HEADER_TOTAL >= oracleAnalyzer->transactionMax) {
            oracleAnalyzer->skipXidList.insert(transaction->xid);
            oracleAnalyzer->xidTransactionMap.erase(xidMap);
            delete transaction;
            return;
        }

        transaction->add(redoLogRecord, &zero);
    }

    void RedoLog::appendToTransactionUndo(RedoLogRecord* redoLogRecord) {
        bool system = false;
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord);

        if ((redoLogRecord->flg & (FLG_MULTIBLOCKUNDOHEAD | FLG_MULTIBLOCKUNDOMID | FLG_MULTIBLOCKUNDOTAIL)) == 0)
            return;

        //skip list
        if (oracleAnalyzer->skipXidList.find(redoLogRecord->xid) != oracleAnalyzer->skipXidList.end())
            return;

        OracleObject* object = oracleAnalyzer->schema->checkDict(redoLogRecord->obj, redoLogRecord->dataObj);
        if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) == 0) {
            if (object == nullptr)
                return;
        }
        if (object != nullptr && (object->options & OPTIONS_SYSTEM_TABLE) != 0)
            system = true;

        Transaction* transaction = nullptr;
        typeXIDMAP xidMap = (redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32);

        auto transactionIter = oracleAnalyzer->xidTransactionMap.find(xidMap);
        if (transactionIter != oracleAnalyzer->xidTransactionMap.end()) {
            transaction = transactionIter->second;
            if (transaction->xid != redoLogRecord->xid) {
                RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #undo");
            }
        } else {
            if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS) == 0)
                return;

            transaction = new Transaction(oracleAnalyzer, redoLogRecord->xid);
            if (transaction == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: append to transaction#2)");
            }
            oracleAnalyzer->xidTransactionMap[xidMap] = transaction;
        }

        if (system)
            transaction->system = true;

        //cluster key
        if ((redoLogRecord->fb & FB_K) != 0)
            return;

        //partition move
        if ((redoLogRecord->suppLogFb & FB_K) != 0)
            return;

        //transaction size limit
        if (oracleAnalyzer->transactionMax > 0 &&
                transaction->size + redoLogRecord->length + ROW_HEADER_TOTAL >= oracleAnalyzer->transactionMax) {
            oracleAnalyzer->skipXidList.insert(transaction->xid);
            oracleAnalyzer->xidTransactionMap.erase(xidMap);
            delete transaction;
            return;
        }

        transaction->add(redoLogRecord);
    }

    void RedoLog::appendToTransactionBegin(RedoLogRecord* redoLogRecord) {
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord);

        //skip SQN cleanup
        if (SQN(redoLogRecord->xid) == 0)
            return;

        Transaction* transaction = nullptr;
        typeXIDMAP xidMap = (redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32);

        auto transactionIter = oracleAnalyzer->xidTransactionMap.find(xidMap);
        if (transactionIter != oracleAnalyzer->xidTransactionMap.end()) {
            transaction = transactionIter->second;
            RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #begin");
        }

        transaction = new Transaction(oracleAnalyzer, redoLogRecord->xid);
        if (transaction == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: begin transaction)");
        }
        oracleAnalyzer->xidTransactionMap[xidMap] = transaction;

        transaction->begin = true;
        transaction->firstSequence = sequence;
        transaction->firstOffset = lwnCheckpointBlock * reader->blockSize;
    }

    void RedoLog::appendToTransactionCommit(RedoLogRecord* redoLogRecord) {
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord);

        //skip list
        auto it = oracleAnalyzer->skipXidList.find(redoLogRecord->xid);
        if (it != oracleAnalyzer->skipXidList.end()) {
            WARNING("skipping transaction: " << PRINTXID(redoLogRecord->xid));
            oracleAnalyzer->skipXidList.erase(it);
            return;
        }

        Transaction* transaction = nullptr;
        typeXIDMAP xidMap = (redoLogRecord->xid >> 32) | (((uint64_t)redoLogRecord->conId) << 32);

        //broken transaction
        auto iter = oracleAnalyzer->brokenXidMapList.find(xidMap);
        if (iter != oracleAnalyzer->brokenXidMapList.end())
            oracleAnalyzer->brokenXidMapList.erase(xidMap);

        auto transactionIter = oracleAnalyzer->xidTransactionMap.find(xidMap);
        if (transactionIter == oracleAnalyzer->xidTransactionMap.end()) {
            //unknown transaction
            return;
        }

        transaction = transactionIter->second;
        if (transaction->xid != redoLogRecord->xid) {
            RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #commit");
        }

        transaction->commitTimestamp = lwnTimestamp;
        transaction->commitScn = redoLogRecord->scnRecord;
        transaction->commitSequence = redoLogRecord->sequence;
        if ((redoLogRecord->flg & FLG_ROLLBACK_OP0504) != 0)
            transaction->rollback = true;

        if ((transaction->commitScn > oracleAnalyzer->firstScn && transaction->system == false) ||
            (transaction->commitScn > oracleAnalyzer->schemaScn && transaction->system == true)) {

            if (transaction->shutdown) {
                INFO("shutdown started - initiated by debug transaction " << PRINTXID(transaction->xid) << " at scn " << dec << transaction->commitScn);
                instrumentedShutdown = true;
            }

            if (transaction->begin) {
                transaction->flush();

                if (oracleAnalyzer->stopTransactions > 0) {
                    --oracleAnalyzer->stopTransactions;
                    if (oracleAnalyzer->stopTransactions == 0) {
                        INFO("shutdown started - exhausted number of transactions");
                        instrumentedShutdown = true;
                    }
                }

            } else {
                WARNING("skipping transaction with no begin: " << *transaction);
            }
        } else {
            DEBUG("skipping transaction already committed: " << *transaction);
        }

        oracleAnalyzer->xidTransactionMap.erase(xidMap);
        delete transaction;
    }

    void RedoLog::appendToTransaction(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2) {
        bool shutdownFound = false;
        bool system = false;
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord1);
        TRACE(TRACE2_DUMP, "DUMP: " << *redoLogRecord2);

        //skip other PDB vectors
        if (oracleAnalyzer->conId > 0 && redoLogRecord2->conId != oracleAnalyzer->conId &&
                redoLogRecord1->opCode == 0x0501)
            return;

        if (oracleAnalyzer->conId > 0 && redoLogRecord1->conId != oracleAnalyzer->conId &&
                (redoLogRecord2->opCode == 0x0506 || redoLogRecord2->opCode == 0x050B))
            return;

        //skip list
        if (oracleAnalyzer->skipXidList.find(redoLogRecord1->xid) != oracleAnalyzer->skipXidList.end())
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
            REDOLOG_FAIL("BDBA does not match (0x" << hex << redoLogRecord1->bdba << ", " << redoLogRecord2->bdba << ")");
        }

        OracleObject* object = oracleAnalyzer->schema->checkDict(obj, dataObj);
        if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) == 0) {
            if (object == nullptr)
                return;
        }
        if (object != nullptr && (object->options & OPTIONS_SYSTEM_TABLE) != 0)
            system = true;

        //cluster key
        if ((redoLogRecord1->fb & FB_K) != 0 || (redoLogRecord2->fb & FB_K) != 0)
            return;

        //partition move
        if ((redoLogRecord1->suppLogFb & FB_K) != 0 || (redoLogRecord2->suppLogFb & FB_K) != 0)
            return;

        long opCodeLong = (redoLogRecord1->opCode << 16) | redoLogRecord2->opCode;
        if (object != nullptr && (object->options & OPTIONS_DEBUG_TABLE) != 0 && opCodeLong == 0x05010B02)
            shutdownFound = true;

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
                Transaction* transaction = nullptr;
                typeXIDMAP xidMap = (redoLogRecord1->xid >> 32) | (((uint64_t)redoLogRecord1->conId) << 32);

                auto transactionIter = oracleAnalyzer->xidTransactionMap.find(xidMap);
                if (transactionIter != oracleAnalyzer->xidTransactionMap.end()) {
                    transaction = transactionIter->second;
                    if (transaction->xid != redoLogRecord1->xid) {
                        RUNTIME_FAIL("Transaction " << PRINTXID(redoLogRecord1->xid) << " conflicts with " << PRINTXID(transaction->xid) << " #append");
                    }
                } else {
                    if ((oracleAnalyzer->flags & REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS) == 0)
                        return;

                    transaction = new Transaction(oracleAnalyzer, redoLogRecord1->xid);
                    if (transaction == nullptr) {
                        RUNTIME_FAIL("couldn't allocate " << dec << sizeof(Transaction) << " bytes memory (for: append to transaction#3)");
                    }
                    oracleAnalyzer->xidTransactionMap[xidMap] = transaction;
                }
                if (system)
                    transaction->system = true;

                //transaction size limit
                if (oracleAnalyzer->transactionMax > 0 &&
                        transaction->size + redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_TOTAL >= oracleAnalyzer->transactionMax) {
                    oracleAnalyzer->skipXidList.insert(transaction->xid);
                    oracleAnalyzer->xidTransactionMap.erase(xidMap);
                    delete transaction;
                    return;
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
                Transaction* transaction = nullptr;
                typeXIDMAP xidMap = (((uint64_t)redoLogRecord2->usn) << 16) | ((uint64_t)redoLogRecord2->slt) | (((uint64_t)redoLogRecord2->conId) << 32);

                auto transactionIter = oracleAnalyzer->xidTransactionMap.find(xidMap);
                if (transactionIter != oracleAnalyzer->xidTransactionMap.end()) {
                    transaction = transactionIter->second;
                    transaction->rollbackLastOp(redoLogRecord1->scn);
                } else {
                    auto iter = oracleAnalyzer->brokenXidMapList.find(xidMap);
                    if (iter == oracleAnalyzer->brokenXidMapList.end()) {
                        WARNING("no match found for transaction rollback, skipping, SLT: " << dec << (uint64_t)redoLogRecord2->slt <<
                                " USN: " << (uint64_t)redoLogRecord2->usn);
                        oracleAnalyzer->brokenXidMapList.insert(xidMap);
                    }
                }
                if (system)
                    transaction->system = true;
            }

            break;
        }
    }

    void RedoLog::dumpRedoVector(uint8_t* data, uint64_t recordLength) const {
        if (trace >= TRACE_WARNING) {
            stringstream ss;
            ss << "dumping redo vector" << endl;
            ss << "##: " << dec << recordLength;
            for (uint64_t j = 0; j < recordLength; ++j) {
                if ((j & 0x0F) == 0)
                    ss << endl << "##  " << setfill(' ') << setw(2) << hex << j << ": ";
                if ((j & 0x07) == 0)
                    ss << " ";
                ss << setfill('0') << setw(2) << hex << (uint64_t)data[j] << " ";
            }
            WARNING(ss.str());
        }
    }

    uint64_t RedoLog::processLog(void) {
        if (firstScn == ZERO_SCN && nextScn == ZERO_SCN && reader->firstScn != 0) {
            firstScn = reader->firstScn;
            nextScn = reader->nextScn;
        }
        oracleAnalyzer->suppLogSize = 0;

        if (reader->bufferStart == reader->blockSize * 2) {
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                string fileName = oracleAnalyzer->dumpPath + "/" + oracleAnalyzer->database + "-" + to_string(sequence) + ".logdump";
                oracleAnalyzer->dumpStream.open(fileName);
                if (!oracleAnalyzer->dumpStream.is_open()) {
                    WARNING("can't open " << fileName << " for write. Aborting log dump.");
                    oracleAnalyzer->dumpRedoLog = 0;
                }
                printHeaderInfo();
            }
        }

        if (oracleAnalyzer->offset > 0) {
            if ((oracleAnalyzer->offset % reader->blockSize) != 0) {
                RUNTIME_FAIL("incorrect offset start:  " << dec << oracleAnalyzer->offset << " - not a multiplication of block size: " << dec << reader->blockSize);
            }
            lwnConfirmedBlock = oracleAnalyzer->offset / reader->blockSize;
            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: setting reader start position to " << dec << oracleAnalyzer->offset << " (block " << dec << lwnConfirmedBlock << ")");

            reader->bufferStart = oracleAnalyzer->offset;
            reader->bufferEnd = oracleAnalyzer->offset;
            oracleAnalyzer->offset = 0;
        } else {
            lwnConfirmedBlock = 2;
            reader->bufferStart = lwnConfirmedBlock * reader->blockSize;
            reader->bufferEnd = lwnConfirmedBlock * reader->blockSize;

            oracleAnalyzer->checkpointLastOffset = 0;
        }

        INFO("processing redo log: " << *this << " offset: " << dec << reader->bufferStart);

        if (oracleAnalyzer->resetlogs == 0)
            oracleAnalyzer->resetlogs = reader->resetlogsHeader;

        if (reader->resetlogsHeader != oracleAnalyzer->resetlogs) {
            RUNTIME_FAIL("invalid resetlogs value (found: " << dec << reader->resetlogsHeader << ", expected: " << dec << oracleAnalyzer->resetlogs << "): " << reader->fileName);
        }

        if (oracleAnalyzer->activation == 0) {
            INFO("new activation detected: " << dec << reader->activationHeader);
            oracleAnalyzer->activation = reader->activationHeader;
            oracleAnalyzer->activationChanged = true;
        }

        if (reader->activationHeader != 0 && reader->activationHeader != oracleAnalyzer->activation) {
            RUNTIME_FAIL("invalid activation id value (found: " << dec << reader->activationHeader << ", expected: " << dec << oracleAnalyzer->activation << "): " << reader->fileName);
        }

        if (oracleAnalyzer->nextScn != ZERO_SCN) {
            if (oracleAnalyzer->nextScn != reader->firstScnHeader) {
                RUNTIME_FAIL("incorrect SCN for redo log file, read: " << dec << reader->firstScnHeader << " expected: " << oracleAnalyzer->nextScn);
            }
        }

        time_t cStart = oracleAnalyzer->getTime();
        {
            unique_lock<mutex> lck(oracleAnalyzer->mtx);
            reader->status = READER_STATUS_READ;
            oracleAnalyzer->readerCond.notify_all();
            oracleAnalyzer->sleepingCond.notify_all();
        }
        LwnMember* lwnMember;
        uint64_t currentBlock = lwnConfirmedBlock;
        uint64_t blockOffset = 16;
        uint64_t startBlock = lwnConfirmedBlock;
        uint64_t tmpBufferStart = reader->bufferStart;
        uint64_t recordLength4 = 0;
        uint64_t recordPos = 0;
        uint64_t recordLeftToCopy = 0;
        uint64_t lwnEndBlock = lwnConfirmedBlock;
        uint64_t lwnStartBlock = lwnConfirmedBlock;
        uint16_t lwnNum = 0;
        uint16_t lwnNumMax = 0;
        uint16_t lwnNumCur = 0;
        uint16_t lwnNumCnt = 0;
        lwnCheckpointBlock = lwnConfirmedBlock;
        bool switchRedo = false;

        while (!oracleAnalyzer->shutdown) {
            //there is some work to do
            while (tmpBufferStart < reader->bufferEnd) {
                uint64_t redoBufferPos = (currentBlock * reader->blockSize) % MEMORY_CHUNK_SIZE;
                uint64_t redoBufferNum = ((currentBlock * reader->blockSize) / MEMORY_CHUNK_SIZE) % oracleAnalyzer->readBufferMax;
                uint8_t* redoBlock = reader->redoBufferList[redoBufferNum] + redoBufferPos;

                blockOffset = 16;
                //new LWN block
                if (currentBlock == lwnEndBlock) {
                    uint8_t vld = redoBlock[blockOffset + 4];

                    if ((vld & 0x04) != 0) {
                        lwnNum = oracleAnalyzer->read16(redoBlock + blockOffset + 24);
                        uint32_t lwnLength = oracleAnalyzer->read32(redoBlock + blockOffset + 28);
                        lwnStartBlock = currentBlock;
                        lwnEndBlock = currentBlock + lwnLength;
                        lwnScn = oracleAnalyzer->readSCN(redoBlock + blockOffset + 40);
                        lwnTimestamp = oracleAnalyzer->read32(redoBlock + blockOffset + 64);

                        if (lwnNumCnt == 0) {
                            lwnCheckpointBlock = currentBlock;
                            lwnNumMax = oracleAnalyzer->read16(redoBlock + blockOffset + 26);
                            //verify LWN header start
                            if (lwnScn < reader->firstScn || (lwnScn > reader->nextScn && reader->nextScn != ZERO_SCN)) {
                                RUNTIME_FAIL("invalid LWN SCN: " << dec << lwnScn);
                            }
                        } else {
                            lwnNumCur = oracleAnalyzer->read16(redoBlock + blockOffset + 26);
                            if (lwnNumCur != lwnNumMax) {
                                RUNTIME_FAIL("invalid LWN MAX: " << dec << lwnNum << "/" << dec << lwnNumCur << "/" << dec << lwnNumMax);
                            }
                        }
                        ++lwnNumCnt;

                        TRACE(TRACE2_LWN, "LWN: at: " << dec << lwnStartBlock << " length: " << lwnLength << " chk: " << dec << lwnNum << " max: " << lwnNumMax);

                    } else {
                        RUNTIME_FAIL("did not find LWN at offset: " << dec << tmpBufferStart);
                    }
                }

                while (blockOffset < reader->blockSize) {
                    //next record
                    if (recordLeftToCopy == 0) {
                        if (blockOffset + 20 >= reader->blockSize)
                            break;

                        recordLength4 = (((uint64_t)oracleAnalyzer->read32(redoBlock + blockOffset)) + 3) & 0xFFFFFFFC;
                        if (recordLength4 > 0) {
                            uint64_t* length = (uint64_t*) (lwnChunks[lwnAllocated - 1]);

                            if (((*length + sizeof(struct LwnMember) + recordLength4 + 7) & 0xFFFFFFF8) > MEMORY_CHUNK_SIZE_MB * 1024 * 1024) {
                                if (lwnAllocated == MAX_LWN_CHUNKS) {
                                    RUNTIME_FAIL("all " << dec << MAX_LWN_CHUNKS << " LWN buffers allocated");
                                }

                                lwnChunks[lwnAllocated++] = oracleAnalyzer->getMemoryChunk("LWN transaction chunk", false);
                                if (lwnAllocated > lwnAllocatedMax)
                                    lwnAllocatedMax = lwnAllocated;
                                length = (uint64_t*) (lwnChunks[lwnAllocated - 1]);
                                *length = sizeof(uint64_t);
                            }

                            if (((*length + sizeof(struct LwnMember) + recordLength4 + 7) & 0xFFFFFFF8) > MEMORY_CHUNK_SIZE_MB * 1024 * 1024) {
                                RUNTIME_FAIL("Too big redo log record, length: " << recordLength4);
                            }

                            lwnMember = (struct LwnMember*) (lwnChunks[lwnAllocated - 1] + *length);
                            *length += (sizeof(struct LwnMember) + recordLength4 + 7) & 0xFFFFFFF8;
                            lwnMember->scn = oracleAnalyzer->read32(redoBlock + blockOffset + 8) |
                                    ((uint64_t)(oracleAnalyzer->read16(redoBlock + blockOffset + 6)) << 32);
                            lwnMember->subScn = oracleAnalyzer->read16(redoBlock + blockOffset + 12);
                            lwnMember->block = currentBlock;
                            lwnMember->offset = blockOffset;
                            lwnMember->length = recordLength4;
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

                        recordLeftToCopy = recordLength4;
                        recordPos = 0;
                    }

                    //nothing more
                    if (recordLeftToCopy == 0)
                        break;

                    uint64_t toCopy;
                    if (blockOffset + recordLeftToCopy > reader->blockSize)
                        toCopy = reader->blockSize - blockOffset;
                    else
                        toCopy = recordLeftToCopy;

                    memcpy(((uint8_t*)lwnMember) + sizeof(struct LwnMember) + recordPos, redoBlock + blockOffset, toCopy);
                    recordLeftToCopy -= toCopy;
                    blockOffset += toCopy;
                    recordPos += toCopy;
                }

                ++currentBlock;
                tmpBufferStart += reader->blockSize;
                redoBufferPos += reader->blockSize;

                //checkpoint
                TRACE(TRACE2_LWN, "LWN: checkpoint at " << dec << currentBlock << "/" << lwnEndBlock << " num: " << lwnNumCnt << "/" << lwnNumMax);
                if (currentBlock == lwnEndBlock && lwnNumCnt == lwnNumMax) {
                    try {
                        TRACE(TRACE2_LWN, "LWN: analyze");
                        for (uint64_t i = 0; i < lwnRecords; ++i) {
                            TRACE(TRACE2_LWN, "LWN: analyze blk: " << dec << lwnMembers[i]->block << " offset: " << lwnMembers[i]->offset <<
                                    " scn: " << lwnMembers[i]->scn << " subscn: " << lwnMembers[i]->subScn);
                            analyzeLwn(lwnMembers[i]);
                            if (lwnScnMax < lwnMembers[i]->scn)
                                lwnScnMax = lwnMembers[i]->scn;
                        }

                        if (lwnScn > oracleAnalyzer->firstScn &&
                                oracleAnalyzer->checkpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->blockSize, false) &&
                                oracleAnalyzer->checkpointOutputCheckpoint) {
                            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << lwnScn);
                            oracleAnalyzer->outputBuffer->processCheckpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->blockSize, switchRedo);

                            if (oracleAnalyzer->stopCheckpoints > 0) {
                                --oracleAnalyzer->stopCheckpoints;
                                if (oracleAnalyzer->stopCheckpoints == 0) {
                                    INFO("shutdown started - exhausted number of checkpoints");
                                    instrumentedShutdown = true;
                                }
                            }
                        }
                    } catch (RedoLogException& ex) {
                        if ((oracleAnalyzer->flags & REDO_FLAGS_ON_ERROR_CONTINUE) == 0) {
                            RUNTIME_FAIL("runtime error, aborting further redo log processing");
                        } else
                            WARNING("forced to continue working in spite of error");
                    }

                    TRACE(TRACE2_LWN, "LWN: scn: " << dec << lwnScnMax);
                    lwnNumCnt = 0;
                    freeLwn();
                    lwnConfirmedBlock = currentBlock;
                } else
                if (lwnNumCnt > lwnNumMax) {
                    RUNTIME_FAIL("LWN overflow: " << dec << lwnNumCnt << "/" << lwnNumMax);
                }

                if (redoBufferPos == MEMORY_CHUNK_SIZE) {
                    redoBufferPos = 0;
                    reader->bufferFree(redoBufferNum);
                    if (++redoBufferNum == oracleAnalyzer->readBufferMax)
                        redoBufferNum = 0;

                    {
                        unique_lock<mutex> lck(oracleAnalyzer->mtx);
                        reader->bufferStart = tmpBufferStart;
                        if (reader->status == READER_STATUS_READ) {
                            oracleAnalyzer->readerCond.notify_all();
                        }
                    }
                }
            }

            if (oracleAnalyzer->checkpointOutputCheckpoint) {
                if (!switchRedo && lwnScn > 0 && lwnScn > oracleAnalyzer->firstScn && tmpBufferStart == reader->bufferEnd &&
                        reader->ret == REDO_FINISHED) {
                    switchRedo = true;
                    TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << lwnScn << " with switch");
                    oracleAnalyzer->checkpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->blockSize, switchRedo);
                    oracleAnalyzer->outputBuffer->processCheckpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->blockSize, switchRedo);
                } else if (instrumentedShutdown) {
                    TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: on: " << lwnScn << " at exit");
                    oracleAnalyzer->outputBuffer->processCheckpoint(lwnScn, lwnTimestamp, sequence, currentBlock * reader->blockSize, false);
                }
            }

            if (instrumentedShutdown) {
                stopMain();
                oracleAnalyzer->shutdown = true;
            } else if (!oracleAnalyzer->shutdown) {
                unique_lock<mutex> lck(oracleAnalyzer->mtx);
                if (reader->bufferStart < tmpBufferStart)
                    reader->bufferStart = tmpBufferStart;

                //all work done
                if (tmpBufferStart == reader->bufferEnd) {
                    if (reader->ret == REDO_FINISHED && nextScn == ZERO_SCN && reader->nextScn != 0)
                        nextScn = reader->nextScn;

                    if (reader->ret == REDO_STOPPED || reader->ret == REDO_OVERWRITTEN) {
                        oracleAnalyzer->offset = lwnConfirmedBlock * reader->blockSize;
                        break;
                    } else
                    if (reader->ret == REDO_FINISHED || reader->status == READER_STATUS_SLEEPING)
                        break;
                    oracleAnalyzer->analyzerCond.wait(lck);
                }
            }
        }

        //print performance information
        if ((trace2 & TRACE2_PERFORMANCE) != 0) {
            double suppLogPercent = 0.0;
            if (currentBlock != startBlock)
                suppLogPercent = 100.0 * oracleAnalyzer->suppLogSize / ((currentBlock - startBlock) * reader->blockSize);

            if (group == 0) {
                time_t cEnd = oracleAnalyzer->getTime();
                double mySpeed = 0;
                double myTime = (cEnd - cStart) / 1000.0;
                if (myTime > 0)
                    mySpeed = (currentBlock - startBlock) * reader->blockSize * 1000.0 / 1024 / 1024 / myTime;

                double myReadSpeed = 0;
                if (reader->sumTime > 0)
                    myReadSpeed = (reader->sumRead * 1000000.0 / 1024 / 1024 / reader->sumTime);

                TRACE(TRACE2_PERFORMANCE, "PERFORMANCE: " << myTime << " ms, " <<
                        "Speed: " << fixed << setprecision(2) << mySpeed << " MB/s, " <<
                        "Redo log size: " << dec << ((currentBlock - startBlock) * reader->blockSize / 1024 / 1024) << " MB, " <<
                        "Read size: " << (reader->sumRead / 1024 / 1024) << " MB, " <<
                        "Read speed: " << myReadSpeed << " MB/s, " <<
                        "Max LWN size: " << dec << lwnAllocatedMax << ", " <<
                        "Supplemental redo log size: " << dec << oracleAnalyzer->suppLogSize << " bytes " <<
                        "(" << fixed << setprecision(2) << suppLogPercent << " %)");
            } else {
                TRACE(TRACE2_PERFORMANCE, "PERFORMANCE: " <<
                        "Redo log size: " << dec << ((currentBlock - startBlock) * reader->blockSize / 1024 / 1024) << " MB, " <<
                        "Max LWN size: " << dec << lwnAllocatedMax << ", " <<
                        "Supplemental redo log size: " << dec << oracleAnalyzer->suppLogSize << " bytes " <<
                        "(" << fixed << setprecision(2) << suppLogPercent << " %)");
            }
        }

        if (oracleAnalyzer->dumpRedoLog >= 1 && oracleAnalyzer->dumpStream.is_open()) {
            oracleAnalyzer->dumpStream << "END OF REDO DUMP" << endl;
            oracleAnalyzer->dumpStream.close();
        }

        freeLwn();
        return reader->ret;
    }

    ostream& operator<<(ostream& os, const RedoLog& redoLog) {
        os << "group: " << dec << redoLog.group << " scn: " << redoLog.firstScn << " to " <<
                ((redoLog.nextScn != ZERO_SCN) ? redoLog.nextScn : 0) << " seq: " << redoLog.sequence << " path: " << redoLog.path;
        return os;
    }
}
