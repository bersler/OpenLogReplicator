/* Header for RedoLogRecord class
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

#include "types.h"

#ifndef REDOLOGRECORD_H_
#define REDOLOGRECORD_H_

namespace OpenLogReplicator {
    class OracleAnalyzer;

    class RedoLogRecord {
    public:
        RedoLogRecord* next;
        RedoLogRecord* prev;
        uint16_t cls;
        typeSCN scnRecord;
        uint32_t rbl;
        uint8_t seq;
        uint8_t typ;
        typeCONID conId;
        uint32_t flgRecord;
        uint32_t vectorNo;
        typeOBJ recordObj;
        typeOBJ recordDataObj;

        typeSEQ sequence;
        typeSCN scn;              //scn
        typeSubSCN subScn;        //subscn
        uint8_t* data;            //data
        uint64_t dataOffset;
        typeFIELD fieldCnt;
        uint64_t fieldPos;
        typeFIELD rowData;
        uint8_t nrow;
        uint64_t slotsDelta;
        uint64_t rowLenghsDelta;
        uint64_t fieldLengthsDelta;
        uint64_t nullsDelta;
        uint64_t colNumsDelta;

        typeAFN afn;             //absolute file number
        uint64_t length;          //length
        typeDBA dba;
        typeDBA bdba;             //block DBA
        typeOBJ obj;              //object ID
        typeOBJ dataObj;          //data object ID
        uint32_t tsn;
        uint32_t undo;
        typeUSN usn;
        typeXID xid;              //transaction id
        typeUBA uba;              //Undo Block Address
        uint32_t pdbId;

        typeSLT slt;
        typeRCI rci;
        uint16_t flg;             //flag
        typeOP1 opCode;          //operation code
        typeOP1 opc;             //operation code for UNDO

        uint8_t op;
        uint8_t cc;
        uint8_t itli;
        typeSLOT slot;
        uint8_t flags;            //flags like xtype, kdoOpCode
        uint8_t fb;               //row flags like F,L
        uint8_t tabn;             //table number for clustered tables, for nonclustered: 0
        uint16_t sizeDelt;

        typeDBA nridBdba;         //next row id bdba
        typeSLOT nridSlot;        //next row id slot

        uint8_t suppLogType;
        uint8_t suppLogFb;
        uint16_t suppLogCC;
        uint16_t suppLogBefore;
        uint16_t suppLogAfter;
        typeDBA suppLogBdba;
        typeSLOT suppLogSlot;
        uint64_t suppLogRowData;
        uint64_t suppLogNumsDelta;
        uint64_t suppLogLenDelta;
        uint64_t opFlags;

        void dumpHex(std::ostream& str, OracleAnalyzer* oracleAnalyzer) const;
        friend std::ostream& operator<<(std::ostream& os, const RedoLogRecord& redo);
    };
}

#endif
