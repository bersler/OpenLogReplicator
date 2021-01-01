/* Header for RedoLogRecord class
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

#include "types.h"

#ifndef REDOLOGRECORD_H_
#define REDOLOGRECORD_H_

using namespace std;

namespace OpenLogReplicator {

#define FLAGS_XA                0x01
#define FLAGS_XR                0x02
#define FLAGS_CR                0x03
#define FLAGS_KDO_KDOM2         0x80

#define FLG_KTUCF_OP0504        0x0002
#define FLG_ROLLBACK_OP0504     0x0004

#define FLG_MULTIBLOCKUNDOHEAD  0x0001
#define FLG_MULTIBLOCKUNDOTAIL  0x0002
#define FLG_LASTBUFFERSPLIT     0x0004
#define FLG_KTUBL               0x0008
#define FLG_USERUNDODDONE       0x0010
#define FLG_ISTEMPOBJECT        0x0020
#define FLG_USERONLY            0x0040
#define FLG_TABLESPACEUNDO      0x0080
#define FLG_MULTIBLOCKUNDOMID   0x0100

#define FB_N                    0x01
#define FB_P                    0x02
#define FB_L                    0x04
#define FB_F                    0x08
#define FB_D                    0x10
#define FB_H                    0x20
#define FB_C                    0x40
#define FB_K                    0x80

#define OP_IUR                  0x01
#define OP_IRP                  0x02
#define OP_DRP                  0x03
#define OP_LKR                  0x04
#define OP_URP                  0x05
#define OP_ORP                  0x06
#define OP_MFC                  0x07
#define OP_CFA                  0x08
#define OP_CKI                  0x09
#define OP_SKL                  0x0A
#define OP_QMI                  0x0B
#define OP_QMD                  0x0C
#define OP_DSC                  0x0E
#define OP_LMN                  0x10
#define OP_LLB                  0x11
#define OP__19                  0x13
#define OP_SHK                  0x14
#define OP__21                  0x15
#define OP_CMP                  0x16
#define OP_DCU                  0x17
#define OP_MRK                  0x18
#define OP_ROWDEPENDENCIES      0x40

#define KTBOP_F                 0x01
#define KTBOP_C                 0x02
#define KTBOP_Z                 0x03
#define KTBOP_L                 0x04
#define KTBOP_N                 0x06
#define KTBOP_BLOCKCLEANOUT     0x10

#define SUPPLOG_UPDATE          0x01
#define SUPPLOG_INSERT          0x02
#define SUPPLOG_DELETE          0x04

#define OPFLAG_BEGIN_TRANS      0x01

    class OracleObject;
    class OracleAnalyzer;

    class RedoLogRecord {
    public:
        RedoLogRecord *next;
        RedoLogRecord *prev;
        uint16_t cls;
        typeSCN scnRecord;
        uint32_t rbl;
        uint8_t seq;
        uint8_t typ;
        typeconid conId;
        uint32_t flgRecord;
        uint32_t vectorNo;
        typeOBJ recordObj;
        typeOBJ recordDataObj;

        typeSEQ sequence;
        typeSCN scn;              //scn
        typeSubSCN subScn;        //subscn
        uint8_t *data;            //data
        uint16_t fieldCnt;
        uint64_t fieldPos;
        uint64_t rowData;
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
        int16_t usn;
        OracleObject *object;
        typeXID xid;              //transaction id
        typeUBA uba;              //Undo Block Address
        uint32_t pdbId;

        typeslt slt;
        typerci rci;
        uint16_t flg;             //flag
        typeop1 opCode;          //operation code
        typeop1 opc;             //operation code for UNDO

        uint8_t op;
        uint8_t cc;
        uint8_t itli;
        typeSLOT slot;
        uint8_t flags;            //flags like xtype, kdoOpCode
        uint8_t fb;               //row flags like F,L
        uint8_t tabn;             //table number for clustered tables, for nonclustered: 0

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

        void dumpHex(ostream &str, OracleAnalyzer *oracleAnalyzer) const;
        friend ostream& operator<<(ostream& os, const RedoLogRecord& redo);
    };
}

#endif
