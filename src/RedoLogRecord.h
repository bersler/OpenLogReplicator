/* Header for RedoLogRecord class
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

#include "types.h"

#ifndef REDOLOGRECORD_H_
#define REDOLOGRECORD_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleObject;

    class RedoLogRecord {
    public:
        uint16_t cls;
        typescn scnRecord;
        uint32_t rbl;
        uint8_t seq;
        uint8_t typ;
        uint32_t conId;
        uint32_t flgRecord;
        uint32_t vectorNo;
        uint32_t recordObjn;
        uint32_t recordObjd;

        typescn scn;              //scn
        uint8_t *data;            //data
        uint16_t fieldNum;
        uint32_t fieldPos;
        uint16_t nrow;
        uint32_t slotsDelta;
        uint32_t rowLenghsDelta;
        uint32_t fieldLengthsDelta;
        uint32_t nullsDelta;

        uint32_t afn;             //file number
        uint32_t length;          //length
        uint32_t dba;
        uint32_t bdba;            //block DBA
        uint32_t objn;            //object ID
        uint32_t objd;            //data object ID
        uint32_t tsn;
        uint32_t undo;
        int16_t usn;
        OracleObject *object;
        typexid xid;              //transaction id
        typeuba uba;              //Undo Block Address

        uint8_t slt;
        uint8_t rci;
        uint16_t flg;             //flag (for opCode 0504)
        uint16_t opCode;          //operation code
        uint16_t opc;             //operation code for UNDO

        uint8_t op;
        uint8_t cc;
        uint8_t itli;
        uint16_t slot;
        uint8_t flags;            //flags like xtype, kdoOpCode

        void dump();
    };

#define ROW_HEADER_MEMORY (sizeof(struct RedoLogRecord)+sizeof(struct RedoLogRecord)+sizeof(typeuba)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(typescn))

}

#endif
