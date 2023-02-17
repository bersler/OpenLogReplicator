/* Header for RedoLogRecord class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "Ctx.h"
#include "RedoLogException.h"
#include "types.h"
#include "typeLobId.h"
#include "typeXid.h"

#ifndef REDO_LOG_RECORD_H_
#define REDO_LOG_RECORD_H_

#define INVALID_LOB_PAGE_NO 0xFFFFFFFF

namespace OpenLogReplicator {
    class RedoLogRecord {
    public:
        RedoLogRecord* next;
        RedoLogRecord* prev;
        uint16_t cls;
        typeScn scnRecord;
        uint32_t rbl;
        uint8_t seq;
        uint8_t typ;
        typeConId conId;
        uint32_t flgRecord;
        uint32_t vectorNo;
        typeObj recordObj;
        typeObj recordDataObj;

        uint64_t lobData;
        uint64_t indKey;
        uint64_t indKeyData;
        uint32_t lobPageNo;
        uint32_t lobPageSize;
        typeLobId lobId;
        uint32_t lobLengthPages;
        uint16_t lobLengthRest;
        uint16_t lobDataLength;
        uint16_t indKeyLength;
        uint16_t indKeyDataLength;

        typeSeq sequence;
        typeScn scn;              // scn
        typeSubScn subScn;        // subscn
        uint8_t* data;            // ctx
        uint64_t dataOffset;
        typeField fieldCnt;
        uint64_t fieldPos;
        typeField rowData;
        uint8_t nrow;
        uint64_t slotsDelta;
        uint64_t rowLenghsDelta;
        uint64_t fieldLengthsDelta;
        uint64_t nullsDelta;
        uint64_t colNumsDelta;

        typeAfn afn;              // absolute file number
        uint64_t length;          // length
        typeDba dba;
        typeDba bdba;             // block DBA
        typeObj obj;              // object ID
        typeCol col;              // LOB column ID
        typeDataObj dataObj;      // data object ID
        uint32_t tsn;
        uint32_t undo;
        typeUsn usn;
        typeXid xid;              // transaction id
        typeUba uba;              // Undo Block Address
        uint32_t pdbId;

        typeSlt slt;
        typeRci rci;
        uint16_t flg;             // flag
        typeOp1 opCode;           // operation code
        typeOp1 opc;              // operation code for UNDO

        uint8_t op;
        uint8_t ccData;
        uint8_t cc;
        uint8_t itli;
        typeSlot slot;
        uint8_t flags;            // flags like xtype, kdoOpCode
        uint8_t fb;               // row flags like F,L
        uint8_t tabn;             // table number for clustered tables, for nonclustered: 0
        uint16_t sizeDelt;

        typeDba nridBdba;         // next row id bdba
        typeSlot nridSlot;        // next row id slot

        uint8_t suppLogType;
        uint8_t suppLogFb;
        uint16_t suppLogCC;
        uint16_t suppLogBefore;
        uint16_t suppLogAfter;
        typeDba suppLogBdba;
        typeSlot suppLogSlot;
        uint64_t suppLogRowData;
        uint64_t suppLogNumsDelta;
        uint64_t suppLogLenDelta;
        bool compressed;

        static bool nextFieldOpt(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength, uint32_t code) {
            if (fieldNum >= redoLogRecord->fieldCnt)
                return false;
            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = ctx->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (static_cast<uint64_t>(fieldNum) * 2));

            if (fieldPos + fieldLength > redoLogRecord->length)
                throw RedoLogException("field length out of vector, field: " + std::to_string(fieldNum) + "/" +
                        std::to_string(redoLogRecord->fieldCnt) + ", pos: " + std::to_string(fieldPos) + ", length:" +
                        std::to_string(fieldLength) + ", max: " + std::to_string(redoLogRecord->length) + ", code: " + std::to_string(code));
            return true;
        };

        static void nextField(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength, uint32_t code) {
            ++fieldNum;
            if (fieldNum > redoLogRecord->fieldCnt)
                throw RedoLogException("field missing in vector, field: " + std::to_string(fieldNum) + "/" +
                        std::to_string(redoLogRecord->fieldCnt) + ", ctx: " + std::to_string(redoLogRecord->rowData) + ", obj: " +
                        std::to_string(redoLogRecord->obj) + ", dataobj: " + std::to_string(redoLogRecord->dataObj) + ", op: " +
                        std::to_string(redoLogRecord->opCode) + ", cc: " + std::to_string(static_cast<uint64_t>(redoLogRecord->cc)) + ", suppCC: " +
                        std::to_string(redoLogRecord->suppLogCC) + ", fieldLength: " + std::to_string(fieldLength) + ", code: " +
                        std::to_string(code));

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldLength + 3) & 0xFFFC;
            fieldLength = ctx->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (static_cast<uint64_t>(fieldNum) * 2));

            if (fieldPos + fieldLength > redoLogRecord->length)
                throw RedoLogException("field length out of vector, field: " + std::to_string(fieldNum) + "/" +
                        std::to_string(redoLogRecord->fieldCnt) + ", pos: " + std::to_string(fieldPos) + ", length:" +
                        std::to_string(fieldLength) + ", max: " + std::to_string(redoLogRecord->length) + ", code: " + std::to_string(code));
        };

        static void skipEmptyFields(Ctx* ctx, RedoLogRecord* redoLogRecord, typeField& fieldNum, uint64_t& fieldPos, uint16_t& fieldLength) {
            uint16_t nextFieldLength;
            while (fieldNum + 1 <= redoLogRecord->fieldCnt) {
                nextFieldLength = ctx->read16(redoLogRecord->data + redoLogRecord->fieldLengthsDelta + (static_cast<uint64_t>(fieldNum) + 1) * 2);
                if (nextFieldLength != 0)
                    return;
                ++fieldNum;

                if (fieldNum == 1)
                    fieldPos = redoLogRecord->fieldPos;
                else
                    fieldPos += (fieldLength + 3) & 0xFFFC;
                fieldLength = nextFieldLength;

                if (fieldPos + fieldLength > redoLogRecord->length)
                    throw RedoLogException("field length out of vector: field: " + std::to_string(fieldNum) + "/" +
                            std::to_string(redoLogRecord->fieldCnt) + ", pos: " + std::to_string(fieldPos) + ", length:" +
                            std::to_string(fieldLength) + ", max: " + std::to_string(redoLogRecord->length));
            }
        }

        friend std::ostream& operator<<(std::ostream& os, const RedoLogRecord& redo);
    };
}

#endif
