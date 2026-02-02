/* Header for RedoLogRecord class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef REDO_LOG_RECORD_H_
#define REDO_LOG_RECORD_H_

#include "Ctx.h"
#include "exception/RedoLogException.h"
#include "types/FileOffset.h"
#include "types/LobId.h"
#include "types/Scn.h"
#include "types/Seq.h"
#include "types/Time.h"
#include "types/Types.h"
#include "types/Xid.h"

namespace OpenLogReplicator {
    class RedoLogRecord final {
    public:
        static constexpr uint8_t FB_N{0x01};
        static constexpr uint8_t FB_P{0x02};
        static constexpr uint8_t FB_L{0x04};
        static constexpr uint8_t FB_F{0x08};
        static constexpr uint8_t FB_D{0x10};
        static constexpr uint8_t FB_H{0x20};
        static constexpr uint8_t FB_C{0x40};
        static constexpr uint8_t FB_K{0x80};

        static constexpr typeDba INVALID_LOB_PAGE_NO{0xFFFFFFFF};

        static constexpr uint8_t OP_IUR{0x01};
        static constexpr uint8_t OP_IRP{0x02};
        static constexpr uint8_t OP_DRP{0x03};
        static constexpr uint8_t OP_LKR{0x04};
        static constexpr uint8_t OP_URP{0x05};
        static constexpr uint8_t OP_ORP{0x06};
        static constexpr uint8_t OP_MFC{0x07};
        static constexpr uint8_t OP_CFA{0x08};
        static constexpr uint8_t OP_CKI{0x09};
        static constexpr uint8_t OP_SKL{0x0A};
        static constexpr uint8_t OP_QMI{0x0B};
        static constexpr uint8_t OP_QMD{0x0C};
        static constexpr uint8_t OP_DSC{0x0E};
        static constexpr uint8_t OP_LMN{0x10};
        static constexpr uint8_t OP_LLB{0x11};
        static constexpr uint8_t OP_019{0x13};
        static constexpr uint8_t OP_SHK{0x14};
        static constexpr uint8_t OP_021{0x15};
        static constexpr uint8_t OP_CMP{0x16};
        static constexpr uint8_t OP_DCU{0x17};
        static constexpr uint8_t OP_MRK{0x18};
        static constexpr uint8_t OP_ROWDEPENDENCIES{0x40};

        static constexpr uint32_t REDO_VERSION_12_1{0x0C100000};
        static constexpr uint32_t REDO_VERSION_12_2{0x0C200000};
        static constexpr uint32_t REDO_VERSION_18_0{0x12000000};
        static constexpr uint32_t REDO_VERSION_19_0{0x13000000};
        static constexpr uint32_t REDO_VERSION_23_0{0x17000000};

        static constexpr uint8_t TYP_ENCRYPTED_TABLESPACE{0x80};

        uint8_t* dataExt;
        FileOffset fileOffset;
        Xid xid;                  // Transaction id
        Seq sequence;
        Scn scnRecord;
        Scn scn;
        Time timestamp;
        typeDbId dbId;
        typeSubScn subScn;
        typeConId conId;
        typeDba dba;
        typeDba bdba;             // Block DBA
        typeObj obj;              // Object ID
        typeDataObj dataObj;      // Data object ID
        uint32_t size;            // Size of the record

        typeCol col;              // LOB column ID
        typeField fieldCnt;
        typePos fieldPos;
        typeField rowData;
        typePos slotsDelta;
        typePos rowSizesDelta;
        typePos fieldSizesDelta;
        typePos nullsDelta;
        typePos colNumsDelta;
        uint8_t typ;
        uint8_t nRow;
        uint16_t flg;             // Flag
        typeOp1 opCode;           // Operation code
        typeOp1 opc;              // Operation code for UNDO
        typeSlot slot;
        uint16_t sizeDelt;
        uint8_t op;
        uint8_t ccData;
        uint8_t cc;
        uint8_t flags;            // Flags like xtype, kdoOpCode
        uint8_t fb;               // Row flags like F,L
        // supplemental log data
        uint8_t suppLogFb;
        uint16_t suppLogCC;
        uint16_t suppLogBefore;
        uint16_t suppLogAfter;
        typeSlot suppLogSlot;
        typeDba suppLogBdba;
        typeField suppLogRowData;
        typePos suppLogNumsDelta;
        typePos suppLogLenDelta;
        typeUsn usn;
        typeDba dba0;
        typeDba dba1;

        typeDba dba2;
        typeDba dba3;
        // lob data
        typeDba lobPageNo;
        uint32_t lobPageSize;
        uint32_t lobSizePages;
        typePos lobOffset;
        typePos lobData;
        typePos indKey;
        typePos indKeyData;
        typeSize lobSizeRest;
        typeSize lobDataSize;
        typeSize indKeySize;
        typeSize indKeyDataSize;
        uint8_t indKeyDataCode;
        LobId lobId;
        bool compressed;
        bool encryptedTablespace;
        // other
        uint32_t vectorNo;
        typeSlt slt;
        uint16_t cls;
        uint16_t rbl;
        uint16_t flgRecord;
        uint16_t thread;
        typeAfn afn;              // Absolute File Number
        uint8_t seq;

        typeObj recordObj;
        typeObj recordDataObj;

        RedoLogRecord& operator=(const RedoLogRecord& r) {
            if (this == &r)
                return *this;
            memcpy(reinterpret_cast<void*>(this), reinterpret_cast<const void*>(&r), sizeof(RedoLogRecord));
            return *this;
        }

        void clear() {
            memset(reinterpret_cast<void*>(this), 0, sizeof(RedoLogRecord));
        }

        [[nodiscard]] std::string toString() const {
            std::ostringstream ss;
            ss << "O scn: " << scnRecord.to64() <<
                    " scn: " << scn.toString() <<
                    " subScn: " << std::dec << subScn <<
                    " xid: " << xid.toString() <<
                    " op: " << std::setfill('0') << std::setw(4) << std::hex << opCode <<
                    " cls: " << std::dec << cls <<
                    " rbl: " << std::dec << rbl <<
                    " seq: " << std::dec << static_cast<uint>(seq) <<
                    " typ: " << std::dec << static_cast<uint>(typ) <<
                    " dbId: " << std::dec << dbId <<
                    " conId: " << std::dec << conId <<
                    " flgRecord: " << std::dec << flgRecord <<
                    " robj: " << std::dec << recordObj <<
                    " rdataObj: " << std::dec << recordDataObj <<
                    " nrow: " << std::dec << static_cast<uint>(nRow) <<
                    " afn: " << std::dec << afn <<
                    " size: " << std::dec << size <<
                    " dba: 0x" << std::hex << dba <<
                    " bdba: 0x" << std::hex << bdba <<
                    " obj: " << std::dec << obj <<
                    " dataobj: " << std::dec << dataObj <<
                    " usn: " << std::dec << usn <<
                    " slt: " << std::dec << static_cast<uint>(slt) <<
                    " flg: " << std::dec << static_cast<uint>(flg) <<
                    " opc: 0x" << std::hex << opc <<
                    " op: " << std::dec << static_cast<uint>(op) <<
                    " cc: " << std::dec << static_cast<uint>(cc) <<
                    " slot: " << std::dec << slot <<
                    " flags: 0x" << std::hex << static_cast<uint>(flags) <<
                    " fb: 0x" << std::hex << static_cast<uint>(fb);
            return ss.str();
        }

        [[nodiscard]] const uint8_t* data(uint shift = 0) const {
            if (dataExt != nullptr)
                return dataExt + shift;
            return reinterpret_cast<const uint8_t*>(this) + sizeof(RedoLogRecord) + shift;
        }

        static bool nextFieldOpt(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize,
                                 uint32_t code) {
            if (fieldNum >= redoLogRecord->fieldCnt)
                return false;
            ++fieldNum;

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldSize + 3) & 0xFFFC;
            fieldSize = ctx->read16(redoLogRecord->data(redoLogRecord->fieldSizesDelta + (static_cast<uint>(fieldNum) * 2)));

            if (unlikely(fieldPos + fieldSize > redoLogRecord->size))
                throw RedoLogException(50005, "field size out of vector, field: " + std::to_string(fieldNum) + "/" +
                                       std::to_string(redoLogRecord->fieldCnt) + ", pos: " + std::to_string(fieldPos) + ", size: " +
                                       std::to_string(fieldSize) + ", max: " + std::to_string(redoLogRecord->size) + ", code: " +
                                       std::to_string(code));
            return true;
        }

        static void nextField(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize, uint32_t code) {
            ++fieldNum;
            if (unlikely(fieldNum > redoLogRecord->fieldCnt))
                throw RedoLogException(50006, "field missing in vector, field: " + std::to_string(fieldNum) + "/" +
                                       std::to_string(redoLogRecord->fieldCnt) + ", ctx: " + std::to_string(redoLogRecord->rowData) + ", obj: " +
                                       std::to_string(redoLogRecord->obj) + ", dataobj: " + std::to_string(redoLogRecord->dataObj) + ", op: " +
                                       std::to_string(redoLogRecord->opCode) + ", cc: " + std::to_string(static_cast<uint>(redoLogRecord->cc)) +
                                       ", suppCC: " + std::to_string(redoLogRecord->suppLogCC) + ", fieldSize: " + std::to_string(fieldSize) +
                                       ", code: " + std::to_string(code));

            if (fieldNum == 1)
                fieldPos = redoLogRecord->fieldPos;
            else
                fieldPos += (fieldSize + 3) & 0xFFFC;
            fieldSize = ctx->read16(redoLogRecord->data(redoLogRecord->fieldSizesDelta + (static_cast<uint>(fieldNum) * 2)));

            if (unlikely(fieldPos + fieldSize > redoLogRecord->size))
                throw RedoLogException(50007, "field size out of vector, field: " + std::to_string(fieldNum) + "/" +
                                       std::to_string(redoLogRecord->fieldCnt) + ", pos: " + std::to_string(fieldPos) + ", size: " +
                                       std::to_string(fieldSize) + ", max: " + std::to_string(redoLogRecord->size) + ", code: " +
                                       std::to_string(code));
        }

        static void skipEmptyFields(const Ctx* ctx, const RedoLogRecord* redoLogRecord, typeField& fieldNum, typePos& fieldPos, typeSize& fieldSize) {
            while (fieldNum + 1U <= redoLogRecord->fieldCnt) {
                const typeSize nextFieldSize = ctx->read16(redoLogRecord->data(redoLogRecord->fieldSizesDelta + ((static_cast<uint>(fieldNum) + 1) * 2)));
                if (nextFieldSize != 0)
                    return;
                ++fieldNum;

                if (fieldNum == 1)
                    fieldPos = redoLogRecord->fieldPos;
                else
                    fieldPos += (fieldSize + 3) & 0xFFFC;
                fieldSize = nextFieldSize;

                if (unlikely(fieldPos + fieldSize > redoLogRecord->size))
                    throw RedoLogException(50008, "field size out of vector: field: " + std::to_string(fieldNum) + "/" +
                                           std::to_string(redoLogRecord->fieldCnt) + ", pos: " + std::to_string(fieldPos) + ", size: " +
                                           std::to_string(fieldSize) + ", max: " + std::to_string(redoLogRecord->size));
            }
        }
    };
}

#endif
