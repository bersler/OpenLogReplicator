/* Header for Parser class
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../common/types.h"
#include "../common/typeTime.h"
#include "../common/typeXid.h"

#ifndef PARSER_H_
#define PARSER_H_

namespace OpenLogReplicator {
    class Builder;
    class Reader;
    class Metadata;
    class Transaction;
    class TransactionBuffer;
    class XmlCtx;

    struct LwnMember {
        uint64_t offset;
        uint32_t size;
        typeScn scn;
        typeSubScn subScn;
        typeBlk block;

        bool operator<(const LwnMember& other) const {
            if (scn < other.scn)
                return true;
            if (other.scn < scn)
                return false;
            if (subScn < other.subScn)
                return true;
            if (other.subScn < subScn)
                return false;
            if (block < other.block)
                return true;
            if (block > other.block)
                return false;
            return (offset < other.offset);
        }
    };

    class Parser final {
    protected:
        static constexpr uint64_t MAX_LWN_CHUNKS = 512 * 2 / Ctx::MEMORY_CHUNK_SIZE_MB;
        static constexpr uint64_t MAX_RECORDS_IN_LWN = 1048576;

        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        RedoLogRecord zero;
        Transaction* lastTransaction;

        uint8_t* lwnChunks[MAX_LWN_CHUNKS];
        LwnMember* lwnMembers[MAX_RECORDS_IN_LWN + 1];
        uint64_t lwnAllocated;
        uint64_t lwnAllocatedMax;
        typeTime lwnTimestamp;
        typeScn lwnScn;
        typeBlk lwnCheckpointBlock;

        void freeLwn();
        void analyzeLwn(LwnMember* lwnMember);
        void appendToTransactionDdl(RedoLogRecord* redoLogRecord1);
        void appendToTransactionBegin(RedoLogRecord* redoLogRecord1);
        void appendToTransactionCommit(RedoLogRecord* redoLogRecord1);
        void appendToTransactionLob(RedoLogRecord* redoLogRecord1);
        void appendToTransactionIndex(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void appendToTransaction(RedoLogRecord* redoLogRecord1);
        void appendToTransactionRollback(RedoLogRecord* redoLogRecord1);
        void appendToTransaction(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void appendToTransactionRollback(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void dumpRedoVector(const uint8_t* data, typeSize recordSize) const;

    public:
        int64_t group;
        std::string path;
        typeSeq sequence;
        typeScn firstScn;
        typeScn nextScn;
        Reader* reader;

        Parser(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer, int64_t newGroup, const std::string& newPath);
        virtual ~Parser();

        uint64_t parse();
        std::string toString() const;
    };
}

#endif
