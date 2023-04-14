/* Header for Parser class
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

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../common/types.h"
#include "../common/typeTime.h"
#include "../common/typeXid.h"

#ifndef PARSER_H_
#define PARSER_H_

#define MAX_LWN_CHUNKS (512*2/MEMORY_CHUNK_SIZE_MB)

namespace OpenLogReplicator {
    class Builder;
    class Reader;
    class Metadata;
    class TransactionBuffer;

    struct LwnMember {
        uint64_t offset;
        uint64_t length;
        typeScn scn;
        typeSubScn subScn;
        typeBlk block;
    };

    class Parser {
    protected:
        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        RedoLogRecord zero;

        uint8_t* lwnChunks[MAX_LWN_CHUNKS];
        LwnMember* lwnMembers[MAX_RECORDS_IN_LWN];
        uint64_t lwnAllocated;
        uint64_t lwnAllocatedMax;
        typeTime lwnTimestamp;
        typeScn lwnScn;
        uint64_t lwnCheckpointBlock;

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
        void dumpRedoVector(uint8_t* data, uint64_t recordLength4) const;

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
        std::string toString();
    };
}

#endif
