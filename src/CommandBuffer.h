/* Header for CommandBuffer class
   Copyright (C) 2018-2019 Adam Leszczynski.

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

#include <stdint.h>
#include <string>
#include <mutex>
#include <condition_variable>
#include "types.h"

#ifndef COMMANDBUFFER_H_
#define COMMANDBUFFER_H_

using namespace std;

namespace OpenLogReplicator {

    class Writer;

    class CommandBuffer {
    protected:
        volatile bool shutdown;
    public:
        static char translationMap[65];
        Writer *writer;
        uint8_t *intraThreadBuffer;
        mutex mtx;
        condition_variable readersCond;
        condition_variable writerCond;
        volatile uint64_t posStart;
        volatile uint64_t posEnd;
        volatile uint64_t posEndTmp;
        volatile uint32_t posSize;

        void terminate(void);
        CommandBuffer* appendRowid(uint32_t objn, uint32_t objd, uint16_t afn, uint32_t bdba, uint16_t slot);
        CommandBuffer* appendEscape(const uint8_t *str, uint32_t length);
        CommandBuffer* append(const string str);
        CommandBuffer* append(char chr);
        CommandBuffer* beginTran();
        CommandBuffer* commitTran();
        CommandBuffer* rewind();
        uint32_t currentTranSize();

        CommandBuffer();
        virtual ~CommandBuffer();
    };
}

#endif
