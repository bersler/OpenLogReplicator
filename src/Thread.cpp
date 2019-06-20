/* Base class for source and target thread
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

#include <iostream>
#include "Thread.h"

#include "CommandBuffer.h"
#include "OracleObject.h"
#include "OracleColumn.h"
#include "RedoLogRecord.h"
#include "RedoLogException.h"

using namespace std;

namespace OpenLogReplicator {

    Thread::Thread(const string alias, CommandBuffer *commandBuffer) :
        shutdown(false),
        pthread(0),
        alias(alias.c_str()),
        commandBuffer(commandBuffer) {
    }

    Thread::~Thread() {
    }

    void *Thread::runStatic(void *context){
        void *ret = nullptr;
        try {
            ret = ((Thread *) context)->run();
        } catch(RedoLogException &ex) {
                cerr << "ERROR: " << ex.msg << endl;
        }
        return ret;
    }

    void Thread::terminate(void) {
        this->shutdown = true;
    }
}
