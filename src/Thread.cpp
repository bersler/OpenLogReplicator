/* Base class for source and target thread
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

#include <iostream>
#include "Thread.h"

#include "ConfigurationException.h"
#include "MemoryException.h"
#include "OracleObject.h"
#include "OracleColumn.h"
#include "RedoLogRecord.h"
#include "RedoLogException.h"
#include "RuntimeException.h"

using namespace std;

void stopMain();

namespace OpenLogReplicator {

    Thread::Thread(const string alias) :
        shutdown(false),
        pthread(0),
        alias(alias) {
    }

    Thread::~Thread() {
    }

    void *Thread::runStatic(void *context){
        void *ret = nullptr;
        try {
            ret = ((Thread *) context)->run();
        } catch(ConfigurationException &ex) {
            cerr << "ERROR: configuration error: " << ex.msg << endl;
            stopMain();
        } catch(RedoLogException &ex) {
            cerr << "ERROR: error reading redo log: " << ex.msg << endl;
            stopMain();
        } catch(RuntimeException &ex) {
            cerr << "ERROR: runtime: " << ex.msg << endl;
            stopMain();
        } catch (MemoryException &e) {
            cerr << "ERROR: memory allocation error for " << e.msg << " for " << e.bytes << " bytes" << endl;
            stopMain();
        }
        return ret;
    }

    void Thread::stop(void) {
        shutdown = true;
    }
}
