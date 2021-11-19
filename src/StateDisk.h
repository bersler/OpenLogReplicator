/* Header for StateDisk class
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

#include "State.h"

#ifndef STATEDISK_H_
#define STATEDISK_H_

using namespace std;

namespace OpenLogReplicator {
    class StateDisk : public State {
    protected:
        string path;

    public:
        StateDisk(const char* path);
        virtual ~StateDisk();

        virtual void list(set<string>& namesList);
        virtual bool read(string& name, uint64_t maxSize, string& in, bool noFail);
        virtual void write(string& name, stringstream& out);
        virtual void drop(string& name);
    };
}

#endif
