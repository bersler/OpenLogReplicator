/* Header for RedoLogException class
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

#include <exception>

#include "types.h"

#ifndef REDOLOGEXCEPTION_H_
#define REDOLOGEXCEPTION_H_

#define REDOLOG_FAIL(x) {stringstream s; s << "ERROR: " << x << endl; cerr << s.str(); throw RedoLogException("error");}

using namespace std;

namespace OpenLogReplicator {

    class RedoLogException: public exception {
    public:
        const char* msg;

        RedoLogException(const char* msg);
        virtual ~RedoLogException();

        friend ostream& operator<<(ostream& os, const RedoLogException& ors);
    };
}

#endif
