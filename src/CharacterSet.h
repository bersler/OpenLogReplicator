/* Header for CharacterSet class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
along with Open Log Replicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include "types.h"

#ifndef CHARACTERSET_H_
#define CHARACTERSET_H_

#define UNICODE_UNKNOWN_CHARACTER           0xFFFD

using namespace std;

namespace OpenLogReplicator {

    class CharacterSet {
    protected:
        uint64_t badChar(uint64_t byte1);
        uint64_t badChar(uint64_t byte1, uint64_t byte2);
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3);
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4);
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5);
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5, uint64_t byte6);

    public:
        const char *name;

        CharacterSet(const char *name);
        virtual ~CharacterSet();

        virtual uint64_t decode(const uint8_t* &str, uint64_t &length) = 0;
    };
}

#endif
