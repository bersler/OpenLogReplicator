/* Header for CharacterSet class
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

#include "types.h"

#ifndef CHARACTERSET_H_
#define CHARACTERSET_H_

#define UNICODE_UNKNOWN_CHARACTER           0xFFFD

using namespace std;

namespace OpenLogReplicator {
    class CharacterSet {
    protected:
        uint64_t badChar(uint64_t byte1) const;
        uint64_t badChar(uint64_t byte1, uint64_t byte2) const;
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3) const;
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4) const;
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5) const;
        uint64_t badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5, uint64_t byte6) const;

    public:
        const char *name;

        CharacterSet(const char *name);
        virtual ~CharacterSet();

        virtual uint64_t decode(const uint8_t* &str, uint64_t &length) const = 0;
    };
}

#endif
