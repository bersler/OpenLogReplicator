/* Header for CharacterSetAL16UTF16 class
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

#include "CharacterSet.h"

#ifndef CHARACTERSETAL16UTF16_H_
#define CHARACTERSETAL16UTF16_H_

using namespace std;

namespace OpenLogReplicator {

    class CharacterSetAL16UTF16 : public CharacterSet {
    public:
        CharacterSetAL16UTF16();
        virtual ~CharacterSetAL16UTF16();

        virtual typeunicode decode(const uint8_t* &str, uint64_t &length);
    };
}

#endif
