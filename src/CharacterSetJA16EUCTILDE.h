/* Header for CharacterSetJA16EUCTILDE class
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

#include "CharacterSetJA16EUC.h"

#ifndef CHARACTERSETJA16EUCTILDE_H_
#define CHARACTERSETJA16EUCTILDE_H_

using namespace std;

namespace OpenLogReplicator {

    class CharacterSetJA16EUCTILDE : public CharacterSetJA16EUC {
    protected:
        virtual typeunicode readMap2(uint64_t byte1, uint64_t byte2);

    public:
        CharacterSetJA16EUCTILDE();
        virtual ~CharacterSetJA16EUCTILDE();
    };
}

#endif
