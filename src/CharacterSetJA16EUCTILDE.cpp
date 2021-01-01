/* Class to handle character set JA16EUCTILDE
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

#include "CharacterSetJA16EUCTILDE.h"

using namespace std;

namespace OpenLogReplicator {
    CharacterSetJA16EUCTILDE::CharacterSetJA16EUCTILDE() :
        CharacterSetJA16EUC("JA16EUCTILDE") {
    }

    CharacterSetJA16EUCTILDE::~CharacterSetJA16EUCTILDE() {
    }

    typeunicode CharacterSetJA16EUCTILDE::readMap2(uint64_t byte1, uint64_t byte2) const {
        if (byte1 == 0xA1 && byte2 == 0xC1)
            return 0xFF5E;

        return CharacterSetJA16EUC::readMap2(byte1, byte2);
    }
}
