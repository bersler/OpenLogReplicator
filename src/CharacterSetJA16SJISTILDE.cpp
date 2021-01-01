/* Class to handle character set JA16SJISTILDE
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

#include "CharacterSetJA16SJISTILDE.h"

using namespace std;

namespace OpenLogReplicator {
    CharacterSetJA16SJISTILDE::CharacterSetJA16SJISTILDE() :
        CharacterSetJA16SJIS("JA16SJISTILDE") {
    }

    CharacterSetJA16SJISTILDE::~CharacterSetJA16SJISTILDE() {
    }

    typeunicode CharacterSetJA16SJISTILDE::readMap(uint64_t byte1, uint64_t byte2) const {
        if (byte1 == 0x81 && byte2 == 0x60)
            return 0xFF5E;

        return CharacterSet16bit::readMap(byte1, byte2);
    }
}
