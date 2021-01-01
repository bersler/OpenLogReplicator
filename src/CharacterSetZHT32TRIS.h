/* Header for CharacterSetZHT32TRIS class
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

#include "CharacterSet.h"

#ifndef CHARACTERSETZHT32TRIS_H_
#define CHARACTERSETZHT32TRIS_H_

#define ZHT32TRIS_b1        0x8E
#define ZHT32TRIS_b2_min    0xA1
#define ZHT32TRIS_b2_max    0xAE
#define ZHT32TRIS_b3_min    0xA1
#define ZHT32TRIS_b3_max    0xFE
#define ZHT32TRIS_b4_min    0xA1
#define ZHT32TRIS_b4_max    0xFE

using namespace std;

namespace OpenLogReplicator {

    class CharacterSetZHT32TRIS : public CharacterSet {
    protected:
        static typeunicode16 unicode_map_ZHT32TRIS_4b[(ZHT32TRIS_b2_max - ZHT32TRIS_b2_min + 1) *
                                                      (ZHT32TRIS_b3_max - ZHT32TRIS_b3_min + 1) *
                                                      (ZHT32TRIS_b4_max - ZHT32TRIS_b4_min + 1)];

    public:
        CharacterSetZHT32TRIS();
        virtual ~CharacterSetZHT32TRIS();

        virtual typeunicode decode(const uint8_t* &str, uint64_t &length) const;
    };
}

#endif
