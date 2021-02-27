/* Header for CharacterSetZHT16HKSCS31 class
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

#include "CharacterSet16bit.h"

#ifndef CHARACTERSETZHT16HKSCS31_H_
#define CHARACTERSETZHT16HKSCS31_H_

#define ZHT16HKSCS31_b1_min     0x81
#define ZHT16HKSCS31_b1_max     0xFE
#define ZHT16HKSCS31_b2_min     0x40
#define ZHT16HKSCS31_b2_max     0xFE

using namespace std;

namespace OpenLogReplicator {
    class CharacterSetZHT16HKSCS31 : public CharacterSet16bit {
    protected:
        static typeunicode32 unicode_map_ZHT16HKSCS31_2b[(ZHT16HKSCS31_b1_max - ZHT16HKSCS31_b1_min + 1) *
                                                         (ZHT16HKSCS31_b2_max - ZHT16HKSCS31_b2_min + 1)];
        virtual typeunicode readMap(uint64_t byte1, uint64_t byte2) const;

    public:
        CharacterSetZHT16HKSCS31();
        virtual ~CharacterSetZHT16HKSCS31();
    };
}

#endif
