/* Header for CharacterSetZHT32EUC class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef CHARACTERSETZHT32EUC_H_
#define CHARACTERSETZHT32EUC_H_

#define ZHT32EUC_2_b1_min    0xA1
#define ZHT32EUC_2_b1_max    0xFD
#define ZHT32EUC_2_b2_min    0xA1
#define ZHT32EUC_2_b2_max    0xFE

#define ZHT32EUC_4_b1        0x8E
#define ZHT32EUC_4_b2_min    0xA2
#define ZHT32EUC_4_b2_max    0xAE
#define ZHT32EUC_4_b3_min    0xA1
#define ZHT32EUC_4_b3_max    0xF2
#define ZHT32EUC_4_b4_min    0xA1
#define ZHT32EUC_4_b4_max    0xFE

using namespace std;

namespace OpenLogReplicator {

    class CharacterSetZHT32EUC : public CharacterSet {
    protected:
        static typeunicode16 unicode_map_ZHT32EUC_2b[(ZHT32EUC_2_b1_max - ZHT32EUC_2_b1_min + 1) *
                                                     (ZHT32EUC_2_b2_max - ZHT32EUC_2_b2_min + 1)];
        static typeunicode16 unicode_map_ZHT32EUC_4b[(ZHT32EUC_4_b2_max - ZHT32EUC_4_b2_min + 1) *
                                                     (ZHT32EUC_4_b3_max - ZHT32EUC_4_b3_min + 1) *
                                                     (ZHT32EUC_4_b4_max - ZHT32EUC_4_b4_min + 1)];

    public:
        CharacterSetZHT32EUC();
        virtual ~CharacterSetZHT32EUC();

        virtual typeunicode decode(const uint8_t* &str, uint64_t &length) const;
    };
}

#endif
