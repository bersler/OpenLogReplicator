/* Header for CharacterSetJA16EUC class
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

#ifndef CHARACTERSETJA16EUC_H_
#define CHARACTERSETJA16EUC_H_

#define JA16EUC_b1_min      0x8E
#define JA16EUC_b1_max      0xFE
#define JA16EUC_b2_min      0xA1
#define JA16EUC_b2_max      0xFE
#define JA16EUC_b3_min      0xA1
#define JA16EUC_b3_max      0xFE

using namespace std;

namespace OpenLogReplicator {

    class CharacterSetJA16EUC : public CharacterSet {
    protected:
        virtual bool validCode(uint64_t byte1, uint64_t byte2) const;
        virtual typeunicode readMap2(uint64_t byte1, uint64_t byte2) const;
        virtual typeunicode readMap3(uint64_t byte2, uint64_t byte3) const;
        static typeunicode16 unicode_map_JA16EUC_2b[(JA16EUC_b1_max - JA16EUC_b1_min + 1) *
                                                    (JA16EUC_b2_max - JA16EUC_b2_min + 1)];
        static typeunicode16 unicode_map_JA16EUC_3b[(JA16EUC_b2_max - JA16EUC_b2_min + 1) *
                                                    (JA16EUC_b3_max - JA16EUC_b3_min + 1)];

    public:
        CharacterSetJA16EUC();
        CharacterSetJA16EUC(const char *name);
        virtual ~CharacterSetJA16EUC();

        virtual typeunicode decode(const uint8_t* &str, uint64_t &length) const;
    };
}

#endif
