/* Header for CharacterSetZHS16GBK class
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

#include "CharacterSet16bit.h"

#ifndef CHARACTERSETZHS16GBK_H_
#define CHARACTERSETZHS16GBK_H_

#define ZHS16GBK_b1_min         0x81
#define ZHS16GBK_b1_max         0xFE
#define ZHS16GBK_b2_min         0x40
#define ZHS16GBK_b2_max         0xFE

using namespace std;

namespace OpenLogReplicator {

    class CharacterSetZHS16GBK : public CharacterSet16bit {
    protected:
        static typeunicode16 unicode_map_ZHS16GBK_2b[(ZHS16GBK_b1_max - ZHS16GBK_b1_min + 1) *
                                                     (ZHS16GBK_b2_max - ZHS16GBK_b2_min + 1)];

    public:
        CharacterSetZHS16GBK();
        virtual ~CharacterSetZHS16GBK();

        virtual typeunicode decode(const uint8_t* &str, uint64_t &length) const;
    };
}

#endif
