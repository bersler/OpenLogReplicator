/* Header for CharacterSetZHS16GBK class
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef CHARACTER_SET_ZHS16GBK_H_
#define CHARACTER_SET_ZHS16GBK_H_

namespace OpenLogReplicator {
    class CharacterSetZHS16GBK final : public CharacterSet16bit {
    public:
        static constexpr uint64_t ZHS16GBK_b1_min = 0x81;
        static constexpr uint64_t ZHS16GBK_b1_max = 0xFE;
        static constexpr uint64_t ZHS16GBK_b2_min = 0x40;
        static constexpr uint64_t ZHS16GBK_b2_max = 0xFE;

    protected:
        static typeUnicode16 unicode_map_ZHS16GBK_2b[(ZHS16GBK_b1_max - ZHS16GBK_b1_min + 1) *
                                                     (ZHS16GBK_b2_max - ZHS16GBK_b2_min + 1)];

    public:
        CharacterSetZHS16GBK();
        ~CharacterSetZHS16GBK() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;
    };
}

#endif
