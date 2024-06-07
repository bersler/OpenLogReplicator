/* Header for CharacterSetZHS32GB18030 class
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

#include "CharacterSet.h"

#ifndef CHARACTER_SET_ZHS32GB18030_H_
#define CHARACTER_SET_ZHS32GB18030_H_

namespace OpenLogReplicator {
    class CharacterSetZHS32GB18030 final : public CharacterSet {
    public:
        static constexpr uint64_t ZHS32GB18030_2_b1_min = 0x81;
        static constexpr uint64_t ZHS32GB18030_2_b1_max = 0xFE;
        static constexpr uint64_t ZHS32GB18030_2_b2_min = 0x40;
        static constexpr uint64_t ZHS32GB18030_2_b2_max = 0xFE;

        static constexpr uint64_t ZHS32GB18030_41_b1_min = 0x81;
        static constexpr uint64_t ZHS32GB18030_41_b1_max = 0x84;
        static constexpr uint64_t ZHS32GB18030_41_b2_min = 0x30;
        static constexpr uint64_t ZHS32GB18030_41_b2_max = 0x39;
        static constexpr uint64_t ZHS32GB18030_41_b3_min = 0x81;
        static constexpr uint64_t ZHS32GB18030_41_b3_max = 0xFE;
        static constexpr uint64_t ZHS32GB18030_41_b4_min = 0x30;
        static constexpr uint64_t ZHS32GB18030_41_b4_max = 0x39;

        static constexpr uint64_t ZHS32GB18030_42_b1_min = 0x90;
        static constexpr uint64_t ZHS32GB18030_42_b1_max = 0xE3;
        static constexpr uint64_t ZHS32GB18030_42_b2_min = 0x30;
        static constexpr uint64_t ZHS32GB18030_42_b2_max = 0x39;
        static constexpr uint64_t ZHS32GB18030_42_b3_min = 0x81;
        static constexpr uint64_t ZHS32GB18030_42_b3_max = 0xFE;
        static constexpr uint64_t ZHS32GB18030_42_b4_min = 0x30;
        static constexpr uint64_t ZHS32GB18030_42_b4_max = 0x39;

    protected:
        static typeUnicode16 unicode_map_ZHS32GB18030_2b[(ZHS32GB18030_2_b1_max - ZHS32GB18030_2_b1_min + 1) *
                                                         (ZHS32GB18030_2_b2_max - ZHS32GB18030_2_b2_min + 1)];
        static typeUnicode16 unicode_map_ZHS32GB18030_4b1[(ZHS32GB18030_41_b1_max - ZHS32GB18030_41_b1_min + 1) *
                                                          (ZHS32GB18030_41_b2_max - ZHS32GB18030_41_b2_min + 1) *
                                                          (ZHS32GB18030_41_b3_max - ZHS32GB18030_41_b3_min + 1) *
                                                          (ZHS32GB18030_41_b4_max - ZHS32GB18030_41_b4_min + 1)];
        static typeUnicode32 unicode_map_ZHS32GB18030_4b2[(ZHS32GB18030_42_b1_max - ZHS32GB18030_42_b1_min + 1) *
                                                          (ZHS32GB18030_42_b2_max - ZHS32GB18030_42_b2_min + 1) *
                                                          (ZHS32GB18030_42_b3_max - ZHS32GB18030_42_b3_min + 1) *
                                                          (ZHS32GB18030_42_b4_max - ZHS32GB18030_42_b4_min + 1)];

    public:
        CharacterSetZHS32GB18030();
        ~CharacterSetZHS32GB18030() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;
    };
}

#endif
