/* Header for CharacterSetZHT32TRIS class
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

#ifndef CHARACTER_SET_ZHT32TRIS_H_
#define CHARACTER_SET_ZHT32TRIS_H_


namespace OpenLogReplicator {
    class CharacterSetZHT32TRIS final : public CharacterSet {
    public:
        static constexpr uint64_t ZHT32TRIS_b1 = 0x8E;
        static constexpr uint64_t ZHT32TRIS_b2_min = 0xA1;
        static constexpr uint64_t ZHT32TRIS_b2_max = 0xAE;
        static constexpr uint64_t ZHT32TRIS_b3_min = 0xA1;
        static constexpr uint64_t ZHT32TRIS_b3_max = 0xFE;
        static constexpr uint64_t ZHT32TRIS_b4_min = 0xA1;
        static constexpr uint64_t ZHT32TRIS_b4_max = 0xFE;

    protected:
        static typeUnicode16 unicode_map_ZHT32TRIS_4b[(ZHT32TRIS_b2_max - ZHT32TRIS_b2_min + 1) *
                                                      (ZHT32TRIS_b3_max - ZHT32TRIS_b3_min + 1) *
                                                      (ZHT32TRIS_b4_max - ZHT32TRIS_b4_min + 1)];

    public:
        CharacterSetZHT32TRIS();
        ~CharacterSetZHT32TRIS() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;
    };
}

#endif
