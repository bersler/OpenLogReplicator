/* Header for CharacterSetJA16SJIS class
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

#ifndef CHARACTER_SET_JA16SJIS_H_
#define CHARACTER_SET_JA16SJIS_H_

namespace OpenLogReplicator {
    class CharacterSetJA16SJIS : public CharacterSet16bit {
    public:
        static constexpr uint64_t JA16SJIS_b1_min = 0x81;
        static constexpr uint64_t JA16SJIS_b1_max = 0xFC;
        static constexpr uint64_t JA16SJIS_b2_min = 0x40;
        static constexpr uint64_t JA16SJIS_b2_max = 0xFC;

    protected:
        [[nodiscard]] virtual bool validCode(uint64_t byte1, uint64_t byte2) const;
        static typeUnicode16 unicode_map_JA16SJIS_2b[(JA16SJIS_b1_max - JA16SJIS_b1_min + 1) *
                                                     (JA16SJIS_b2_max - JA16SJIS_b2_min + 1)];

    public:
        explicit CharacterSetJA16SJIS(const char* newName);
        CharacterSetJA16SJIS();
        ~CharacterSetJA16SJIS() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;
    };
}

#endif
