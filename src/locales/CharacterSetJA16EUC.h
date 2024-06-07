/* Header for CharacterSetJA16EUC class
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

#ifndef CHARACTER_SET_JA16EUC_H_
#define CHARACTER_SET_JA16EUC_H_

namespace OpenLogReplicator {
    class CharacterSetJA16EUC : public CharacterSet {
    public:
        static constexpr uint64_t JA16EUC_b1_min = 0x8E;
        static constexpr uint64_t JA16EUC_b1_max = 0xFE;
        static constexpr uint64_t JA16EUC_b2_min = 0xA1;
        static constexpr uint64_t JA16EUC_b2_max = 0xFE;
        static constexpr uint64_t JA16EUC_b3_min = 0xA1;
        static constexpr uint64_t JA16EUC_b3_max = 0xFE;

    protected:
        [[nodiscard]] virtual bool validCode(uint64_t byte1, uint64_t byte2) const;
        [[nodiscard]] virtual typeUnicode readMap2(uint64_t byte1, uint64_t byte2) const;
        [[nodiscard]] virtual typeUnicode readMap3(uint64_t byte2, uint64_t byte3) const;
        static typeUnicode16 unicode_map_JA16EUC_2b[(JA16EUC_b1_max - JA16EUC_b1_min + 1) *
                                                    (JA16EUC_b2_max - JA16EUC_b2_min + 1)];
        static typeUnicode16 unicode_map_JA16EUC_3b[(JA16EUC_b2_max - JA16EUC_b2_min + 1) *
                                                    (JA16EUC_b3_max - JA16EUC_b3_min + 1)];

    public:
        CharacterSetJA16EUC();
        explicit CharacterSetJA16EUC(const char* newName);
        virtual ~CharacterSetJA16EUC() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;
    };
}

#endif
