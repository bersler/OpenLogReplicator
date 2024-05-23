/* Header for CharacterSetKO16KSCCS class
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

#ifndef CHARACTER_SET_KO16KSCCS_H_
#define CHARACTER_SET_KO16KSCCS_H_

namespace OpenLogReplicator {
    class CharacterSetKO16KSCCS final : public CharacterSet16bit {
    public:
        static constexpr uint64_t KO16KSCCS_b1_min = 0x84;
        static constexpr uint64_t KO16KSCCS_b1_max = 0xF9;
        static constexpr uint64_t KO16KSCCS_b2_min = 0x31;
        static constexpr uint64_t KO16KSCCS_b2_max = 0xFE;

    protected:
        static typeUnicode16 unicode_map_KO16KSCCS_2b[(KO16KSCCS_b1_max - KO16KSCCS_b1_min + 1) *
                                                      (KO16KSCCS_b2_max - KO16KSCCS_b2_min + 1)];
        [[nodiscard]] virtual bool validCode(uint64_t byte1, uint64_t byte2) const;

    public:
        CharacterSetKO16KSCCS();
        virtual ~CharacterSetKO16KSCCS() override;
    };
}

#endif
