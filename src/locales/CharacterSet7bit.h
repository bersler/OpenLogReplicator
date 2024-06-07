/* Header for CharacterSet7bit class
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

#ifndef CHARACTER_SET_7BIT_H_
#define CHARACTER_SET_7BIT_H_

namespace OpenLogReplicator {
    class CharacterSet7bit : public CharacterSet {
    protected:
        const typeUnicode16* map;
        [[nodiscard]] virtual typeUnicode readMap(uint64_t character) const;

    public:
        CharacterSet7bit(const char* newName, const typeUnicode16* newMap);
        ~CharacterSet7bit() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;

        // Conversion arrays for 7-bit character sets
        static typeUnicode16 unicode_map_D7DEC[128];
        static typeUnicode16 unicode_map_D7SIEMENS9780X[128];
        static typeUnicode16 unicode_map_DK7SIEMENS9780X[128];
        static typeUnicode16 unicode_map_E7DEC[128];
        static typeUnicode16 unicode_map_E7SIEMENS9780X[128];
        static typeUnicode16 unicode_map_I7DEC[128];
        static typeUnicode16 unicode_map_I7SIEMENS9780X[128];
        static typeUnicode16 unicode_map_N7SIEMENS9780X[128];
        static typeUnicode16 unicode_map_NDK7DEC[128];
        static typeUnicode16 unicode_map_S7DEC[128];
        static typeUnicode16 unicode_map_S7SIEMENS9780X[128];
        static typeUnicode16 unicode_map_SF7ASCII[128];
        static typeUnicode16 unicode_map_SF7DEC[128];
        static typeUnicode16 unicode_map_US7ASCII[128];
    };
}

#endif
