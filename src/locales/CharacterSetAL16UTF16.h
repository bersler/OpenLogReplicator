/* Header for CharacterSetAL16UTF16 class
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

#ifndef CHARACTER_SET_AL16UTF16_H_
#define CHARACTER_SET_AL16UTF16_H_

namespace OpenLogReplicator {
    class CharacterSetAL16UTF16 final : public CharacterSet {
    public:
        CharacterSetAL16UTF16();
        virtual ~CharacterSetAL16UTF16() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;
    };
}

#endif
