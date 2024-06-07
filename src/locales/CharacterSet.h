/* Header for CharacterSet class
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

#include "../common/types.h"
#include "../common/typeXid.h"

#ifndef CHARACTER_SET_H_
#define CHARACTER_SET_H_

namespace OpenLogReplicator {
    class Ctx;

    class CharacterSet {
    public:
        static constexpr uint64_t MAX_CHARACTER_LENGTH = 8;
        static constexpr uint64_t UNICODE_UNKNOWN_CHARACTER = 0xFFFD;

    protected:
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, typeXid xid, uint64_t byte1) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5,
                                       uint64_t byte6) const;

    public:
        const char* name;

        explicit CharacterSet(const char* newName);
        virtual ~CharacterSet();

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const = 0;
    };
}

#endif
