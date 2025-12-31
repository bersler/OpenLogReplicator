/* Header for CharacterSet class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef CHARACTER_SET_H_
#define CHARACTER_SET_H_

#include "../common/types/Types.h"
#include "../common/types/Xid.h"

namespace OpenLogReplicator {
    class Ctx;

    class CharacterSet {
    public:
        static constexpr uint64_t MAX_CHARACTER_LENGTH{8};
        static constexpr uint64_t UNICODE_UNKNOWN_CHARACTER{0xFFFD};

    protected:
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, Xid xid, uint64_t byte1) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, Xid xid, uint64_t byte1, uint64_t byte2) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, Xid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, Xid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, Xid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5) const;
        [[nodiscard]] uint64_t badChar(const Ctx* ctx, Xid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5,
                                       uint64_t byte6) const;

    public:
        std::string name;

        explicit CharacterSet(std::string newName);
        virtual ~CharacterSet() = default;

        virtual typeUnicode decode(const Ctx* ctx, Xid xid, const uint8_t*& str, uint64_t& length) const = 0;
    };
}

#endif
