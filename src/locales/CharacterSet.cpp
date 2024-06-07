/* Class to handle different character sets
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
#include "../common/Ctx.h"
#include "../common/types.h"

namespace OpenLogReplicator {
    CharacterSet::CharacterSet(const char* newName) :
            name(newName) {
    }

    CharacterSet::~CharacterSet() = default;

    uint64_t CharacterSet::badChar(const Ctx* ctx, typeXid xid, uint64_t byte1) const {
        ctx->warning(60008, "can't decode character: (" + std::to_string(byte1) + ") using character set " + name + ", xid: " +
                            xid.toString());
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2) const {
        ctx->warning(60008, "can't decode character: (" + std::to_string(byte1) + ", " + std::to_string(byte2) +
                            ") using character set " + name + ", xid: " + xid.toString());
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3) const {
        ctx->warning(60008, "can't decode character: (" + std::to_string(byte1) + ", " + std::to_string(byte2) + ", " +
                            std::to_string(byte3) + ") using character set " + name + ", xid: " + xid.toString());
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4) const {
        ctx->warning(60008, "can't decode character: (" + std::to_string(byte1) + ", " + std::to_string(byte2) + ", " +
                            std::to_string(byte3) + ", " + std::to_string(byte4) + ") using character set " + name + ", xid: " + xid.toString());
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5) const {
        ctx->warning(60008, "can't decode character: (" + std::to_string(byte1) + ", " + std::to_string(byte2) + ", " +
                            std::to_string(byte3) + ", " + std::to_string(byte4) + ", " + std::to_string(byte5) + ") using character set " + name + ", xid: " +
                            xid.toString());
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(const Ctx* ctx, typeXid xid, uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5,
                                   uint64_t byte6) const {
        ctx->warning(60008, "can't decode character: (" + std::to_string(byte1) + ", " + std::to_string(byte2) + ", " +
                            std::to_string(byte3) + ", " + std::to_string(byte4) + ", " + std::to_string(byte5) + std::to_string(byte6) +
                            ") using character set " + name + ", xid: " + xid.toString());
        return UNICODE_UNKNOWN_CHARACTER;
    }
}
