/* Header for LobKey class
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

#include "types.h"
#include "typeLobId.h"

#ifndef LOB_KEY_H_
#define LOB_KEY_H_

namespace OpenLogReplicator {
    class LobKey final {
    public:
        LobKey(const typeLobId& newLobId, typeDba newPage);
        LobKey(const LobKey& other);
        virtual ~LobKey();

        bool operator<(const LobKey& other) const;
        bool operator!=(const LobKey& other) const;
        bool operator==(const LobKey& other) const;
        LobKey& operator=(const LobKey& other);

        typeLobId lobId;
        typeDba page;
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::LobKey> {
        size_t operator()(const OpenLogReplicator::LobKey& lobKey) const;
    };
}

#endif
