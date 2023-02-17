/* Header for LOB context
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <map>
#include <unordered_map>

#include "LobData.h"
#include "LobKey.h"
#include "typeLobId.h"
#include "typeXid.h"

#ifndef LOBCTX_H_
#define LOBCTX_H_

namespace OpenLogReplicator {
    class Ctx;
    class Schema;

    class LobCtx {
    public:
        std::unordered_map<typeLobId, LobData*> lobs;
        std::map<LobKey, uint8_t*>* orphanedLobs;

        void checkOrphanedLobs(Ctx* ctx, typeLobId lobId, typeXid xid);
        void addLob(Ctx* ctx, typeLobId lobId, typeDba page, uint8_t* data, typeXid xid);
        void setLength(typeLobId lobId, uint32_t sizePages, uint16_t sizeRest);
        void setPage(typeLobId lobId, typeDba page, uint32_t pageNo, typeXid xid);
        void purge();
    };
}

#endif
