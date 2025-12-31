/* Header for XmlCtx class
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

#ifndef XML_CTX_H_
#define XML_CTX_H_

#include <map>
#include <unordered_map>

#include "../common/Ctx.h"
#include "../common/table/TablePack.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXQn.h"
#include "../common/table/XdbXPt.h"
#include "../common/types/RowId.h"
#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class XmlCtx final {
    public:
        TablePack<XdbXNm, TabRowIdKeyDefault, XdbXNmKey> xdbXNmPack;
        TablePack<XdbXQn, TabRowIdKeyDefault, XdbXQnKey> xdbXQnPack;
        TablePack<XdbXPt, TabRowIdKeyDefault, XdbXPtKey> xdbXPtPack;

        Ctx* ctx;
        std::string tokSuf;
        uint64_t flags;

        XmlCtx(Ctx* newCtx, std::string newTokSuf, uint64_t newFlags);
        ~XmlCtx();

        void purgeDicts() noexcept;
    };
}
#endif
