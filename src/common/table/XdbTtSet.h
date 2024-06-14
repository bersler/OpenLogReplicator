/* Definition of schema XDB.XDB$TTSET
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

#include "../types.h"
#include "../typeRowId.h"

#ifndef XDB_TTSET_H_
#define XDB_TTSET_H_

namespace OpenLogReplicator {
    class XdbTtSet final {
    public:
        static constexpr uint64_t GUID_LENGTH = 32;
        static constexpr uint64_t TOKSUF_LENGTH = 26;

        XdbTtSet(typeRowId newRowId, const char* newGuid, const char* newTokSuf, uint64_t newFlags, typeObj newObj) :
                rowId(newRowId),
                guid(newGuid),
                tokSuf(newTokSuf),
                flags(newFlags),
                obj(newObj) {
        }

        bool operator!=(const XdbTtSet& other) const {
            return (other.rowId != rowId) || (other.guid != guid) || (other.tokSuf != tokSuf) || (other.flags != flags) || (other.obj != obj);
        }

        typeRowId rowId;
        std::string guid;
        std::string tokSuf;
        uint64_t flags;
        typeObj obj;
    };
}

#endif
