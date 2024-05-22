/* Definition of schema XDB.X$QNxxx
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

#ifndef XDB_XQN_H_
#define XDB_XQN_H_

namespace OpenLogReplicator {
    class XdbXQn final {
    public:
        static constexpr uint64_t NMSPCID_LENGTH = 16;
        static constexpr uint64_t LOCALNAME_LENGTH = 2000;
        static constexpr uint64_t FLAGS_LENGTH = 8;
        static constexpr uint64_t ID_LENGTH = 16;
        static constexpr uint64_t FLAG_ISATTRIBUTE = 1;

        XdbXQn(typeRowId newRowId, const char* newNmSpcId, const char* newLocalName, const char* newFlags, const char* newId) :
                rowId(newRowId),
                nmSpcId(newNmSpcId),
                localName(newLocalName),
                flags(newFlags),
                id(newId) {
        }

        bool operator!=(const XdbXQn& other) const {
            return (other.rowId != rowId) || (other.nmSpcId != nmSpcId) || (other.localName != localName) || (other.flags != flags) || (other.id != id);
        }

        typeRowId rowId;
        std::string nmSpcId;
        std::string localName;
        std::string flags;
        std::string id;
    };
}

#endif
