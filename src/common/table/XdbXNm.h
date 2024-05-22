/* Definition of schema XDB.X$NMxxx
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

#ifndef XDB_XNM_H_
#define XDB_XNM_H_

namespace OpenLogReplicator {
    class XdbXNm final {
    public:
        static constexpr uint64_t NMSPCURI_LENGTH = 2000;
        static constexpr uint64_t ID_LENGTH = 16;

        XdbXNm(typeRowId newRowId, const char* newNmSpcUri, const char* newId) :
                rowId(newRowId),
                nmSpcUri(newNmSpcUri),
                id(newId) {
        }

        bool operator!=(const XdbXNm& other) const {
            return (other.rowId != rowId) || (other.nmSpcUri != nmSpcUri) || (other.id != id);
        }

        typeRowId rowId;
        std::string nmSpcUri;
        std::string id;
    };
}

#endif
