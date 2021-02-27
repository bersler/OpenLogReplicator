/* Header for RowId class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <functional>
#include "types.h"

#ifndef ROWID_H_
#define ROWID_H_

using namespace std;

namespace OpenLogReplicator {
    class RowId {
    public:
        static const char map64[65];
        static const char map64R[256];

        typeDATAOBJ dataObj;
        typeDBA dba;
        typeSLOT slot;

        RowId();
        RowId(const char *rowid);
        RowId(typeDATAOBJ dataObj, typeDBA dba, typeSLOT slot);
        bool operator<(const RowId& other) const;
        bool operator!=(const RowId& other) const;
        bool operator==(const RowId& other) const;
        void toString(char *str) const;
        friend ostream& operator<<(ostream& os, const RowId& tran);
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::RowId> {
        size_t operator()(const OpenLogReplicator::RowId &rowId) const;
    };
}

#endif
