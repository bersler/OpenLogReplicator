/* Header for typeRowId class
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

#include <functional>

#include "types.h"

#ifndef TYPE_ROWID_H_
#define TYPE_ROWID_H_

#define ROWID_LENGTH 18

namespace OpenLogReplicator {
    class typeRowId {
    protected:
        static const char map64[65];
        static const char map64R[256];

    public:
        typeDataObj dataObj;
        typeDba dba;
        typeSlot slot;

        typeRowId();
        explicit typeRowId(const char* rowid);
        typeRowId(typeDataObj newDataObj, typeDba newDba, typeSlot newSlot);

        bool operator<(const typeRowId& other) const;
        bool operator!=(const typeRowId& other) const;
        bool operator==(const typeRowId& other) const;
        void toString(char* str) const;
        std::string toString() const;

        friend std::ostream& operator<<(std::ostream& os, const typeRowId& other);
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::typeRowId> {
        size_t operator()(const OpenLogReplicator::typeRowId& other) const;
    };
}

#endif
