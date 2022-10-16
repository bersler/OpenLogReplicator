/* Header for typeLobId class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <cstdint>
#include <string>

#include "DataException.h"

#ifndef TYPE_LOBID_H_
#define TYPE_LOBID_H_

#define TYPE_LOBID_LENGTH                       10

namespace OpenLogReplicator {
    class typeLobId {
    public:
        uint8_t data[TYPE_LOBID_LENGTH];

        typeLobId();
        typeLobId(const typeLobId& other);

        bool operator!=(const typeLobId& other) const;
        bool operator==(const typeLobId& other) const;
        bool operator<(const typeLobId& other) const;
        typeLobId& operator=(const typeLobId& other);
        void set(const uint8_t* newData);
        std::string lower();
        std::string upper();
        std::string narrow();

        friend std::ostream& operator<<(std::ostream& os, const typeLobId& other);
    };
}

namespace std {
    template <>
    struct hash<OpenLogReplicator::typeLobId> {
        size_t operator()(const OpenLogReplicator::typeLobId& other) const;
    };
}

#endif
