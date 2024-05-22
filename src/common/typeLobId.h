/* Definition of type typeLobId
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

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

#include "types.h"

#ifndef TYPE_LOBID_H_
#define TYPE_LOBID_H_

namespace OpenLogReplicator {
    class typeLobId final {
    public:
        static constexpr uint64_t LENGTH = 10;

        uint8_t data[LENGTH];

        typeLobId() {
            memset(reinterpret_cast<void*>(data), 0, LENGTH);
        }

        ~typeLobId() {
        }

        typeLobId(const typeLobId& other) {
            memcpy(reinterpret_cast<void*>(data),
                   reinterpret_cast<const void*>(other.data), LENGTH);
        }

        explicit typeLobId(const uint8_t* newData) {
            memcpy(reinterpret_cast<void*>(data),
                   reinterpret_cast<const void*>(newData), LENGTH);
        }

        bool operator!=(const typeLobId& other) const {
            int ret = memcmp(reinterpret_cast<const void*>(data), reinterpret_cast<const void*>(other.data), LENGTH);
            return ret != 0;
        }

        bool operator==(const typeLobId& other) const {
            int ret = memcmp(reinterpret_cast<const void*>(data), reinterpret_cast<const void*>(other.data), LENGTH);
            return ret == 0;
        }

        bool operator<(const typeLobId& other) const {
            int ret = memcmp(reinterpret_cast<const void*>(data), reinterpret_cast<const void*>(other.data), LENGTH);
            return ret < 0;
        }

        typeLobId& operator=(const typeLobId& other) {
            if (&other != this)
                memcpy(reinterpret_cast<void*>(data),
                       reinterpret_cast<const void*>(other.data), LENGTH);
            return *this;
        }

        void set(const uint8_t* newData) {
            memcpy(reinterpret_cast<void*>(data),
                   reinterpret_cast<const void*>(newData), LENGTH);
        }

        std::string lower() const {
            std::ostringstream ss;
            ss << std::setfill('0') << std::hex <<
               std::setw(2) << static_cast<uint64_t>(data[0]) << std::setw(2) << static_cast<uint64_t>(data[1]) <<
               std::setw(2) << static_cast<uint64_t>(data[2]) << std::setw(2) << static_cast<uint64_t>(data[3]) <<
               std::setw(2) << static_cast<uint64_t>(data[4]) << std::setw(2) << static_cast<uint64_t>(data[5]) <<
               std::setw(2) << static_cast<uint64_t>(data[6]) << std::setw(2) << static_cast<uint64_t>(data[7]) <<
               std::setw(2) << static_cast<uint64_t>(data[8]) << std::setw(2) << static_cast<uint64_t>(data[9]);
            return ss.str();
        }

        std::string upper() const {
            std::ostringstream ss;
            ss << std::uppercase << std::setfill('0') << std::hex <<
               std::setw(2) << static_cast<uint64_t>(data[0]) << std::setw(2) << static_cast<uint64_t>(data[1]) <<
               std::setw(2) << static_cast<uint64_t>(data[2]) << std::setw(2) << static_cast<uint64_t>(data[3]) <<
               std::setw(2) << static_cast<uint64_t>(data[4]) << std::setw(2) << static_cast<uint64_t>(data[5]) <<
               std::setw(2) << static_cast<uint64_t>(data[6]) << std::setw(2) << static_cast<uint64_t>(data[7]) <<
               std::setw(2) << static_cast<uint64_t>(data[8]) << std::setw(2) << static_cast<uint64_t>(data[9]) << std::nouppercase;
            return ss.str();
        }

        std::string narrow() const {
            std::ostringstream ss;
            ss << std::uppercase << std::setfill('0') << std::hex << static_cast<uint64_t>(data[0]) << static_cast<uint64_t>(data[1]) <<
               static_cast<uint64_t>(data[2]) << static_cast<uint64_t>(data[3]) <<
               static_cast<uint64_t>(data[4]) << static_cast<uint64_t>(data[5]) <<
               static_cast<uint64_t>(data[6]) << static_cast<uint64_t>(data[7]) <<
               static_cast<uint64_t>(data[8]) << static_cast<uint64_t>(data[9]) << std::nouppercase;
            return ss.str();
        }

        friend std::ostream& operator<<(std::ostream& os, const typeLobId& other) {
            os << std::uppercase << std::setfill('0') << std::hex <<
               std::setw(2) << static_cast<uint64_t>(other.data[0]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[1]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[2]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[3]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[4]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[5]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[6]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[7]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[8]) <<
               std::setw(2) << static_cast<uint64_t>(other.data[9]) << std::nouppercase;
            return os;
        }
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::typeLobId> {
        size_t operator()(const OpenLogReplicator::typeLobId& lobId) const {
            return (static_cast<size_t>(lobId.data[9]) << 56) ^
                   (static_cast<size_t>(lobId.data[8]) << 50) ^
                   (static_cast<size_t>(lobId.data[7]) << 42) ^
                   (static_cast<size_t>(lobId.data[6]) << 36) ^
                   (static_cast<size_t>(lobId.data[5]) << 30) ^
                   (static_cast<size_t>(lobId.data[4]) << 24) ^
                   (static_cast<size_t>(lobId.data[3]) << 18) ^
                   (static_cast<size_t>(lobId.data[2]) << 12) ^
                   (static_cast<size_t>(lobId.data[1]) << 6) ^
                   (static_cast<size_t>(lobId.data[0]));
        }
    };
}

#endif
