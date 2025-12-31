/* Definition of type LobId
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

#ifndef LOBID_H_
#define LOBID_H_

#include <cstring>
#include <iostream>
#include <string>

#include "Types.h"

namespace OpenLogReplicator {
    class LobId final {
    public:
        static constexpr uint64_t LENGTH{10};

        uint8_t data[LENGTH]{};

        LobId() {
            memset(data, 0, LENGTH);
        }

        LobId(const LobId& other) {
            memcpy(data, other.data, LENGTH);
        }

        explicit LobId(const uint8_t* newData) {
            memcpy(data, newData, LENGTH);
        }

        bool operator!=(const LobId& other) const {
            const int ret = memcmp(data, other.data, LENGTH);
            return ret != 0;
        }

        bool operator==(const LobId& other) const {
            const int ret = memcmp(data, other.data, LENGTH);
            return ret == 0;
        }

        bool operator<(const LobId& other) const {
            const int ret = memcmp(data, other.data, LENGTH);
            return ret < 0;
        }

        LobId& operator=(const LobId& other) {
            if (&other != this)
                memcpy(data, other.data, LENGTH);
            return *this;
        }

        void set(const uint8_t* newData) {
            memcpy(data, newData, LENGTH);
        }

        [[nodiscard]] std::string lower() const {
            std::ostringstream ss;
            ss << std::setfill('0') << std::hex <<
                    std::setw(2) << static_cast<uint>(data[0]) << std::setw(2) << static_cast<uint>(data[1]) <<
                    std::setw(2) << static_cast<uint>(data[2]) << std::setw(2) << static_cast<uint>(data[3]) <<
                    std::setw(2) << static_cast<uint>(data[4]) << std::setw(2) << static_cast<uint>(data[5]) <<
                    std::setw(2) << static_cast<uint>(data[6]) << std::setw(2) << static_cast<uint>(data[7]) <<
                    std::setw(2) << static_cast<uint>(data[8]) << std::setw(2) << static_cast<uint>(data[9]);
            return ss.str();
        }

        [[nodiscard]] std::string upper() const {
            std::ostringstream ss;
            ss << std::uppercase << std::setfill('0') << std::hex <<
                    std::setw(2) << static_cast<uint>(data[0]) << std::setw(2) << static_cast<uint>(data[1]) <<
                    std::setw(2) << static_cast<uint>(data[2]) << std::setw(2) << static_cast<uint>(data[3]) <<
                    std::setw(2) << static_cast<uint>(data[4]) << std::setw(2) << static_cast<uint>(data[5]) <<
                    std::setw(2) << static_cast<uint>(data[6]) << std::setw(2) << static_cast<uint>(data[7]) <<
                    std::setw(2) << static_cast<uint>(data[8]) << std::setw(2) << static_cast<uint>(data[9]) << std::nouppercase;
            return ss.str();
        }

        [[nodiscard]] std::string narrow() const {
            std::ostringstream ss;
            ss << std::uppercase << std::setfill('0') << std::hex << static_cast<uint>(data[0]) << static_cast<uint>(data[1]) <<
                    static_cast<uint64_t>(data[2]) << static_cast<uint>(data[3]) <<
                    static_cast<uint64_t>(data[4]) << static_cast<uint>(data[5]) <<
                    static_cast<uint64_t>(data[6]) << static_cast<uint>(data[7]) <<
                    static_cast<uint64_t>(data[8]) << static_cast<uint>(data[9]) << std::nouppercase;
            return ss.str();
        }

        friend std::ostream& operator<<(std::ostream& os, const LobId& other) {
            os << std::uppercase << std::setfill('0') << std::hex <<
                    std::setw(2) << static_cast<uint>(other.data[0]) <<
                    std::setw(2) << static_cast<uint>(other.data[1]) <<
                    std::setw(2) << static_cast<uint>(other.data[2]) <<
                    std::setw(2) << static_cast<uint>(other.data[3]) <<
                    std::setw(2) << static_cast<uint>(other.data[4]) <<
                    std::setw(2) << static_cast<uint>(other.data[5]) <<
                    std::setw(2) << static_cast<uint>(other.data[6]) <<
                    std::setw(2) << static_cast<uint>(other.data[7]) <<
                    std::setw(2) << static_cast<uint>(other.data[8]) <<
                    std::setw(2) << static_cast<uint>(other.data[9]) << std::nouppercase;
            return os;
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::LobId> {
    size_t operator()(const OpenLogReplicator::LobId& lobId) const noexcept {
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

#endif
