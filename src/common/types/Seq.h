/* Definition of type Seq
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

#ifndef SEQ_H_
#define SEQ_H_

#include <iomanip>
#include <ostream>

namespace OpenLogReplicator {
    class Seq final {
        uint32_t data{0};
        static constexpr uint32_t NONE{0xFFFFFFFF};

    public:
        Seq() = default;

        static Seq none() {
            return Seq(NONE);
        }

        static Seq zero() {
            return Seq{};
        }

        explicit Seq(uint32_t newData): data(newData) {}

        ~Seq() = default;

        [[nodiscard]] std::string toString() const {
            return std::to_string(data);
        }

        [[nodiscard]] std::string toStringHex(int width) const {
            std::stringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(width) << std::hex << data;
            return ss.str();
        }

        [[nodiscard]] uint32_t getData() const {
            return this->data;
        }

        Seq operator++() {
            ++data;
            return *this;
        }

        bool operator==(const Seq other) const {
            return data == other.data;
        }

        bool operator!=(const Seq other) const {
            return data != other.data;
        }

        bool operator<(const Seq other) const {
            return data < other.data;
        }

        bool operator<=(const Seq other) const {
            return data <= other.data;
        }

        bool operator>(const Seq other) const {
            return data > other.data;
        }

        bool operator>=(const Seq other) const {
            return data >= other.data;
        }

        Seq& operator=(uint32_t newData) {
            data = newData;
            return *this;
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::Seq> {
    size_t operator()(const OpenLogReplicator::Seq seq) const noexcept {
        return hash<uint32_t>()(seq.getData());
    }
};

#endif
