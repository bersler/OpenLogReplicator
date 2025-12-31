/* Definition of type Scn
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

#ifndef SCN_H_
#define SCN_H_

#include <iomanip>
#include <ostream>

namespace OpenLogReplicator {
    class Scn final {
        uint64_t data{0};
        static constexpr uint64_t NONE{0xFFFFFFFFFFFFFFFF};

    public:
        Scn() = default;

        static Scn none() {
            return Scn(NONE);
        }

        static Scn zero() {
            return Scn{};
        }

        explicit Scn(uint64_t newData): data(newData) {}

        explicit Scn(uint8_t newByte0, uint8_t newByte1, uint8_t newByte2, uint8_t newByte3, uint8_t newByte4, uint8_t newByte5, uint8_t newByte6,
                     uint8_t newByte7):
                data(static_cast<uint64_t>(newByte0) |
                        (static_cast<uint64_t>(newByte1) << 8) |
                        (static_cast<uint64_t>(newByte2) << 16) |
                        (static_cast<uint64_t>(newByte3) << 24) |
                        (static_cast<uint64_t>(newByte4) << 32) |
                        (static_cast<uint64_t>(newByte5) << 40) |
                        (static_cast<uint64_t>(newByte6) << 48) |
                        (static_cast<uint64_t>(newByte7) << 56)) {}

        explicit Scn(uint8_t newByte0, uint8_t newByte1, uint8_t newByte2, uint8_t newByte3, uint8_t newByte4, uint8_t newByte5):
                data(static_cast<uint64_t>(newByte0) |
                        (static_cast<uint64_t>(newByte1) << 8) |
                        (static_cast<uint64_t>(newByte2) << 16) |
                        (static_cast<uint64_t>(newByte3) << 24) |
                        (static_cast<uint64_t>(newByte4) << 32) |
                        (static_cast<uint64_t>(newByte5) << 40)) {}

        explicit Scn(uint32_t newData0, uint32_t newData1):
                data((static_cast<uint64_t>(newData0) << 32) | newData1) {}

        ~Scn() = default;

        [[nodiscard]] std::string to48() const {
            std::stringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(4) << std::hex << (static_cast<uint32_t>(data >> 32) & 0xFFFF) <<
                    "." << std::setw(8) << (data & 0xFFFFFFFF);
            return ss.str();
        }

        [[nodiscard]] std::string to64() const {
            std::stringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(16) << std::hex << data;
            return ss.str();
        }

        [[nodiscard]] std::string to64D() const {
            std::stringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(4) << std::hex << (static_cast<uint32_t>(data >> 48) & 0xFFFF) <<
                    "." << std::setw(4) << (static_cast<uint32_t>(data >> 32) & 0xFFFF) << "." << std::setw(8) << (data & 0xFFFFFFFF);
            return ss.str();
        }

        [[nodiscard]] std::string toStringHex12() const {
            std::stringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(12) << std::hex << data;
            return ss.str();
        }

        [[nodiscard]] std::string toStringHex16() const {
            std::stringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(16) << std::hex << (data & 0xFFFF7FFFFFFFFFFF);
            return ss.str();
        }

        [[nodiscard]] std::string toString() const {
            return std::to_string(data);
        }

        [[nodiscard]] uint64_t getData() const {
            return this->data;
        }

        bool operator==(const Scn other) const {
            return data == other.data;
        }

        bool operator!=(const Scn other) const {
            return data != other.data;
        }

        bool operator<(const Scn other) const {
            return data < other.data;
        }

        bool operator<=(const Scn other) const {
            return data <= other.data;
        }

        bool operator>(const Scn other) const {
            return data > other.data;
        }

        bool operator>=(const Scn other) const {
            return data >= other.data;
        }

        Scn& operator=(uint64_t newData) {
            data = newData;
            return *this;
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::Scn> {
    size_t operator()(const OpenLogReplicator::Scn scn) const noexcept {
        return hash<uint64_t>()(scn.getData());
    }
};

#endif
