/* Definition of type FileOffset
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

#ifndef FILE_OFFSET_H_
#define FILE_OFFSET_H_

#include <iomanip>
#include <ostream>

namespace OpenLogReplicator {
    class FileOffset final {
        uint64_t data{0};

    public:
        FileOffset() = default;

        static FileOffset zero() {
            return FileOffset{};
        }

        explicit FileOffset(uint64_t newData): data(newData) {}

        explicit FileOffset(uint32_t block, uint blockSize): data(static_cast<uint64_t>(block) * blockSize) {}

        ~FileOffset() = default;

        FileOffset operator+=(uint64_t offset) {
            data += offset;
            return *this;
        }

        FileOffset operator-(FileOffset other) const {
            return FileOffset(data - other.data);
        }

        FileOffset operator+(uint64_t offset) const {
            return FileOffset(data + offset);
        }

        [[nodiscard]] bool matchesBlockSize(uint blockSize) const {
            return (data & (blockSize - 1)) == 0;
        }

        void alignBlockSize(uint blockSize) {
            data &= blockSize - 1;
        }

        [[nodiscard]] uint32_t getBlock(uint blockSize) const {
            return static_cast<uint32_t>(data / blockSize);
        }

        [[nodiscard]] uint64_t getFileOffset() const {
            return data;
        }

        [[nodiscard]] bool isZero() const {
            return data == 0;
        }

        [[nodiscard]] std::string toString() const {
            return std::to_string(data);
        }

        [[nodiscard]] std::string toStringHex(int width) const {
            std::stringstream ss;
            ss << std::setfill('0') << std::setw(width) << std::hex << data;
            return ss.str();
        }

        [[nodiscard]] uint64_t getData() const {
            return this->data;
        }

        bool operator==(const FileOffset other) const {
            return data == other.data;
        }

        bool operator!=(const FileOffset other) const {
            return data != other.data;
        }

        bool operator<(const FileOffset other) const {
            return data < other.data;
        }

        bool operator<=(const FileOffset other) const {
            return data <= other.data;
        }

        bool operator>(const FileOffset other) const {
            return data > other.data;
        }

        bool operator>=(const FileOffset other) const {
            return data >= other.data;
        }

        FileOffset& operator=(uint64_t newData) {
            data = newData;
            return *this;
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::FileOffset> {
    size_t operator()(const OpenLogReplicator::FileOffset fileOffset) const noexcept {
        return hash<uint64_t>()(fileOffset.getData());
    }
};

#endif
