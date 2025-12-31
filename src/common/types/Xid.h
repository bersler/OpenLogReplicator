/* Definition of type Xid
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

#ifndef XID_H_
#define XID_H_

#include <cstring>
#include <iomanip>
#include <ostream>

#include "Types.h"
#include "../exception/DataException.h"

namespace OpenLogReplicator {
    class Xid final {
        uint64_t data;

    public:
        Xid(): data(0) {}

        explicit Xid(uint64_t newData): data(newData) {}

        Xid(typeUsn usn, typeSlt slt, typeSqn sqn) {
            data = (static_cast<uint64_t>(usn) << 48) | (static_cast<uint64_t>(slt) << 32) | static_cast<uint64_t>(sqn);
        }

        explicit Xid(const std::string& str) {
            std::string usn_;
            std::string slt_;
            std::string sqn_;

            // UUUUSSSSQQQQQQQQ
            if (str.length() == 16) {
                for (uint64_t i = 0; i < 16; ++i)
                    if (unlikely(!isxdigit(str[i])))
                        throw DataException(20002, "bad XID value: " + str);
                usn_.assign(str.c_str(), 4);
                slt_.assign(str.c_str() + 4, 4);
                sqn_.assign(str.c_str() + 8, 8);
            } else if (str.length() == 17) {
                // UUUU.SSS.QQQQQQQQ
                for (uint i = 0; i < 17; ++i)
                    if (unlikely(!isxdigit(str[i]) && i != 4 && i != 8))
                        throw DataException(20002, "bad XID value: " + str);
                if (unlikely(str[4] != '.' || str[8] != '.'))
                    throw DataException(20002, "bad XID value: " + str);
                usn_.assign(str.c_str(), 4);
                slt_.assign(str.c_str() + 5, 3);
                sqn_.assign(str.c_str() + 9, 8);
            } else if (str.length() == 18) {
                // UUUU.SSSS.QQQQQQQQ
                for (uint64_t i = 0; i < 18; ++i)
                    if (unlikely(!isxdigit(str[i]) && i != 4 && i != 9))
                        throw DataException(20002, "bad XID value: " + str);
                if (unlikely(str[4] != '.' || str[9] != '.'))
                    throw DataException(20002, "bad XID value: " + str);
                usn_.assign(str.c_str(), 4);
                slt_.assign(str.c_str() + 5, 4);
                sqn_.assign(str.c_str() + 10, 8);
            } else if (str.length() == 19) {
                // 0xUUUU.SSS.QQQQQQQQ
                for (uint64_t i = 2; i < 19; ++i)
                    if (unlikely(!isxdigit(str[i]) && i != 6 && i != 10))
                        throw DataException(20002, "bad XID value: " + str);
                if (unlikely(str[0] != '0' || str[1] != 'x' || str[6] != '.' || str[10] != '.'))
                    throw DataException(20002, "bad XID value: " + str);
                usn_.assign(str.c_str() + 2, 4);
                slt_.assign(str.c_str() + 7, 3);
                sqn_.assign(str.c_str() + 11, 8);
            } else if (str.length() == 20) {
                // 0xUUUU.SSSS.QQQQQQQQ
                for (uint64_t i = 2; i < 20; ++i)
                    if (unlikely(!isxdigit(str[i]) && i != 6 && i != 11))
                        throw DataException(20002, "bad XID value: " + str);
                if (unlikely(str[0] != '0' || str[1] != 'x' || str[6] != '.' || str[11] != '.'))
                    throw DataException(20002, "bad XID value: " + str);
                usn_.assign(str.c_str() + 2, 4);
                slt_.assign(str.c_str() + 7, 4);
                sqn_.assign(str.c_str() + 12, 8);
            } else
                throw DataException(20002, "bad XID value: " + str);

            data = (static_cast<uint64_t>(stoul(usn_, nullptr, 16)) << 48) |
                    (static_cast<uint64_t>(stoul(slt_, nullptr, 16)) << 32) |
                    static_cast<uint64_t>(stoul(sqn_, nullptr, 16));
        }

        static Xid zero() {
            return Xid(0);
        }

        [[nodiscard]] uint64_t getData() const {
            return data;
        }

        [[nodiscard]] bool isEmpty() const {
            return (data == 0);
        }

        [[nodiscard]] typeUsn usn() const {
            return static_cast<typeUsn>(data >> 48);
        }

        [[nodiscard]] typeSlt slt() const {
            return static_cast<typeSlt>((data >> 32) & 0xFFFF);
        }

        [[nodiscard]] typeSqn sqn() const {
            return static_cast<typeSqn>(data & 0xFFFFFFFF);
        }

        bool operator!=(Xid other) const {
            return data != other.data;
        }

        bool operator<(Xid other) const {
            return data < other.data;
        }

        bool operator==(Xid other) const {
            return data == other.data;
        }

        Xid& operator=(uint64_t newData) {
            data = newData;
            return *this;
        }

        [[nodiscard]] uint64_t toUint() const {
            return data;
        }

        [[nodiscard]] std::string toString() const {
            std::ostringstream ss;
            ss << "0x" << std::setfill('0') << std::setw(4) << std::hex << (data >> 48) << "." << std::setw(3) <<
                    ((data >> 32) & 0xFFFF) << "." << std::setw(8) << (data & 0xFFFFFFFF);
            return ss.str();
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::Xid> {
    size_t operator()(const OpenLogReplicator::Xid xid) const noexcept {
        return hash<size_t>()(xid.getData());
    }
};
#endif
