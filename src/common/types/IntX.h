/* Definition of type IntX
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

#ifndef INTX_H_
#define INTX_H_

#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>

namespace OpenLogReplicator {
    class IntX final {
    public:
        static constexpr uint LENGTH{2};
        static constexpr uint DIGITS{39};

    private:
        uint64_t data[LENGTH]{};
        static IntX BASE10[DIGITS][10];

    public:
        explicit IntX(uint64_t other) {
            data[0] = other;
            for (uint i = 1; i < LENGTH; ++i)
                data[i] = 0;
        }

        IntX(const IntX& other) {
            for (uint i = 0; i < LENGTH; ++i)
                data[i] = other.data[i];
        }

        IntX(uint64_t other1, uint64_t other2) {
            data[0] = other1;
            data[1] = other2;
            for (uint i = 2; i < LENGTH; ++i)
                data[i] = 0;
        }

        IntX() {
            for (uint64_t& i: data)
                i = 0;
        }

        static void initializeBASE10() {
            memset(BASE10, 0, sizeof(BASE10));
            for (uint digit = 0; digit < 10; ++digit) {
                BASE10[0][digit] = digit;

                for (uint pos = 1; pos < DIGITS; ++pos) {
                    BASE10[pos][digit] = BASE10[pos - 1][digit];
                    for (uint j = 1; j < 10; ++j)
                        BASE10[pos][digit] += BASE10[pos - 1][digit];
                }
            }
        }

        bool operator!=(const IntX& other) const {
            for (uint i = 0; i < LENGTH; ++i)
                if (this->data[i] != other.data[i])
                    return true;
            return false;
        }

        bool operator==(const IntX& other) const {
            for (uint i = 0; i < LENGTH; ++i)
                if (this->data[i] != other.data[i])
                    return false;
            return true;
        }

        IntX& operator+=(const IntX& other) {
            uint carry = 0;

            for (uint i = 0; i < LENGTH; ++i) {
                if (this->data[i] + other.data[i] + carry < (this->data[i] | other.data[i] | carry)) {
                    this->data[i] += other.data[i] + carry;
                    carry = 1;
                } else {
                    this->data[i] += other.data[i] + carry;
                    carry = 0;
                }
            }
            return *this;
        }

        IntX& operator=(const IntX& other) {
            if (&other != this) {
                for (uint i = 0; i < LENGTH; ++i)
                    this->data[i] = other.data[i];
            }
            return *this;
        }

        IntX& operator=(uint64_t other) {
            this->data[0] = other;
            for (uint i = 1; i < LENGTH; ++i)
                this->data[i] = 0;
            return *this;
        }

        IntX& setStr(const char* other, uint length, std::string& err) {
            *this = 0;
            if (length > DIGITS) {
                err = "incorrect conversion of string: " + std::string(other);
                return *this;
            }

            for (uint i = 0; i < length; ++i) {
                if (*other < '0' || *other > '9') {
                    err = "incorrect conversion of string: " + std::string(other);
                    return *this;
                }

                *this += BASE10[length - i - 1][*other - '0'];
                ++other;
            }
            return *this;
        }

        [[nodiscard]] uint64_t get64() const {
            return data[0];
        }

        [[nodiscard]] bool isSet64(uint64_t mask) const {
            return (data[0] & mask) != 0;
        }

        [[nodiscard]] bool isZero() const {
            for (uint64_t const i: data)
                if (i != 0)
                    return false;
            return true;
        }

        [[nodiscard]] std::string toString() const {
            std::ostringstream ss;
            ss << "[";
            for (uint i = 0; i < LENGTH; ++i) {
                if (i > 0)
                    ss << ",";
                ss << std::dec << data[i];
            }
            ss << "]";
            return ss.str();
        }
    };
}

#endif
