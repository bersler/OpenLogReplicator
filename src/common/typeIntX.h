/* Definition of type typeIntX
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

#ifndef TYPE_INTX_T_H_
#define TYPE_INTX_T_H_

namespace OpenLogReplicator {
    class typeIntX final {
    public:
        static constexpr uint64_t LENGTH = 2;
        static constexpr uint64_t DIGITS = 39;

    private:
        uint64_t data[LENGTH];
        static typeIntX BASE10[DIGITS][10];

    public:
        explicit typeIntX(uint64_t other) {
            data[0] = other;
            for (uint64_t i = 1; i < LENGTH; ++i)
                data[i] = 0;
        }

        explicit typeIntX(const typeIntX& other) {
            for (uint64_t i = 0; i < LENGTH; ++i)
                data[i] = other.data[i];
        }

        typeIntX(uint64_t other1, uint64_t other2) {
            data[0] = other1;
            data[1] = other2;
            for (uint64_t i = 2; i < LENGTH; ++i)
                data[i] = 0;
        }

        typeIntX() {
            for (uint64_t i = 0; i < LENGTH; ++i)
                data[i] = 0;
        }

        ~typeIntX() {
        }

        static void initializeBASE10() {
            memset(reinterpret_cast<void*>(BASE10), 0, sizeof(BASE10));
            for (uint64_t digit = 0; digit < 10; ++digit) {
                BASE10[0][digit] = digit;

                for (uint64_t pos = 1; pos < DIGITS; ++pos) {
                    BASE10[pos][digit] = BASE10[pos - 1][digit];
                    for (uint64_t j = 1; j < 10; ++j)
                        BASE10[pos][digit] += BASE10[pos - 1][digit];
                }
            }
        }

        bool operator!=(const typeIntX& other) const {
            for (uint64_t i = 0; i < LENGTH; ++i)
                if (this->data[i] != other.data[i])
                    return true;
            return false;
        }

        bool operator==(const typeIntX& other) const {
            for (uint64_t i = 0; i < LENGTH; ++i)
                if (this->data[i] != other.data[i])
                    return false;
            return true;
        }

        typeIntX& operator+=(const typeIntX& other) {
            uint64_t carry = 0;

            for (uint64_t i = 0; i < LENGTH; ++i) {
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

        typeIntX& operator=(const typeIntX& other) {
            if (&other != this) {
                for (uint64_t i = 0; i < LENGTH; ++i)
                    this->data[i] = other.data[i];
            }
            return *this;
        }

        typeIntX& operator=(uint64_t other) {
            this->data[0] = other;
            for (uint64_t i = 1; i < LENGTH; ++i)
                this->data[i] = 0;
            return *this;
        }

        typeIntX& set(uint64_t other1, uint64_t other2) {
            this->data[0] = other1;
            this->data[1] = other2;
            for (uint64_t i = 2; i < LENGTH; ++i)
                this->data[i] = 0;
            return *this;
        }

        typeIntX& setStr(const char* other, uint64_t length, std::string& err) {
            *this = static_cast<uint64_t>(0);
            if (length > DIGITS) {
                err = "incorrect conversion of string: " + std::string(other);
                return *this;
            }

            for (uint64_t i = 0; i < length; ++i) {
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
            return data[0] & mask;
        }

        [[nodiscard]] bool isZero() const {
            for (uint64_t i = 0; i < LENGTH; ++i)
                if (data[i] != 0)
                    return false;
            return true;
        }

        std::string toString(void) const {
            std::ostringstream ss;
            ss << "[";
            for (uint64_t i = 0; i < LENGTH; ++i) {
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
