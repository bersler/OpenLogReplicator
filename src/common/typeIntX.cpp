/* Definition of type typeIntX
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

#include <cstring>
#include <iostream>
#include <string>

#include "types.h"
#include "typeIntX.h"

namespace OpenLogReplicator {
    typeIntX typeIntX::BASE10[TYPE_INTX_DIGITS][10];

    typeIntX::typeIntX() {
        for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i)
            data[i] = 0;
    }

    typeIntX::typeIntX(uint64_t other) {
        data[0] = other;
        for (uint64_t i = 1; i < TYPE_INTX_LENGTH; ++i)
            data[i] = 0;
    }

    typeIntX::typeIntX(uint64_t other1, uint64_t other2) {
        data[0] = other1;
        data[1] = other2;
        for (uint64_t i = 2; i < TYPE_INTX_LENGTH; ++i)
            data[i] = 0;
    }

    void typeIntX::initializeBASE10() {
        memset(reinterpret_cast<void*>(BASE10), 0, sizeof(BASE10));
        for (uint64_t digit = 0; digit < 10; ++digit) {
            BASE10[0][digit] = digit;

            for (uint64_t pos = 1; pos < TYPE_INTX_DIGITS; ++pos) {
                BASE10[pos][digit] = BASE10[pos - 1][digit];
                for (uint64_t j = 1; j < 10; ++j)
                    BASE10[pos][digit] += BASE10[pos - 1][digit];
            }
        }
    }

    bool typeIntX::operator!=(const typeIntX& other) const {
        for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i)
            if (this->data[i] != other.data[i])
                return true;
        return false;
    }

    bool typeIntX::operator==(const typeIntX& other) const {
        for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i)
            if (this->data[i] != other.data[i])
                return false;
        return true;
    }

    typeIntX& typeIntX::operator+=(const typeIntX& other) {
        uint64_t carry = 0;

        for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i) {
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

    typeIntX& typeIntX::operator=(const typeIntX& other) {
        if (&other != this) {
            for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i)
                this->data[i] = other.data[i];
        }
        return *this;
    }

    typeIntX& typeIntX::operator=(uint64_t other) {
        this->data[0] = other;
        for (uint64_t i = 1; i < TYPE_INTX_LENGTH; ++i)
            this->data[i] = 0;
        return *this;
    }

    typeIntX& typeIntX::operator=(const std::string& other) {
        setStr(other.c_str(), other.length());
        return *this;
    }

    typeIntX& typeIntX::operator=(const char* other) {
        setStr(other, strlen(other));
        return *this;
    }

    typeIntX& typeIntX::setStr(const char* other, uint64_t length) {
        *this = static_cast<uint64_t>(0);
        if (length > TYPE_INTX_DIGITS) {
            ERROR("incorrect conversion of string: " << other)
            return *this;
        }

        for (uint64_t i = 0; i < length; ++i) {
            if (*other < '0' || *other > '9') {
                ERROR("incorrect conversion of string: " << other)
                return *this;
            }

            *this += BASE10[length - i - 1][*other - '0'];
            ++other;
        }
        return *this;
    }

    uint64_t typeIntX::get64() const {
        return data[0];
    }

    bool typeIntX::isSet64(uint64_t mask) const {
        return data[0] & mask;
    }

    bool typeIntX::isZero() const {
        for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i)
            if (data[i] != 0)
                return false;
        return true;
    }

    typeIntX& typeIntX::set(uint64_t other1, uint64_t other2) {
        this->data[0] = other1;
        this->data[1] = other2;
        for (uint64_t i = 2; i < TYPE_INTX_LENGTH; ++i)
            this->data[i] = 0;
        return *this;
    }

    std::ostream& operator<<(std::ostream& os, const typeIntX& other) {
        os << "[";
        for (uint64_t i = 0; i < TYPE_INTX_LENGTH; ++i) {
            if (i > 0)
                os << ",";
            os << std::dec << other.data[i];
        }
        os << "]";
        return os;
    }
}
