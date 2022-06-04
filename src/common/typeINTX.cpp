/* Definition of type uint256_t
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
#include "typeINTX.h"

namespace OpenLogReplicator {
    typeINTX typeINTX::BASE10[TYPEINTXDIGITS][10];

    typeINTX::typeINTX() {
        for (uint64_t i = 0; i < TYPEINTXLEN; ++i)
            data[i] = 0;
    }

    typeINTX::typeINTX(uint64_t val) {
        data[0] = val;
        for (uint64_t i = 1; i < TYPEINTXLEN; ++i)
            data[i] = 0;
    }

    typeINTX::typeINTX(uint64_t val1, uint64_t val2) {
        data[0] = val1;
        data[1] = val2;
        for (uint64_t i = 2; i < TYPEINTXLEN; ++i)
            data[i] = 0;
    }

    void typeINTX::initializeBASE10() {
        memset(BASE10, 0, sizeof(BASE10));
        for (uint64_t digit = 0; digit < 10; ++digit) {
            BASE10[0][digit] = digit;

            for (uint64_t pos = 1; pos < TYPEINTXDIGITS; ++pos) {
                BASE10[pos][digit] = BASE10[pos - 1][digit];
                for (uint64_t j = 1; j < 10; ++j)
                    BASE10[pos][digit] += BASE10[pos - 1][digit];
            }
        }
    }

    bool typeINTX::operator!=(const typeINTX& other) const {
        for (uint64_t i = 0; i < TYPEINTXLEN; ++i)
            if (this->data[i] != other.data[i])
                return true;
        return false;
    }

    bool typeINTX::operator==(const typeINTX& other) const {
        for (uint64_t i = 0; i < TYPEINTXLEN; ++i)
            if (this->data[i] != other.data[i])
                return false;
        return true;
    }

    typeINTX& typeINTX::operator+=(const typeINTX& val) {
        uint64_t carry = 0;

        for (uint64_t i = 0; i < TYPEINTXLEN; ++i) {
            if (this->data[i] + val.data[i] + carry < (this->data[i] | val.data[i] | carry)) {
                this->data[i] += val.data[i] + carry;
                carry = 1;
            } else {
                this->data[i] += val.data[i] + carry;
                carry = 0;
            }
        }
        return *this;
    }

    typeINTX& typeINTX::operator=(const typeINTX& val) {
        if (&val != this) {
            for (uint64_t i = 0; i < TYPEINTXLEN; ++i)
                this->data[i] = val.data[i];
        }
        return *this;
    }

    typeINTX& typeINTX::operator=(uint64_t val) {
        this->data[0] = val;
        for (uint64_t i = 1; i < TYPEINTXLEN; ++i)
            this->data[i] = 0;
        return *this;
    }

    typeINTX& typeINTX::operator=(const std::string& val) {
        setStr(val.c_str(), val.length());
        return *this;
    }

    typeINTX& typeINTX::operator=(const char* val) {
        setStr(val, strlen(val));
        return *this;
    }

    typeINTX& typeINTX::setStr(const char* val, uint64_t length) {
        *this = (uint64_t)0;
        if (length > TYPEINTXDIGITS) {
            ERROR("incorrect conversion of string: " << val)
            return *this;
        }

        for (uint64_t i = 0; i < length; ++i) {
            if (*val < '0' || *val > '9') {
                ERROR("incorrect conversion of string: " << val)
                return *this;
            }

            *this += BASE10[length - i - 1][*val - '0'];
            ++val;
        }
        return *this;
    }

    uint64_t typeINTX::get64() {
        return data[0];
    }

    bool typeINTX::isSet64(uint64_t mask) {
        return data[0] & mask;
    }

    bool typeINTX::isZero() {
        for (uint64_t i = 0; i < TYPEINTXLEN; ++i)
            if (data[i] != 0)
                return false;
        return true;
    }

    typeINTX& typeINTX::set(uint64_t val1, uint64_t val2) {
        this->data[0] = val1;
        this->data[1] = val2;
        for (uint64_t i = 2; i < TYPEINTXLEN; ++i)
            this->data[i] = 0;
        return *this;
    }

    std::ostream& operator<<(std::ostream& os, const typeINTX& val) {
        os << "[";
        for (uint64_t i = 0; i < TYPEINTXLEN; ++i) {
            if (i > 0)
                os << ",";
            os << std::dec << val.data[i];
        }
        os << "]";
        return os;
    }
}
