/* Definition of type uint256_t
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "types.h"

namespace OpenLogReplicator {

    uintX_t uintX_t::BASE10[TYPEINTXLENDIGITS][10];

    void uintX_t::initializeBASE10(void) {
        memset(BASE10, 0, sizeof(BASE10));
        for (uint64_t digit = 0; digit < 10; ++digit) {
            BASE10[0][digit] = digit;

            for (uint64_t pos = 1; pos < TYPEINTXLENDIGITS; ++pos) {
                BASE10[pos][digit] = BASE10[pos - 1][digit];
                for (uint64_t j = 1; j < 10; ++j)
                    BASE10[pos][digit] += BASE10[pos - 1][digit];
            }
        }
    }

    uintX_t& uintX_t::operator+=(const uintX_t &val) {
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

    uintX_t& uintX_t::operator=(const uintX_t &val) {
        if (&val != this) {
            for (uint64_t i = 0; i < TYPEINTXLEN; ++i)
                this->data[i] = val.data[i];
        }
        return *this;
    }

    uintX_t& uintX_t::operator=(uint64_t val) {
        this->data[0] = val;
        for (uint64_t i = 1; i < TYPEINTXLEN; ++i)
            this->data[i] = 0;
        return *this;
    }

    uintX_t& uintX_t::operator=(const string &val) {
        return setStr(val.c_str(), val.length());
    }

    uintX_t& uintX_t::operator=(const char *val) {
        return setStr(val, strlen(val));
    }

    uintX_t& uintX_t::setStr(const char *val, uint64_t length) {
        *this = (uint64_t)0;
        if (length > TYPEINTXLENDIGITS) {
            ERROR("incorrect conversion of string: " << val);
            return *this;
        }

        for (uint64_t i = 0; i < length; ++i) {
            if (*val < '0' || *val > '9') {
                ERROR("incorrect conversion of string: " << val);
                return *this;
            }

            *this += BASE10[length - i - 1][*val - '0'];
            ++val;
        }
        return *this;
    }

    uintX_t& uintX_t::set(uint64_t val1, uint64_t val2) {
        this->data[0] = val1;
        this->data[1] = val2;
        for (uint64_t i = 2; i < TYPEINTXLEN; ++i)
            this->data[i] = 0;
        return *this;
    }

    ostream& operator<<(ostream& os, const uintX_t& val) {
        os << "[";
        for (uint64_t i = 0; i < TYPEINTXLEN; ++i) {
            if (i > 0)
                os << ",";
            os << dec << val.data[i];
        }
        os << "]";
        return os;
    }
}
