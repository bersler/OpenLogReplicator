/* Header for type typeIntX
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

#include <cstdint>
#include <string>

#include "DataException.h"

#ifndef TYPE_INTX_T_H_
#define TYPE_INTX_T_H_

#define TYPE_INTX_LENGTH                        2
#define TYPE_INTX_DIGITS                        39

namespace OpenLogReplicator {
    class typeIntX {
    private:
        uint64_t data[TYPE_INTX_LENGTH];
        static typeIntX BASE10[TYPE_INTX_DIGITS][10];
    public:
        explicit typeIntX(uint64_t other);
        typeIntX(uint64_t other1, uint64_t other2);
        typeIntX();

        static void initializeBASE10();

        bool operator!=(const typeIntX& other) const;
        bool operator==(const typeIntX& other) const;
        typeIntX& operator+=(const typeIntX& other);
        typeIntX& operator=(const typeIntX& other);
        typeIntX& operator=(uint64_t other);
        typeIntX& operator=(const std::string& other);
        typeIntX& operator=(const char* other);
        typeIntX& set(uint64_t other1, uint64_t other2);
        typeIntX& setStr(const char* other, uint64_t length);
        [[nodiscard]] uint64_t get64() const;
        [[nodiscard]] bool isSet64(uint64_t mask) const;
        [[nodiscard]] bool isZero() const;

        friend std::ostream& operator<<(std::ostream& os, const typeIntX& other);
    };
}

#endif
