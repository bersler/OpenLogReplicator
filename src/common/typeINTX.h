/* Header for type typeINTX
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

#define TYPEINTXLEN                             2
#define TYPEINTXDIGITS                          39

#ifndef TYPEINTX_T_H_
#define TYPEINTX_T_H_

namespace OpenLogReplicator {
    class typeINTX {
    private:
        uint64_t data[TYPEINTXLEN];
        static typeINTX BASE10[TYPEINTXDIGITS][10];
    public:
        explicit typeINTX(uint64_t val);
        typeINTX(uint64_t val1, uint64_t val2);
        typeINTX();

        static void initializeBASE10();

        bool operator!=(const typeINTX& other) const;
        bool operator==(const typeINTX& other) const;
        typeINTX& operator+=(const typeINTX& val);
        typeINTX& operator=(const typeINTX& val);
        typeINTX& operator=(uint64_t val);
        typeINTX& operator=(const std::string& val);
        typeINTX& operator=(const char* val);
        typeINTX& set(uint64_t val1, uint64_t val2);
        typeINTX& setStr(const char* val, uint64_t length);
        [[nodiscard]] uint64_t get64();
        [[nodiscard]] bool isSet64(uint64_t mask);
        [[nodiscard]] bool isZero();

        friend std::ostream& operator<<(std::ostream& os, const typeINTX& val);
    };
}

#endif
