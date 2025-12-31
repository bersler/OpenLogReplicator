/* Header for Locales class
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

#ifndef LOCALES_H_
#define LOCALES_H_

#include <unordered_map>

#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class CharacterSet;

    class Locales final {
    public:
        std::unordered_map<uint64_t, CharacterSet*> characterMap;
        std::unordered_map<uint16_t, std::string_view> timeZoneMap;

        Locales();
        ~Locales();

        void initialize();
    };
}

#endif
