/* Header for Serializer class
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

#ifndef SERIALIZER_H_
#define SERIALIZER_H_

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <unordered_map>
#include <vector>

#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class Metadata;

    class Serializer {
    public:
        Serializer();
        virtual ~Serializer() = default;

        [[nodiscard]] virtual bool deserialize(Metadata* metadata, const std::string& ss, const std::string& fileName, std::vector<std::string>& msgs,
                                               std::unordered_map<typeObj, std::string>& tablesUpdated, bool loadMetadata, bool storeSchema) = 0;
        virtual void serialize(Metadata* metadata, std::ostringstream& ss, bool noSchema) = 0;
    };
}

#endif
