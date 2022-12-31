/* Header for Serializer class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>

#include "../common/types.h"

#ifndef SERIALIZER_H_
#define SERIALIZER_H_

namespace OpenLogReplicator {
    class Metadata;

    class Serializer {
    public:
        Serializer();
        virtual ~Serializer();

        [[nodiscard]] virtual bool deserialize(Metadata* metadata, const std::string& ss, const std::string& name, std::set<std::string>& msgs,
                                               bool loadMetadata, bool storeSchema) = 0;
        virtual void serialize(Metadata* metadata, std::ostringstream& ss, bool noSchema) = 0;
    };
}

#endif
