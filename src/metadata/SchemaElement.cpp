/* Base class for handling of schema schemaElements
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

#include <algorithm>
#include <functional>
#include <cctype>
#include <locale>

#include "SchemaElement.h"

namespace OpenLogReplicator {
    SchemaElement::SchemaElement(const char* newOwner, const char* newTable, typeOptions newOptions) :
            owner(newOwner),
            table(newTable),
            options(newOptions),
            tagType(TAG_TYPE::NONE) {
    }

    void SchemaElement::parseKey(std::string value, const std::string& separator) {
        size_t pos = 0;
        while ((pos = value.find(separator)) != std::string::npos) {
            std::string val = value.substr(0, pos);
            keyList.push_back(val);
            value.erase(0, pos + separator.length());
        }
        keyList.push_back(value);
    }

    void SchemaElement::parseTag(std::string value, const std::string& separator) {
        if (value == "[pk]") {
            tagType = TAG_TYPE::PK;
            return;
        }

        if (value == "[all]") {
            tagType = TAG_TYPE::ALL;
            return;
        }

        tagType = TAG_TYPE::LIST;
        size_t pos = 0;
        while ((pos = value.find(separator)) != std::string::npos) {
            std::string val = value.substr(0, pos);
            tagList.push_back(val);
            value.erase(0, pos + separator.length());
        }
        tagList.push_back(value);
    }
}
