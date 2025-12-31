/* Struct to store schema element information.
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

#ifndef SCHEMA_ELEMENT_H_
#define SCHEMA_ELEMENT_H_

#include <cctype>
#include <locale>
#include <vector>

#include "../common/DbTable.h"
#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class SchemaElement final {
    public:
        enum class TAG_TYPE : unsigned char {
            NONE,
            ALL,
            PK,
            LIST
        };

        std::string condition;
        std::string key;
        std::string owner;
        std::string table;
        std::string tag;
        DbTable::OPTIONS options;
        TAG_TYPE tagType{TAG_TYPE::NONE};
        std::vector<std::string> keyList;
        std::vector<std::string> tagList;

        SchemaElement(std::string newOwner, std::string newTable, DbTable::OPTIONS newOptions):
            owner(std::move(newOwner)),
            table(std::move(newTable)),
            options(newOptions) {}

        void parseKey(std::string value, const std::string& separator) {
            size_t pos = 0;
            while ((pos = value.find(separator)) != std::string::npos) {
                const std::string val = value.substr(0, pos);
                keyList.push_back(val);
                value.erase(0, pos + separator.length());
            }
            keyList.push_back(value);
        }

        void parseTag(std::string value, const std::string& separator) {
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
                const std::string val = value.substr(0, pos);
                tagList.push_back(val);
                value.erase(0, pos + separator.length());
            }
            tagList.push_back(value);
        }
    };
}

#endif
