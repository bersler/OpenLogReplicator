/* Token for parsing of expressions evaluation to string values
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

#include <utility>

#include "../exception/RuntimeException.h"
#include "StringValue.h"
#include "../Attribute.h"

namespace OpenLogReplicator {
    StringValue::StringValue(TYPE newStringType, std::string newStringValue):
            stringType(newStringType),
            stringValue(std::move(newStringValue)) {}

    bool StringValue::evaluateToBool(char op __attribute__((unused)), const AttributeMap* attributes __attribute__((unused))) {
        throw RuntimeException(50066, "invalid expression evaluation: string to bool");
    }

    std::string StringValue::evaluateToString(char op, const AttributeMap* attributes) {
        switch (stringType) {
            case TYPE::SESSION_ATTRIBUTE: {
                if (attributes == nullptr) return "";

                const auto enumIt = Attribute::fromString().find(stringValue);
                if (enumIt == Attribute::fromString().end())
                    return "";

                const auto attributesIt = attributes->find(enumIt->second);
                if (attributesIt == attributes->end())
                    return "";
                return attributesIt->second;
            }

            case TYPE::OP:
                return {op};

            case TYPE::VALUE:
                return stringValue;
        }

        throw RuntimeException(50066, "invalid expression evaluation: invalid string type");
    }
}
