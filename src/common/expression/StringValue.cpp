/* Token for parsing of expressions evaluation to string values
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

#include "../exception/RuntimeException.h"
#include "StringValue.h"

namespace OpenLogReplicator {
    StringValue::StringValue(uint64_t newStringType, const std::string& newStringValue) :
            Expression(),
            stringType(newStringType),
            stringValue(newStringValue) {
    }

    StringValue::~StringValue() {
    }

    bool StringValue::evaluateToBool(char op __attribute__((unused)), const std::unordered_map<std::string, std::string>* attributes __attribute__((unused))) {
        throw RuntimeException(50066, "invalid expression evaluation: string to bool");
    }

    std::string StringValue::evaluateToString(char op, const std::unordered_map<std::string, std::string>* attributes) {
        switch (stringType) {
            case SESSION_ATTRIBUTE: {
                auto attributesIt = attributes->find(stringValue);
                if (attributesIt == attributes->end())
                    return "";
                return attributesIt->second;
            }

            case OP:
                return std::string(1, op);

            case VALUE:
                return stringValue;
        }

        throw RuntimeException(50066, "invalid expression evaluation: invalid string type");
    }
}
