/* Token for parsing of expressions evaluation to boolean values
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

#include "../Ctx.h"
#include "../exception/RuntimeException.h"
#include "BoolValue.h"
#include "StringValue.h"

namespace OpenLogReplicator {
    BoolValue::BoolValue(uint64_t newBoolType, Expression* newLeft, Expression* newRight) :
            Expression(),
            boolType(newBoolType),
            left(newLeft),
            right(newRight) {
    }

    BoolValue::~BoolValue() {
        if (left != nullptr) {
            delete left;
            left = nullptr;
        }

        if (right != nullptr) {
            delete right;
            right = nullptr;
        }
    }

    bool BoolValue::evaluateToBool(char op, const std::unordered_map<std::string, std::string>* attributes) {
        switch (boolType) {
            case VALUE_FALSE:
                return false;

            case VALUE_TRUE:
                return true;

            case OPERATOR_AND:
                if (!left->evaluateToBool(op, attributes))
                    return false;
                return right->evaluateToBool(op, attributes);

            case OPERATOR_OR:
                if (left->evaluateToBool(op, attributes))
                    return true;
                return right->evaluateToBool(op, attributes);

            case OPERATOR_NOT:
                return !left->evaluateToBool(op, attributes);

            case OPERATOR_EQUAL:
                return (left->evaluateToString(op, attributes) == right->evaluateToString(op, attributes));

            case OPERATOR_NOT_EQUAL:
                return (left->evaluateToString(op, attributes) != right->evaluateToString(op, attributes));
        }
        throw RuntimeException(50066, "invalid expression evaluation: invalid bool type");
    }

    std::string BoolValue::evaluateToString(char op __attribute__((unused)), const std::unordered_map<std::string,
            std::string>* attributes __attribute__((unused))) {
        throw RuntimeException(50066, "invalid expression evaluation: bool to string");
    }
}
