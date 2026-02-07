/* Header for BoolValue class
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

#ifndef EXPRESSION_BOOL_H_
#define EXPRESSION_BOOL_H_

#include "Expression.h"

namespace OpenLogReplicator {
    class BoolValue final : public Expression {
    public:
        enum class VALUE : unsigned char {
            FALSE,
            TRUE,
            OPERATOR_AND,
            OPERATOR_OR,
            OPERATOR_NOT,
            OPERATOR_EQUAL,
            OPERATOR_NOT_EQUAL
        };

    protected:
        VALUE boolType;
        Expression* left;
        Expression* right;

    public:
        BoolValue(VALUE newBoolType, Expression* newLeft, Expression* newRight);
        ~BoolValue() override;

        bool isBool() override { return true; }

        bool evaluateToBool(char op, const AttributeMap* attributes) override;
        std::string evaluateToString(char op, const AttributeMap* attributes) override;
    };
}

#endif
