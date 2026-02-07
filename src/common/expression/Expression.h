/* Header for Expression class
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

#ifndef EXPRESSION_H_
#define EXPRESSION_H_

#include <unordered_map>
#include <vector>

#include "../types/Types.h"
#include "../Attribute.h"

namespace OpenLogReplicator {
    class BoolValue;
    class Token;

    class Expression {
    public:
        static void buildTokens(const std::string& condition, std::vector<Token*>& tokens);
        static BoolValue* buildCondition(const std::string& condition, std::vector<Token*>& tokens, std::vector<Expression*>& stack);

        Expression();
        virtual ~Expression() = default;

        virtual bool isBool() { return false; }

        virtual bool isString() { return false; }

        virtual bool isToken() { return false; }

        virtual bool evaluateToBool(char op, const AttributeMap* attributes) = 0;
        virtual std::string evaluateToString(char op, const AttributeMap* attributes) = 0;
    };
}

#endif
