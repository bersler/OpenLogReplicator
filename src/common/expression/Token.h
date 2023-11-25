/* Header for ExpressionToken class
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

#include <unordered_map>
#include <vector>

#include "Expression.h"
#include "../types.h"

#ifndef TOKEN_H_
#define TOKEN_H_

#define TOKEN_TYPE_NONE                 0
#define TOKEN_TYPE_IDENTIFIER           1
#define TOKEN_TYPE_LEFT_PARENTHESIS     2
#define TOKEN_TYPE_RIGHT_PARENTHESIS    3
#define TOKEN_TYPE_COMMA                4
#define TOKEN_TYPE_OPERATOR             5
#define TOKEN_TYPE_NUMBER               6
#define TOKEN_TYPE_STRING               7

namespace OpenLogReplicator {
    class Token : public Expression {
    public:
        uint64_t tokenType;
        std::string stringValue;

        Token(uint64_t newTokenType, const std::string& newStringValue);
        virtual ~Token();

        virtual bool isToken() { return true; }
        virtual bool evaluateToBool(char op, const std::unordered_map<std::string, std::string>* attributes);
        virtual std::string evaluateToString(char op, const std::unordered_map<std::string, std::string>* attributes);
    };
}

#endif
