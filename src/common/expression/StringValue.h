/* Header for ExpressionString class
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

#include "Expression.h"

#ifndef STRING_VALUE_H_
#define STRING_VALUE_H_

namespace OpenLogReplicator {
    class StringValue : public Expression {
    public:
        uint64_t stringType;
        std::string stringValue;

        static constexpr uint64_t SESSION_ATTRIBUTE = 0;
        static constexpr uint64_t OP = 1;
        static constexpr uint64_t VALUE = 2;

    public:
        StringValue(uint64_t newStringType, const std::string& newStringValue);
        virtual ~StringValue();

        virtual bool isString() override { return true; }

        virtual bool evaluateToBool(char op, const std::unordered_map<std::string, std::string>* attributes) override;
        virtual std::string evaluateToString(char op, const std::unordered_map<std::string, std::string>* attributes) override;
    };
}

#endif
