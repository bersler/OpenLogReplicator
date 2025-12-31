/* Token for parsing of expressions
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

#include "../exception/RuntimeException.h"
#include "BoolValue.h"
#include "Expression.h"
#include "StringValue.h"
#include "Token.h"

namespace OpenLogReplicator {
    void Expression::buildTokens(const std::string& condition, std::vector<Token*>& tokens) {
        auto expressionType = Token::TYPE::NONE;
        uint64_t tokenIndex = 0;

        uint64_t i = 0;
        while (i < condition.length()) {
            switch (expressionType) {
                case Token::TYPE::NONE:
                    if (condition[i] == ' ' || condition[i] == '\t' || condition[i] == '\n' || condition[i] == '\r') {
                        ++i;
                        continue;
                    }
                    if (condition[i] == '(') {
                        expressionType = Token::TYPE::LEFT_PARENTHESIS;
                        tokenIndex = i++;
                        continue;
                    }
                    if (condition[i] == ')') {
                        expressionType = Token::TYPE::RIGHT_PARENTHESIS;
                        tokenIndex = i++;
                        continue;
                    }
                    if (condition[i] == ',') {
                        expressionType = Token::TYPE::COMMA;
                        tokenIndex = i++;
                        continue;
                    }
                    if (condition[i] == '[') {
                        expressionType = Token::TYPE::IDENTIFIER;
                        tokenIndex = ++i;
                        continue;
                    }
                    if (condition[i] == '|' || condition[i] == '&' || condition[i] == '!' || condition[i] == '=') {
                        expressionType = Token::TYPE::OPERATOR;
                        tokenIndex = i++;
                        continue;
                    }
                    if ((condition[i] >= '0' && condition[i] <= '9') || condition[i] == '.') {
                        expressionType = Token::TYPE::NUMBER;
                        tokenIndex = i++;
                        continue;
                    }
                    if (condition[i] == '\'') {
                        expressionType = Token::TYPE::STRING;
                        tokenIndex = ++i;
                        continue;
                    }
                    throw RuntimeException(50067, "invalid condition: " + condition + " position: " + std::to_string(i));

                case Token::TYPE::IDENTIFIER:
                    if (condition[i] != ']') {
                        ++i;
                        continue;
                    }

                    // ends with ']'
                    tokens.push_back(new Token(expressionType, condition.substr(tokenIndex, i - tokenIndex)));
                    expressionType = Token::TYPE::NONE;
                    ++i;
                    continue;

                case Token::TYPE::LEFT_PARENTHESIS:
                case Token::TYPE::RIGHT_PARENTHESIS:
                case Token::TYPE::COMMA:
                    tokens.push_back(new Token(expressionType, condition.substr(tokenIndex, i - tokenIndex)));
                    expressionType = Token::TYPE::NONE;
                    continue;

                case Token::TYPE::OPERATOR:
                    if (tokenIndex + 1 == i && condition[tokenIndex] == '!') {
                        if (condition[i] == '=') {
                            ++i;
                            continue;
                        }
                    } else if (condition[i] == '|' || condition[i] == '&' || condition[i] == '!' || condition[i] == '=') {
                        ++i;
                        continue;
                    }

                    tokens.push_back(new Token(expressionType, condition.substr(tokenIndex, i - tokenIndex)));
                    expressionType = Token::TYPE::NONE;
                    continue;

                case Token::TYPE::STRING:
                    if (condition[i] != '\'') {
                        ++i;
                        continue;
                    }

                    // ends with apostrophe
                    tokens.push_back(new Token(expressionType, condition.substr(tokenIndex, i - tokenIndex)));
                    expressionType = Token::TYPE::NONE;
                    ++i;
                    continue;

                case Token::TYPE::NUMBER:
                    if ((condition[i] >= '0' && condition[i] <= '9') || condition[i] == '.' || condition[i] == 'e' || condition[i] == 'E') {
                        ++i;
                        continue;
                    }
                    if ((condition[i] >= 'a' && condition[i] <= 'z') || condition[i] == '_')
                        throw RuntimeException(50067, "invalid condition: " + condition + " number on position: " + std::to_string(i));

                    tokens.push_back(new Token(expressionType, condition.substr(tokenIndex, i - tokenIndex)));
                    expressionType = Token::TYPE::NONE;
                    continue;
            }
        }

        // Reached end and the token is not finished
        if (expressionType == Token::TYPE::STRING || expressionType == Token::TYPE::IDENTIFIER)
            throw RuntimeException(50067, "invalid condition: " + condition + " unfinished token: " + condition.substr(tokenIndex, i - tokenIndex));

        if (expressionType != Token::TYPE::NONE)
            tokens.push_back(new Token(expressionType, condition.substr(tokenIndex, i - tokenIndex)));
    }

    BoolValue* Expression::buildCondition(const std::string& condition, std::vector<Token*>& tokens, std::vector<Expression*>& stack) {
        uint64_t i = 0;
        while (stack.size() > 1 || i < tokens.size()) {
            if (stack.size() >= 2) {
                Expression* first = stack[stack.size() - 2];
                Expression* second = stack[stack.size() - 1];

                // TOKEN x
                if (first->isToken() && !second->isToken() && second->isBool()) {
                    const Token* firstToken = dynamic_cast<Token*>(first);

                    // ! x
                    if (firstToken->tokenType == Token::TYPE::OPERATOR && firstToken->stringValue == "!") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BoolValue::VALUE::OPERATOR_NOT, second, nullptr));
                        continue;
                    }
                }
            }

            if (stack.size() >= 3) {
                Expression* left = stack[stack.size() - 3];
                Expression* middle = stack[stack.size() - 2];
                Expression* right = stack[stack.size() - 1];

                // TOKEN x TOKEN
                if (left->isToken() && right->isToken()) {
                    const Token* leftToken = dynamic_cast<Token*>(left);
                    const Token* rightToken = dynamic_cast<Token*>(right);

                    // ( x )
                    if (leftToken->tokenType == Token::TYPE::LEFT_PARENTHESIS && rightToken->tokenType == Token::TYPE::RIGHT_PARENTHESIS) {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(middle);
                        continue;
                    }
                }

                // STRING TOKEN STRING
                if (left->isString() && middle->isToken() && right->isString()) {
                    const Token* middleToken = dynamic_cast<Token*>(middle);

                    // A == B
                    if (middleToken->stringValue == "==") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BoolValue::VALUE::OPERATOR_EQUAL, left, right));
                        continue;
                    }

                    // A != B
                    if (middleToken->stringValue == "!=") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BoolValue::VALUE::OPERATOR_NOT_EQUAL, left, right));
                        continue;
                    }
                }

                if (left->isBool() && middle->isToken() && right->isBool()) {
                    const Token* middleToken = dynamic_cast<Token*>(middle);

                    // A && B
                    if (middleToken->stringValue == "&&") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BoolValue::VALUE::OPERATOR_AND, left, right));
                        continue;
                    }

                    // A || B
                    if (middleToken->stringValue == "||") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BoolValue::VALUE::OPERATOR_OR, left, right));
                        continue;
                    }
                }
            }

            if (i < tokens.size()) {
                Token* token = tokens[i++];

                switch (token->tokenType) {
                    case Token::TYPE::NONE:
                        break;

                    case Token::TYPE::IDENTIFIER:
                        if (token->stringValue == "op")
                            stack.push_back(new StringValue(StringValue::TYPE::OP, token->stringValue));
                        else if (token->stringValue == "true")
                            stack.push_back(new BoolValue(BoolValue::VALUE::TRUE, nullptr, nullptr));
                        else if (token->stringValue == "false")
                            stack.push_back(new BoolValue(BoolValue::VALUE::FALSE, nullptr, nullptr));
                        else
                            stack.push_back(new StringValue(StringValue::TYPE::SESSION_ATTRIBUTE, token->stringValue));
                        continue;

                    case Token::TYPE::LEFT_PARENTHESIS:
                    case Token::TYPE::RIGHT_PARENTHESIS:
                    case Token::TYPE::COMMA:
                    case Token::TYPE::OPERATOR:
                        stack.push_back(token);
                        continue;

                    // Unsupported
                    case Token::TYPE::NUMBER:
                        break;

                    case Token::TYPE::STRING:
                        stack.push_back(new StringValue(StringValue::TYPE::VALUE, token->stringValue));
                        continue;
                }
            }

            throw RuntimeException(50067, "invalid condition: " + condition + " stack size: " + std::to_string(stack.size()));
        }

        BoolValue* root = nullptr;
        if (stack.size() == 1)
            root = dynamic_cast<BoolValue*>(stack[0]);

        for (Expression* expression: stack) {
            if (!expression->isToken() && expression != root)
                delete expression;
        }
        stack.clear();

        if (root == nullptr || !root->isBool())
            throw RuntimeException(50067, "invalid condition: " + condition + " is not evaluated to bool");

        for (const Token* token: tokens)
            delete token;
        tokens.clear();

        return root;
    }

    Expression::Expression() = default;
}
