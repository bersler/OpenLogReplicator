/* Token for parsing of expressions
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

#include "../RuntimeException.h"
#include "BoolValue.h"
#include "Expression.h"
#include "StringValue.h"
#include "Token.h"

namespace OpenLogReplicator {
    void Expression::buildTokens(const std::string& conditionStr, std::vector<Token*>& tokens) {
        uint64_t expressionType = TOKEN_TYPE_NONE;
        uint64_t tokenIndex = 0;

        uint64_t i = 0;
        while (i < conditionStr.length()) {
            switch (expressionType) {
                case TOKEN_TYPE_NONE:
                    if (conditionStr[i] == ' ' || conditionStr[i] == '\t' || conditionStr[i] == '\n' || conditionStr[i] == '\r') {
                        ++i;
                        continue;
                    } else if (conditionStr[i] == '(') {
                        expressionType = TOKEN_TYPE_LEFT_PARENTHESIS;
                        tokenIndex = i++;
                        continue;
                    } else if (conditionStr[i] == ')') {
                        expressionType = TOKEN_TYPE_RIGHT_PARENTHESIS;
                        tokenIndex = i++;
                        continue;
                    } else if (conditionStr[i] == ',') {
                        expressionType = TOKEN_TYPE_COMMA;
                        tokenIndex = i++;
                        continue;
                    } else if (conditionStr[i] == '[') {
                        expressionType = TOKEN_TYPE_IDENTIFIER;
                        tokenIndex = ++i;
                        continue;
                    } else if (conditionStr[i] == '|' || conditionStr[i] == '&' || conditionStr[i] == '!' || conditionStr[i] == '=') {
                        expressionType = TOKEN_TYPE_OPERATOR;
                        tokenIndex = i++;
                        continue;
                    } else if ((conditionStr[i] >= '0' && conditionStr[i] <= '9') || conditionStr[i] == '.') {
                        expressionType = TOKEN_TYPE_NUMBER;
                        tokenIndex = i++;
                        continue;
                    } else if (conditionStr[i] == '\'') {
                        expressionType = TOKEN_TYPE_STRING;
                        tokenIndex = ++i;
                        continue;
                    }
                    throw RuntimeException(50067, "invalid condition: " + conditionStr + " position: " + std::to_string(i));

                case TOKEN_TYPE_IDENTIFIER:
                    if (conditionStr[i] != ']') {
                        ++i;
                        continue;
                    }

                    // ends with ']'
                    tokens.push_back(new Token(expressionType, conditionStr.substr(tokenIndex, i - tokenIndex)));
                    expressionType = TOKEN_TYPE_NONE;
                    ++i;
                    continue;

                case TOKEN_TYPE_LEFT_PARENTHESIS:
                case TOKEN_TYPE_RIGHT_PARENTHESIS:
                case TOKEN_TYPE_COMMA:
                    tokens.push_back(new Token(expressionType, conditionStr.substr(tokenIndex, i - tokenIndex)));
                    expressionType = TOKEN_TYPE_NONE;
                    continue;

                case TOKEN_TYPE_OPERATOR:
                    if (tokenIndex + 1 == i && conditionStr[tokenIndex] == '!') {
                        if (conditionStr[i] == '=') {
                            ++i;
                            continue;
                        }
                    } else
                    if (conditionStr[i] == '|' || conditionStr[i] == '&' || conditionStr[i] == '!' || conditionStr[i] == '=') {
                        ++i;
                        continue;
                    }

                    tokens.push_back(new Token(expressionType, conditionStr.substr(tokenIndex, i - tokenIndex)));
                    expressionType = TOKEN_TYPE_NONE;
                    continue;

                case TOKEN_TYPE_STRING:
                    if (conditionStr[i] != '\'') {
                        ++i;
                        continue;
                    }

                    // ends with apostrophe
                    tokens.push_back(new Token(expressionType, conditionStr.substr(tokenIndex, i - tokenIndex)));
                    expressionType = TOKEN_TYPE_NONE;
                    ++i;
                    continue;

                case TOKEN_TYPE_NUMBER:
                    if ((conditionStr[i] >= '0' && conditionStr[i] <= '9') || conditionStr[i] == '.' || conditionStr[i] == 'e' || conditionStr[i] == 'E') {
                        ++i;
                        continue;
                    } else if ((conditionStr[i] >= 'a' && conditionStr[i] <= 'z') || conditionStr[i] == '_')
                        throw RuntimeException(50067, "invalid condition: " + conditionStr + " number on position: " + std::to_string(i));

                    tokens.push_back(new Token(expressionType, conditionStr.substr(tokenIndex, i - tokenIndex)));
                    expressionType = TOKEN_TYPE_NONE;
                    continue;
            }
        }

        // Reached end and the token is not finished
        if (expressionType == TOKEN_TYPE_STRING || expressionType == TOKEN_TYPE_IDENTIFIER)
            throw RuntimeException(50067, "invalid condition: " + conditionStr + " unfinished token: " + conditionStr.substr(tokenIndex, i - tokenIndex));

        if (expressionType != TOKEN_TYPE_NONE)
            tokens.push_back(new Token(expressionType, conditionStr.substr(tokenIndex, i - tokenIndex)));
    }

    BoolValue* Expression::buildCondition(const std::string& conditionStr, std::vector<Token*>& tokens, std::vector<Expression*>& stack) {
        uint64_t i = 0;
        while (stack.size() > 1 || i < tokens.size()) {
            if (stack.size() >= 2) {
                Expression* first = stack[stack.size() - 2];
                Expression* second = stack[stack.size() - 1];

                // TOKEN x
                if (first->isToken() && !second->isToken() && second->isBool()) {
                    Token* firstToken = dynamic_cast<Token*>(first);

                    // ! x
                    if (firstToken->tokenType == TOKEN_TYPE_OPERATOR && firstToken->stringValue == "!") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BOOL_OPERATOR_NOT, second, nullptr));
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
                    Token* leftToken = dynamic_cast<Token*>(left);
                    Token* rightToken = dynamic_cast<Token*>(right);

                    // ( x )
                    if (leftToken->tokenType == TOKEN_TYPE_LEFT_PARENTHESIS && rightToken->tokenType == TOKEN_TYPE_RIGHT_PARENTHESIS) {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(middle);
                        continue;
                    }
                }

                // STRING TOKEN STRING
                if (left->isString() && middle->isToken() && right->isString()) {
                    Token* middleToken = dynamic_cast<Token*>(middle);

                    // A == B
                    if (middleToken->stringValue == "==") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BOOL_OPERATOR_EQUAL, left, right));
                        continue;
                    }

                    // A != B
                    if (middleToken->stringValue == "!=") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BOOL_OPERATOR_NOT_EQUAL, left, right));
                        continue;
                    }
                }

                if (left->isBool() && middle->isToken() && right->isBool()) {
                    Token* middleToken = dynamic_cast<Token*>(middle);

                    // A && B
                    if (middleToken->stringValue == "&&") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BOOL_OPERATOR_AND, left, right));
                        continue;
                    }

                    // A || B
                    if (middleToken->stringValue == "||") {
                        stack.pop_back();
                        stack.pop_back();
                        stack.pop_back();
                        stack.push_back(new BoolValue(BOOL_OPERATOR_OR, left, right));
                        continue;
                    }
                }
            }

            if (i < tokens.size()) {
                Token* token = tokens[i++];

                switch (token->tokenType) {
                    case TOKEN_TYPE_IDENTIFIER:
                        if (token->stringValue == "op")
                            stack.push_back(new StringValue(STRING_OP, token->stringValue));
                        else if (token->stringValue == "true")
                            stack.push_back(new BoolValue(BOOL_VALUE_TRUE, nullptr, nullptr));
                        else if (token->stringValue == "false")
                            stack.push_back(new BoolValue(BOOL_VALUE_FALSE, nullptr, nullptr));
                        else
                            stack.push_back(new StringValue(STRING_SESSION_ATTRIBUTE, token->stringValue));
                        continue;

                    case TOKEN_TYPE_LEFT_PARENTHESIS:
                    case TOKEN_TYPE_RIGHT_PARENTHESIS:
                    case TOKEN_TYPE_COMMA:
                    case TOKEN_TYPE_OPERATOR:
                        stack.push_back(token);
                        continue;

                    case TOKEN_TYPE_STRING:
                        stack.push_back(new StringValue(STRING_VALUE, token->stringValue));
                        continue;
                }
            }

            throw RuntimeException(50067, "invalid condition: " + conditionStr + " stack size: " + std::to_string(stack.size()));
        }

        BoolValue* root = nullptr;
        if (stack.size() == 1)
            root = dynamic_cast<BoolValue *>(stack[0]);

        for (Expression* expression: stack) {
            if (!expression->isToken() && expression != root)
                delete expression;
        }
        stack.clear();

        if (root == nullptr || !root->isBool())
            throw RuntimeException(50067, "invalid condition: " + conditionStr + " is not evaluated to bool");

        for (Token *token: tokens)
            delete token;
        tokens.clear();

        return root;
    }

    Expression::Expression() {
    }

    Expression::~Expression() {
    }
}
