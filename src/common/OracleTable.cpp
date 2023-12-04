/* Table from Oracle Database
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

#include "Ctx.h"
#include "OracleColumn.h"
#include "OracleLob.h"
#include "OracleTable.h"
#include "exception/RuntimeException.h"
#include "expression/BoolValue.h"
#include "expression/Token.h"

namespace OpenLogReplicator {
    OracleTable::OracleTable(typeObj newObj, typeDataObj newDataObj, typeUser newUser, typeCol newCluCols, typeOptions newOptions, const std::string& newOwner,
                             const std::string& newName) :
            obj(newObj),
            dataObj(newDataObj),
            user(newUser),
            cluCols(newCluCols),
            totalPk(0),
            totalLobs(0),
            options(newOptions),
            maxSegCol(0),
            guardSegNo(-1),
            owner(newOwner),
            name(newName),
            conditionStr(""),
            condition(nullptr) {

        systemTable = 0;
        if (this->owner == "SYS") {
            sys = true;
            if (this->name == "CCOL$")
                systemTable = TABLE_SYS_CCOL;
            else if (this->name == "CDEF$")
                systemTable = TABLE_SYS_CDEF;
            else if (this->name == "COL$")
                systemTable = TABLE_SYS_COL;
            else if (this->name == "DEFERRED_STG$")
                systemTable = TABLE_SYS_DEFERRED_STG;
            else if (this->name == "ECOL$")
                systemTable = TABLE_SYS_ECOL;
            else if (this->name == "LOB$")
                systemTable = TABLE_SYS_LOB;
            else if (this->name == "LOBCOMPPART$")
                systemTable = TABLE_SYS_LOB_COMP_PART;
            else if (this->name == "LOBFRAG$")
                systemTable = TABLE_SYS_LOB_FRAG;
            else if (this->name == "OBJ$")
                systemTable = TABLE_SYS_OBJ;
            else if (this->name == "TAB$")
                systemTable = TABLE_SYS_TAB;
            else if (this->name == "TABPART$")
                systemTable = TABLE_SYS_TABPART;
            else if (this->name == "TABCOMPART$")
                systemTable = TABLE_SYS_TABCOMPART;
            else if (this->name == "TABSUBPART$")
                systemTable = TABLE_SYS_TABSUBPART;
            else if (this->name == "TS$")
                systemTable = TABLE_SYS_TS;
            else if (this->name == "USER$")
                systemTable = TABLE_SYS_USER;
        } else if (this->owner == "XDB") {
            sys = true;
            if (this->name == "XDB$TTSET")
                systemTable = TABLE_XDB_TTSET;
            else if (this->name.substr(0, 4) == "X$NM") {
                systemTable = TABLE_XDB_XNM;
                tokSuf = this->name.substr(4);
            } else if (this->name.substr(0, 4) == "X$PT") {
                systemTable = TABLE_XDB_XPT;
                tokSuf = this->name.substr(4);
            } else if (this->name.substr(0, 4) == "X$QN") {
                systemTable = TABLE_XDB_XQN;
                tokSuf = this->name.substr(4);
            }
        } else
            sys = false;
    }

    OracleTable::~OracleTable() {
        for (OracleColumn* column: columns)
            delete column;
        pk.clear();
        columns.clear();
        tablePartitions.clear();

        for (OracleLob* lob: lobs)
            delete lob;
        lobs.clear();

        for (Expression *expression: stack)
            if (!expression->isToken())
                delete expression;
        stack.clear();

        for (Token *token: tokens)
            delete token;
        tokens.clear();

        if (condition != nullptr)
            delete condition;
        condition = nullptr;
    }

    void OracleTable::addColumn(OracleColumn* column) {
        if (column->segCol != static_cast<typeCol>(columns.size() + 1))
            throw RuntimeException(50002, "trying to insert table: " + owner + "." + name + " (obj: " + std::to_string(obj) +
                                   ", dataobj: " + std::to_string(dataObj) + ") column: " + column->name + " (col#: " + std::to_string(column->col) +
                                   ", segcol#: " + std::to_string(column->segCol) + ") on position " + std::to_string(columns.size() + 1));

        if (column->guard)
            guardSegNo = column->segCol - static_cast<typeCol>(1);

        totalPk += column->numPk;
        if (column->numPk > 0)
            pk.push_back(columns.size());

        if (column->segCol > maxSegCol)
            maxSegCol = column->segCol;

        columns.push_back(column);
    }

    void OracleTable::addLob(OracleLob* lob) {
        ++totalLobs;
        lobs.push_back(lob);
    }

    void OracleTable::addTablePartition(typeObj newObj, typeDataObj newDataObj) {
        typeObj2 objx = (static_cast<typeObj2>(newObj) << 32) | static_cast<typeObj2>(newDataObj);
        tablePartitions.push_back(objx);
    }

    bool OracleTable::matchesCondition(Ctx* ctx, char op, const std::unordered_map<std::string, std::string>* attributes) {
        bool result = true;
        if (condition != nullptr)
            result = condition->evaluateToBool(op, attributes);

        if (ctx->trace & TRACE_CONDITION)
            ctx->logTrace(TRACE_CONDITION, "matchesCondition: table: " + owner + "." + name + ", condition: " + conditionStr + ", result: " +
                                           std::to_string(result));
        return result;
    }

    void OracleTable::setConditionStr(const std::string& newConditionStr) {
        this->conditionStr = newConditionStr;
        if (newConditionStr == "")
            return;

        Expression::buildTokens(newConditionStr, tokens);
        condition = Expression::buildCondition(newConditionStr, tokens, stack);
    }

    std::ostream& operator<<(std::ostream& os, const OracleTable& table) {
        os << "('" << table.owner << "'.'" << table.name << "', " << std::dec << table.obj << ", " << table.dataObj << ", " << table.cluCols << ", " <<
                table.maxSegCol << ")\n";
        for (OracleColumn* column : table.columns)
            os << "     - " << *column << '\n';
        return os;
    }
}
