/* Table from the database
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

#include <algorithm>
#include <utility>

#include "Ctx.h"
#include "DbColumn.h"
#include "DbLob.h"
#include "DbTable.h"
#include "exception/RuntimeException.h"
#include "expression/BoolValue.h"
#include "expression/Token.h"

namespace OpenLogReplicator {
    DbTable::DbTable(typeObj newObj, typeDataObj newDataObj, typeUser newUser, typeCol newCluCols, OPTIONS newOptions, std::string newOwner,
                     std::string newName):
            obj(newObj),
            dataObj(newDataObj),
            user(newUser),
            cluCols(newCluCols),
            options(newOptions),
            owner(std::move(newOwner)),
            name(std::move(newName)) {
        systemTable = TABLE::NONE;
        if (this->owner == "SYS") {
            sys = true;
            if (this->name == "CCOL$")
                systemTable = TABLE::SYS_CCOL;
            else if (this->name == "CDEF$")
                systemTable = TABLE::SYS_CDEF;
            else if (this->name == "COL$")
                systemTable = TABLE::SYS_COL;
            else if (this->name == "DEFERRED_STG$")
                systemTable = TABLE::SYS_DEFERRED_STG;
            else if (this->name == "ECOL$")
                systemTable = TABLE::SYS_ECOL;
            else if (this->name == "LOB$")
                systemTable = TABLE::SYS_LOB;
            else if (this->name == "LOBCOMPPART$")
                systemTable = TABLE::SYS_LOB_COMP_PART;
            else if (this->name == "LOBFRAG$")
                systemTable = TABLE::SYS_LOB_FRAG;
            else if (this->name == "OBJ$")
                systemTable = TABLE::SYS_OBJ;
            else if (this->name == "TAB$")
                systemTable = TABLE::SYS_TAB;
            else if (this->name == "TABPART$")
                systemTable = TABLE::SYS_TABPART;
            else if (this->name == "TABCOMPART$")
                systemTable = TABLE::SYS_TABCOMPART;
            else if (this->name == "TABSUBPART$")
                systemTable = TABLE::SYS_TABSUBPART;
            else if (this->name == "TS$")
                systemTable = TABLE::SYS_TS;
            else if (this->name == "USER$")
                systemTable = TABLE::SYS_USER;
        } else if (this->owner == "XDB") {
            sys = true;
            if (this->name == "XDB$TTSET")
                systemTable = TABLE::XDB_TTSET;
            else if (this->name.substr(0, 4) == "X$NM") {
                systemTable = TABLE::XDB_XNM;
                tokSuf = this->name.substr(4);
            } else if (this->name.substr(0, 4) == "X$PT") {
                systemTable = TABLE::XDB_XPT;
                tokSuf = this->name.substr(4);
            } else if (this->name.substr(0, 4) == "X$QN") {
                systemTable = TABLE::XDB_XQN;
                tokSuf = this->name.substr(4);
            }
        } else
            sys = false;
    }

    DbTable::~DbTable() {
        for (const DbColumn* column: columns)
            delete column;
        pk.clear();
        columns.clear();
        tablePartitions.clear();

        for (const DbLob* lob: lobs)
            delete lob;
        lobs.clear();

        for (Expression* expression: stack)
            if (!expression->isToken())
                delete expression;
        stack.clear();

        for (const Token* token: tokens)
            delete token;
        tokens.clear();

        delete conditionValue;
        conditionValue = nullptr;
    }

    void DbTable::addColumn(DbColumn* column) {
        if (unlikely(column->segCol != static_cast<typeCol>(columns.size() + 1)))
            throw RuntimeException(50002, "trying to insert table: " + owner + "." + name + " (obj: " + std::to_string(obj) +
                                   ", dataobj: " + std::to_string(dataObj) + ") column: " + column->name + " (col#: " + std::to_string(column->col) +
                                   ", segcol#: " + std::to_string(column->segCol) + ") on position " + std::to_string(columns.size() + 1));

        if (column->guard)
            guardSegNo = column->segCol - static_cast<typeCol>(1);

        totalPk += column->numPk;
        if (column->numPk > 0)
            pk.push_back(columns.size());

        maxSegCol = std::max(column->segCol, maxSegCol);

        columns.push_back(column);
    }

    void DbTable::addLob(DbLob* lob) {
        ++totalLobs;
        lobs.push_back(lob);
    }

    void DbTable::addTablePartition(typeObj newObj, typeDataObj newDataObj) {
        const typeObj2 objx = (static_cast<typeObj2>(newObj) << 32) | static_cast<typeObj2>(newDataObj);
        tablePartitions.push_back(objx);
    }

    bool DbTable::matchesCondition(const Ctx* ctx, char op, const AttributeMap* attributes) const {
        bool result = true;
        if (conditionValue != nullptr)
            result = conditionValue->evaluateToBool(op, attributes);

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CONDITION)))
            ctx->logTrace(Ctx::TRACE::CONDITION, "matchesCondition: table: " + owner + "." + name + ", condition: " + condition + ", result: " +
                          std::to_string(result ? 1 : 0));
        return result;
    }

    void DbTable::setCondition(const std::string& newCondition) {
        this->condition = newCondition;
        if (newCondition.empty())
            return;

        Expression::buildTokens(newCondition, tokens);
        conditionValue = Expression::buildCondition(newCondition, tokens, stack);
    }

    std::ostream& operator<<(std::ostream& os, const DbTable& table) {
        os << "('" << table.owner << "'.'" << table.name << "', " << std::dec << table.obj << ", " << table.dataObj << ", " << table.cluCols << ", " <<
                table.maxSegCol << ")\n";
        for (const DbColumn* column: table.columns)
            os << "     - " << *column << '\n';
        return os;
    }
}
