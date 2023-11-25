/* Header for OracleTable class
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

#include "types.h"
#include "expression/Token.h"

#ifndef ORACLE_OBJECT_H_
#define ORACLE_OBJECT_H_

namespace OpenLogReplicator {
    class BoolValue;
    class Ctx;
    class Expression;
    class OracleColumn;
    class OracleLob;
    class Token;

    class OracleTable final {
    public:
        typeObj obj;
        typeDataObj dataObj;
        typeUser user;
        typeCol cluCols;
        uint64_t totalPk;
        uint64_t totalLobs;
        typeOptions options;
        typeCol maxSegCol;
        typeCol guardSegNo;
        std::string owner;
        std::string name;
        std::string conditionStr;
        BoolValue* condition;
        std::vector<OracleColumn*> columns;
        std::vector<OracleLob*> lobs;
        std::vector<typeObj2> tablePartitions;
        std::vector<typeCol> pk;
        std::vector<Token*> tokens;
        std::vector<Expression*> stack;
        uint64_t systemTable;
        bool sys;

        OracleTable(typeObj newObj, typeDataObj newDataObj, typeUser newUser, typeCol newCluCols, typeOptions newOptions, const std::string& newOwner,
                    const std::string& newName);
        virtual ~OracleTable();

        void addColumn(OracleColumn* column);
        void addLob(OracleLob* lob);
        void addTablePartition(typeObj newObj, typeDataObj newDataObj);
        bool matchesCondition(Ctx* ctx, char op, const std::unordered_map<std::string, std::string>* attributes);
        void setConditionStr(const std::string& newConditionStr);

        friend std::ostream& operator<<(std::ostream& os, const OracleTable& table);
    };
}

#endif
