/* Header for OracleTable class
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
        static constexpr typeOptions OPTIONS_DEBUG_TABLE = 1U << 0;
        static constexpr typeOptions OPTIONS_SYSTEM_TABLE = 1U << 1;
        static constexpr typeOptions OPTIONS_SCHEMA_TABLE = 1U << 2;

        static constexpr uint64_t VCONTEXT_LENGTH = 30;
        static constexpr uint64_t VPARAMETER_LENGTH = 4000;
        static constexpr uint64_t VPROPERTY_LENGTH = 4000;

        static constexpr uint64_t SYS_CCOL = 1;
        static constexpr uint64_t SYS_CDEF = 2;
        static constexpr uint64_t SYS_COL = 3;
        static constexpr uint64_t SYS_DEFERRED_STG = 4;
        static constexpr uint64_t SYS_ECOL = 5;
        static constexpr uint64_t SYS_LOB = 6;
        static constexpr uint64_t SYS_LOB_COMP_PART = 7;
        static constexpr uint64_t SYS_LOB_FRAG = 8;
        static constexpr uint64_t SYS_OBJ = 9;
        static constexpr uint64_t SYS_TAB = 10;
        static constexpr uint64_t SYS_TABPART = 11;
        static constexpr uint64_t SYS_TABCOMPART = 12;
        static constexpr uint64_t SYS_TABSUBPART = 13;
        static constexpr uint64_t SYS_TS = 14;
        static constexpr uint64_t SYS_USER = 15;
        static constexpr uint64_t XDB_TTSET = 16;
        static constexpr uint64_t XDB_XNM = 17;
        static constexpr uint64_t XDB_XPT = 18;
        static constexpr uint64_t XDB_XQN = 19;

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
        std::string tokSuf;
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
        bool matchesCondition(const Ctx* ctx, char op, const std::unordered_map<std::string, std::string>* attributes);
        void setConditionStr(const std::string& newConditionStr);

        friend std::ostream& operator<<(std::ostream& os, const OracleTable& table);
    };
}

#endif
