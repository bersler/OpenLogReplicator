/* Header for DbTable class
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

#ifndef DB_TABLE_H_
#define DB_TABLE_H_

#include <unordered_map>
#include <vector>

#include "expression/Token.h"
#include "types/Types.h"

namespace OpenLogReplicator {
    class BoolValue;
    class Ctx;
    class DbColumn;
    class DbLob;
    class Expression;
    class Token;

    class DbTable final {
    public:
        enum class OPTIONS : unsigned char {
            DEFAULT      = 0,
            DEBUG_TABLE  = 1 << 0,
            SYSTEM_TABLE = 1 << 1,
            SCHEMA_TABLE = 1 << 2
        };

        enum class TABLE : unsigned char {
            NONE,
            SYS_CCOL,
            SYS_CDEF,
            SYS_COL,
            SYS_DEFERRED_STG,
            SYS_ECOL,
            SYS_LOB,
            SYS_LOB_COMP_PART,
            SYS_LOB_FRAG,
            SYS_OBJ,
            SYS_TAB,
            SYS_TABPART,
            SYS_TABCOMPART,
            SYS_TABSUBPART,
            SYS_TS,
            SYS_USER,
            XDB_TTSET,
            XDB_XNM,
            XDB_XPT,
            XDB_XQN
        };

        static constexpr uint VCONTEXT_LENGTH{30};
        static constexpr uint VPARAMETER_LENGTH{4000};
        static constexpr uint VPROPERTY_LENGTH{4000};

        typeObj obj;
        typeDataObj dataObj;
        typeUser user;
        typeCol cluCols;
        typeCol totalPk{0};
        typeCol totalLobs{0};
        OPTIONS options;
        typeCol maxSegCol{0};
        typeCol guardSegNo{-1};
        std::string owner;
        std::string name;
        std::string tokSuf;
        std::string condition;
        BoolValue* conditionValue{nullptr};
        std::vector<DbColumn*> columns;
        std::vector<DbLob*> lobs;
        std::vector<typeObj2> tablePartitions;
        std::vector<typeCol> pk;
        std::vector<typeCol> tagCols;
        std::vector<Token*> tokens;
        std::vector<Expression*> stack;
        TABLE systemTable;
        bool sys;

        DbTable(typeObj newObj, typeDataObj newDataObj, typeUser newUser, typeCol newCluCols, OPTIONS newOptions, std::string newOwner,
                std::string newName);
        ~DbTable();

        void addColumn(DbColumn* column);
        void addLob(DbLob* lob);
        void addTablePartition(typeObj newObj, typeDataObj newDataObj);
        bool matchesCondition(const Ctx* ctx, char op, const AttributeMap* attributes) const;
        void setCondition(const std::string& newCondition);

        static bool isDebugTable(OPTIONS options) {
            return (static_cast<uint>(options) & static_cast<uint>(OPTIONS::DEBUG_TABLE)) != 0;
        }

        static bool isSchemaTable(OPTIONS options) {
            return (static_cast<uint>(options) & static_cast<uint>(OPTIONS::SCHEMA_TABLE)) != 0;
        }

        static bool isSystemTable(OPTIONS options) {
            return (static_cast<uint>(options) & static_cast<uint>(OPTIONS::SYSTEM_TABLE)) != 0;
        }

        friend std::ostream& operator<<(std::ostream& os, const DbTable& table);
    };
}

#endif
