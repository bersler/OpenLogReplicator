/* Definition of schema SYS.OBJ$
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

#include "../types.h"
#include "../typeIntX.h"
#include "../typeRowId.h"

#ifndef SYS_OBJ_H_
#define SYS_OBJ_H_

namespace OpenLogReplicator {
    class SysObjNameKey final {
    public:
        SysObjNameKey(typeUser newOwner, const char* newName, typeObj newObj, typeDataObj newDataObj) :
                owner(newOwner),
                name(newName),
                obj(newObj),
                dataObj(newDataObj) {
        }

        bool operator<(const SysObjNameKey& other) const {
            if (owner < other.owner)
                return true;
            if (other.owner < owner)
                return false;
            int cmp = other.name.compare(name);
            if (0 < cmp)
                return true;
            if (cmp < 0)
                return false;
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (dataObj < other.dataObj)
                return true;
            if (other.dataObj < dataObj)
                return false;
            return false;
        }

        typeUser owner;
        std::string name;
        typeObj obj;
        typeDataObj dataObj;
    };

    class SysObj final {
    public:
        enum FLAGS {
            FDOM = 1UL << 0, TEMPORARY = 1UL << 1, SYSTEM_GENERATED = 1UL << 2, UNBOUND = 1UL << 3, SECONDARY = 1UL << 4, IN_MEMORY_TEMP = 1UL << 5,
            PERMANENTLY_KEPT_JAVA_CLASS = 1UL << 6, DROPPED = 1UL << 7, SYNONYM_HAS_VPD_POLICIES = 1UL << 8, SYNONYM_HAS_VPD_GROUPS = 1UL << 9,
            SYNONYM_HAS_VPD_CONTEXT = 1UL << 10, CURSOR_DURATION = 1UL << 11, DEPENDENCY_TYPE_EVOLVED = 1UL << 12, DISABLE_FAST_VALIDATION = 1UL << 13,
            NESTED_TABLE_PARTITION = 1UL << 14, OBJERROR_ROW = 1UL << 15, METADATA_LINK = 1UL << 16, OBJECT_LINK = 1UL << 17, LONG_IDENTIFIER = 1UL << 18,
            ALLOW_FAST_ALTER_TABLE_UPGRADE = 1UL << 19, NOT_EDITIONABLE = 1UL << 20, SPECIAL_INVOKER_RIGHTS = 1UL << 21, DATABASE_SUPPLIED_OBJECT = 1UL << 22,
            NO_FINE_GRAINED_DEP = 1UL << 23, COMMON_OBJECT_MISMATCH = 1UL << 24, LOCAL_MCODE = 1UL << 25, LOCAL_DIANA = 1UL << 26,
            FEDERATION_OBJECT = 1UL << 27, DEFAULT_COLLATION = 1UL << 28, ON_ALL_SHARDS = 1UL << 29, SHARDED = 1UL << 30, REFERENCE = 1UL << 31,
            EXTENDED_DATA_LINK = 1UL << 32, BINARY_COLLATION = 1UL << 32, DISABLE_LOG_REPLICATION = 1UL << 34
        };

        static constexpr uint NAME_LENGTH = 128;

        enum OBJTYPE {
            // 11.2
            NEXT_OBJECT = 0, INDEX = 1, TABLE = 2, CLUSTER = 3, VIEW = 4, SYNONYM = 5, SEQUENCE = 6, PROCEDURE = 7, FUNCTION = 8, PACKAGE = 9,
            NON_EXISTENT = 10, PACKAGE_BODY = 11, TRIGGER = 12, TYPE = 13, TYPE_BODY = 14, VARCHAR_STORED_LOB = 21, LIBRARY = 22, JAVA_SOURCE = 28,
            JAVA_CLASS = 29, INDEXTYPE = 32, OPERATOR = 33, LOB = 40, MATERIALIZED_VIEW = 42, DIMENSION = 43, RULE_SET = 46, XML_SCHEMA = 55,
            JAVA_DATA = 56, RULE = 59, EVALUATION_CONTEXT = 62, ASSEMBLY = 87, CREDENTIAL = 90, CUBE_DIMENSION = 92, CUBE = 93, MEASURE_FOLDER = 94,
            CUBE_BUILD_PROCESS = 95,
            // 12.1
            DIRECTORY = 23,
            // 12.2
            HIERARCHY = 150, ATTRIBUTE_DIMENSION = 151, ANALYTIC_VIEW = 152,
            // 19.0
            QUEUE = 24
        };

        SysObj(typeRowId newRowId, typeUser newOwner, typeObj newObj, typeDataObj newDataObj, typeType newType, const char* newName, uint64_t newFlags1,
               uint64_t newFlags2, bool newSingle) :
                rowId(newRowId),
                owner(newOwner),
                obj(newObj),
                dataObj(newDataObj),
                type(newType),
                name(newName),
                flags(newFlags1, newFlags2),
                single(newSingle) {
        }

        bool operator!=(const SysObj& other) const {
            return (other.rowId != rowId) || (other.owner != owner) || (other.obj != obj) || (other.dataObj != dataObj) || (other.type != type) ||
                   (other.name != name) || (other.flags != flags);
        }

        [[nodiscard]] bool isLob() const {
            return (type == OBJTYPE::LOB || type == OBJTYPE::VARCHAR_STORED_LOB);
        }

        [[nodiscard]] bool isTable() const {
            return (type == OBJTYPE::TABLE);
        }

        [[nodiscard]] bool isTemporary() const {
            return flags.isSet64(FLAGS::TEMPORARY) ||
                   flags.isSet64(FLAGS::SECONDARY) ||
                   flags.isSet64(FLAGS::IN_MEMORY_TEMP);
        }

        [[nodiscard]] bool isDropped() const {
            return flags.isSet64(FLAGS::DROPPED);
        }

        typeRowId rowId;
        typeUser owner;
        typeObj obj;
        typeDataObj dataObj;        // NULL
        typeType type;
        std::string name;
        typeIntX flags;             // NULL
        bool single;
    };
}

#endif
