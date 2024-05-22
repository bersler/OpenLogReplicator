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
        static constexpr uint64_t FLAGS_FDOM = 1;
        static constexpr uint64_t FLAGS_TEMPORARY = 2;
        static constexpr uint64_t FLAGS_SYSTEM_GENERATED = 4;
        static constexpr uint64_t FLAGS_UNBOUND = 8;
        static constexpr uint64_t FLAGS_SECONDARY = 16;
        static constexpr uint64_t FLAGS_IN_MEMORY_TEMP = 32;
        static constexpr uint64_t FLAGS_PERMANENTLY_KEPT_JAVA_CLASS = 64;
        static constexpr uint64_t FLAGS_DROPPED = 128;
        static constexpr uint64_t FLAGS_SYNONYM_HAS_VPD_POLICIES = 256;
        static constexpr uint64_t FLAGS_SYNONYM_HAS_VPD_GROUPS = 512;
        static constexpr uint64_t FLAGS_SYNONYM_HAS_VPD_CONTEXT = 1024;
        static constexpr uint64_t FLAGS_CURSOR_DURATION = 2048;
        static constexpr uint64_t FLAGS_DEPENDENCY_TYPE_EVOLVED = 4096;
        static constexpr uint64_t FLAGS_DISABLE_FAST_VALIDATION = 8192;
        static constexpr uint64_t FLAGS_NESTED_TABLE_PARTITION = 16384;
        static constexpr uint64_t FLAGS_OBJERROR_ROW = 32768;
        static constexpr uint64_t FLAGS_METADATA_LINK = 65536;
        static constexpr uint64_t FLAGS_OBJECT_LINK = 131072;
        static constexpr uint64_t FLAGS_LONG_IDENTIFIER = 262144;
        static constexpr uint64_t FLAGS_ALLOW_FAST_ALTER_TABLE_UPGRADE = 524288;
        static constexpr uint64_t FLAGS_NOT_EDITIONABLE = 1048576;
        static constexpr uint64_t FLAGS_SPECIAL_INVOKER_RIGHTS = 2097152;
        static constexpr uint64_t FLAGS_ORACLE_SUPPLIED_OBJECT = 4194304;
        static constexpr uint64_t FLAGS_NO_FINE_GRAINED_DEP = 8388608;
        static constexpr uint64_t FLAGS_COMMON_OBJECT_MISMATCH = 16777216;
        static constexpr uint64_t FLAGS_LOCAL_MCODE = 33554432;
        static constexpr uint64_t FLAGS_LOCAL_DIANA = 67108864;
        static constexpr uint64_t FLAGS_FEDERATION_OBJECT = 134217728;
        static constexpr uint64_t FLAGS_DEFAULT_COLLATION = 268435456;
        static constexpr uint64_t FLAGS_ON_ALL_SHARDS = 536870912;
        static constexpr uint64_t FLAGS_SHARDED = 1073741824;
        static constexpr uint64_t FLAGS_REFERENCE = 2147483648;
        static constexpr uint64_t FLAGS_EXTENDED_DATA_LINK = 4294967296;
        static constexpr uint64_t FLAGS_BINARY_COLLATION = 8589934592;
        static constexpr uint64_t FLAGS_DISABLE_LOG_REPLICATION = 17179869184;

        static constexpr uint64_t NAME_LENGTH = 128;

        // 11.2
        static constexpr uint64_t TYPE_NEXT_OBJECT = 0;
        static constexpr uint64_t TYPE_INDEX = 1;
        static constexpr uint64_t TYPE_TABLE = 2;
        static constexpr uint64_t TYPE_CLUSTER = 3;
        static constexpr uint64_t TYPE_VIEW = 4;
        static constexpr uint64_t TYPE_SYNONYM = 5;
        static constexpr uint64_t TYPE_SEQUENCE = 6;
        static constexpr uint64_t TYPE_PROCEDURE = 7;
        static constexpr uint64_t TYPE_FUNCTION = 8;
        static constexpr uint64_t TYPE_PACKAGE = 9;
        static constexpr uint64_t TYPE_NON_EXISTENT = 10;
        static constexpr uint64_t TYPE_PACKAGE_BODY = 11;
        static constexpr uint64_t TYPE_TRIGGER = 12;
        static constexpr uint64_t TYPE_TYPE = 13;
        static constexpr uint64_t TYPE_TYPE_BODY = 14;
        static constexpr uint64_t TYPE_VARCHAR_STORED_LOB = 21;
        static constexpr uint64_t TYPE_LIBRARY = 22;
        static constexpr uint64_t TYPE_JAVA_SOURCE = 28;
        static constexpr uint64_t TYPE_JAVA_CLASS = 29;
        static constexpr uint64_t TYPE_INDEXTYPE = 32;
        static constexpr uint64_t TYPE_OPERATOR = 33;
        static constexpr uint64_t TYPE_LOB = 40;
        static constexpr uint64_t TYPE_MATERIALIZED_VIEW = 42;
        static constexpr uint64_t TYPE_DIMENSION = 43;
        static constexpr uint64_t TYPE_RULE_SET = 46;
        static constexpr uint64_t TYPE_XML_SCHEMA = 55;
        static constexpr uint64_t TYPE_JAVA_DATA = 56;
        static constexpr uint64_t TYPE_RULE = 59;
        static constexpr uint64_t TYPE_EVALUATION_CONTEXT = 62;
        static constexpr uint64_t TYPE_ASSEMBLY = 87;
        static constexpr uint64_t TYPE_CREDENTIAL = 90;
        static constexpr uint64_t TYPE_CUBE_DIMENSION = 92;
        static constexpr uint64_t TYPE_CUBE = 93;
        static constexpr uint64_t TYPE_MEASURE_FOLDER = 94;
        static constexpr uint64_t TYPE_CUBE_BUILD_PROCESS = 95;
        // 12.1
        static constexpr uint64_t TYPE_DIRECTORY = 23;
        // 12.2
        static constexpr uint64_t TYPE_HIERARCHY = 150;
        static constexpr uint64_t TYPE_ATTRIBUTE_DIMENSION = 151;
        static constexpr uint64_t TYPE_ANALYTIC_VIEW = 152;
        // 19.0
        static constexpr uint64_t TYPE_QUEUE = 24;

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
            return (type == TYPE_LOB || type == TYPE_VARCHAR_STORED_LOB);
        }

        [[nodiscard]] bool isTable() const {
            return (type == TYPE_TABLE);
        }

        [[nodiscard]] bool isTemporary() const {
            return flags.isSet64(FLAGS_TEMPORARY) ||
                   flags.isSet64(FLAGS_SECONDARY) ||
                   flags.isSet64(FLAGS_IN_MEMORY_TEMP);
        }

        [[nodiscard]] bool isDropped() const {
            return flags.isSet64(FLAGS_DROPPED);
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
