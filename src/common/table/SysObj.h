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

#define SYS_OBJ_FLAGS_FDOM                              1
#define SYS_OBJ_FLAGS_TEMPORARY                         2
#define SYS_OBJ_FLAGS_SYSTEM_GENERATED                  4
#define SYS_OBJ_FLAGS_UNBOUND                           8
#define SYS_OBJ_FLAGS_SECONDARY                         16
#define SYS_OBJ_FLAGS_IN_MEMORY_TEMP                    32
#define SYS_OBJ_FLAGS_PERMANENTLY_KEPT_JAVA_CLASS       64
#define SYS_OBJ_FLAGS_DROPPED                           128
#define SYS_OBJ_FLAGS_SYNONYM_HAS_VPD_POLICIES          256
#define SYS_OBJ_FLAGS_SYNONYM_HAS_VPD_GROUPS            512
#define SYS_OBJ_FLAGS_SYNONYM_HAS_VPD_CONTEXT           1024
#define SYS_OBJ_FLAGS_CURSOR_DURATION                   2048
#define SYS_OBJ_FLAGS_DEPENDENCY_TYPE_EVOLVED           4096
#define SYS_OBJ_FLAGS_DISABLE_FAST_VALIDATION           8192
#define SYS_OBJ_FLAGS_NESTED_TABLE_PARTITION            16384
#define SYS_OBJ_FLAGS_OBJERROR_ROW                      32768
#define SYS_OBJ_FLAGS_METADATA_LINK                     65536
#define SYS_OBJ_FLAGS_OBJECT_LINK                       131072
#define SYS_OBJ_FLAGS_LONG_IDENTIFIER                   262144
#define SYS_OBJ_FLAGS_ALLOW_FAST_ALTER_TABLE_UPGRADE    524288
#define SYS_OBJ_FLAGS_NOT_EDITIONABLE                   1048576
#define SYS_OBJ_FLAGS_SPECIAL_INVOKER_RIGHTS            2097152
#define SYS_OBJ_FLAGS_ORACLE_SUPPLIED_OBJECT            4194304
#define SYS_OBJ_FLAGS_NO_FINE_GRAINED_DEP               8388608
#define SYS_OBJ_FLAGS_COMMON_OBJECT_MISMATCH            16777216
#define SYS_OBJ_FLAGS_LOCAL_MCODE                       33554432
#define SYS_OBJ_FLAGS_LOCAL_DIANA                       67108864
#define SYS_OBJ_FLAGS_FEDERATION_OBJECT                 134217728
#define SYS_OBJ_FLAGS_DEFAULT_COLLATION                 268435456
#define SYS_OBJ_FLAGS_ON_ALL_SHARDS                     536870912
#define SYS_OBJ_FLAGS_SHARDED                           1073741824
#define SYS_OBJ_FLAGS_REFERENCE                         2147483648
#define SYS_OBJ_FLAGS_EXTENDED_DATA_LINK                4294967296
#define SYS_OBJ_FLAGS_BINARY_COLLATION                  8589934592
#define SYS_OBJ_FLAGS_DISABLE_LOG_REPLICATION           17179869184

#define SYS_OBJ_NAME_LENGTH                  128

// 11.2
#define SYS_OBJ_TYPE_NEXT_OBJECT             0
#define SYS_OBJ_TYPE_INDEX                   1
#define SYS_OBJ_TYPE_TABLE                   2
#define SYS_OBJ_TYPE_CLUSTER                 3
#define SYS_OBJ_TYPE_VIEW                    4
#define SYS_OBJ_TYPE_SYNONYM                 5
#define SYS_OBJ_TYPE_SEQUENCE                6
#define SYS_OBJ_TYPE_PROCEDURE               7
#define SYS_OBJ_TYPE_FUNCTION                8
#define SYS_OBJ_TYPE_PACKAGE                 9
#define SYS_OBJ_TYPE_NON_EXISTENT            10
#define SYS_OBJ_TYPE_PACKAGE_BODY            11
#define SYS_OBJ_TYPE_TRIGGER                 12
#define SYS_OBJ_TYPE_TYPE                    13
#define SYS_OBJ_TYPE_TYPE_BODY               14
#define SYS_OBJ_TYPE_VARCHAR_STORED_LOB      21
#define SYS_OBJ_TYPE_LIBRARY                 22
#define SYS_OBJ_TYPE_JAVA_SOURCE             28
#define SYS_OBJ_TYPE_JAVA_CLASS              29
#define SYS_OBJ_TYPE_INDEXTYPE               32
#define SYS_OBJ_TYPE_OPERATOR                33
#define SYS_OBJ_TYPE_LOB                     40
#define SYS_OBJ_TYPE_MATERIALIZED_VIEW       42
#define SYS_OBJ_TYPE_DIMENSION               43
#define SYS_OBJ_TYPE_RULE_SET                46
#define SYS_OBJ_TYPE_XML_SCHEMA              55
#define SYS_OBJ_TYPE_JAVA_DATA               56
#define SYS_OBJ_TYPE_RULE                    59
#define SYS_OBJ_TYPE_EVALUATION_CONTEXT      62
#define SYS_OBJ_TYPE_ASSEMBLY                87
#define SYS_OBJ_TYPE_CREDENTIAL              90
#define SYS_OBJ_TYPE_CUBE_DIMENSION          92
#define SYS_OBJ_TYPE_CUBE                    93
#define SYS_OBJ_TYPE_MEASURE_FOLDER          94
#define SYS_OBJ_TYPE_CUBE_BUILD_PROCESS      95
// 12.1
#define SYS_OBJ_TYPE_DIRECTORY               23
#define SYS_OBJ_TYPE_CREDENTIAL              90
// 12.2
#define SYS_OBJ_TYPE_HIERARCHY               150
#define SYS_OBJ_TYPE_ATTRIBUTE_DIMENSION     151
#define SYS_OBJ_TYPE_ANALYTIC_VIEW           152
// 19.0
#define SYS_OBJ_TYPE_QUEUE                   24

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
            return (type == SYS_OBJ_TYPE_LOB || type == SYS_OBJ_TYPE_VARCHAR_STORED_LOB);
        }

        [[nodiscard]] bool isTable() const {
            return (type == SYS_OBJ_TYPE_TABLE);
        }

        [[nodiscard]] bool isTemporary() const {
            return flags.isSet64(SYS_OBJ_FLAGS_TEMPORARY) ||
                   flags.isSet64(SYS_OBJ_FLAGS_SECONDARY) ||
                   flags.isSet64(SYS_OBJ_FLAGS_IN_MEMORY_TEMP);
        }

        [[nodiscard]] bool isDropped() const {
            return flags.isSet64(SYS_OBJ_FLAGS_DROPPED);
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
