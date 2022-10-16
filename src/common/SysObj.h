/* Header for SysObj class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "types.h"
#include "typeIntX.h"
#include "typeRowId.h"

#ifndef SYS_OBJ_H_
#define SYS_OBJ_H_

#define SYS_OBJ_FLAGS_TEMPORARY              2
#define SYS_OBJ_FLAGS_SECONDARY              16
#define SYS_OBJ_FLAGS_IN_MEMORY_TEMP         32
#define SYS_OBJ_FLAGS_DROPPED                128
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
#define SYS_OBJ_TYPE_LOB                     21
#define SYS_OBJ_TYPE_LIBRARY                 22
#define SYS_OBJ_TYPE_JAVA_SOURCE             28
#define SYS_OBJ_TYPE_JAVA_CLASS              29
#define SYS_OBJ_TYPE_INDEXTYPE               32
#define SYS_OBJ_TYPE_OPERATOR                33
#define SYS_OBJ_TYPE_MATERIALIZED_VIEW       42
#define SYS_OBJ_TYPE_DIMENSION               43
#define SYS_OBJ_TYPE_RULE_SET                46
#define SYS_OBJ_TYPE_XML_SCHEMA              55
#define SYS_OBJ_TYPE_JAVA_DATA               56
#define SYS_OBJ_TYPE_RULE                    59
#define SYS_OBJ_TYPE_EVALUATION_CONTXT       62
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
    class SysObjNameKey {
    public:
        SysObjNameKey(typeUser newOwner, const char* newName, typeObj newObj);

        bool operator<(const SysObjNameKey& other) const;

        typeUser owner;
        std::string name;
        typeObj obj;
    };

    class SysObj {
    public:
        SysObj(typeRowId& newRowId, typeUser newOwner, typeObj newObj, typeDataObj newDataObj, typeType newType, const char* newName, uint64_t newFlags1,
               uint64_t newFlags2, bool newSingle, bool newTouched);

        bool operator!=(const SysObj& other) const;
        [[nodiscard]] bool isLob() const;
        [[nodiscard]] bool isTable() const;
        [[nodiscard]] bool isTemporary() const;
        [[nodiscard]] bool isDropped() const;

        typeRowId rowId;
        typeUser owner;
        typeObj obj;
        typeDataObj dataObj;        // NULL
        typeType type;
        std::string name;
        typeIntX flags;             // NULL
        bool single;
        bool touched;
    };
}

#endif
