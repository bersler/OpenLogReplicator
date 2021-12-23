/* Header for OracleObject class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <vector>

#include "types.h"

#ifndef ORACLEOBJECT_H_
#define ORACLEOBJECT_H_

namespace OpenLogReplicator {
    class OracleColumn;

    class OracleObject {
    public:
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeUSER user;
        typeCOL cluCols;
        uint64_t totalPk;
        typeOPTIONS options;
        typeCOL maxSegCol;
        typeCOL guardSegNo;
        std::string owner;
        std::string name;
        std::vector<OracleColumn*> columns;
        std::vector<typeOBJ2> partitions;
        std::vector<uint16_t> pk;
        uint64_t systemTable;
        bool sys;

        void addColumn(OracleColumn* column);
        void addPartition(typeOBJ partitionObj, typeDATAOBJ partitionDataObj);
        void updatePK(void);

        OracleObject(typeOBJ obj, typeDATAOBJ dataObj, typeUSER user, typeCOL cluCols, typeOPTIONS options, std::string& owner, std::string& name);
        virtual ~OracleObject();

        friend std::ostream& operator<<(std::ostream& os, const OracleObject& object);
    };
}

#endif
