/* LOB in a table in an Oracle database
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

#include "OracleLob.h"
#include "OracleTable.h"

namespace OpenLogReplicator {
    OracleLob::OracleLob(OracleTable* newTable, typeObj newObj, typeObj newDataObj, typeObj newLObj, typeCol newCol, typeCol newIntCol) :
            table(newTable),
            obj(newObj),
            dataObj(newDataObj),
            lObj(newLObj),
            col(newCol),
            intCol(newIntCol) {
    }

    OracleLob::~OracleLob() {
        lobIndexes.clear();
        lobPartitions.clear();
        lobPageMap.clear();
    }

    void OracleLob::addIndex(typeDataObj newDataObj) {
        lobIndexes.push_back(newDataObj);
    }

    void OracleLob::addPartition(typeDataObj newDataObj, uint16_t pageSize) {
        lobPartitions.push_back(newDataObj);
        lobPageMap[newDataObj] = pageSize;
    }

    uint32_t OracleLob::checkLobPageSize(typeDataObj newDataObj) {
        auto lobPageMapIt = lobPageMap.find(newDataObj);
        if (lobPageMapIt != lobPageMap.end())
            return lobPageMapIt->second;

        // Default value?
        return 8132;
    }

    std::ostream& operator<<(std::ostream& os, const OracleLob& lob) {
        os << lob.obj << ": (" << lob.col << ", " << lob.intCol << ", " << lob.lObj << ")";
        return os;
    }
}
