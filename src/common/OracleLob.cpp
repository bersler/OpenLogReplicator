/* LOB in a table in an Oracle database
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

#include "../common/DataException.h"
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
    }

    void OracleLob::addIndex(typeDataObj newDataObj) {
        lobIndexes.push_back(newDataObj);
    }

    std::ostream& operator<<(std::ostream& os, const OracleLob& lob) {
        os << lob.obj << ": (" << lob.col << ", " << lob.intCol << ", " << lob.lObj << ")";
        return os;
    }
}
