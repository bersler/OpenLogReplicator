/* Column of a table in an Oracle database
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

#include "../common/table/SysCol.h"
#include "OracleColumn.h"

namespace OpenLogReplicator {
    OracleColumn::OracleColumn(typeCol newCol, typeCol newGuardSeg, typeCol newSegCol, const std::string& newName, uint64_t newType, uint64_t newLength,
                               int64_t newPrecision, int64_t newScale, typeCol newNumPk, uint64_t newCharsetId, bool newNullable, bool newHidden,
                               bool newStoredAsLob, bool newSystemGenerated, bool newNested, bool newUnused, bool newAdded, bool newGuard, bool newXmlType) :
            col(newCol),
            guardSeg(newGuardSeg),
            segCol(newSegCol),
            name(newName),
            type(newType),
            length(newLength),
            precision(newPrecision),
            scale(newScale),
            numPk(newNumPk),
            charsetId(newCharsetId),
            nullable(newNullable),
            hidden(newHidden),
            storedAsLob(newStoredAsLob),
            systemGenerated(newSystemGenerated),
            nested(newNested),
            unused(newUnused),
            added(newAdded),
            guard(newGuard),
            xmlType(newXmlType),
            nullWarning(false) {
    }

    std::ostream& operator<<(std::ostream& os, const OracleColumn& column) {
        os << column.segCol << ": (" << column.col << ", '" << column.name << "', " << column.type << ", " << column.length << ")";
        return os;
    }
}
