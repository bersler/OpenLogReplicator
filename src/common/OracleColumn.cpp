/* Column of a table in an Oracle database
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
#include "OracleColumn.h"

namespace OpenLogReplicator {
    OracleColumn::OracleColumn(typeCol col, typeCol guardSeg, typeCol segCol, std::string& name, uint64_t type, uint64_t length, int64_t precision,
                               int64_t scale, typeCol numPk, uint64_t charsetId, bool nullable, bool invisible, bool storedAsLob, bool constraint, bool nested,
                               bool unused, bool added, bool guard) :
            col(col),
            guardSeg(guardSeg),
            segCol(segCol),
            name(name),
            type(type),
            length(length),
            precision(precision),
            scale(scale),
            numPk(numPk),
            charsetId(charsetId),
            nullable(nullable),
            invisible(invisible),
            storedAsLob(storedAsLob),
            constraint(constraint),
            nested(nested),
            unused(unused),
            added(added),
            guard(guard) {
        if (segCol > 1000)
            throw DataException("invalid segCol value (" + std::to_string(segCol) + "), metadata error");
    }

    std::ostream& operator<<(std::ostream& os, const OracleColumn& column) {
        os << column.segCol << ": (" << column.col << ", '" << column.name << "', " << column.type << ", " << column.length << ")";
        return os;
    }
}
