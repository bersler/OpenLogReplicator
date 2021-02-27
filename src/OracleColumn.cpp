/* Column of a table in an Oracle database
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

#include "OracleColumn.h"

namespace OpenLogReplicator {
    OracleColumn::OracleColumn(typeCOL colNo, typeCOL guardSegNo, typeCOL segColNo, const char *name, uint64_t typeNo, uint64_t length, int64_t precision,
            int64_t scale, typeCOL numPk, uint64_t charsetId, bool nullable, bool invisible, bool storedAsLob, bool constraint, bool added, bool guard) :
		colNo(colNo),
		guardSegNo(guardSegNo),
		segColNo(segColNo),
		name(name),
		typeNo(typeNo),
		length(length),
		precision(precision),
		scale(scale),
		numPk(numPk),
		charsetId(charsetId),
		nullable(nullable),
		invisible(invisible),
		storedAsLob(storedAsLob),
		constraint(constraint),
		added(added),
		guard(guard) {
    }

    OracleColumn::~OracleColumn() {
    }

    ostream& operator<<(ostream& os, const OracleColumn& column) {
        os << column.segColNo << ": (" << column.colNo << ", \"" << column.name << "\", " << column.typeNo << ", " << column.length << ")";
        return os;
    }
}
